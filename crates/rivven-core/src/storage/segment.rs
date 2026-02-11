//! Log segment storage with memory-mapped reads.
//!
//! # Data Directory Exclusivity
//!
//! **IMPORTANT**: The Rivven data directory MUST be treated as exclusive to the broker process.
//! External modification of segment files (e.g., by admin scripts or concurrent processes)
//! while the broker is running can cause undefined behavior including SIGBUS signals.
//!
//! This is a fundamental property of memory-mapped I/O and is true for all production-grade
//! storage engines (Kafka, Redpanda, RocksDB, etc.).
//!
//! Best practices:
//! - Use dedicated storage volumes for Rivven data directories
//! - Never modify segment files while the broker is running
//! - Use the Admin API for all data management operations
//! - If external tooling is required, stop the broker first

use crate::{Error, Message, Result};
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const INDEX_ENTRY_SIZE: usize = 12; // 4 bytes relative offset, 8 bytes position
const LOG_SUFFIX: &str = "log";
const INDEX_SUFFIX: &str = "index";
/// Index every 4KB of data (sparse indexing for performance)
const INDEX_INTERVAL_BYTES: u64 = 4096;

/// Represents a segment of the log on disk
/// A segment consists of a .log file (data) and a .index file (sparse index)
#[derive(Debug)]
pub struct Segment {
    base_offset: u64,
    log_path: PathBuf,
    index_path: PathBuf,
    log_file: Arc<Mutex<BufWriter<File>>>,
    current_size: u64,
    index_buffer: Vec<(u32, u64)>, // Relative offset -> Position
    /// Position of last index entry (for sparse indexing)
    last_index_position: u64,
    /// Pending index entries to batch write (behind std::sync::Mutex for &self flush)
    pending_index_entries: std::sync::Mutex<Vec<(u32, u64)>>,
    /// Fsync policy for segment writes (H-1 fix).
    /// Controls durability guarantees: None for OS page cache only,
    /// EveryWrite for per-append fsync, EveryNWrites for batched fsync.
    sync_policy: SegmentSyncPolicy,
    /// Counter for tracking writes between fsyncs (for EveryNWrites policy)
    writes_since_sync: std::sync::atomic::AtomicU64,
    /// Cached read-only memory map (H-11/H-12 fix).
    /// Invalidated on write, lazily re-created on next read.
    /// Avoids creating a new mmap per read and moves the blocking
    /// mmap syscall off the hot path for repeat reads.
    cached_mmap: tokio::sync::RwLock<Option<Arc<Mmap>>>,
}

/// Fsync policy for segment writes (H-1 fix).
///
/// Controls when segment data is flushed to durable storage via fsync/fdatasync.
/// Mirrors Kafka's `log.flush.interval.messages` concept.
///
/// - `None`: No fsync — data lives in OS page cache until kernel writeback (fastest, least durable)
/// - `EveryWrite`: fsync after every append — maximum durability, equivalent to `acks=all` + sync
/// - `EveryNWrites(n)`: fsync every N writes — balances throughput and durability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentSyncPolicy {
    /// No fsync — relies on OS page cache writeback (fastest)
    None,
    /// fsync after every write (maximum durability)
    EveryWrite,
    /// fsync every N writes (balanced)
    EveryNWrites(u64),
}

impl Segment {
    pub fn new(dir: &Path, base_offset: u64) -> Result<Self> {
        Self::with_sync_policy(dir, base_offset, SegmentSyncPolicy::None)
    }

    /// Create a new segment with a configurable fsync policy (H-1 fix).
    pub fn with_sync_policy(
        dir: &Path,
        base_offset: u64,
        sync_policy: SegmentSyncPolicy,
    ) -> Result<Self> {
        let log_path = dir.join(format!("{:020}.{}", base_offset, LOG_SUFFIX));
        let index_path = dir.join(format!("{:020}.{}", base_offset, INDEX_SUFFIX));

        // Open or create log file with buffered writes (8KB buffer for batching)
        let mut log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)?;

        let current_size = log_file.seek(SeekFrom::End(0))?;
        let log_writer = BufWriter::with_capacity(8192, log_file);

        // Open or create index file
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Preserve existing data
            .open(&index_path)?;

        let mut segment = Self {
            base_offset,
            log_path,
            index_path,
            log_file: Arc::new(Mutex::new(log_writer)),
            current_size,
            index_buffer: Vec::new(),
            last_index_position: 0,
            pending_index_entries: std::sync::Mutex::new(Vec::new()),
            sync_policy,
            writes_since_sync: std::sync::atomic::AtomicU64::new(0),
            cached_mmap: tokio::sync::RwLock::new(None),
        };

        // Load index if exists
        if index_file.metadata()?.len() > 0 {
            segment.load_index(&index_file)?;
            // Set last_index_position from loaded index
            if let Some((_, pos)) = segment.index_buffer.last() {
                segment.last_index_position = *pos;
            }
        }

        Ok(segment)
    }

    fn load_index(&mut self, file: &File) -> Result<()> {
        let len = file.metadata()?.len();
        let count = len as usize / INDEX_ENTRY_SIZE;
        // SAFETY: The file is opened for reading and remains valid for the mmap lifetime.
        // The mmap is read-only and we only access within its bounds.
        let mmap = unsafe { Mmap::map(file)? };

        let mut cursor = 0;
        for _ in 0..count {
            if cursor + INDEX_ENTRY_SIZE > mmap.len() {
                break;
            }

            let rel_offset_bytes: [u8; 4] = mmap[cursor..cursor + 4].try_into().unwrap();
            let pos_bytes: [u8; 8] = mmap[cursor + 4..cursor + 12].try_into().unwrap();

            self.index_buffer.push((
                u32::from_be_bytes(rel_offset_bytes),
                u64::from_be_bytes(pos_bytes),
            ));

            cursor += INDEX_ENTRY_SIZE;
        }

        Ok(())
    }

    /// Append a message to the segment
    /// Optimized with buffered writes and sparse indexing
    pub async fn append(&mut self, offset: u64, mut message: Message) -> Result<u64> {
        if offset < self.base_offset {
            return Err(Error::Other(format!(
                "Offset {} is smaller than segment base offset {}",
                offset, self.base_offset
            )));
        }

        // 1. Serialize message
        message.offset = offset;
        let bytes = message.to_bytes()?;
        let len = bytes.len() as u32;

        // 2. Calculate CRC
        let mut hasher = Hasher::new();
        hasher.update(&bytes);
        let crc = hasher.finalize();

        // 3. Prepare frame: [CRC: 4][Len: 4][Payload: N]
        let mut frame = BytesMut::with_capacity(8 + bytes.len());
        frame.put_u32(crc);
        frame.put_u32(len);
        frame.put_slice(&bytes);

        let position = self.current_size;
        let frame_len = frame.len() as u64;

        // 4. Write to disk using buffered writer (fast path - no syscall per write)
        {
            let mut writer = self.log_file.lock().await;
            writer.write_all(&frame)?;

            // configurable fsync after write for durability
            self.maybe_sync(&mut writer)?;
        }

        self.current_size += frame_len;

        // Invalidate cached mmap since the file has changed
        *self.cached_mmap.write().await = None;

        // 5. Sparse indexing: only add index entry every INDEX_INTERVAL_BYTES
        if position == 0 || position - self.last_index_position >= INDEX_INTERVAL_BYTES {
            let relative_offset = (offset - self.base_offset) as u32;
            self.pending_index_entries
                .lock()
                .unwrap()
                .push((relative_offset, position));
            self.index_buffer.push((relative_offset, position));
            self.last_index_position = position;
        }

        Ok(position)
    }

    /// Append a batch of messages efficiently (single lock acquisition, batched index)
    pub async fn append_batch(&mut self, messages: Vec<(u64, Message)>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let mut positions = Vec::with_capacity(messages.len());
        let mut total_frame = BytesMut::with_capacity(messages.len() * 256); // Estimate

        for (offset, mut message) in messages {
            if offset < self.base_offset {
                return Err(Error::Other(format!(
                    "Offset {} is smaller than segment base offset {}",
                    offset, self.base_offset
                )));
            }

            // Serialize
            message.offset = offset;
            let bytes = message.to_bytes()?;
            let len = bytes.len() as u32;

            // CRC
            let mut hasher = Hasher::new();
            hasher.update(&bytes);
            let crc = hasher.finalize();

            let position = self.current_size + total_frame.len() as u64;
            positions.push(position);

            // Frame: [CRC: 4][Len: 4][Payload: N]
            total_frame.put_u32(crc);
            total_frame.put_u32(len);
            total_frame.put_slice(&bytes);

            // Sparse indexing
            if position == 0 || position - self.last_index_position >= INDEX_INTERVAL_BYTES {
                let relative_offset = (offset - self.base_offset) as u32;
                self.pending_index_entries
                    .lock()
                    .unwrap()
                    .push((relative_offset, position));
                self.index_buffer.push((relative_offset, position));
                self.last_index_position = position;
            }
        }

        // Single write for entire batch
        {
            let mut writer = self.log_file.lock().await;
            writer.write_all(&total_frame)?;

            // configurable fsync after batch write for durability
            self.maybe_sync(&mut writer)?;
        }

        self.current_size += total_frame.len() as u64;

        // Invalidate cached mmap since the file has changed
        *self.cached_mmap.write().await = None;

        Ok(positions)
    }

    /// Flush segment data to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        // Flush buffered writes
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        // Drain and write pending index entries (uses std::sync::Mutex for &self access)
        let entries: Vec<(u32, u64)> = {
            let mut guard = self.pending_index_entries.lock().unwrap();
            guard.drain(..).collect()
        };

        if !entries.is_empty() {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.index_path)?;

            let mut buf = BytesMut::with_capacity(entries.len() * INDEX_ENTRY_SIZE);
            for (rel_offset, pos) in &entries {
                buf.put_u32(*rel_offset);
                buf.put_u64(*pos);
            }
            file.write_all(&buf)?;
            file.sync_all()?;
        }

        Ok(())
    }

    /// Apply fsync policy after a write (H-1 fix).
    ///
    /// Handles the three sync modes:
    /// - `None`: no-op (fastest, least durable)
    /// - `EveryWrite`: flush + fdatasync on every write (maximum durability)
    /// - `EveryNWrites(n)`: flush + fdatasync every N writes (balanced)
    fn maybe_sync(&self, writer: &mut BufWriter<File>) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;

        match self.sync_policy {
            SegmentSyncPolicy::None => {}
            SegmentSyncPolicy::EveryWrite => {
                writer.flush()?;
                writer.get_ref().sync_data()?;
            }
            SegmentSyncPolicy::EveryNWrites(n) => {
                let count = self.writes_since_sync.fetch_add(1, Relaxed) + 1;
                if count >= n {
                    writer.flush()?;
                    writer.get_ref().sync_data()?;
                    self.writes_since_sync.store(0, Relaxed);
                }
            }
        }
        Ok(())
    }

    /// Flush pending index entries and clear the buffer
    pub async fn flush_index(&mut self) -> Result<()> {
        let entries: Vec<(u32, u64)> = {
            let mut guard = self.pending_index_entries.lock().unwrap();
            guard.drain(..).collect()
        };

        if entries.is_empty() {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.index_path)?;

        let mut buf = BytesMut::with_capacity(entries.len() * INDEX_ENTRY_SIZE);
        for (rel_offset, pos) in &entries {
            buf.put_u32(*rel_offset);
            buf.put_u64(*pos);
        }
        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }

    /// Read a batch of messages starting from a given offset.
    ///
    /// Uses a cached mmap (H-12) and performs the blocking mmap syscall via
    /// `spawn_blocking` when a new map is needed (H-11) so the tokio runtime
    /// thread is never blocked on filesystem I/O.
    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Message>> {
        if offset < self.base_offset {
            return Ok(Vec::new());
        }

        // Flush buffered writes before reading to ensure data visibility
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        // 1. Find position from index
        let relative_offset = (offset - self.base_offset).try_into().unwrap_or(u32::MAX);
        let mut start_pos = 0;

        // Binary search for the closest index entry <= relative_offset
        if let Some(idx) = self
            .index_buffer
            .partition_point(|&(off, _)| off <= relative_offset)
            .checked_sub(1)
        {
            start_pos = self.index_buffer[idx].1;
        }

        // 2. Get or create cached mmap (H-11/H-12 fix)
        let mmap = {
            // Fast path: check if we have a cached mmap
            let cached = self.cached_mmap.read().await;
            if let Some(ref m) = *cached {
                Arc::clone(m)
            } else {
                drop(cached);

                // Slow path: create new mmap via spawn_blocking to avoid
                // blocking the tokio runtime thread
                let log_path = self.log_path.clone();
                let new_mmap = tokio::task::spawn_blocking(move || -> Result<Arc<Mmap>> {
                    let file = File::open(&log_path)?;
                    let file_len = file.metadata()?.len();
                    if file_len == 0 {
                        return Err(Error::Other("Empty segment file".to_string()));
                    }
                    // SAFETY: File is opened read-only and remains valid for mmap lifetime.
                    let mmap = unsafe { Mmap::map(&file)? };
                    Ok(Arc::new(mmap))
                })
                .await
                .map_err(|e| Error::Other(format!("spawn_blocking failed: {}", e)))??;

                // Cache it for future reads
                let mut cached = self.cached_mmap.write().await;
                *cached = Some(Arc::clone(&new_mmap));
                new_mmap
            }
        };

        if start_pos >= mmap.len() as u64 {
            return Ok(Vec::new());
        }

        let mut current_pos = start_pos as usize;
        let mut messages = Vec::new();
        let mut bytes_read = 0;

        while current_pos < mmap.len() && bytes_read < max_bytes {
            // Check headers
            if current_pos + 8 > mmap.len() {
                break;
            }

            let slice = &mmap[current_pos..];

            // Read CRC and Len
            let crc_bytes: [u8; 4] = slice[0..4].try_into().unwrap();
            let len_bytes: [u8; 4] = slice[4..8].try_into().unwrap();
            let stored_crc = u32::from_be_bytes(crc_bytes);
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + 8 + msg_len > mmap.len() {
                break; // Incomplete message
            }

            // Verify CRC
            let payload = &slice[8..8 + msg_len];
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let computed_crc = hasher.finalize();

            if computed_crc != stored_crc {
                return Err(Error::Other(format!(
                    "CRC mismatch at position {}",
                    current_pos
                )));
            }

            // Deserialize
            let msg = Message::from_bytes(payload)?;
            if msg.offset >= offset {
                messages.push(msg);
                bytes_read += 8 + msg_len;
            }

            current_pos += 8 + msg_len;
        }

        Ok(messages)
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Delete the segment's log and index files from disk.
    ///
    /// After calling this method, the segment must not be used again.
    pub fn delete_files(&self) -> Result<()> {
        if self.log_path.exists() {
            std::fs::remove_file(&self.log_path)?;
        }
        if self.index_path.exists() {
            std::fs::remove_file(&self.index_path)?;
        }
        Ok(())
    }

    pub async fn recover_last_offset(&self) -> Result<Option<u64>> {
        // Flush buffered writes before recovery scan
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let mut start_pos = 0;
        if let Some((_, pos)) = self.index_buffer.last() {
            start_pos = *pos;
        }

        let file = File::open(&self.log_path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(None);
        }

        // SAFETY: File is opened read-only, checked non-empty, and remains valid.
        // We check bounds before all slice accesses.
        let mmap = unsafe { Mmap::map(&file)? };

        if start_pos >= mmap.len() as u64 {
            return Ok(None);
        }

        let mut current_pos = start_pos as usize;
        let mut last_offset = None;

        while current_pos < mmap.len() {
            if current_pos + 8 > mmap.len() {
                break;
            }

            let slice = &mmap[current_pos..];
            let stored_crc_bytes: [u8; 4] = slice[0..4].try_into().unwrap();
            let stored_crc = u32::from_be_bytes(stored_crc_bytes);
            let len_bytes: [u8; 4] = slice[4..8].try_into().unwrap();
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + 8 + msg_len > mmap.len() {
                break;
            }

            let payload = &slice[8..8 + msg_len];

            // Validate CRC before accepting this frame
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let computed_crc = hasher.finalize();
            if computed_crc != stored_crc {
                // Corrupt frame — stop recovery here
                break;
            }

            if let Ok(msg) = Message::from_bytes(payload) {
                last_offset = Some(msg.offset);
            }

            current_pos += 8 + msg_len;
        }

        // Truncate the segment at the first invalid/incomplete frame.
        // This ensures the segment is clean for subsequent appends after crash recovery.
        // Standard Kafka recovery behavior: truncate at first corruption point.
        let valid_len = current_pos as u64;
        if valid_len < len {
            // Must drop the mmap before truncating — can't modify file while mapped
            drop(mmap);
            drop(file);

            let truncate_file = OpenOptions::new()
                .write(true)
                .open(&self.log_path)?;
            truncate_file.set_len(valid_len)?;
            truncate_file.sync_all()?;

            tracing::warn!(
                "Segment {:020}: truncated from {} to {} bytes during recovery (removed {} bytes of corrupt/incomplete data)",
                self.base_offset,
                len,
                valid_len,
                len - valid_len
            );
        }

        Ok(last_offset)
    }

    /// Find the first offset with timestamp >= target_timestamp
    /// Uses linear scan through the segment (timestamps may not be monotonic due to clock skew)
    /// Returns None if no matching offset is found
    pub async fn find_offset_for_timestamp(&self, target_timestamp: i64) -> Result<Option<u64>> {
        // Flush buffered writes before timestamp scan
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let file = File::open(&self.log_path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(None);
        }

        // SAFETY: File is opened read-only, checked non-empty, and remains valid.
        // We check bounds before all slice accesses.
        let mmap = unsafe { Mmap::map(&file)? };
        let mut current_pos = 0usize;

        while current_pos < mmap.len() {
            if current_pos + 8 > mmap.len() {
                break;
            }

            let slice = &mmap[current_pos..];
            let len_bytes: [u8; 4] = slice[4..8].try_into().unwrap();
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + 8 + msg_len > mmap.len() {
                break;
            }

            let payload = &slice[8..8 + msg_len];
            if let Ok(msg) = Message::from_bytes(payload) {
                let msg_timestamp = msg.timestamp.timestamp_millis();
                if msg_timestamp >= target_timestamp {
                    return Ok(Some(msg.offset));
                }
            }

            current_pos += 8 + msg_len;
        }

        Ok(None)
    }

    /// Get the timestamp range of messages in this segment
    /// Returns (min_timestamp, max_timestamp) in milliseconds since epoch
    /// Useful for quickly determining if a segment might contain a target timestamp
    pub async fn timestamp_bounds(&self) -> Result<Option<(i64, i64)>> {
        // Flush buffered writes before scanning
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let file = File::open(&self.log_path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            return Ok(None);
        }

        // SAFETY: File is opened read-only, checked non-empty, and remains valid.
        // We check bounds before all slice accesses.
        let mmap = unsafe { Mmap::map(&file)? };
        let mut current_pos = 0usize;
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        while current_pos < mmap.len() {
            if current_pos + 8 > mmap.len() {
                break;
            }

            let slice = &mmap[current_pos..];
            let len_bytes: [u8; 4] = slice[4..8].try_into().unwrap();
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + 8 + msg_len > mmap.len() {
                break;
            }

            let payload = &slice[8..8 + msg_len];
            if let Ok(msg) = Message::from_bytes(payload) {
                let ts = msg.timestamp.timestamp_millis();
                min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
                max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
            }

            current_pos += 8 + msg_len;
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Ok(Some((min, max))),
            _ => Ok(None),
        }
    }
}
