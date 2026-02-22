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
use arc_swap::ArcSwapOption;
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::Mutex;

// Use parking_lot::Mutex for non-async synchronization.
// Unlike std::sync::Mutex, parking_lot::Mutex is non-poisoning (a panic
// in one thread does not permanently break the mutex) and has better
// contention behavior (adaptive spinning before syscall).
use parking_lot::Mutex as SyncMutex;

const INDEX_ENTRY_SIZE: usize = 12; // 4 bytes relative offset, 8 bytes position
const LOG_SUFFIX: &str = "log";
const INDEX_SUFFIX: &str = "index";
/// Index every 4KB of data (sparse indexing for performance)
const INDEX_INTERVAL_BYTES: u64 = 4096;

/// Frame header size: 4 bytes CRC + 4 bytes length.
const FRAME_HEADER_SIZE: usize = 8;

/// Read a fixed-size byte array from a slice at a given offset, returning a
/// corruption error instead of panicking if the slice is too small.
#[inline]
fn read_bytes<const N: usize>(data: &[u8], offset: usize) -> Result<[u8; N]> {
    data.get(offset..offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| {
            Error::Other(format!(
                "truncated frame: need {} bytes at offset {}",
                N, offset
            ))
        })
}

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
    /// Pending index entries to batch write (parking_lot::Mutex — non-poisoning).
    pending_index_entries: SyncMutex<Vec<(u32, u64)>>,
    /// Fsync policy for segment writes (H-1 fix).
    /// Controls durability guarantees: None for OS page cache only,
    /// EveryWrite for per-append fsync, EveryNWrites for batched fsync.
    sync_policy: SegmentSyncPolicy,
    /// Counter for tracking writes between fsyncs (for EveryNWrites policy)
    writes_since_sync: std::sync::atomic::AtomicU64,
    /// Cached read-only memory map (H-11/H-12 fix).
    /// Uses lock-free `ArcSwapOption` instead of `tokio::sync::RwLock` to
    /// eliminate contention between concurrent producers and consumers.
    /// Lazily re-created on read when stale (write_generation > mmap_generation).
    cached_mmap: ArcSwapOption<Mmap>,
    /// Monotonically increasing counter bumped on every append.
    /// Compared against `mmap_generation` to detect staleness without
    /// invalidating the mmap on each write.
    write_generation: AtomicU64,
    /// The `write_generation` value at which the current cached mmap was created.
    /// When `mmap_generation < write_generation`, the cached mmap is stale and
    /// will be refreshed on the next read (after flushing dirty buffers).
    mmap_generation: AtomicU64,
    /// Cached (min_ts, max_ts) for sealed segments.
    /// Once a segment is sealed, timestamps never change, so we cache
    /// the result of `timestamp_bounds()` to avoid repeated O(n) scans.
    cached_timestamp_bounds: SyncMutex<Option<Option<(i64, i64)>>>,
    /// Dirty flag: set after append, cleared after flush.
    /// Allows the read path to skip acquiring the write mutex when
    /// no data is buffered, eliminating head-of-line blocking.
    write_dirty: AtomicBool,
    /// Duplicate file descriptor for deferred fsync.
    /// Allows `sync_data()` to run AFTER releasing the LogManager write lock,
    /// unblocking readers during the slow fdatasync syscall.
    sync_file: Arc<File>,
    /// Flag indicating a deferred fsync is needed.
    /// Set by `maybe_flush_and_flag_sync()`, consumed by `take_pending_sync()`.
    pending_sync: AtomicBool,
    /// Reusable frame buffer to avoid per-append Vec allocation (L-3 fix).
    frame_buf: Vec<u8>,
}

/// Fsync policy for segment writes (H-1 fix).
///
/// Controls when segment data is flushed to durable storage via fsync/fdatasync.
/// Mirrors Kafka's `log.flush.interval.messages` concept.
///
/// - `None`: No fsync — data lives in OS page cache until kernel writeback (fastest, least durable)
/// - `EveryWrite`: fsync after every append — maximum durability, equivalent to `acks=all` + sync
/// - `EveryNWrites(n)`: fsync every N writes — balances throughput and durability
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SegmentSyncPolicy {
    /// No fsync — relies on OS page cache writeback (fastest)
    None,
    /// fsync after every write (maximum durability)
    EveryWrite,
    /// fsync every N writes (balanced)
    EveryNWrites(u64),
}

impl Segment {
    /// Create a new segment with the production-safe default sync policy.
    ///
    /// defaults to `EveryNWrites(1)` (fsync every write) instead
    /// of `None`, matching `LogManager`'s default. This prevents data loss
    /// for callers using `Segment::new()` directly.
    pub fn new(dir: &Path, base_offset: u64) -> Result<Self> {
        Self::with_sync_policy(dir, base_offset, SegmentSyncPolicy::EveryNWrites(1))
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
        // dup the fd BEFORE wrapping in BufWriter — used for
        // deferred fsync outside the LogManager write lock.
        let sync_file = Arc::new(log_file.try_clone()?);
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
            pending_index_entries: SyncMutex::new(Vec::new()),
            sync_policy,
            writes_since_sync: std::sync::atomic::AtomicU64::new(0),
            cached_mmap: ArcSwapOption::empty(),
            write_generation: AtomicU64::new(0),
            mmap_generation: AtomicU64::new(0),
            cached_timestamp_bounds: SyncMutex::new(None),
            write_dirty: AtomicBool::new(false),
            sync_file,
            pending_sync: AtomicBool::new(false),
            frame_buf: Vec::with_capacity(8 + 256),
        };

        // Load index if exists
        if index_file.metadata()?.len() > 0 {
            segment.load_index(&index_file)?;
            // Set last_index_position from loaded index
            if let Some((_, pos)) = segment.index_buffer.last() {
                segment.last_index_position = *pos;
            }
        }

        // fsync the parent directory after creating new segment files.
        // This ensures the directory entries (new .log / .index files) are
        // durable — without this, a crash could leave the directory in a
        // state where the files don't appear despite having been written.
        if current_size == 0 {
            File::open(dir)?.sync_all()?;
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

            let rel_offset_bytes: [u8; 4] = read_bytes(&mmap, cursor)?;
            let pos_bytes: [u8; 8] = read_bytes(&mmap, cursor + 4)?;

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

        // Single allocation, zero copies.
        // Serialize directly into the reusable frame buffer after an 8-byte header
        // placeholder (CRC + len), then patch the header in place.
        message.offset = offset;
        self.frame_buf.clear();
        self.frame_buf.extend_from_slice(&[0u8; 8]); // placeholder for [CRC: 4][Len: 4]
        self.frame_buf = postcard::to_extend(&message, std::mem::take(&mut self.frame_buf))?;

        // Patch CRC + length into the reserved header
        let payload = &self.frame_buf[8..];
        let len = u32::try_from(payload.len()).map_err(|_| {
            Error::Other(format!(
                "Message payload too large: {} bytes exceeds u32::MAX",
                payload.len()
            ))
        })?;
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let crc = hasher.finalize();
        self.frame_buf[0..4].copy_from_slice(&crc.to_be_bytes());
        self.frame_buf[4..8].copy_from_slice(&len.to_be_bytes());

        let position = self.current_size;
        let frame_len = self.frame_buf.len() as u64;

        // 4. Write to disk using buffered writer (fast path - no syscall per write)
        {
            let mut writer = self.log_file.lock().await;
            writer.write_all(&self.frame_buf)?;

            // configurable fsync after write for durability
            self.maybe_flush_and_flag_sync(&mut writer)?;
        }

        // Mark buffer dirty so the read path knows to flush before reading
        self.write_dirty.store(true, AtomicOrdering::Release);
        self.current_size += frame_len;

        // Bump write generation so readers know the mmap is stale.
        // Lock-free: no mmap invalidation on the write path.
        self.write_generation.fetch_add(1, AtomicOrdering::Release);
        // Invalidate cached timestamp bounds since new data was written
        self.invalidate_timestamp_cache();

        // 5. Sparse indexing: only add index entry every INDEX_INTERVAL_BYTES
        if position == 0 || position - self.last_index_position >= INDEX_INTERVAL_BYTES {
            let relative_offset = u32::try_from(offset - self.base_offset).map_err(|_| {
                Error::Other(format!(
                    "segment relative offset overflow: offset {} exceeds u32::MAX beyond base {}",
                    offset, self.base_offset
                ))
            })?;
            self.pending_index_entries
                .lock()
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

        // Reuse a single serialization buffer across all messages.
        // `postcard::to_extend` serializes into the Vec, then `clear()` resets
        // length to 0 without deallocating. This reduces N allocations to 1.
        let mut msg_buf = Vec::with_capacity(1024);

        for (offset, mut message) in messages {
            if offset < self.base_offset {
                return Err(Error::Other(format!(
                    "Offset {} is smaller than segment base offset {}",
                    offset, self.base_offset
                )));
            }

            // Serialize into reusable buffer (grows as needed, never deallocates)
            message.offset = offset;
            msg_buf.clear();
            msg_buf = postcard::to_extend(&message, msg_buf)?;
            // Validate size fits in u32 frame header, matching append().
            // The prior `as u32` silently truncated payloads > 4GB, corrupting
            // all subsequent frames in the batch.
            let len = u32::try_from(msg_buf.len()).map_err(|_| {
                Error::Other(format!(
                    "Message payload too large: {} bytes exceeds u32::MAX",
                    msg_buf.len()
                ))
            })?;

            // CRC
            let mut hasher = Hasher::new();
            hasher.update(&msg_buf);
            let crc = hasher.finalize();

            let position = self.current_size + total_frame.len() as u64;
            positions.push(position);

            // Frame: [CRC: 4][Len: 4][Payload: N]
            total_frame.put_u32(crc);
            total_frame.put_u32(len);
            total_frame.put_slice(&msg_buf);

            // Sparse indexing
            if position == 0 || position - self.last_index_position >= INDEX_INTERVAL_BYTES {
                let relative_offset = u32::try_from(offset - self.base_offset).map_err(|_| {
                    Error::Other(format!(
                        "segment relative offset overflow: offset {} exceeds u32::MAX beyond base {}",
                        offset, self.base_offset
                    ))
                })?;
                self.pending_index_entries
                    .lock()
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
            self.maybe_flush_and_flag_sync(&mut writer)?;
        }

        // Mark buffer dirty so the read path knows to flush before reading
        self.write_dirty.store(true, AtomicOrdering::Release);
        self.current_size += total_frame.len() as u64;

        // Bump write generation so readers know the mmap is stale.
        // Lock-free: no mmap invalidation on the write path.
        self.write_generation.fetch_add(1, AtomicOrdering::Release);
        // Invalidate cached timestamp bounds since new data was written
        self.invalidate_timestamp_cache();

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
        self.write_dirty.store(false, AtomicOrdering::Release);

        // Invalidate cached mmap on flush so the next read creates a fresh
        // mmap that reflects all flushed data.
        self.cached_mmap.store(None);

        // Drain and write pending index entries (uses std::sync::Mutex for &self access)
        let entries: Vec<(u32, u64)> = {
            let mut guard = self.pending_index_entries.lock();
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
    /// This method now only flushes the BufWriter to the OS page cache
    /// and syncs index entries. The expensive `sync_data()` (fdatasync) is deferred
    /// — the caller retrieves the dup'd fd via `take_pending_sync()` and runs
    /// the fsync AFTER releasing the LogManager write lock, unblocking readers.
    ///
    /// Handles the three sync modes:
    /// - `None`: no-op (fastest, least durable)
    /// - `EveryWrite`: flush + flag sync on every write (maximum durability)
    /// - `EveryNWrites(n)`: flush + flag sync every N writes (balanced)
    fn maybe_flush_and_flag_sync(&self, writer: &mut BufWriter<File>) -> Result<()> {
        match self.sync_policy {
            SegmentSyncPolicy::None => {}
            SegmentSyncPolicy::EveryWrite => {
                writer.flush()?;
                // also flush and fsync pending index entries so
                // the index stays consistent with the log after a crash.
                self.sync_pending_index_entries()?;
                // flag for deferred sync instead of blocking here
                self.pending_sync.store(true, AtomicOrdering::Release);
            }
            SegmentSyncPolicy::EveryNWrites(n) => {
                // Use AcqRel ordering on the counter to ensure correct visibility
                // on weakly-ordered architectures (ARM/AArch64). The cost is
                // negligible — this path already performs I/O (flush + fsync).
                let count = self.writes_since_sync.fetch_add(1, AtomicOrdering::AcqRel) + 1;
                if count >= n {
                    writer.flush()?;
                    // also flush and fsync pending index entries
                    self.sync_pending_index_entries()?;
                    // flag for deferred sync instead of blocking here
                    self.pending_sync.store(true, AtomicOrdering::Release);
                    // Atomically reset: CAS from current value to 0.
                    // If another thread incremented in the meantime, retry.
                    let mut current = count;
                    while self
                        .writes_since_sync
                        .compare_exchange_weak(
                            current,
                            0,
                            AtomicOrdering::AcqRel,
                            AtomicOrdering::Acquire,
                        )
                        .is_err()
                    {
                        current = self.writes_since_sync.load(AtomicOrdering::Acquire);
                    }
                }
            }
        }
        Ok(())
    }

    /// Take the pending sync handle if a deferred fsync is needed.
    ///
    /// Returns a clone of the dup'd file descriptor if `sync_data()` should be
    /// called. The caller should run this AFTER releasing the LogManager write
    /// lock (typically via `spawn_blocking`) to avoid blocking readers.
    ///
    /// Multiple calls between syncs are safe — `fdatasync()` is idempotent and
    /// syncs all data up to the current file position.
    pub fn take_pending_sync(&self) -> Option<Arc<File>> {
        if self.pending_sync.swap(false, AtomicOrdering::AcqRel) {
            Some(Arc::clone(&self.sync_file))
        } else {
            None
        }
    }

    /// Synchronously flush and fsync pending index entries.
    ///
    /// Called from `maybe_flush_and_flag_sync` (which is non-async) to ensure the index
    /// file is durable whenever the log file is fsynced. Without this,
    /// a crash after a log fsync but before the next `flush()` would leave
    /// the index stale — reads after recovery would miss recently written
    /// offsets until the index is rebuilt.
    fn sync_pending_index_entries(&self) -> Result<()> {
        let entries: Vec<(u32, u64)> = {
            let mut guard = self.pending_index_entries.lock();
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

    /// Flush pending index entries and clear the buffer
    pub async fn flush_index(&mut self) -> Result<()> {
        let entries: Vec<(u32, u64)> = {
            let mut guard = self.pending_index_entries.lock();
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

        // Flush buffered writes before reading to ensure data visibility.
        // Fast path: skip the lock entirely when nothing was written since
        // the last flush (avoids head-of-line blocking behind concurrent appends).
        //
        // Uses swap(false, AcqRel) inside the lock to atomically clear the flag
        // and flush only if dirty — eliminates the TOCTOU race where a concurrent
        // append could set the flag between our check and lock acquisition.
        if self.write_dirty.load(AtomicOrdering::Acquire) {
            let mut writer = self.log_file.lock().await;
            // Re-check atomically: if still dirty, flush and clear in one step.
            // A concurrent append may have already flushed, making this a no-op.
            if self.write_dirty.swap(false, AtomicOrdering::AcqRel) {
                writer.flush()?;
            }
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
        //    Uses lock-free ArcSwapOption with generation tracking:
        //    the mmap is refreshed only when write_generation has advanced
        //    past the generation recorded when the mmap was created.
        let current_gen = self.write_generation.load(AtomicOrdering::Acquire);
        let mmap = {
            // Fast path: load cached mmap (lock-free)
            let cached = self.cached_mmap.load();
            let is_stale = self.mmap_generation.load(AtomicOrdering::Acquire) < current_gen;
            if let Some(m) = cached.as_ref().filter(|_| !is_stale) {
                Arc::clone(m)
            } else {
                // Slow path: create new mmap via spawn_blocking to avoid
                // blocking the tokio runtime thread
                let log_path = self.log_path.clone();
                let new_mmap = tokio::task::spawn_blocking(move || -> Result<Option<Arc<Mmap>>> {
                    let file = File::open(&log_path)?;
                    let file_len = file.metadata()?.len();
                    if file_len == 0 {
                        // Segment exists but has no data yet (e.g. freshly created
                        // topic with no published messages). Return None so the
                        // caller can short-circuit to an empty result instead of
                        // attempting to mmap a zero-length file.
                        return Ok(None);
                    }
                    // SAFETY: File is opened read-only and remains valid for mmap lifetime.
                    let mmap = unsafe { Mmap::map(&file)? };
                    Ok(Some(Arc::new(mmap)))
                })
                .await
                .map_err(|e| Error::Other(format!("spawn_blocking failed: {}", e)))??;

                match new_mmap {
                    Some(m) => {
                        // Cache it for future reads (lock-free store)
                        self.cached_mmap.store(Some(Arc::clone(&m)));
                        self.mmap_generation
                            .store(current_gen, AtomicOrdering::Release);
                        m
                    }
                    None => {
                        // Empty segment file — no data to read
                        return Ok(Vec::new());
                    }
                }
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
            if current_pos + FRAME_HEADER_SIZE > mmap.len() {
                break;
            }

            let slice = &mmap[current_pos..];

            // Read CRC and Len
            let crc_bytes: [u8; 4] = read_bytes(slice, 0)?;
            let len_bytes: [u8; 4] = read_bytes(slice, 4)?;
            let stored_crc = u32::from_be_bytes(crc_bytes);
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + FRAME_HEADER_SIZE + msg_len > mmap.len() {
                break; // Incomplete message
            }

            // Verify CRC
            let payload = &slice[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + msg_len];
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let computed_crc = hasher.finalize();

            if computed_crc != stored_crc {
                return Err(Error::Other(format!(
                    "CRC mismatch at position {}",
                    current_pos
                )));
            }

            // Extract offset varint (first field in postcard encoding)
            // without deserializing the full message. This avoids allocating/copying
            // key, value, and headers for messages below the target offset — a
            // significant win when seeking into large segments.
            let (msg_offset, _rest) = postcard::take_from_bytes::<u64>(payload).map_err(|e| {
                Error::Other(format!("offset decode at pos {}: {}", current_pos, e))
            })?;

            if msg_offset >= offset {
                let msg = Message::from_bytes(payload)?;
                messages.push(msg);
                bytes_read += FRAME_HEADER_SIZE + msg_len;
            }

            current_pos += FRAME_HEADER_SIZE + msg_len;
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

    pub async fn recover_last_offset(&mut self) -> Result<Option<u64>> {
        // Flush buffered writes before recovery scan
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let mut start_pos = 0;
        if let Some((_, pos)) = self.index_buffer.last() {
            start_pos = *pos;
        }

        let log_path = self.log_path.clone();
        let base_offset = self.base_offset;

        // run mmap creation and CRC scan on spawn_blocking to avoid
        // blocking the Tokio runtime during recovery of many segments.
        // Returns (last_offset, valid_file_length) so we can update current_size.
        let (last_offset, valid_len) = tokio::task::spawn_blocking(move || -> crate::Result<_> {
            let file = File::open(&log_path)?;
            let len = file.metadata()?.len();
            if len == 0 {
                return Ok((None, len));
            }

            // SAFETY: File is opened read-only, checked non-empty, and remains valid.
            // We check bounds before all slice accesses.
            let mmap = unsafe { Mmap::map(&file)? };

            if start_pos >= mmap.len() as u64 {
                return Ok((None, len));
            }

            let mut current_pos = start_pos as usize;
            let mut last_offset = None;

            while current_pos < mmap.len() {
                if current_pos + FRAME_HEADER_SIZE > mmap.len() {
                    break;
                }

                let slice = &mmap[current_pos..];
                let stored_crc_bytes: [u8; 4] = read_bytes(slice, 0)?;
                let stored_crc = u32::from_be_bytes(stored_crc_bytes);
                let len_bytes: [u8; 4] = read_bytes(slice, 4)?;
                let msg_len = u32::from_be_bytes(len_bytes) as usize;

                if current_pos + FRAME_HEADER_SIZE + msg_len > mmap.len() {
                    break;
                }

                let payload = &slice[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + msg_len];

                // Validate CRC before accepting this frame
                let mut hasher = Hasher::new();
                hasher.update(payload);
                let computed_crc = hasher.finalize();
                if computed_crc != stored_crc {
                    // Corrupt frame — stop recovery here
                    break;
                }

                // Extract just the offset varint from the payload
                // without deserializing the full message. Recovery only needs
                // the offset, not key/value/headers.
                match postcard::take_from_bytes::<u64>(payload) {
                    Ok((msg_offset, _)) => last_offset = Some(msg_offset),
                    Err(_) => break, // Corrupt payload — stop recovery
                }

                current_pos += FRAME_HEADER_SIZE + msg_len;
            }

            // Truncate the segment at the first invalid/incomplete frame.
            // This ensures the segment is clean for subsequent appends after crash recovery.
            // Standard Kafka recovery behavior: truncate at first corruption point.
            let valid_len = current_pos as u64;
            if valid_len < len {
                // Must drop the mmap before truncating — can't modify file while mapped
                drop(mmap);
                drop(file);

                let truncate_file = OpenOptions::new().write(true).open(&log_path)?;
                truncate_file.set_len(valid_len)?;
                truncate_file.sync_all()?;

                tracing::warn!(
                    "Segment {:020}: truncated from {} to {} bytes during recovery (removed {} bytes of corrupt/incomplete data)",
                    base_offset,
                    len,
                    valid_len,
                    len - valid_len
                );
            }

            Ok((last_offset, valid_len))
        })
        .await
        .map_err(|e| Error::Other(format!("recover_last_offset task panicked: {}", e)))??;

        // Sync in-memory current_size with the (possibly truncated)
        // file length. Without this, the segment's size() would still reflect
        // the pre-truncation value, causing premature segment rolling.
        self.current_size = valid_len;

        // Invalidate/rebuild sparse index after truncation.
        // After crash recovery truncates the segment, the index_buffer may
        // contain entries pointing beyond the new file end. Trim stale entries
        // and rewrite the .index file to match the valid data.
        self.rebuild_index_after_truncation(valid_len);

        // Force-invalidate cached mmap since the file was truncated.
        // Also bump write_generation so any concurrent reader treats
        // a previously loaded mmap as stale.
        self.cached_mmap.store(None);
        self.write_generation.fetch_add(1, AtomicOrdering::Release);
        // Invalidate cached timestamp bounds
        self.invalidate_timestamp_cache();

        Ok(last_offset)
    }

    /// Trim the sparse index to remove entries at or beyond `valid_len`
    /// and rewrite the on-disk `.index` file to match.
    ///
    /// Called after crash-recovery truncation in `recover_last_offset()` and
    /// whenever the underlying segment data changes in a way that invalidates
    /// previously recorded byte positions.
    fn rebuild_index_after_truncation(&mut self, valid_len: u64) {
        let before = self.index_buffer.len();
        self.index_buffer.retain(|&(_, pos)| pos < valid_len);
        let removed = before - self.index_buffer.len();

        // Update last_index_position to the last valid entry
        self.last_index_position = self.index_buffer.last().map(|&(_, pos)| pos).unwrap_or(0);

        // Clear pending index entries — they may also reference stale positions
        self.pending_index_entries.lock().clear();

        if removed > 0 {
            tracing::info!(
                base_offset = self.base_offset,
                removed,
                remaining = self.index_buffer.len(),
                "Trimmed stale sparse index entries after segment truncation"
            );

            // Rewrite the .index file from the trimmed buffer
            if let Err(e) = self.rewrite_index_file() {
                tracing::warn!(
                    base_offset = self.base_offset,
                    error = %e,
                    "Failed to rewrite index file after truncation"
                );
            }
        }
    }

    /// Rewrite the `.index` file from the in-memory `index_buffer`.
    fn rewrite_index_file(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.index_path)?;

        let mut buf = BytesMut::with_capacity(self.index_buffer.len() * INDEX_ENTRY_SIZE);
        for &(rel_offset, pos) in &self.index_buffer {
            buf.put_u32(rel_offset);
            buf.put_u64(pos);
        }
        file.write_all(&buf)?;
        file.sync_all()?;

        Ok(())
    }

    /// Find the first offset with timestamp >= target_timestamp
    /// Uses linear scan through the segment (timestamps may not be monotonic due to clock skew)
    /// Returns None if no matching offset is found
    ///
    /// The mmap creation and scan are offloaded to `spawn_blocking`
    /// to avoid blocking the tokio runtime on slow/contended file systems.
    pub async fn find_offset_for_timestamp(&self, target_timestamp: i64) -> Result<Option<u64>> {
        // Flush buffered writes before timestamp scan
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let log_path = self.log_path.clone();
        let base_offset = self.base_offset;

        tokio::task::spawn_blocking(move || -> Result<Option<u64>> {
            let file = File::open(&log_path)?;
            let len = file.metadata()?.len();
            if len == 0 {
                return Ok(None);
            }

            // SAFETY: File is opened read-only, checked non-empty, and remains valid.
            // We check bounds before all slice accesses.
            let mmap = unsafe { Mmap::map(&file)? };
            let mut current_pos = 0usize;

            while current_pos < mmap.len() {
                if current_pos + FRAME_HEADER_SIZE > mmap.len() {
                    break;
                }

                let slice = &mmap[current_pos..];

                // Read and validate CRC before processing the record
                let crc_bytes: [u8; 4] = read_bytes(slice, 0)?;
                let stored_crc = u32::from_be_bytes(crc_bytes);
                let len_bytes: [u8; 4] = read_bytes(slice, 4)?;
                let msg_len = u32::from_be_bytes(len_bytes) as usize;

                if current_pos + FRAME_HEADER_SIZE + msg_len > mmap.len() {
                    break;
                }

                let payload = &slice[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + msg_len];

                let mut hasher = Hasher::new();
                hasher.update(payload);
                let computed_crc = hasher.finalize();
                if computed_crc != stored_crc {
                    return Err(Error::Other(format!(
                        "CRC mismatch in find_offset_for_timestamp at position {} (base_offset {})",
                        current_pos, base_offset
                    )));
                }

                if let Ok(msg) = Message::from_bytes(payload) {
                    let msg_timestamp = msg.timestamp.timestamp_millis();
                    if msg_timestamp >= target_timestamp {
                        return Ok(Some(msg.offset));
                    }
                }

                current_pos += FRAME_HEADER_SIZE + msg_len;
            }

            Ok(None)
        })
        .await
        .map_err(|e| Error::Other(format!("find_offset_for_timestamp join: {}", e)))?
    }

    /// Get the timestamp range of messages in this segment
    /// Returns (min_timestamp, max_timestamp) in milliseconds since epoch
    /// Useful for quickly determining if a segment might contain a target timestamp.
    ///
    /// Results are cached in `cached_timestamp_bounds`. For sealed segments
    /// (where timestamps never change), subsequent calls return the cached value in O(1)
    /// instead of performing a full O(n) scan. Call `invalidate_timestamp_cache()` when
    /// the segment is mutated (e.g., during append).
    ///
    /// The mmap creation and scan are offloaded to `spawn_blocking`
    /// to avoid blocking the tokio runtime.
    pub async fn timestamp_bounds(&self) -> Result<Option<(i64, i64)>> {
        // Fast path: return cached result if available
        {
            let cached = self.cached_timestamp_bounds.lock();
            if let Some(bounds) = *cached {
                return Ok(bounds);
            }
        }

        // Flush buffered writes before scanning
        {
            let mut writer = self.log_file.lock().await;
            writer.flush()?;
        }

        let log_path = self.log_path.clone();
        let base_offset = self.base_offset;

        let result = tokio::task::spawn_blocking(move || -> Result<Option<(i64, i64)>> {
            let file = File::open(&log_path)?;
            let len = file.metadata()?.len();
            if len == 0 {
                return Ok(None);
            }

            // SAFETY: File is opened read-only, checked non-empty, and remains valid.
            let mmap = unsafe { Mmap::map(&file)? };
            let mut current_pos = 0usize;
            let mut min_ts: Option<i64> = None;
            let mut max_ts: Option<i64> = None;

            while current_pos < mmap.len() {
                if current_pos + FRAME_HEADER_SIZE > mmap.len() {
                    break;
                }

                let slice = &mmap[current_pos..];

                let crc_bytes: [u8; 4] = read_bytes(slice, 0)?;
                let stored_crc = u32::from_be_bytes(crc_bytes);
                let len_bytes: [u8; 4] = read_bytes(slice, 4)?;
                let msg_len = u32::from_be_bytes(len_bytes) as usize;

                if current_pos + FRAME_HEADER_SIZE + msg_len > mmap.len() {
                    break;
                }

                let payload = &slice[FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + msg_len];

                let mut hasher = Hasher::new();
                hasher.update(payload);
                let computed_crc = hasher.finalize();
                if computed_crc != stored_crc {
                    return Err(Error::Other(format!(
                        "CRC mismatch in timestamp_bounds at position {} (base_offset {})",
                        current_pos, base_offset
                    )));
                }

                if let Ok(msg) = Message::from_bytes(payload) {
                    let ts = msg.timestamp.timestamp_millis();
                    min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                }

                current_pos += FRAME_HEADER_SIZE + msg_len;
            }

            Ok(match (min_ts, max_ts) {
                (Some(min), Some(max)) => Some((min, max)),
                _ => None,
            })
        })
        .await
        .map_err(|e| Error::Other(format!("timestamp_bounds join: {}", e)))??;

        // Cache the result for subsequent calls
        let mut cached = self.cached_timestamp_bounds.lock();
        *cached = Some(result);

        Ok(result)
    }

    /// Invalidate the cached timestamp bounds (call after appending to this segment)
    pub fn invalidate_timestamp_cache(&self) {
        let mut cached = self.cached_timestamp_bounds.lock();
        *cached = None;
    }
}
