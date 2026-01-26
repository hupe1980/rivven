use crate::{Error, Message, Result};
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

const INDEX_ENTRY_SIZE: usize = 12; // 4 bytes relative offset, 8 bytes position
const LOG_SUFFIX: &str = "log";
const INDEX_SUFFIX: &str = "index";

/// Represents a segment of the log on disk
/// A segment consists of a .log file (data) and a .index file (sparse index)
#[derive(Debug)]
pub struct Segment {
    base_offset: u64,
    log_path: PathBuf,
    index_path: PathBuf,
    log_file: Arc<Mutex<File>>,
    current_size: u64,
    index_buffer: Vec<(u32, u64)>, // Relative offset -> Position
}

impl Segment {
    pub fn new(dir: &Path, base_offset: u64) -> Result<Self> {
        let log_path = dir.join(format!("{:020}.{}", base_offset, LOG_SUFFIX));
        let index_path = dir.join(format!("{:020}.{}", base_offset, INDEX_SUFFIX));

        // Open or create log file
        let mut log_file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&log_path)?;

        let current_size = log_file.seek(SeekFrom::End(0))?;

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
            log_file: Arc::new(Mutex::new(log_file)),
            current_size,
            index_buffer: Vec::new(),
        };

        // Load index if exists
        if index_file.metadata()?.len() > 0 {
            segment.load_index(&index_file)?;
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

        // 4. Write to disk (Holding lock)
        {
            let mut file = self.log_file.lock().await;
            file.write_all(&frame)?;
        }
        let position = self.current_size;
        // file.sync_data()?; // Optional: Call sync for durability (slow) or rely on OS cache

        self.current_size += frame.len() as u64;

        // 5. Update Index (every 4KB or so)

        // Simple strategy: Index every message for now for exact lookup in concept
        // optimized: only index if position - last_index_position > 4096
        let relative_offset = (offset - self.base_offset) as u32;
        self.append_index(relative_offset, position)?;

        Ok(position)
    }

    fn append_index(&mut self, relative_offset: u32, position: u64) -> Result<()> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.index_path)?;

        let mut buf = BytesMut::with_capacity(12);
        buf.put_u32(relative_offset);
        buf.put_u64(position);
        file.write_all(&buf)?;

        self.index_buffer.push((relative_offset, position));
        Ok(())
    }

    /// Flush segment data to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        let file = self.log_file.lock().await;
        file.sync_all()?;
        Ok(())
    }

    /// Read a batch of messages starting from a given offset
    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<Message>> {
        if offset < self.base_offset {
            return Ok(Vec::new()); // Or error? LogManager should handle this
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

        // 2. Mmap the file for reading (Zero clone from kernel cache context)
        let file = File::open(&self.log_path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(Vec::new());
        }

        // SAFETY: File is opened read-only and remains valid for mmap lifetime.
        // We check bounds before all slice accesses below.
        let mmap = unsafe { Mmap::map(&file)? };

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
            let _cursor = std::io::Cursor::new(slice);

            // Read CRC and Len
            // Using converting methods
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

    pub async fn recover_last_offset(&self) -> Result<Option<u64>> {
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
            let len_bytes: [u8; 4] = slice[4..8].try_into().unwrap();
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            if current_pos + 8 + msg_len > mmap.len() {
                break;
            }

            let payload = &slice[8..8 + msg_len];
            if let Ok(msg) = Message::from_bytes(payload) {
                last_offset = Some(msg.offset);
            }

            current_pos += 8 + msg_len;
        }

        Ok(last_offset)
    }

    /// Find the first offset with timestamp >= target_timestamp
    /// Uses linear scan through the segment (timestamps may not be monotonic due to clock skew)
    /// Returns None if no matching offset is found
    pub async fn find_offset_for_timestamp(&self, target_timestamp: i64) -> Result<Option<u64>> {
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
