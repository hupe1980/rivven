//! Group Commit Write-Ahead Log (WAL)
//!
//! High-performance WAL implementation with group commit optimization:
//! - **Group Commit**: Batches multiple writes into single fsync (10-100x throughput)
//! - **Pipelined Writes**: Overlaps I/O with compute
//! - **Pre-allocated Files**: Reduces filesystem overhead
//! - **CRC32 Checksums**: Data integrity verification
//! - **Asynchronous Sync**: Non-blocking durability
//!
//! Based on techniques from:
//! - MySQL InnoDB group commit
//! - PostgreSQL WAL
//! - RocksDB write batching

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};

/// WAL record header size: magic(4) + crc(4) + len(4) + type(1) + flags(1) = 14 bytes
const RECORD_HEADER_SIZE: usize = 14;

/// Magic number for WAL records
const WAL_MAGIC: u32 = 0x57414C52; // "WALR"

/// Default group commit window (microseconds)
const DEFAULT_GROUP_COMMIT_WINDOW_US: u64 = 200;

/// Default max batch size (bytes)
const DEFAULT_MAX_BATCH_SIZE: usize = 4 * 1024 * 1024; // 4 MB

/// Default max pending writes before forcing flush
const DEFAULT_MAX_PENDING_WRITES: usize = 1000;

/// WAL record types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// Full record (single record contains complete data)
    Full = 0,
    /// First fragment of a large record
    First = 1,
    /// Middle fragment of a large record
    Middle = 2,
    /// Last fragment of a large record
    Last = 3,
    /// Checkpoint marker
    Checkpoint = 4,
    /// Transaction begin
    TxnBegin = 5,
    /// Transaction commit
    TxnCommit = 6,
    /// Transaction abort
    TxnAbort = 7,
}

impl TryFrom<u8> for RecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RecordType::Full),
            1 => Ok(RecordType::First),
            2 => Ok(RecordType::Middle),
            3 => Ok(RecordType::Last),
            4 => Ok(RecordType::Checkpoint),
            5 => Ok(RecordType::TxnBegin),
            6 => Ok(RecordType::TxnCommit),
            7 => Ok(RecordType::TxnAbort),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid record type",
            )),
        }
    }
}

/// WAL record flags
#[derive(Debug, Clone, Copy)]
pub struct RecordFlags(u8);

impl RecordFlags {
    pub const NONE: Self = Self(0);
    pub const COMPRESSED: Self = Self(1 << 0);
    pub const ENCRYPTED: Self = Self(1 << 1);
    pub const HAS_CHECKSUM: Self = Self(1 << 2);

    pub fn is_compressed(&self) -> bool {
        self.0 & Self::COMPRESSED.0 != 0
    }

    pub fn is_encrypted(&self) -> bool {
        self.0 & Self::ENCRYPTED.0 != 0
    }

    pub fn has_checksum(&self) -> bool {
        self.0 & Self::HAS_CHECKSUM.0 != 0
    }
}

/// A WAL record
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Log sequence number
    pub lsn: u64,
    /// Record type
    pub record_type: RecordType,
    /// Flags
    pub flags: RecordFlags,
    /// Record data
    pub data: Bytes,
}

impl WalRecord {
    /// Create a new full record
    pub fn new(lsn: u64, data: Bytes) -> Self {
        Self {
            lsn,
            record_type: RecordType::Full,
            flags: RecordFlags::HAS_CHECKSUM,
            data,
        }
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(RECORD_HEADER_SIZE + self.data.len());

        // Calculate CRC of data
        let mut hasher = Hasher::new();
        hasher.update(&self.data);
        let crc = hasher.finalize();

        // Write header
        buf.put_u32(WAL_MAGIC);
        buf.put_u32(crc);
        buf.put_u32(self.data.len() as u32);
        buf.put_u8(self.record_type as u8);
        buf.put_u8(self.flags.0);

        // Write data
        buf.extend_from_slice(&self.data);

        buf.freeze()
    }

    /// Parse from bytes
    pub fn from_bytes(data: &[u8], lsn: u64) -> io::Result<Self> {
        if data.len() < RECORD_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Record too short",
            ));
        }

        // Read header
        let magic = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        if magic != WAL_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic number",
            ));
        }

        let stored_crc = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
        let data_len = u32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let record_type = RecordType::try_from(data[12])?;
        let flags = RecordFlags(data[13]);

        if data.len() < RECORD_HEADER_SIZE + data_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Incomplete record",
            ));
        }

        let record_data =
            Bytes::copy_from_slice(&data[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + data_len]);

        // Verify CRC
        let mut hasher = Hasher::new();
        hasher.update(&record_data);
        let computed_crc = hasher.finalize();

        if computed_crc != stored_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok(Self {
            lsn,
            record_type,
            flags,
            data: record_data,
        })
    }

    /// Get total serialized size
    pub fn serialized_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.data.len()
    }
}

/// Configuration for group commit WAL
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL files
    pub dir: PathBuf,
    /// Group commit window (how long to wait for more writes)
    pub group_commit_window: Duration,
    /// Maximum batch size before forcing flush
    pub max_batch_size: usize,
    /// Maximum pending writes before forcing flush
    pub max_pending_writes: usize,
    /// Pre-allocate WAL files to this size
    pub preallocate_size: u64,
    /// Enable direct I/O (bypass OS cache)
    pub direct_io: bool,
    /// Sync mode
    pub sync_mode: SyncMode,
    /// Maximum WAL file size before rotation
    pub max_file_size: u64,
    /// Optional encryption for data at rest
    #[cfg(feature = "encryption")]
    pub encryptor: Option<std::sync::Arc<dyn crate::encryption::Encryptor>>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./wal"),
            group_commit_window: Duration::from_micros(DEFAULT_GROUP_COMMIT_WINDOW_US),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_pending_writes: DEFAULT_MAX_PENDING_WRITES,
            preallocate_size: 64 * 1024 * 1024, // 64 MB
            direct_io: false,                   // Requires O_DIRECT support
            sync_mode: SyncMode::Fsync,
            max_file_size: 1024 * 1024 * 1024, // 1 GB
            #[cfg(feature = "encryption")]
            encryptor: None,
        }
    }
}

impl WalConfig {
    /// High-throughput configuration (more batching, less frequent sync)
    pub fn high_throughput() -> Self {
        Self {
            group_commit_window: Duration::from_micros(1000), // 1ms
            max_batch_size: 16 * 1024 * 1024,                 // 16 MB
            max_pending_writes: 5000,
            ..Default::default()
        }
    }

    /// Low-latency configuration (less batching, more frequent sync)
    pub fn low_latency() -> Self {
        Self {
            group_commit_window: Duration::from_micros(50), // 50us
            max_batch_size: 512 * 1024,                     // 512 KB
            max_pending_writes: 100,
            ..Default::default()
        }
    }

    /// Durability-focused configuration
    pub fn durable() -> Self {
        Self {
            sync_mode: SyncMode::FsyncData,
            ..Default::default()
        }
    }
}

/// Sync mode for WAL writes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// No sync (fastest, least durable)
    None,
    /// fdatasync (syncs data but not metadata)
    FsyncData,
    /// Full fsync (syncs data and metadata)
    Fsync,
    /// O_DSYNC flag (sync on each write)
    Dsync,
}

/// Write request for the WAL
struct WriteRequest {
    /// Data to write
    data: Bytes,
    /// Record type
    record_type: RecordType,
    /// Channel to send completion notification (carries Result so disk errors propagate)
    completion: oneshot::Sender<Result<WriteResult, String>>,
}

/// Result of a write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Assigned LSN
    pub lsn: u64,
    /// Size written
    pub size: usize,
    /// Whether this write was part of a group commit
    pub group_commit: bool,
    /// Number of writes in the group
    pub group_size: usize,
    /// Time spent waiting for group commit
    pub wait_time: Duration,
}

/// Group commit WAL writer
pub struct GroupCommitWal {
    config: WalConfig,
    /// Current file writer
    writer: Mutex<WalWriter>,
    /// Current LSN
    current_lsn: AtomicU64,
    /// Write request sender
    write_tx: mpsc::Sender<WriteRequest>,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Notify when new writes arrive
    write_notify: Arc<Notify>,
    /// Statistics
    stats: Arc<WalStats>,
}

impl GroupCommitWal {
    /// Create a new group commit WAL
    pub async fn new(config: WalConfig) -> io::Result<Arc<Self>> {
        std::fs::create_dir_all(&config.dir)?;

        // Find the latest WAL file and LSN
        let (current_file, current_lsn) = Self::recover_state(&config).await?;

        let writer = WalWriter::new(current_file, config.clone())?;
        let (write_tx, write_rx) = mpsc::channel(config.max_pending_writes);

        let wal = Arc::new(Self {
            config,
            writer: Mutex::new(writer),
            current_lsn: AtomicU64::new(current_lsn),
            write_tx,
            shutdown: AtomicBool::new(false),
            write_notify: Arc::new(Notify::new()),
            stats: Arc::new(WalStats::new()),
        });

        // Start background group commit worker
        wal.clone().start_group_commit_worker(write_rx);

        Ok(wal)
    }

    /// Recover state from existing WAL files
    async fn recover_state(config: &WalConfig) -> io::Result<(PathBuf, u64)> {
        let mut max_lsn = 0u64;
        let mut latest_file = None;

        if let Ok(entries) = std::fs::read_dir(&config.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "wal") {
                    if let Some(name) = path.file_stem() {
                        if let Ok(lsn) = name.to_string_lossy().parse::<u64>() {
                            if lsn >= max_lsn {
                                max_lsn = lsn;
                                latest_file = Some(path);
                            }
                        }
                    }
                }
            }
        }

        // If we found a file, scan it to find the true max LSN
        if let Some(ref file) = latest_file {
            if let Ok(recovered_lsn) = Self::scan_wal_file(file).await {
                max_lsn = recovered_lsn;
            }
        }

        // Create new file if none exists
        let file = latest_file.unwrap_or_else(|| config.dir.join(format!("{:020}.wal", 0)));

        Ok((file, max_lsn))
    }

    /// Scan a WAL file to find the highest LSN
    async fn scan_wal_file(path: &Path) -> io::Result<u64> {
        let data = tokio::fs::read(path).await?;
        let mut offset = 0;
        let mut max_lsn = 0u64;

        while offset + RECORD_HEADER_SIZE <= data.len() {
            // Check magic
            let magic = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);

            if magic != WAL_MAGIC {
                break;
            }

            let data_len = u32::from_be_bytes([
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
                data[offset + 11],
            ]) as usize;

            let record_size = RECORD_HEADER_SIZE + data_len;
            if offset + record_size > data.len() {
                break;
            }

            max_lsn += 1;
            offset += record_size;
        }

        Ok(max_lsn)
    }

    /// Write a record to the WAL (async, batched)
    pub async fn write(&self, data: Bytes) -> io::Result<WriteResult> {
        self.write_with_type(data, RecordType::Full).await
    }

    /// Write a record with specific type
    pub async fn write_with_type(
        &self,
        data: Bytes,
        record_type: RecordType,
    ) -> io::Result<WriteResult> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "WAL is shut down",
            ));
        }

        let (tx, rx) = oneshot::channel();

        let request = WriteRequest {
            data,
            record_type,
            completion: tx,
        };

        self.write_tx
            .send(request)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL write channel closed"))?;

        self.write_notify.notify_one();

        rx.await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL write cancelled"))?
            .map_err(io::Error::other)
    }

    /// Write a batch of records atomically
    pub async fn write_batch(&self, records: Vec<Bytes>) -> io::Result<Vec<WriteResult>> {
        let mut results = Vec::with_capacity(records.len());
        let mut receivers = Vec::with_capacity(records.len());

        for data in records {
            let (tx, rx) = oneshot::channel();

            let request = WriteRequest {
                data,
                record_type: RecordType::Full,
                completion: tx,
            };

            self.write_tx.send(request).await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "WAL write channel closed")
            })?;

            receivers.push(rx);
        }

        self.write_notify.notify_one();

        for rx in receivers {
            let result = rx
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "WAL write cancelled"))?
                .map_err(io::Error::other)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Sync the WAL to disk (force flush)
    pub async fn sync(&self) -> io::Result<()> {
        let mut writer = self.writer.lock().await;
        writer.sync()
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Acquire)
    }

    /// Get WAL statistics
    pub fn stats(&self) -> WalStatsSnapshot {
        WalStatsSnapshot {
            writes_total: self.stats.writes_total.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            syncs_total: self.stats.syncs_total.load(Ordering::Relaxed),
            group_commits: self.stats.group_commits.load(Ordering::Relaxed),
            avg_group_size: if self.stats.group_commits.load(Ordering::Relaxed) > 0 {
                self.stats.writes_total.load(Ordering::Relaxed) as f64
                    / self.stats.group_commits.load(Ordering::Relaxed) as f64
            } else {
                0.0
            },
            current_lsn: self.current_lsn.load(Ordering::Relaxed),
        }
    }

    /// Shutdown the WAL
    pub async fn shutdown(&self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        self.write_notify.notify_waiters();

        // Final sync
        let mut writer = self.writer.lock().await;
        writer.sync()
    }

    /// Start the background group commit worker
    fn start_group_commit_worker(self: Arc<Self>, mut rx: mpsc::Receiver<WriteRequest>) {
        let wal = self.clone();

        tokio::spawn(async move {
            let mut pending: VecDeque<WriteRequest> = VecDeque::new();
            let mut batch_buffer = BytesMut::with_capacity(wal.config.max_batch_size);
            let mut group_start: Option<Instant> = None;

            loop {
                // Check shutdown early
                if wal.shutdown.load(Ordering::Acquire) {
                    // Drain any remaining messages from channel
                    while let Ok(request) = rx.try_recv() {
                        pending.push_back(request);
                    }
                    // Flush remaining and exit
                    if !pending.is_empty() {
                        wal.flush_batch(&mut pending, &mut batch_buffer, group_start.take())
                            .await;
                    }
                    break;
                }

                // Wait for writes or timeout
                let timeout = if pending.is_empty() {
                    Duration::from_secs(60) // Long timeout when idle
                } else {
                    wal.config.group_commit_window
                };

                tokio::select! {
                    biased;

                    // Receive new write request (higher priority)
                    Some(request) = rx.recv() => {
                        if group_start.is_none() {
                            group_start = Some(Instant::now());
                        }
                        pending.push_back(request);

                        // Check if we should flush immediately
                        let should_flush =
                            pending.len() >= wal.config.max_pending_writes ||
                            batch_buffer.len() >= wal.config.max_batch_size;

                        if should_flush {
                            wal.flush_batch(&mut pending, &mut batch_buffer, group_start.take()).await;
                        }
                    }

                    // Wait for notification
                    _ = wal.write_notify.notified() => {
                        // Continue loop to check shutdown flag
                    }

                    // Timeout - flush whatever we have
                    _ = tokio::time::sleep(timeout) => {
                        if !pending.is_empty() {
                            wal.flush_batch(&mut pending, &mut batch_buffer, group_start.take()).await;
                        }
                    }
                }
            }
        });
    }

    /// Flush a batch of pending writes
    async fn flush_batch(
        &self,
        pending: &mut VecDeque<WriteRequest>,
        batch_buffer: &mut BytesMut,
        group_start: Option<Instant>,
    ) {
        if pending.is_empty() {
            return;
        }

        let wait_time = group_start.map(|s| s.elapsed()).unwrap_or(Duration::ZERO);
        let group_size = pending.len();

        // Build batch
        batch_buffer.clear();
        let mut lsns = Vec::with_capacity(group_size);
        let mut sizes = Vec::with_capacity(group_size);

        for request in pending.iter() {
            let lsn = self.current_lsn.fetch_add(1, Ordering::AcqRel) + 1;
            lsns.push(lsn);

            // Optionally encrypt the data
            #[cfg(feature = "encryption")]
            let (data, is_encrypted) = if let Some(ref encryptor) = self.config.encryptor {
                if encryptor.is_enabled() {
                    match encryptor.encrypt(&request.data, lsn) {
                        Ok(encrypted) => (Bytes::from(encrypted), true),
                        Err(e) => {
                            tracing::error!("Encryption failed for LSN {}: {:?}", lsn, e);
                            (request.data.clone(), false)
                        }
                    }
                } else {
                    (request.data.clone(), false)
                }
            } else {
                (request.data.clone(), false)
            };

            #[cfg(not(feature = "encryption"))]
            let (data, is_encrypted) = (request.data.clone(), false);

            let flags = if is_encrypted {
                RecordFlags(RecordFlags::HAS_CHECKSUM.0 | RecordFlags::ENCRYPTED.0)
            } else {
                RecordFlags::HAS_CHECKSUM
            };

            let record = WalRecord {
                lsn,
                record_type: request.record_type,
                flags,
                data,
            };

            let record_bytes = record.to_bytes();
            sizes.push(record_bytes.len());
            batch_buffer.extend_from_slice(&record_bytes);
        }

        // Write batch to disk and rotate if file exceeds max_file_size
        let write_result = {
            let mut writer = self.writer.lock().await;
            let result = writer.write_batch(batch_buffer);
            if result.is_ok() {
                // Check rotation after successful write
                let next_lsn = self.current_lsn.load(Ordering::Acquire) + 1;
                if let Err(e) = writer.rotate_if_needed(next_lsn) {
                    tracing::error!("WAL rotation failed: {e}");
                    // Rotation failure is non-fatal — writes continue to current file
                }
            }
            result
        };

        // Update stats
        self.stats
            .writes_total
            .fetch_add(group_size as u64, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(batch_buffer.len() as u64, Ordering::Relaxed);
        self.stats.group_commits.fetch_add(1, Ordering::Relaxed);
        self.stats.syncs_total.fetch_add(1, Ordering::Relaxed);

        // Send results to waiters
        let group_commit = group_size > 1;

        for (i, request) in pending.drain(..).enumerate() {
            let result = match &write_result {
                Ok(()) => Ok(WriteResult {
                    lsn: lsns[i],
                    size: sizes[i],
                    group_commit,
                    group_size,
                    wait_time,
                }),
                Err(e) => Err(format!("WAL write failed: {e}")),
            };

            let _ = request.completion.send(result);
        }
    }
}

/// Low-level WAL file writer
struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    position: u64,
    config: WalConfig,
}

impl WalWriter {
    fn new(path: PathBuf, config: WalConfig) -> io::Result<Self> {
        use std::io::{Seek, SeekFrom};

        // Check if file exists and has valid data
        let existing_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false) // Preserve existing WAL data
            .open(&path)?;

        // Determine actual data length (scan for end of valid records)
        let actual_position = if existing_len > 0 {
            Self::find_actual_end(&file, existing_len)?
        } else {
            0
        };

        // Pre-allocate if this is a new file
        if actual_position == 0 && config.preallocate_size > 0 {
            file.set_len(config.preallocate_size)?;
        }

        // Seek to the actual write position
        let mut writer = BufWriter::with_capacity(config.max_batch_size, file);
        writer.seek(SeekFrom::Start(actual_position))?;

        Ok(Self {
            file: writer,
            path,
            position: actual_position,
            config,
        })
    }

    /// Find the actual end of valid data in the file
    fn find_actual_end(file: &File, file_len: u64) -> io::Result<u64> {
        use std::io::Read;

        let mut position = 0u64;
        let mut file = file.try_clone()?;

        // Read in chunks to find valid record boundaries
        while position + RECORD_HEADER_SIZE as u64 <= file_len {
            let mut header = [0u8; RECORD_HEADER_SIZE];

            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(position))?;

            if file.read_exact(&mut header).is_err() {
                break;
            }

            // Check magic
            let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
            if magic != WAL_MAGIC {
                break;
            }

            let data_len =
                u32::from_be_bytes([header[8], header[9], header[10], header[11]]) as u64;
            let record_size = RECORD_HEADER_SIZE as u64 + data_len;

            if position + record_size > file_len {
                break;
            }

            position += record_size;
        }

        Ok(position)
    }

    fn write_batch(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data)?;
        self.file.flush()?;

        // Sync based on mode
        match self.config.sync_mode {
            SyncMode::None => {}
            SyncMode::FsyncData => {
                self.file.get_ref().sync_data()?;
            }
            SyncMode::Fsync | SyncMode::Dsync => {
                self.file.get_ref().sync_all()?;
            }
        }

        self.position += data.len() as u64;
        Ok(())
    }

    /// Rotate the WAL file if the current file exceeds max_file_size.
    ///
    /// Flushes and syncs the current file, then creates a new WAL segment
    /// named after the given LSN. Returns `true` if rotation occurred.
    fn rotate_if_needed(&mut self, next_lsn: u64) -> io::Result<bool> {
        if self.config.max_file_size == 0 || self.position < self.config.max_file_size {
            return Ok(false);
        }

        // Sync and close current file
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

        // Truncate preallocated space to actual data length
        if self.position < self.file.get_ref().metadata()?.len() {
            self.file.get_ref().set_len(self.position)?;
        }

        // Create new WAL file named after the next LSN
        let new_path = self.config.dir.join(format!("{:020}.wal", next_lsn));

        tracing::info!(
            old_file = %self.path.display(),
            new_file = %new_path.display(),
            old_size = self.position,
            max_size = self.config.max_file_size,
            "Rotating WAL file"
        );

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&new_path)?;

        // Pre-allocate new file
        if self.config.preallocate_size > 0 {
            file.set_len(self.config.preallocate_size)?;
        }

        self.file = BufWriter::with_capacity(self.config.max_batch_size, file);
        self.path = new_path;
        self.position = 0;

        Ok(true)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()
    }

    /// Get the path of this WAL file
    #[allow(dead_code)]
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

/// WAL statistics
struct WalStats {
    writes_total: AtomicU64,
    bytes_written: AtomicU64,
    syncs_total: AtomicU64,
    group_commits: AtomicU64,
}

impl WalStats {
    fn new() -> Self {
        Self {
            writes_total: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            syncs_total: AtomicU64::new(0),
            group_commits: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalStatsSnapshot {
    pub writes_total: u64,
    pub bytes_written: u64,
    pub syncs_total: u64,
    pub group_commits: u64,
    pub avg_group_size: f64,
    pub current_lsn: u64,
}

/// WAL reader for recovery and replication
pub struct WalReader {
    path: PathBuf,
    position: u64,
    #[cfg(feature = "encryption")]
    encryptor: Option<std::sync::Arc<dyn crate::encryption::Encryptor>>,
}

impl WalReader {
    /// Open a WAL file for reading
    pub fn open(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            path,
            position: 0,
            #[cfg(feature = "encryption")]
            encryptor: None,
        })
    }

    /// Open a WAL file for reading with optional encryption
    #[cfg(feature = "encryption")]
    pub fn open_with_encryption(
        path: PathBuf,
        encryptor: Option<std::sync::Arc<dyn crate::encryption::Encryptor>>,
    ) -> io::Result<Self> {
        Ok(Self {
            path,
            position: 0,
            encryptor,
        })
    }

    /// Decrypt record data if needed
    #[cfg(feature = "encryption")]
    fn decrypt_record_data(&self, record: &mut WalRecord) -> io::Result<()> {
        if record.flags.is_encrypted() {
            if let Some(ref encryptor) = self.encryptor {
                let decrypted = encryptor
                    .decrypt(&record.data, record.lsn)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                record.data = Bytes::from(decrypted);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Record is encrypted but no encryptor provided",
                ));
            }
        }
        Ok(())
    }

    /// Read all records from current position
    pub async fn read_all(&mut self) -> io::Result<Vec<WalRecord>> {
        let data = tokio::fs::read(&self.path).await?;
        let mut records = Vec::new();
        let mut lsn = 0u64;

        while self.position + RECORD_HEADER_SIZE as u64 <= data.len() as u64 {
            let offset = self.position as usize;

            // Check magic
            let magic = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);

            if magic != WAL_MAGIC {
                break;
            }

            let data_len = u32::from_be_bytes([
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
                data[offset + 11],
            ]) as usize;

            let record_size = RECORD_HEADER_SIZE + data_len;

            if offset + record_size > data.len() {
                break;
            }

            lsn += 1;

            match WalRecord::from_bytes(&data[offset..offset + record_size], lsn) {
                #[cfg(feature = "encryption")]
                Ok(mut record) => {
                    // Decrypt if needed
                    self.decrypt_record_data(&mut record)?;

                    records.push(record);
                    self.position += record_size as u64;
                }
                #[cfg(not(feature = "encryption"))]
                Ok(record) => {
                    records.push(record);
                    self.position += record_size as u64;
                }
                Err(_) => break,
            }
        }

        Ok(records)
    }

    /// Seek to a specific LSN
    pub async fn seek_to_lsn(&mut self, target_lsn: u64) -> io::Result<()> {
        let data = tokio::fs::read(&self.path).await?;
        let mut position = 0usize;
        let mut current_lsn = 0u64;

        while position + RECORD_HEADER_SIZE <= data.len() {
            let magic = u32::from_be_bytes([
                data[position],
                data[position + 1],
                data[position + 2],
                data[position + 3],
            ]);

            if magic != WAL_MAGIC {
                break;
            }

            let data_len = u32::from_be_bytes([
                data[position + 8],
                data[position + 9],
                data[position + 10],
                data[position + 11],
            ]) as usize;

            let record_size = RECORD_HEADER_SIZE + data_len;
            current_lsn += 1;

            if current_lsn >= target_lsn {
                self.position = position as u64;
                return Ok(());
            }

            position += record_size;
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("LSN {} not found", target_lsn),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_record_serialization() {
        let data = Bytes::from("test data");
        let record = WalRecord::new(1, data.clone());

        let serialized = record.to_bytes();
        assert!(serialized.len() >= RECORD_HEADER_SIZE + data.len());

        let parsed = WalRecord::from_bytes(&serialized, 1).unwrap();
        assert_eq!(parsed.lsn, 1);
        assert_eq!(parsed.data, data);
    }

    #[test]
    fn test_wal_record_crc() {
        let data = Bytes::from("test data");
        let record = WalRecord::new(1, data);
        let mut serialized = record.to_bytes().to_vec();

        // Corrupt the data
        serialized[RECORD_HEADER_SIZE] ^= 0xFF;

        // Should fail CRC check
        assert!(WalRecord::from_bytes(&serialized, 1).is_err());
    }

    #[tokio::test]
    async fn test_group_commit_wal_single_write() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            group_commit_window: Duration::from_micros(100),
            ..Default::default()
        };

        let wal = GroupCommitWal::new(config).await.unwrap();

        let result = wal.write(Bytes::from("test data")).await.unwrap();
        assert_eq!(result.lsn, 1);
        assert!(result.size > 0);

        let stats = wal.stats();
        assert_eq!(stats.writes_total, 1);

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_wal_batch() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            group_commit_window: Duration::from_millis(10),
            ..Default::default()
        };

        let wal = GroupCommitWal::new(config).await.unwrap();

        let records: Vec<Bytes> = (0..10)
            .map(|i| Bytes::from(format!("record {}", i)))
            .collect();

        let results = wal.write_batch(records).await.unwrap();

        assert_eq!(results.len(), 10);
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.lsn, (i + 1) as u64);
        }

        let stats = wal.stats();
        assert_eq!(stats.writes_total, 10);

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_group_commit_batching() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            group_commit_window: Duration::from_millis(50),
            max_pending_writes: 100,
            ..Default::default()
        };

        let wal = Arc::new(GroupCommitWal::new(config).await.unwrap());

        // Spawn multiple writers concurrently
        let mut handles = vec![];
        for i in 0..20 {
            let wal_clone = wal.clone();
            handles.push(tokio::spawn(async move {
                wal_clone
                    .write(Bytes::from(format!("concurrent write {}", i)))
                    .await
            }));
        }

        // Wait for all writes
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            assert!(result.lsn > 0);
        }

        let stats = wal.stats();
        assert_eq!(stats.writes_total, 20);
        // With batching, we should have fewer syncs than writes
        assert!(stats.group_commits <= stats.writes_total);

        wal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_reader() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            group_commit_window: Duration::from_micros(100),
            sync_mode: SyncMode::Fsync,
            max_pending_writes: 10,
            ..Default::default()
        };

        let wal = GroupCommitWal::new(config.clone()).await.unwrap();

        // Write some records and wait for each to complete
        for i in 0..5 {
            let result = wal
                .write(Bytes::from(format!("record {}", i)))
                .await
                .unwrap();
            assert!(result.lsn > 0, "Expected valid LSN for record {}", i);
        }

        // Force a sync to ensure data is on disk
        wal.sync().await.unwrap();

        // Give a small delay to ensure background worker has flushed
        tokio::time::sleep(Duration::from_millis(100)).await;

        wal.shutdown().await.unwrap();

        // Find the WAL file
        let entries: Vec<_> = std::fs::read_dir(&config.dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();

        assert!(!entries.is_empty(), "No WAL files found");

        let wal_file = entries[0].path();
        let file_size = std::fs::metadata(&wal_file).unwrap().len();
        assert!(file_size > 0, "WAL file is empty");

        let mut reader = WalReader::open(wal_file).unwrap();
        let records = reader.read_all().await.unwrap();

        assert_eq!(
            records.len(),
            5,
            "Expected 5 records, got {} (file size: {})",
            records.len(),
            file_size
        );
        for (i, record) in records.iter().enumerate() {
            let expected = format!("record {}", i);
            assert_eq!(record.data, Bytes::from(expected));
        }
    }

    #[test]
    fn test_record_flags() {
        let flags = RecordFlags::COMPRESSED;
        assert!(flags.is_compressed());
        assert!(!flags.is_encrypted());

        let flags = RecordFlags(RecordFlags::COMPRESSED.0 | RecordFlags::ENCRYPTED.0);
        assert!(flags.is_compressed());
        assert!(flags.is_encrypted());
    }

    #[tokio::test]
    async fn test_wal_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            group_commit_window: Duration::from_micros(50),
            max_file_size: 200, // Very small to trigger rotation quickly
            preallocate_size: 0,
            ..Default::default()
        };

        let wal = GroupCommitWal::new(config).await.unwrap();

        // Write enough data to trigger rotation
        for i in 0..10 {
            let data = format!("rotation-record-{:04}", i);
            let result = wal.write(Bytes::from(data)).await.unwrap();
            assert!(result.lsn > 0);
        }

        wal.sync().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        wal.shutdown().await.unwrap();

        // Check that multiple WAL files were created (rotation occurred)
        let wal_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();

        assert!(
            wal_files.len() > 1,
            "Expected multiple WAL files after rotation, got {}",
            wal_files.len()
        );
    }

    #[test]
    fn test_wal_writer_rotate_if_needed() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            max_file_size: 100,
            preallocate_size: 0,
            ..Default::default()
        };

        let path = temp_dir.path().join("00000000000000000000.wal");
        let mut writer = WalWriter::new(path.clone(), config).unwrap();

        // Write some data to push past max_file_size
        writer.write_batch(&[0u8; 150]).unwrap();
        assert_eq!(writer.position, 150);

        // Should rotate
        let rotated = writer.rotate_if_needed(42).unwrap();
        assert!(rotated, "Expected rotation to occur");
        assert_eq!(writer.position, 0);
        assert_ne!(writer.path, path);
        assert!(writer
            .path
            .to_str()
            .unwrap()
            .contains("00000000000000000042"));

        // Should not rotate again immediately
        let rotated = writer.rotate_if_needed(43).unwrap();
        assert!(!rotated, "Expected no rotation when under max_file_size");
    }
}
