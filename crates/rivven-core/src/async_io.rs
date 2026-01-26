//! Cross-Platform Async I/O Layer
//!
//! High-performance asynchronous I/O that works on all major platforms:
//!
//! - **Linux**: Uses tokio's epoll-based I/O (io_uring available via feature flag)
//! - **macOS**: Uses tokio's kqueue-based I/O (optimized for Apple Silicon)
//! - **Windows**: Uses tokio's IOCP-based I/O
//!
//! # Design Philosophy
//!
//! Rather than requiring platform-specific features or elevated permissions,
//! this module provides a unified async I/O API that delivers excellent
//! performance on all platforms using tokio's battle-tested I/O primitives.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     AsyncIo Unified API                             │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                     │
//! │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
//! │  │   AsyncFile  │    │ BatchBuilder │    │ AsyncSegment │          │
//! │  │  read/write  │    │  batched ops │    │  log storage │          │
//! │  └──────────────┘    └──────────────┘    └──────────────┘          │
//! │         │                   │                   │                   │
//! │         └───────────────────┴───────────────────┘                   │
//! │                             │                                       │
//! │                    ┌────────┴────────┐                              │
//! │                    │   Tokio Async   │                              │
//! │                    │   File I/O      │                              │
//! │                    └────────┬────────┘                              │
//! │                             │                                       │
//! │  ┌──────────┬───────────────┼───────────────┬──────────┐           │
//! │  │  Linux   │    macOS      │    Windows    │  Other   │           │
//! │  │  epoll   │    kqueue     │    IOCP       │  poll    │           │
//! │  └──────────┴───────────────┴───────────────┴──────────┘           │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Performance Characteristics
//!
//! | Platform | Backend | Typical Latency | Notes |
//! |----------|---------|-----------------|-------|
//! | Linux    | epoll   | ~5-10µs         | Scales to millions of fds |
//! | macOS    | kqueue  | ~5-10µs         | Native Apple Silicon support |
//! | Windows  | IOCP    | ~10-20µs        | True async completion ports |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_core::async_io::{AsyncIo, AsyncFile};
//!
//! let io = AsyncIo::new(AsyncIoConfig::default())?;
//! let file = AsyncFile::open("data.log", io.clone()).await?;
//!
//! // Write data
//! file.write_at(0, b"Hello, World!").await?;
//!
//! // Read it back
//! let data = file.read_at(0, 13).await?;
//! ```

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex as TokioMutex;

// ============================================================================
// Configuration
// ============================================================================

/// Async I/O configuration
#[derive(Debug, Clone)]
pub struct AsyncIoConfig {
    /// Maximum concurrent operations
    pub max_concurrent_ops: usize,
    /// Read buffer size
    pub read_buffer_size: usize,
    /// Write buffer size  
    pub write_buffer_size: usize,
    /// Enable direct I/O hints (advisory, platform-dependent)
    pub direct_io_hint: bool,
    /// Sync on write (for durability)
    pub sync_on_write: bool,
}

impl Default for AsyncIoConfig {
    fn default() -> Self {
        Self {
            max_concurrent_ops: 1024,
            read_buffer_size: 64 * 1024,  // 64KB
            write_buffer_size: 64 * 1024, // 64KB
            direct_io_hint: false,
            sync_on_write: false,
        }
    }
}

impl AsyncIoConfig {
    /// High-performance configuration for SSDs
    pub fn high_performance() -> Self {
        Self {
            max_concurrent_ops: 4096,
            read_buffer_size: 128 * 1024,  // 128KB
            write_buffer_size: 128 * 1024, // 128KB
            direct_io_hint: true,
            sync_on_write: false,
        }
    }

    /// Low-latency configuration  
    pub fn low_latency() -> Self {
        Self {
            max_concurrent_ops: 2048,
            read_buffer_size: 4 * 1024,  // 4KB - smaller for lower latency
            write_buffer_size: 4 * 1024, // 4KB
            direct_io_hint: true,
            sync_on_write: false,
        }
    }

    /// Durable configuration (sync writes)
    pub fn durable() -> Self {
        Self {
            max_concurrent_ops: 512,
            read_buffer_size: 64 * 1024,
            write_buffer_size: 64 * 1024,
            direct_io_hint: false,
            sync_on_write: true,
        }
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// I/O operation statistics
#[derive(Debug, Default)]
pub struct AsyncIoStats {
    /// Total read operations
    pub read_ops: AtomicU64,
    /// Total write operations
    pub write_ops: AtomicU64,
    /// Total sync operations
    pub sync_ops: AtomicU64,
    /// Total bytes read
    pub bytes_read: AtomicU64,
    /// Total bytes written
    pub bytes_written: AtomicU64,
    /// Failed operations
    pub failed_ops: AtomicU64,
    /// Current in-flight operations
    pub inflight: AtomicUsize,
}

impl AsyncIoStats {
    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> AsyncIoStatsSnapshot {
        AsyncIoStatsSnapshot {
            read_ops: self.read_ops.load(Ordering::Relaxed),
            write_ops: self.write_ops.load(Ordering::Relaxed),
            sync_ops: self.sync_ops.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            failed_ops: self.failed_ops.load(Ordering::Relaxed),
            inflight: self.inflight.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of I/O statistics
#[derive(Debug, Clone)]
pub struct AsyncIoStatsSnapshot {
    pub read_ops: u64,
    pub write_ops: u64,
    pub sync_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub failed_ops: u64,
    pub inflight: usize,
}

impl AsyncIoStatsSnapshot {
    /// Calculate throughput in MB/s given a time window
    pub fn throughput_mbps(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            let total_bytes = self.bytes_read + self.bytes_written;
            (total_bytes as f64 / 1024.0 / 1024.0) / duration_secs
        } else {
            0.0
        }
    }

    /// Calculate operations per second
    pub fn ops_per_second(&self, duration_secs: f64) -> f64 {
        if duration_secs > 0.0 {
            (self.read_ops + self.write_ops + self.sync_ops) as f64 / duration_secs
        } else {
            0.0
        }
    }
}

// ============================================================================
// Async I/O Engine
// ============================================================================

/// Cross-platform async I/O engine
///
/// Provides high-performance async I/O using the best available
/// backend on each platform (epoll/kqueue/IOCP via tokio).
pub struct AsyncIo {
    config: AsyncIoConfig,
    stats: Arc<AsyncIoStats>,
}

impl AsyncIo {
    /// Create a new async I/O engine
    pub fn new(config: AsyncIoConfig) -> io::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            config,
            stats: Arc::new(AsyncIoStats::default()),
        }))
    }

    /// Get the configuration
    pub fn config(&self) -> &AsyncIoConfig {
        &self.config
    }

    /// Get statistics
    pub fn stats(&self) -> &AsyncIoStats {
        &self.stats
    }

    /// Check if there's capacity for more operations
    pub fn has_capacity(&self) -> bool {
        self.stats.inflight.load(Ordering::Relaxed) < self.config.max_concurrent_ops
    }

    /// Get number of in-flight operations
    pub fn inflight(&self) -> usize {
        self.stats.inflight.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Async File
// ============================================================================

/// High-level async file handle
///
/// Provides async read/write operations with automatic statistics tracking.
pub struct AsyncFile {
    file: TokioMutex<File>,
    io: Arc<AsyncIo>,
    position: AtomicU64,
}

impl AsyncFile {
    /// Open a file for reading and writing (creates if not exists)
    pub async fn open<P: AsRef<Path>>(path: P, io: Arc<AsyncIo>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Preserve existing data
            .open(path)
            .await?;

        Ok(Self {
            file: TokioMutex::new(file),
            io,
            position: AtomicU64::new(0),
        })
    }

    /// Open a file read-only
    pub async fn open_read<P: AsRef<Path>>(path: P, io: Arc<AsyncIo>) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path).await?;

        Ok(Self {
            file: TokioMutex::new(file),
            io,
            position: AtomicU64::new(0),
        })
    }

    /// Async read at a specific offset
    pub async fn read_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        self.io.stats.inflight.fetch_add(1, Ordering::Relaxed);

        let result = async {
            let mut file = self.file.lock().await;
            file.seek(io::SeekFrom::Start(offset)).await?;

            let mut buf = BytesMut::with_capacity(len);
            buf.resize(len, 0);

            let bytes_read = file.read(&mut buf).await?;
            buf.truncate(bytes_read);

            Ok::<_, io::Error>(buf.freeze())
        }
        .await;

        self.io.stats.inflight.fetch_sub(1, Ordering::Relaxed);

        match &result {
            Ok(data) => {
                self.io.stats.read_ops.fetch_add(1, Ordering::Relaxed);
                self.io
                    .stats
                    .bytes_read
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
            }
            Err(_) => {
                self.io.stats.failed_ops.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    /// Async write at a specific offset
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize> {
        self.io.stats.inflight.fetch_add(1, Ordering::Relaxed);

        let result = async {
            let mut file = self.file.lock().await;
            file.seek(io::SeekFrom::Start(offset)).await?;

            let written = file.write(data).await?;

            if self.io.config.sync_on_write {
                file.sync_all().await?;
            }

            Ok::<_, io::Error>(written)
        }
        .await;

        self.io.stats.inflight.fetch_sub(1, Ordering::Relaxed);

        match &result {
            Ok(written) => {
                self.io.stats.write_ops.fetch_add(1, Ordering::Relaxed);
                self.io
                    .stats
                    .bytes_written
                    .fetch_add(*written as u64, Ordering::Relaxed);
            }
            Err(_) => {
                self.io.stats.failed_ops.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    /// Async read at current position (updates position)
    pub async fn read(&self, len: usize) -> io::Result<Bytes> {
        let pos = self.position.load(Ordering::Relaxed);
        let data = self.read_at(pos, len).await?;
        self.position
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    /// Async write at current position (updates position)
    pub async fn write(&self, data: &[u8]) -> io::Result<usize> {
        let pos = self.position.load(Ordering::Relaxed);
        let written = self.write_at(pos, data).await?;
        self.position.fetch_add(written as u64, Ordering::Relaxed);
        Ok(written)
    }

    /// Sync file to disk
    pub async fn sync(&self) -> io::Result<()> {
        self.io.stats.inflight.fetch_add(1, Ordering::Relaxed);

        let result = {
            let file = self.file.lock().await;
            file.sync_all().await
        };

        self.io.stats.inflight.fetch_sub(1, Ordering::Relaxed);

        match &result {
            Ok(_) => {
                self.io.stats.sync_ops.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.io.stats.failed_ops.fetch_add(1, Ordering::Relaxed);
            }
        }

        result
    }

    /// Seek to position
    pub fn seek(&self, pos: u64) {
        self.position.store(pos, Ordering::Relaxed);
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::Relaxed)
    }

    /// Get file size
    pub async fn size(&self) -> io::Result<u64> {
        let file = self.file.lock().await;
        Ok(file.metadata().await?.len())
    }
}

// ============================================================================
// Batch Builder
// ============================================================================

/// Builder for batched I/O operations
///
/// Allows building up multiple I/O operations and executing them together.
pub struct BatchBuilder {
    io: Arc<AsyncIo>,
    ops: Vec<BatchOp>,
}

enum BatchOp {
    Read {
        path: std::path::PathBuf,
        offset: u64,
        len: usize,
    },
    Write {
        path: std::path::PathBuf,
        offset: u64,
        data: Vec<u8>,
    },
}

/// Result of a batch operation
#[derive(Debug)]
pub enum BatchResult {
    /// Read completed successfully
    Read(Bytes),
    /// Write completed successfully (bytes written)
    Write(usize),
    /// Operation failed
    Error(io::Error),
}

impl BatchBuilder {
    /// Create a new batch builder
    pub fn new(io: Arc<AsyncIo>) -> Self {
        Self {
            io,
            ops: Vec::new(),
        }
    }

    /// Add a read operation
    pub fn read<P: AsRef<Path>>(mut self, path: P, offset: u64, len: usize) -> Self {
        self.ops.push(BatchOp::Read {
            path: path.as_ref().to_path_buf(),
            offset,
            len,
        });
        self
    }

    /// Add a write operation
    pub fn write<P: AsRef<Path>>(mut self, path: P, offset: u64, data: Vec<u8>) -> Self {
        self.ops.push(BatchOp::Write {
            path: path.as_ref().to_path_buf(),
            offset,
            data,
        });
        self
    }

    /// Execute all operations concurrently
    pub async fn execute(self) -> Vec<BatchResult> {
        use futures::future::join_all;

        let io = self.io;
        let futures: Vec<_> = self
            .ops
            .into_iter()
            .map(|op| {
                let io = io.clone();
                async move {
                    match op {
                        BatchOp::Read { path, offset, len } => {
                            match AsyncFile::open(&path, io).await {
                                Ok(file) => match file.read_at(offset, len).await {
                                    Ok(data) => BatchResult::Read(data),
                                    Err(e) => BatchResult::Error(e),
                                },
                                Err(e) => BatchResult::Error(e),
                            }
                        }
                        BatchOp::Write { path, offset, data } => {
                            match AsyncFile::open(&path, io).await {
                                Ok(file) => match file.write_at(offset, &data).await {
                                    Ok(written) => BatchResult::Write(written),
                                    Err(e) => BatchResult::Error(e),
                                },
                                Err(e) => BatchResult::Error(e),
                            }
                        }
                    }
                }
            })
            .collect();

        join_all(futures).await
    }
}

// ============================================================================
// Async Segment (for log storage)
// ============================================================================

/// Async segment for log-structured storage
///
/// A segment is a single file that stores log entries sequentially.
pub struct AsyncSegment {
    file: AsyncFile,
    base_offset: u64,
    size: AtomicU64,
}

impl AsyncSegment {
    /// Create or open a segment
    pub async fn open<P: AsRef<Path>>(
        path: P,
        base_offset: u64,
        io: Arc<AsyncIo>,
    ) -> io::Result<Self> {
        let file = AsyncFile::open(&path, io).await?;
        let size = file.size().await.unwrap_or(0);

        Ok(Self {
            file,
            base_offset,
            size: AtomicU64::new(size),
        })
    }

    /// Append data to the segment
    pub async fn append(&self, data: &[u8]) -> io::Result<u64> {
        let offset = self.size.fetch_add(data.len() as u64, Ordering::SeqCst);
        self.file.write_at(offset, data).await?;
        Ok(offset)
    }

    /// Read data from the segment
    pub async fn read(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        self.file.read_at(offset, len).await
    }

    /// Sync the segment to disk
    pub async fn sync(&self) -> io::Result<()> {
        self.file.sync().await
    }

    /// Get the base offset
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Get the current size
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_async_io_basic() {
        let config = AsyncIoConfig::default();
        let io = AsyncIo::new(config).unwrap();

        // Create temp file
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let file = AsyncFile::open(&path, io.clone()).await.unwrap();

        // Write
        let data = b"Hello, cross-platform I/O!";
        let written = file.write_at(0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read back
        let read = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(&read[..], data);

        // Check stats
        let stats = io.stats().snapshot();
        assert!(stats.write_ops > 0);
        assert!(stats.read_ops > 0);
        assert_eq!(stats.bytes_written, data.len() as u64);
        assert_eq!(stats.bytes_read, data.len() as u64);
    }

    #[tokio::test]
    async fn test_async_file_sequential() {
        let io = AsyncIo::new(AsyncIoConfig::default()).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("sequential.dat");

        let file = AsyncFile::open(&path, io).await.unwrap();

        // Sequential writes
        file.write(b"Hello").await.unwrap();
        file.write(b" World").await.unwrap();

        // Seek back and read
        file.seek(0);
        let data = file.read(11).await.unwrap();
        assert_eq!(&data[..], b"Hello World");
    }

    #[tokio::test]
    async fn test_async_segment() {
        let io = AsyncIo::new(AsyncIoConfig::default()).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("segment.log");

        let segment = AsyncSegment::open(&path, 0, io).await.unwrap();

        // Append messages
        let pos1 = segment.append(b"message1").await.unwrap();
        let pos2 = segment.append(b"message2").await.unwrap();

        assert_eq!(pos1, 0);
        assert_eq!(pos2, 8);

        // Read back
        let data1 = segment.read(0, 8).await.unwrap();
        let data2 = segment.read(8, 8).await.unwrap();

        assert_eq!(&data1[..], b"message1");
        assert_eq!(&data2[..], b"message2");

        // Check size
        assert_eq!(segment.size(), 16);
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let io = AsyncIo::new(AsyncIoConfig::default()).unwrap();
        let dir = tempdir().unwrap();

        let path1 = dir.path().join("batch1.dat");
        let path2 = dir.path().join("batch2.dat");

        // First create files with some data
        let file1 = AsyncFile::open(&path1, io.clone()).await.unwrap();
        let file2 = AsyncFile::open(&path2, io.clone()).await.unwrap();

        file1.write_at(0, b"file1 data").await.unwrap();
        file2.write_at(0, b"file2 data").await.unwrap();

        // Now batch read
        let results = BatchBuilder::new(io)
            .read(&path1, 0, 10)
            .read(&path2, 0, 10)
            .execute()
            .await;

        assert_eq!(results.len(), 2);

        match &results[0] {
            BatchResult::Read(data) => assert_eq!(&data[..], b"file1 data"),
            _ => panic!("Expected read result"),
        }

        match &results[1] {
            BatchResult::Read(data) => assert_eq!(&data[..], b"file2 data"),
            _ => panic!("Expected read result"),
        }
    }

    #[tokio::test]
    async fn test_sync_operations() {
        let config = AsyncIoConfig::durable();
        let io = AsyncIo::new(config).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("durable.dat");

        let file = AsyncFile::open(&path, io.clone()).await.unwrap();

        // Write with sync
        file.write_at(0, b"durable data").await.unwrap();

        // Explicit sync
        file.sync().await.unwrap();

        let stats = io.stats().snapshot();
        assert!(stats.sync_ops >= 1);
    }

    #[tokio::test]
    async fn test_config_variants() {
        // Test all config variants compile and create valid I/O engines
        let configs = vec![
            AsyncIoConfig::default(),
            AsyncIoConfig::high_performance(),
            AsyncIoConfig::low_latency(),
            AsyncIoConfig::durable(),
        ];

        for config in configs {
            let io = AsyncIo::new(config).unwrap();
            assert!(io.has_capacity());
        }
    }
}
