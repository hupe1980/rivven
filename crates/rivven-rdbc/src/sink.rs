//! Table sink for writing data to databases
//!
//! Provides:
//! - TableSink: High-performance batch writes
//! - Upsert/insert/delete modes
//! - Batching with configurable flush strategies
//! - Dead letter queue for failed records

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::error::Result;
use crate::schema::{AutoDdlMode, SchemaEvolutionMode};
use crate::types::Value;

/// Write mode for sink operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Insert only (fails on duplicate keys)
    Insert,
    /// Update only (fails if row doesn't exist)
    Update,
    /// Upsert (insert or update on conflict)
    #[default]
    Upsert,
    /// Delete records
    Delete,
}

/// Batch configuration for sink writes
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size (records)
    pub max_size: usize,
    /// Maximum batch latency before flush
    pub max_latency: Duration,
    /// Maximum batch memory size (bytes, approximate)
    pub max_bytes: usize,
    /// Whether to use ordered (transactional) batches
    pub ordered: bool,
    /// Number of retries for failed batches
    pub max_retries: u32,
    /// Retry backoff base (exponential)
    pub retry_backoff: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_latency: Duration::from_millis(100),
            max_bytes: 16 * 1024 * 1024, // 16MB
            ordered: true,
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

impl BatchConfig {
    /// Create batch config with specified size
    pub fn with_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Create batch config with specified latency
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.max_latency = latency;
        self
    }

    /// Set max bytes
    pub fn with_max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes;
        self
    }

    /// Set ordered mode
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Set retry config
    pub fn with_retries(mut self, max_retries: u32, backoff: Duration) -> Self {
        self.max_retries = max_retries;
        self.retry_backoff = backoff;
        self
    }
}

/// A record to write to the sink
#[derive(Debug, Clone)]
pub struct SinkRecord {
    /// Target schema
    pub schema: Option<String>,
    /// Target table
    pub table: String,
    /// Record key (primary key values)
    pub key: Vec<Value>,
    /// Record values (all columns including key)
    pub values: HashMap<String, Value>,
    /// Write mode for this record
    pub mode: WriteMode,
    /// Original offset (for tracking)
    pub offset: Option<u64>,
}

impl SinkRecord {
    /// Create a new sink record for upsert
    pub fn upsert(
        schema: Option<String>,
        table: impl Into<String>,
        key: Vec<Value>,
        values: HashMap<String, Value>,
    ) -> Self {
        Self {
            schema,
            table: table.into(),
            key,
            values,
            mode: WriteMode::Upsert,
            offset: None,
        }
    }

    /// Create a new sink record for delete
    pub fn delete(schema: Option<String>, table: impl Into<String>, key: Vec<Value>) -> Self {
        Self {
            schema,
            table: table.into(),
            key,
            values: HashMap::new(),
            mode: WriteMode::Delete,
            offset: None,
        }
    }

    /// Set the offset
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Result of a batch write
#[derive(Debug, Clone, Default)]
pub struct BatchResult {
    /// Number of records written successfully
    pub success_count: u64,
    /// Number of records that failed
    pub failure_count: u64,
    /// Failed records (for dead letter queue)
    pub failed_records: Vec<FailedRecord>,
    /// Duration of the batch operation
    pub duration: Duration,
}

impl BatchResult {
    /// Check if all records succeeded
    pub fn is_success(&self) -> bool {
        self.failure_count == 0
    }
}

/// A record that failed to write
#[derive(Debug, Clone)]
pub struct FailedRecord {
    /// The original record
    pub record: SinkRecord,
    /// The error message
    pub error: String,
    /// Number of retry attempts
    pub attempts: u32,
    /// Whether the error is retriable
    pub retriable: bool,
}

/// Sink statistics
#[derive(Debug, Clone, Default)]
pub struct SinkStats {
    /// Total records written
    pub records_written: u64,
    /// Total records failed
    pub records_failed: u64,
    /// Total batches written
    pub batches_written: u64,
    /// Total batches failed
    pub batches_failed: u64,
    /// Total write duration (milliseconds)
    pub total_write_time_ms: u64,
    /// Average records per second
    pub records_per_second: f64,
}

/// Atomic sink statistics
#[derive(Debug, Default)]
#[allow(missing_docs)]
pub struct AtomicSinkStats {
    pub records_written: AtomicU64,
    pub records_failed: AtomicU64,
    pub batches_written: AtomicU64,
    pub batches_failed: AtomicU64,
    pub total_write_time_ms: AtomicU64,
}

impl AtomicSinkStats {
    /// Record a successful batch
    pub fn record_batch(&self, records: u64, duration: Duration) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.batches_written.fetch_add(1, Ordering::Relaxed);
        self.total_write_time_ms
            .fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Record a failed batch
    pub fn record_batch_failure(&self, failed_records: u64) {
        self.records_failed
            .fetch_add(failed_records, Ordering::Relaxed);
        self.batches_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot
    pub fn snapshot(&self) -> SinkStats {
        let records = self.records_written.load(Ordering::Relaxed);
        let time_ms = self.total_write_time_ms.load(Ordering::Relaxed);
        let rps = if time_ms > 0 {
            (records as f64 * 1000.0) / time_ms as f64
        } else {
            0.0
        };

        SinkStats {
            records_written: records,
            records_failed: self.records_failed.load(Ordering::Relaxed),
            batches_written: self.batches_written.load(Ordering::Relaxed),
            batches_failed: self.batches_failed.load(Ordering::Relaxed),
            total_write_time_ms: time_ms,
            records_per_second: rps,
        }
    }
}

/// Table sink for writing records to a database
#[async_trait]
pub trait TableSink: Send + Sync {
    /// Write a single record
    async fn write(&self, record: SinkRecord) -> Result<()>;

    /// Write a batch of records
    async fn write_batch(&self, records: Vec<SinkRecord>) -> Result<BatchResult>;

    /// Flush any buffered records
    async fn flush(&self) -> Result<()>;

    /// Close the sink
    async fn close(&self) -> Result<()>;

    /// Get sink statistics
    fn stats(&self) -> SinkStats;
}

/// Configuration for table sink
#[derive(Debug, Clone)]
pub struct TableSinkConfig {
    /// Batch configuration
    pub batch: BatchConfig,
    /// Write mode
    pub write_mode: WriteMode,
    /// Auto-DDL mode
    pub auto_ddl: AutoDdlMode,
    /// Schema evolution mode
    pub schema_evolution: SchemaEvolutionMode,
    /// Delete handling mode
    pub delete_enabled: bool,
    /// Primary key columns (if not auto-detected)
    pub pk_columns: Option<Vec<String>>,
    /// Columns to include (None = all)
    pub include_columns: Option<Vec<String>>,
    /// Columns to exclude
    pub exclude_columns: Vec<String>,
}

impl Default for TableSinkConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            write_mode: WriteMode::Upsert,
            auto_ddl: AutoDdlMode::None,
            schema_evolution: SchemaEvolutionMode::AddColumnsOnly,
            delete_enabled: true,
            pk_columns: None,
            include_columns: None,
            exclude_columns: Vec::new(),
        }
    }
}

/// Builder for table sink
pub struct TableSinkBuilder {
    config: TableSinkConfig,
}

impl TableSinkBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: TableSinkConfig::default(),
        }
    }

    /// Set batch size
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch.max_size = size;
        self
    }

    /// Set batch latency
    pub fn batch_latency(mut self, latency: Duration) -> Self {
        self.config.batch.max_latency = latency;
        self
    }

    /// Set write mode
    pub fn write_mode(mut self, mode: WriteMode) -> Self {
        self.config.write_mode = mode;
        self
    }

    /// Set auto-DDL mode
    pub fn auto_ddl(mut self, mode: AutoDdlMode) -> Self {
        self.config.auto_ddl = mode;
        self
    }

    /// Set schema evolution mode
    pub fn schema_evolution(mut self, mode: SchemaEvolutionMode) -> Self {
        self.config.schema_evolution = mode;
        self
    }

    /// Enable/disable delete handling
    pub fn delete_enabled(mut self, enabled: bool) -> Self {
        self.config.delete_enabled = enabled;
        self
    }

    /// Set primary key columns
    pub fn pk_columns(mut self, columns: Vec<String>) -> Self {
        self.config.pk_columns = Some(columns);
        self
    }

    /// Set included columns
    pub fn include_columns(mut self, columns: Vec<String>) -> Self {
        self.config.include_columns = Some(columns);
        self
    }

    /// Set excluded columns
    pub fn exclude_columns(mut self, columns: Vec<String>) -> Self {
        self.config.exclude_columns = columns;
        self
    }

    /// Build the configuration
    pub fn build(self) -> TableSinkConfig {
        self.config
    }
}

impl Default for TableSinkBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple buffering sink wrapper
pub struct BufferedSink<S: TableSink> {
    inner: S,
    buffer: Mutex<Vec<SinkRecord>>,
    config: BatchConfig,
    last_flush: Mutex<Instant>,
    stats: AtomicSinkStats,
}

impl<S: TableSink> BufferedSink<S> {
    /// Create a new buffered sink
    pub fn new(inner: S, config: BatchConfig) -> Self {
        Self {
            inner,
            buffer: Mutex::new(Vec::new()),
            config,
            last_flush: Mutex::new(Instant::now()),
            stats: AtomicSinkStats::default(),
        }
    }

    /// Check if buffer should be flushed
    async fn should_flush(&self) -> bool {
        let buffer = self.buffer.lock().await;
        if buffer.len() >= self.config.max_size {
            return true;
        }

        let last_flush = self.last_flush.lock().await;
        if !buffer.is_empty() && last_flush.elapsed() >= self.config.max_latency {
            return true;
        }

        false
    }

    /// Get inner sink
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

#[async_trait]
impl<S: TableSink + 'static> TableSink for BufferedSink<S> {
    async fn write(&self, record: SinkRecord) -> Result<()> {
        {
            let mut buffer = self.buffer.lock().await;
            buffer.push(record);
        }

        if self.should_flush().await {
            self.flush().await?;
        }

        Ok(())
    }

    async fn write_batch(&self, records: Vec<SinkRecord>) -> Result<BatchResult> {
        let start = Instant::now();
        let _count = records.len() as u64;

        // For batch writes, bypass buffer and write directly
        let result = self.inner.write_batch(records).await?;

        self.stats
            .record_batch(result.success_count, start.elapsed());
        if result.failure_count > 0 {
            self.stats.record_batch_failure(result.failure_count);
        }

        Ok(result)
    }

    async fn flush(&self) -> Result<()> {
        let records: Vec<_> = {
            let mut buffer = self.buffer.lock().await;
            std::mem::take(&mut *buffer)
        };

        if !records.is_empty() {
            let start = Instant::now();
            let _count = records.len() as u64;

            let result = self.inner.write_batch(records).await?;

            self.stats
                .record_batch(result.success_count, start.elapsed());
            if result.failure_count > 0 {
                self.stats.record_batch_failure(result.failure_count);
            }
        }

        *self.last_flush.lock().await = Instant::now();
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.flush().await?;
        self.inner.close().await
    }

    fn stats(&self) -> SinkStats {
        self.stats.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_builder() {
        let config = BatchConfig::default()
            .with_size(500)
            .with_latency(Duration::from_millis(50))
            .ordered(false)
            .with_retries(5, Duration::from_millis(200));

        assert_eq!(config.max_size, 500);
        assert_eq!(config.max_latency, Duration::from_millis(50));
        assert!(!config.ordered);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_sink_record_creation() {
        let mut values = HashMap::new();
        values.insert("id".into(), Value::Int32(1));
        values.insert("name".into(), Value::String("test".into()));

        let record = SinkRecord::upsert(
            Some("public".into()),
            "users",
            vec![Value::Int32(1)],
            values,
        )
        .with_offset(100);

        assert_eq!(record.table, "users");
        assert_eq!(record.schema, Some("public".into()));
        assert_eq!(record.mode, WriteMode::Upsert);
        assert_eq!(record.offset, Some(100));
    }

    #[test]
    fn test_sink_record_delete() {
        let record = SinkRecord::delete(None, "users", vec![Value::Int32(1)]);

        assert_eq!(record.mode, WriteMode::Delete);
        assert!(record.values.is_empty());
    }

    #[test]
    fn test_batch_result() {
        let result = BatchResult {
            success_count: 100,
            failure_count: 0,
            failed_records: vec![],
            duration: Duration::from_millis(50),
        };

        assert!(result.is_success());

        let result = BatchResult {
            failure_count: 5,
            ..result
        };

        assert!(!result.is_success());
    }

    #[test]
    fn test_atomic_sink_stats() {
        let stats = AtomicSinkStats::default();

        stats.record_batch(100, Duration::from_millis(200));
        stats.record_batch(50, Duration::from_millis(100));
        stats.record_batch_failure(5);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.records_written, 150);
        assert_eq!(snapshot.batches_written, 2);
        assert_eq!(snapshot.records_failed, 5);
        assert_eq!(snapshot.batches_failed, 1);
        assert_eq!(snapshot.total_write_time_ms, 300);
        assert!(snapshot.records_per_second > 0.0);
    }

    #[test]
    fn test_table_sink_builder() {
        let config = TableSinkBuilder::new()
            .batch_size(500)
            .batch_latency(Duration::from_millis(50))
            .write_mode(WriteMode::Insert)
            .auto_ddl(AutoDdlMode::Create)
            .delete_enabled(false)
            .pk_columns(vec!["id".into()])
            .build();

        assert_eq!(config.batch.max_size, 500);
        assert_eq!(config.write_mode, WriteMode::Insert);
        assert_eq!(config.auto_ddl, AutoDdlMode::Create);
        assert!(!config.delete_enabled);
        assert_eq!(config.pk_columns, Some(vec!["id".into()]));
    }
}
