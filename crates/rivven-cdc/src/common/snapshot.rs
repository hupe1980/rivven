//! # CDC Snapshot Support
//!
//! Initial table synchronization before streaming changes.
//!
//! ## Features
//!
//! - Multiple snapshot modes
//! - Chunked reads with configurable batch size
//! - Progress tracking and resumability
//! - Watermark-based consistency
//! - Parallel table snapshots
//! - Memory-efficient streaming
//!
//! ## Snapshot Modes
//!
//! | Mode | Description |
//! |------|-------------|
//! | `Initial` | Snapshot on first start, then stream (default) |
//! | `Always` | Snapshot on every start |
//! | `InitialOnly` | Snapshot and stop (no streaming) |
//! | `SchemaOnly` | Capture schema, skip data |
//! | `WhenNeeded` | Snapshot if offsets unavailable |
//! | `Recovery` | Rebuild schema from source |
//! | `Custom` | User-defined snapshot logic |
//!
//! ## Example
//!
//! ```rust,ignore
//! use rivven_cdc::common::snapshot::{SnapshotConfig, SnapshotCoordinator, SnapshotMode};
//!
//! let config = SnapshotConfig::builder()
//!     .mode(SnapshotMode::Initial)
//!     .batch_size(10_000)
//!     .parallel_tables(4)
//!     .build();
//!
//! let coordinator = SnapshotCoordinator::new(config, progress_store);
//! let mut stream = coordinator.snapshot_table("users", "id").await?;
//!
//! while let Some(batch) = stream.next().await {
//!     process_batch(batch?).await;
//! }
//! ```

use crate::common::{CdcError, CdcEvent, CdcOp, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Snapshot mode determines when and how snapshots are taken.
///
/// # Examples
///
/// ```rust
/// use rivven_cdc::common::SnapshotMode;
///
/// // Default mode: snapshot on first start
/// let mode = SnapshotMode::Initial;
///
/// // Always snapshot on start (useful for debugging)
/// let mode = SnapshotMode::Always;
///
/// // Custom snapshot logic with user-defined function
/// let mode = SnapshotMode::Custom("my_snapshotter".to_string());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotMode {
    /// Perform a snapshot every time the connector starts.
    ///
    /// After the snapshot completes, the connector begins streaming changes.
    /// Useful when WAL segments may have been deleted.
    Always,

    /// Perform a snapshot only on first start (when no offsets exist).
    ///
    /// This is the default and most common mode. After the initial snapshot
    /// completes, the connector streams changes and never snapshots again
    /// unless offsets are lost.
    #[default]
    Initial,

    /// Perform a snapshot and then stop (no streaming).
    ///
    /// Useful for one-time data migration or backfill scenarios.
    InitialOnly,

    /// Capture schema only, skip data snapshot.
    ///
    /// The connector captures table structures but does not snapshot data.
    /// Only changes occurring after connector start are captured.
    SchemaOnly,

    /// Perform a snapshot only if offsets are unavailable.
    ///
    /// The connector checks for existing offsets:
    /// - If offsets exist: resume streaming from stored position
    /// - If no offsets: perform a full snapshot first
    WhenNeeded,

    /// Recovery mode for corrupted schema history.
    ///
    /// Rebuilds the schema from source tables. Use after schema history
    /// topic corruption or when adding new tables to capture.
    ///
    /// **Warning**: Do not use if schema changes occurred after last shutdown.
    Recovery,

    /// Configuration-based snapshot control.
    ///
    /// Fine-grained control via additional parameters:
    /// - `snapshot_data`: include table data
    /// - `snapshot_schema`: include table schema  
    /// - `start_stream`: begin streaming after snapshot
    ConfigurationBased {
        /// Include table data in snapshot
        snapshot_data: bool,
        /// Include table schema in snapshot
        snapshot_schema: bool,
        /// Start streaming after snapshot completes
        start_stream: bool,
        /// Snapshot if schema history unavailable
        snapshot_on_schema_error: bool,
        /// Snapshot if offsets not found in log
        snapshot_on_data_error: bool,
    },

    /// Custom snapshot implementation.
    ///
    /// Provide a custom snapshotter name that implements the
    /// snapshot logic. The name is used to look up the implementation.
    Custom(String),

    /// Never perform a snapshot.
    ///
    /// The connector only streams changes. If no offsets exist,
    /// streaming begins from the current position.
    ///
    /// **Warning**: May miss historical data. Use only when certain
    /// all needed data is still in the transaction log.
    #[serde(alias = "never")]
    NoSnapshot,
}

impl SnapshotMode {
    /// Check if this mode includes a data snapshot.
    pub fn includes_data(&self) -> bool {
        match self {
            Self::Always | Self::Initial | Self::InitialOnly | Self::WhenNeeded => true,
            Self::SchemaOnly | Self::Recovery | Self::NoSnapshot => false,
            Self::ConfigurationBased { snapshot_data, .. } => *snapshot_data,
            Self::Custom(_) => true, // Assume custom may include data
        }
    }

    /// Check if streaming should occur after snapshot.
    pub fn should_stream(&self) -> bool {
        match self {
            Self::Always | Self::Initial | Self::WhenNeeded | Self::SchemaOnly | Self::Recovery => {
                true
            }
            Self::InitialOnly | Self::NoSnapshot => false,
            Self::ConfigurationBased { start_stream, .. } => *start_stream,
            Self::Custom(_) => true, // Assume custom may stream
        }
    }

    /// Check if schema should be captured.
    pub fn includes_schema(&self) -> bool {
        match self {
            Self::NoSnapshot => false,
            Self::ConfigurationBased {
                snapshot_schema, ..
            } => *snapshot_schema,
            _ => true,
        }
    }

    /// Parse snapshot mode from string representation.
    pub fn from_str_value(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "always" => Some(Self::Always),
            "initial" => Some(Self::Initial),
            "initial_only" => Some(Self::InitialOnly),
            "schema_only" | "no_data" => Some(Self::SchemaOnly),
            "when_needed" => Some(Self::WhenNeeded),
            "recovery" | "schema_only_recovery" => Some(Self::Recovery),
            "never" => Some(Self::NoSnapshot),
            "configuration_based" => Some(Self::ConfigurationBased {
                snapshot_data: false,
                snapshot_schema: false,
                start_stream: false,
                snapshot_on_schema_error: false,
                snapshot_on_data_error: false,
            }),
            _ if s.starts_with("custom:") => Some(Self::Custom(s[7..].to_string())),
            _ => None,
        }
    }
}

/// Snapshot state for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SnapshotState {
    /// Not started
    #[default]
    Pending,
    /// Currently running
    InProgress,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed,
    /// Paused (can resume)
    Paused,
}

/// Progress information for a table snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotProgress {
    /// Table being snapshotted
    pub table: String,
    /// Schema name
    pub schema: String,
    /// Current state
    pub state: SnapshotState,
    /// Rows processed so far
    pub rows_processed: u64,
    /// Estimated total rows (if known)
    pub total_rows: Option<u64>,
    /// Last processed key (for resumption)
    pub last_key: Option<String>,
    /// Start timestamp
    pub started_at: Option<i64>,
    /// Completion timestamp
    pub completed_at: Option<i64>,
    /// Error message if failed
    pub error: Option<String>,
    /// Watermark LSN/position at snapshot start
    pub watermark: Option<String>,
}

impl SnapshotProgress {
    /// Create new progress for a table.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            schema: schema.into(),
            state: SnapshotState::Pending,
            rows_processed: 0,
            total_rows: None,
            last_key: None,
            started_at: None,
            completed_at: None,
            error: None,
            watermark: None,
        }
    }

    /// Start the snapshot.
    pub fn start(&mut self, watermark: Option<String>) {
        self.state = SnapshotState::InProgress;
        self.started_at = Some(chrono::Utc::now().timestamp());
        self.watermark = watermark;
    }

    /// Update progress.
    pub fn update(&mut self, rows: u64, last_key: Option<String>) {
        self.rows_processed += rows;
        if last_key.is_some() {
            self.last_key = last_key;
        }
    }

    /// Mark as completed.
    pub fn complete(&mut self) {
        self.state = SnapshotState::Completed;
        self.completed_at = Some(chrono::Utc::now().timestamp());
    }

    /// Mark as failed.
    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = SnapshotState::Failed;
        self.error = Some(error.into());
        self.completed_at = Some(chrono::Utc::now().timestamp());
    }

    /// Pause the snapshot.
    pub fn pause(&mut self) {
        self.state = SnapshotState::Paused;
    }

    /// Get completion percentage.
    pub fn percentage(&self) -> Option<f64> {
        self.total_rows.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.rows_processed as f64 / total as f64) * 100.0
            }
        })
    }

    /// Check if can resume.
    pub fn can_resume(&self) -> bool {
        matches!(self.state, SnapshotState::Paused | SnapshotState::Failed)
            && self.last_key.is_some()
    }
}

/// Configuration for snapshots.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Snapshot mode (when to snapshot)
    pub mode: SnapshotMode,
    /// Rows per batch
    pub batch_size: usize,
    /// Number of tables to snapshot in parallel
    pub parallel_tables: usize,
    /// Query timeout
    pub query_timeout: Duration,
    /// Progress save interval
    pub progress_interval: Duration,
    /// Use consistent read (snapshot isolation)
    pub consistent_read: bool,
    /// Include row count estimate
    pub estimate_rows: bool,
    /// Maximum retries per batch
    pub max_retries: u32,
    /// Throttle delay between batches
    pub throttle_delay: Option<Duration>,
    /// Delay before starting snapshot
    pub snapshot_delay: Option<Duration>,
    /// Delay before streaming after snapshot
    pub streaming_delay: Option<Duration>,
    /// Lock timeout for acquiring table locks
    pub lock_timeout: Option<Duration>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            batch_size: 10_000,
            parallel_tables: 4,
            query_timeout: Duration::from_secs(300),
            progress_interval: Duration::from_secs(10),
            consistent_read: true,
            estimate_rows: true,
            max_retries: 3,
            throttle_delay: None,
            snapshot_delay: None,
            streaming_delay: None,
            lock_timeout: Some(Duration::from_secs(10)),
        }
    }
}

impl SnapshotConfig {
    pub fn builder() -> SnapshotConfigBuilder {
        SnapshotConfigBuilder::default()
    }

    /// High-throughput preset.
    pub fn high_throughput() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            batch_size: 50_000,
            parallel_tables: 8,
            query_timeout: Duration::from_secs(600),
            progress_interval: Duration::from_secs(30),
            consistent_read: false,
            estimate_rows: false,
            max_retries: 5,
            throttle_delay: None,
            snapshot_delay: None,
            streaming_delay: None,
            lock_timeout: None,
        }
    }

    /// Memory-efficient preset.
    pub fn low_memory() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            batch_size: 1_000,
            parallel_tables: 2,
            query_timeout: Duration::from_secs(120),
            progress_interval: Duration::from_secs(5),
            consistent_read: true,
            estimate_rows: true,
            max_retries: 3,
            throttle_delay: Some(Duration::from_millis(100)),
            snapshot_delay: None,
            streaming_delay: None,
            lock_timeout: Some(Duration::from_secs(10)),
        }
    }

    /// Check if a snapshot should be performed.
    pub fn should_snapshot(&self, has_offsets: bool) -> bool {
        match self.mode {
            SnapshotMode::Always => true,
            SnapshotMode::Initial => !has_offsets,
            SnapshotMode::InitialOnly => !has_offsets,
            SnapshotMode::SchemaOnly => !has_offsets,
            SnapshotMode::WhenNeeded => !has_offsets,
            SnapshotMode::Recovery => true,
            SnapshotMode::ConfigurationBased { snapshot_data, .. } => snapshot_data && !has_offsets,
            SnapshotMode::Custom(_) => true, // Custom decides
            SnapshotMode::NoSnapshot => false,
        }
    }
}

/// Builder for SnapshotConfig.
#[derive(Default)]
pub struct SnapshotConfigBuilder {
    config: SnapshotConfig,
}

impl SnapshotConfigBuilder {
    /// Set snapshot mode.
    pub fn mode(mut self, mode: SnapshotMode) -> Self {
        self.config.mode = mode;
        self
    }
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size.max(1);
        self
    }

    pub fn parallel_tables(mut self, count: usize) -> Self {
        self.config.parallel_tables = count.max(1);
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.config.query_timeout = timeout;
        self
    }

    pub fn progress_interval(mut self, interval: Duration) -> Self {
        self.config.progress_interval = interval;
        self
    }

    pub fn consistent_read(mut self, enabled: bool) -> Self {
        self.config.consistent_read = enabled;
        self
    }

    pub fn estimate_rows(mut self, enabled: bool) -> Self {
        self.config.estimate_rows = enabled;
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    pub fn throttle_delay(mut self, delay: Duration) -> Self {
        self.config.throttle_delay = Some(delay);
        self
    }

    pub fn build(self) -> SnapshotConfig {
        self.config
    }
}

/// A batch of snapshot rows.
#[derive(Debug, Clone)]
pub struct SnapshotBatch {
    /// Events in this batch
    pub events: Vec<CdcEvent>,
    /// Batch sequence number
    pub sequence: u64,
    /// Is this the last batch?
    pub is_last: bool,
    /// Last key in batch (for pagination)
    pub last_key: Option<String>,
}

impl SnapshotBatch {
    /// Create a new batch.
    pub fn new(events: Vec<CdcEvent>, sequence: u64) -> Self {
        Self {
            events,
            sequence,
            is_last: false,
            last_key: None,
        }
    }

    /// Mark as last batch.
    pub fn mark_last(mut self) -> Self {
        self.is_last = true;
        self
    }

    /// Set last key for pagination.
    pub fn with_last_key(mut self, key: impl Into<String>) -> Self {
        self.last_key = Some(key.into());
        self
    }

    /// Get number of events.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Trait for snapshot progress persistence.
#[async_trait::async_trait]
pub trait ProgressStore: Send + Sync {
    /// Save progress for a table.
    async fn save(&self, progress: &SnapshotProgress) -> Result<()>;

    /// Load progress for a table.
    async fn load(&self, schema: &str, table: &str) -> Result<Option<SnapshotProgress>>;

    /// Delete progress for a table.
    async fn delete(&self, schema: &str, table: &str) -> Result<()>;

    /// List all progress entries.
    async fn list(&self) -> Result<Vec<SnapshotProgress>>;
}

/// In-memory progress store for testing.
#[derive(Default)]
pub struct MemoryProgressStore {
    progress: RwLock<HashMap<String, SnapshotProgress>>,
}

impl MemoryProgressStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn key(schema: &str, table: &str) -> String {
        format!("{}.{}", schema, table)
    }
}

#[async_trait::async_trait]
impl ProgressStore for MemoryProgressStore {
    async fn save(&self, progress: &SnapshotProgress) -> Result<()> {
        let key = Self::key(&progress.schema, &progress.table);
        self.progress.write().await.insert(key, progress.clone());
        Ok(())
    }

    async fn load(&self, schema: &str, table: &str) -> Result<Option<SnapshotProgress>> {
        let key = Self::key(schema, table);
        Ok(self.progress.read().await.get(&key).cloned())
    }

    async fn delete(&self, schema: &str, table: &str) -> Result<()> {
        let key = Self::key(schema, table);
        self.progress.write().await.remove(&key);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SnapshotProgress>> {
        Ok(self.progress.read().await.values().cloned().collect())
    }
}

/// Trait for snapshot data source.
#[async_trait::async_trait]
pub trait SnapshotSource: Send + Sync {
    /// Get current position/watermark.
    async fn get_watermark(&self) -> Result<String>;

    /// Estimate row count for a table.
    async fn estimate_row_count(&self, schema: &str, table: &str) -> Result<Option<u64>>;

    /// Fetch a batch of rows.
    async fn fetch_batch(
        &self,
        schema: &str,
        table: &str,
        key_column: &str,
        last_key: Option<&str>,
        batch_size: usize,
    ) -> Result<SnapshotBatch>;
}

/// Snapshot statistics.
#[derive(Debug, Default)]
pub struct SnapshotStats {
    tables_completed: AtomicU64,
    tables_failed: AtomicU64,
    rows_processed: AtomicU64,
    batches_processed: AtomicU64,
    bytes_processed: AtomicU64,
}

impl SnapshotStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_rows(&self, count: u64) {
        self.rows_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_batch(&self) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_table_completed(&self) {
        self.tables_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_table_failed(&self) {
        self.tables_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> SnapshotStatsSnapshot {
        SnapshotStatsSnapshot {
            tables_completed: self.tables_completed.load(Ordering::Relaxed),
            tables_failed: self.tables_failed.load(Ordering::Relaxed),
            rows_processed: self.rows_processed.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            bytes_processed: self.bytes_processed.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot for stats.
#[derive(Debug, Clone)]
pub struct SnapshotStatsSnapshot {
    pub tables_completed: u64,
    pub tables_failed: u64,
    pub rows_processed: u64,
    pub batches_processed: u64,
    pub bytes_processed: u64,
}

/// Table specification for snapshot.
#[derive(Debug, Clone)]
pub struct TableSpec {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Primary key column for pagination
    pub key_column: String,
    /// Optional WHERE clause
    pub filter: Option<String>,
}

impl TableSpec {
    pub fn new(
        schema: impl Into<String>,
        table: impl Into<String>,
        key_column: impl Into<String>,
    ) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
            key_column: key_column.into(),
            filter: None,
        }
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

/// Coordinator for multi-table snapshots.
pub struct SnapshotCoordinator<S: SnapshotSource, P: ProgressStore> {
    config: SnapshotConfig,
    source: Arc<S>,
    progress_store: Arc<P>,
    stats: Arc<SnapshotStats>,
    cancelled: AtomicBool,
}

impl<S: SnapshotSource, P: ProgressStore> SnapshotCoordinator<S, P> {
    /// Create a new coordinator.
    pub fn new(config: SnapshotConfig, source: S, progress_store: P) -> Self {
        Self {
            config,
            source: Arc::new(source),
            progress_store: Arc::new(progress_store),
            stats: Arc::new(SnapshotStats::new()),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Cancel all running snapshots.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Get statistics.
    pub fn stats(&self) -> SnapshotStatsSnapshot {
        self.stats.snapshot()
    }

    /// Snapshot a single table.
    pub async fn snapshot_table(&self, spec: &TableSpec) -> Result<Vec<SnapshotBatch>> {
        let mut batches = Vec::new();
        let mut progress = self
            .progress_store
            .load(&spec.schema, &spec.table)
            .await?
            .unwrap_or_else(|| SnapshotProgress::new(&spec.schema, &spec.table));

        // Get watermark
        let watermark = self.source.get_watermark().await?;

        // Start or resume
        if progress.state == SnapshotState::Pending {
            progress.start(Some(watermark));

            // Estimate rows if configured
            if self.config.estimate_rows {
                progress.total_rows = self
                    .source
                    .estimate_row_count(&spec.schema, &spec.table)
                    .await?;
            }
        }

        let mut last_key = progress.last_key.clone();
        let mut sequence = 0u64;
        let mut last_save = Instant::now();

        loop {
            if self.is_cancelled() {
                progress.pause();
                self.progress_store.save(&progress).await?;
                return Err(CdcError::replication("Snapshot cancelled"));
            }

            // Fetch batch with retry
            let batch = self.fetch_with_retry(spec, last_key.as_deref()).await?;

            let batch_len = batch.len() as u64;
            let is_last = batch.is_last || batch.is_empty();

            if !batch.is_empty() {
                last_key = batch.last_key.clone();
                progress.update(batch_len, last_key.clone());
                self.stats.record_rows(batch_len);
                self.stats.record_batch();

                batches.push(SnapshotBatch {
                    events: batch.events,
                    sequence,
                    is_last,
                    last_key: last_key.clone(),
                });
                sequence += 1;
            }

            // Save progress periodically
            if last_save.elapsed() >= self.config.progress_interval {
                self.progress_store.save(&progress).await?;
                last_save = Instant::now();
            }

            if is_last {
                break;
            }

            // Throttle if configured
            if let Some(delay) = self.config.throttle_delay {
                tokio::time::sleep(delay).await;
            }
        }

        // Mark complete
        progress.complete();
        self.progress_store.save(&progress).await?;
        self.stats.record_table_completed();

        Ok(batches)
    }

    /// Fetch batch with retry.
    async fn fetch_with_retry(
        &self,
        spec: &TableSpec,
        last_key: Option<&str>,
    ) -> Result<SnapshotBatch> {
        let mut attempts = 0;
        loop {
            match self
                .source
                .fetch_batch(
                    &spec.schema,
                    &spec.table,
                    &spec.key_column,
                    last_key,
                    self.config.batch_size,
                )
                .await
            {
                Ok(batch) => return Ok(batch),
                Err(e) => {
                    attempts += 1;
                    if attempts >= self.config.max_retries {
                        return Err(e);
                    }
                    let delay = Duration::from_millis(100 * 2u64.pow(attempts));
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Snapshot multiple tables.
    pub async fn snapshot_tables(
        &self,
        specs: Vec<TableSpec>,
    ) -> Result<HashMap<String, Vec<SnapshotBatch>>> {
        let mut results = HashMap::new();

        // Process tables (could be parallelized with semaphore)
        for spec in specs {
            let key = format!("{}.{}", spec.schema, spec.table);
            match self.snapshot_table(&spec).await {
                Ok(batches) => {
                    results.insert(key, batches);
                }
                Err(e) => {
                    self.stats.record_table_failed();
                    // Continue with other tables or fail fast based on config
                    return Err(e);
                }
            }
        }

        Ok(results)
    }

    /// Get progress for all tables.
    pub async fn get_all_progress(&self) -> Result<Vec<SnapshotProgress>> {
        self.progress_store.list().await
    }

    /// Reset progress for a table.
    pub async fn reset_table(&self, schema: &str, table: &str) -> Result<()> {
        self.progress_store.delete(schema, table).await
    }
}

/// Mock snapshot source for testing.
pub struct MockSnapshotSource {
    rows: RwLock<HashMap<String, Vec<serde_json::Value>>>,
}

impl MockSnapshotSource {
    pub fn new() -> Self {
        Self {
            rows: RwLock::new(HashMap::new()),
        }
    }

    /// Add test rows.
    pub async fn add_rows(&self, schema: &str, table: &str, rows: Vec<serde_json::Value>) {
        let key = format!("{}.{}", schema, table);
        self.rows.write().await.insert(key, rows);
    }
}

impl Default for MockSnapshotSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SnapshotSource for MockSnapshotSource {
    async fn get_watermark(&self) -> Result<String> {
        Ok("0/0".to_string())
    }

    async fn estimate_row_count(&self, schema: &str, table: &str) -> Result<Option<u64>> {
        let key = format!("{}.{}", schema, table);
        let rows = self.rows.read().await;
        Ok(rows.get(&key).map(|r| r.len() as u64))
    }

    async fn fetch_batch(
        &self,
        schema: &str,
        table: &str,
        key_column: &str,
        last_key: Option<&str>,
        batch_size: usize,
    ) -> Result<SnapshotBatch> {
        let key = format!("{}.{}", schema, table);
        let rows = self.rows.read().await;

        let table_rows = rows.get(&key).cloned().unwrap_or_default();

        // Find start position
        let start_idx = if let Some(last) = last_key {
            let last_val: i64 = last.parse().unwrap_or(0);
            table_rows
                .iter()
                .position(|r| {
                    r.get(key_column)
                        .and_then(|v| v.as_i64())
                        .map(|v| v > last_val)
                        .unwrap_or(false)
                })
                .unwrap_or(table_rows.len())
        } else {
            0
        };

        let end_idx = (start_idx + batch_size).min(table_rows.len());
        let batch_rows: Vec<_> = table_rows[start_idx..end_idx].to_vec();
        let is_last = end_idx >= table_rows.len();

        let events: Vec<CdcEvent> = batch_rows
            .iter()
            .map(|row| {
                CdcEvent {
                    source_type: "snapshot".to_string(),
                    database: "test".to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                    op: CdcOp::Snapshot, // Snapshot events use Snapshot op
                    before: None,
                    after: Some(row.clone()),
                    timestamp: chrono::Utc::now().timestamp(),
                    transaction: None,
                }
            })
            .collect();

        let last_key = batch_rows
            .last()
            .and_then(|r| r.get(key_column))
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string());

        Ok(SnapshotBatch {
            events,
            sequence: 0,
            is_last,
            last_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_progress_new() {
        let progress = SnapshotProgress::new("public", "users");
        assert_eq!(progress.schema, "public");
        assert_eq!(progress.table, "users");
        assert_eq!(progress.state, SnapshotState::Pending);
        assert_eq!(progress.rows_processed, 0);
    }

    #[test]
    fn test_snapshot_progress_lifecycle() {
        let mut progress = SnapshotProgress::new("public", "users");

        // Start
        progress.start(Some("0/1234".to_string()));
        assert_eq!(progress.state, SnapshotState::InProgress);
        assert!(progress.started_at.is_some());
        assert_eq!(progress.watermark, Some("0/1234".to_string()));

        // Update
        progress.update(100, Some("100".to_string()));
        assert_eq!(progress.rows_processed, 100);
        assert_eq!(progress.last_key, Some("100".to_string()));

        // Complete
        progress.complete();
        assert_eq!(progress.state, SnapshotState::Completed);
        assert!(progress.completed_at.is_some());
    }

    #[test]
    fn test_snapshot_progress_failure() {
        let mut progress = SnapshotProgress::new("public", "users");
        progress.start(None);
        progress.fail("Connection lost");

        assert_eq!(progress.state, SnapshotState::Failed);
        assert_eq!(progress.error, Some("Connection lost".to_string()));
    }

    #[test]
    fn test_snapshot_progress_percentage() {
        let mut progress = SnapshotProgress::new("public", "users");
        progress.total_rows = Some(1000);
        progress.rows_processed = 500;

        assert_eq!(progress.percentage(), Some(50.0));
    }

    #[test]
    fn test_snapshot_progress_can_resume() {
        let mut progress = SnapshotProgress::new("public", "users");
        progress.start(None);
        progress.update(100, Some("100".to_string()));
        progress.pause();

        assert!(progress.can_resume());
    }

    #[test]
    fn test_snapshot_config_defaults() {
        let config = SnapshotConfig::default();
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.parallel_tables, 4);
        assert!(config.consistent_read);
    }

    #[test]
    fn test_snapshot_config_builder() {
        let config = SnapshotConfig::builder()
            .batch_size(5000)
            .parallel_tables(2)
            .consistent_read(false)
            .max_retries(5)
            .build();

        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.parallel_tables, 2);
        assert!(!config.consistent_read);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_snapshot_config_presets() {
        let high = SnapshotConfig::high_throughput();
        assert_eq!(high.batch_size, 50_000);
        assert!(!high.consistent_read);

        let low = SnapshotConfig::low_memory();
        assert_eq!(low.batch_size, 1_000);
        assert!(low.throttle_delay.is_some());
    }

    #[test]
    fn test_snapshot_batch() {
        let events = vec![CdcEvent {
            source_type: "snapshot".to_string(),
            database: "test".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Snapshot,
            before: None,
            after: Some(serde_json::json!({"id": 1})),
            timestamp: 0,
            transaction: None,
        }];

        let batch = SnapshotBatch::new(events, 0).with_last_key("1").mark_last();

        assert_eq!(batch.len(), 1);
        assert!(batch.is_last);
        assert_eq!(batch.last_key, Some("1".to_string()));
    }

    #[test]
    fn test_table_spec() {
        let spec = TableSpec::new("public", "users", "id").with_filter("active = true");

        assert_eq!(spec.schema, "public");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.key_column, "id");
        assert_eq!(spec.filter, Some("active = true".to_string()));
    }

    #[tokio::test]
    async fn test_memory_progress_store() {
        let store = MemoryProgressStore::new();

        let mut progress = SnapshotProgress::new("public", "users");
        progress.start(None);
        progress.update(100, Some("100".to_string()));

        store.save(&progress).await.unwrap();

        let loaded = store.load("public", "users").await.unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.rows_processed, 100);

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 1);

        store.delete("public", "users").await.unwrap();
        let deleted = store.load("public", "users").await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_mock_snapshot_source() {
        let source = MockSnapshotSource::new();

        let rows = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
            serde_json::json!({"id": 3, "name": "Charlie"}),
        ];
        source.add_rows("public", "users", rows).await;

        // First batch
        let batch = source
            .fetch_batch("public", "users", "id", None, 2)
            .await
            .unwrap();
        assert_eq!(batch.events.len(), 2);
        assert!(!batch.is_last);
        assert_eq!(batch.last_key, Some("2".to_string()));

        // Second batch (from last key)
        let batch = source
            .fetch_batch("public", "users", "id", Some("2"), 2)
            .await
            .unwrap();
        assert_eq!(batch.events.len(), 1);
        assert!(batch.is_last);
    }

    #[tokio::test]
    async fn test_mock_snapshot_source_estimate() {
        let source = MockSnapshotSource::new();

        let rows = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        source.add_rows("public", "users", rows).await;

        let count = source.estimate_row_count("public", "users").await.unwrap();
        assert_eq!(count, Some(2));
    }

    #[tokio::test]
    async fn test_snapshot_coordinator_single_table() {
        let source = MockSnapshotSource::new();
        let rows = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];
        source.add_rows("public", "users", rows).await;

        let progress_store = MemoryProgressStore::new();
        let config = SnapshotConfig::builder().batch_size(10).build();

        let coordinator = SnapshotCoordinator::new(config, source, progress_store);
        let spec = TableSpec::new("public", "users", "id");

        let batches = coordinator.snapshot_table(&spec).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].events.len(), 2);
        assert!(batches[0].is_last);
    }

    #[tokio::test]
    async fn test_snapshot_coordinator_stats() {
        let source = MockSnapshotSource::new();
        let rows = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        source.add_rows("public", "users", rows).await;

        let progress_store = MemoryProgressStore::new();
        let config = SnapshotConfig::default();

        let coordinator = SnapshotCoordinator::new(config, source, progress_store);
        let spec = TableSpec::new("public", "users", "id");

        coordinator.snapshot_table(&spec).await.unwrap();

        let stats = coordinator.stats();
        assert_eq!(stats.tables_completed, 1);
        assert_eq!(stats.rows_processed, 2);
        assert_eq!(stats.batches_processed, 1);
    }

    #[tokio::test]
    async fn test_snapshot_coordinator_cancel() {
        let source = MockSnapshotSource::new();
        let rows: Vec<_> = (1..=100).map(|i| serde_json::json!({"id": i})).collect();
        source.add_rows("public", "users", rows).await;

        let progress_store = MemoryProgressStore::new();
        let config = SnapshotConfig::builder().batch_size(10).build();

        let coordinator = SnapshotCoordinator::new(config, source, progress_store);
        coordinator.cancel();

        let spec = TableSpec::new("public", "users", "id");
        let result = coordinator.snapshot_table(&spec).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_snapshot_coordinator_progress_persistence() {
        let source = MockSnapshotSource::new();
        let rows = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];
        source.add_rows("public", "users", rows).await;

        let progress_store = MemoryProgressStore::new();
        let config = SnapshotConfig::default();

        let coordinator = SnapshotCoordinator::new(config, source, progress_store);
        let spec = TableSpec::new("public", "users", "id");

        coordinator.snapshot_table(&spec).await.unwrap();

        // Progress should be saved after completion
        let all_progress = coordinator.get_all_progress().await.unwrap();
        assert_eq!(all_progress.len(), 1);
        assert_eq!(all_progress[0].state, SnapshotState::Completed);
    }

    #[test]
    fn test_snapshot_stats() {
        let stats = SnapshotStats::new();

        stats.record_rows(100);
        stats.record_batch();
        stats.record_bytes(1024);
        stats.record_table_completed();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.rows_processed, 100);
        assert_eq!(snapshot.batches_processed, 1);
        assert_eq!(snapshot.bytes_processed, 1024);
        assert_eq!(snapshot.tables_completed, 1);
    }

    #[tokio::test]
    async fn test_snapshot_multiple_tables() {
        let source = MockSnapshotSource::new();
        source
            .add_rows("public", "users", vec![serde_json::json!({"id": 1})])
            .await;
        source
            .add_rows("public", "orders", vec![serde_json::json!({"id": 1})])
            .await;

        let progress_store = MemoryProgressStore::new();
        let config = SnapshotConfig::default();

        let coordinator = SnapshotCoordinator::new(config, source, progress_store);

        let specs = vec![
            TableSpec::new("public", "users", "id"),
            TableSpec::new("public", "orders", "id"),
        ];

        let results = coordinator.snapshot_tables(specs).await.unwrap();

        assert_eq!(results.len(), 2);
        assert!(results.contains_key("public.users"));
        assert!(results.contains_key("public.orders"));
    }

    #[test]
    fn test_snapshot_state_default() {
        let state = SnapshotState::default();
        assert_eq!(state, SnapshotState::Pending);
    }
}
