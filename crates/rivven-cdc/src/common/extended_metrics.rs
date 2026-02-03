//! # Extended CDC Metrics
//!
//! Production-grade metrics collection providing comprehensive observability for
//! snapshot, streaming, incremental snapshot, and schema history operations.
//!
//! ## Metric Categories
//!
//! This module implements metrics for CDC operations:
//!
//! - **Snapshot Metrics**
//!   - `SnapshotRunning`, `SnapshotPaused`, `SnapshotCompleted`, `SnapshotAborted`
//!   - `SnapshotDurationInSeconds`, `TotalTableCount`, `RemainingTableCount`
//!   - `RowsScanned` (per table)
//!
//! - **Streaming Metrics**
//!   - `Connected`, `MilliSecondsBehindSource`, `TotalNumberOfEventsSeen`
//!   - `NumberOfCommittedTransactions`, `NumberOfRolledBackTransactions`
//!   - `SourceEventPosition`, `LastTransactionId`
//!
//! - **Incremental Snapshot Metrics**
//!   - `IncrementalSnapshotRunning`, `IncrementalSnapshotChunkId`
//!   - `IncrementalSnapshotWindowOpen`, `MaxQueueSizeInBytes`
//!
//! - **Schema History Metrics**
//!   - `SchemaHistoryRecovered`, `SchemaHistoryRecoveryDurationInMs`
//!   - `ChangesRecovered`, `ChangesApplied`
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::ExtendedCdcMetrics;
//!
//! let metrics = ExtendedCdcMetrics::new("postgres", "mydb");
//!
//! // Snapshot operations
//! metrics.start_snapshot(5); // 5 tables
//! metrics.record_table_snapshot_complete("users", 10_000);
//! metrics.complete_snapshot();
//!
//! // Streaming operations
//! metrics.set_streaming_connected(true);
//! metrics.record_streaming_event();
//! metrics.update_source_position("0/1234567");
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

// ============================================================================
// Lock Recovery Helpers
// ============================================================================

/// Extension trait for RwLock to recover from poisoned locks.
/// This is safe for metrics because the data remains valid even if a thread panicked.
trait RwLockExt<T> {
    /// Acquire read lock, recovering from poisoning.
    fn read_recovered(&self) -> RwLockReadGuard<'_, T>;
    /// Acquire write lock, recovering from poisoning.
    fn write_recovered(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_recovered(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_recovered(&self) -> RwLockWriteGuard<'_, T> {
        self.write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// ============================================================================
// Extended CDC Metrics
// ============================================================================

/// Extended CDC metrics for comprehensive observability.
///
/// Provides production-grade metrics for CDC deployments with
/// metrics categorized by operation type: snapshot, streaming, incremental
/// snapshot, and schema history.
#[derive(Debug)]
pub struct ExtendedCdcMetrics {
    /// Source type (postgres, mysql, mariadb, mssql, oracle)
    source_type: String,
    /// Database/server name
    database: String,
    /// Connector name (unique identifier)
    connector_name: String,

    // ========================================================================
    // Snapshot Metrics
    // ========================================================================
    /// Whether a snapshot is currently running
    snapshot_running: AtomicBool,
    /// Whether the snapshot is paused
    snapshot_paused: AtomicBool,
    /// Whether the snapshot completed successfully
    snapshot_completed: AtomicBool,
    /// Whether the snapshot was aborted
    snapshot_aborted: AtomicBool,
    /// Snapshot start time (for duration calculation)
    snapshot_start_time: RwLock<Option<Instant>>,
    /// Snapshot duration in milliseconds
    snapshot_duration_ms: AtomicU64,
    /// Total number of tables to snapshot
    total_table_count: AtomicU64,
    /// Remaining tables to snapshot
    remaining_table_count: AtomicU64,
    /// Rows scanned per table
    rows_scanned: RwLock<HashMap<String, u64>>,
    /// Total rows scanned across all tables
    total_rows_scanned: AtomicU64,
    /// Current table being snapshotted
    current_snapshot_table: RwLock<Option<String>>,
    /// Snapshot phase (LOCK, READ, UNLOCK, etc.)
    snapshot_phase: RwLock<SnapshotPhase>,

    // ========================================================================
    // Streaming Metrics
    // ========================================================================
    /// Whether streaming is connected
    streaming_connected: AtomicBool,
    /// Milliseconds behind source (replication lag)
    milliseconds_behind_source: AtomicI64,
    /// Total events seen (across all operations)
    total_number_of_events_seen: AtomicU64,
    /// Create events (inserts)
    number_of_create_events_seen: AtomicU64,
    /// Update events
    number_of_update_events_seen: AtomicU64,
    /// Delete events
    number_of_delete_events_seen: AtomicU64,
    /// Committed transactions
    number_of_committed_transactions: AtomicU64,
    /// Rolled back transactions (if tracked)
    number_of_rolled_back_transactions: AtomicU64,
    /// Source event position (LSN, binlog position, etc.)
    source_event_position: RwLock<String>,
    /// Last transaction ID
    last_transaction_id: RwLock<Option<String>>,
    /// Last event timestamp (source time)
    last_event_timestamp: AtomicU64,
    /// Queue size in bytes
    queue_size_in_bytes: AtomicU64,
    /// Maximum queue size in bytes
    max_queue_size_in_bytes: AtomicU64,
    /// Number of events filtered
    number_of_events_filtered: AtomicU64,
    /// Streaming start time
    streaming_start_time: RwLock<Option<Instant>>,

    // ========================================================================
    // Incremental Snapshot Metrics
    // ========================================================================
    /// Whether an incremental snapshot is running
    incremental_snapshot_running: AtomicBool,
    /// Whether the incremental snapshot is paused
    incremental_snapshot_paused: AtomicBool,
    /// Current chunk ID
    incremental_snapshot_chunk_id: RwLock<String>,
    /// Whether the watermark window is open
    incremental_snapshot_window_open: AtomicBool,
    /// Tables being incrementally snapshotted
    incremental_snapshot_tables: RwLock<Vec<String>>,
    /// Chunks processed
    incremental_snapshot_chunks_processed: AtomicU64,
    /// Total chunks for current table (for progress tracking)
    incremental_snapshot_total_chunks: AtomicU64,
    /// Rows captured in incremental snapshot
    incremental_snapshot_rows_captured: AtomicU64,
    /// Events deduplicated (from streaming during window)
    incremental_snapshot_events_deduplicated: AtomicU64,

    // ========================================================================
    // Schema History Metrics
    // ========================================================================
    /// Whether schema history was recovered
    schema_history_recovered: AtomicBool,
    /// Schema history recovery duration in milliseconds
    schema_history_recovery_duration_ms: AtomicU64,
    /// Number of schema changes recovered from history
    schema_history_changes_recovered: AtomicU64,
    /// Number of schema changes applied
    schema_history_changes_applied: AtomicU64,
    /// Last schema change timestamp
    last_schema_change_timestamp: AtomicU64,

    // ========================================================================
    // Error Metrics
    // ========================================================================
    /// Total errors encountered
    total_errors: AtomicU64,
    /// Errors by category
    errors_by_category: RwLock<HashMap<String, u64>>,
    /// Last error message
    last_error_message: RwLock<Option<String>>,
    /// Last error timestamp
    last_error_timestamp: AtomicU64,
    /// Consecutive errors (for circuit breaker)
    consecutive_errors: AtomicU64,

    // ========================================================================
    // Health Monitoring Metrics
    // ========================================================================
    /// Whether health monitoring is enabled
    health_monitoring_enabled: AtomicBool,
    /// Current health state (healthy/unhealthy)
    health_state_healthy: AtomicBool,
    /// Current readiness state
    health_state_ready: AtomicBool,
    /// Number of health checks performed
    health_checks_performed: AtomicU64,
    /// Number of health checks passed
    health_checks_passed: AtomicU64,
    /// Number of health checks failed
    health_checks_failed: AtomicU64,
    /// Consecutive health failures
    health_consecutive_failures: AtomicU64,
    /// Recovery attempts
    health_recovery_attempts: AtomicU64,
    /// Successful recoveries
    health_recoveries_succeeded: AtomicU64,
    /// Failed recoveries
    health_recoveries_failed: AtomicU64,
    /// Time spent in unhealthy state (ms)
    health_unhealthy_time_ms: AtomicU64,
    /// Last health check timestamp
    last_health_check_timestamp: AtomicU64,

    // ========================================================================
    // Performance Metrics
    // ========================================================================
    /// Event processing time histogram (microseconds)
    processing_time_histogram: RwLock<LatencyHistogram>,
    /// Capture to emit latency (source timestamp to emit)
    capture_to_emit_latency_ms: AtomicU64,
    /// Average batch size
    average_batch_size: AtomicU64,
    /// Batches processed
    batches_processed: AtomicU64,
}

impl ExtendedCdcMetrics {
    /// Create a new extended metrics collector.
    pub fn new(source_type: &str, database: &str) -> Self {
        Self::with_connector_name(
            source_type,
            database,
            &format!("{}-{}", source_type, database),
        )
    }

    /// Create a new extended metrics collector with custom connector name.
    pub fn with_connector_name(source_type: &str, database: &str, connector_name: &str) -> Self {
        Self {
            source_type: source_type.to_string(),
            database: database.to_string(),
            connector_name: connector_name.to_string(),

            // Snapshot
            snapshot_running: AtomicBool::new(false),
            snapshot_paused: AtomicBool::new(false),
            snapshot_completed: AtomicBool::new(false),
            snapshot_aborted: AtomicBool::new(false),
            snapshot_start_time: RwLock::new(None),
            snapshot_duration_ms: AtomicU64::new(0),
            total_table_count: AtomicU64::new(0),
            remaining_table_count: AtomicU64::new(0),
            rows_scanned: RwLock::new(HashMap::new()),
            total_rows_scanned: AtomicU64::new(0),
            current_snapshot_table: RwLock::new(None),
            snapshot_phase: RwLock::new(SnapshotPhase::NotStarted),

            // Streaming
            streaming_connected: AtomicBool::new(false),
            milliseconds_behind_source: AtomicI64::new(0),
            total_number_of_events_seen: AtomicU64::new(0),
            number_of_create_events_seen: AtomicU64::new(0),
            number_of_update_events_seen: AtomicU64::new(0),
            number_of_delete_events_seen: AtomicU64::new(0),
            number_of_committed_transactions: AtomicU64::new(0),
            number_of_rolled_back_transactions: AtomicU64::new(0),
            source_event_position: RwLock::new(String::new()),
            last_transaction_id: RwLock::new(None),
            last_event_timestamp: AtomicU64::new(0),
            queue_size_in_bytes: AtomicU64::new(0),
            max_queue_size_in_bytes: AtomicU64::new(0),
            number_of_events_filtered: AtomicU64::new(0),
            streaming_start_time: RwLock::new(None),

            // Incremental Snapshot
            incremental_snapshot_running: AtomicBool::new(false),
            incremental_snapshot_paused: AtomicBool::new(false),
            incremental_snapshot_chunk_id: RwLock::new(String::new()),
            incremental_snapshot_window_open: AtomicBool::new(false),
            incremental_snapshot_tables: RwLock::new(Vec::new()),
            incremental_snapshot_chunks_processed: AtomicU64::new(0),
            incremental_snapshot_total_chunks: AtomicU64::new(0),
            incremental_snapshot_rows_captured: AtomicU64::new(0),
            incremental_snapshot_events_deduplicated: AtomicU64::new(0),

            // Schema History
            schema_history_recovered: AtomicBool::new(false),
            schema_history_recovery_duration_ms: AtomicU64::new(0),
            schema_history_changes_recovered: AtomicU64::new(0),
            schema_history_changes_applied: AtomicU64::new(0),
            last_schema_change_timestamp: AtomicU64::new(0),

            // Errors
            total_errors: AtomicU64::new(0),
            errors_by_category: RwLock::new(HashMap::new()),
            last_error_message: RwLock::new(None),
            last_error_timestamp: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),

            // Health Monitoring
            health_monitoring_enabled: AtomicBool::new(false),
            health_state_healthy: AtomicBool::new(false),
            health_state_ready: AtomicBool::new(false),
            health_checks_performed: AtomicU64::new(0),
            health_checks_passed: AtomicU64::new(0),
            health_checks_failed: AtomicU64::new(0),
            health_consecutive_failures: AtomicU64::new(0),
            health_recovery_attempts: AtomicU64::new(0),
            health_recoveries_succeeded: AtomicU64::new(0),
            health_recoveries_failed: AtomicU64::new(0),
            health_unhealthy_time_ms: AtomicU64::new(0),
            last_health_check_timestamp: AtomicU64::new(0),

            // Performance
            processing_time_histogram: RwLock::new(LatencyHistogram::new()),
            capture_to_emit_latency_ms: AtomicU64::new(0),
            average_batch_size: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
        }
    }

    // ========================================================================
    // Snapshot Operations
    // ========================================================================

    /// Start a snapshot operation.
    pub fn start_snapshot(&self, table_count: u64) {
        self.snapshot_running.store(true, Ordering::SeqCst);
        self.snapshot_paused.store(false, Ordering::SeqCst);
        self.snapshot_completed.store(false, Ordering::SeqCst);
        self.snapshot_aborted.store(false, Ordering::SeqCst);
        self.total_table_count.store(table_count, Ordering::SeqCst);
        self.remaining_table_count
            .store(table_count, Ordering::SeqCst);
        self.total_rows_scanned.store(0, Ordering::SeqCst);
        self.rows_scanned.write_recovered().clear();
        *self.snapshot_start_time.write_recovered() = Some(Instant::now());
        *self.snapshot_phase.write_recovered() = SnapshotPhase::Lock;

        // Emit to metrics facade
        metrics::gauge!(
            "rivven_cdc_snapshot_running",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(1.0);

        metrics::gauge!(
            "rivven_cdc_snapshot_total_tables",
            "connector" => self.connector_name.clone()
        )
        .set(table_count as f64);
    }

    /// Set the current snapshot phase.
    pub fn set_snapshot_phase(&self, phase: SnapshotPhase) {
        let phase_str = phase.as_str();
        *self.snapshot_phase.write_recovered() = phase;

        metrics::gauge!(
            "rivven_cdc_snapshot_phase",
            "connector" => self.connector_name.clone(),
            "phase" => phase_str
        )
        .set(1.0);
    }

    /// Start snapshotting a specific table.
    pub fn start_table_snapshot(&self, table: &str) {
        *self.current_snapshot_table.write_recovered() = Some(table.to_string());
        self.rows_scanned
            .write()
            .unwrap()
            .entry(table.to_string())
            .or_insert(0);
        *self.snapshot_phase.write_recovered() = SnapshotPhase::Read;
    }

    /// Record rows scanned for a table.
    pub fn record_rows_scanned(&self, table: &str, rows: u64) {
        {
            let mut map = self.rows_scanned.write_recovered();
            *map.entry(table.to_string()).or_insert(0) += rows;
        }
        self.total_rows_scanned.fetch_add(rows, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_snapshot_rows_scanned",
            "connector" => self.connector_name.clone(),
            "table" => table.to_string()
        )
        .increment(rows);
    }

    /// Complete snapshot for a table.
    pub fn complete_table_snapshot(&self, table: &str, total_rows: u64) {
        self.record_rows_scanned(table, total_rows);
        self.remaining_table_count.fetch_sub(1, Ordering::SeqCst);
        *self.current_snapshot_table.write_recovered() = None;

        let remaining = self.remaining_table_count.load(Ordering::SeqCst);
        metrics::gauge!(
            "rivven_cdc_snapshot_remaining_tables",
            "connector" => self.connector_name.clone()
        )
        .set(remaining as f64);
    }

    /// Pause the snapshot.
    pub fn pause_snapshot(&self) {
        self.snapshot_paused.store(true, Ordering::SeqCst);
        metrics::gauge!(
            "rivven_cdc_snapshot_paused",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);
    }

    /// Resume the snapshot.
    pub fn resume_snapshot(&self) {
        self.snapshot_paused.store(false, Ordering::SeqCst);
        metrics::gauge!(
            "rivven_cdc_snapshot_paused",
            "connector" => self.connector_name.clone()
        )
        .set(0.0);
    }

    /// Complete the snapshot successfully.
    pub fn complete_snapshot(&self) {
        self.snapshot_running.store(false, Ordering::SeqCst);
        self.snapshot_completed.store(true, Ordering::SeqCst);
        *self.snapshot_phase.write_recovered() = SnapshotPhase::Completed;

        if let Some(start) = *self.snapshot_start_time.read_recovered() {
            let duration = start.elapsed();
            self.snapshot_duration_ms
                .store(duration.as_millis() as u64, Ordering::SeqCst);

            metrics::histogram!(
                "rivven_cdc_snapshot_duration_seconds",
                "connector" => self.connector_name.clone()
            )
            .record(duration.as_secs_f64());
        }

        metrics::gauge!(
            "rivven_cdc_snapshot_running",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(0.0);

        metrics::gauge!(
            "rivven_cdc_snapshot_completed",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);
    }

    /// Abort the snapshot.
    pub fn abort_snapshot(&self, reason: &str) {
        self.snapshot_running.store(false, Ordering::SeqCst);
        self.snapshot_aborted.store(true, Ordering::SeqCst);
        *self.snapshot_phase.write_recovered() = SnapshotPhase::Aborted;

        if let Some(start) = *self.snapshot_start_time.read_recovered() {
            let duration = start.elapsed();
            self.snapshot_duration_ms
                .store(duration.as_millis() as u64, Ordering::SeqCst);
        }

        metrics::gauge!(
            "rivven_cdc_snapshot_running",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(0.0);

        metrics::gauge!(
            "rivven_cdc_snapshot_aborted",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);

        tracing::warn!(connector = %self.connector_name, reason = %reason, "Snapshot aborted");
    }

    // ========================================================================
    // Streaming Operations
    // ========================================================================

    /// Set streaming connection state.
    pub fn set_streaming_connected(&self, connected: bool) {
        let was_connected = self.streaming_connected.swap(connected, Ordering::SeqCst);

        if connected && !was_connected {
            *self.streaming_start_time.write_recovered() = Some(Instant::now());
            self.consecutive_errors.store(0, Ordering::SeqCst);
        }

        metrics::gauge!(
            "rivven_cdc_streaming_connected",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(if connected { 1.0 } else { 0.0 });
    }

    /// Record a streaming event.
    pub fn record_streaming_event(&self) {
        self.total_number_of_events_seen
            .fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::SeqCst);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_event_timestamp.store(now, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_events_total",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Record a create (insert) event.
    pub fn record_create_event(&self) {
        self.record_streaming_event();
        self.number_of_create_events_seen
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_creates_total",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Record an update event.
    pub fn record_update_event(&self) {
        self.record_streaming_event();
        self.number_of_update_events_seen
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_updates_total",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Record a delete event.
    pub fn record_delete_event(&self) {
        self.record_streaming_event();
        self.number_of_delete_events_seen
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_deletes_total",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Record a committed transaction.
    pub fn record_committed_transaction(&self, transaction_id: Option<&str>) {
        self.number_of_committed_transactions
            .fetch_add(1, Ordering::Relaxed);

        if let Some(txid) = transaction_id {
            *self.last_transaction_id.write_recovered() = Some(txid.to_string());
        }

        metrics::counter!(
            "rivven_cdc_streaming_transactions_committed",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Record a rolled back transaction.
    pub fn record_rolled_back_transaction(&self) {
        self.number_of_rolled_back_transactions
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_transactions_rolled_back",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Update source event position (LSN, binlog position, etc.).
    pub fn update_source_position(&self, position: &str) {
        *self.source_event_position.write_recovered() = position.to_string();

        // Note: Position is typically a string, so we log it rather than gauge it
        tracing::trace!(connector = %self.connector_name, position = %position, "Source position updated");
    }

    /// Update replication lag.
    pub fn update_milliseconds_behind_source(&self, lag_ms: i64) {
        self.milliseconds_behind_source
            .store(lag_ms, Ordering::Relaxed);

        metrics::gauge!(
            "rivven_cdc_streaming_lag_milliseconds",
            "connector" => self.connector_name.clone()
        )
        .set(lag_ms as f64);
    }

    /// Record filtered event.
    pub fn record_filtered_event(&self) {
        self.number_of_events_filtered
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_streaming_events_filtered",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Update queue size.
    pub fn update_queue_size(&self, size_bytes: u64) {
        self.queue_size_in_bytes
            .store(size_bytes, Ordering::Relaxed);
        let max = self.max_queue_size_in_bytes.load(Ordering::Relaxed);
        if size_bytes > max {
            self.max_queue_size_in_bytes
                .store(size_bytes, Ordering::Relaxed);
        }

        metrics::gauge!(
            "rivven_cdc_streaming_queue_bytes",
            "connector" => self.connector_name.clone()
        )
        .set(size_bytes as f64);
    }

    // ========================================================================
    // Incremental Snapshot Operations
    // ========================================================================

    /// Start an incremental snapshot.
    pub fn start_incremental_snapshot(&self, tables: Vec<String>) {
        self.start_incremental_snapshot_with_chunks(tables, 0)
    }

    /// Start an incremental snapshot with known total chunk count.
    pub fn start_incremental_snapshot_with_chunks(&self, tables: Vec<String>, total_chunks: u64) {
        self.incremental_snapshot_running
            .store(true, Ordering::SeqCst);
        self.incremental_snapshot_paused
            .store(false, Ordering::SeqCst);
        self.incremental_snapshot_chunks_processed
            .store(0, Ordering::SeqCst);
        self.incremental_snapshot_total_chunks
            .store(total_chunks, Ordering::SeqCst);
        self.incremental_snapshot_rows_captured
            .store(0, Ordering::SeqCst);
        self.incremental_snapshot_events_deduplicated
            .store(0, Ordering::SeqCst);
        *self.incremental_snapshot_tables.write_recovered() = tables.clone();

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_running",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);

        if total_chunks > 0 {
            metrics::gauge!(
                "rivven_cdc_incremental_snapshot_total_chunks",
                "connector" => self.connector_name.clone()
            )
            .set(total_chunks as f64);
        }

        tracing::info!(
            connector = %self.connector_name,
            tables = ?tables,
            total_chunks = total_chunks,
            "Incremental snapshot started"
        );
    }

    /// Get incremental snapshot progress as percentage (0.0 to 1.0).
    pub fn incremental_snapshot_progress(&self) -> f64 {
        let total = self
            .incremental_snapshot_total_chunks
            .load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let processed = self
            .incremental_snapshot_chunks_processed
            .load(Ordering::Relaxed);
        (processed as f64 / total as f64).min(1.0)
    }

    /// Open the watermark window.
    pub fn open_incremental_snapshot_window(&self, chunk_id: &str) {
        self.incremental_snapshot_window_open
            .store(true, Ordering::SeqCst);
        *self.incremental_snapshot_chunk_id.write_recovered() = chunk_id.to_string();

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_window_open",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);
    }

    /// Close the watermark window.
    pub fn close_incremental_snapshot_window(&self) {
        self.incremental_snapshot_window_open
            .store(false, Ordering::SeqCst);

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_window_open",
            "connector" => self.connector_name.clone()
        )
        .set(0.0);
    }

    /// Record a chunk processed.
    pub fn record_incremental_snapshot_chunk(&self, rows: u64) {
        self.incremental_snapshot_chunks_processed
            .fetch_add(1, Ordering::Relaxed);
        self.incremental_snapshot_rows_captured
            .fetch_add(rows, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_incremental_snapshot_chunks_total",
            "connector" => self.connector_name.clone()
        )
        .increment(1);

        metrics::counter!(
            "rivven_cdc_incremental_snapshot_rows_total",
            "connector" => self.connector_name.clone()
        )
        .increment(rows);
    }

    /// Record a deduplicated event (from streaming during window).
    pub fn record_incremental_snapshot_dedup(&self) {
        self.incremental_snapshot_events_deduplicated
            .fetch_add(1, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_incremental_snapshot_events_deduplicated",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    /// Pause incremental snapshot.
    pub fn pause_incremental_snapshot(&self) {
        self.incremental_snapshot_paused
            .store(true, Ordering::SeqCst);

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_paused",
            "connector" => self.connector_name.clone()
        )
        .set(1.0);
    }

    /// Resume incremental snapshot.
    pub fn resume_incremental_snapshot(&self) {
        self.incremental_snapshot_paused
            .store(false, Ordering::SeqCst);

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_paused",
            "connector" => self.connector_name.clone()
        )
        .set(0.0);
    }

    /// Complete incremental snapshot.
    pub fn complete_incremental_snapshot(&self) {
        self.incremental_snapshot_running
            .store(false, Ordering::SeqCst);
        self.incremental_snapshot_window_open
            .store(false, Ordering::SeqCst);

        metrics::gauge!(
            "rivven_cdc_incremental_snapshot_running",
            "connector" => self.connector_name.clone()
        )
        .set(0.0);

        let chunks = self
            .incremental_snapshot_chunks_processed
            .load(Ordering::Relaxed);
        let rows = self
            .incremental_snapshot_rows_captured
            .load(Ordering::Relaxed);
        let deduped = self
            .incremental_snapshot_events_deduplicated
            .load(Ordering::Relaxed);

        tracing::info!(
            connector = %self.connector_name,
            chunks_processed = chunks,
            rows_captured = rows,
            events_deduplicated = deduped,
            "Incremental snapshot completed"
        );
    }

    // ========================================================================
    // Schema History Operations
    // ========================================================================

    /// Record schema history recovery.
    pub fn record_schema_history_recovery(&self, duration: Duration, changes_recovered: u64) {
        self.schema_history_recovered.store(true, Ordering::SeqCst);
        self.schema_history_recovery_duration_ms
            .store(duration.as_millis() as u64, Ordering::SeqCst);
        self.schema_history_changes_recovered
            .store(changes_recovered, Ordering::SeqCst);

        metrics::histogram!(
            "rivven_cdc_schema_history_recovery_seconds",
            "connector" => self.connector_name.clone()
        )
        .record(duration.as_secs_f64());

        metrics::counter!(
            "rivven_cdc_schema_history_changes_recovered",
            "connector" => self.connector_name.clone()
        )
        .increment(changes_recovered);
    }

    /// Record a schema change applied.
    pub fn record_schema_change_applied(&self) {
        self.schema_history_changes_applied
            .fetch_add(1, Ordering::Relaxed);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_schema_change_timestamp
            .store(now, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_schema_history_changes_applied",
            "connector" => self.connector_name.clone()
        )
        .increment(1);
    }

    // ========================================================================
    // Error Operations
    // ========================================================================

    /// Record an error.
    pub fn record_error(&self, category: &str, message: &str) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);

        {
            let mut map = self.errors_by_category.write_recovered();
            *map.entry(category.to_string()).or_insert(0) += 1;
        }

        *self.last_error_message.write_recovered() = Some(message.to_string());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_error_timestamp.store(now, Ordering::Relaxed);

        metrics::counter!(
            "rivven_cdc_errors_total",
            "connector" => self.connector_name.clone(),
            "category" => category.to_string()
        )
        .increment(1);
    }

    /// Clear consecutive error count (on success).
    pub fn clear_consecutive_errors(&self) {
        self.consecutive_errors.store(0, Ordering::SeqCst);
    }

    // ========================================================================
    // Performance Operations
    // ========================================================================

    /// Record event processing time.
    pub fn record_processing_time(&self, duration: Duration) {
        self.processing_time_histogram
            .write()
            .unwrap()
            .record(duration);

        metrics::histogram!(
            "rivven_cdc_processing_time_seconds",
            "connector" => self.connector_name.clone()
        )
        .record(duration.as_secs_f64());
    }

    /// Record capture to emit latency.
    pub fn record_capture_to_emit_latency(&self, latency_ms: u64) {
        self.capture_to_emit_latency_ms
            .store(latency_ms, Ordering::Relaxed);

        metrics::histogram!(
            "rivven_cdc_capture_to_emit_latency_seconds",
            "connector" => self.connector_name.clone()
        )
        .record(latency_ms as f64 / 1000.0);
    }

    /// Record a batch processed.
    pub fn record_batch(&self, size: u64) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);

        // Update rolling average batch size
        let batches = self.batches_processed.load(Ordering::Relaxed);
        let current_avg = self.average_batch_size.load(Ordering::Relaxed);
        let new_avg = if batches == 1 {
            size
        } else {
            // Exponential moving average
            (current_avg * 9 + size) / 10
        };
        self.average_batch_size.store(new_avg, Ordering::Relaxed);

        metrics::histogram!(
            "rivven_cdc_batch_size",
            "connector" => self.connector_name.clone()
        )
        .record(size as f64);
    }

    // ========================================================================
    // Snapshot Accessors
    // ========================================================================

    /// Get a complete metrics snapshot.
    pub fn snapshot(&self) -> ExtendedMetricsSnapshot {
        let latency_percentiles = self
            .processing_time_histogram
            .read_recovered()
            .percentiles();

        ExtendedMetricsSnapshot {
            connector_name: self.connector_name.clone(),
            source_type: self.source_type.clone(),
            database: self.database.clone(),

            // Snapshot
            snapshot_running: self.snapshot_running.load(Ordering::Relaxed),
            snapshot_paused: self.snapshot_paused.load(Ordering::Relaxed),
            snapshot_completed: self.snapshot_completed.load(Ordering::Relaxed),
            snapshot_aborted: self.snapshot_aborted.load(Ordering::Relaxed),
            snapshot_duration_ms: self.snapshot_duration_ms.load(Ordering::Relaxed),
            total_table_count: self.total_table_count.load(Ordering::Relaxed),
            remaining_table_count: self.remaining_table_count.load(Ordering::Relaxed),
            total_rows_scanned: self.total_rows_scanned.load(Ordering::Relaxed),
            snapshot_phase: self.snapshot_phase.read_recovered().clone(),

            // Streaming
            streaming_connected: self.streaming_connected.load(Ordering::Relaxed),
            milliseconds_behind_source: self.milliseconds_behind_source.load(Ordering::Relaxed),
            total_number_of_events_seen: self.total_number_of_events_seen.load(Ordering::Relaxed),
            number_of_create_events_seen: self.number_of_create_events_seen.load(Ordering::Relaxed),
            number_of_update_events_seen: self.number_of_update_events_seen.load(Ordering::Relaxed),
            number_of_delete_events_seen: self.number_of_delete_events_seen.load(Ordering::Relaxed),
            number_of_committed_transactions: self
                .number_of_committed_transactions
                .load(Ordering::Relaxed),
            number_of_rolled_back_transactions: self
                .number_of_rolled_back_transactions
                .load(Ordering::Relaxed),
            source_event_position: self.source_event_position.read_recovered().clone(),
            last_transaction_id: self.last_transaction_id.read_recovered().clone(),
            last_event_timestamp: self.last_event_timestamp.load(Ordering::Relaxed),
            queue_size_in_bytes: self.queue_size_in_bytes.load(Ordering::Relaxed),
            max_queue_size_in_bytes: self.max_queue_size_in_bytes.load(Ordering::Relaxed),
            number_of_events_filtered: self.number_of_events_filtered.load(Ordering::Relaxed),

            // Incremental Snapshot
            incremental_snapshot_running: self.incremental_snapshot_running.load(Ordering::Relaxed),
            incremental_snapshot_paused: self.incremental_snapshot_paused.load(Ordering::Relaxed),
            incremental_snapshot_chunk_id: self
                .incremental_snapshot_chunk_id
                .read()
                .unwrap()
                .clone(),
            incremental_snapshot_window_open: self
                .incremental_snapshot_window_open
                .load(Ordering::Relaxed),
            incremental_snapshot_chunks_processed: self
                .incremental_snapshot_chunks_processed
                .load(Ordering::Relaxed),
            incremental_snapshot_rows_captured: self
                .incremental_snapshot_rows_captured
                .load(Ordering::Relaxed),
            incremental_snapshot_events_deduplicated: self
                .incremental_snapshot_events_deduplicated
                .load(Ordering::Relaxed),

            // Schema History
            schema_history_recovered: self.schema_history_recovered.load(Ordering::Relaxed),
            schema_history_recovery_duration_ms: self
                .schema_history_recovery_duration_ms
                .load(Ordering::Relaxed),
            schema_history_changes_recovered: self
                .schema_history_changes_recovered
                .load(Ordering::Relaxed),
            schema_history_changes_applied: self
                .schema_history_changes_applied
                .load(Ordering::Relaxed),
            last_schema_change_timestamp: self.last_schema_change_timestamp.load(Ordering::Relaxed),

            // Errors
            total_errors: self.total_errors.load(Ordering::Relaxed),
            consecutive_errors: self.consecutive_errors.load(Ordering::Relaxed),
            last_error_message: self.last_error_message.read_recovered().clone(),
            last_error_timestamp: self.last_error_timestamp.load(Ordering::Relaxed),

            // Performance
            processing_time_p50_us: latency_percentiles.p50,
            processing_time_p95_us: latency_percentiles.p95,
            processing_time_p99_us: latency_percentiles.p99,
            processing_time_max_us: latency_percentiles.max,
            capture_to_emit_latency_ms: self.capture_to_emit_latency_ms.load(Ordering::Relaxed),
            average_batch_size: self.average_batch_size.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
        }
    }

    /// Check if the connector is healthy.
    pub fn is_healthy(&self) -> bool {
        let connected = self.streaming_connected.load(Ordering::Relaxed);
        let consecutive_errors = self.consecutive_errors.load(Ordering::Relaxed);

        // Healthy if connected and fewer than 5 consecutive errors
        connected && consecutive_errors < 5
    }

    // ========================================================================
    // Health Monitoring Operations
    // ========================================================================

    /// Enable health monitoring metrics.
    pub fn enable_health_monitoring(&self) {
        self.health_monitoring_enabled.store(true, Ordering::SeqCst);
        metrics::gauge!(
            "rivven_cdc_health_monitoring_enabled",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(1.0);
    }

    /// Record a health check result.
    pub fn record_health_check(&self, passed: bool) {
        self.health_checks_performed.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_health_check_timestamp
            .store(now, Ordering::Relaxed);

        if passed {
            self.health_checks_passed.fetch_add(1, Ordering::Relaxed);
            self.health_consecutive_failures.store(0, Ordering::Relaxed);
            metrics::counter!(
                "rivven_cdc_health_checks_passed_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone()
            )
            .increment(1);
        } else {
            self.health_checks_failed.fetch_add(1, Ordering::Relaxed);
            self.health_consecutive_failures
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "rivven_cdc_health_checks_failed_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone()
            )
            .increment(1);
        }
    }

    /// Update health state.
    pub fn set_health_state(&self, healthy: bool, ready: bool) {
        let was_healthy = self.health_state_healthy.swap(healthy, Ordering::SeqCst);
        self.health_state_ready.store(ready, Ordering::SeqCst);

        // Track transition to unhealthy state
        if was_healthy && !healthy {
            metrics::counter!(
                "rivven_cdc_health_state_transitions_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone(),
                "to" => "unhealthy"
            )
            .increment(1);
        } else if !was_healthy && healthy {
            metrics::counter!(
                "rivven_cdc_health_state_transitions_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone(),
                "to" => "healthy"
            )
            .increment(1);
        }

        metrics::gauge!(
            "rivven_cdc_health_state_healthy",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(if healthy { 1.0 } else { 0.0 });

        metrics::gauge!(
            "rivven_cdc_health_state_ready",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(if ready { 1.0 } else { 0.0 });
    }

    /// Record a recovery attempt.
    pub fn record_recovery_attempt(&self, success: bool) {
        self.health_recovery_attempts
            .fetch_add(1, Ordering::Relaxed);

        if success {
            self.health_recoveries_succeeded
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "rivven_cdc_health_recoveries_succeeded_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone()
            )
            .increment(1);
        } else {
            self.health_recoveries_failed
                .fetch_add(1, Ordering::Relaxed);
            metrics::counter!(
                "rivven_cdc_health_recoveries_failed_total",
                "connector" => self.connector_name.clone(),
                "source" => self.source_type.clone(),
                "database" => self.database.clone()
            )
            .increment(1);
        }

        metrics::gauge!(
            "rivven_cdc_health_recovery_attempts_total",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .set(self.health_recovery_attempts.load(Ordering::Relaxed) as f64);
    }

    /// Get health monitoring snapshot.
    pub fn health_snapshot(&self) -> HealthMetricsSnapshot {
        HealthMetricsSnapshot {
            monitoring_enabled: self.health_monitoring_enabled.load(Ordering::Relaxed),
            healthy: self.health_state_healthy.load(Ordering::Relaxed),
            ready: self.health_state_ready.load(Ordering::Relaxed),
            checks_performed: self.health_checks_performed.load(Ordering::Relaxed),
            checks_passed: self.health_checks_passed.load(Ordering::Relaxed),
            checks_failed: self.health_checks_failed.load(Ordering::Relaxed),
            consecutive_failures: self.health_consecutive_failures.load(Ordering::Relaxed),
            recovery_attempts: self.health_recovery_attempts.load(Ordering::Relaxed),
            recoveries_succeeded: self.health_recoveries_succeeded.load(Ordering::Relaxed),
            recoveries_failed: self.health_recoveries_failed.load(Ordering::Relaxed),
            unhealthy_time_ms: self.health_unhealthy_time_ms.load(Ordering::Relaxed),
            last_check_timestamp: self.last_health_check_timestamp.load(Ordering::Relaxed),
        }
    }

    /// Record time spent in unhealthy state.
    pub fn record_unhealthy_duration(&self, duration_ms: u64) {
        self.health_unhealthy_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
        metrics::counter!(
            "rivven_cdc_health_unhealthy_time_ms_total",
            "connector" => self.connector_name.clone(),
            "source" => self.source_type.clone(),
            "database" => self.database.clone()
        )
        .increment(duration_ms);
    }

    /// Get rows scanned for a specific table.
    pub fn get_rows_scanned(&self, table: &str) -> u64 {
        self.rows_scanned
            .read()
            .unwrap()
            .get(table)
            .copied()
            .unwrap_or(0)
    }

    /// Get all tables being incrementally snapshotted.
    pub fn get_incremental_snapshot_tables(&self) -> Vec<String> {
        self.incremental_snapshot_tables.read_recovered().clone()
    }

    /// Get error count by category.
    pub fn get_errors_by_category(&self, category: &str) -> u64 {
        self.errors_by_category
            .read()
            .unwrap()
            .get(category)
            .copied()
            .unwrap_or(0)
    }
}

// ============================================================================
// Snapshot Phase
// ============================================================================

/// Snapshot phase states.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SnapshotPhase {
    /// Snapshot not started
    #[default]
    NotStarted,
    /// Acquiring table locks
    Lock,
    /// Reading data
    Read,
    /// Releasing locks
    Unlock,
    /// Streaming
    Streaming,
    /// Completed successfully
    Completed,
    /// Aborted
    Aborted,
}

impl SnapshotPhase {
    /// Get the phase as a string for metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            SnapshotPhase::NotStarted => "not_started",
            SnapshotPhase::Lock => "lock",
            SnapshotPhase::Read => "read",
            SnapshotPhase::Unlock => "unlock",
            SnapshotPhase::Streaming => "streaming",
            SnapshotPhase::Completed => "completed",
            SnapshotPhase::Aborted => "aborted",
        }
    }
}

// ============================================================================
// Latency Histogram
// ============================================================================

/// Latency histogram using reservoir sampling.
#[derive(Debug)]
struct LatencyHistogram {
    samples: Vec<u64>,
    max_samples: usize,
    count: u64,
    max: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(1000),
            max_samples: 1000,
            count: 0,
            max: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let micros = latency.as_micros() as u64;
        self.count += 1;
        self.max = self.max.max(micros);

        if self.samples.len() < self.max_samples {
            self.samples.push(micros);
        } else {
            let idx = (self.count % self.max_samples as u64) as usize;
            self.samples[idx] = micros;
        }
    }

    fn percentiles(&self) -> LatencyPercentiles {
        if self.samples.is_empty() {
            return LatencyPercentiles::default();
        }

        let mut sorted = self.samples.clone();
        sorted.sort_unstable();

        let len = sorted.len();
        LatencyPercentiles {
            p50: sorted[len * 50 / 100],
            p95: sorted[len * 95 / 100],
            p99: sorted[(len * 99 / 100).min(len - 1)],
            max: self.max,
        }
    }
}

/// Latency percentiles.
#[derive(Debug, Clone, Default)]
struct LatencyPercentiles {
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

// ============================================================================
// Health Metrics Snapshot
// ============================================================================

/// Snapshot of health monitoring metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthMetricsSnapshot {
    /// Whether health monitoring is enabled
    pub monitoring_enabled: bool,
    /// Current health state
    pub healthy: bool,
    /// Current readiness state
    pub ready: bool,
    /// Total health checks performed
    pub checks_performed: u64,
    /// Health checks that passed
    pub checks_passed: u64,
    /// Health checks that failed
    pub checks_failed: u64,
    /// Current consecutive failures
    pub consecutive_failures: u64,
    /// Total recovery attempts
    pub recovery_attempts: u64,
    /// Successful recoveries
    pub recoveries_succeeded: u64,
    /// Failed recoveries
    pub recoveries_failed: u64,
    /// Total time spent in unhealthy state (ms)
    pub unhealthy_time_ms: u64,
    /// Last health check timestamp (ms since epoch)
    pub last_check_timestamp: u64,
}

impl HealthMetricsSnapshot {
    /// Get the success rate as a percentage.
    pub fn success_rate(&self) -> f64 {
        if self.checks_performed == 0 {
            100.0
        } else {
            (self.checks_passed as f64 / self.checks_performed as f64) * 100.0
        }
    }

    /// Get the recovery success rate.
    pub fn recovery_success_rate(&self) -> f64 {
        if self.recovery_attempts == 0 {
            100.0
        } else {
            (self.recoveries_succeeded as f64 / self.recovery_attempts as f64) * 100.0
        }
    }
}

// ============================================================================
// Extended Metrics Snapshot
// ============================================================================

/// Complete snapshot of extended metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExtendedMetricsSnapshot {
    pub connector_name: String,
    pub source_type: String,
    pub database: String,

    // Snapshot
    pub snapshot_running: bool,
    pub snapshot_paused: bool,
    pub snapshot_completed: bool,
    pub snapshot_aborted: bool,
    pub snapshot_duration_ms: u64,
    pub total_table_count: u64,
    pub remaining_table_count: u64,
    pub total_rows_scanned: u64,
    pub snapshot_phase: SnapshotPhase,

    // Streaming
    pub streaming_connected: bool,
    pub milliseconds_behind_source: i64,
    pub total_number_of_events_seen: u64,
    pub number_of_create_events_seen: u64,
    pub number_of_update_events_seen: u64,
    pub number_of_delete_events_seen: u64,
    pub number_of_committed_transactions: u64,
    pub number_of_rolled_back_transactions: u64,
    pub source_event_position: String,
    pub last_transaction_id: Option<String>,
    pub last_event_timestamp: u64,
    pub queue_size_in_bytes: u64,
    pub max_queue_size_in_bytes: u64,
    pub number_of_events_filtered: u64,

    // Incremental Snapshot
    pub incremental_snapshot_running: bool,
    pub incremental_snapshot_paused: bool,
    pub incremental_snapshot_chunk_id: String,
    pub incremental_snapshot_window_open: bool,
    pub incremental_snapshot_chunks_processed: u64,
    pub incremental_snapshot_rows_captured: u64,
    pub incremental_snapshot_events_deduplicated: u64,

    // Schema History
    pub schema_history_recovered: bool,
    pub schema_history_recovery_duration_ms: u64,
    pub schema_history_changes_recovered: u64,
    pub schema_history_changes_applied: u64,
    pub last_schema_change_timestamp: u64,

    // Errors
    pub total_errors: u64,
    pub consecutive_errors: u64,
    pub last_error_message: Option<String>,
    pub last_error_timestamp: u64,

    // Performance
    pub processing_time_p50_us: u64,
    pub processing_time_p95_us: u64,
    pub processing_time_p99_us: u64,
    pub processing_time_max_us: u64,
    pub capture_to_emit_latency_ms: u64,
    pub average_batch_size: u64,
    pub batches_processed: u64,
}

impl ExtendedMetricsSnapshot {
    /// Format as Prometheus exposition format.
    pub fn to_prometheus(&self) -> String {
        let labels = format!(
            "connector=\"{}\",source=\"{}\",database=\"{}\"",
            self.connector_name, self.source_type, self.database
        );

        format!(
            r#"# HELP rivven_cdc_snapshot_running Whether a snapshot is currently running
# TYPE rivven_cdc_snapshot_running gauge
rivven_cdc_snapshot_running{{{labels}}} {snapshot_running}

# HELP rivven_cdc_snapshot_paused Whether the snapshot is paused
# TYPE rivven_cdc_snapshot_paused gauge
rivven_cdc_snapshot_paused{{{labels}}} {snapshot_paused}

# HELP rivven_cdc_snapshot_completed Whether the snapshot completed successfully
# TYPE rivven_cdc_snapshot_completed gauge
rivven_cdc_snapshot_completed{{{labels}}} {snapshot_completed}

# HELP rivven_cdc_snapshot_duration_seconds Snapshot duration in seconds
# TYPE rivven_cdc_snapshot_duration_seconds gauge
rivven_cdc_snapshot_duration_seconds{{{labels}}} {snapshot_duration_secs:.3}

# HELP rivven_cdc_snapshot_tables_total Total tables in snapshot
# TYPE rivven_cdc_snapshot_tables_total gauge
rivven_cdc_snapshot_tables_total{{{labels}}} {total_tables}

# HELP rivven_cdc_snapshot_tables_remaining Remaining tables in snapshot
# TYPE rivven_cdc_snapshot_tables_remaining gauge
rivven_cdc_snapshot_tables_remaining{{{labels}}} {remaining_tables}

# HELP rivven_cdc_snapshot_rows_scanned_total Total rows scanned during snapshot
# TYPE rivven_cdc_snapshot_rows_scanned_total counter
rivven_cdc_snapshot_rows_scanned_total{{{labels}}} {rows_scanned}

# HELP rivven_cdc_streaming_connected Whether streaming is connected
# TYPE rivven_cdc_streaming_connected gauge
rivven_cdc_streaming_connected{{{labels}}} {streaming_connected}

# HELP rivven_cdc_streaming_lag_milliseconds Replication lag in milliseconds
# TYPE rivven_cdc_streaming_lag_milliseconds gauge
rivven_cdc_streaming_lag_milliseconds{{{labels}}} {lag_ms}

# HELP rivven_cdc_streaming_events_total Total streaming events
# TYPE rivven_cdc_streaming_events_total counter
rivven_cdc_streaming_events_total{{{labels}}} {events_total}

# HELP rivven_cdc_streaming_creates_total Total create events
# TYPE rivven_cdc_streaming_creates_total counter
rivven_cdc_streaming_creates_total{{{labels}}} {creates}

# HELP rivven_cdc_streaming_updates_total Total update events
# TYPE rivven_cdc_streaming_updates_total counter
rivven_cdc_streaming_updates_total{{{labels}}} {updates}

# HELP rivven_cdc_streaming_deletes_total Total delete events
# TYPE rivven_cdc_streaming_deletes_total counter
rivven_cdc_streaming_deletes_total{{{labels}}} {deletes}

# HELP rivven_cdc_streaming_transactions_committed Total committed transactions
# TYPE rivven_cdc_streaming_transactions_committed counter
rivven_cdc_streaming_transactions_committed{{{labels}}} {txn_committed}

# HELP rivven_cdc_incremental_snapshot_running Whether an incremental snapshot is running
# TYPE rivven_cdc_incremental_snapshot_running gauge
rivven_cdc_incremental_snapshot_running{{{labels}}} {inc_snapshot_running}

# HELP rivven_cdc_incremental_snapshot_window_open Whether the watermark window is open
# TYPE rivven_cdc_incremental_snapshot_window_open gauge
rivven_cdc_incremental_snapshot_window_open{{{labels}}} {inc_window_open}

# HELP rivven_cdc_schema_history_recovered Whether schema history was recovered
# TYPE rivven_cdc_schema_history_recovered gauge
rivven_cdc_schema_history_recovered{{{labels}}} {schema_recovered}

# HELP rivven_cdc_errors_total Total errors
# TYPE rivven_cdc_errors_total counter
rivven_cdc_errors_total{{{labels}}} {errors_total}

# HELP rivven_cdc_processing_time_microseconds Event processing time percentiles
# TYPE rivven_cdc_processing_time_microseconds summary
rivven_cdc_processing_time_microseconds{{{labels},quantile="0.50"}} {p50}
rivven_cdc_processing_time_microseconds{{{labels},quantile="0.95"}} {p95}
rivven_cdc_processing_time_microseconds{{{labels},quantile="0.99"}} {p99}
"#,
            labels = labels,
            snapshot_running = if self.snapshot_running { 1 } else { 0 },
            snapshot_paused = if self.snapshot_paused { 1 } else { 0 },
            snapshot_completed = if self.snapshot_completed { 1 } else { 0 },
            snapshot_duration_secs = self.snapshot_duration_ms as f64 / 1000.0,
            total_tables = self.total_table_count,
            remaining_tables = self.remaining_table_count,
            rows_scanned = self.total_rows_scanned,
            streaming_connected = if self.streaming_connected { 1 } else { 0 },
            lag_ms = self.milliseconds_behind_source,
            events_total = self.total_number_of_events_seen,
            creates = self.number_of_create_events_seen,
            updates = self.number_of_update_events_seen,
            deletes = self.number_of_delete_events_seen,
            txn_committed = self.number_of_committed_transactions,
            inc_snapshot_running = if self.incremental_snapshot_running {
                1
            } else {
                0
            },
            inc_window_open = if self.incremental_snapshot_window_open {
                1
            } else {
                0
            },
            schema_recovered = if self.schema_history_recovered { 1 } else { 0 },
            errors_total = self.total_errors,
            p50 = self.processing_time_p50_us,
            p95 = self.processing_time_p95_us,
            p99 = self.processing_time_p99_us,
        )
    }

    /// Format as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }
}

impl serde::Serialize for SnapshotPhase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

// ============================================================================
// Shared Handle
// ============================================================================

/// Shared extended metrics handle for use across async tasks.
pub type SharedExtendedMetrics = Arc<ExtendedCdcMetrics>;

/// Create a new shared extended metrics instance.
pub fn new_shared_extended_metrics(source_type: &str, database: &str) -> SharedExtendedMetrics {
    Arc::new(ExtendedCdcMetrics::new(source_type, database))
}

/// Create a new shared extended metrics instance with custom connector name.
pub fn new_shared_extended_metrics_with_name(
    source_type: &str,
    database: &str,
    connector_name: &str,
) -> SharedExtendedMetrics {
    Arc::new(ExtendedCdcMetrics::with_connector_name(
        source_type,
        database,
        connector_name,
    ))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_lifecycle() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Start snapshot
        metrics.start_snapshot(3);
        assert!(metrics.snapshot_running.load(Ordering::Relaxed));
        assert_eq!(metrics.total_table_count.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.remaining_table_count.load(Ordering::Relaxed), 3);

        // Snapshot tables
        metrics.start_table_snapshot("users");
        metrics.record_rows_scanned("users", 1000);
        metrics.complete_table_snapshot("users", 0);
        assert_eq!(metrics.remaining_table_count.load(Ordering::Relaxed), 2);

        metrics.start_table_snapshot("orders");
        metrics.complete_table_snapshot("orders", 5000);
        assert_eq!(metrics.remaining_table_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.get_rows_scanned("orders"), 5000);

        metrics.start_table_snapshot("products");
        metrics.complete_table_snapshot("products", 500);
        assert_eq!(metrics.remaining_table_count.load(Ordering::Relaxed), 0);

        // Complete snapshot
        metrics.complete_snapshot();
        assert!(!metrics.snapshot_running.load(Ordering::Relaxed));
        assert!(metrics.snapshot_completed.load(Ordering::Relaxed));
        assert_eq!(
            metrics.total_rows_scanned.load(Ordering::Relaxed),
            1000 + 5000 + 500
        );
    }

    #[test]
    fn test_snapshot_pause_resume() {
        let metrics = ExtendedCdcMetrics::new("mysql", "testdb");

        metrics.start_snapshot(1);
        assert!(!metrics.snapshot_paused.load(Ordering::Relaxed));

        metrics.pause_snapshot();
        assert!(metrics.snapshot_paused.load(Ordering::Relaxed));

        metrics.resume_snapshot();
        assert!(!metrics.snapshot_paused.load(Ordering::Relaxed));
    }

    #[test]
    fn test_snapshot_abort() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.start_snapshot(5);
        metrics.start_table_snapshot("users");
        metrics.abort_snapshot("Connection lost");

        assert!(!metrics.snapshot_running.load(Ordering::Relaxed));
        assert!(metrics.snapshot_aborted.load(Ordering::Relaxed));
        assert!(!metrics.snapshot_completed.load(Ordering::Relaxed));
    }

    #[test]
    fn test_streaming_events() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.set_streaming_connected(true);
        assert!(metrics.streaming_connected.load(Ordering::Relaxed));

        metrics.record_create_event();
        metrics.record_create_event();
        metrics.record_update_event();
        metrics.record_delete_event();

        assert_eq!(
            metrics.total_number_of_events_seen.load(Ordering::Relaxed),
            4
        );
        assert_eq!(
            metrics.number_of_create_events_seen.load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics.number_of_update_events_seen.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics.number_of_delete_events_seen.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_streaming_transactions() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_committed_transaction(Some("txn-123"));
        metrics.record_committed_transaction(Some("txn-124"));
        metrics.record_rolled_back_transaction();

        assert_eq!(
            metrics
                .number_of_committed_transactions
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics
                .number_of_rolled_back_transactions
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            *metrics.last_transaction_id.read_recovered(),
            Some("txn-124".to_string())
        );
    }

    #[test]
    fn test_streaming_lag() {
        let metrics = ExtendedCdcMetrics::new("mysql", "testdb");

        metrics.update_milliseconds_behind_source(5000);
        assert_eq!(
            metrics.milliseconds_behind_source.load(Ordering::Relaxed),
            5000
        );

        metrics.update_milliseconds_behind_source(100);
        assert_eq!(
            metrics.milliseconds_behind_source.load(Ordering::Relaxed),
            100
        );
    }

    #[test]
    fn test_source_position() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.update_source_position("0/1234567");
        assert_eq!(*metrics.source_event_position.read_recovered(), "0/1234567");

        metrics.update_source_position("0/1234600");
        assert_eq!(*metrics.source_event_position.read_recovered(), "0/1234600");
    }

    #[test]
    fn test_incremental_snapshot() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Start incremental snapshot
        metrics.start_incremental_snapshot(vec!["users".to_string(), "orders".to_string()]);
        assert!(metrics.incremental_snapshot_running.load(Ordering::Relaxed));
        assert_eq!(
            metrics.get_incremental_snapshot_tables(),
            vec!["users".to_string(), "orders".to_string()]
        );

        // Open window
        metrics.open_incremental_snapshot_window("chunk-1");
        assert!(metrics
            .incremental_snapshot_window_open
            .load(Ordering::Relaxed));
        assert_eq!(
            *metrics.incremental_snapshot_chunk_id.read_recovered(),
            "chunk-1"
        );

        // Record chunk
        metrics.record_incremental_snapshot_chunk(1000);
        assert_eq!(
            metrics
                .incremental_snapshot_chunks_processed
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics
                .incremental_snapshot_rows_captured
                .load(Ordering::Relaxed),
            1000
        );

        // Record dedup
        metrics.record_incremental_snapshot_dedup();
        metrics.record_incremental_snapshot_dedup();
        assert_eq!(
            metrics
                .incremental_snapshot_events_deduplicated
                .load(Ordering::Relaxed),
            2
        );

        // Close window
        metrics.close_incremental_snapshot_window();
        assert!(!metrics
            .incremental_snapshot_window_open
            .load(Ordering::Relaxed));

        // Complete
        metrics.complete_incremental_snapshot();
        assert!(!metrics.incremental_snapshot_running.load(Ordering::Relaxed));
    }

    #[test]
    fn test_schema_history() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_schema_history_recovery(Duration::from_millis(500), 10);
        assert!(metrics.schema_history_recovered.load(Ordering::Relaxed));
        assert_eq!(
            metrics
                .schema_history_recovery_duration_ms
                .load(Ordering::Relaxed),
            500
        );
        assert_eq!(
            metrics
                .schema_history_changes_recovered
                .load(Ordering::Relaxed),
            10
        );

        metrics.record_schema_change_applied();
        metrics.record_schema_change_applied();
        assert_eq!(
            metrics
                .schema_history_changes_applied
                .load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_error_tracking() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_error("connection", "Connection refused");
        metrics.record_error("connection", "Connection timeout");
        metrics.record_error("decode", "Invalid message format");

        assert_eq!(metrics.total_errors.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.consecutive_errors.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.get_errors_by_category("connection"), 2);
        assert_eq!(metrics.get_errors_by_category("decode"), 1);
        assert_eq!(
            *metrics.last_error_message.read_recovered(),
            Some("Invalid message format".to_string())
        );

        // Clear on success
        metrics.record_streaming_event();
        assert_eq!(metrics.consecutive_errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_health_check() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Initially unhealthy (not connected)
        assert!(!metrics.is_healthy());

        // Healthy when connected
        metrics.set_streaming_connected(true);
        assert!(metrics.is_healthy());

        // Unhealthy with too many errors
        for _ in 0..5 {
            metrics.record_error("connection", "error");
        }
        assert!(!metrics.is_healthy());

        // Healthy again after success
        metrics.record_streaming_event();
        assert!(metrics.is_healthy());
    }

    #[test]
    fn test_performance_metrics() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_processing_time(Duration::from_micros(100));
        metrics.record_processing_time(Duration::from_micros(200));
        metrics.record_processing_time(Duration::from_micros(150));

        metrics.record_batch(100);
        metrics.record_batch(200);

        assert_eq!(metrics.batches_processed.load(Ordering::Relaxed), 2);

        metrics.record_capture_to_emit_latency(50);
        assert_eq!(
            metrics.capture_to_emit_latency_ms.load(Ordering::Relaxed),
            50
        );
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.start_snapshot(2);
        metrics.set_streaming_connected(true);
        metrics.record_create_event();

        let snapshot = metrics.snapshot();

        assert!(snapshot.snapshot_running);
        assert!(snapshot.streaming_connected);
        assert_eq!(snapshot.total_number_of_events_seen, 1);
        assert_eq!(snapshot.source_type, "postgres");
        assert_eq!(snapshot.database, "testdb");
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.start_snapshot(3);
        metrics.set_streaming_connected(true);
        metrics.record_create_event();

        let snapshot = metrics.snapshot();
        let prom = snapshot.to_prometheus();

        assert!(prom.contains("rivven_cdc_snapshot_running"));
        assert!(prom.contains("rivven_cdc_streaming_connected"));
        assert!(prom.contains("connector=\"postgres-testdb\""));
    }

    #[test]
    fn test_json_format() {
        let metrics = ExtendedCdcMetrics::new("mysql", "testdb");

        metrics.set_streaming_connected(true);
        metrics.record_update_event();

        let snapshot = metrics.snapshot();
        let json = snapshot.to_json();

        assert!(json.contains("\"source_type\": \"mysql\""));
        assert!(json.contains("\"streaming_connected\": true"));
    }

    #[test]
    fn test_queue_metrics() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.update_queue_size(1000);
        assert_eq!(metrics.queue_size_in_bytes.load(Ordering::Relaxed), 1000);
        assert_eq!(
            metrics.max_queue_size_in_bytes.load(Ordering::Relaxed),
            1000
        );

        metrics.update_queue_size(2000);
        assert_eq!(metrics.queue_size_in_bytes.load(Ordering::Relaxed), 2000);
        assert_eq!(
            metrics.max_queue_size_in_bytes.load(Ordering::Relaxed),
            2000
        );

        metrics.update_queue_size(500);
        assert_eq!(metrics.queue_size_in_bytes.load(Ordering::Relaxed), 500);
        assert_eq!(
            metrics.max_queue_size_in_bytes.load(Ordering::Relaxed),
            2000
        );
    }

    #[test]
    fn test_filtered_events() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_filtered_event();
        metrics.record_filtered_event();
        metrics.record_filtered_event();

        assert_eq!(metrics.number_of_events_filtered.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_snapshot_phase() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        assert_eq!(
            *metrics.snapshot_phase.read_recovered(),
            SnapshotPhase::NotStarted
        );

        metrics.start_snapshot(1);
        assert_eq!(
            *metrics.snapshot_phase.read_recovered(),
            SnapshotPhase::Lock
        );

        metrics.set_snapshot_phase(SnapshotPhase::Read);
        assert_eq!(
            *metrics.snapshot_phase.read_recovered(),
            SnapshotPhase::Read
        );

        metrics.complete_snapshot();
        assert_eq!(
            *metrics.snapshot_phase.read_recovered(),
            SnapshotPhase::Completed
        );
    }

    #[test]
    fn test_shared_metrics() {
        let metrics = new_shared_extended_metrics("postgres", "testdb");

        metrics.record_create_event();
        assert_eq!(
            metrics.number_of_create_events_seen.load(Ordering::Relaxed),
            1
        );

        let metrics2 = new_shared_extended_metrics_with_name("mysql", "mydb", "my-connector");
        assert_eq!(metrics2.connector_name, "my-connector");
    }

    #[test]
    fn test_health_monitoring_metrics() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Initially disabled
        assert!(!metrics.health_monitoring_enabled.load(Ordering::Relaxed));

        // Enable health monitoring
        metrics.enable_health_monitoring();
        assert!(metrics.health_monitoring_enabled.load(Ordering::Relaxed));

        // Record health checks
        metrics.record_health_check(true);
        metrics.record_health_check(true);
        metrics.record_health_check(false);

        let snapshot = metrics.health_snapshot();
        assert!(snapshot.monitoring_enabled);
        assert_eq!(snapshot.checks_performed, 3);
        assert_eq!(snapshot.checks_passed, 2);
        assert_eq!(snapshot.checks_failed, 1);
        assert_eq!(snapshot.consecutive_failures, 1);

        // Successful check resets consecutive failures
        metrics.record_health_check(true);
        assert_eq!(
            metrics.health_consecutive_failures.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_health_state_tracking() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Initially unhealthy
        assert!(!metrics.health_state_healthy.load(Ordering::Relaxed));
        assert!(!metrics.health_state_ready.load(Ordering::Relaxed));

        // Mark healthy
        metrics.set_health_state(true, true);
        assert!(metrics.health_state_healthy.load(Ordering::Relaxed));
        assert!(metrics.health_state_ready.load(Ordering::Relaxed));

        // Mark unhealthy
        metrics.set_health_state(false, false);
        assert!(!metrics.health_state_healthy.load(Ordering::Relaxed));
    }

    #[test]
    fn test_health_recovery_tracking() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        // Record recovery attempts
        metrics.record_recovery_attempt(true);
        metrics.record_recovery_attempt(false);
        metrics.record_recovery_attempt(true);

        let snapshot = metrics.health_snapshot();
        assert_eq!(snapshot.recovery_attempts, 3);
        assert_eq!(snapshot.recoveries_succeeded, 2);
        assert_eq!(snapshot.recoveries_failed, 1);
        assert!((snapshot.recovery_success_rate() - 66.666).abs() < 1.0);
    }

    #[test]
    fn test_health_unhealthy_duration() {
        let metrics = ExtendedCdcMetrics::new("postgres", "testdb");

        metrics.record_unhealthy_duration(1000);
        metrics.record_unhealthy_duration(500);

        let snapshot = metrics.health_snapshot();
        assert_eq!(snapshot.unhealthy_time_ms, 1500);
    }

    #[test]
    fn test_health_metrics_snapshot_rates() {
        let snapshot = HealthMetricsSnapshot {
            monitoring_enabled: true,
            healthy: true,
            ready: true,
            checks_performed: 100,
            checks_passed: 95,
            checks_failed: 5,
            consecutive_failures: 0,
            recovery_attempts: 10,
            recoveries_succeeded: 8,
            recoveries_failed: 2,
            unhealthy_time_ms: 5000,
            last_check_timestamp: 1234567890,
        };

        assert!((snapshot.success_rate() - 95.0).abs() < 0.01);
        assert!((snapshot.recovery_success_rate() - 80.0).abs() < 0.01);
    }
}
