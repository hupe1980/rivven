//! # Incremental Snapshots
//!
//! Debezium-compatible incremental snapshot implementation using watermark-based
//! deduplication. Allows re-snapshotting tables while streaming continues.
//!
//! ## How It Works
//!
//! 1. **Chunk-based processing**: Tables are split into chunks by primary key
//! 2. **Watermark signals**: Open/close window markers for deduplication
//! 3. **Buffer deduplication**: Compare snapshot events with streaming events
//! 4. **Resume support**: Store chunk state for restart recovery
//!
//! ## Watermark-Based Deduplication (DBLog Algorithm)
//!
//! ```text
//! Timeline:
//! ────────────────────────────────────────────────────────────────►
//!     │         │                     │         │
//!     │  OPEN   │   Chunk Query       │  CLOSE  │
//!     │ Window  │   (buffer results)  │ Window  │
//!     │         │                     │         │
//!
//! During window:
//! - Snapshot rows go into memory buffer
//! - Streaming events with matching PKs cause buffer entries to be dropped
//! - On window close: remaining buffer entries emitted as READ events
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::incremental_snapshot::{
//!     IncrementalSnapshotConfig, IncrementalSnapshotCoordinator, ChunkRequest
//! };
//!
//! // Configure incremental snapshots
//! let config = IncrementalSnapshotConfig::builder()
//!     .chunk_size(1024)
//!     .watermark_strategy(WatermarkStrategy::InsertInsert)
//!     .build();
//!
//! // Create coordinator
//! let mut coordinator = IncrementalSnapshotCoordinator::new(config);
//!
//! // Start snapshot for table
//! coordinator.start_snapshot("public.orders", "id").await?;
//!
//! // Process chunks
//! while let Some(chunk) = coordinator.next_chunk().await? {
//!     // Open window (write to signal table)
//!     coordinator.open_window(&chunk).await?;
//!     
//!     // Execute chunk query and buffer results
//!     let rows = execute_chunk_query(&chunk).await?;
//!     coordinator.buffer_chunk_results(rows);
//!     
//!     // Close window
//!     let events = coordinator.close_window().await?;
//!     
//!     // Emit remaining events
//!     for event in events {
//!         emit_event(event).await?;
//!     }
//! }
//! ```
//!
//! ## Signal Table Watermarks
//!
//! Incremental snapshots use the signal table to write watermark markers:
//!
//! | Signal ID | Type | Data |
//! |-----------|------|------|
//! | `snap-1-open` | `snapshot-window-open` | `{"chunk_id": "...", "table": "..."}` |
//! | `snap-1-close` | `snapshot-window-close` | `{"chunk_id": "...", "table": "..."}` |
//!
//! These markers flow through the CDC stream, enabling deduplication.

use crate::common::{CdcError, CdcEvent, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};
use uuid::Uuid;

/// Watermark strategy for incremental snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkStrategy {
    /// Insert open marker, insert close marker (default)
    /// Both markers remain in signal table
    #[default]
    InsertInsert,
    /// Insert open marker, delete it on close
    /// Prevents signal table growth
    InsertDelete,
}

impl WatermarkStrategy {
    /// Check if this strategy requires delete permissions.
    pub fn requires_delete(&self) -> bool {
        matches!(self, WatermarkStrategy::InsertDelete)
    }
}

/// Configuration for incremental snapshots.
#[derive(Debug, Clone)]
pub struct IncrementalSnapshotConfig {
    /// Number of rows per chunk (default: 1024)
    pub chunk_size: usize,
    /// Watermark strategy
    pub watermark_strategy: WatermarkStrategy,
    /// Maximum memory for chunk buffer (bytes)
    pub max_buffer_memory: usize,
    /// Query timeout per chunk
    pub chunk_timeout: Duration,
    /// Delay between chunks (backpressure)
    pub inter_chunk_delay: Option<Duration>,
    /// Maximum concurrent chunk queries
    pub max_concurrent_chunks: usize,
    /// Enable surrogate key support
    pub allow_surrogate_key: bool,
    /// Signal table name for watermarks
    pub signal_table: String,
}

impl Default for IncrementalSnapshotConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1024,
            watermark_strategy: WatermarkStrategy::InsertInsert,
            max_buffer_memory: 64 * 1024 * 1024, // 64MB
            chunk_timeout: Duration::from_secs(60),
            inter_chunk_delay: None,
            max_concurrent_chunks: 1,
            allow_surrogate_key: true,
            signal_table: "debezium_signal".to_string(),
        }
    }
}

impl IncrementalSnapshotConfig {
    /// Create a builder for configuration.
    pub fn builder() -> IncrementalSnapshotConfigBuilder {
        IncrementalSnapshotConfigBuilder::default()
    }
}

/// Builder for IncrementalSnapshotConfig.
#[derive(Default)]
pub struct IncrementalSnapshotConfigBuilder {
    config: IncrementalSnapshotConfig,
}

impl IncrementalSnapshotConfigBuilder {
    /// Set chunk size (rows per chunk).
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.config.chunk_size = size.max(1);
        self
    }

    /// Set watermark strategy.
    pub fn watermark_strategy(mut self, strategy: WatermarkStrategy) -> Self {
        self.config.watermark_strategy = strategy;
        self
    }

    /// Set maximum buffer memory.
    pub fn max_buffer_memory(mut self, bytes: usize) -> Self {
        self.config.max_buffer_memory = bytes;
        self
    }

    /// Set chunk query timeout.
    pub fn chunk_timeout(mut self, timeout: Duration) -> Self {
        self.config.chunk_timeout = timeout;
        self
    }

    /// Set delay between chunks.
    pub fn inter_chunk_delay(mut self, delay: Duration) -> Self {
        self.config.inter_chunk_delay = Some(delay);
        self
    }

    /// Set maximum concurrent chunks.
    pub fn max_concurrent_chunks(mut self, count: usize) -> Self {
        self.config.max_concurrent_chunks = count.max(1);
        self
    }

    /// Allow surrogate keys.
    pub fn allow_surrogate_key(mut self, allow: bool) -> Self {
        self.config.allow_surrogate_key = allow;
        self
    }

    /// Set signal table name.
    pub fn signal_table(mut self, table: impl Into<String>) -> Self {
        self.config.signal_table = table.into();
        self
    }

    /// Build the configuration.
    pub fn build(self) -> IncrementalSnapshotConfig {
        self.config
    }
}

/// State of an incremental snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IncrementalSnapshotState {
    /// Not started
    #[default]
    Pending,
    /// Currently running
    Running,
    /// Paused (can resume)
    Paused,
    /// Completed successfully
    Completed,
    /// Stopped by signal
    Stopped,
    /// Failed with error
    Failed,
}

impl IncrementalSnapshotState {
    /// Check if snapshot is active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Paused)
    }

    /// Check if snapshot can be resumed.
    pub fn can_resume(&self) -> bool {
        matches!(self, Self::Paused)
    }
}

/// A table being incrementally snapshotted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTable {
    /// Fully qualified table name (schema.table)
    pub table_name: String,
    /// Primary key column(s)
    pub primary_key: Vec<String>,
    /// Surrogate key (if specified, overrides primary key for chunking)
    pub surrogate_key: Option<String>,
    /// Current state
    pub state: IncrementalSnapshotState,
    /// Maximum primary key value (snapshot endpoint)
    pub max_key: Option<String>,
    /// Last processed key (for resumption)
    pub last_key: Option<String>,
    /// Total chunks estimated
    pub total_chunks: Option<u64>,
    /// Chunks completed
    pub completed_chunks: u64,
    /// Rows processed
    pub rows_processed: u64,
    /// Additional filter conditions
    pub additional_conditions: Option<String>,
}

impl SnapshotTable {
    /// Create a new snapshot table entry.
    pub fn new(table_name: impl Into<String>, primary_key: Vec<String>) -> Self {
        Self {
            table_name: table_name.into(),
            primary_key,
            surrogate_key: None,
            state: IncrementalSnapshotState::Pending,
            max_key: None,
            last_key: None,
            total_chunks: None,
            completed_chunks: 0,
            rows_processed: 0,
            additional_conditions: None,
        }
    }

    /// Set surrogate key.
    pub fn with_surrogate_key(mut self, key: impl Into<String>) -> Self {
        self.surrogate_key = Some(key.into());
        self
    }

    /// Set additional conditions (WHERE clause filter).
    pub fn with_conditions(mut self, conditions: impl Into<String>) -> Self {
        self.additional_conditions = Some(conditions.into());
        self
    }

    /// Get the key column for chunking.
    pub fn chunk_key(&self) -> &str {
        self.surrogate_key
            .as_deref()
            .unwrap_or_else(|| self.primary_key.first().map(|s| s.as_str()).unwrap_or("id"))
    }

    /// Calculate progress percentage.
    pub fn progress_percentage(&self) -> Option<f64> {
        self.total_chunks.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.completed_chunks as f64 / total as f64) * 100.0
            }
        })
    }
}

/// A chunk of a table to snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotChunk {
    /// Unique chunk identifier
    pub chunk_id: String,
    /// Table being snapshotted
    pub table_name: String,
    /// Chunk sequence number (0-indexed)
    pub sequence: u64,
    /// Lower bound of primary key (exclusive, None for first chunk)
    pub from_key: Option<String>,
    /// Upper bound of primary key (inclusive)
    pub to_key: Option<String>,
    /// Key column name
    pub key_column: String,
    /// Additional filter conditions
    pub conditions: Option<String>,
    /// Chunk state
    pub state: ChunkState,
}

/// State of a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ChunkState {
    /// Pending execution
    #[default]
    Pending,
    /// Window opened, query executing
    WindowOpen,
    /// Query complete, window closing
    Buffered,
    /// Emitted, complete
    Complete,
    /// Failed
    Failed,
}

impl SnapshotChunk {
    /// Create a new chunk.
    pub fn new(
        table_name: impl Into<String>,
        sequence: u64,
        key_column: impl Into<String>,
    ) -> Self {
        Self {
            chunk_id: format!("chunk-{}-{}", Uuid::new_v4(), sequence),
            table_name: table_name.into(),
            sequence,
            from_key: None,
            to_key: None,
            key_column: key_column.into(),
            conditions: None,
            state: ChunkState::Pending,
        }
    }

    /// Set key range.
    pub fn with_range(mut self, from: Option<String>, to: Option<String>) -> Self {
        self.from_key = from;
        self.to_key = to;
        self
    }

    /// Set additional conditions.
    pub fn with_conditions(mut self, conditions: impl Into<String>) -> Self {
        self.conditions = Some(conditions.into());
        self
    }

    /// Generate SQL for this chunk.
    pub fn to_sql(&self, chunk_size: usize) -> String {
        let mut sql = format!("SELECT * FROM {} WHERE ", self.table_name);

        // Add key range condition
        if let Some(ref from) = self.from_key {
            sql.push_str(&format!("{} > '{}' AND ", self.key_column, from));
        }
        if let Some(ref to) = self.to_key {
            sql.push_str(&format!("{} <= '{}' ", self.key_column, to));
        } else {
            // No upper bound - use chunk size limit
            sql.push_str("1=1 ");
        }

        // Add additional conditions
        if let Some(ref conditions) = self.conditions {
            sql.push_str(&format!("AND ({}) ", conditions));
        }

        // Order and limit
        sql.push_str(&format!(
            "ORDER BY {} LIMIT {}",
            self.key_column, chunk_size
        ));

        sql
    }
}

/// Watermark signal for snapshot window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkSignal {
    /// Signal ID
    pub id: String,
    /// Signal type (snapshot-window-open or snapshot-window-close)
    pub signal_type: WatermarkType,
    /// Chunk being processed
    pub chunk_id: String,
    /// Table name
    pub table_name: String,
    /// Timestamp
    pub timestamp: i64,
}

/// Type of watermark signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WatermarkType {
    /// Snapshot window opened
    SnapshotWindowOpen,
    /// Snapshot window closed
    SnapshotWindowClose,
}

impl WatermarkType {
    /// Get signal type string for Debezium compatibility.
    pub fn as_str(&self) -> &str {
        match self {
            WatermarkType::SnapshotWindowOpen => "snapshot-window-open",
            WatermarkType::SnapshotWindowClose => "snapshot-window-close",
        }
    }
}

impl WatermarkSignal {
    /// Create an open window signal.
    pub fn open(chunk: &SnapshotChunk) -> Self {
        Self {
            id: format!("{}-open", chunk.chunk_id),
            signal_type: WatermarkType::SnapshotWindowOpen,
            chunk_id: chunk.chunk_id.clone(),
            table_name: chunk.table_name.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Create a close window signal.
    pub fn close(chunk: &SnapshotChunk) -> Self {
        Self {
            id: format!("{}-close", chunk.chunk_id),
            signal_type: WatermarkType::SnapshotWindowClose,
            chunk_id: chunk.chunk_id.clone(),
            table_name: chunk.table_name.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Generate SQL INSERT for signal table.
    pub fn to_insert_sql(&self, signal_table: &str) -> String {
        let data = serde_json::json!({
            "chunk_id": self.chunk_id,
            "table": self.table_name,
        });
        format!(
            "INSERT INTO {} (id, type, data) VALUES ('{}', '{}', '{}')",
            signal_table,
            self.id,
            self.signal_type.as_str(),
            data
        )
    }

    /// Generate SQL DELETE for signal table (InsertDelete strategy).
    pub fn to_delete_sql(&self, signal_table: &str) -> String {
        format!("DELETE FROM {} WHERE id = '{}'", signal_table, self.id)
    }
}

/// Buffer for chunk deduplication.
///
/// Holds snapshot rows and compares with streaming events to detect conflicts.
#[derive(Debug)]
pub struct ChunkBuffer {
    /// Buffered events keyed by primary key
    events: HashMap<String, CdcEvent>,
    /// Primary key column name (reserved for future conflict resolution)
    #[allow(dead_code)]
    key_column: String,
    /// Table being snapshotted
    table_name: String,
    /// Chunk ID
    chunk_id: String,
    /// Window open timestamp
    window_opened: Option<Instant>,
    /// Is window currently open
    is_open: bool,
    /// Events dropped due to conflicts
    dropped_count: u64,
}

impl ChunkBuffer {
    /// Create a new buffer for a chunk.
    pub fn new(chunk: &SnapshotChunk) -> Self {
        Self {
            events: HashMap::new(),
            key_column: chunk.key_column.clone(),
            table_name: chunk.table_name.clone(),
            chunk_id: chunk.chunk_id.clone(),
            window_opened: None,
            is_open: false,
            dropped_count: 0,
        }
    }

    /// Open the snapshot window.
    pub fn open_window(&mut self) {
        self.is_open = true;
        self.window_opened = Some(Instant::now());
        self.events.clear();
        debug!(
            chunk_id = %self.chunk_id,
            table = %self.table_name,
            "Snapshot window opened"
        );
    }

    /// Add a snapshot row to the buffer.
    pub fn add_row(&mut self, event: CdcEvent, key: String) {
        if self.is_open {
            self.events.insert(key, event);
        }
    }

    /// Check if a streaming event conflicts with buffered snapshot.
    ///
    /// Returns true if the event was handled (buffer entry dropped).
    pub fn check_conflict(&mut self, key: &str) -> bool {
        if !self.is_open {
            return false;
        }

        if self.events.remove(key).is_some() {
            self.dropped_count += 1;
            debug!(
                key = %key,
                chunk_id = %self.chunk_id,
                "Dropped buffered snapshot event due to streaming conflict"
            );
            true
        } else {
            false
        }
    }

    /// Close the window and return remaining events.
    pub fn close_window(&mut self) -> Vec<CdcEvent> {
        self.is_open = false;
        let duration = self.window_opened.map(|t| t.elapsed());
        let events: Vec<_> = self.events.drain().map(|(_, e)| e).collect();

        debug!(
            chunk_id = %self.chunk_id,
            table = %self.table_name,
            emitted = events.len(),
            dropped = self.dropped_count,
            duration_ms = ?duration.map(|d| d.as_millis()),
            "Snapshot window closed"
        );

        events
    }

    /// Check if window is open.
    pub fn is_window_open(&self) -> bool {
        self.is_open
    }

    /// Get buffer size.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get dropped count.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}

/// Context for an incremental snapshot execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalSnapshotContext {
    /// Snapshot ID
    pub snapshot_id: String,
    /// Tables being snapshotted (first is current)
    pub tables: VecDeque<SnapshotTable>,
    /// Current chunk (if any)
    pub current_chunk: Option<SnapshotChunk>,
    /// Started timestamp
    pub started_at: i64,
    /// Last activity timestamp
    pub last_activity: i64,
}

impl IncrementalSnapshotContext {
    /// Create a new snapshot context.
    pub fn new(tables: Vec<SnapshotTable>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            snapshot_id: format!("incr-snap-{}", Uuid::new_v4()),
            tables: tables.into_iter().collect(),
            current_chunk: None,
            started_at: now,
            last_activity: now,
        }
    }

    /// Get current table being snapshotted.
    pub fn current_table(&self) -> Option<&SnapshotTable> {
        self.tables.front()
    }

    /// Get mutable current table.
    pub fn current_table_mut(&mut self) -> Option<&mut SnapshotTable> {
        self.tables.front_mut()
    }

    /// Move to next table.
    pub fn next_table(&mut self) -> Option<SnapshotTable> {
        self.tables.pop_front()
    }

    /// Check if all tables are complete.
    pub fn is_complete(&self) -> bool {
        self.tables.is_empty()
    }

    /// Get total progress percentage.
    pub fn progress_percentage(&self) -> f64 {
        if self.tables.is_empty() {
            return 100.0;
        }

        let completed: u64 = self.tables.iter().map(|t| t.completed_chunks).sum();
        let total: u64 = self
            .tables
            .iter()
            .filter_map(|t| t.total_chunks)
            .sum::<u64>()
            .max(1);

        (completed as f64 / total as f64) * 100.0
    }
}

/// Statistics for incremental snapshot operations.
#[derive(Debug, Default)]
pub struct IncrementalSnapshotStats {
    /// Total snapshots started
    snapshots_started: AtomicU64,
    /// Total snapshots completed
    snapshots_completed: AtomicU64,
    /// Total snapshots failed
    snapshots_failed: AtomicU64,
    /// Total chunks processed
    chunks_processed: AtomicU64,
    /// Total rows snapshotted
    rows_snapshotted: AtomicU64,
    /// Events dropped due to conflicts
    events_dropped: AtomicU64,
    /// Total window open time (ms)
    window_time_ms: AtomicU64,
    /// Active snapshots
    active_snapshots: AtomicU64,
}

impl IncrementalSnapshotStats {
    /// Record snapshot started.
    pub fn record_started(&self) {
        self.snapshots_started.fetch_add(1, Ordering::Relaxed);
        self.active_snapshots.fetch_add(1, Ordering::Relaxed);
    }

    /// Record snapshot completed.
    pub fn record_completed(&self) {
        self.snapshots_completed.fetch_add(1, Ordering::Relaxed);
        self.active_snapshots.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record snapshot failed.
    pub fn record_failed(&self) {
        self.snapshots_failed.fetch_add(1, Ordering::Relaxed);
        self.active_snapshots.fetch_sub(1, Ordering::Relaxed);
    }

    /// Record chunk processed.
    pub fn record_chunk(&self, rows: u64, dropped: u64, window_time_ms: u64) {
        self.chunks_processed.fetch_add(1, Ordering::Relaxed);
        self.rows_snapshotted.fetch_add(rows, Ordering::Relaxed);
        self.events_dropped.fetch_add(dropped, Ordering::Relaxed);
        self.window_time_ms
            .fetch_add(window_time_ms, Ordering::Relaxed);
    }

    /// Get snapshot statistics.
    pub fn snapshot(&self) -> IncrementalSnapshotStatsSnapshot {
        IncrementalSnapshotStatsSnapshot {
            snapshots_started: self.snapshots_started.load(Ordering::Relaxed),
            snapshots_completed: self.snapshots_completed.load(Ordering::Relaxed),
            snapshots_failed: self.snapshots_failed.load(Ordering::Relaxed),
            chunks_processed: self.chunks_processed.load(Ordering::Relaxed),
            rows_snapshotted: self.rows_snapshotted.load(Ordering::Relaxed),
            events_dropped: self.events_dropped.load(Ordering::Relaxed),
            window_time_ms: self.window_time_ms.load(Ordering::Relaxed),
            active_snapshots: self.active_snapshots.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot statistics snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalSnapshotStatsSnapshot {
    pub snapshots_started: u64,
    pub snapshots_completed: u64,
    pub snapshots_failed: u64,
    pub chunks_processed: u64,
    pub rows_snapshotted: u64,
    pub events_dropped: u64,
    pub window_time_ms: u64,
    pub active_snapshots: u64,
}

/// Request to execute an incremental snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalSnapshotRequest {
    /// Tables to snapshot (fully qualified names)
    pub data_collections: Vec<String>,
    /// Snapshot type (always "incremental")
    pub snapshot_type: String,
    /// Additional conditions per table
    #[serde(default)]
    pub additional_conditions: Vec<AdditionalCondition>,
    /// Surrogate key per table
    #[serde(default)]
    pub surrogate_keys: HashMap<String, String>,
}

/// Additional condition for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdditionalCondition {
    /// Table name
    #[serde(rename = "data-collection")]
    pub data_collection: String,
    /// Filter condition (WHERE clause)
    pub filter: String,
}

impl IncrementalSnapshotRequest {
    /// Create a simple request for tables.
    pub fn new(tables: Vec<String>) -> Self {
        Self {
            data_collections: tables,
            snapshot_type: "incremental".to_string(),
            additional_conditions: Vec::new(),
            surrogate_keys: HashMap::new(),
        }
    }

    /// Add a condition for a table.
    pub fn with_condition(mut self, table: impl Into<String>, filter: impl Into<String>) -> Self {
        self.additional_conditions.push(AdditionalCondition {
            data_collection: table.into(),
            filter: filter.into(),
        });
        self
    }

    /// Add a surrogate key for a table.
    pub fn with_surrogate_key(mut self, table: impl Into<String>, key: impl Into<String>) -> Self {
        self.surrogate_keys.insert(table.into(), key.into());
        self
    }

    /// Get condition for a table.
    pub fn get_condition(&self, table: &str) -> Option<&str> {
        self.additional_conditions
            .iter()
            .find(|c| c.data_collection == table)
            .map(|c| c.filter.as_str())
    }

    /// Get surrogate key for a table.
    pub fn get_surrogate_key(&self, table: &str) -> Option<&str> {
        self.surrogate_keys.get(table).map(|s| s.as_str())
    }
}

/// Incremental snapshot coordinator.
///
/// Manages the lifecycle of incremental snapshots including:
/// - Chunk generation and sequencing
/// - Watermark signal generation
/// - Buffer management and deduplication
/// - Progress tracking and resumption
pub struct IncrementalSnapshotCoordinator {
    /// Configuration
    config: IncrementalSnapshotConfig,
    /// Current snapshot context
    context: Arc<RwLock<Option<IncrementalSnapshotContext>>>,
    /// Current chunk buffer
    buffer: Arc<RwLock<Option<ChunkBuffer>>>,
    /// Is snapshot active
    active: Arc<AtomicBool>,
    /// Is snapshot paused
    paused: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<IncrementalSnapshotStats>,
}

impl IncrementalSnapshotCoordinator {
    /// Create a new coordinator.
    pub fn new(config: IncrementalSnapshotConfig) -> Self {
        Self {
            config,
            context: Arc::new(RwLock::new(None)),
            buffer: Arc::new(RwLock::new(None)),
            active: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(IncrementalSnapshotStats::default()),
        }
    }

    /// Start an incremental snapshot.
    pub async fn start(&self, request: IncrementalSnapshotRequest) -> Result<String> {
        if self.active.load(Ordering::Acquire) {
            return Err(CdcError::InvalidState(
                "Incremental snapshot already in progress".to_string(),
            ));
        }

        // Build table list
        let tables: Vec<SnapshotTable> = request
            .data_collections
            .iter()
            .map(|table| {
                let mut st = SnapshotTable::new(table.clone(), vec!["id".to_string()]);
                if let Some(key) = request.get_surrogate_key(table) {
                    st = st.with_surrogate_key(key);
                }
                if let Some(cond) = request.get_condition(table) {
                    st = st.with_conditions(cond);
                }
                st
            })
            .collect();

        let context = IncrementalSnapshotContext::new(tables);
        let snapshot_id = context.snapshot_id.clone();

        *self.context.write().await = Some(context);
        self.active.store(true, Ordering::Release);
        self.paused.store(false, Ordering::Release);
        self.stats.record_started();

        info!(
            snapshot_id = %snapshot_id,
            tables = ?request.data_collections,
            "Incremental snapshot started"
        );

        Ok(snapshot_id)
    }

    /// Stop the current snapshot.
    pub async fn stop(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            return Ok(());
        }

        self.active.store(false, Ordering::Release);
        self.paused.store(false, Ordering::Release);

        // Clear buffer if window is open
        if let Some(mut buffer) = self.buffer.write().await.take() {
            if buffer.is_window_open() {
                let _ = buffer.close_window();
            }
        }

        if let Some(context) = self.context.write().await.take() {
            info!(
                snapshot_id = %context.snapshot_id,
                progress = context.progress_percentage(),
                "Incremental snapshot stopped"
            );
        }

        Ok(())
    }

    /// Pause the current snapshot.
    pub async fn pause(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            return Err(CdcError::InvalidState(
                "No incremental snapshot in progress".to_string(),
            ));
        }

        self.paused.store(true, Ordering::Release);

        if let Some(ref mut context) = *self.context.write().await {
            if let Some(ref mut table) = context.current_table_mut() {
                table.state = IncrementalSnapshotState::Paused;
            }
            info!(
                snapshot_id = %context.snapshot_id,
                "Incremental snapshot paused"
            );
        }

        Ok(())
    }

    /// Resume a paused snapshot.
    pub async fn resume(&self) -> Result<()> {
        if !self.active.load(Ordering::Acquire) {
            return Err(CdcError::InvalidState(
                "No incremental snapshot in progress".to_string(),
            ));
        }

        self.paused.store(false, Ordering::Release);

        if let Some(ref mut context) = *self.context.write().await {
            if let Some(ref mut table) = context.current_table_mut() {
                table.state = IncrementalSnapshotState::Running;
            }
            info!(
                snapshot_id = %context.snapshot_id,
                "Incremental snapshot resumed"
            );
        }

        Ok(())
    }

    /// Check if snapshot is active.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Check if snapshot is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    /// Get the next chunk to process.
    pub async fn next_chunk(&self) -> Result<Option<SnapshotChunk>> {
        if !self.active.load(Ordering::Acquire) || self.paused.load(Ordering::Acquire) {
            return Ok(None);
        }

        let mut context_guard = self.context.write().await;
        let context = match context_guard.as_mut() {
            Some(ctx) => ctx,
            None => return Ok(None),
        };

        // Get current table info for chunk creation
        let (table_name, completed_chunks, chunk_key, last_key, max_key, conditions) = {
            let table = match context.current_table() {
                Some(t) => t,
                None => return Ok(None),
            };
            (
                table.table_name.clone(),
                table.completed_chunks,
                table.chunk_key().to_string(),
                table.last_key.clone(),
                table.max_key.clone(),
                table.additional_conditions.clone(),
            )
        };

        // Create next chunk
        let chunk = SnapshotChunk::new(table_name, completed_chunks, chunk_key)
            .with_range(last_key, max_key);

        let chunk = if let Some(cond) = conditions {
            chunk.with_conditions(cond)
        } else {
            chunk
        };

        // Update context state
        context.current_chunk = Some(chunk.clone());
        if let Some(table) = context.current_table_mut() {
            table.state = IncrementalSnapshotState::Running;
        }

        Ok(Some(chunk))
    }

    /// Generate watermark signal for opening a chunk window.
    pub fn open_window_signal(&self, chunk: &SnapshotChunk) -> WatermarkSignal {
        WatermarkSignal::open(chunk)
    }

    /// Generate watermark signal for closing a chunk window.
    pub fn close_window_signal(&self, chunk: &SnapshotChunk) -> WatermarkSignal {
        WatermarkSignal::close(chunk)
    }

    /// Open the deduplication window for a chunk.
    pub async fn open_window(&self, chunk: &SnapshotChunk) {
        let mut buffer = ChunkBuffer::new(chunk);
        buffer.open_window();
        *self.buffer.write().await = Some(buffer);
    }

    /// Add a snapshot row to the buffer.
    pub async fn buffer_row(&self, event: CdcEvent, key: String) {
        if let Some(ref mut buffer) = *self.buffer.write().await {
            buffer.add_row(event, key);
        }
    }

    /// Check if a streaming event conflicts with the buffer.
    ///
    /// Call this for every streaming event while a window is open.
    /// Returns true if the event caused a buffer entry to be dropped.
    pub async fn check_streaming_conflict(&self, table: &str, key: &str) -> bool {
        if let Some(ref mut buffer) = *self.buffer.write().await {
            if buffer.table_name == table {
                return buffer.check_conflict(key);
            }
        }
        false
    }

    /// Close the window and get remaining events to emit.
    pub async fn close_window(&self) -> Result<Vec<CdcEvent>> {
        let mut buffer_guard = self.buffer.write().await;
        let buffer = match buffer_guard.as_mut() {
            Some(b) => b,
            None => return Ok(Vec::new()),
        };

        let window_start = buffer.window_opened;
        let dropped = buffer.dropped_count();
        let events = buffer.close_window();
        let rows = events.len() as u64;

        // Record stats
        let window_time_ms = window_start
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0);
        self.stats.record_chunk(rows, dropped, window_time_ms);

        // Update context
        if let Some(ref mut context) = *self.context.write().await {
            if let Some(ref mut table) = context.current_table_mut() {
                table.completed_chunks += 1;
                table.rows_processed += rows;
            }
            context.current_chunk = None;
            context.last_activity = chrono::Utc::now().timestamp_millis();
        }

        Ok(events)
    }

    /// Mark current table as complete and move to next.
    pub async fn complete_current_table(&self) -> Result<Option<String>> {
        let mut context_guard = self.context.write().await;
        let context = match context_guard.as_mut() {
            Some(ctx) => ctx,
            None => return Ok(None),
        };

        // Complete current table
        if let Some(completed) = context.next_table() {
            info!(
                table = %completed.table_name,
                rows = completed.rows_processed,
                chunks = completed.completed_chunks,
                "Table incremental snapshot completed"
            );
        }

        // Check if all done
        if context.is_complete() {
            drop(context_guard);
            self.active.store(false, Ordering::Release);
            self.stats.record_completed();

            if let Some(ctx) = self.context.read().await.as_ref() {
                info!(
                    snapshot_id = %ctx.snapshot_id,
                    "Incremental snapshot completed"
                );
            }
            return Ok(None);
        }

        // Return next table name
        Ok(context.current_table().map(|t| t.table_name.clone()))
    }

    /// Get current snapshot context (for serialization/resumption).
    pub async fn get_context(&self) -> Option<IncrementalSnapshotContext> {
        self.context.read().await.clone()
    }

    /// Restore from a saved context.
    pub async fn restore(&self, context: IncrementalSnapshotContext) -> Result<()> {
        if self.active.load(Ordering::Acquire) {
            return Err(CdcError::InvalidState(
                "Cannot restore: snapshot already in progress".to_string(),
            ));
        }

        let snapshot_id = context.snapshot_id.clone();
        *self.context.write().await = Some(context);
        self.active.store(true, Ordering::Release);
        self.paused.store(false, Ordering::Release);
        self.stats.record_started();

        info!(
            snapshot_id = %snapshot_id,
            "Incremental snapshot restored"
        );

        Ok(())
    }

    /// Get configuration.
    pub fn config(&self) -> &IncrementalSnapshotConfig {
        &self.config
    }

    /// Get statistics.
    pub fn stats(&self) -> IncrementalSnapshotStatsSnapshot {
        self.stats.snapshot()
    }

    /// Check if window is currently open.
    pub async fn is_window_open(&self) -> bool {
        if let Some(ref buffer) = *self.buffer.read().await {
            return buffer.is_window_open();
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a test CdcEvent for use in tests.
    fn test_event() -> CdcEvent {
        CdcEvent::insert(
            "postgres",
            "testdb",
            "public",
            "orders",
            serde_json::json!({}),
            0,
        )
    }

    #[test]
    fn test_watermark_strategy() {
        assert!(!WatermarkStrategy::InsertInsert.requires_delete());
        assert!(WatermarkStrategy::InsertDelete.requires_delete());
    }

    #[test]
    fn test_config_builder() {
        let config = IncrementalSnapshotConfig::builder()
            .chunk_size(2048)
            .watermark_strategy(WatermarkStrategy::InsertDelete)
            .max_buffer_memory(128 * 1024 * 1024)
            .build();

        assert_eq!(config.chunk_size, 2048);
        assert_eq!(config.watermark_strategy, WatermarkStrategy::InsertDelete);
        assert_eq!(config.max_buffer_memory, 128 * 1024 * 1024);
    }

    #[test]
    fn test_snapshot_table() {
        let table = SnapshotTable::new("public.orders", vec!["id".to_string()])
            .with_surrogate_key("order_id")
            .with_conditions("status = 'active'");

        assert_eq!(table.chunk_key(), "order_id");
        assert_eq!(
            table.additional_conditions,
            Some("status = 'active'".to_string())
        );
    }

    #[test]
    fn test_snapshot_chunk_sql() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id")
            .with_range(Some("100".to_string()), Some("200".to_string()))
            .with_conditions("status = 'active'");

        let sql = chunk.to_sql(1024);
        assert!(sql.contains("public.orders"));
        assert!(sql.contains("id > '100'"));
        assert!(sql.contains("id <= '200'"));
        assert!(sql.contains("status = 'active'"));
        assert!(sql.contains("LIMIT 1024"));
    }

    #[test]
    fn test_watermark_signal() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let open = WatermarkSignal::open(&chunk);
        let close = WatermarkSignal::close(&chunk);

        assert_eq!(open.signal_type, WatermarkType::SnapshotWindowOpen);
        assert_eq!(close.signal_type, WatermarkType::SnapshotWindowClose);
        assert!(open.id.ends_with("-open"));
        assert!(close.id.ends_with("-close"));
    }

    #[test]
    fn test_chunk_buffer_deduplication() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let mut buffer = ChunkBuffer::new(&chunk);

        // Window not open - should not buffer
        buffer.add_row(test_event(), "key1".to_string());
        assert!(buffer.is_empty());

        // Open window
        buffer.open_window();
        assert!(buffer.is_window_open());

        // Add some rows
        buffer.add_row(test_event(), "key1".to_string());
        buffer.add_row(test_event(), "key2".to_string());
        buffer.add_row(test_event(), "key3".to_string());
        assert_eq!(buffer.len(), 3);

        // Simulate streaming conflict
        assert!(buffer.check_conflict("key2"));
        assert!(!buffer.check_conflict("key4")); // Non-existent key
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.dropped_count(), 1);

        // Close window
        let events = buffer.close_window();
        assert_eq!(events.len(), 2);
        assert!(!buffer.is_window_open());
    }

    #[test]
    fn test_incremental_snapshot_context() {
        let tables = vec![
            SnapshotTable::new("public.orders", vec!["id".to_string()]),
            SnapshotTable::new("public.customers", vec!["id".to_string()]),
        ];
        let mut context = IncrementalSnapshotContext::new(tables);

        assert_eq!(
            context.current_table().map(|t| &t.table_name),
            Some(&"public.orders".to_string())
        );
        assert!(!context.is_complete());

        // Complete first table
        context.next_table();
        assert_eq!(
            context.current_table().map(|t| &t.table_name),
            Some(&"public.customers".to_string())
        );

        // Complete second table
        context.next_table();
        assert!(context.is_complete());
    }

    #[test]
    fn test_snapshot_request() {
        let request = IncrementalSnapshotRequest::new(vec![
            "public.orders".to_string(),
            "public.customers".to_string(),
        ])
        .with_condition("public.orders", "status = 'active'")
        .with_surrogate_key("public.orders", "order_id");

        assert_eq!(
            request.get_condition("public.orders"),
            Some("status = 'active'")
        );
        assert_eq!(request.get_surrogate_key("public.orders"), Some("order_id"));
        assert!(request.get_condition("public.customers").is_none());
    }

    #[tokio::test]
    async fn test_coordinator_lifecycle() {
        let config = IncrementalSnapshotConfig::builder().chunk_size(100).build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        // Start snapshot
        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        let snapshot_id = coordinator.start(request).await.unwrap();
        assert!(!snapshot_id.is_empty());
        assert!(coordinator.is_active());

        // Cannot start another while active
        let request2 = IncrementalSnapshotRequest::new(vec!["public.customers".to_string()]);
        assert!(coordinator.start(request2).await.is_err());

        // Pause and resume
        coordinator.pause().await.unwrap();
        assert!(coordinator.is_paused());
        coordinator.resume().await.unwrap();
        assert!(!coordinator.is_paused());

        // Stop
        coordinator.stop().await.unwrap();
        assert!(!coordinator.is_active());
    }

    #[tokio::test]
    async fn test_coordinator_chunking() {
        let config = IncrementalSnapshotConfig::builder().chunk_size(100).build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        // Start snapshot
        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        // Get first chunk
        let chunk = coordinator.next_chunk().await.unwrap();
        assert!(chunk.is_some());
        let chunk = chunk.unwrap();
        assert_eq!(chunk.table_name, "public.orders");
        assert_eq!(chunk.sequence, 0);

        // Open window
        coordinator.open_window(&chunk).await;
        assert!(coordinator.is_window_open().await);

        // Buffer some rows
        coordinator.buffer_row(test_event(), "1".to_string()).await;
        coordinator.buffer_row(test_event(), "2".to_string()).await;

        // Check conflict
        assert!(
            coordinator
                .check_streaming_conflict("public.orders", "1")
                .await
        );
        assert!(
            !coordinator
                .check_streaming_conflict("public.orders", "3")
                .await
        );

        // Close window
        let events = coordinator.close_window().await.unwrap();
        assert_eq!(events.len(), 1); // Only "2" remains

        // Check stats
        let stats = coordinator.stats();
        assert_eq!(stats.chunks_processed, 1);
        assert_eq!(stats.rows_snapshotted, 1);
        assert_eq!(stats.events_dropped, 1);
    }

    #[test]
    fn test_snapshot_state() {
        assert!(IncrementalSnapshotState::Running.is_active());
        assert!(IncrementalSnapshotState::Paused.is_active());
        assert!(!IncrementalSnapshotState::Completed.is_active());
        assert!(IncrementalSnapshotState::Paused.can_resume());
        assert!(!IncrementalSnapshotState::Running.can_resume());
    }
}
