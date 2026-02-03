//! # Incremental Snapshots
//!
//! Watermark-based incremental snapshot implementation using the DBLog algorithm.
//! Allows re-snapshotting tables while streaming continues.
//!
//! ## How It Works
//!
//! 1. **Chunk-based processing**: Tables are split into chunks by primary key
//! 2. **Watermark signals**: Open/close window markers for deduplication
//! 3. **Buffer deduplication**: Compare snapshot events with streaming events
//! 4. **Resume support**: Store chunk state for restart recovery
//! 5. **Parallel processing**: Multiple concurrent chunk windows for high throughput
//!
//! ## Primary Key Chunking
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │ Chunk 1: id > 0 AND id <= 1000                          │
//! │ Chunk 2: id > 1000 AND id <= 2000                       │
//! │ Chunk 3: id > 2000 AND id <= 3000                       │
//! └─────────────────────────────────────────────────────────┘
//! ```
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
//! - Streaming events with matching PKs and event_ts >= window_opened_ts → drop
//! - Delete events always drop buffered rows (deletes win)
//! - On window close: remaining buffer entries emitted as READ events
//! ```
//!
//! ## Time Column as Watermark (NOT for Chunking!)
//!
//! Time columns should be used for **deduplication watermarks**, not chunking:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ CORRECT APPROACH                                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │ 1. Chunk by PK: WHERE id > $1 AND id <= $2                      │
//! │ 2. Record window_opened_ts (DB timestamp) when opening window   │
//! │ 3. For streaming events:                                        │
//! │    - Same key + event_ts >= window_opened_ts → drop snapshot    │
//! │    - Same key + DELETE → always drop snapshot (deletes win)     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::incremental_snapshot::{
//!     IncrementalSnapshotConfig, IncrementalSnapshotCoordinator,
//!     ChunkingStrategy, SnapshotKey
//! };
//!
//! // Configure with parallel processing
//! let config = IncrementalSnapshotConfig::builder()
//!     .chunk_size(1024)
//!     .max_concurrent_chunks(4)
//!     .chunking_strategy(ChunkingStrategy::PrimaryKey)
//!     .build();
//!
//! let coordinator = IncrementalSnapshotCoordinator::new(config);
//!
//! // Start snapshot
//! coordinator.start(request).await?;
//!
//! // Get DB timestamp for watermark (e.g., SELECT EXTRACT(EPOCH FROM NOW()) * 1000)
//! let watermark_ts = get_db_timestamp().await?;
//!
//! // Process chunks in parallel
//! let chunks = vec![chunk1, chunk2, chunk3, chunk4];
//! for chunk in &chunks {
//!     coordinator.open_window_with_watermark(chunk, watermark_ts).await?;
//! }
//!
//! // Execute queries and buffer results
//! for (chunk, rows) in chunks.iter().zip(results) {
//!     for row in rows {
//!         let key = SnapshotKey::from_row(&row, &key_columns)?;
//!         coordinator.buffer_row_for_chunk_with_key(&chunk.chunk_id, event, key).await;
//!     }
//! }
//!
//! // Handle streaming events during window (with watermark check)
//! for event in streaming_events {
//!     let key = SnapshotKey::from_row(&event.after, &key_columns)?;
//!     let is_delete = event.op == "d";
//!     coordinator.check_streaming_conflict_with_timestamp(
//!         "public.orders",
//!         &key,
//!         event.ts_ms,
//!         is_delete,
//!     ).await;
//! }
//!
//! // Close windows and emit events
//! for chunk in &chunks {
//!     let events = coordinator.close_window_for_chunk(&chunk.chunk_id).await?;
//!     emit_events(events).await?;
//! }
//! ```
//!
//! ## Signal Table Watermarks
//!
//! Incremental snapshots use the signal table to write watermark markers:
//!
//! | Signal ID | Type | Data |
//! |-----------|------|------|
//! | `snap-1-open` | `snapshot-window-open` | `{"chunk_id": "...", "table": "...", "watermark_ts": ...}` |
//! | `snap-1-close` | `snapshot-window-close` | `{"chunk_id": "...", "table": "..."}` |

use crate::common::{CdcError, CdcEvent, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ============================================================================
// Chunking Strategy
// ============================================================================

/// Strategy for splitting tables into chunks during incremental snapshots.
///
/// # Strategy Selection
///
/// - **PrimaryKey** (default): Chunk by primary key or surrogate key ranges.
///   This is the **only safe choice for general CDC** because:
///   - Keys are immutable (don't change across commits)
///   - Chunk boundaries are stable and predictable
///   - Deletes and updates are handled correctly
///
/// - **Custom**: For complex partitioning schemes (e.g., composite keys, hash-based).
///   You're responsible for ensuring chunk boundaries are stable.
///
/// # ⚠️ Why NOT Time-Based Chunking
///
/// Time-based chunking (chunking by timestamp column) is **fundamentally unsafe**
/// for general CDC and has been intentionally omitted. The problems:
///
/// 1. **Late commits**: A row with `created_at = 10:00` committed at 10:05
///    lands in the wrong slice if you've already processed 10:00-10:02.
/// 2. **Updates moving rows**: `UPDATE ... SET updated_at = NOW()` moves a row
///    from an old slice to a new one, causing duplicates or misses.
/// 3. **Deletes are painful**: A delete for `created_at = 10:00` arrives at 10:30 —
///    which slice should handle it?
///
/// # Correct Use of Time Columns
///
/// Time columns should be used as **watermarks for deduplication**, not for chunking.
/// When opening a chunk window, record the DB/CDC timestamp (`window_opened_ts`).
/// When a streaming event arrives:
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ CORRECT: Chunk by PK, use time as watermark                      │
/// ├─────────────────────────────────────────────────────────────────┤
/// │ 1. Chunk by PK ranges: WHERE id > $1 AND id <= $2               │
/// │ 2. Record window_opened_ts when opening chunk window            │
/// │ 3. For streaming events with same key:                          │
/// │    - If event_ts >= window_opened_ts → drop snapshot row        │
/// │    - If delete → always drop snapshot row (deletes win)         │
/// └─────────────────────────────────────────────────────────────────┘
/// ```
///
/// This approach ensures correct deduplication for CDC workloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChunkingStrategy {
    /// Chunk by primary key range (default, only safe option for general CDC)
    ///
    /// Divides the table by primary key value ranges. Best for tables with
    /// auto-incrementing integer IDs or any stable key column.
    ///
    /// ```text
    /// Chunk 1: id > 0 AND id <= 1000
    /// Chunk 2: id > 1000 AND id <= 2000
    /// ```
    #[default]
    PrimaryKey,

    /// Custom chunking with user-defined boundaries
    ///
    /// For complex scenarios requiring custom logic. You're responsible for
    /// ensuring chunk boundaries are stable and don't cause row movements.
    Custom {
        /// Column to chunk on
        column: String,
        /// Pre-computed chunk boundaries
        boundaries: Vec<ChunkBoundary>,
    },
}

impl ChunkingStrategy {
    /// Create a custom chunking strategy.
    pub fn custom(column: impl Into<String>, boundaries: Vec<ChunkBoundary>) -> Self {
        Self::Custom {
            column: column.into(),
            boundaries,
        }
    }

    /// Get the column used for chunking (if applicable).
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::PrimaryKey => None,
            Self::Custom { column, .. } => Some(column),
        }
    }

    /// Check if this is primary key based chunking.
    pub fn is_primary_key(&self) -> bool {
        matches!(self, Self::PrimaryKey)
    }
}

/// A boundary for custom chunking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkBoundary {
    /// Lower bound (exclusive, None for first chunk)
    pub from: Option<serde_json::Value>,
    /// Upper bound (inclusive)
    pub to: Option<serde_json::Value>,
}

impl ChunkBoundary {
    /// Create a new chunk boundary.
    pub fn new(from: Option<serde_json::Value>, to: Option<serde_json::Value>) -> Self {
        Self { from, to }
    }

    /// Create boundaries for time ranges.
    pub fn time_range(
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            from: Some(serde_json::json!(from.to_rfc3339())),
            to: Some(serde_json::json!(to.to_rfc3339())),
        }
    }
}

// ============================================================================
// Query Builder Trait
// ============================================================================

/// Trait for building database-specific parameterized queries.
///
/// Implement this trait for your database to generate safe, parameterized queries
/// for incremental snapshots. This avoids SQL injection vulnerabilities.
///
/// # Example
///
/// ```rust,ignore
/// struct PostgresQueryBuilder;
///
/// impl ChunkQueryBuilder for PostgresQueryBuilder {
///     fn build_chunk_query(&self, chunk: &SnapshotChunk, chunk_size: usize) -> QuerySpec {
///         let mut params = Vec::new();
///         let mut conditions = Vec::new();
///
///         if let Some(from) = &chunk.key_range.from {
///             params.push(from.clone());
///             conditions.push(format!("{} > ${}", chunk.key_column, params.len()));
///         }
///         // ... more conditions
///
///         QuerySpec {
///             sql: format!("SELECT * FROM {} WHERE {} ORDER BY {} LIMIT ${}",
///                 chunk.table_name, conditions.join(" AND "), chunk.key_column, params.len() + 1),
///             params,
///         }
///     }
/// }
/// ```
pub trait ChunkQueryBuilder: Send + Sync {
    /// Build a parameterized query for a chunk.
    fn build_chunk_query(&self, chunk: &SnapshotChunk, chunk_size: usize) -> QuerySpec;

    /// Build a query to get the min/max of a column for chunking.
    fn build_bounds_query(&self, table: &str, column: &str) -> QuerySpec;

    /// Build a query to get the current database timestamp for watermarking.
    ///
    /// This timestamp is used as `window_opened_ts` for deduplication.
    /// Returns a query that produces a single integer (Unix milliseconds).
    fn build_current_timestamp_query(&self) -> QuerySpec;
}

/// A parameterized query specification.
#[derive(Debug, Clone)]
pub struct QuerySpec {
    /// The SQL query with parameter placeholders ($1, $2, etc. for Postgres; ? for MySQL)
    pub sql: String,
    /// The parameter values in order
    pub params: Vec<serde_json::Value>,
}

impl QuerySpec {
    /// Create a new query spec.
    pub fn new(sql: impl Into<String>, params: Vec<serde_json::Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    /// Create an empty/no-op query.
    pub fn empty() -> Self {
        Self {
            sql: String::new(),
            params: Vec::new(),
        }
    }
}

// ============================================================================
// Snapshot Key Abstraction
// ============================================================================

/// A key used for snapshot deduplication.
///
/// Supports both single-column and composite primary keys. This abstraction
/// allows the buffer to handle complex key structures without breaking changes.
///
/// # Examples
///
/// ```rust,ignore
/// // Single column key
/// let key = SnapshotKey::single("12345");
///
/// // Composite key (multiple columns)
/// let key = SnapshotKey::composite(vec!["US".into(), "12345".into()]);
/// ```
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub enum SnapshotKey {
    /// Single-column primary key (most common)
    Single(String),
    /// Composite primary key (multiple columns)
    Composite(Vec<serde_json::Value>),
}

impl SnapshotKey {
    /// Create a single-column key.
    pub fn single(value: impl Into<String>) -> Self {
        Self::Single(value.into())
    }

    /// Create a composite key from multiple values.
    pub fn composite(values: Vec<serde_json::Value>) -> Self {
        Self::Composite(values)
    }

    /// Create a key from a row and key columns.
    ///
    /// # Arguments
    /// * `row` - The row data as a JSON object
    /// * `key_columns` - Column names that form the primary key
    pub fn from_row(row: &serde_json::Value, key_columns: &[String]) -> Option<Self> {
        if key_columns.is_empty() {
            return None;
        }

        if key_columns.len() == 1 {
            // Single column key
            row.get(&key_columns[0]).map(|v| match v {
                serde_json::Value::String(s) => Self::Single(s.clone()),
                other => Self::Single(other.to_string()),
            })
        } else {
            // Composite key
            let values: Option<Vec<_>> = key_columns
                .iter()
                .map(|col| row.get(col).cloned())
                .collect();
            values.map(Self::Composite)
        }
    }

    /// Convert to a canonical string representation for hashing.
    pub fn to_canonical_string(&self) -> String {
        match self {
            Self::Single(s) => s.clone(),
            Self::Composite(values) => {
                // Use JSON array format for deterministic ordering
                serde_json::to_string(values).unwrap_or_else(|_| format!("{:?}", values))
            }
        }
    }

    /// Check if this is a composite key.
    pub fn is_composite(&self) -> bool {
        matches!(self, Self::Composite(_))
    }
}

impl PartialEq for SnapshotKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Single(a), Self::Single(b)) => a == b,
            (Self::Composite(a), Self::Composite(b)) => {
                // Compare JSON values
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
            }
            _ => false,
        }
    }
}

impl Hash for SnapshotKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Single(s) => {
                state.write_u8(0);
                s.hash(state);
            }
            Self::Composite(_) => {
                state.write_u8(1);
                // Hash the canonical JSON representation
                self.to_canonical_string().hash(state);
            }
        }
    }
}

impl std::fmt::Display for SnapshotKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_canonical_string())
    }
}

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
///
/// # Concurrency Model
///
/// The coordinator supports **parallel chunk processing** with multiple concurrent windows.
/// Use `max_concurrent_chunks` to control parallelism (default: 4). Each open window
/// maintains its own deduplication buffer, and streaming events are checked against
/// ALL open windows for conflict detection.
///
/// Higher concurrency improves throughput but increases memory usage proportionally.
/// Consider the trade-off based on your workload and available resources.
///
/// # Chunking Strategies
///
/// - **PrimaryKey** (default): Chunk by primary key ranges. Best for tables with
///   auto-incrementing IDs.
/// - **TimeBased**: Chunk by timestamp ranges. Best for time-series data.
/// - **Custom**: Provide your own chunk boundaries.
///
/// # Memory Management
///
/// Total memory usage scales with `max_concurrent_chunks * max_buffer_memory`.
/// The `max_buffer_memory` setting enforces a soft limit per buffer.
/// Monitor `buffer_stats_all()` for aggregate memory usage across all windows.
#[derive(Debug, Clone)]
pub struct IncrementalSnapshotConfig {
    /// Number of rows per chunk (default: 1024)
    pub chunk_size: usize,
    /// Chunking strategy (default: PrimaryKey)
    pub chunking_strategy: ChunkingStrategy,
    /// Watermark strategy
    pub watermark_strategy: WatermarkStrategy,
    /// Maximum memory for chunk buffer in bytes (default: 64MB)
    ///
    /// This is a soft limit - the buffer will emit warnings when exceeded
    /// but won't hard-fail. Use `max_buffer_rows` for a hard limit.
    pub max_buffer_memory: usize,
    /// Maximum rows in buffer (hard limit, default: 100_000)
    ///
    /// Unlike `max_buffer_memory`, this is enforced strictly.
    /// Rows beyond this limit are dropped with a warning.
    pub max_buffer_rows: usize,
    /// Query timeout per chunk
    pub chunk_timeout: Duration,
    /// Delay between chunks (backpressure)
    pub inter_chunk_delay: Option<Duration>,
    /// Maximum concurrent chunk queries (default: 4)
    ///
    /// Controls how many chunk windows can be open simultaneously for parallel
    /// processing. Higher values improve throughput but increase memory usage.
    ///
    /// Each concurrent chunk maintains its own deduplication buffer, so total
    /// memory usage is approximately `max_concurrent_chunks * max_buffer_memory`.
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
            chunking_strategy: ChunkingStrategy::PrimaryKey,
            watermark_strategy: WatermarkStrategy::InsertInsert,
            max_buffer_memory: 64 * 1024 * 1024, // 64MB per buffer
            max_buffer_rows: 100_000,
            chunk_timeout: Duration::from_secs(60),
            inter_chunk_delay: None,
            max_concurrent_chunks: 4, // Parallel processing enabled
            allow_surrogate_key: true,
            signal_table: "rivven_signal".to_string(),
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

    /// Set chunking strategy.
    pub fn chunking_strategy(mut self, strategy: ChunkingStrategy) -> Self {
        self.config.chunking_strategy = strategy;
        self
    }

    /// Set watermark strategy.
    pub fn watermark_strategy(mut self, strategy: WatermarkStrategy) -> Self {
        self.config.watermark_strategy = strategy;
        self
    }

    /// Set maximum buffer memory (soft limit).
    pub fn max_buffer_memory(mut self, bytes: usize) -> Self {
        self.config.max_buffer_memory = bytes;
        self
    }

    /// Set maximum buffer rows (hard limit).
    pub fn max_buffer_rows(mut self, rows: usize) -> Self {
        self.config.max_buffer_rows = rows.max(1);
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
    /// Key range for this chunk
    pub key_range: KeyRange,
    /// Key column name (primary key or surrogate)
    pub key_column: String,
    /// Additional filter conditions (safe, parameterized)
    pub conditions: Option<String>,
    /// Chunk state
    pub state: ChunkState,
    /// Chunking strategy used
    pub strategy: ChunkingStrategy,
}

/// Range specification for a chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRange {
    /// Lower bound (exclusive, None for first chunk)
    pub from: Option<serde_json::Value>,
    /// Upper bound (inclusive, None for last chunk)
    pub to: Option<serde_json::Value>,
}

impl KeyRange {
    /// Create a new key range.
    pub fn new(from: Option<serde_json::Value>, to: Option<serde_json::Value>) -> Self {
        Self { from, to }
    }

    /// Create a range from string bounds (for backwards compatibility).
    pub fn from_strings(from: Option<String>, to: Option<String>) -> Self {
        Self {
            from: from.map(serde_json::Value::String),
            to: to.map(serde_json::Value::String),
        }
    }

    /// Create a time range.
    pub fn time_range(
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            from: Some(serde_json::json!(from.to_rfc3339())),
            to: Some(serde_json::json!(to.to_rfc3339())),
        }
    }

    /// Check if this range is bounded on both ends.
    pub fn is_bounded(&self) -> bool {
        self.from.is_some() && self.to.is_some()
    }
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
    /// Create a new chunk with primary key chunking.
    pub fn new(
        table_name: impl Into<String>,
        sequence: u64,
        key_column: impl Into<String>,
    ) -> Self {
        Self {
            chunk_id: format!("chunk-{}-{}", Uuid::new_v4(), sequence),
            table_name: table_name.into(),
            sequence,
            key_range: KeyRange::new(None, None),
            key_column: key_column.into(),
            conditions: None,
            state: ChunkState::Pending,
            strategy: ChunkingStrategy::PrimaryKey,
        }
    }

    /// Set key range (for primary key chunking).
    pub fn with_range(mut self, from: Option<String>, to: Option<String>) -> Self {
        self.key_range = KeyRange::from_strings(from, to);
        self
    }

    /// Set key range with JSON values.
    pub fn with_key_range(mut self, range: KeyRange) -> Self {
        self.key_range = range;
        self
    }

    /// Set additional conditions.
    ///
    /// These should be parameterized conditions that will be AND-ed with the chunk range.
    pub fn with_conditions(mut self, conditions: impl Into<String>) -> Self {
        self.conditions = Some(conditions.into());
        self
    }

    /// Get the key range bounds as strings (for backwards compatibility).
    ///
    /// Returns (from_key, to_key).
    pub fn key_bounds_as_strings(&self) -> (Option<String>, Option<String>) {
        let from = self.key_range.from.as_ref().map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        let to = self.key_range.to.as_ref().map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        });
        (from, to)
    }

    /// Check if this uses primary key chunking (the recommended approach).
    pub fn is_primary_key_based(&self) -> bool {
        self.strategy.is_primary_key()
    }

    /// Get query parameters for use with a [`ChunkQueryBuilder`].
    ///
    /// Returns (from_bound, to_bound, key_column, conditions).
    pub fn query_params(
        &self,
    ) -> (
        Option<&serde_json::Value>,
        Option<&serde_json::Value>,
        &str,
        Option<&str>,
    ) {
        (
            self.key_range.from.as_ref(),
            self.key_range.to.as_ref(),
            &self.key_column,
            self.conditions.as_deref(),
        )
    }
}

/// Watermark signal for snapshot window.
///
/// Watermarks are written to the signal table to mark window boundaries. These markers
/// flow through the CDC stream, enabling the deduplication logic to know when to compare
/// buffered snapshot rows against streaming events.
///
/// # Lifecycle
///
/// ```text
/// 1. Get current DB timestamp (watermark_ts)
/// 2. Coordinator writes OPEN marker → Signal table INSERT
/// 3. OPEN marker flows through CDC stream → Deduplication window starts
/// 4. Coordinator executes chunk query → Rows buffered locally
/// 5. Coordinator writes CLOSE marker → Signal table INSERT (or DELETE of OPEN)
/// 6. CLOSE marker flows through CDC → Buffer flushed, window ends
/// ```
///
/// # Watermark Timestamp
///
/// The `watermark_ts` field stores the DB timestamp at window open. This is critical
/// for correct deduplication:
///
/// - Streaming events with `event_ts >= watermark_ts` supersede snapshot rows
/// - Streaming events with `event_ts < watermark_ts` are ignored (snapshot is fresher)
/// - DELETE events always supersede (deletes win)
///
/// # Crash Recovery
///
/// **Orphaned markers**: If the process crashes between OPEN and CLOSE, an orphaned
/// OPEN marker may remain in the signal table. Recovery strategies:
///
/// 1. **On restart**: Query signal table for OPEN markers without matching CLOSE
/// 2. **Cleanup**: Either delete orphaned markers or re-process the chunks they represent
/// 3. **Timeout**: Markers older than a threshold (e.g., 1 hour) can be assumed orphaned
///
/// The `InsertDelete` strategy helps reduce orphaned markers since CLOSE deletes OPEN,
/// but can still leave orphans if crash happens during the chunk query phase.
///
/// # Signal Table Schema
///
/// The signal table should have this structure:
///
/// ```sql
/// CREATE TABLE rivven_signal (
///     id VARCHAR(42) PRIMARY KEY,
///     type VARCHAR(32) NOT NULL,
///     data TEXT
/// );
/// ```
///
/// # Cleanup Responsibility
///
/// - **InsertInsert**: Caller is responsible for periodic cleanup of old markers
/// - **InsertDelete**: CLOSE markers delete OPEN markers; only CLOSE markers accumulate
///
/// Recommendation: Run periodic cleanup for markers older than the maximum expected
/// snapshot duration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatermarkSignal {
    /// Signal ID (format: `{chunk_id}-open` or `{chunk_id}-close`)
    pub id: String,
    /// Signal type (snapshot-window-open or snapshot-window-close)
    pub signal_type: WatermarkType,
    /// Chunk being processed
    pub chunk_id: String,
    /// Table name
    pub table_name: String,
    /// Timestamp of signal creation (Unix millis, local clock)
    pub timestamp: i64,
    /// Watermark timestamp (Unix millis, DB clock)
    ///
    /// Only present on OPEN signals. This is the DB timestamp at the moment
    /// the window was opened, used for deduplication decisions.
    pub watermark_ts: Option<i64>,
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
    /// Get signal type as a string.
    pub fn as_str(&self) -> &str {
        match self {
            WatermarkType::SnapshotWindowOpen => "snapshot-window-open",
            WatermarkType::SnapshotWindowClose => "snapshot-window-close",
        }
    }
}

impl WatermarkSignal {
    /// Create an open window signal with a watermark timestamp.
    ///
    /// # Arguments
    /// * `chunk` - The chunk being processed
    /// * `watermark_ts` - The DB timestamp (Unix millis) at window open
    ///
    /// The `watermark_ts` should be obtained from the database using a query
    /// like `SELECT EXTRACT(EPOCH FROM NOW()) * 1000`.
    pub fn open_with_watermark(chunk: &SnapshotChunk, watermark_ts: i64) -> Self {
        Self {
            id: format!("{}-open", chunk.chunk_id),
            signal_type: WatermarkType::SnapshotWindowOpen,
            chunk_id: chunk.chunk_id.clone(),
            table_name: chunk.table_name.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            watermark_ts: Some(watermark_ts),
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
            watermark_ts: None, // Not needed for close signals
        }
    }

    /// Get parameters for a parameterized INSERT query.
    ///
    /// Returns (id, type, data_json) for use with parameterized queries.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let signal = WatermarkSignal::open_with_watermark(&chunk, watermark_ts);
    /// let (id, signal_type, data) = signal.insert_params();
    /// // Use with: INSERT INTO rivven_signal (id, type, data) VALUES ($1, $2, $3)
    /// ```
    pub fn insert_params(&self) -> (&str, &str, String) {
        let mut data = serde_json::json!({
            "chunk_id": self.chunk_id,
            "table": self.table_name,
        });

        // Include watermark timestamp for OPEN signals
        if let Some(watermark_ts) = self.watermark_ts {
            data["watermark_ts"] = serde_json::json!(watermark_ts);
        }

        (&self.id, self.signal_type.as_str(), data.to_string())
    }

    /// Get parameters for a parameterized DELETE query.
    ///
    /// Returns the signal ID for use with `DELETE FROM signal_table WHERE id = $1`.
    pub fn delete_param(&self) -> &str {
        &self.id
    }

    /// Check if this is an orphaned OPEN marker (no matching CLOSE within timeout).
    ///
    /// # Arguments
    /// * `timeout_ms` - Maximum age in milliseconds before considering orphaned
    pub fn is_orphaned(&self, timeout_ms: i64) -> bool {
        if self.signal_type != WatermarkType::SnapshotWindowOpen {
            return false;
        }
        let now = chrono::Utc::now().timestamp_millis();
        now - self.timestamp > timeout_ms
    }
}

/// Buffer for chunk deduplication.
///
/// Holds snapshot rows and compares with streaming events to detect conflicts.
/// Supports both single-column and composite primary keys via [`SnapshotKey`].
///
/// # Watermark-Based Deduplication
///
/// The buffer uses a watermark timestamp (`window_opened_ts`) for correct deduplication:
///
/// - **On window open**: Record the DB/CDC timestamp as `window_opened_ts`
/// - **On streaming conflict**: Compare `event_ts` with `window_opened_ts`:
///   - If `event_ts >= window_opened_ts` → drop the buffered snapshot row
///   - If DELETE event → always drop (deletes win regardless of timestamp)
///
/// This ensures that events committed AFTER the window opened correctly
/// supersede snapshot rows, while events from BEFORE the window don't cause
/// false positives.
///
/// # Memory Management
///
/// The buffer tracks estimated memory usage and enforces configurable limits:
/// - `max_rows`: Hard limit on buffered rows (default: 100,000)
/// - Memory estimation is approximate (based on serialized event size)
///
/// When limits are exceeded, additional rows are dropped with a warning.
///
/// # Thread Safety
///
/// This struct is not thread-safe. The coordinator wraps it in appropriate
/// synchronization primitives.
#[derive(Debug)]
pub struct ChunkBuffer {
    /// Buffered events keyed by primary key (supports composite keys)
    events: HashMap<SnapshotKey, CdcEvent>,
    /// Key columns for this buffer (supports composite PKs)
    key_columns: Vec<String>,
    /// Table being snapshotted
    table_name: String,
    /// Chunk ID
    chunk_id: String,
    /// Window open timestamp (local clock, for metrics)
    window_opened: Option<Instant>,
    /// Watermark timestamp (DB/CDC time in Unix millis)
    ///
    /// This is the database or CDC commit timestamp at window open time.
    /// Used for deduplication: streaming events with `ts >= window_opened_ts`
    /// supersede buffered snapshot rows.
    window_opened_ts: Option<i64>,
    /// Is window currently open
    is_open: bool,
    /// Events dropped due to conflicts
    dropped_count: u64,
    /// Events dropped due to buffer limits
    overflow_dropped: u64,
    /// Estimated memory usage in bytes
    estimated_memory_bytes: usize,
    /// Maximum rows allowed (hard limit)
    max_rows: usize,
    /// Maximum memory allowed (soft limit)
    max_memory_bytes: usize,
    /// Whether memory warning has been emitted
    memory_warning_emitted: bool,
}

impl ChunkBuffer {
    /// Create a new buffer for a chunk with default limits.
    pub fn new(chunk: &SnapshotChunk) -> Self {
        Self::with_limits(chunk, 100_000, 64 * 1024 * 1024)
    }

    /// Create a new buffer with custom limits.
    ///
    /// # Arguments
    /// * `chunk` - The chunk this buffer is for
    /// * `max_rows` - Maximum number of rows to buffer (hard limit)
    /// * `max_memory_bytes` - Maximum estimated memory usage (soft limit)
    pub fn with_limits(chunk: &SnapshotChunk, max_rows: usize, max_memory_bytes: usize) -> Self {
        Self {
            events: HashMap::new(),
            key_columns: vec![chunk.key_column.clone()],
            table_name: chunk.table_name.clone(),
            chunk_id: chunk.chunk_id.clone(),
            window_opened: None,
            window_opened_ts: None,
            is_open: false,
            dropped_count: 0,
            overflow_dropped: 0,
            estimated_memory_bytes: 0,
            max_rows,
            max_memory_bytes,
            memory_warning_emitted: false,
        }
    }

    /// Create a buffer with composite key support.
    pub fn with_composite_key(
        chunk: &SnapshotChunk,
        key_columns: Vec<String>,
        max_rows: usize,
        max_memory_bytes: usize,
    ) -> Self {
        Self {
            events: HashMap::new(),
            key_columns,
            table_name: chunk.table_name.clone(),
            chunk_id: chunk.chunk_id.clone(),
            window_opened: None,
            window_opened_ts: None,
            is_open: false,
            dropped_count: 0,
            overflow_dropped: 0,
            estimated_memory_bytes: 0,
            max_rows,
            max_memory_bytes,
            memory_warning_emitted: false,
        }
    }

    /// Open the snapshot window with a watermark timestamp.
    ///
    /// # Arguments
    /// * `watermark_ts` - The DB/CDC timestamp (Unix millis) at window open time.
    ///   This should be obtained from the database, not the local clock.
    ///
    /// # Deduplication Logic
    ///
    /// Once the window is open:
    /// - Streaming events with `event_ts >= watermark_ts` will drop buffered rows
    /// - DELETE events always drop buffered rows (deletes win)
    /// - Streaming events with `event_ts < watermark_ts` are ignored (snapshot is newer)
    pub fn open_window_with_watermark(&mut self, watermark_ts: i64) {
        self.is_open = true;
        self.window_opened = Some(Instant::now());
        self.window_opened_ts = Some(watermark_ts);
        self.events.clear();
        self.estimated_memory_bytes = 0;
        self.memory_warning_emitted = false;
        self.overflow_dropped = 0;
        debug!(
            chunk_id = %self.chunk_id,
            table = %self.table_name,
            watermark_ts = watermark_ts,
            "Snapshot window opened with watermark"
        );
    }

    /// Get the watermark timestamp for this window.
    ///
    /// Returns `None` if the window is not open.
    pub fn watermark_ts(&self) -> Option<i64> {
        self.window_opened_ts
    }

    /// Add a snapshot row to the buffer using a string key (single-column PK).
    ///
    /// Returns `true` if the row was added, `false` if dropped due to limits.
    pub fn add_row(&mut self, event: CdcEvent, key: String) -> bool {
        self.add_row_with_key(event, SnapshotKey::single(key))
    }

    /// Add a snapshot row to the buffer using a [`SnapshotKey`].
    ///
    /// Returns `true` if the row was added, `false` if dropped due to limits.
    pub fn add_row_with_key(&mut self, event: CdcEvent, key: SnapshotKey) -> bool {
        if !self.is_open {
            return false;
        }

        // Check row limit (hard limit)
        if self.events.len() >= self.max_rows {
            self.overflow_dropped += 1;
            if self.overflow_dropped == 1 {
                warn!(
                    chunk_id = %self.chunk_id,
                    max_rows = self.max_rows,
                    "Buffer row limit exceeded, dropping rows"
                );
            }
            return false;
        }

        // Estimate event size
        let event_size = Self::estimate_event_size(&event);

        // Check memory limit (soft limit - warn but allow)
        if self.estimated_memory_bytes + event_size > self.max_memory_bytes
            && !self.memory_warning_emitted
        {
            warn!(
                chunk_id = %self.chunk_id,
                current_bytes = self.estimated_memory_bytes,
                max_bytes = self.max_memory_bytes,
                rows = self.events.len(),
                "Buffer memory limit exceeded (soft limit)"
            );
            self.memory_warning_emitted = true;
        }

        self.estimated_memory_bytes += event_size;
        self.events.insert(key, event);
        true
    }

    /// Estimate the memory size of a CDC event.
    fn estimate_event_size(event: &CdcEvent) -> usize {
        // Base struct size + estimated payload size
        // This is approximate - exact measurement would require serialization
        let base_size = std::mem::size_of::<CdcEvent>();

        // Estimate payload size from 'after' field if present
        let payload_size = event
            .after
            .as_ref()
            .map(|v| {
                // Rough estimate: JSON string length
                serde_json::to_string(v).map(|s| s.len()).unwrap_or(256)
            })
            .unwrap_or(0);

        base_size + payload_size
    }

    /// Check if a streaming event conflicts with buffered snapshot using timestamp-based deduplication.
    ///
    /// Call this for every streaming event while the window is open.
    ///
    /// # Arguments
    /// * `key` - The primary key (single or composite)
    /// * `event_ts` - The event's timestamp (DB/CDC commit time in Unix millis)
    /// * `is_delete` - Whether this is a DELETE operation
    ///
    /// # Deduplication Logic
    ///
    /// - **DELETE events**: Always drop the buffered row (deletes win)
    /// - **Other events**: Drop if `event_ts >= window_opened_ts`
    /// - **Stale events**: Events with `event_ts < window_opened_ts` are ignored
    ///   because the snapshot row is fresher
    ///
    /// # Returns
    ///
    /// `true` if a buffered row was dropped (conflict handled), `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Process streaming event during window
    /// let key = SnapshotKey::from_row(&event.after, &key_columns)?;
    /// let is_delete = event.op == "d";
    /// let dropped = buffer.check_conflict_with_timestamp(&key, event.ts_ms, is_delete);
    /// ```
    pub fn check_conflict_with_timestamp(
        &mut self,
        key: &SnapshotKey,
        event_ts: i64,
        is_delete: bool,
    ) -> bool {
        if !self.is_open {
            return false;
        }

        // Check if this key exists in the buffer
        if !self.events.contains_key(key) {
            return false;
        }

        // Get the watermark timestamp
        let watermark_ts = match self.window_opened_ts {
            Some(ts) => ts,
            None => {
                // No watermark set - fall back to always dropping (legacy behavior)
                warn!(
                    chunk_id = %self.chunk_id,
                    "No watermark timestamp set, using legacy deduplication"
                );
                if let Some(removed) = self.events.remove(key) {
                    self.dropped_count += 1;
                    self.estimated_memory_bytes = self
                        .estimated_memory_bytes
                        .saturating_sub(Self::estimate_event_size(&removed));
                    return true;
                }
                return false;
            }
        };

        // Deduplication logic:
        // - DELETE always wins (drop the snapshot row)
        // - Other events: only drop if event_ts >= watermark_ts (event is newer)
        let should_drop = is_delete || event_ts >= watermark_ts;

        if should_drop {
            if let Some(removed) = self.events.remove(key) {
                self.dropped_count += 1;
                self.estimated_memory_bytes = self
                    .estimated_memory_bytes
                    .saturating_sub(Self::estimate_event_size(&removed));
                debug!(
                    key = %key,
                    chunk_id = %self.chunk_id,
                    event_ts = event_ts,
                    watermark_ts = watermark_ts,
                    is_delete = is_delete,
                    "Dropped buffered snapshot event (streaming wins)"
                );
                return true;
            }
        } else {
            debug!(
                key = %key,
                chunk_id = %self.chunk_id,
                event_ts = event_ts,
                watermark_ts = watermark_ts,
                "Ignored stale streaming event (snapshot is fresher)"
            );
        }

        false
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
            dropped_conflicts = self.dropped_count,
            dropped_overflow = self.overflow_dropped,
            memory_bytes = self.estimated_memory_bytes,
            duration_ms = ?duration.map(|d| d.as_millis()),
            "Snapshot window closed"
        );

        self.estimated_memory_bytes = 0;
        events
    }

    /// Check if window is open.
    pub fn is_window_open(&self) -> bool {
        self.is_open
    }

    /// Get buffer size (number of rows).
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get count of events dropped due to streaming conflicts.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Get count of events dropped due to buffer overflow.
    pub fn overflow_dropped(&self) -> u64 {
        self.overflow_dropped
    }

    /// Get estimated memory usage in bytes.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.estimated_memory_bytes
    }

    /// Get the key columns used for conflict detection.
    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    /// Get the primary key column name (first column for composite keys).
    ///
    /// For backwards compatibility with single-column key usage.
    pub fn key_column(&self) -> &str {
        self.key_columns.first().map(|s| s.as_str()).unwrap_or("id")
    }

    /// Get the table name being snapshotted.
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the chunk ID.
    pub fn chunk_id(&self) -> &str {
        &self.chunk_id
    }

    /// Check if this buffer has composite key support.
    pub fn has_composite_key(&self) -> bool {
        self.key_columns.len() > 1
    }

    /// Get buffer statistics.
    pub fn stats(&self) -> ChunkBufferStats {
        ChunkBufferStats {
            rows: self.events.len(),
            estimated_memory_bytes: self.estimated_memory_bytes,
            dropped_conflicts: self.dropped_count,
            dropped_overflow: self.overflow_dropped,
            is_open: self.is_open,
            memory_limit_exceeded: self.memory_warning_emitted,
        }
    }
}

/// Statistics for a chunk buffer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkBufferStats {
    /// Number of buffered rows
    pub rows: usize,
    /// Estimated memory usage in bytes
    pub estimated_memory_bytes: usize,
    /// Events dropped due to streaming conflicts
    pub dropped_conflicts: u64,
    /// Events dropped due to buffer overflow
    pub dropped_overflow: u64,
    /// Whether window is currently open
    pub is_open: bool,
    /// Whether memory limit was exceeded
    pub memory_limit_exceeded: bool,
}

/// Aggregate statistics across all open buffers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggregateBufferStats {
    /// Total rows across all buffers
    pub total_rows: usize,
    /// Total estimated memory usage in bytes
    pub total_memory_bytes: usize,
    /// Total events dropped due to streaming conflicts
    pub total_dropped_conflicts: u64,
    /// Total events dropped due to buffer overflow
    pub total_dropped_overflow: u64,
    /// Number of currently open windows
    pub open_windows: usize,
}

impl AggregateBufferStats {
    /// Check if memory usage is within acceptable bounds.
    ///
    /// # Arguments
    /// * `max_memory` - Maximum acceptable total memory in bytes
    pub fn is_memory_ok(&self, max_memory: usize) -> bool {
        self.total_memory_bytes <= max_memory
    }

    /// Get memory utilization as a percentage of a given limit.
    pub fn memory_utilization(&self, max_memory: usize) -> f64 {
        if max_memory == 0 {
            return 0.0;
        }
        (self.total_memory_bytes as f64 / max_memory as f64) * 100.0
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
/// - Buffer management and deduplication (with parallel window support)
/// - Progress tracking and resumption
///
/// # Parallel Processing
///
/// The coordinator supports multiple concurrent chunk windows for improved throughput.
/// Each window maintains its own deduplication buffer, and streaming events are
/// checked against ALL open windows for conflict detection.
///
/// ```rust,ignore
/// // Process multiple chunks in parallel
/// let chunks: Vec<SnapshotChunk> = get_next_chunks(4).await?;
/// let watermark_ts = current_timestamp_micros();
///
/// // Open windows for all chunks with watermark
/// for chunk in &chunks {
///     coordinator.open_window_with_watermark(chunk, watermark_ts).await?;
/// }
///
/// // Execute chunk queries in parallel
/// let results = futures::future::join_all(
///     chunks.iter().map(|c| execute_chunk_query(c))
/// ).await;
///
/// // Buffer results (order doesn't matter)
/// for (chunk, rows) in chunks.iter().zip(results) {
///     for row in rows? {
///         coordinator.buffer_row_for_chunk(&chunk.chunk_id, event, key).await;
///     }
/// }
///
/// // Close windows and collect events
/// for chunk in &chunks {
///     let events = coordinator.close_window_for_chunk(&chunk.chunk_id).await?;
///     emit_events(events).await?;
/// }
/// ```
pub struct IncrementalSnapshotCoordinator {
    /// Configuration
    config: IncrementalSnapshotConfig,
    /// Current snapshot context
    context: Arc<RwLock<Option<IncrementalSnapshotContext>>>,
    /// Active chunk buffers (keyed by chunk_id)
    buffers: Arc<RwLock<HashMap<String, ChunkBuffer>>>,
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
            buffers: Arc::new(RwLock::new(HashMap::new())),
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

        // Clear all buffers
        {
            let mut buffers = self.buffers.write().await;
            for (_, mut buffer) in buffers.drain() {
                if buffer.is_window_open() {
                    let _ = buffer.close_window();
                }
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

    /// Generate watermark signal for opening a chunk window with a watermark timestamp.
    ///
    /// # Arguments
    /// * `chunk` - The chunk being processed
    /// * `watermark_ts` - The DB timestamp (Unix millis) at window open
    pub fn open_window_signal_with_watermark(
        &self,
        chunk: &SnapshotChunk,
        watermark_ts: i64,
    ) -> WatermarkSignal {
        WatermarkSignal::open_with_watermark(chunk, watermark_ts)
    }

    /// Generate watermark signal for closing a chunk window.
    pub fn close_window_signal(&self, chunk: &SnapshotChunk) -> WatermarkSignal {
        WatermarkSignal::close(chunk)
    }

    /// Open the deduplication window for a chunk with a watermark timestamp.
    ///
    /// # Arguments
    /// * `chunk` - The chunk to open a window for
    /// * `watermark_ts` - The DB timestamp (Unix millis) at window open
    ///
    /// # Parallel Processing
    ///
    /// Multiple windows can be open simultaneously up to `max_concurrent_chunks`.
    /// Each window is identified by the chunk's `chunk_id`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `max_concurrent_chunks` limit is reached.
    pub async fn open_window_with_watermark(
        &self,
        chunk: &SnapshotChunk,
        watermark_ts: i64,
    ) -> Result<()> {
        let mut buffers = self.buffers.write().await;

        // Check concurrency limit
        if buffers.len() >= self.config.max_concurrent_chunks {
            return Err(CdcError::InvalidState(format!(
                "Maximum concurrent chunks ({}) reached. Close a window before opening another.",
                self.config.max_concurrent_chunks
            )));
        }

        let mut buffer = ChunkBuffer::with_limits(
            chunk,
            self.config.max_buffer_rows,
            self.config.max_buffer_memory,
        );
        buffer.open_window_with_watermark(watermark_ts);
        buffers.insert(chunk.chunk_id.clone(), buffer);

        debug!(
            chunk_id = %chunk.chunk_id,
            watermark_ts = watermark_ts,
            open_windows = buffers.len(),
            max_concurrent = self.config.max_concurrent_chunks,
            "Opened chunk window with watermark"
        );

        Ok(())
    }

    /// Open the deduplication window with composite key support and watermark.
    ///
    /// Use this when the table has a composite primary key.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `max_concurrent_chunks` limit is reached.
    pub async fn open_window_composite_with_watermark(
        &self,
        chunk: &SnapshotChunk,
        key_columns: Vec<String>,
        watermark_ts: i64,
    ) -> Result<()> {
        let mut buffers = self.buffers.write().await;

        if buffers.len() >= self.config.max_concurrent_chunks {
            return Err(CdcError::InvalidState(format!(
                "Maximum concurrent chunks ({}) reached. Close a window before opening another.",
                self.config.max_concurrent_chunks
            )));
        }

        let mut buffer = ChunkBuffer::with_composite_key(
            chunk,
            key_columns,
            self.config.max_buffer_rows,
            self.config.max_buffer_memory,
        );
        buffer.open_window_with_watermark(watermark_ts);
        buffers.insert(chunk.chunk_id.clone(), buffer);

        Ok(())
    }

    /// Add a snapshot row to a specific chunk's buffer (string key).
    ///
    /// # Arguments
    /// * `chunk_id` - The chunk ID to buffer the row for
    /// * `event` - The CDC event to buffer
    /// * `key` - The primary key value
    pub async fn buffer_row_for_chunk(&self, chunk_id: &str, event: CdcEvent, key: String) -> bool {
        if let Some(buffer) = self.buffers.write().await.get_mut(chunk_id) {
            return buffer.add_row(event, key);
        }
        false
    }

    /// Add a snapshot row to a specific chunk's buffer with a [`SnapshotKey`].
    ///
    /// Use this for composite key support.
    pub async fn buffer_row_for_chunk_with_key(
        &self,
        chunk_id: &str,
        event: CdcEvent,
        key: SnapshotKey,
    ) -> bool {
        if let Some(buffer) = self.buffers.write().await.get_mut(chunk_id) {
            return buffer.add_row_with_key(event, key);
        }
        false
    }

    /// Check if a streaming event conflicts with ANY open buffer using timestamp-based deduplication.
    ///
    /// Call this for every streaming event while windows are open.
    ///
    /// # Arguments
    /// * `table` - The table name
    /// * `key` - The primary key (single or composite)
    /// * `event_ts` - The event's timestamp (DB/CDC commit time in Unix millis)
    /// * `is_delete` - Whether this is a DELETE operation
    ///
    /// # Deduplication Logic
    ///
    /// - **DELETE events**: Always drop the buffered row (deletes win)
    /// - **Other events**: Drop if `event_ts >= window_opened_ts`
    /// - **Stale events**: Events with `event_ts < window_opened_ts` are ignored
    ///
    /// # Returns
    ///
    /// Number of buffers where a conflict was found and handled.
    /// This can be >1 if the event conflicts with multiple concurrent chunk windows.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Process streaming events during snapshot
    /// for event in streaming_events {
    ///     let key = SnapshotKey::from_row(&event.after, &key_columns)?;
    ///     let is_delete = event.op == "d";
    ///     coordinator.check_streaming_conflict_with_timestamp(
    ///         "public.orders",
    ///         &key,
    ///         event.ts_ms,
    ///         is_delete,
    ///     ).await;
    /// }
    /// ```
    pub async fn check_streaming_conflict_with_timestamp(
        &self,
        table: &str,
        key: &SnapshotKey,
        event_ts: i64,
        is_delete: bool,
    ) -> usize {
        let mut buffers = self.buffers.write().await;
        let mut conflicts = 0;

        for buffer in buffers.values_mut() {
            if buffer.table_name() == table
                && buffer.check_conflict_with_timestamp(key, event_ts, is_delete)
            {
                conflicts += 1;
            }
        }

        conflicts
    }

    /// Get statistics for a specific chunk's buffer.
    pub async fn buffer_stats(&self, chunk_id: &str) -> Option<ChunkBufferStats> {
        self.buffers.read().await.get(chunk_id).map(|b| b.stats())
    }

    /// Get statistics for all open buffers.
    pub async fn buffer_stats_all(&self) -> HashMap<String, ChunkBufferStats> {
        self.buffers
            .read()
            .await
            .iter()
            .map(|(id, b)| (id.clone(), b.stats()))
            .collect()
    }

    /// Get aggregate statistics across all open buffers.
    pub async fn buffer_stats_aggregate(&self) -> AggregateBufferStats {
        let buffers = self.buffers.read().await;
        let mut agg = AggregateBufferStats::default();

        for buffer in buffers.values() {
            let stats = buffer.stats();
            agg.total_rows += stats.rows;
            agg.total_memory_bytes += stats.estimated_memory_bytes;
            agg.total_dropped_conflicts += stats.dropped_conflicts;
            agg.total_dropped_overflow += stats.dropped_overflow;
            if stats.is_open {
                agg.open_windows += 1;
            }
        }

        agg
    }

    /// Close a specific chunk's window and get remaining events to emit.
    ///
    /// # Arguments
    /// * `chunk_id` - The chunk ID of the window to close
    ///
    /// # Returns
    /// The events remaining in the buffer after deduplication, or empty vec if
    /// the chunk was not found.
    pub async fn close_window_for_chunk(&self, chunk_id: &str) -> Result<Vec<CdcEvent>> {
        let mut buffers = self.buffers.write().await;
        let buffer = match buffers.get_mut(chunk_id) {
            Some(b) => b,
            None => {
                warn!(chunk_id = %chunk_id, "Attempted to close non-existent window");
                return Ok(Vec::new());
            }
        };

        let window_start = buffer.window_opened;
        let dropped = buffer.dropped_count();
        let events = buffer.close_window();
        let rows = events.len() as u64;

        // Remove the buffer
        buffers.remove(chunk_id);

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
            context.last_activity = chrono::Utc::now().timestamp_millis();
        }

        debug!(
            chunk_id = %chunk_id,
            emitted = events.len(),
            dropped = dropped,
            remaining_windows = buffers.len(),
            "Closed chunk window"
        );

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

    /// Check if any window is currently open.
    pub async fn is_window_open(&self) -> bool {
        let buffers = self.buffers.read().await;
        buffers.values().any(|b| b.is_window_open())
    }

    /// Check if a specific chunk's window is open.
    pub async fn is_window_open_for_chunk(&self, chunk_id: &str) -> bool {
        self.buffers
            .read()
            .await
            .get(chunk_id)
            .is_some_and(|b| b.is_window_open())
    }

    /// Get the number of currently open windows.
    pub async fn open_window_count(&self) -> usize {
        self.buffers.read().await.len()
    }

    /// Get the IDs of all currently open chunk windows.
    pub async fn open_chunk_ids(&self) -> Vec<String> {
        self.buffers.read().await.keys().cloned().collect()
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
    fn test_snapshot_chunk_creation() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id")
            .with_range(Some("100".to_string()), Some("200".to_string()))
            .with_conditions("status = 'active'");

        assert_eq!(chunk.table_name, "public.orders");
        assert_eq!(chunk.key_column, "id");
        assert_eq!(chunk.conditions.as_deref(), Some("status = 'active'"));

        let (from, to) = chunk.key_bounds_as_strings();
        assert_eq!(from, Some("100".to_string()));
        assert_eq!(to, Some("200".to_string()));
    }

    #[test]
    fn test_watermark_signal() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let watermark_ts = 1000000000i64;
        let open = WatermarkSignal::open_with_watermark(&chunk, watermark_ts);
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

        // Open window with watermark
        let watermark_ts = 1000000000i64;
        buffer.open_window_with_watermark(watermark_ts);
        assert!(buffer.is_window_open());

        // Add some rows
        buffer.add_row(test_event(), "key1".to_string());
        buffer.add_row(test_event(), "key2".to_string());
        buffer.add_row(test_event(), "key3".to_string());
        assert_eq!(buffer.len(), 3);

        // Simulate streaming conflict with timestamp after watermark
        let key2 = SnapshotKey::single("key2");
        assert!(buffer.check_conflict_with_timestamp(&key2, watermark_ts + 100, false));
        let key4 = SnapshotKey::single("key4");
        assert!(!buffer.check_conflict_with_timestamp(&key4, watermark_ts + 100, false)); // Non-existent key
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.dropped_count(), 1);

        // Close window
        let events = buffer.close_window();
        assert_eq!(events.len(), 2);
        assert!(!buffer.is_window_open());
    }

    #[test]
    fn test_chunk_buffer_watermark_deduplication() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let mut buffer = ChunkBuffer::new(&chunk);

        // Use a specific watermark timestamp
        let watermark_ts = 1000000000i64; // T=1000s
        buffer.open_window_with_watermark(watermark_ts);
        assert!(buffer.is_window_open());
        assert_eq!(buffer.watermark_ts(), Some(watermark_ts));

        // Buffer some rows
        buffer.add_row(test_event(), "key1".to_string());
        buffer.add_row(test_event(), "key2".to_string());
        buffer.add_row(test_event(), "key3".to_string());
        assert_eq!(buffer.len(), 3);

        // Streaming event with timestamp BEFORE watermark - should NOT drop (snapshot fresher)
        let key1 = SnapshotKey::single("key1");
        let stale_event_ts = watermark_ts - 100; // Before window opened
        assert!(!buffer.check_conflict_with_timestamp(&key1, stale_event_ts, false));
        assert_eq!(buffer.len(), 3); // key1 still in buffer

        // Streaming event with timestamp AFTER watermark - should drop (streaming wins)
        let fresh_event_ts = watermark_ts + 100; // After window opened
        assert!(buffer.check_conflict_with_timestamp(&key1, fresh_event_ts, false));
        assert_eq!(buffer.len(), 2); // key1 removed

        // DELETE always wins, even if timestamp is stale
        let key2 = SnapshotKey::single("key2");
        assert!(buffer.check_conflict_with_timestamp(&key2, stale_event_ts, true)); // is_delete=true
        assert_eq!(buffer.len(), 1); // key2 removed despite stale timestamp

        // Close window
        let events = buffer.close_window();
        assert_eq!(events.len(), 1); // Only key3 remains
    }

    #[test]
    fn test_watermark_signal_with_watermark() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let watermark_ts = 1706790000000i64;

        // Create signal with watermark
        let open = WatermarkSignal::open_with_watermark(&chunk, watermark_ts);
        assert_eq!(open.signal_type, WatermarkType::SnapshotWindowOpen);
        assert_eq!(open.watermark_ts, Some(watermark_ts));

        // Close signal has no watermark
        let close = WatermarkSignal::close(&chunk);
        assert_eq!(close.signal_type, WatermarkType::SnapshotWindowClose);
        assert_eq!(close.watermark_ts, None);

        // Check insert params include watermark
        let (id, type_str, data) = open.insert_params();
        assert!(id.ends_with("-open"));
        assert_eq!(type_str, "snapshot-window-open");
        assert!(data.contains("watermark_ts"));
        assert!(data.contains("1706790000000"));
    }

    #[tokio::test]
    async fn test_coordinator_watermark_conflict_detection() {
        let config = IncrementalSnapshotConfig::builder().chunk_size(100).build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        let chunk = coordinator.next_chunk().await.unwrap().unwrap();

        // Use specific watermark timestamp
        let watermark_ts = 1000000000i64;
        coordinator
            .open_window_with_watermark(&chunk, watermark_ts)
            .await
            .unwrap();

        // Buffer rows
        let key1 = SnapshotKey::single("1");
        let key2 = SnapshotKey::single("2");
        let key3 = SnapshotKey::single("3");

        coordinator
            .buffer_row_for_chunk_with_key(&chunk.chunk_id, test_event(), key1.clone())
            .await;
        coordinator
            .buffer_row_for_chunk_with_key(&chunk.chunk_id, test_event(), key2.clone())
            .await;
        coordinator
            .buffer_row_for_chunk_with_key(&chunk.chunk_id, test_event(), key3.clone())
            .await;

        // Stale event (before watermark) - no conflict
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key1,
                    watermark_ts - 100,
                    false,
                )
                .await,
            0
        );

        // Fresh event (after watermark) - conflict, drops key1
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key1,
                    watermark_ts + 100,
                    false,
                )
                .await,
            1
        );

        // DELETE always wins - drops key2 even with stale timestamp
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key2,
                    watermark_ts - 100,
                    true, // is_delete
                )
                .await,
            1
        );

        // Close window - only key3 should remain
        let events = coordinator
            .close_window_for_chunk(&chunk.chunk_id)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
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

        // Open window with watermark timestamp
        let watermark_ts = 1000000000i64;
        coordinator
            .open_window_with_watermark(&chunk, watermark_ts)
            .await
            .unwrap();
        assert!(coordinator.is_window_open().await);

        // Buffer some rows
        coordinator
            .buffer_row_for_chunk(&chunk.chunk_id, test_event(), "1".to_string())
            .await;
        coordinator
            .buffer_row_for_chunk(&chunk.chunk_id, test_event(), "2".to_string())
            .await;

        // Check conflict with timestamp (fresh event after watermark)
        let key1 = SnapshotKey::single("1");
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key1,
                    watermark_ts + 100,
                    false
                )
                .await,
            1
        );
        let key3 = SnapshotKey::single("3");
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key3,
                    watermark_ts + 100,
                    false
                )
                .await,
            0
        );

        // Close window
        let events = coordinator
            .close_window_for_chunk(&chunk.chunk_id)
            .await
            .unwrap();
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

    #[test]
    fn test_snapshot_key_single() {
        let key1 = SnapshotKey::single("12345");
        let key2 = SnapshotKey::single("12345");
        let key3 = SnapshotKey::single("67890");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_eq!(key1.to_canonical_string(), "12345");
        assert!(!key1.is_composite());
    }

    #[test]
    fn test_snapshot_key_composite() {
        let key1 = SnapshotKey::composite(vec![serde_json::json!("US"), serde_json::json!(12345)]);
        let key2 = SnapshotKey::composite(vec![serde_json::json!("US"), serde_json::json!(12345)]);
        let key3 = SnapshotKey::composite(vec![serde_json::json!("EU"), serde_json::json!(12345)]);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert!(key1.is_composite());
    }

    #[test]
    fn test_snapshot_key_from_row() {
        let row = serde_json::json!({
            "id": 12345,
            "region": "US",
            "name": "test"
        });

        // Single column key
        let key = SnapshotKey::from_row(&row, &["id".to_string()]).unwrap();
        assert_eq!(key, SnapshotKey::single("12345"));

        // Composite key
        let key = SnapshotKey::from_row(&row, &["region".to_string(), "id".to_string()]).unwrap();
        assert!(key.is_composite());
    }

    #[test]
    fn test_chunk_buffer_with_limits() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let mut buffer = ChunkBuffer::with_limits(&chunk, 3, 1024 * 1024);
        let watermark_ts = 1000000000i64;

        buffer.open_window_with_watermark(watermark_ts);

        // Add rows up to limit
        assert!(buffer.add_row(test_event(), "1".to_string()));
        assert!(buffer.add_row(test_event(), "2".to_string()));
        assert!(buffer.add_row(test_event(), "3".to_string()));
        assert_eq!(buffer.len(), 3);

        // Exceeds limit - should be dropped
        assert!(!buffer.add_row(test_event(), "4".to_string()));
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.overflow_dropped(), 1);

        let stats = buffer.stats();
        assert_eq!(stats.rows, 3);
        assert_eq!(stats.dropped_overflow, 1);
    }

    #[test]
    fn test_chunk_buffer_composite_key() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let mut buffer = ChunkBuffer::with_composite_key(
            &chunk,
            vec!["region".to_string(), "id".to_string()],
            100,
            1024 * 1024,
        );
        let watermark_ts = 1000000000i64;

        buffer.open_window_with_watermark(watermark_ts);
        assert!(buffer.has_composite_key());
        assert_eq!(buffer.key_columns(), &["region", "id"]);

        // Add with composite key
        let key1 = SnapshotKey::composite(vec![serde_json::json!("US"), serde_json::json!(1)]);
        let key2 = SnapshotKey::composite(vec![serde_json::json!("EU"), serde_json::json!(1)]);

        assert!(buffer.add_row_with_key(test_event(), key1.clone()));
        assert!(buffer.add_row_with_key(test_event(), key2.clone()));
        assert_eq!(buffer.len(), 2);

        // Conflict with composite key (using timestamp after watermark)
        assert!(buffer.check_conflict_with_timestamp(&key1, watermark_ts + 100, false));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.dropped_count(), 1);
    }

    #[test]
    fn test_watermark_signal_orphan_detection() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let watermark_ts = 1000000000i64;
        let mut signal = WatermarkSignal::open_with_watermark(&chunk, watermark_ts);

        // Fresh signal is not orphaned
        assert!(!signal.is_orphaned(3600 * 1000)); // 1 hour

        // Old signal is orphaned
        signal.timestamp = chrono::Utc::now().timestamp_millis() - (2 * 3600 * 1000); // 2 hours ago
        assert!(signal.is_orphaned(3600 * 1000)); // 1 hour threshold

        // Close signals are never orphaned
        let close = WatermarkSignal::close(&chunk);
        assert!(!close.is_orphaned(0)); // Even with 0 timeout
    }

    #[test]
    fn test_watermark_insert_params() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id");
        let watermark_ts = 1000000000i64;
        let signal = WatermarkSignal::open_with_watermark(&chunk, watermark_ts);

        let (id, type_str, data) = signal.insert_params();
        assert!(id.ends_with("-open"));
        assert_eq!(type_str, "snapshot-window-open");
        assert!(data.contains("chunk_id"));
        assert!(data.contains("table"));
    }

    #[test]
    fn test_chunk_query_params() {
        let chunk = SnapshotChunk::new("public.orders", 0, "id")
            .with_range(Some("100".to_string()), Some("200".to_string()))
            .with_conditions("status = 'active'");

        let (from, to, key_col, conditions) = chunk.query_params();
        assert_eq!(from.map(|v| v.as_str().unwrap()), Some("100"));
        assert_eq!(to.map(|v| v.as_str().unwrap()), Some("200"));
        assert_eq!(key_col, "id");
        assert_eq!(conditions, Some("status = 'active'"));
    }

    #[test]
    fn test_config_builder_max_buffer_rows() {
        let config = IncrementalSnapshotConfig::builder()
            .chunk_size(2048)
            .max_buffer_rows(50_000)
            .max_buffer_memory(32 * 1024 * 1024)
            .build();

        assert_eq!(config.chunk_size, 2048);
        assert_eq!(config.max_buffer_rows, 50_000);
        assert_eq!(config.max_buffer_memory, 32 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_coordinator_buffer_stats() {
        let config = IncrementalSnapshotConfig::builder()
            .chunk_size(100)
            .max_buffer_rows(10)
            .build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        // Start snapshot
        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        // Get first chunk
        let chunk = coordinator.next_chunk().await.unwrap().unwrap();
        let watermark_ts = 1000000000i64;
        coordinator
            .open_window_with_watermark(&chunk, watermark_ts)
            .await
            .unwrap();

        // Buffer some rows using chunk-specific API
        coordinator
            .buffer_row_for_chunk(&chunk.chunk_id, test_event(), "1".to_string())
            .await;
        coordinator
            .buffer_row_for_chunk(&chunk.chunk_id, test_event(), "2".to_string())
            .await;

        // Check buffer stats (using chunk-specific method)
        let stats = coordinator.buffer_stats(&chunk.chunk_id).await.unwrap();
        assert_eq!(stats.rows, 2);
        assert!(stats.estimated_memory_bytes > 0);
        assert!(stats.is_open);
    }

    #[tokio::test]
    async fn test_coordinator_composite_key() {
        let config = IncrementalSnapshotConfig::builder().chunk_size(100).build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        let chunk = coordinator.next_chunk().await.unwrap().unwrap();
        let watermark_ts = 1000000000i64;
        coordinator
            .open_window_composite_with_watermark(
                &chunk,
                vec!["region".to_string(), "id".to_string()],
                watermark_ts,
            )
            .await
            .unwrap();

        let key1 = SnapshotKey::composite(vec![serde_json::json!("US"), serde_json::json!(1)]);
        let key2 = SnapshotKey::composite(vec![serde_json::json!("EU"), serde_json::json!(1)]);

        coordinator
            .buffer_row_for_chunk_with_key(&chunk.chunk_id, test_event(), key1.clone())
            .await;
        coordinator
            .buffer_row_for_chunk_with_key(&chunk.chunk_id, test_event(), key2.clone())
            .await;

        // Conflict check with timestamp
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key1,
                    watermark_ts + 100,
                    false
                )
                .await,
            1
        );
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key1,
                    watermark_ts + 100,
                    false
                )
                .await,
            0 // Already removed
        );

        let events = coordinator
            .close_window_for_chunk(&chunk.chunk_id)
            .await
            .unwrap();
        assert_eq!(events.len(), 1); // Only key2 remains
    }

    #[tokio::test]
    async fn test_coordinator_parallel_chunks() {
        let config = IncrementalSnapshotConfig::builder()
            .chunk_size(100)
            .max_concurrent_chunks(4)
            .build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        // Start snapshot
        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        // Open multiple windows
        let chunk1 =
            SnapshotChunk::new("public.orders", 0, "id").with_range(None, Some("100".to_string()));
        let chunk2 = SnapshotChunk::new("public.orders", 1, "id")
            .with_range(Some("100".to_string()), Some("200".to_string()));
        let chunk3 = SnapshotChunk::new("public.orders", 2, "id")
            .with_range(Some("200".to_string()), Some("300".to_string()));

        let watermark_ts = 1000000000i64;
        coordinator
            .open_window_with_watermark(&chunk1, watermark_ts)
            .await
            .unwrap();
        coordinator
            .open_window_with_watermark(&chunk2, watermark_ts)
            .await
            .unwrap();
        coordinator
            .open_window_with_watermark(&chunk3, watermark_ts)
            .await
            .unwrap();

        assert_eq!(coordinator.open_window_count().await, 3);

        // Buffer rows to different chunks
        coordinator
            .buffer_row_for_chunk(&chunk1.chunk_id, test_event(), "50".to_string())
            .await;
        coordinator
            .buffer_row_for_chunk(&chunk2.chunk_id, test_event(), "150".to_string())
            .await;
        coordinator
            .buffer_row_for_chunk(&chunk3.chunk_id, test_event(), "250".to_string())
            .await;

        // Aggregate stats
        let agg = coordinator.buffer_stats_aggregate().await;
        assert_eq!(agg.total_rows, 3);
        assert_eq!(agg.open_windows, 3);

        // Conflict in chunk1 (with timestamp after watermark)
        let key50 = SnapshotKey::single("50");
        assert_eq!(
            coordinator
                .check_streaming_conflict_with_timestamp(
                    "public.orders",
                    &key50,
                    watermark_ts + 100,
                    false
                )
                .await,
            1
        );

        // Close windows one by one
        let events1 = coordinator
            .close_window_for_chunk(&chunk1.chunk_id)
            .await
            .unwrap();
        assert_eq!(events1.len(), 0); // Was removed by conflict

        let events2 = coordinator
            .close_window_for_chunk(&chunk2.chunk_id)
            .await
            .unwrap();
        assert_eq!(events2.len(), 1);

        let events3 = coordinator
            .close_window_for_chunk(&chunk3.chunk_id)
            .await
            .unwrap();
        assert_eq!(events3.len(), 1);

        assert_eq!(coordinator.open_window_count().await, 0);
    }

    #[tokio::test]
    async fn test_coordinator_max_concurrent_chunks() {
        let config = IncrementalSnapshotConfig::builder()
            .chunk_size(100)
            .max_concurrent_chunks(2)
            .build();
        let coordinator = IncrementalSnapshotCoordinator::new(config);

        let request = IncrementalSnapshotRequest::new(vec!["public.orders".to_string()]);
        coordinator.start(request).await.unwrap();

        let chunk1 = SnapshotChunk::new("public.orders", 0, "id");
        let chunk2 = SnapshotChunk::new("public.orders", 1, "id");
        let chunk3 = SnapshotChunk::new("public.orders", 2, "id");
        let watermark_ts = 1000000000i64;

        // First two should succeed
        coordinator
            .open_window_with_watermark(&chunk1, watermark_ts)
            .await
            .unwrap();
        coordinator
            .open_window_with_watermark(&chunk2, watermark_ts)
            .await
            .unwrap();

        // Third should fail - max concurrent reached
        let result = coordinator
            .open_window_with_watermark(&chunk3, watermark_ts)
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Maximum concurrent chunks"));

        // After closing one, we can open another
        coordinator
            .close_window_for_chunk(&chunk1.chunk_id)
            .await
            .unwrap();
        coordinator
            .open_window_with_watermark(&chunk3, watermark_ts)
            .await
            .unwrap();
        assert_eq!(coordinator.open_window_count().await, 2);
    }
}
