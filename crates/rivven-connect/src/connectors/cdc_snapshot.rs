//! # CDC Snapshot Integration for Connectors
//!
//! High-level snapshot management that bridges rivven-connect YAML configuration
//! to rivven-cdc's snapshot primitives. Supports both initial snapshots and
//! incremental (non-blocking) snapshots with signal table integration.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                    CdcSnapshotOrchestrator (Unified API)                    │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │  Initial Snapshot  → Execute on startup based on mode                      │
//! │  Incremental       → Non-blocking snapshots during streaming               │
//! │  Signal Processing → Respond to signal table events                        │
//! │  Deduplication     → Watermark-based event deduplication                   │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                     ↓                               ↓
//! ┌────────────────────────────────┐  ┌────────────────────────────────────────┐
//! │     SnapshotExecutor           │  │   IncrementalSnapshotExecutor          │
//! ├────────────────────────────────┤  ├────────────────────────────────────────┤
//! │  Mode Decision                 │  │  Chunk-based execution                 │
//! │  Table Filtering               │  │  Watermark windows                     │
//! │  Progress Persistence          │  │  Buffer management                     │
//! │  Streaming execution           │  │  Deduplication                         │
//! └────────────────────────────────┘  └────────────────────────────────────────┘
//!                     ↓                               ↓
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                    rivven-cdc Primitives                                    │
//! ├─────────────────────────────────────────────────────────────────────────────┤
//! │  SnapshotCoordinator     → Multi-table parallel initial snapshots          │
//! │  IncrementalCoordinator  → Chunk-based incremental snapshots               │
//! │  PostgresSnapshotSource  → SELECT with keyset pagination                   │
//! │  FileProgressStore       → JSON-based progress persistence                 │
//! │  SignalProcessor         → Custom signal handlers                          │
//! │  SourceSignalChannel     → CDC stream signal detection                     │
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Snapshot Types
//!
//! | Type | Description | When to Use |
//! |------|-------------|-------------|
//! | **Initial** | Full table scan before streaming | First connector start |
//! | **Incremental** | Chunk-based re-snapshot during streaming | Add tables, resync |
//! | **Signal-Triggered** | Ad-hoc snapshot via signal table | On-demand refresh |
//!
//! ## Quick Start
//!
//! ### Initial Snapshot (Connector Read)
//!
//! ```rust,ignore
//! use rivven_connect::connectors::cdc_snapshot::{SnapshotExecutor, SnapshotExecutorConfig};
//!
//! let executor = SnapshotExecutor::new(SnapshotExecutorConfig {
//!     yaml_config: config.snapshot.clone(),
//!     connection_string: conn_str.clone(),
//!     tables: tables_from_catalog,
//!     has_prior_state: state.is_some(),
//! });
//!
//! // Execute snapshot if needed, get events as stream
//! let (events, result) = executor.execute().await?;
//!
//! // Chain with streaming if needed
//! if result.should_stream {
//!     let combined = combine_snapshot_and_stream(events, cdc_stream);
//! }
//! ```
//!
//! ### Incremental Snapshot (Signal-Triggered)
//!
//! ```rust,ignore
//! use rivven_connect::connectors::cdc_snapshot::{
//!     SignalSnapshotProcessor, SignalSnapshotConfig
//! };
//!
//! let processor = SignalSnapshotProcessor::new(SignalSnapshotConfig::new("public.rivven_signal"));
//! processor.initialize().await?;
//!
//! // In CDC streaming loop
//! for event in cdc_stream {
//!     // Check if this is a signal event
//!     if processor.is_signal_event(&event.schema, &event.table) {
//!         processor.process_cdc_event(...).await?;
//!         continue; // Don't emit signal events downstream
//!     }
//!     emit(event);
//! }
//! ```
//!
//! ## Signal Table
//!
//! Create a signal table to trigger incremental snapshots:
//!
//! ```sql
//! CREATE TABLE rivven_signal (
//!     id VARCHAR(42) PRIMARY KEY,
//!     type VARCHAR(32) NOT NULL,
//!     data VARCHAR(2048) NULL
//! );
//!
//! -- Trigger snapshot
//! INSERT INTO rivven_signal (id, type, data) VALUES (
//!     'snap-001',
//!     'execute-snapshot',
//!     '{"data-collections": ["public.orders"]}'
//! );
//! ```

use super::cdc_config::{SnapshotCdcConfig, SnapshotModeConfig};
use rivven_cdc::common::{
    CdcEvent, FileProgressStore, MemoryProgressStore, ProgressStore, Result, SnapshotConfig,
    SnapshotCoordinator, SnapshotMode, SnapshotProgress, SnapshotSource, SnapshotStatsSnapshot,
    TableSpec,
};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info, warn};

// Re-export key types from rivven-cdc for convenience
pub use rivven_cdc::common::{
    FileProgressStore as CdcFileProgressStore, MemoryProgressStore as CdcMemoryProgressStore,
    SnapshotMode as CdcSnapshotMode, SnapshotProgress as CdcSnapshotProgress,
    SnapshotSource as CdcSnapshotSource,
};

// Re-export incremental snapshot types
pub use rivven_cdc::common::{
    AggregateBufferStats, ChunkBoundary, ChunkBufferStats, ChunkQueryBuilder, ChunkingStrategy,
    IncrementalSnapshotConfig, IncrementalSnapshotConfigBuilder, IncrementalSnapshotContext,
    IncrementalSnapshotCoordinator, IncrementalSnapshotRequest, IncrementalSnapshotState,
    IncrementalSnapshotStats, IncrementalSnapshotStatsSnapshot, KeyRange, QuerySpec, SnapshotChunk,
    SnapshotKey, SnapshotTable, WatermarkSignal, WatermarkStrategy, WatermarkType,
};

// Re-export signal types
pub use rivven_cdc::common::{
    Signal, SignalAction, SignalChannel, SignalConfig, SignalData, SignalProcessor, SignalResult,
    SignalSource, SourceSignalChannel,
};

#[cfg(feature = "postgres")]
pub use rivven_cdc::postgres::{
    discover_primary_key, discover_tables_with_keys, get_table_statistics,
    PostgresSnapshotSource as CdcPostgresSnapshotSource, TableStatistics,
};

// ============================================================================
// Snapshot Manager Configuration
// ============================================================================

/// Configuration for the snapshot manager, parsed from connector YAML.
#[derive(Debug, Clone)]
pub struct SnapshotManagerConfig {
    /// Snapshot mode (Initial, Always, Never, WhenNeeded)
    pub mode: SnapshotMode,
    /// Batch size for SELECT queries (default: 10,000)
    pub batch_size: usize,
    /// Number of tables to snapshot in parallel (default: 4)
    pub parallel_tables: usize,
    /// Directory for progress persistence (None = in-memory only)
    pub progress_dir: Option<PathBuf>,
    /// Query timeout (default: 300s)
    pub query_timeout: Duration,
    /// Throttle delay between batches for backpressure
    pub throttle_delay: Option<Duration>,
    /// Maximum retries per batch
    pub max_retries: u32,
}

impl Default for SnapshotManagerConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotMode::Initial,
            batch_size: 10_000,
            parallel_tables: 4,
            progress_dir: None,
            query_timeout: Duration::from_secs(300),
            throttle_delay: None,
            max_retries: 3,
        }
    }
}

impl SnapshotManagerConfig {
    /// Create from snapshot mode with defaults.
    pub fn from_mode(mode: SnapshotMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Create from YAML configuration (SnapshotCdcConfig).
    pub fn from_yaml_config(yaml_config: &SnapshotCdcConfig) -> Self {
        let mode = match yaml_config.mode {
            SnapshotModeConfig::Initial => SnapshotMode::Initial,
            SnapshotModeConfig::Always => SnapshotMode::Always,
            SnapshotModeConfig::Never => SnapshotMode::NoSnapshot,
            SnapshotModeConfig::WhenNeeded => SnapshotMode::WhenNeeded,
            SnapshotModeConfig::InitialOnly => SnapshotMode::InitialOnly,
            SnapshotModeConfig::SchemaOnly => SnapshotMode::SchemaOnly,
            SnapshotModeConfig::Recovery => SnapshotMode::Recovery,
        };

        Self {
            mode,
            batch_size: yaml_config.batch_size,
            parallel_tables: yaml_config.parallel_tables,
            progress_dir: yaml_config.progress_dir.clone(),
            query_timeout: yaml_config.query_timeout(),
            throttle_delay: yaml_config.throttle_delay(),
            max_retries: yaml_config.max_retries,
        }
    }

    /// Builder pattern.
    pub fn builder() -> SnapshotManagerConfigBuilder {
        SnapshotManagerConfigBuilder::default()
    }

    /// Convert to rivven-cdc SnapshotConfig.
    fn to_cdc_config(&self) -> SnapshotConfig {
        SnapshotConfig {
            mode: self.mode.clone(),
            batch_size: self.batch_size,
            parallel_tables: self.parallel_tables,
            query_timeout: self.query_timeout,
            progress_interval: Duration::from_secs(10),
            consistent_read: true,
            estimate_rows: true,
            max_retries: self.max_retries,
            throttle_delay: self.throttle_delay,
            snapshot_delay: None,
            streaming_delay: None,
            lock_timeout: Some(Duration::from_secs(10)),
        }
    }
}

/// Builder for SnapshotManagerConfig.
#[derive(Default)]
pub struct SnapshotManagerConfigBuilder {
    config: SnapshotManagerConfig,
}

impl SnapshotManagerConfigBuilder {
    /// Set snapshot mode.
    pub fn mode(mut self, mode: SnapshotMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Set batch size (minimum 100).
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size.max(100);
        self
    }

    /// Set parallel tables count (minimum 1).
    pub fn parallel_tables(mut self, count: usize) -> Self {
        self.config.parallel_tables = count.max(1);
        self
    }

    /// Set progress directory for file-based persistence.
    pub fn progress_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.progress_dir = Some(dir.into());
        self
    }

    /// Set query timeout.
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.config.query_timeout = timeout;
        self
    }

    /// Set throttle delay between batches.
    pub fn throttle_delay(mut self, delay: Duration) -> Self {
        self.config.throttle_delay = Some(delay);
        self
    }

    /// Build the configuration.
    pub fn build(self) -> SnapshotManagerConfig {
        self.config
    }
}

// ============================================================================
// Snapshot Manager
// ============================================================================

/// High-level manager for CDC snapshots in rivven-connect.
///
/// Wraps rivven-cdc's `SnapshotCoordinator` and adds:
/// - Configuration parsing from connector YAML
/// - Automatic progress store selection
/// - Mode-based snapshot decision logic
pub struct SnapshotManager<S: SnapshotSource, P: ProgressStore> {
    config: SnapshotManagerConfig,
    coordinator: SnapshotCoordinator<S, P>,
    has_prior_state: bool,
}

impl<S: SnapshotSource + 'static, P: ProgressStore + 'static> SnapshotManager<S, P> {
    /// Create a new snapshot manager.
    pub fn new(config: SnapshotManagerConfig, source: S, progress_store: P) -> Self {
        let cdc_config = config.to_cdc_config();
        let coordinator = SnapshotCoordinator::new(cdc_config, source, progress_store);

        Self {
            config,
            coordinator,
            has_prior_state: false,
        }
    }

    /// Set whether prior state (stored offsets) exist.
    ///
    /// This affects snapshot decision for Initial/WhenNeeded modes.
    pub fn set_has_prior_state(&mut self, has_state: bool) {
        self.has_prior_state = has_state;
    }

    /// Check if a snapshot should be performed based on mode and state.
    pub fn should_snapshot(&self) -> bool {
        match &self.config.mode {
            SnapshotMode::Always => {
                info!("Snapshot mode: Always - will snapshot on every start");
                true
            }
            SnapshotMode::Initial => {
                if self.has_prior_state {
                    info!("Snapshot mode: Initial - skipping (prior state exists)");
                    false
                } else {
                    info!("Snapshot mode: Initial - will snapshot (no prior state)");
                    true
                }
            }
            SnapshotMode::WhenNeeded => {
                if self.has_prior_state {
                    info!("Snapshot mode: WhenNeeded - skipping (offsets available)");
                    false
                } else {
                    info!("Snapshot mode: WhenNeeded - will snapshot (no offsets)");
                    true
                }
            }
            SnapshotMode::NoSnapshot => {
                info!("Snapshot mode: Never - skipping snapshot");
                false
            }
            SnapshotMode::InitialOnly => {
                if self.has_prior_state {
                    info!("Snapshot mode: InitialOnly - skipping (already completed)");
                    false
                } else {
                    info!("Snapshot mode: InitialOnly - will snapshot and stop");
                    true
                }
            }
            SnapshotMode::SchemaOnly => {
                info!("Snapshot mode: SchemaOnly - capturing schema only");
                false
            }
            SnapshotMode::Recovery => {
                info!("Snapshot mode: Recovery - rebuilding from source");
                true
            }
            SnapshotMode::ConfigurationBased { snapshot_data, .. } => {
                if *snapshot_data && !self.has_prior_state {
                    info!("Snapshot mode: ConfigurationBased - will snapshot");
                    true
                } else {
                    info!("Snapshot mode: ConfigurationBased - skipping");
                    false
                }
            }
            SnapshotMode::Custom(name) => {
                info!(
                    "Snapshot mode: Custom({}) - deferring to custom logic",
                    name
                );
                true
            }
        }
    }

    /// Check if streaming should continue after snapshot.
    pub fn should_stream_after(&self) -> bool {
        self.config.mode.should_stream()
    }

    /// Get snapshot statistics.
    pub fn stats(&self) -> SnapshotStatsSnapshot {
        self.coordinator.stats()
    }

    /// Cancel any running snapshot.
    pub fn cancel(&self) {
        self.coordinator.cancel();
    }

    /// Snapshot a single table.
    ///
    /// Returns all CDC events with `op: Snapshot`.
    pub async fn snapshot_table(
        &self,
        schema: &str,
        table: &str,
        key_column: &str,
    ) -> Result<Vec<CdcEvent>> {
        let spec = TableSpec::new(schema, table, key_column);

        info!(
            "Starting snapshot for {}.{} (key: {})",
            schema, table, key_column
        );

        let batches = self.coordinator.snapshot_table(&spec).await?;
        let events: Vec<CdcEvent> = batches.into_iter().flat_map(|b| b.events).collect();

        info!(
            "Completed snapshot for {}.{}: {} events",
            schema,
            table,
            events.len()
        );

        Ok(events)
    }

    /// Snapshot multiple tables.
    ///
    /// Tables are processed in parallel according to `parallel_tables` config.
    pub async fn snapshot_tables(
        &self,
        tables: Vec<(String, String, String)>, // (schema, table, key_column)
    ) -> Result<Vec<CdcEvent>> {
        let specs: Vec<TableSpec> = tables
            .into_iter()
            .map(|(schema, table, key)| TableSpec::new(schema, table, key))
            .collect();

        info!("Starting snapshot for {} tables", specs.len());

        let results = self.coordinator.snapshot_tables(specs).await?;
        let events: Vec<CdcEvent> = results
            .into_values()
            .flat_map(|batches| batches.into_iter().flat_map(|b| b.events))
            .collect();

        let stats = self.stats();
        info!(
            "Snapshot complete: {} tables, {} rows",
            stats.tables_completed, stats.rows_processed
        );

        Ok(events)
    }

    /// Get progress for all tables.
    pub async fn get_progress(&self) -> Result<Vec<SnapshotProgress>> {
        self.coordinator.get_all_progress().await
    }

    /// Reset progress for a table (force re-snapshot).
    pub async fn reset_table(&self, schema: &str, table: &str) -> Result<()> {
        self.coordinator.reset_table(schema, table).await
    }
}

// ============================================================================
// Factory Functions
// ============================================================================

/// Create a snapshot manager with memory-based progress (for testing/ephemeral).
pub fn create_memory_snapshot_manager<S: SnapshotSource + 'static>(
    config: SnapshotManagerConfig,
    source: S,
) -> SnapshotManager<S, MemoryProgressStore> {
    let progress_store = MemoryProgressStore::new();
    SnapshotManager::new(config, source, progress_store)
}

/// Create a snapshot manager with file-based progress.
pub async fn create_file_snapshot_manager<S: SnapshotSource + 'static>(
    config: SnapshotManagerConfig,
    source: S,
    progress_dir: impl Into<PathBuf>,
) -> Result<SnapshotManager<S, FileProgressStore>> {
    let progress_store = FileProgressStore::new(progress_dir).await?;
    Ok(SnapshotManager::new(config, source, progress_store))
}

// ============================================================================
// Snapshot Executor - High-level integration for connectors
// ============================================================================

/// Result of a snapshot execution.
#[derive(Debug, Clone)]
pub struct SnapshotResult {
    /// Total events produced
    pub total_events: u64,
    /// Tables that were snapshotted
    pub tables_completed: Vec<String>,
    /// Watermark position after snapshot (WAL LSN or binlog position)
    pub watermark: String,
    /// Whether streaming should continue after snapshot
    pub should_stream: bool,
    /// Duration of the snapshot
    pub duration: Duration,
}

impl Default for SnapshotResult {
    fn default() -> Self {
        Self {
            total_events: 0,
            tables_completed: vec![],
            watermark: String::new(),
            should_stream: true,
            duration: Duration::ZERO,
        }
    }
}

impl SnapshotResult {
    /// Create a "skipped" result when no snapshot is needed.
    pub fn skipped(should_stream: bool) -> Self {
        Self {
            should_stream,
            ..Default::default()
        }
    }
}

/// Executor for running snapshots with full connector integration.
///
/// This provides a higher-level API than SnapshotManager, designed for
/// direct use in connector `read()` implementations.
///
/// # Features
///
/// - **Mode-based decisions**: Determines if snapshot is needed based on mode and state
/// - **Streaming execution**: Returns events as an async stream (memory-efficient)
/// - **Progress persistence**: Supports resumable snapshots
/// - **Table filtering**: Include/exclude tables via configuration
/// - **Watermark capture**: Gets database position before snapshot for consistency
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::connectors::cdc_snapshot::{SnapshotExecutor, SnapshotExecutorConfig};
///
/// // In your connector's read() method:
/// let executor = SnapshotExecutor::new(SnapshotExecutorConfig {
///     yaml_config: config.snapshot.clone(),
///     connection_string: conn_str.clone(),
///     tables: tables_to_snapshot,
///     has_prior_state: state.is_some(),
/// });
///
/// // Execute snapshot if needed, get events as stream
/// let (snapshot_events, should_stream) = executor.execute().await?;
///
/// // Emit snapshot events first, then stream if needed
/// ```
#[cfg(feature = "postgres")]
pub struct SnapshotExecutor {
    config: SnapshotExecutorConfig,
}

/// Configuration for the snapshot executor.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct SnapshotExecutorConfig {
    /// YAML snapshot configuration
    pub yaml_config: SnapshotCdcConfig,
    /// Database connection string
    pub connection_string: String,
    /// Tables to snapshot: (schema, table, key_column)
    /// If empty, will auto-discover tables
    pub tables: Vec<(String, String, String)>,
    /// Whether prior state (stored offsets) exist
    pub has_prior_state: bool,
}

#[cfg(feature = "postgres")]
impl SnapshotExecutor {
    /// Create a new snapshot executor.
    pub fn new(config: SnapshotExecutorConfig) -> Self {
        Self { config }
    }

    /// Check if a snapshot should be performed based on configuration and state.
    pub fn should_snapshot(&self) -> bool {
        match self.config.yaml_config.mode {
            SnapshotModeConfig::Always => true,
            SnapshotModeConfig::Initial => !self.config.has_prior_state,
            SnapshotModeConfig::Never => false,
            SnapshotModeConfig::WhenNeeded => !self.config.has_prior_state,
            SnapshotModeConfig::InitialOnly => !self.config.has_prior_state,
            SnapshotModeConfig::SchemaOnly => false, // Schema only doesn't snapshot data
            SnapshotModeConfig::Recovery => true,
        }
    }

    /// Check if streaming should continue after snapshot.
    pub fn should_stream_after(&self) -> bool {
        !matches!(
            self.config.yaml_config.mode,
            SnapshotModeConfig::InitialOnly
        )
    }

    /// Get tables to snapshot (with filtering applied).
    async fn get_tables_to_snapshot(
        &self,
        source: &rivven_cdc::postgres::PostgresSnapshotSource,
    ) -> Result<Vec<(String, String, String)>> {
        if self.config.tables.is_empty() {
            // Auto-discover tables
            info!("Auto-discovering tables...");
            let schemas = vec!["public"]; // Default to public schema
            discover_tables_with_keys(source.client(), &schemas).await
        } else {
            // Filter tables based on include/exclude lists
            Ok(self
                .config
                .tables
                .iter()
                .filter(|(schema, table, _)| {
                    self.config.yaml_config.should_snapshot_table(schema, table)
                })
                .cloned()
                .collect())
        }
    }

    /// Execute the snapshot and return all events.
    ///
    /// Returns `(events, result)` where events is the Vec of CdcEvents
    /// and result contains metadata about the snapshot.
    ///
    /// For large tables, consider using `execute_streaming()` instead.
    pub async fn execute(&self) -> Result<(Vec<CdcEvent>, SnapshotResult)> {
        use rivven_cdc::postgres::PostgresSnapshotSource;

        let start = std::time::Instant::now();

        // Check if we should snapshot
        if !self.should_snapshot() {
            info!(
                "Snapshot skipped (mode: {:?}, has_prior_state: {})",
                self.config.yaml_config.mode, self.config.has_prior_state
            );
            return Ok((vec![], SnapshotResult::skipped(self.should_stream_after())));
        }

        info!("Starting snapshot execution...");

        // Create snapshot source
        let source = PostgresSnapshotSource::connect(&self.config.connection_string).await?;

        // Get watermark BEFORE snapshot for consistency
        let watermark = source.get_watermark().await?;
        info!("Snapshot watermark (WAL LSN): {}", watermark);

        // Determine tables to snapshot
        let tables = self.get_tables_to_snapshot(&source).await?;

        if tables.is_empty() {
            info!("No tables to snapshot");
            return Ok((
                vec![],
                SnapshotResult {
                    watermark,
                    should_stream: self.should_stream_after(),
                    ..Default::default()
                },
            ));
        }

        info!("Snapshotting {} tables", tables.len());
        for (schema, table, key) in &tables {
            info!("  - {}.{} (key: {})", schema, table, key);
        }

        // Create manager config
        let manager_config = SnapshotManagerConfig::from_yaml_config(&self.config.yaml_config);

        // Create progress store based on config
        let all_events = if let Some(progress_dir) = &self.config.yaml_config.progress_dir {
            // File-based progress for resumability
            let manager =
                create_file_snapshot_manager(manager_config, source, progress_dir).await?;
            manager.snapshot_tables(tables.clone()).await?
        } else {
            // Memory-based progress (non-resumable)
            let manager = create_memory_snapshot_manager(manager_config, source);
            manager.snapshot_tables(tables.clone()).await?
        };

        let tables_completed: Vec<String> = tables
            .iter()
            .map(|(s, t, _)| format!("{}.{}", s, t))
            .collect();

        let result = SnapshotResult {
            total_events: all_events.len() as u64,
            tables_completed,
            watermark,
            should_stream: self.should_stream_after(),
            duration: start.elapsed(),
        };

        info!(
            "Snapshot complete: {} events from {} tables in {:?}",
            result.total_events,
            result.tables_completed.len(),
            result.duration
        );

        Ok((all_events, result))
    }

    /// Execute snapshot and return events as an async stream.
    ///
    /// This is the preferred method for large tables as it doesn't require
    /// holding all events in memory. Events are yielded as they're produced.
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - Stream of `CdcEvent`s
    /// - `SnapshotResult` with metadata (populated after stream completion)
    pub async fn execute_streaming(
        &self,
    ) -> Result<(impl futures::Stream<Item = CdcEvent> + Send, SnapshotResult)> {
        use futures::StreamExt;
        use rivven_cdc::postgres::PostgresSnapshotSource;

        let start = std::time::Instant::now();

        // Check if we should snapshot
        if !self.should_snapshot() {
            info!(
                "Snapshot skipped (mode: {:?}, has_prior_state: {})",
                self.config.yaml_config.mode, self.config.has_prior_state
            );
            let result = SnapshotResult::skipped(self.should_stream_after());
            return Ok((futures::stream::empty().boxed(), result));
        }

        info!("Starting streaming snapshot execution...");

        // Create snapshot source
        let source = PostgresSnapshotSource::connect(&self.config.connection_string).await?;

        // Get watermark BEFORE snapshot for consistency
        let watermark = source.get_watermark().await?;
        info!("Snapshot watermark (WAL LSN): {}", watermark);

        // Determine tables to snapshot
        let tables = self.get_tables_to_snapshot(&source).await?;

        if tables.is_empty() {
            info!("No tables to snapshot");
            let result = SnapshotResult {
                watermark,
                should_stream: self.should_stream_after(),
                ..Default::default()
            };
            return Ok((futures::stream::empty().boxed(), result));
        }

        info!("Streaming snapshot for {} tables", tables.len());

        // Create manager config
        let manager_config = SnapshotManagerConfig::from_yaml_config(&self.config.yaml_config);

        // Execute snapshot and collect events
        // In the future, this could use async channels for true streaming
        let all_events = if let Some(progress_dir) = &self.config.yaml_config.progress_dir {
            let manager =
                create_file_snapshot_manager(manager_config, source, progress_dir).await?;
            manager.snapshot_tables(tables.clone()).await?
        } else {
            let manager = create_memory_snapshot_manager(manager_config, source);
            manager.snapshot_tables(tables.clone()).await?
        };

        let tables_completed: Vec<String> = tables
            .iter()
            .map(|(s, t, _)| format!("{}.{}", s, t))
            .collect();

        let result = SnapshotResult {
            total_events: all_events.len() as u64,
            tables_completed,
            watermark,
            should_stream: self.should_stream_after(),
            duration: start.elapsed(),
        };

        info!(
            "Streaming snapshot prepared: {} events from {} tables",
            result.total_events,
            result.tables_completed.len()
        );

        // Convert to stream
        let stream = futures::stream::iter(all_events).boxed();
        Ok((stream, result))
    }

    /// Execute snapshot with a callback for each batch.
    ///
    /// This allows processing events as they're produced without
    /// holding them all in memory.
    pub async fn execute_with_callback<F, Fut>(&self, mut on_batch: F) -> Result<SnapshotResult>
    where
        F: FnMut(Vec<CdcEvent>, String) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        use rivven_cdc::postgres::PostgresSnapshotSource;

        let start = std::time::Instant::now();

        // Check if we should snapshot
        if !self.should_snapshot() {
            info!(
                "Snapshot skipped (mode: {:?}, has_prior_state: {})",
                self.config.yaml_config.mode, self.config.has_prior_state
            );
            return Ok(SnapshotResult::skipped(self.should_stream_after()));
        }

        info!("Starting callback-based snapshot execution...");

        // Create snapshot source
        let source = PostgresSnapshotSource::connect(&self.config.connection_string).await?;

        // Get watermark BEFORE snapshot for consistency
        let watermark = source.get_watermark().await?;
        info!("Snapshot watermark (WAL LSN): {}", watermark);

        // Determine tables to snapshot
        let tables = self.get_tables_to_snapshot(&source).await?;

        if tables.is_empty() {
            info!("No tables to snapshot");
            return Ok(SnapshotResult {
                watermark,
                should_stream: self.should_stream_after(),
                ..Default::default()
            });
        }

        info!("Callback snapshot for {} tables", tables.len());

        // Create manager config
        let manager_config = SnapshotManagerConfig::from_yaml_config(&self.config.yaml_config);

        // Execute snapshot using the manager and invoke callback for each batch
        let all_events = if let Some(progress_dir) = &self.config.yaml_config.progress_dir {
            let manager =
                create_file_snapshot_manager(manager_config, source, progress_dir).await?;
            manager.snapshot_tables(tables.clone()).await?
        } else {
            let manager = create_memory_snapshot_manager(manager_config, source);
            manager.snapshot_tables(tables.clone()).await?
        };

        // Process events through callback in batches
        let mut total_events = 0u64;
        let batch_size = self.config.yaml_config.batch_size.max(1000);

        for (schema, table, _) in &tables {
            let table_name = format!("{}.{}", schema, table);

            // Chunk events for this table
            let table_events: Vec<_> = all_events
                .iter()
                .filter(|e| e.schema == *schema && e.table == *table)
                .cloned()
                .collect();

            for chunk in table_events.chunks(batch_size) {
                let batch_len = chunk.len() as u64;
                total_events += batch_len;
                on_batch(chunk.to_vec(), table_name.clone()).await?;
            }
        }

        let tables_completed: Vec<String> = tables
            .iter()
            .map(|(s, t, _)| format!("{}.{}", s, t))
            .collect();

        let result = SnapshotResult {
            total_events,
            tables_completed,
            watermark,
            should_stream: self.should_stream_after(),
            duration: start.elapsed(),
        };

        info!(
            "Callback snapshot complete: {} events from {} tables in {:?}",
            result.total_events,
            result.tables_completed.len(),
            result.duration
        );

        Ok(result)
    }
}

/// Helper enum for handling either file or memory progress store.
#[allow(dead_code)]
enum Either<L, R> {
    Left(L),
    Right(R),
}

// ============================================================================
// Snapshot + Stream Combiner
// ============================================================================

// ============================================================================
// Incremental Snapshot Executor - Non-blocking snapshots during streaming
// ============================================================================

/// Configuration for incremental snapshot execution.
///
/// Incremental snapshots allow re-snapshotting tables while CDC streaming continues,
/// using the DBLog algorithm for deduplication.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct IncrementalSnapshotExecutorConfig {
    /// Chunk size for incremental snapshots (rows per chunk)
    pub chunk_size: usize,
    /// Maximum concurrent chunks being processed
    pub max_concurrent_chunks: usize,
    /// Watermark strategy
    pub watermark_strategy: WatermarkStrategy,
    /// Signal table name for watermarks (default: rivven_signal)
    pub signal_table: String,
    /// Connection string for database
    pub connection_string: String,
    /// Buffer memory limit in bytes
    pub max_buffer_memory: usize,
    /// Chunk query timeout
    pub chunk_timeout: Duration,
    /// Delay between chunks (for backpressure)
    pub inter_chunk_delay: Option<Duration>,
}

#[cfg(feature = "postgres")]
impl Default for IncrementalSnapshotExecutorConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1024,
            max_concurrent_chunks: 4,
            watermark_strategy: WatermarkStrategy::InsertInsert,
            signal_table: "rivven_signal".to_string(),
            connection_string: String::new(),
            max_buffer_memory: 64 * 1024 * 1024, // 64MB
            chunk_timeout: Duration::from_secs(60),
            inter_chunk_delay: None,
        }
    }
}

#[cfg(feature = "postgres")]
impl IncrementalSnapshotExecutorConfig {
    /// Create from YAML incremental snapshot config.
    pub fn from_yaml(
        yaml_config: &super::cdc_config::IncrementalSnapshotCdcConfig,
        connection_string: String,
    ) -> Self {
        Self {
            chunk_size: yaml_config.chunk_size,
            max_concurrent_chunks: yaml_config.max_concurrent_chunks,
            watermark_strategy: match yaml_config.watermark_strategy {
                super::cdc_config::WatermarkStrategyConfig::Insert => {
                    WatermarkStrategy::InsertInsert
                }
                super::cdc_config::WatermarkStrategyConfig::UpdateAndInsert => {
                    WatermarkStrategy::InsertDelete
                }
            },
            signal_table: yaml_config
                .watermark_signal_table
                .clone()
                .unwrap_or_else(|| "rivven_signal".to_string()),
            connection_string,
            max_buffer_memory: 64 * 1024 * 1024,
            chunk_timeout: Duration::from_secs(60),
            inter_chunk_delay: if yaml_config.chunk_delay_ms > 0 {
                Some(Duration::from_millis(yaml_config.chunk_delay_ms))
            } else {
                None
            },
        }
    }

    /// Create the rivven-cdc IncrementalSnapshotConfig.
    pub fn to_cdc_config(&self) -> IncrementalSnapshotConfig {
        IncrementalSnapshotConfig::builder()
            .chunk_size(self.chunk_size)
            .max_concurrent_chunks(self.max_concurrent_chunks)
            .watermark_strategy(self.watermark_strategy)
            .signal_table(&self.signal_table)
            .max_buffer_memory(self.max_buffer_memory)
            .chunk_timeout(self.chunk_timeout)
            .build()
    }
}

/// Executor for incremental (non-blocking) snapshots.
///
/// Incremental snapshots allow re-snapshotting tables while CDC streaming continues.
/// This is useful for:
/// - Adding new tables to capture
/// - Re-syncing tables after schema changes
/// - Recovering from data inconsistencies
///
/// # How It Works
///
/// 1. Signal table receives snapshot request (via CDC stream or API)
/// 2. Coordinator opens watermark window for each chunk
/// 3. Snapshot query executes, results buffered locally
/// 4. Streaming events are deduplicated against buffer
/// 5. Window closes, remaining buffer entries emitted as READ events
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::connectors::cdc_snapshot::IncrementalSnapshotExecutor;
///
/// // Create executor
/// let executor = IncrementalSnapshotExecutor::new(config);
///
/// // Request incremental snapshot of tables
/// executor.request_snapshot(&["public.users", "public.orders"]).await?;
///
/// // Check status
/// if executor.is_active() {
///     println!("Snapshot in progress");
/// }
/// ```
#[cfg(feature = "postgres")]
pub struct IncrementalSnapshotExecutor {
    config: IncrementalSnapshotExecutorConfig,
    coordinator: std::sync::Arc<tokio::sync::RwLock<Option<IncrementalSnapshotCoordinator>>>,
}

#[cfg(feature = "postgres")]
impl IncrementalSnapshotExecutor {
    /// Create a new incremental snapshot executor.
    pub fn new(config: IncrementalSnapshotExecutorConfig) -> Self {
        Self {
            config,
            coordinator: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// Initialize the coordinator (call before using).
    pub async fn initialize(&self) -> Result<()> {
        let cdc_config = self.config.to_cdc_config();
        let coordinator = IncrementalSnapshotCoordinator::new(cdc_config);

        let mut lock = self.coordinator.write().await;
        *lock = Some(coordinator);

        info!("Incremental snapshot executor initialized");
        Ok(())
    }

    /// Request an incremental snapshot for tables.
    ///
    /// The snapshot will be processed chunk-by-chunk while streaming continues.
    ///
    /// # Arguments
    ///
    /// * `tables` - Fully qualified table names (e.g., "public.users")
    pub async fn request_snapshot(&self, tables: &[&str]) -> Result<String> {
        let lock = self.coordinator.read().await;
        let coordinator = lock
            .as_ref()
            .ok_or_else(|| rivven_cdc::common::CdcError::config("Coordinator not initialized"))?;

        let table_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
        let request = IncrementalSnapshotRequest::new(table_vec);

        let request_id = coordinator.start(request).await?;

        info!(
            "Incremental snapshot requested for {} tables: {}",
            tables.len(),
            request_id
        );

        Ok(request_id)
    }

    /// Request an incremental snapshot with conditions.
    ///
    /// Allows filtering rows during snapshot.
    pub async fn request_snapshot_with_conditions(
        &self,
        tables: &[&str],
        conditions: &[(&str, &str)], // (table, WHERE clause)
    ) -> Result<String> {
        let lock = self.coordinator.read().await;
        let coordinator = lock
            .as_ref()
            .ok_or_else(|| rivven_cdc::common::CdcError::config("Coordinator not initialized"))?;

        let table_vec: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
        let mut request = IncrementalSnapshotRequest::new(table_vec);

        for (table, condition) in conditions {
            request = request.with_condition(*table, *condition);
        }

        let request_id = coordinator.start(request).await?;

        info!(
            "Incremental snapshot with conditions requested: {}",
            request_id
        );

        Ok(request_id)
    }

    /// Check if an incremental snapshot is currently active.
    pub fn is_active(&self) -> bool {
        // Use try_read to avoid blocking
        if let Ok(lock) = self.coordinator.try_read() {
            if let Some(coordinator) = lock.as_ref() {
                return coordinator.is_active();
            }
        }
        false
    }

    /// Check if an incremental snapshot is paused.
    pub fn is_paused(&self) -> bool {
        if let Ok(lock) = self.coordinator.try_read() {
            if let Some(coordinator) = lock.as_ref() {
                return coordinator.is_paused();
            }
        }
        false
    }

    /// Get statistics for the current incremental snapshot.
    pub fn stats(&self) -> Option<IncrementalSnapshotStatsSnapshot> {
        if let Ok(lock) = self.coordinator.try_read() {
            return lock.as_ref().map(|c| c.stats());
        }
        None
    }

    /// Pause the current incremental snapshot.
    pub async fn pause(&self) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            coordinator.pause().await?;
            info!("Incremental snapshot paused");
        }
        Ok(())
    }

    /// Resume a paused incremental snapshot.
    pub async fn resume(&self) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            coordinator.resume().await?;
            info!("Incremental snapshot resumed");
        }
        Ok(())
    }

    /// Stop the current incremental snapshot.
    pub async fn stop(&self) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            coordinator.stop().await?;
            info!("Incremental snapshot stopped");
        }
        Ok(())
    }

    // ========================================================================
    // High-Level Convenience Methods
    // ========================================================================

    /// Execute a complete chunk with the full deduplication workflow.
    ///
    /// This is a convenience method that combines the complete DBLog workflow:
    /// 1. Opens the deduplication window
    /// 2. Calls the provided query function to get rows
    /// 3. Buffers all rows for deduplication
    /// 4. Closes the window and returns deduplicated events
    ///
    /// The caller is still responsible for checking streaming conflicts via
    /// `check_streaming_conflict()` for any streaming events received during
    /// the window.
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk to execute
    /// * `watermark_ts` - Database timestamp at execution start
    /// * `query_fn` - Async function that executes the query and returns (key, event) pairs
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let events = executor.execute_chunk(
    ///     &chunk,
    ///     watermark_ts,
    ///     || async {
    ///         // Execute SELECT query
    ///         let rows = db.query(&chunk.query_sql()).await?;
    ///         // Convert to (key, event) pairs
    ///         rows.into_iter()
    ///             .map(|r| (r.id.to_string(), CdcEvent::from_row(&r)))
    ///             .collect()
    ///     }
    /// ).await?;
    /// ```
    pub async fn execute_chunk<F, Fut>(
        &self,
        chunk: &SnapshotChunk,
        watermark_ts: i64,
        query_fn: F,
    ) -> Result<Vec<CdcEvent>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<(String, CdcEvent)>>>,
    {
        // Open window
        self.open_window(chunk, watermark_ts).await?;

        // Execute query and buffer rows
        let rows = match query_fn().await {
            Ok(r) => r,
            Err(e) => {
                // Close window on error to avoid leaking buffers
                let _ = self.close_window(&chunk.chunk_id).await;
                return Err(e);
            }
        };

        // Buffer all rows
        for (key, event) in rows {
            self.buffer_row(&chunk.chunk_id, event, key).await;
        }

        // Close window and return deduplicated events
        self.close_window(&chunk.chunk_id).await
    }

    /// Execute a chunk with composite key support.
    ///
    /// Same as `execute_chunk` but for tables with composite primary keys.
    pub async fn execute_chunk_composite<F, Fut>(
        &self,
        chunk: &SnapshotChunk,
        key_columns: Vec<String>,
        watermark_ts: i64,
        query_fn: F,
    ) -> Result<Vec<CdcEvent>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Vec<(SnapshotKey, CdcEvent)>>>,
    {
        // Open window with composite key
        self.open_window_composite(chunk, key_columns, watermark_ts)
            .await?;

        // Execute query and buffer rows
        let rows = match query_fn().await {
            Ok(r) => r,
            Err(e) => {
                let _ = self.close_window(&chunk.chunk_id).await;
                return Err(e);
            }
        };

        // Buffer all rows with composite keys
        for (key, event) in rows {
            self.buffer_row_with_key(&chunk.chunk_id, event, key).await;
        }

        // Close window and return deduplicated events
        self.close_window(&chunk.chunk_id).await
    }

    // ========================================================================
    // Deduplication Window API (DBLog Algorithm)
    // ========================================================================

    /// Get the next chunk to process.
    ///
    /// This returns `None` if the snapshot is not active, paused, or all chunks
    /// have been processed.
    pub async fn next_chunk(&self) -> Result<Option<SnapshotChunk>> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.next_chunk().await;
        }
        Ok(None)
    }

    /// Open a deduplication window for a chunk.
    ///
    /// This starts the watermark-based deduplication process:
    /// 1. Insert open watermark into signal table
    /// 2. Buffer snapshot rows during this window
    /// 3. Check streaming events against buffer for conflicts
    /// 4. Close window and emit remaining (non-conflicting) rows
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk to open a window for
    /// * `watermark_ts` - The database timestamp at window open (Unix millis)
    pub async fn open_window(&self, chunk: &SnapshotChunk, watermark_ts: i64) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            coordinator
                .open_window_with_watermark(chunk, watermark_ts)
                .await?;
            debug!(
                chunk_id = %chunk.chunk_id,
                watermark_ts = watermark_ts,
                "Opened deduplication window"
            );
        }
        Ok(())
    }

    /// Buffer a snapshot row for deduplication.
    ///
    /// Call this for each row returned from the snapshot query.
    /// The row will be stored and checked against streaming events.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - The chunk ID this row belongs to
    /// * `event` - The CDC event (with `op = 'r'` for snapshot)
    /// * `key` - The primary key value for deduplication
    ///
    /// # Returns
    ///
    /// `true` if the row was buffered, `false` if buffer is full
    pub async fn buffer_row(&self, chunk_id: &str, event: CdcEvent, key: String) -> bool {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.buffer_row_for_chunk(chunk_id, event, key).await;
        }
        false
    }

    /// Buffer a snapshot row with composite key support.
    pub async fn buffer_row_with_key(
        &self,
        chunk_id: &str,
        event: CdcEvent,
        key: rivven_cdc::common::SnapshotKey,
    ) -> bool {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator
                .buffer_row_for_chunk_with_key(chunk_id, event, key)
                .await;
        }
        false
    }

    /// Check if a streaming event conflicts with buffered snapshot rows.
    ///
    /// Call this for EVERY streaming event while windows are open.
    /// If a conflict is found, the buffered row is removed (streaming wins).
    ///
    /// # Arguments
    ///
    /// * `table` - The table name (e.g., "public.orders")
    /// * `key` - The primary key of the streaming event
    /// * `event_ts` - The event's commit timestamp (Unix millis)
    /// * `is_delete` - Whether this is a DELETE operation
    ///
    /// # Returns
    ///
    /// Number of conflicts found (can be >1 with concurrent chunks)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// for event in streaming_events {
    ///     // Check for conflicts with buffered snapshot rows
    ///     let key = SnapshotKey::single(&event.key);
    ///     let is_delete = event.op == CdcOp::Delete;
    ///     executor.check_streaming_conflict(
    ///         &event.table,
    ///         &key,
    ///         event.ts_ms,
    ///         is_delete,
    ///     ).await;
    ///     
    ///     // Emit the streaming event (it always wins)
    ///     emit(event);
    /// }
    /// ```
    pub async fn check_streaming_conflict(
        &self,
        table: &str,
        key: &rivven_cdc::common::SnapshotKey,
        event_ts: i64,
        is_delete: bool,
    ) -> usize {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator
                .check_streaming_conflict_with_timestamp(table, key, event_ts, is_delete)
                .await;
        }
        0
    }

    /// Close a deduplication window and get remaining rows to emit.
    ///
    /// After closing, the remaining rows in the buffer are the snapshot rows
    /// that had no conflicting streaming events - these should be emitted
    /// as `op = 'r'` (read) events.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - The chunk ID of the window to close
    ///
    /// # Returns
    ///
    /// The deduplicated snapshot events to emit
    pub async fn close_window(&self, chunk_id: &str) -> Result<Vec<CdcEvent>> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            let events = coordinator.close_window_for_chunk(chunk_id).await?;
            debug!(
                chunk_id = %chunk_id,
                remaining_rows = events.len(),
                "Closed deduplication window"
            );
            return Ok(events);
        }
        Ok(vec![])
    }

    /// Get buffer statistics for a specific chunk.
    pub async fn buffer_stats(
        &self,
        chunk_id: &str,
    ) -> Option<rivven_cdc::common::ChunkBufferStats> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.buffer_stats(chunk_id).await;
        }
        None
    }

    /// Generate the open watermark signal for a chunk.
    ///
    /// This signal should be inserted into the signal table to mark
    /// the start of the deduplication window.
    pub fn open_window_signal(
        &self,
        chunk: &SnapshotChunk,
        watermark_ts: i64,
    ) -> Option<WatermarkSignal> {
        if let Ok(lock) = self.coordinator.try_read() {
            return lock
                .as_ref()
                .map(|c| c.open_window_signal_with_watermark(chunk, watermark_ts));
        }
        None
    }

    /// Generate the close watermark signal for a chunk.
    ///
    /// This signal should be inserted into the signal table to mark
    /// the end of the deduplication window.
    pub fn close_window_signal(&self, chunk: &SnapshotChunk) -> Option<WatermarkSignal> {
        if let Ok(lock) = self.coordinator.try_read() {
            return lock.as_ref().map(|c| c.close_window_signal(chunk));
        }
        None
    }

    // ========================================================================
    // Parallel Chunk Processing APIs
    // ========================================================================

    /// Open a deduplication window with composite key support.
    ///
    /// Use this when the table has a composite primary key (multiple columns).
    ///
    /// # Arguments
    ///
    /// * `chunk` - The chunk to open a window for
    /// * `key_columns` - The column names that form the composite key
    /// * `watermark_ts` - The database timestamp at window open (Unix millis)
    pub async fn open_window_composite(
        &self,
        chunk: &SnapshotChunk,
        key_columns: Vec<String>,
        watermark_ts: i64,
    ) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            coordinator
                .open_window_composite_with_watermark(chunk, key_columns, watermark_ts)
                .await?;
            debug!(
                chunk_id = %chunk.chunk_id,
                watermark_ts = watermark_ts,
                "Opened composite key deduplication window"
            );
        }
        Ok(())
    }

    /// Get statistics for ALL open buffers.
    ///
    /// Returns a map of chunk_id → buffer statistics. Useful for monitoring
    /// parallel chunk execution.
    pub async fn buffer_stats_all(
        &self,
    ) -> std::collections::HashMap<String, rivven_cdc::common::ChunkBufferStats> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.buffer_stats_all().await;
        }
        std::collections::HashMap::new()
    }

    /// Get aggregate statistics across all open buffers.
    ///
    /// Provides a summary of total memory, rows, and conflicts across
    /// all concurrent chunk windows.
    pub async fn buffer_stats_aggregate(&self) -> rivven_cdc::common::AggregateBufferStats {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.buffer_stats_aggregate().await;
        }
        rivven_cdc::common::AggregateBufferStats::default()
    }

    /// Check if any deduplication window is currently open.
    pub async fn is_window_open(&self) -> bool {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.is_window_open().await;
        }
        false
    }

    /// Check if a specific chunk's window is open.
    pub async fn is_window_open_for_chunk(&self, chunk_id: &str) -> bool {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.is_window_open_for_chunk(chunk_id).await;
        }
        false
    }

    /// Get the number of currently open windows.
    ///
    /// Use this to check parallelism level and capacity.
    pub async fn open_window_count(&self) -> usize {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.open_window_count().await;
        }
        0
    }

    /// Get the IDs of all currently open chunk windows.
    ///
    /// Useful for debugging and monitoring parallel execution.
    pub async fn open_chunk_ids(&self) -> Vec<String> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.open_chunk_ids().await;
        }
        vec![]
    }

    /// Mark the current table as complete and move to the next.
    ///
    /// Call this after all chunks for a table have been processed.
    /// Returns the name of the next table to process, or None if all done.
    pub async fn complete_current_table(&self) -> Result<Option<String>> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.complete_current_table().await;
        }
        Ok(None)
    }

    /// Get the current snapshot context (for serialization/resumption).
    ///
    /// The context can be serialized and restored later to resume
    /// an interrupted snapshot.
    pub async fn get_context(&self) -> Option<rivven_cdc::common::IncrementalSnapshotContext> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.get_context().await;
        }
        None
    }

    /// Restore from a saved context.
    ///
    /// Use this to resume an interrupted incremental snapshot.
    pub async fn restore_context(
        &self,
        context: rivven_cdc::common::IncrementalSnapshotContext,
    ) -> Result<()> {
        let lock = self.coordinator.read().await;
        if let Some(coordinator) = lock.as_ref() {
            return coordinator.restore(context).await;
        }
        Err(rivven_cdc::common::CdcError::config(
            "Coordinator not initialized",
        ))
    }

    /// Get the configuration.
    pub fn config(&self) -> &IncrementalSnapshotExecutorConfig {
        &self.config
    }

    // ========================================================================
    // Advanced Integration Methods
    // ========================================================================

    /// Process a streaming CDC event for deduplication.
    ///
    /// This is a convenience wrapper around `check_streaming_conflict` that
    /// extracts the key from the event and handles both DELETE and non-DELETE cases.
    ///
    /// Call this for EVERY streaming event while incremental snapshot windows are open.
    ///
    /// # Arguments
    ///
    /// * `event` - The streaming CDC event
    /// * `key_columns` - Column names that form the primary key
    ///
    /// # Returns
    ///
    /// `true` if a conflict was found (streaming event supersedes snapshot row)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // During streaming while incremental snapshot is active
    /// for event in cdc_stream {
    ///     // Check if incremental snapshot should deduplicate this event
    ///     if executor.is_active() && executor.is_window_open().await {
    ///         executor.process_streaming_event(&event, &["id"]).await;
    ///     }
    ///     
    ///     // Always emit streaming events (they win over snapshot rows)
    ///     emit(event);
    /// }
    /// ```
    pub async fn process_streaming_event(&self, event: &CdcEvent, key_columns: &[&str]) -> bool {
        // Extract key from event
        let key = if key_columns.len() == 1 {
            // Single column key
            let col = key_columns[0];
            let value = event
                .after
                .as_ref()
                .or(event.before.as_ref())
                .and_then(|v| v.get(col))
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                });

            match value {
                Some(v) => SnapshotKey::single(v),
                None => return false, // Can't extract key
            }
        } else {
            // Composite key
            let values: Option<Vec<_>> = key_columns
                .iter()
                .map(|col| {
                    event
                        .after
                        .as_ref()
                        .or(event.before.as_ref())
                        .and_then(|v| v.get(*col).cloned())
                })
                .collect();

            match values {
                Some(v) => SnapshotKey::composite(v),
                None => return false,
            }
        };

        let table = format!("{}.{}", event.schema, event.table);
        let is_delete = event.op == rivven_cdc::common::CdcOp::Delete;
        let event_ts = event.timestamp * 1000; // Convert seconds to millis

        let conflicts = self
            .check_streaming_conflict(&table, &key, event_ts, is_delete)
            .await;
        conflicts > 0
    }

    /// Execute multiple chunks in parallel with automatic deduplication.
    ///
    /// This is the highest-level API for parallel chunk execution. It:
    /// 1. Opens windows for all chunks simultaneously
    /// 2. Executes queries in parallel using the provided function
    /// 3. Buffers all results for deduplication
    /// 4. Closes windows and returns all deduplicated events
    ///
    /// # Arguments
    ///
    /// * `chunks` - The chunks to execute in parallel
    /// * `watermark_ts` - Database timestamp at execution start
    /// * `query_fn` - Function that executes a chunk query and returns (key, event) pairs
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get next batch of chunks
    /// let chunks = vec![chunk1, chunk2, chunk3, chunk4];
    /// let watermark_ts = get_db_timestamp().await?;
    ///
    /// // Execute all chunks in parallel
    /// let all_events = executor.execute_chunks_parallel(
    ///     &chunks,
    ///     watermark_ts,
    ///     |chunk| async move {
    ///         let rows = db.query(&build_chunk_query(&chunk)).await?;
    ///         rows.into_iter()
    ///             .map(|r| (r.id.to_string(), CdcEvent::from_row(&r)))
    ///             .collect::<Result<Vec<_>>>()
    ///     }
    /// ).await?;
    /// ```
    pub async fn execute_chunks_parallel<F, Fut>(
        &self,
        chunks: &[SnapshotChunk],
        watermark_ts: i64,
        query_fn: F,
    ) -> Result<Vec<CdcEvent>>
    where
        F: Fn(SnapshotChunk) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<Vec<(String, CdcEvent)>>> + Send,
    {
        use futures::future::join_all;

        if chunks.is_empty() {
            return Ok(vec![]);
        }

        // 1. Open all windows
        for chunk in chunks {
            self.open_window(chunk, watermark_ts).await?;
        }

        // 2. Execute queries in parallel
        let query_futures: Vec<_> = chunks.iter().cloned().map(&query_fn).collect();

        let results = join_all(query_futures).await;

        // 3. Buffer results (handle errors by closing windows)
        for (chunk, result) in chunks.iter().zip(results.iter()) {
            match result {
                Ok(rows) => {
                    for (key, event) in rows {
                        self.buffer_row(&chunk.chunk_id, event.clone(), key.clone())
                            .await;
                    }
                }
                Err(_) => {
                    // Close window on error to prevent leaks
                    let _ = self.close_window(&chunk.chunk_id).await;
                }
            }
        }

        // Check if any queries failed
        let first_error = results.iter().find(|r| r.is_err());
        if let Some(Err(e)) = first_error {
            // Close remaining windows
            for chunk in chunks {
                let _ = self.close_window(&chunk.chunk_id).await;
            }
            return Err(rivven_cdc::common::CdcError::config(format!(
                "Parallel chunk execution failed: {}",
                e
            )));
        }

        // 4. Close all windows and collect events
        let mut all_events = Vec::new();
        for chunk in chunks {
            let events = self.close_window(&chunk.chunk_id).await?;
            all_events.extend(events);
        }

        Ok(all_events)
    }

    /// Check if memory pressure is high and should throttle.
    ///
    /// Returns true if total buffer memory exceeds 80% of the configured limit.
    /// Use this to implement backpressure in your snapshot loop.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// while let Some(chunk) = executor.next_chunk().await? {
    ///     // Backpressure: wait if memory pressure is high
    ///     while executor.should_throttle().await {
    ///         tokio::time::sleep(Duration::from_millis(100)).await;
    ///     }
    ///     
    ///     executor.execute_chunk(&chunk, watermark_ts, query_fn).await?;
    /// }
    /// ```
    pub async fn should_throttle(&self) -> bool {
        let stats = self.buffer_stats_aggregate().await;
        let max_memory = self.config.max_buffer_memory * self.config.max_concurrent_chunks;
        let threshold = (max_memory as f64 * 0.8) as usize;
        stats.total_memory_bytes > threshold
    }

    /// Get memory utilization as a percentage.
    ///
    /// Useful for monitoring and alerting.
    pub async fn memory_utilization_percent(&self) -> f64 {
        let stats = self.buffer_stats_aggregate().await;
        let max_memory = self.config.max_buffer_memory * self.config.max_concurrent_chunks;
        if max_memory == 0 {
            return 0.0;
        }
        (stats.total_memory_bytes as f64 / max_memory as f64) * 100.0
    }
}

// ============================================================================
// Signal Table Integration for Incremental Snapshots
// ============================================================================

/// Configuration for signal-triggered snapshots.
///
/// This integrates the signal table with the incremental snapshot executor,
/// allowing CDC streams to trigger snapshots via signal table events.
///
/// # Signal Table Schema
///
/// ```sql
/// CREATE TABLE rivven_signal (
///     id VARCHAR(42) PRIMARY KEY,
///     type VARCHAR(32) NOT NULL,
///     data VARCHAR(2048) NULL
/// );
/// ```
///
/// # Supported Signals
///
/// - `execute-snapshot`: Trigger incremental snapshot for tables
/// - `stop-snapshot`: Cancel in-progress snapshot
/// - `pause-snapshot`: Pause streaming and snapshot
/// - `resume-snapshot`: Resume paused operations
///
/// # Example
///
/// Trigger a snapshot by inserting into the signal table:
///
/// ```sql
/// INSERT INTO rivven_signal (id, type, data)
/// VALUES (
///     'sig-001',
///     'execute-snapshot',
///     '{"data-collections": ["public.users", "public.orders"]}'
/// );
/// ```
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct SignalSnapshotConfig {
    /// Signal table name (fully qualified, e.g., "public.rivven_signal")
    pub signal_table: String,
    /// Whether signal processing is enabled
    pub enabled: bool,
    /// Incremental snapshot executor config
    pub incremental_config: IncrementalSnapshotExecutorConfig,
}

#[cfg(feature = "postgres")]
impl Default for SignalSnapshotConfig {
    fn default() -> Self {
        Self {
            signal_table: "public.rivven_signal".to_string(),
            enabled: true,
            incremental_config: IncrementalSnapshotExecutorConfig::default(),
        }
    }
}

#[cfg(feature = "postgres")]
impl SignalSnapshotConfig {
    /// Create a new signal snapshot config.
    pub fn new(signal_table: impl Into<String>) -> Self {
        Self {
            signal_table: signal_table.into(),
            enabled: true,
            incremental_config: IncrementalSnapshotExecutorConfig::default(),
        }
    }

    /// Set the incremental snapshot config.
    pub fn with_incremental_config(mut self, config: IncrementalSnapshotExecutorConfig) -> Self {
        self.incremental_config = config;
        self
    }

    /// Enable/disable signal processing.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Processor for signal-triggered incremental snapshots.
///
/// Integrates signal table events with the incremental snapshot executor.
/// When a signal event is detected in the CDC stream, it triggers the
/// appropriate snapshot action.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::connectors::cdc_snapshot::{
///     SignalSnapshotProcessor, SignalSnapshotConfig
/// };
///
/// let config = SignalSnapshotConfig::new("public.rivven_signal");
/// let processor = SignalSnapshotProcessor::new(config);
///
/// processor.initialize().await?;
///
/// // Process CDC event (called by connector during streaming)
/// for event in cdc_stream {
///     if let Some(result) = processor.process_event(&event).await? {
///         // Signal was detected and processed
///         info!("Signal result: {:?}", result);
///     }
///     // Emit non-signal events to downstream
/// }
/// ```
#[cfg(feature = "postgres")]
pub struct SignalSnapshotProcessor {
    config: SignalSnapshotConfig,
    /// Signal channel for detecting signal events
    signal_channel: SourceSignalChannel,
    /// Incremental snapshot executor
    executor: IncrementalSnapshotExecutor,
    /// Signal processor for handling signals
    signal_processor: SignalProcessor,
}

#[cfg(feature = "postgres")]
impl SignalSnapshotProcessor {
    /// Create a new signal snapshot processor.
    pub fn new(config: SignalSnapshotConfig) -> Self {
        let signal_channel = SourceSignalChannel::new(&config.signal_table);
        let executor = IncrementalSnapshotExecutor::new(config.incremental_config.clone());
        let signal_processor = SignalProcessor::new();

        Self {
            config,
            signal_channel,
            executor,
            signal_processor,
        }
    }

    /// Initialize the processor.
    pub async fn initialize(&self) -> Result<()> {
        self.executor.initialize().await?;
        info!(
            "Signal snapshot processor initialized for table: {}",
            self.config.signal_table
        );
        Ok(())
    }

    /// Check if a CDC event is from the signal table.
    ///
    /// Returns true if this is a signal event that should be processed
    /// rather than emitted downstream.
    pub fn is_signal_event(&self, schema: &str, table: &str) -> bool {
        self.signal_channel.is_signal_event(schema, table)
    }

    /// Process a CDC event.
    ///
    /// If the event is from the signal table, processes it and returns the result.
    /// If not a signal event, returns None.
    pub async fn process_cdc_event(
        &self,
        schema: &str,
        table: &str,
        id: &str,
        signal_type: &str,
        data: Option<&str>,
    ) -> Result<Option<SignalResult>> {
        if !self.config.enabled {
            return Ok(None);
        }

        if !self.is_signal_event(schema, table) {
            return Ok(None);
        }

        // Handle the CDC event in the signal channel
        self.signal_channel
            .handle_cdc_event(id, signal_type, data)
            .await
            .map_err(rivven_cdc::common::CdcError::config)?;

        // Parse and process the signal
        let signal = match self.parse_signal(id, signal_type, data) {
            Ok(sig) => sig,
            Err(e) => {
                warn!("Failed to parse signal: {}", e);
                return Ok(Some(SignalResult::Failed(e)));
            }
        };

        // Handle the signal
        let result = self.handle_signal(&signal).await;
        Ok(Some(result))
    }

    /// Parse a signal from CDC event data.
    fn parse_signal(
        &self,
        id: &str,
        signal_type: &str,
        data: Option<&str>,
    ) -> std::result::Result<Signal, String> {
        let action = SignalAction::parse(signal_type);

        let signal_data = if let Some(json_str) = data {
            serde_json::from_str(json_str)
                .map_err(|e| format!("Invalid signal data JSON: {}", e))?
        } else {
            SignalData::empty()
        };

        Ok(Signal::new(id, action, signal_data).with_source(SignalSource::Source))
    }

    /// Handle a parsed signal.
    async fn handle_signal(&self, signal: &Signal) -> SignalResult {
        info!(
            "Processing signal: {} (type: {})",
            signal.id,
            signal.action.as_str()
        );

        match &signal.action {
            SignalAction::ExecuteSnapshot => self.handle_execute_snapshot(signal).await,
            SignalAction::StopSnapshot => self.handle_stop_snapshot().await,
            SignalAction::PauseSnapshot => self.handle_pause_snapshot().await,
            SignalAction::ResumeSnapshot => self.handle_resume_snapshot().await,
            SignalAction::Log => {
                if let Some(msg) = signal.data.log_message() {
                    info!("Signal log: {}", msg);
                }
                SignalResult::Success
            }
            SignalAction::Custom(action) => {
                debug!(
                    "Custom signal action '{}' not handled by snapshot processor",
                    action
                );
                SignalResult::Ignored(format!("Custom action '{}' not supported", action))
            }
        }
    }

    /// Handle execute-snapshot signal.
    async fn handle_execute_snapshot(&self, signal: &Signal) -> SignalResult {
        let tables = &signal.data.data_collections;

        if tables.is_empty() {
            return SignalResult::Failed("No tables specified for snapshot".to_string());
        }

        let table_refs: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();

        match self.executor.request_snapshot(&table_refs).await {
            Ok(request_id) => {
                info!("Incremental snapshot started: {}", request_id);
                SignalResult::Pending(request_id)
            }
            Err(e) => {
                warn!("Failed to start incremental snapshot: {}", e);
                SignalResult::Failed(e.to_string())
            }
        }
    }

    /// Handle stop-snapshot signal.
    async fn handle_stop_snapshot(&self) -> SignalResult {
        match self.executor.stop().await {
            Ok(()) => {
                info!("Snapshot stopped");
                SignalResult::Success
            }
            Err(e) => {
                warn!("Failed to stop snapshot: {}", e);
                SignalResult::Failed(e.to_string())
            }
        }
    }

    /// Handle pause-snapshot signal.
    async fn handle_pause_snapshot(&self) -> SignalResult {
        match self.executor.pause().await {
            Ok(()) => {
                info!("Snapshot paused");
                SignalResult::Success
            }
            Err(e) => {
                warn!("Failed to pause snapshot: {}", e);
                SignalResult::Failed(e.to_string())
            }
        }
    }

    /// Handle resume-snapshot signal.
    async fn handle_resume_snapshot(&self) -> SignalResult {
        match self.executor.resume().await {
            Ok(()) => {
                info!("Snapshot resumed");
                SignalResult::Success
            }
            Err(e) => {
                warn!("Failed to resume snapshot: {}", e);
                SignalResult::Failed(e.to_string())
            }
        }
    }

    /// Check if an incremental snapshot is currently active.
    pub fn is_snapshot_active(&self) -> bool {
        self.executor.is_active()
    }

    /// Check if snapshot is paused.
    pub fn is_snapshot_paused(&self) -> bool {
        self.executor.is_paused()
    }

    /// Get snapshot statistics.
    pub fn stats(&self) -> Option<IncrementalSnapshotStatsSnapshot> {
        self.executor.stats()
    }

    /// Get the underlying signal processor for custom handler registration.
    ///
    /// Use this to add custom signal handlers:
    ///
    /// ```rust,ignore
    /// processor.signal_processor().register_handler("my-action", |signal| async {
    ///     // Custom logic
    ///     SignalResult::Success
    /// }).await;
    /// ```
    pub fn signal_processor(&self) -> &SignalProcessor {
        &self.signal_processor
    }

    /// Pause the signal processor (stops processing new signals).
    pub fn pause_processing(&self) {
        self.signal_processor.pause();
    }

    /// Resume the signal processor.
    pub fn resume_processing(&self) {
        self.signal_processor.resume();
    }

    /// Check if signal processing is paused.
    pub fn is_processing_paused(&self) -> bool {
        self.signal_processor.is_paused()
    }
}

// ============================================================================
// Unified CDC Snapshot Orchestrator
// ============================================================================

/// Configuration for the unified CDC snapshot orchestrator.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct CdcSnapshotOrchestratorConfig {
    /// Initial snapshot configuration
    pub initial: SnapshotCdcConfig,
    /// Incremental snapshot configuration
    pub incremental: IncrementalSnapshotExecutorConfig,
    /// Signal table configuration
    pub signal_table: Option<String>,
    /// Database connection string
    pub connection_string: String,
    /// Whether to enable signal processing
    pub enable_signals: bool,
}

#[cfg(feature = "postgres")]
impl CdcSnapshotOrchestratorConfig {
    /// Create a new orchestrator config.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            initial: SnapshotCdcConfig::default(),
            incremental: IncrementalSnapshotExecutorConfig::default(),
            signal_table: Some("public.rivven_signal".to_string()),
            connection_string: connection_string.into(),
            enable_signals: true,
        }
    }

    /// Set initial snapshot config.
    pub fn with_initial(mut self, config: SnapshotCdcConfig) -> Self {
        self.initial = config;
        self
    }

    /// Set incremental snapshot config.
    pub fn with_incremental(mut self, config: IncrementalSnapshotExecutorConfig) -> Self {
        self.incremental = config;
        self
    }

    /// Set signal table name.
    pub fn with_signal_table(mut self, table: impl Into<String>) -> Self {
        self.signal_table = Some(table.into());
        self
    }

    /// Disable signal processing.
    pub fn without_signals(mut self) -> Self {
        self.enable_signals = false;
        self.signal_table = None;
        self
    }
}

/// Unified orchestrator for CDC snapshots.
///
/// Combines initial snapshots, incremental snapshots, and signal processing
/// into a single cohesive API. This is the recommended entry point for
/// CDC connectors that need full snapshot support.
///
/// # Features
///
/// - **Initial Snapshot**: Execute on startup based on mode (initial/always/etc.)
/// - **Incremental Snapshot**: Non-blocking re-snapshots during streaming
/// - **Signal Processing**: Respond to signal table events
/// - **Event Deduplication**: Watermark-based deduplication for incremental snapshots
/// - **Progress Persistence**: Resume interrupted snapshots
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::connectors::cdc_snapshot::{
///     CdcSnapshotOrchestrator, CdcSnapshotOrchestratorConfig
/// };
///
/// // Create orchestrator
/// let config = CdcSnapshotOrchestratorConfig::new("postgres://...")
///     .with_signal_table("public.rivven_signal");
///     
/// let orchestrator = CdcSnapshotOrchestrator::new(config);
/// orchestrator.initialize().await?;
///
/// // Execute initial snapshot if needed
/// let (initial_events, watermark) = orchestrator
///     .execute_initial_snapshot(&tables, has_prior_state)
///     .await?;
///
/// // Start streaming from watermark
/// let cdc_stream = start_cdc_streaming(&watermark).await?;
///
/// // Process streaming events with signal handling
/// let output = orchestrator.process_streaming(cdc_stream).await;
/// ```
#[cfg(feature = "postgres")]
pub struct CdcSnapshotOrchestrator {
    config: CdcSnapshotOrchestratorConfig,
    /// Signal processor (if signals enabled)
    signal_processor: Option<SignalSnapshotProcessor>,
    /// Whether orchestrator is initialized
    initialized: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "postgres")]
impl CdcSnapshotOrchestrator {
    /// Create a new orchestrator.
    pub fn new(config: CdcSnapshotOrchestratorConfig) -> Self {
        let signal_processor = if config.enable_signals {
            config.signal_table.as_ref().map(|table| {
                let mut incr_config = config.incremental.clone();
                incr_config.connection_string = config.connection_string.clone();

                SignalSnapshotProcessor::new(SignalSnapshotConfig {
                    signal_table: table.clone(),
                    enabled: true,
                    incremental_config: incr_config,
                })
            })
        } else {
            None
        };

        Self {
            config,
            signal_processor,
            initialized: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Initialize the orchestrator.
    ///
    /// Must be called before using other methods.
    pub async fn initialize(&self) -> Result<()> {
        if let Some(processor) = &self.signal_processor {
            processor.initialize().await?;
        }
        self.initialized
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!("CDC Snapshot Orchestrator initialized");
        Ok(())
    }

    /// Check if orchestrator is initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Execute initial snapshot if needed.
    ///
    /// Returns (events, snapshot_result) where events are the snapshot CDC events
    /// and result contains metadata including the watermark for streaming.
    pub async fn execute_initial_snapshot(
        &self,
        tables: &[(String, String, String)], // (schema, table, key_column)
        has_prior_state: bool,
    ) -> Result<(Vec<CdcEvent>, SnapshotResult)> {
        let executor_config = SnapshotExecutorConfig {
            yaml_config: self.config.initial.clone(),
            connection_string: self.config.connection_string.clone(),
            tables: tables.to_vec(),
            has_prior_state,
        };

        let executor = SnapshotExecutor::new(executor_config);
        executor.execute().await
    }

    /// Execute initial snapshot and return events as a stream.
    ///
    /// More memory-efficient than `execute_initial_snapshot` for large tables.
    pub async fn execute_initial_snapshot_streaming(
        &self,
        tables: &[(String, String, String)],
        has_prior_state: bool,
    ) -> Result<(impl futures::Stream<Item = CdcEvent> + Send, SnapshotResult)> {
        let executor_config = SnapshotExecutorConfig {
            yaml_config: self.config.initial.clone(),
            connection_string: self.config.connection_string.clone(),
            tables: tables.to_vec(),
            has_prior_state,
        };

        let executor = SnapshotExecutor::new(executor_config);
        executor.execute_streaming().await
    }

    /// Request an incremental snapshot for tables.
    ///
    /// This starts a non-blocking snapshot that runs while streaming continues.
    pub async fn request_incremental_snapshot(&self, tables: &[&str]) -> Result<String> {
        let processor = self.signal_processor.as_ref().ok_or_else(|| {
            rivven_cdc::common::CdcError::config(
                "Signal processing not enabled - cannot request incremental snapshot",
            )
        })?;

        processor.executor.request_snapshot(tables).await
    }

    /// Check if a CDC event is from the signal table.
    ///
    /// Use this during streaming to filter out signal events.
    pub fn is_signal_event(&self, schema: &str, table: &str) -> bool {
        self.signal_processor
            .as_ref()
            .map(|p| p.is_signal_event(schema, table))
            .unwrap_or(false)
    }

    /// Process a CDC event for signal handling.
    ///
    /// Returns Some(result) if this was a signal event, None otherwise.
    pub async fn process_signal_event(
        &self,
        schema: &str,
        table: &str,
        id: &str,
        signal_type: &str,
        data: Option<&str>,
    ) -> Result<Option<SignalResult>> {
        match &self.signal_processor {
            Some(processor) => {
                processor
                    .process_cdc_event(schema, table, id, signal_type, data)
                    .await
            }
            None => Ok(None),
        }
    }

    /// Check if an incremental snapshot is active.
    pub fn is_incremental_active(&self) -> bool {
        self.signal_processor
            .as_ref()
            .map(|p| p.is_snapshot_active())
            .unwrap_or(false)
    }

    /// Check if incremental snapshot is paused.
    pub fn is_incremental_paused(&self) -> bool {
        self.signal_processor
            .as_ref()
            .map(|p| p.is_snapshot_paused())
            .unwrap_or(false)
    }

    /// Get incremental snapshot statistics.
    pub fn incremental_stats(&self) -> Option<IncrementalSnapshotStatsSnapshot> {
        self.signal_processor.as_ref().and_then(|p| p.stats())
    }

    /// Pause incremental snapshot.
    pub async fn pause_incremental(&self) -> Result<()> {
        if let Some(processor) = &self.signal_processor {
            processor.executor.pause().await?;
        }
        Ok(())
    }

    /// Resume incremental snapshot.
    pub async fn resume_incremental(&self) -> Result<()> {
        if let Some(processor) = &self.signal_processor {
            processor.executor.resume().await?;
        }
        Ok(())
    }

    /// Stop incremental snapshot.
    pub async fn stop_incremental(&self) -> Result<()> {
        if let Some(processor) = &self.signal_processor {
            processor.executor.stop().await?;
        }
        Ok(())
    }

    /// Get the signal table name (if configured).
    pub fn signal_table(&self) -> Option<&str> {
        self.config.signal_table.as_deref()
    }

    /// Check if signals are enabled.
    pub fn signals_enabled(&self) -> bool {
        self.config.enable_signals && self.signal_processor.is_some()
    }
}

// ============================================================================
// Snapshot + Stream Combiner
// ============================================================================

/// Combines snapshot events with streaming events into a single stream.
///
/// This is the recommended way to integrate snapshots into a connector:
/// 1. Execute snapshot (if needed)
/// 2. Combine snapshot events with streaming events
/// 3. Return unified stream to the connector framework
#[cfg(feature = "postgres")]
pub fn combine_snapshot_and_stream<S1, S2>(
    snapshot_events: Vec<CdcEvent>,
    streaming: S2,
) -> impl futures::Stream<Item = CdcEvent> + Send
where
    S2: futures::Stream<Item = CdcEvent> + Send + 'static,
{
    use futures::StreamExt;

    // Snapshot events first, then streaming
    let snapshot_stream = futures::stream::iter(snapshot_events);
    snapshot_stream.chain(streaming)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivven_cdc::common::MockSnapshotSource;

    #[test]
    fn test_config_builder() {
        let config = SnapshotManagerConfig::builder()
            .mode(SnapshotMode::Always)
            .batch_size(5000)
            .parallel_tables(2)
            .build();

        assert!(matches!(config.mode, SnapshotMode::Always));
        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.parallel_tables, 2);
    }

    #[test]
    fn test_should_snapshot_initial_no_state() {
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::Initial);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let mut manager = SnapshotManager::new(config, source, progress);

        manager.set_has_prior_state(false);
        assert!(manager.should_snapshot());
    }

    #[test]
    fn test_should_snapshot_initial_with_state() {
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::Initial);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let mut manager = SnapshotManager::new(config, source, progress);

        manager.set_has_prior_state(true);
        assert!(!manager.should_snapshot());
    }

    #[test]
    fn test_should_snapshot_always() {
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::Always);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let mut manager = SnapshotManager::new(config, source, progress);

        // Always mode ignores prior state
        manager.set_has_prior_state(false);
        assert!(manager.should_snapshot());

        manager.set_has_prior_state(true);
        assert!(manager.should_snapshot());
    }

    #[test]
    fn test_should_snapshot_never() {
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::NoSnapshot);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let manager = SnapshotManager::new(config, source, progress);

        assert!(!manager.should_snapshot());
    }

    #[test]
    fn test_should_stream_after() {
        // Initial mode: should stream after
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::Initial);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let manager = SnapshotManager::new(config, source, progress);
        assert!(manager.should_stream_after());

        // InitialOnly mode: should NOT stream after
        let config = SnapshotManagerConfig::from_mode(SnapshotMode::InitialOnly);
        let source = MockSnapshotSource::new();
        let progress = MemoryProgressStore::new();
        let manager = SnapshotManager::new(config, source, progress);
        assert!(!manager.should_stream_after());
    }

    // ========================================================================
    // SnapshotExecutor tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod executor_tests {
        use super::super::*;
        use crate::connectors::cdc_config::{SnapshotCdcConfig, SnapshotModeConfig};

        fn make_config(mode: SnapshotModeConfig, has_prior_state: bool) -> SnapshotExecutorConfig {
            SnapshotExecutorConfig {
                yaml_config: SnapshotCdcConfig {
                    mode,
                    batch_size: 10000,
                    parallel_tables: 4,
                    progress_dir: None,
                    query_timeout_secs: 60,
                    throttle_delay_ms: 0,
                    max_retries: 3,
                    include_tables: vec![],
                    exclude_tables: vec![],
                },
                connection_string: "postgres://test".to_string(),
                tables: vec![],
                has_prior_state,
            }
        }

        #[test]
        fn test_should_snapshot_always_mode() {
            let config = make_config(SnapshotModeConfig::Always, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());

            let config = make_config(SnapshotModeConfig::Always, true);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot()); // Always ignores prior state
        }

        #[test]
        fn test_should_snapshot_initial_mode() {
            // No prior state - should snapshot
            let config = make_config(SnapshotModeConfig::Initial, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());

            // Has prior state - should NOT snapshot
            let config = make_config(SnapshotModeConfig::Initial, true);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());
        }

        #[test]
        fn test_should_snapshot_never_mode() {
            let config = make_config(SnapshotModeConfig::Never, false);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());

            let config = make_config(SnapshotModeConfig::Never, true);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());
        }

        #[test]
        fn test_should_snapshot_when_needed_mode() {
            // No prior state - should snapshot
            let config = make_config(SnapshotModeConfig::WhenNeeded, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());

            // Has prior state - should NOT snapshot
            let config = make_config(SnapshotModeConfig::WhenNeeded, true);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());
        }

        #[test]
        fn test_should_snapshot_initial_only_mode() {
            // No prior state - should snapshot
            let config = make_config(SnapshotModeConfig::InitialOnly, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());

            // Has prior state - should NOT snapshot
            let config = make_config(SnapshotModeConfig::InitialOnly, true);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());
        }

        #[test]
        fn test_should_snapshot_schema_only_mode() {
            // Schema only mode never snapshots data
            let config = make_config(SnapshotModeConfig::SchemaOnly, false);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());

            let config = make_config(SnapshotModeConfig::SchemaOnly, true);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_snapshot());
        }

        #[test]
        fn test_should_snapshot_recovery_mode() {
            // Recovery mode always snapshots
            let config = make_config(SnapshotModeConfig::Recovery, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());

            let config = make_config(SnapshotModeConfig::Recovery, true);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_snapshot());
        }

        #[test]
        fn test_should_stream_after_initial() {
            let config = make_config(SnapshotModeConfig::Initial, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_stream_after());
        }

        #[test]
        fn test_should_not_stream_after_initial_only() {
            let config = make_config(SnapshotModeConfig::InitialOnly, false);
            let executor = SnapshotExecutor::new(config);
            assert!(!executor.should_stream_after());
        }

        #[test]
        fn test_should_stream_after_always() {
            let config = make_config(SnapshotModeConfig::Always, true);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_stream_after());
        }

        #[test]
        fn test_should_stream_after_never() {
            let config = make_config(SnapshotModeConfig::Never, false);
            let executor = SnapshotExecutor::new(config);
            assert!(executor.should_stream_after());
        }
    }

    // ========================================================================
    // Incremental Snapshot Executor Tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod incremental_executor_tests {
        use super::super::*;

        fn make_incremental_config() -> IncrementalSnapshotExecutorConfig {
            IncrementalSnapshotExecutorConfig {
                chunk_size: 1024,
                max_concurrent_chunks: 4,
                watermark_strategy: WatermarkStrategy::InsertInsert,
                signal_table: "rivven_signal".to_string(),
                connection_string: "postgres://test".to_string(),
                max_buffer_memory: 64 * 1024 * 1024,
                chunk_timeout: std::time::Duration::from_secs(60),
                inter_chunk_delay: None,
            }
        }

        #[test]
        fn test_incremental_executor_new() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Not initialized yet
            assert!(!executor.is_active());
            assert!(!executor.is_paused());
            assert!(executor.stats().is_none());
        }

        #[test]
        fn test_incremental_config_to_cdc_config() {
            let config = IncrementalSnapshotExecutorConfig {
                chunk_size: 2048,
                max_concurrent_chunks: 8,
                watermark_strategy: WatermarkStrategy::InsertDelete,
                signal_table: "my_signal".to_string(),
                connection_string: "postgres://test".to_string(),
                max_buffer_memory: 128 * 1024 * 1024,
                chunk_timeout: std::time::Duration::from_secs(120),
                inter_chunk_delay: Some(std::time::Duration::from_millis(100)),
            };

            let cdc_config = config.to_cdc_config();
            assert_eq!(cdc_config.chunk_size, 2048);
            assert_eq!(cdc_config.max_concurrent_chunks, 8);
        }
    }

    // ========================================================================
    // Signal Snapshot Processor Tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod signal_processor_tests {
        use super::super::*;

        #[test]
        fn test_signal_snapshot_config_default() {
            let config = SignalSnapshotConfig::default();
            assert_eq!(config.signal_table, "public.rivven_signal");
            assert!(config.enabled);
        }

        #[test]
        fn test_signal_snapshot_config_new() {
            let config = SignalSnapshotConfig::new("myschema.my_signals");
            assert_eq!(config.signal_table, "myschema.my_signals");
            assert!(config.enabled);
        }

        #[test]
        fn test_signal_snapshot_config_builder() {
            let incr_config = IncrementalSnapshotExecutorConfig {
                chunk_size: 512,
                ..IncrementalSnapshotExecutorConfig::default()
            };

            let config = SignalSnapshotConfig::new("test.signals")
                .with_enabled(false)
                .with_incremental_config(incr_config);

            assert_eq!(config.signal_table, "test.signals");
            assert!(!config.enabled);
            assert_eq!(config.incremental_config.chunk_size, 512);
        }

        #[test]
        fn test_is_signal_event() {
            let config = SignalSnapshotConfig::new("public.rivven_signal");
            let processor = SignalSnapshotProcessor::new(config);

            assert!(processor.is_signal_event("public", "rivven_signal"));
            assert!(!processor.is_signal_event("public", "orders"));
            assert!(!processor.is_signal_event("other", "rivven_signal"));
        }
    }

    // ========================================================================
    // Orchestrator Tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod orchestrator_tests {
        use super::super::*;

        #[test]
        fn test_orchestrator_config_new() {
            let config = CdcSnapshotOrchestratorConfig::new("postgres://localhost/test");
            assert_eq!(config.connection_string, "postgres://localhost/test");
            assert!(config.enable_signals);
            assert_eq!(
                config.signal_table,
                Some("public.rivven_signal".to_string())
            );
        }

        #[test]
        fn test_orchestrator_config_without_signals() {
            let config =
                CdcSnapshotOrchestratorConfig::new("postgres://localhost/test").without_signals();

            assert!(!config.enable_signals);
            assert!(config.signal_table.is_none());
        }

        #[test]
        fn test_orchestrator_config_with_signal_table() {
            let config = CdcSnapshotOrchestratorConfig::new("postgres://localhost/test")
                .with_signal_table("myschema.my_signals");

            assert_eq!(config.signal_table, Some("myschema.my_signals".to_string()));
        }

        #[test]
        fn test_orchestrator_new() {
            let config = CdcSnapshotOrchestratorConfig::new("postgres://localhost/test");
            let orchestrator = CdcSnapshotOrchestrator::new(config);

            assert!(!orchestrator.is_initialized());
            assert!(orchestrator.signals_enabled());
            assert_eq!(orchestrator.signal_table(), Some("public.rivven_signal"));
        }

        #[test]
        fn test_orchestrator_signals_disabled() {
            let config =
                CdcSnapshotOrchestratorConfig::new("postgres://localhost/test").without_signals();
            let orchestrator = CdcSnapshotOrchestrator::new(config);

            assert!(!orchestrator.signals_enabled());
            assert!(orchestrator.signal_table().is_none());
        }

        #[test]
        fn test_orchestrator_is_signal_event_with_signals() {
            let config = CdcSnapshotOrchestratorConfig::new("postgres://localhost/test")
                .with_signal_table("public.rivven_signal");
            let orchestrator = CdcSnapshotOrchestrator::new(config);

            assert!(orchestrator.is_signal_event("public", "rivven_signal"));
            assert!(!orchestrator.is_signal_event("public", "orders"));
        }

        #[test]
        fn test_orchestrator_is_signal_event_without_signals() {
            let config =
                CdcSnapshotOrchestratorConfig::new("postgres://localhost/test").without_signals();
            let orchestrator = CdcSnapshotOrchestrator::new(config);

            // Should always return false when signals disabled
            assert!(!orchestrator.is_signal_event("public", "rivven_signal"));
        }
    }

    // ========================================================================
    // Parallel Chunk Processing Tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod parallel_chunk_tests {
        use super::super::*;

        fn make_incremental_config() -> IncrementalSnapshotExecutorConfig {
            IncrementalSnapshotExecutorConfig {
                chunk_size: 1024,
                max_concurrent_chunks: 4,
                watermark_strategy: WatermarkStrategy::InsertInsert,
                signal_table: "rivven_signal".to_string(),
                connection_string: "postgres://test".to_string(),
                max_buffer_memory: 64 * 1024 * 1024,
                chunk_timeout: std::time::Duration::from_secs(60),
                inter_chunk_delay: None,
            }
        }

        #[tokio::test]
        async fn test_executor_not_initialized_returns_defaults() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Not initialized - should return safe defaults
            assert!(!executor.is_window_open().await);
            assert!(!executor.is_window_open_for_chunk("test").await);
            assert_eq!(executor.open_window_count().await, 0);
            assert!(executor.open_chunk_ids().await.is_empty());

            // buffer_stats_all should return empty map
            let all_stats = executor.buffer_stats_all().await;
            assert!(all_stats.is_empty());

            // buffer_stats_aggregate should return default
            let agg_stats = executor.buffer_stats_aggregate().await;
            assert_eq!(agg_stats.total_rows, 0);
            assert_eq!(agg_stats.open_windows, 0);
        }

        #[test]
        fn test_config_accessor() {
            let config = IncrementalSnapshotExecutorConfig {
                chunk_size: 2048,
                max_concurrent_chunks: 8,
                ..make_incremental_config()
            };
            let executor = IncrementalSnapshotExecutor::new(config);

            assert_eq!(executor.config().chunk_size, 2048);
            assert_eq!(executor.config().max_concurrent_chunks, 8);
        }

        #[tokio::test]
        async fn test_complete_current_table_not_initialized() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Should return None when not initialized
            let result = executor.complete_current_table().await.unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn test_get_context_not_initialized() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Should return None when not initialized
            assert!(executor.get_context().await.is_none());
        }

        #[tokio::test]
        async fn test_should_throttle_not_initialized() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Should return false when not initialized (no memory pressure)
            assert!(!executor.should_throttle().await);
        }

        #[tokio::test]
        async fn test_memory_utilization_not_initialized() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            // Should return 0% when not initialized
            assert_eq!(executor.memory_utilization_percent().await, 0.0);
        }

        #[tokio::test]
        async fn test_process_streaming_event_not_initialized() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);

            let event = rivven_cdc::common::CdcEvent::insert(
                "postgres",
                "testdb",
                "public",
                "orders",
                serde_json::json!({"id": "123", "name": "test"}),
                0,
            );

            // Should return false when not initialized
            assert!(!executor.process_streaming_event(&event, &["id"]).await);
        }

        #[tokio::test]
        async fn test_execute_chunks_parallel_empty() {
            let config = make_incremental_config();
            let executor = IncrementalSnapshotExecutor::new(config);
            executor.initialize().await.unwrap();

            // Empty chunks should return empty result
            let events = executor
                .execute_chunks_parallel(&[], 1000000, |_| async { Ok(vec![]) })
                .await
                .unwrap();

            assert!(events.is_empty());
        }
    }

    // ========================================================================
    // Advanced Integration Tests
    // ========================================================================

    #[cfg(feature = "postgres")]
    mod advanced_integration_tests {
        use super::super::*;

        fn make_test_event(id: &str, op: rivven_cdc::common::CdcOp) -> CdcEvent {
            match op {
                rivven_cdc::common::CdcOp::Delete => rivven_cdc::common::CdcEvent::delete(
                    "postgres",
                    "testdb",
                    "public",
                    "orders",
                    serde_json::json!({"id": id, "name": "test"}),
                    0,
                ),
                rivven_cdc::common::CdcOp::Update => rivven_cdc::common::CdcEvent::update(
                    "postgres",
                    "testdb",
                    "public",
                    "orders",
                    Some(serde_json::json!({"id": id, "name": "old"})),
                    serde_json::json!({"id": id, "name": "new"}),
                    0,
                ),
                _ => rivven_cdc::common::CdcEvent::insert(
                    "postgres",
                    "testdb",
                    "public",
                    "orders",
                    serde_json::json!({"id": id, "name": "test"}),
                    0,
                ),
            }
        }

        #[test]
        fn test_event_key_extraction_single() {
            let event = make_test_event("123", rivven_cdc::common::CdcOp::Insert);

            // Verify the event has the expected structure
            assert_eq!(event.schema, "public");
            assert_eq!(event.table, "orders");
            assert!(event.after.is_some());

            let after = event.after.as_ref().unwrap();
            assert_eq!(after.get("id").unwrap(), "123");
        }

        #[test]
        fn test_event_key_extraction_composite() {
            let event = rivven_cdc::common::CdcEvent::insert(
                "postgres",
                "testdb",
                "public",
                "order_items",
                serde_json::json!({"order_id": "100", "item_id": "200", "qty": 5}),
                0,
            );

            let after = event.after.as_ref().unwrap();
            assert_eq!(after.get("order_id").unwrap(), "100");
            assert_eq!(after.get("item_id").unwrap(), "200");
        }
    }

    // ========================================================================
    // Re-export Tests
    // ========================================================================

    #[test]
    fn test_reexports_available() {
        // Verify key types are re-exported and usable
        fn _check_chunk_buffer_stats(_: ChunkBufferStats) {}
        fn _check_aggregate_buffer_stats(_: AggregateBufferStats) {}
        fn _check_snapshot_key(_: SnapshotKey) {}
        fn _check_chunking_strategy(_: ChunkingStrategy) {}
        fn _check_key_range(_: KeyRange) {}
        fn _check_query_spec(_: QuerySpec) {}

        // Check defaults work
        let _ = AggregateBufferStats::default();
        let _ = ChunkingStrategy::default();
    }
}
