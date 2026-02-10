//! Shared CDC configuration types
//!
//! Common configuration structures used by all CDC connectors (PostgreSQL, MySQL, etc.)
//! These types map to rivven-cdc features and are database-agnostic.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

// ============================================================================
// Initial Snapshot Configuration
// ============================================================================

/// Configuration for initial snapshot (full table load before streaming).
///
/// # Snapshot Modes
///
/// | Mode | Behavior |
/// |------|----------|
/// | `initial` | Snapshot on first run, then stream |
/// | `always` | Snapshot on every startup |
/// | `never` | Stream only, no snapshot |
/// | `when_needed` | Snapshot if no stored offsets |
/// | `initial_only` | Snapshot only, no streaming |
/// | `schema_only` | Capture schema, no data |
/// | `recovery` | Force re-snapshot from source |
///
/// # Example
///
/// ```yaml
/// snapshot:
///   mode: initial
///   batch_size: 10000
///   parallel_tables: 4
///   progress_dir: /var/lib/rivven/snapshot
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SnapshotCdcConfig {
    /// Snapshot mode (initial, always, never, when_needed, initial_only, schema_only, recovery)
    #[serde(default)]
    pub mode: SnapshotModeConfig,

    /// Batch size for SELECT queries (rows per batch)
    #[serde(default = "default_snapshot_batch_size")]
    pub batch_size: usize,

    /// Number of tables to snapshot in parallel
    #[serde(default = "default_parallel_tables")]
    pub parallel_tables: usize,

    /// Directory for progress persistence (None = in-memory only)
    #[serde(default)]
    pub progress_dir: Option<PathBuf>,

    /// Query timeout in seconds
    #[serde(default = "default_snapshot_query_timeout")]
    pub query_timeout_secs: u64,

    /// Delay between batches for backpressure (ms)
    #[serde(default)]
    pub throttle_delay_ms: u64,

    /// Maximum retries per batch on failure
    #[serde(default = "default_snapshot_max_retries")]
    pub max_retries: u32,

    /// Tables to include in snapshot (empty = all tables)
    /// Format: ["schema.table", "public.users"]
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude from snapshot
    #[serde(default)]
    pub exclude_tables: Vec<String>,
}

impl Default for SnapshotCdcConfig {
    fn default() -> Self {
        Self {
            mode: SnapshotModeConfig::default(),
            batch_size: default_snapshot_batch_size(),
            parallel_tables: default_parallel_tables(),
            progress_dir: None,
            query_timeout_secs: default_snapshot_query_timeout(),
            throttle_delay_ms: 0,
            max_retries: default_snapshot_max_retries(),
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
        }
    }
}

impl SnapshotCdcConfig {
    /// Convert query timeout to Duration
    pub fn query_timeout(&self) -> Duration {
        Duration::from_secs(self.query_timeout_secs)
    }

    /// Convert throttle delay to Duration (if set)
    pub fn throttle_delay(&self) -> Option<Duration> {
        if self.throttle_delay_ms > 0 {
            Some(Duration::from_millis(self.throttle_delay_ms))
        } else {
            None
        }
    }

    /// Check if a table should be snapshotted based on include/exclude lists
    pub fn should_snapshot_table(&self, schema: &str, table: &str) -> bool {
        let qualified = format!("{}.{}", schema, table);

        // Check exclude list first
        if self
            .exclude_tables
            .iter()
            .any(|t| t == &qualified || t == table)
        {
            return false;
        }

        // If include list is empty, include all
        if self.include_tables.is_empty() {
            return true;
        }

        // Check include list
        self.include_tables
            .iter()
            .any(|t| t == &qualified || t == table)
    }
}

fn default_snapshot_batch_size() -> usize {
    10_000
}

fn default_parallel_tables() -> usize {
    4
}

fn default_snapshot_query_timeout() -> u64 {
    300 // 5 minutes
}

fn default_snapshot_max_retries() -> u32 {
    3
}

/// Snapshot mode configuration (maps to rivven_cdc::common::SnapshotMode)
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotModeConfig {
    /// Snapshot on first run, then stream (default)
    #[default]
    Initial,
    /// Snapshot on every startup
    Always,
    /// Stream only, no snapshot
    Never,
    /// Snapshot if no stored offsets
    WhenNeeded,
    /// Snapshot only, no streaming afterward
    InitialOnly,
    /// Capture schema only, no data (preferred name)
    #[serde(alias = "schema_only")]
    NoData,
    /// Force re-snapshot from source (recovery mode)
    Recovery,
}

// ============================================================================
// Signal Table Configuration
// ============================================================================

/// Configuration for CDC signal table (ad-hoc snapshots, pause/resume)
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct SignalTableConfig {
    /// Enable signal processing
    #[serde(default)]
    pub enabled: bool,

    /// Fully-qualified signal table name (schema.table)
    /// Default: public.rivven_signal
    #[serde(default)]
    pub data_collection: Option<String>,

    /// Topic for signal messages (alternative to table)
    #[serde(default)]
    pub topic: Option<String>,

    /// Enabled signal channels
    /// Options: source, topic, file, api
    #[serde(default)]
    pub enabled_channels: Vec<String>,

    /// Poll interval for file channel (ms)
    #[serde(default = "default_signal_poll_interval")]
    pub poll_interval_ms: u64,
}

fn default_signal_poll_interval() -> u64 {
    1000
}

// ============================================================================
// Incremental Snapshot Configuration
// ============================================================================

/// Configuration for incremental (non-blocking) snapshots
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct IncrementalSnapshotCdcConfig {
    /// Enable incremental snapshots
    #[serde(default)]
    pub enabled: bool,

    /// Chunk size (rows per chunk)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Watermark strategy: insert, update_and_insert
    #[serde(default)]
    pub watermark_strategy: WatermarkStrategyConfig,

    /// Signal table for watermark signals
    #[serde(default)]
    pub watermark_signal_table: Option<String>,

    /// Maximum concurrent chunks
    #[serde(default = "default_max_concurrent_chunks")]
    pub max_concurrent_chunks: usize,

    /// Delay between chunks (ms)
    #[serde(default)]
    pub chunk_delay_ms: u64,
}

impl Default for IncrementalSnapshotCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            chunk_size: default_chunk_size(),
            watermark_strategy: WatermarkStrategyConfig::default(),
            watermark_signal_table: None,
            max_concurrent_chunks: default_max_concurrent_chunks(),
            chunk_delay_ms: 0,
        }
    }
}

fn default_chunk_size() -> usize {
    1024
}

fn default_max_concurrent_chunks() -> usize {
    1
}

/// Watermark strategy for incremental snapshots
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkStrategyConfig {
    /// Insert watermark signals into signal table
    #[default]
    Insert,
    /// Update existing row and insert new
    UpdateAndInsert,
}

// ============================================================================
// Single Message Transform (SMT) Configuration
// ============================================================================

/// Predicate configuration for conditional SMT application.
///
/// Predicates allow SMTs to be applied only to events matching
/// certain conditions (e.g., specific tables, schemas, or operations).
///
/// Multiple predicates are combined with AND logic.
///
/// ## Example
///
/// ```yaml
/// transforms:
///   - type: externalize_blob
///     name: externalize_documents
///     predicate:
///       table: "documents"  # Only apply to 'documents' table
///     config:
///       storage_type: s3
///       bucket: my-blobs
///
///   - type: mask_field
///     predicate:
///       tables: ["users", "customers"]  # Apply to multiple tables
///       operations: ["insert", "update"]  # Only on insert/update
///     config:
///       fields: ["ssn"]
///
///   - type: filter
///     predicate:
///       field_exists: "priority"  # Only if field exists
///       field_value:
///         field: "status"
///         value: "active"
///     config:
///       condition: "priority > 5"
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct SmtPredicateConfig {
    /// Apply only to events from this table (regex pattern).
    /// Use `tables` for multiple tables.
    #[serde(default)]
    pub table: Option<String>,

    /// Apply only to events from these tables (regex patterns).
    /// Multiple patterns are OR'd together.
    #[serde(default)]
    pub tables: Vec<String>,

    /// Apply only to events from this schema (regex pattern).
    #[serde(default)]
    pub schema: Option<String>,

    /// Apply only to events from these schemas (regex patterns).
    #[serde(default)]
    pub schemas: Vec<String>,

    /// Apply only to events from this database (regex pattern).
    #[serde(default)]
    pub database: Option<String>,

    /// Apply only to these operations: insert, update, delete, snapshot, truncate, tombstone, schema.
    /// Aliases: c/create=insert, u=update, d=delete, r/read=snapshot, ddl=schema
    #[serde(default)]
    pub operations: Vec<String>,

    /// Apply only if this field exists in the event data.
    #[serde(default)]
    pub field_exists: Option<String>,

    /// Apply only if field has specific value.
    /// Format: { "field": "field_name", "value": <json_value> }
    #[serde(default)]
    pub field_value: Option<FieldValuePredicate>,

    /// Negate the predicate (apply when conditions DON'T match).
    #[serde(default)]
    pub negate: bool,
}

/// Field value predicate for matching specific field values.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct FieldValuePredicate {
    /// Field name to check
    pub field: String,
    /// Expected value (JSON value)
    pub value: serde_json::Value,
}

impl SmtPredicateConfig {
    /// Check if this predicate is empty (no conditions).
    pub fn is_empty(&self) -> bool {
        self.table.is_none()
            && self.tables.is_empty()
            && self.schema.is_none()
            && self.schemas.is_empty()
            && self.database.is_none()
            && self.operations.is_empty()
            && self.field_exists.is_none()
            && self.field_value.is_none()
    }

    /// Validate the predicate configuration.
    pub fn validate(&self) -> Result<(), String> {
        // Warn if both table and tables are specified
        if self.table.is_some() && !self.tables.is_empty() {
            return Err("Cannot specify both 'table' and 'tables' - use one or the other".into());
        }
        // Warn if both schema and schemas are specified
        if self.schema.is_some() && !self.schemas.is_empty() {
            return Err("Cannot specify both 'schema' and 'schemas' - use one or the other".into());
        }
        Ok(())
    }
}

/// Configuration for a single message transform
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SmtTransformConfig {
    /// Transform type
    #[serde(rename = "type")]
    pub transform_type: SmtTransformType,

    /// Transform name (optional, for logging)
    #[serde(default)]
    pub name: Option<String>,

    /// Predicate for conditional application.
    /// If specified, the transform only applies to events matching the predicate.
    #[serde(default)]
    pub predicate: Option<SmtPredicateConfig>,

    /// Transform-specific configuration
    #[serde(default)]
    pub config: HashMap<String, serde_json::Value>,
}

/// Available SMT transform types
///
/// All 17 transforms from rivven-cdc are supported:
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SmtTransformType {
    // ===== Core Transforms =====
    /// Extract only the 'after' state (no before/op wrapper)
    ExtractNewRecordState,
    /// Copy field(s) to message key
    ValueToKey,
    /// Convert timestamp formats (ISO8601, epoch, date)
    TimestampConverter,
    /// Convert between timezones (IANA timezone names)
    TimezoneConverter,
    /// Mask sensitive fields (PII, credit cards)
    MaskField,
    /// Filter events by condition
    Filter,
    /// Flatten nested JSON structures
    Flatten,
    /// Add static or computed fields
    InsertField,
    /// Rename, include, or exclude fields
    RenameField,
    /// Replace field value (alias for RenameField)
    ReplaceField,
    /// Cast field types (string, int, float, bool, json)
    Cast,

    // ===== Routing Transforms =====
    /// Route to different topics based on regex patterns
    RegexRouter,
    /// Route based on field content values
    ContentRouter,

    // ===== Advanced Transforms =====
    /// Add envelope fields to record value
    HeaderToValue,
    /// Extract nested field to top level
    Unwrap,
    /// Conditionally nullify fields
    SetNull,
    /// Compute new fields (concat, hash, uppercase, etc.)
    ComputeField,
    /// Apply transforms conditionally based on predicates.
    ///
    /// **Note**: This is an internal type — not directly usable in YAML.
    /// Instead, add a `predicate:` block to any transform. The system
    /// wraps it with `ConditionalSmt` automatically.
    ConditionalSmt,

    // ===== Storage Transforms =====
    /// Externalize large blobs to object storage (S3/GCS/Azure/local)
    ///
    /// Requires the `cloud-storage` feature. Config options:
    /// - `storage_type`: "local" | "s3" | "gcs" | "azure"
    /// - `path` / `bucket`: Storage location
    /// - `size_threshold`: Minimum size in bytes to externalize (default: 1KB)
    /// - `prefix`: Custom prefix for object paths
    /// - `fields`: Optional list of fields to process (default: all string fields)
    ExternalizeBlob,

    // ===== Not Yet Implemented =====
    /// Add/set header fields (planned)
    InsertHeader,
    /// Custom transform via WASM plugin (planned)
    Custom,
}

// ============================================================================
// Read-Only Replica Configuration
// ============================================================================

/// Configuration for connecting to read-only replicas
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct ReadOnlyReplicaConfig {
    /// Enable read-only mode (for connecting to replicas)
    #[serde(default)]
    pub enabled: bool,

    /// Replication lag threshold before warnings (ms)
    #[serde(default = "default_lag_threshold")]
    pub lag_threshold_ms: u64,

    /// Enable deduplication (required for replicas)
    #[serde(default = "default_true")]
    pub deduplicate: bool,

    /// Watermark source: primary, replica
    #[serde(default)]
    pub watermark_source: WatermarkSourceConfig,
}

fn default_lag_threshold() -> u64 {
    5000
}

fn default_true() -> bool {
    true
}

/// Source of watermark position for read-only mode
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatermarkSourceConfig {
    /// Get watermark from primary (requires connection)
    #[default]
    Primary,
    /// Use local replica position
    Replica,
}

// ============================================================================
// Transaction Topic Configuration
// ============================================================================

/// Configuration for transaction metadata topic
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct TransactionTopicCdcConfig {
    /// Enable transaction topic
    #[serde(default)]
    pub enabled: bool,

    /// Transaction topic name
    /// Default: {database}.transaction
    #[serde(default)]
    pub topic_name: Option<String>,

    /// Include transaction data collections in events
    #[serde(default = "default_true")]
    pub include_data_collections: bool,

    /// Minimum events in transaction to emit (filter small txns)
    #[serde(default)]
    pub min_events_threshold: usize,
}

impl Default for TransactionTopicCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            topic_name: None,
            include_data_collections: true,
            min_events_threshold: 0,
        }
    }
}

// ============================================================================
// Schema Change Topic Configuration
// ============================================================================

/// Configuration for schema change (DDL) topic
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SchemaChangeTopicConfig {
    /// Enable schema change topic
    #[serde(default)]
    pub enabled: bool,

    /// Schema change topic name
    /// Default: {database}.schema_changes
    #[serde(default)]
    pub topic_name: Option<String>,

    /// Include table columns in change events
    #[serde(default = "default_true")]
    pub include_columns: bool,

    /// Schemas to monitor for changes (empty = all)
    #[serde(default)]
    pub schemas: Vec<String>,
}

impl Default for SchemaChangeTopicConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            topic_name: None,
            include_columns: true,
            schemas: Vec::new(),
        }
    }
}

// ============================================================================
// Tombstone Configuration
// ============================================================================

/// Configuration for tombstone events (log compaction)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct TombstoneCdcConfig {
    /// Enable tombstone events for deletes
    #[serde(default)]
    pub enabled: bool,

    /// Emit tombstone after delete event
    #[serde(default = "default_true")]
    pub after_delete: bool,

    /// Tombstone behavior: emit_null, emit_with_key
    #[serde(default)]
    pub behavior: TombstoneBehaviorConfig,
}

impl Default for TombstoneCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            after_delete: true,
            behavior: TombstoneBehaviorConfig::default(),
        }
    }
}

/// Tombstone behavior mode
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneBehaviorConfig {
    /// Emit null payload with key
    #[default]
    EmitNull,
    /// Emit only key fields
    EmitWithKey,
}

// ============================================================================
// Field Encryption Configuration
// ============================================================================

/// Configuration for field-level encryption
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct FieldEncryptionConfig {
    /// Enable field encryption
    #[serde(default)]
    pub enabled: bool,

    /// Encryption key (hex-encoded, 256-bit)
    /// Use environment variable reference: ${CDC_ENCRYPTION_KEY}
    #[serde(default)]
    pub key: Option<String>,

    /// Fields to encrypt (table.column format)
    #[serde(default)]
    pub fields: Vec<String>,

    /// Encryption algorithm (default: aes-256-gcm)
    #[serde(default = "default_encryption_algorithm")]
    pub algorithm: String,
}

fn default_encryption_algorithm() -> String {
    "aes-256-gcm".to_string()
}

// ============================================================================
// Deduplication Configuration
// ============================================================================

/// Configuration for event deduplication
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct DeduplicationCdcConfig {
    /// Enable deduplication
    #[serde(default)]
    pub enabled: bool,

    /// Bloom filter expected insertions
    #[serde(default = "default_bloom_expected")]
    pub bloom_expected_insertions: usize,

    /// Bloom filter false positive rate
    #[serde(default = "default_bloom_fpp")]
    pub bloom_fpp: f64,

    /// LRU cache size
    #[serde(default = "default_lru_size")]
    pub lru_size: usize,

    /// Deduplication window (seconds)
    #[serde(default = "default_dedup_window")]
    pub window_secs: u64,
}

impl Default for DeduplicationCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bloom_expected_insertions: default_bloom_expected(),
            bloom_fpp: default_bloom_fpp(),
            lru_size: default_lru_size(),
            window_secs: default_dedup_window(),
        }
    }
}

fn default_bloom_expected() -> usize {
    100_000
}

fn default_bloom_fpp() -> f64 {
    0.01
}

fn default_lru_size() -> usize {
    10_000
}

fn default_dedup_window() -> u64 {
    3600
}

// ============================================================================
// Heartbeat Configuration
// ============================================================================

/// Configuration for CDC heartbeat monitoring
///
/// Heartbeats keep replication slots healthy by:
/// - Advancing WAL position during low-traffic periods
/// - Detecting lag and connection health issues
/// - Preventing WAL accumulation in PostgreSQL
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HeartbeatCdcConfig {
    /// Enable heartbeat monitoring
    #[serde(default)]
    pub enabled: bool,

    /// Heartbeat interval in seconds (default: 10)
    #[serde(default = "default_heartbeat_interval")]
    pub interval_secs: u32,

    /// Maximum allowed lag before marking as unhealthy (default: 300 = 5 minutes)
    #[serde(default = "default_max_lag")]
    pub max_lag_secs: u32,

    /// Emit heartbeat events to a topic (for downstream monitoring)
    #[serde(default)]
    pub emit_events: bool,

    /// Topic name for heartbeat events (if emit_events is true)
    #[serde(default)]
    pub topic: Option<String>,

    /// Optional SQL query to execute on each heartbeat (keeps connections alive)
    #[serde(default)]
    pub action_query: Option<String>,
}

impl Default for HeartbeatCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_heartbeat_interval(),
            max_lag_secs: default_max_lag(),
            emit_events: false,
            topic: None,
            action_query: None,
        }
    }
}

fn default_heartbeat_interval() -> u32 {
    10
}

fn default_max_lag() -> u32 {
    300 // 5 minutes
}

// ============================================================================
// Column Filter Configuration
// ============================================================================

/// Per-table column filtering configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct ColumnFilterConfig {
    /// Columns to include (empty = all)
    #[serde(default)]
    pub include: Vec<String>,

    /// Columns to exclude
    #[serde(default)]
    pub exclude: Vec<String>,
}

// ============================================================================
// Event Router Configuration
// ============================================================================

/// Configuration for content-based event routing with dead letter queue support.
///
/// Routes CDC events to different destinations based on rules. Unroutable events
/// can be sent to a dead letter queue or dropped.
///
/// # Example
///
/// ```yaml
/// router:
///   enabled: true
///   default_destination: default-events
///   dead_letter_queue: cdc.dlq
///   rules:
///     - name: high_priority
///       priority: 100
///       condition:
///         type: field_equals
///         field: priority
///         value: "high"
///       destinations: ["priority-events"]
///     - name: orders
///       condition:
///         type: table
///         table: orders
///       destinations: ["order-events", "analytics"]
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct EventRouterConfig {
    /// Enable event routing
    #[serde(default)]
    pub enabled: bool,

    /// Default destination when no rules match
    #[serde(default)]
    pub default_destination: Option<String>,

    /// Dead letter queue for unroutable events
    #[serde(default)]
    pub dead_letter_queue: Option<String>,

    /// Drop unroutable events instead of sending to DLQ
    #[serde(default)]
    pub drop_unroutable: bool,

    /// Routing rules (evaluated in priority order)
    #[serde(default)]
    pub rules: Vec<RouteRuleConfig>,
}

/// Configuration for a single routing rule
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct RouteRuleConfig {
    /// Rule name (for logging/debugging)
    pub name: String,

    /// Priority (higher = evaluated first, default: 0)
    #[serde(default)]
    pub priority: i32,

    /// Routing condition
    pub condition: RouteConditionConfig,

    /// Destination(s) for matching events
    pub destinations: Vec<String>,

    /// Continue evaluating rules after match (fan-out)
    #[serde(default)]
    pub continue_matching: bool,
}

/// Routing condition configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RouteConditionConfig {
    /// Always matches
    Always,
    /// Match specific table
    Table { table: String },
    /// Match table pattern (glob)
    TablePattern { pattern: String },
    /// Match schema
    Schema { schema: String },
    /// Match operation type (insert, update, delete)
    Operation { op: String },
    /// Match multiple operations
    Operations { ops: Vec<String> },
    /// Field equals value
    FieldEquals {
        field: String,
        value: serde_json::Value,
    },
    /// Field exists
    FieldExists { field: String },
    /// All conditions must match
    All {
        conditions: Vec<RouteConditionConfig>,
    },
    /// Any condition must match
    Any {
        conditions: Vec<RouteConditionConfig>,
    },
    /// Negate condition
    Not {
        condition: Box<RouteConditionConfig>,
    },
}

// ============================================================================
// Partitioner Configuration
// ============================================================================

/// Configuration for event partitioning (Kafka-style).
///
/// Controls how events are distributed across partitions for ordering guarantees.
///
/// # Example
///
/// ```yaml
/// partitioner:
///   enabled: true
///   num_partitions: 16
///   strategy:
///     type: key_hash
///     columns: [customer_id]
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PartitionerConfig {
    /// Enable partitioning
    #[serde(default)]
    pub enabled: bool,

    /// Number of partitions
    #[serde(default = "default_num_partitions")]
    pub num_partitions: u32,

    /// Partitioning strategy
    #[serde(default)]
    pub strategy: PartitionStrategyConfig,
}

impl Default for PartitionerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            num_partitions: default_num_partitions(),
            strategy: PartitionStrategyConfig::default(),
        }
    }
}

fn default_num_partitions() -> u32 {
    16
}

/// Partitioning strategy configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PartitionStrategyConfig {
    /// Round-robin distribution (no ordering guarantees)
    RoundRobin,
    /// Hash on specific key columns (per-key ordering)
    KeyHash {
        /// Columns to use for hashing
        columns: Vec<String>,
    },
    /// Hash on table name only (per-table ordering)
    TableHash,
    /// Hash on schema.table (per-table ordering)
    FullTableHash,
    /// All events to a single partition (global ordering)
    Sticky {
        /// Fixed partition number
        partition: u32,
    },
}

impl Default for PartitionStrategyConfig {
    fn default() -> Self {
        Self::KeyHash {
            columns: vec!["id".to_string()],
        }
    }
}

// ============================================================================
// Pipeline Configuration
// ============================================================================

/// Configuration for CDC processing pipelines.
///
/// Pipelines provide composable, reusable event processing with transforms,
/// filters, and routing stages.
///
/// # Example
///
/// ```yaml
/// pipeline:
///   enabled: true
///   name: user-events
///   dead_letter_queue: cdc.dlq
///   stages:
///     - type: filter
///       predicate: table != 'audit_log'
///     - type: transform
///       transform: mask_fields
///       config:
///         fields: [password, ssn]
///     - type: route
///       destination_template: "cdc.${schema}.${table}"
/// ```
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct PipelineConfig {
    /// Enable pipeline processing
    #[serde(default)]
    pub enabled: bool,

    /// Pipeline name (for metrics/logging)
    #[serde(default)]
    pub name: Option<String>,

    /// Dead letter queue for failed events
    #[serde(default)]
    pub dead_letter_queue: Option<String>,

    /// Processing stages
    #[serde(default)]
    pub stages: Vec<PipelineStageConfig>,

    /// Maximum concurrent event processing
    #[serde(default = "default_pipeline_concurrency")]
    pub concurrency: usize,
}

fn default_pipeline_concurrency() -> usize {
    10
}

/// Pipeline stage configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PipelineStageConfig {
    /// Filter events
    Filter {
        /// Predicate expression (e.g., "table != 'audit'")
        predicate: String,
    },
    /// Transform events
    Transform {
        /// Transform name (mask_fields, rename_field, etc.)
        transform: String,
        /// Transform configuration
        #[serde(default)]
        config: HashMap<String, serde_json::Value>,
    },
    /// Route events to destination
    Route {
        /// Destination template with ${field} placeholders
        destination_template: String,
    },
}

// ============================================================================
// Compaction Configuration
// ============================================================================

/// Configuration for log compaction (Kafka-style).
///
/// Keeps only the latest value for each key, enabling efficient state reconstruction.
///
/// # Example
///
/// ```yaml
/// compaction:
///   enabled: true
///   key_columns: [id]
///   min_cleanable_ratio: 0.5
///   tombstone_retention_secs: 86400
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CompactionConfig {
    /// Enable log compaction
    #[serde(default)]
    pub enabled: bool,

    /// Key columns for compaction
    #[serde(default)]
    pub key_columns: Vec<String>,

    /// Minimum dirty ratio to trigger compaction (0.0-1.0)
    #[serde(default = "default_min_cleanable_ratio")]
    pub min_cleanable_ratio: f64,

    /// Maximum segment size before compaction
    #[serde(default = "default_segment_size")]
    pub segment_size: usize,

    /// Tombstone retention in seconds
    #[serde(default = "default_tombstone_retention")]
    pub tombstone_retention_secs: u64,

    /// Enable background compaction
    #[serde(default = "default_true")]
    pub background: bool,

    /// Compaction batch size
    #[serde(default = "default_compaction_batch_size")]
    pub batch_size: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            key_columns: vec!["id".to_string()],
            min_cleanable_ratio: default_min_cleanable_ratio(),
            segment_size: default_segment_size(),
            tombstone_retention_secs: default_tombstone_retention(),
            background: true,
            batch_size: default_compaction_batch_size(),
        }
    }
}

fn default_min_cleanable_ratio() -> f64 {
    0.5
}

fn default_segment_size() -> usize {
    1_000_000
}

fn default_tombstone_retention() -> u64 {
    86400 // 24 hours
}

fn default_compaction_batch_size() -> usize {
    10_000
}

// ============================================================================
// Parallel CDC Configuration
// ============================================================================

/// Configuration for multi-table parallel CDC processing.
///
/// Enables concurrent processing of multiple tables with work stealing
/// and backpressure control.
///
/// # Example
///
/// ```yaml
/// parallel:
///   enabled: true
///   concurrency: 4
///   per_table_buffer: 1000
///   work_stealing: true
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ParallelCdcConfig {
    /// Enable parallel processing
    #[serde(default)]
    pub enabled: bool,

    /// Maximum concurrent table workers
    #[serde(default = "default_parallel_concurrency")]
    pub concurrency: usize,

    /// Buffer size per table
    #[serde(default = "default_per_table_buffer")]
    pub per_table_buffer: usize,

    /// Output channel buffer size
    #[serde(default = "default_output_buffer")]
    pub output_buffer: usize,

    /// Enable work stealing between workers
    #[serde(default = "default_true")]
    pub work_stealing: bool,

    /// Maximum events per second per table (None = unlimited)
    #[serde(default)]
    pub per_table_rate_limit: Option<u64>,

    /// Shutdown timeout in seconds
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
}

impl Default for ParallelCdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            concurrency: default_parallel_concurrency(),
            per_table_buffer: default_per_table_buffer(),
            output_buffer: default_output_buffer(),
            work_stealing: true,
            per_table_rate_limit: None,
            shutdown_timeout_secs: default_shutdown_timeout(),
        }
    }
}

fn default_parallel_concurrency() -> usize {
    4
}

fn default_per_table_buffer() -> usize {
    1000
}

fn default_output_buffer() -> usize {
    10_000
}

fn default_shutdown_timeout() -> u64 {
    30
}

// ============================================================================
// Outbox Configuration
// ============================================================================

/// Configuration for the transactional outbox pattern.
///
/// Enables reliable event publishing by storing events in the same transaction
/// as business data, then forwarding them to the event stream.
///
/// # Example
///
/// ```yaml
/// outbox:
///   enabled: true
///   table_name: outbox
///   poll_interval_ms: 100
///   batch_size: 100
///   max_retries: 3
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct OutboxConfig {
    /// Enable outbox pattern
    #[serde(default)]
    pub enabled: bool,

    /// Outbox table name
    #[serde(default = "default_outbox_table")]
    pub table_name: String,

    /// Poll interval in milliseconds
    #[serde(default = "default_outbox_poll_interval")]
    pub poll_interval_ms: u64,

    /// Maximum events per batch
    #[serde(default = "default_outbox_batch_size")]
    pub batch_size: usize,

    /// Maximum delivery retries before DLQ
    #[serde(default = "default_outbox_max_retries")]
    pub max_retries: u32,

    /// Delivery timeout in seconds
    #[serde(default = "default_outbox_timeout")]
    pub delivery_timeout_secs: u64,

    /// Enable ordered delivery per aggregate
    #[serde(default = "default_true")]
    pub ordered_delivery: bool,

    /// Retention period for delivered events in seconds
    #[serde(default = "default_outbox_retention")]
    pub retention_secs: u64,

    /// Maximum concurrent deliveries
    #[serde(default = "default_outbox_concurrency")]
    pub max_concurrency: usize,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            table_name: default_outbox_table(),
            poll_interval_ms: default_outbox_poll_interval(),
            batch_size: default_outbox_batch_size(),
            max_retries: default_outbox_max_retries(),
            delivery_timeout_secs: default_outbox_timeout(),
            ordered_delivery: true,
            retention_secs: default_outbox_retention(),
            max_concurrency: default_outbox_concurrency(),
        }
    }
}

fn default_outbox_table() -> String {
    "outbox".to_string()
}

fn default_outbox_poll_interval() -> u64 {
    100
}

fn default_outbox_batch_size() -> usize {
    100
}

fn default_outbox_max_retries() -> u32 {
    3
}

fn default_outbox_timeout() -> u64 {
    30
}

fn default_outbox_retention() -> u64 {
    86400 // 24 hours
}

fn default_outbox_concurrency() -> usize {
    10
}

// ============================================================================
// Health Monitoring Configuration
// ============================================================================

/// Health monitoring configuration for CDC connectors.
///
/// Provides health checks, lag monitoring, and auto-recovery capabilities.
///
/// ## Example
///
/// ```yaml
/// health:
///   enabled: true
///   check_interval_secs: 10
///   max_lag_ms: 30000
///   failure_threshold: 3
///   auto_recovery: true
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HealthMonitorConfig {
    /// Enable health monitoring
    #[serde(default)]
    pub enabled: bool,

    /// Interval between health checks in seconds
    #[serde(default = "default_health_check_interval")]
    pub check_interval_secs: u64,

    /// Maximum allowed replication lag in milliseconds
    #[serde(default = "default_health_max_lag")]
    pub max_lag_ms: u64,

    /// Number of failed checks before marking unhealthy
    #[serde(default = "default_health_failure_threshold")]
    pub failure_threshold: u32,

    /// Number of successful checks to recover
    #[serde(default = "default_health_success_threshold")]
    pub success_threshold: u32,

    /// Timeout for individual health checks in seconds
    #[serde(default = "default_health_check_timeout")]
    pub check_timeout_secs: u64,

    /// Enable automatic recovery on failure
    #[serde(default = "default_true")]
    pub auto_recovery: bool,

    /// Initial recovery delay in seconds
    #[serde(default = "default_health_recovery_delay")]
    pub recovery_delay_secs: u64,

    /// Maximum recovery delay in seconds (for exponential backoff)
    #[serde(default = "default_health_max_recovery_delay")]
    pub max_recovery_delay_secs: u64,
}

impl Default for HealthMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            check_interval_secs: default_health_check_interval(),
            max_lag_ms: default_health_max_lag(),
            failure_threshold: default_health_failure_threshold(),
            success_threshold: default_health_success_threshold(),
            check_timeout_secs: default_health_check_timeout(),
            auto_recovery: true,
            recovery_delay_secs: default_health_recovery_delay(),
            max_recovery_delay_secs: default_health_max_recovery_delay(),
        }
    }
}

fn default_health_check_interval() -> u64 {
    10
}

fn default_health_max_lag() -> u64 {
    30_000 // 30 seconds
}

fn default_health_failure_threshold() -> u32 {
    3
}

fn default_health_success_threshold() -> u32 {
    2
}

fn default_health_check_timeout() -> u64 {
    5
}

fn default_health_recovery_delay() -> u64 {
    1
}

fn default_health_max_recovery_delay() -> u64 {
    60
}

// ============================================================================
// Notification Configuration
// ============================================================================

/// Notification configuration for CDC progress and status updates.
///
/// Configure how and where CDC notifications are sent.
///
/// ## Example
///
/// ```yaml
/// notifications:
///   enabled: true
///   channels:
///     - type: log
///       level: info
///     - type: webhook
///       url: https://api.example.com/cdc-events
///   snapshot_progress: true
///   streaming_status: true
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct NotificationConfig {
    /// Enable notifications
    #[serde(default)]
    pub enabled: bool,

    /// Notification channels
    #[serde(default)]
    pub channels: Vec<NotificationChannelConfig>,

    /// Send snapshot progress notifications
    #[serde(default = "default_true")]
    pub snapshot_progress: bool,

    /// Send streaming status notifications
    #[serde(default = "default_true")]
    pub streaming_status: bool,

    /// Send error notifications
    #[serde(default = "default_true")]
    pub error_notifications: bool,

    /// Minimum interval between notifications in milliseconds (debouncing)
    #[serde(default = "default_notification_interval")]
    pub min_interval_ms: u64,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channels: vec![],
            snapshot_progress: true,
            streaming_status: true,
            error_notifications: true,
            min_interval_ms: default_notification_interval(),
        }
    }
}

fn default_notification_interval() -> u64 {
    1000 // 1 second
}

/// Notification channel configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NotificationChannelConfig {
    /// Log notifications
    Log {
        /// Log level (debug, info, warn, error)
        #[serde(default = "default_log_level")]
        level: String,
    },

    /// Webhook notifications
    Webhook {
        /// Webhook URL
        url: String,

        /// Optional authorization header
        #[serde(default)]
        authorization: Option<String>,

        /// Request timeout in seconds
        #[serde(default = "default_webhook_timeout")]
        timeout_secs: u64,
    },

    /// Metrics notifications (emit as Prometheus metrics)
    Metrics {
        /// Metric name prefix
        #[serde(default = "default_metric_prefix")]
        prefix: String,
    },
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_webhook_timeout() -> u64 {
    10
}

fn default_metric_prefix() -> String {
    "rivven_cdc".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_config_defaults() {
        let config = SnapshotCdcConfig::default();
        assert!(matches!(config.mode, SnapshotModeConfig::Initial));
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.parallel_tables, 4);
        assert!(config.progress_dir.is_none());
        assert_eq!(config.query_timeout_secs, 300);
        assert_eq!(config.throttle_delay_ms, 0);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_snapshot_config_should_snapshot_table() {
        // Empty lists = include all
        let config = SnapshotCdcConfig::default();
        assert!(config.should_snapshot_table("public", "users"));
        assert!(config.should_snapshot_table("schema1", "orders"));

        // With include list
        let config = SnapshotCdcConfig {
            include_tables: vec!["public.users".to_string(), "orders".to_string()],
            ..Default::default()
        };
        assert!(config.should_snapshot_table("public", "users"));
        assert!(config.should_snapshot_table("any_schema", "orders")); // matches just table name
        assert!(!config.should_snapshot_table("public", "products"));

        // With exclude list
        let config = SnapshotCdcConfig {
            exclude_tables: vec!["public.audit_log".to_string()],
            ..Default::default()
        };
        assert!(config.should_snapshot_table("public", "users"));
        assert!(!config.should_snapshot_table("public", "audit_log"));

        // Exclude takes precedence over include
        let config = SnapshotCdcConfig {
            include_tables: vec!["public.users".to_string()],
            exclude_tables: vec!["public.users".to_string()],
            ..Default::default()
        };
        assert!(!config.should_snapshot_table("public", "users"));
    }

    #[test]
    fn test_snapshot_config_query_timeout() {
        let config = SnapshotCdcConfig {
            query_timeout_secs: 60,
            ..Default::default()
        };
        assert_eq!(config.query_timeout(), Duration::from_secs(60));
    }

    #[test]
    fn test_snapshot_config_throttle_delay() {
        // No throttle
        let config = SnapshotCdcConfig::default();
        assert!(config.throttle_delay().is_none());

        // With throttle
        let config = SnapshotCdcConfig {
            throttle_delay_ms: 100,
            ..Default::default()
        };
        assert_eq!(config.throttle_delay(), Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_snapshot_mode_serialization() {
        // Test all modes serialize correctly
        let modes = [
            (SnapshotModeConfig::Initial, "\"initial\""),
            (SnapshotModeConfig::Always, "\"always\""),
            (SnapshotModeConfig::Never, "\"never\""),
            (SnapshotModeConfig::WhenNeeded, "\"when_needed\""),
            (SnapshotModeConfig::InitialOnly, "\"initial_only\""),
            (SnapshotModeConfig::NoData, "\"no_data\""),
            (SnapshotModeConfig::Recovery, "\"recovery\""),
        ];

        for (mode, expected) in modes {
            let serialized = serde_json::to_string(&mode).unwrap();
            assert_eq!(serialized, expected);
        }
    }

    #[test]
    fn test_snapshot_mode_schema_only_alias() {
        // Test that schema_only deserializes to NoData
        let mode: SnapshotModeConfig = serde_json::from_str("\"schema_only\"").unwrap();
        assert!(matches!(mode, SnapshotModeConfig::NoData));
    }
}
