//! # Schema Change Topic
//!
//! Publish DDL events to a dedicated schema change topic.
//!
//! ## Overview
//!
//! This module provides infrastructure for capturing and publishing database
//! schema changes (DDL) to a dedicated topic, enabling downstream systems to:
//!
//! - **Track schema evolution**: Maintain history of all DDL changes
//! - **Schema synchronization**: Keep downstream systems in sync with source
//! - **Migration automation**: Trigger schema migrations in data warehouses
//! - **Audit compliance**: Record who changed what and when
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{SchemaChangeEmitter, SchemaChangeConfig, SchemaChangeType};
//!
//! // Configure schema change publishing
//! let config = SchemaChangeConfig::builder()
//!     .topic_prefix("mydb")
//!     .include_ddl_sql(true)
//!     .include_column_details(true)
//!     .build();
//!
//! let emitter = SchemaChangeEmitter::new(config);
//!
//! // Detect schema change from PostgreSQL RELATION message
//! if let Some(change) = emitter.detect_change("public", "users", &new_columns).await {
//!     println!("Schema changed: {:?}", change);
//! }
//! ```

use super::pattern::pattern_match;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info};

// ============================================================================
// Schema Change Types
// ============================================================================

/// Type of schema change (DDL operation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaChangeType {
    /// CREATE TABLE
    Create,
    /// ALTER TABLE (add/drop/modify column)
    Alter,
    /// DROP TABLE
    Drop,
    /// TRUNCATE TABLE
    Truncate,
    /// RENAME TABLE
    Rename,
    /// CREATE INDEX
    CreateIndex,
    /// DROP INDEX
    DropIndex,
    /// Other DDL (comments, grants, etc.)
    Other,
}

impl SchemaChangeType {
    /// Parse DDL type from SQL statement.
    pub fn from_sql(sql: &str) -> Self {
        let upper = sql.to_uppercase();
        let trimmed = upper.trim();

        if trimmed.starts_with("CREATE TABLE") || trimmed.contains(" CREATE TABLE") {
            SchemaChangeType::Create
        } else if trimmed.starts_with("ALTER TABLE") || trimmed.contains(" ALTER TABLE") {
            SchemaChangeType::Alter
        } else if trimmed.starts_with("DROP TABLE") || trimmed.contains(" DROP TABLE") {
            SchemaChangeType::Drop
        } else if trimmed.starts_with("TRUNCATE") || trimmed.contains(" TRUNCATE") {
            SchemaChangeType::Truncate
        } else if trimmed.starts_with("RENAME TABLE")
            || trimmed.contains(" RENAME TABLE")
            || trimmed.contains(" RENAME TO")
        {
            SchemaChangeType::Rename
        } else if trimmed.starts_with("CREATE INDEX")
            || trimmed.starts_with("CREATE UNIQUE INDEX")
            || trimmed.contains(" CREATE INDEX")
        {
            SchemaChangeType::CreateIndex
        } else if trimmed.starts_with("DROP INDEX") || trimmed.contains(" DROP INDEX") {
            SchemaChangeType::DropIndex
        } else {
            SchemaChangeType::Other
        }
    }

    /// Get a human-readable description.
    pub fn description(&self) -> &'static str {
        match self {
            SchemaChangeType::Create => "CREATE TABLE",
            SchemaChangeType::Alter => "ALTER TABLE",
            SchemaChangeType::Drop => "DROP TABLE",
            SchemaChangeType::Truncate => "TRUNCATE TABLE",
            SchemaChangeType::Rename => "RENAME TABLE",
            SchemaChangeType::CreateIndex => "CREATE INDEX",
            SchemaChangeType::DropIndex => "DROP INDEX",
            SchemaChangeType::Other => "DDL",
        }
    }
}

impl std::fmt::Display for SchemaChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}

// ============================================================================
// Column Definition
// ============================================================================

/// Column definition for schema change events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Database-native type (e.g., "varchar(255)", "integer")
    pub type_name: String,
    /// Type OID (PostgreSQL) or type ID (MySQL)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_id: Option<i32>,
    /// Column position (1-indexed)
    pub position: u32,
    /// Is nullable
    pub nullable: bool,
    /// Is part of primary key
    pub primary_key: bool,
    /// Default value expression (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
}

impl ColumnDefinition {
    /// Create a new column definition.
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, position: u32) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            type_id: None,
            position,
            nullable: true,
            primary_key: false,
            default_value: None,
        }
    }

    /// Set type ID.
    pub fn with_type_id(mut self, type_id: i32) -> Self {
        self.type_id = Some(type_id);
        self
    }

    /// Set nullable.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set primary key.
    pub fn with_primary_key(mut self, pk: bool) -> Self {
        self.primary_key = pk;
        self
    }

    /// Set default value.
    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default_value = Some(default.into());
        self
    }
}

// ============================================================================
// Schema Change Event
// ============================================================================

/// Schema change event (DDL).
///
/// This structure represents a DDL change event with comprehensive
/// metadata for downstream schema evolution handling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeEvent {
    /// Source database type: "postgres", "mysql", "mariadb"
    pub source_type: String,
    /// Database name
    pub database: String,
    /// Schema/namespace name (PostgreSQL: schema, MySQL: database)
    pub schema: String,
    /// Table name (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Type of schema change
    pub change_type: SchemaChangeType,
    /// Original DDL statement (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ddl: Option<String>,
    /// Columns after the change (for CREATE/ALTER)
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub columns: Vec<ColumnDefinition>,
    /// Previous columns (for ALTER, to detect changes)
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub previous_columns: Vec<ColumnDefinition>,
    /// Event timestamp (Unix epoch millis)
    pub timestamp_ms: i64,
    /// Source-specific position (LSN, binlog position)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
    /// PostgreSQL relation ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation_id: Option<u32>,
}

impl SchemaChangeEvent {
    /// Create a new schema change event.
    pub fn new(
        source_type: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        change_type: SchemaChangeType,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);

        Self {
            source_type: source_type.into(),
            database: database.into(),
            schema: schema.into(),
            table: None,
            change_type,
            ddl: None,
            columns: Vec::new(),
            previous_columns: Vec::new(),
            timestamp_ms,
            position: None,
            relation_id: None,
        }
    }

    /// Set table name.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Set DDL statement.
    pub fn with_ddl(mut self, ddl: impl Into<String>) -> Self {
        self.ddl = Some(ddl.into());
        self
    }

    /// Set current columns.
    pub fn with_columns(mut self, columns: Vec<ColumnDefinition>) -> Self {
        self.columns = columns;
        self
    }

    /// Set previous columns.
    pub fn with_previous_columns(mut self, columns: Vec<ColumnDefinition>) -> Self {
        self.previous_columns = columns;
        self
    }

    /// Set position.
    pub fn with_position(mut self, position: impl Into<String>) -> Self {
        self.position = Some(position.into());
        self
    }

    /// Set relation ID.
    pub fn with_relation_id(mut self, id: u32) -> Self {
        self.relation_id = Some(id);
        self
    }

    /// Set timestamp.
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = timestamp_ms;
        self
    }

    /// Get fully-qualified table name (schema.table).
    pub fn qualified_table_name(&self) -> Option<String> {
        self.table
            .as_ref()
            .map(|t| format!("{}.{}", self.schema, t))
    }

    /// Get topic name for this event.
    pub fn topic_name(&self, prefix: &str) -> String {
        if prefix.is_empty() {
            "schema_changes".to_string()
        } else {
            format!("{}.schema_changes", prefix)
        }
    }

    /// Convert to JSON value.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Get column changes between previous and current.
    pub fn get_column_changes(&self) -> ColumnChanges {
        let previous_map: HashMap<&str, &ColumnDefinition> = self
            .previous_columns
            .iter()
            .map(|c| (c.name.as_str(), c))
            .collect();
        let current_map: HashMap<&str, &ColumnDefinition> =
            self.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        let added: Vec<_> = self
            .columns
            .iter()
            .filter(|c| !previous_map.contains_key(c.name.as_str()))
            .cloned()
            .collect();

        let removed: Vec<_> = self
            .previous_columns
            .iter()
            .filter(|c| !current_map.contains_key(c.name.as_str()))
            .cloned()
            .collect();

        let modified: Vec<_> = self
            .columns
            .iter()
            .filter_map(|c| {
                previous_map.get(c.name.as_str()).and_then(|prev| {
                    if c.type_name != prev.type_name
                        || c.nullable != prev.nullable
                        || c.primary_key != prev.primary_key
                    {
                        Some(ColumnModification {
                            column: c.clone(),
                            previous: (*prev).clone(),
                        })
                    } else {
                        None
                    }
                })
            })
            .collect();

        ColumnChanges {
            added,
            removed,
            modified,
        }
    }
}

/// Column changes detected in an ALTER TABLE.
#[derive(Debug, Clone)]
pub struct ColumnChanges {
    /// Newly added columns
    pub added: Vec<ColumnDefinition>,
    /// Removed columns
    pub removed: Vec<ColumnDefinition>,
    /// Modified columns
    pub modified: Vec<ColumnModification>,
}

impl ColumnChanges {
    /// Check if there are any changes.
    pub fn has_changes(&self) -> bool {
        !self.added.is_empty() || !self.removed.is_empty() || !self.modified.is_empty()
    }

    /// Get summary of changes.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.added.is_empty() {
            parts.push(format!("+{} columns", self.added.len()));
        }
        if !self.removed.is_empty() {
            parts.push(format!("-{} columns", self.removed.len()));
        }
        if !self.modified.is_empty() {
            parts.push(format!("~{} columns", self.modified.len()));
        }
        if parts.is_empty() {
            "no changes".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// A column that was modified.
#[derive(Debug, Clone)]
pub struct ColumnModification {
    /// Current column definition
    pub column: ColumnDefinition,
    /// Previous column definition
    pub previous: ColumnDefinition,
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for schema change publishing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeConfig {
    /// Whether schema change publishing is enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Topic prefix (e.g., "mydb" -> "mydb.schema_changes")
    #[serde(default)]
    pub topic_prefix: String,
    /// Include original DDL SQL in events
    #[serde(default = "default_true")]
    pub include_ddl: bool,
    /// Include column details in events
    #[serde(default = "default_true")]
    pub include_columns: bool,
    /// Include previous column state for ALTERs
    #[serde(default = "default_true")]
    pub include_previous: bool,
    /// Table patterns to exclude (glob patterns)
    #[serde(default)]
    pub exclude_tables: Vec<String>,
    /// DDL types to exclude
    #[serde(default)]
    pub exclude_types: Vec<SchemaChangeType>,
}

fn default_enabled() -> bool {
    true
}

fn default_true() -> bool {
    true
}

impl Default for SchemaChangeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topic_prefix: String::new(),
            include_ddl: true,
            include_columns: true,
            include_previous: true,
            exclude_tables: Vec::new(),
            exclude_types: Vec::new(),
        }
    }
}

impl SchemaChangeConfig {
    /// Create a new builder.
    pub fn builder() -> SchemaChangeConfigBuilder {
        SchemaChangeConfigBuilder::default()
    }

    /// Check if a table should be excluded.
    pub fn is_table_excluded(&self, schema: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", schema, table);
        for pattern in &self.exclude_tables {
            if pattern_match(pattern, &full_name) || pattern_match(pattern, table) {
                return true;
            }
        }
        false
    }

    /// Check if a change type should be excluded.
    pub fn is_type_excluded(&self, change_type: SchemaChangeType) -> bool {
        self.exclude_types.contains(&change_type)
    }
}

/// Builder for SchemaChangeConfig.
#[derive(Debug, Default)]
pub struct SchemaChangeConfigBuilder {
    config: SchemaChangeConfig,
}

impl SchemaChangeConfigBuilder {
    /// Enable or disable schema change publishing.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set topic prefix.
    pub fn topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.topic_prefix = prefix.into();
        self
    }

    /// Include DDL SQL in events.
    pub fn include_ddl(mut self, include: bool) -> Self {
        self.config.include_ddl = include;
        self
    }

    /// Include column details in events.
    pub fn include_columns(mut self, include: bool) -> Self {
        self.config.include_columns = include;
        self
    }

    /// Include previous column state for ALTERs.
    pub fn include_previous(mut self, include: bool) -> Self {
        self.config.include_previous = include;
        self
    }

    /// Add table patterns to exclude.
    pub fn exclude_tables(mut self, patterns: Vec<String>) -> Self {
        self.config.exclude_tables = patterns;
        self
    }

    /// Add DDL types to exclude.
    pub fn exclude_types(mut self, types: Vec<SchemaChangeType>) -> Self {
        self.config.exclude_types = types;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> SchemaChangeConfig {
        self.config
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Statistics for schema change tracking.
#[derive(Debug, Default)]
pub struct SchemaChangeStats {
    /// Total schema changes detected
    pub changes_detected: AtomicU64,
    /// Schema changes published
    pub changes_published: AtomicU64,
    /// Schema changes filtered out
    pub changes_filtered: AtomicU64,
    /// CREATE TABLE events
    pub creates: AtomicU64,
    /// ALTER TABLE events
    pub alters: AtomicU64,
    /// DROP TABLE events
    pub drops: AtomicU64,
    /// Other DDL events
    pub others: AtomicU64,
}

impl SchemaChangeStats {
    /// Create new statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a schema change.
    pub fn record_change(&self, change_type: SchemaChangeType, published: bool) {
        self.changes_detected.fetch_add(1, Ordering::Relaxed);
        if published {
            self.changes_published.fetch_add(1, Ordering::Relaxed);
        } else {
            self.changes_filtered.fetch_add(1, Ordering::Relaxed);
        }

        match change_type {
            SchemaChangeType::Create => self.creates.fetch_add(1, Ordering::Relaxed),
            SchemaChangeType::Alter => self.alters.fetch_add(1, Ordering::Relaxed),
            SchemaChangeType::Drop => self.drops.fetch_add(1, Ordering::Relaxed),
            _ => self.others.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Get snapshot of statistics.
    pub fn snapshot(&self) -> SchemaChangeStatsSnapshot {
        SchemaChangeStatsSnapshot {
            changes_detected: self.changes_detected.load(Ordering::Relaxed),
            changes_published: self.changes_published.load(Ordering::Relaxed),
            changes_filtered: self.changes_filtered.load(Ordering::Relaxed),
            creates: self.creates.load(Ordering::Relaxed),
            alters: self.alters.load(Ordering::Relaxed),
            drops: self.drops.load(Ordering::Relaxed),
            others: self.others.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of schema change statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChangeStatsSnapshot {
    pub changes_detected: u64,
    pub changes_published: u64,
    pub changes_filtered: u64,
    pub creates: u64,
    pub alters: u64,
    pub drops: u64,
    pub others: u64,
}

// ============================================================================
// Schema Change Emitter
// ============================================================================

/// Cached schema information for change detection.
#[derive(Debug, Clone)]
struct CachedSchema {
    columns: Vec<ColumnDefinition>,
    last_seen: u64,
}

/// Schema change emitter for detecting and publishing DDL events.
///
/// Tracks table schemas and detects changes when RELATION messages
/// (PostgreSQL) or DDL queries (MySQL) indicate schema modifications.
pub struct SchemaChangeEmitter {
    /// Configuration
    config: SchemaChangeConfig,
    /// Cached schemas by table (schema.table -> columns)
    schema_cache: RwLock<HashMap<String, CachedSchema>>,
    /// Statistics
    stats: SchemaChangeStats,
}

impl SchemaChangeEmitter {
    /// Create a new schema change emitter.
    pub fn new(config: SchemaChangeConfig) -> Self {
        Self {
            config,
            schema_cache: RwLock::new(HashMap::new()),
            stats: SchemaChangeStats::new(),
        }
    }

    /// Create with default configuration.
    pub fn default_config() -> Self {
        Self::new(SchemaChangeConfig::default())
    }

    /// Check if schema change publishing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get configuration.
    pub fn config(&self) -> &SchemaChangeConfig {
        &self.config
    }

    /// Get statistics.
    pub fn stats(&self) -> &SchemaChangeStats {
        &self.stats
    }

    /// Detect schema change from PostgreSQL RELATION message.
    ///
    /// Compares the new columns against cached schema and emits
    /// a schema change event if differences are detected.
    pub async fn detect_postgres_change(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        relation_id: u32,
        columns: Vec<ColumnDefinition>,
        lsn: Option<&str>,
    ) -> Option<SchemaChangeEvent> {
        if !self.config.enabled {
            return None;
        }

        if self.config.is_table_excluded(schema, table) {
            debug!("Schema change excluded for {}.{}", schema, table);
            return None;
        }

        let key = format!("{}.{}", schema, table);
        let mut cache = self.schema_cache.write().await;

        let previous = cache.get(&key).map(|c| c.columns.clone());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Determine change type
        let (change_type, should_emit) = if let Some(ref prev) = previous {
            // Check if columns changed
            if Self::columns_differ(prev, &columns) {
                (SchemaChangeType::Alter, true)
            } else {
                (SchemaChangeType::Alter, false)
            }
        } else {
            // First time seeing this table - could be CREATE or first RELATION
            (SchemaChangeType::Create, true)
        };

        // Update cache
        cache.insert(
            key,
            CachedSchema {
                columns: columns.clone(),
                last_seen: now,
            },
        );

        if !should_emit {
            return None;
        }

        if self.config.is_type_excluded(change_type) {
            self.stats.record_change(change_type, false);
            return None;
        }

        let mut event = SchemaChangeEvent::new("postgres", database, schema, change_type)
            .with_table(table)
            .with_relation_id(relation_id);

        if self.config.include_columns {
            event = event.with_columns(columns);
        }

        if self.config.include_previous {
            if let Some(prev) = previous {
                event = event.with_previous_columns(prev);
            }
        }

        if let Some(pos) = lsn {
            event = event.with_position(pos);
        }

        self.stats.record_change(change_type, true);
        info!(
            "Schema change detected: {} {}.{} (relation_id={})",
            change_type, schema, table, relation_id
        );

        Some(event)
    }

    /// Detect schema change from MySQL DDL query.
    ///
    /// Parses the DDL statement and emits a schema change event.
    pub async fn detect_mysql_change(
        &self,
        database: &str,
        ddl: &str,
        binlog_position: Option<&str>,
    ) -> Option<SchemaChangeEvent> {
        if !self.config.enabled {
            return None;
        }

        let change_type = SchemaChangeType::from_sql(ddl);

        if self.config.is_type_excluded(change_type) {
            self.stats.record_change(change_type, false);
            return None;
        }

        // Try to extract table name from DDL
        let table = Self::extract_table_from_ddl(ddl);

        if let Some(ref tbl) = table {
            if self.config.is_table_excluded(database, tbl) {
                self.stats.record_change(change_type, false);
                return None;
            }
        }

        let mut event = SchemaChangeEvent::new("mysql", database, database, change_type);

        if let Some(tbl) = table {
            event = event.with_table(tbl);
        }

        if self.config.include_ddl {
            event = event.with_ddl(ddl);
        }

        if let Some(pos) = binlog_position {
            event = event.with_position(pos);
        }

        self.stats.record_change(change_type, true);
        info!("Schema change detected: {} - {:?}", change_type, ddl);

        Some(event)
    }

    /// Clear cached schema for a table (e.g., after DROP TABLE).
    pub async fn clear_cache(&self, schema: &str, table: &str) {
        let key = format!("{}.{}", schema, table);
        let mut cache = self.schema_cache.write().await;
        cache.remove(&key);
        debug!("Cleared schema cache for {}", key);
    }

    /// Clear all cached schemas.
    pub async fn clear_all_cache(&self) {
        let mut cache = self.schema_cache.write().await;
        cache.clear();
        debug!("Cleared all schema cache");
    }

    /// Get cached schema for a table.
    pub async fn get_cached_schema(
        &self,
        schema: &str,
        table: &str,
    ) -> Option<Vec<ColumnDefinition>> {
        let key = format!("{}.{}", schema, table);
        let cache = self.schema_cache.read().await;
        cache.get(&key).map(|c| c.columns.clone())
    }

    /// Prune stale cache entries older than the given age in seconds.
    pub async fn prune_stale_cache(&self, max_age_secs: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let cutoff = now.saturating_sub(max_age_secs);

        let mut cache = self.schema_cache.write().await;
        let initial_size = cache.len();
        cache.retain(|_, v| v.last_seen >= cutoff);
        let removed = initial_size - cache.len();
        if removed > 0 {
            debug!("Pruned {} stale schema cache entries", removed);
        }
    }

    /// Get the number of cached schemas.
    pub async fn cache_size(&self) -> usize {
        self.schema_cache.read().await.len()
    }

    /// Get topic name for schema change events.
    pub fn topic_name(&self) -> String {
        if self.config.topic_prefix.is_empty() {
            "schema_changes".to_string()
        } else {
            format!("{}.schema_changes", self.config.topic_prefix)
        }
    }

    /// Check if two column lists differ.
    fn columns_differ(a: &[ColumnDefinition], b: &[ColumnDefinition]) -> bool {
        if a.len() != b.len() {
            return true;
        }

        for (col_a, col_b) in a.iter().zip(b.iter()) {
            if col_a.name != col_b.name
                || col_a.type_name != col_b.type_name
                || col_a.type_id != col_b.type_id
                || col_a.nullable != col_b.nullable
                || col_a.primary_key != col_b.primary_key
            {
                return true;
            }
        }

        false
    }

    /// Extract table name from DDL statement.
    fn extract_table_from_ddl(ddl: &str) -> Option<String> {
        let upper = ddl.to_uppercase();
        let ddl_bytes = ddl.as_bytes();

        // Patterns to match: CREATE TABLE name, ALTER TABLE name, DROP TABLE name, etc.
        let patterns = [
            "CREATE TABLE ",
            "ALTER TABLE ",
            "DROP TABLE ",
            "TRUNCATE TABLE ",
            "TRUNCATE ",
            "RENAME TABLE ",
            "CREATE INDEX ",
            "CREATE UNIQUE INDEX ",
            "DROP INDEX ",
        ];

        for pattern in patterns {
            if let Some(pos) = upper.find(pattern) {
                let start = pos + pattern.len();
                // Skip "IF EXISTS" or "IF NOT EXISTS"
                let remaining_upper = &upper[start..];

                let skip = if remaining_upper.starts_with("IF EXISTS ") {
                    10
                } else if remaining_upper.starts_with("IF NOT EXISTS ") {
                    14
                } else {
                    0
                };

                let name_start = start + skip;
                if name_start >= ddl.len() {
                    continue;
                }

                // Extract identifier (handle quoted and unquoted)
                let remaining_bytes = &ddl_bytes[name_start..];
                let name = Self::extract_identifier(remaining_bytes);
                if !name.is_empty() {
                    return Some(name);
                }
            }
        }

        None
    }

    /// Extract identifier from byte slice.
    fn extract_identifier(bytes: &[u8]) -> String {
        let mut result = Vec::new();
        let mut i = 0;

        // Skip leading whitespace
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }

        if i >= bytes.len() {
            return String::new();
        }

        // Handle quoted identifier
        let quote_char = if bytes[i] == b'"' || bytes[i] == b'`' {
            let q = bytes[i];
            i += 1;
            Some(q)
        } else {
            None
        };

        while i < bytes.len() {
            let c = bytes[i];
            if let Some(q) = quote_char {
                if c == q {
                    break;
                }
                result.push(c);
            } else {
                // Unquoted: stop at whitespace, parenthesis, or semicolon
                if c.is_ascii_whitespace() || c == b'(' || c == b';' || c == b',' {
                    break;
                }
                result.push(c);
            }
            i += 1;
        }

        String::from_utf8(result).unwrap_or_default()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_type_from_sql() {
        assert_eq!(
            SchemaChangeType::from_sql("CREATE TABLE users (id INT)"),
            SchemaChangeType::Create
        );
        assert_eq!(
            SchemaChangeType::from_sql("ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
            SchemaChangeType::Alter
        );
        assert_eq!(
            SchemaChangeType::from_sql("DROP TABLE IF EXISTS users"),
            SchemaChangeType::Drop
        );
        assert_eq!(
            SchemaChangeType::from_sql("TRUNCATE TABLE users"),
            SchemaChangeType::Truncate
        );
        assert_eq!(
            SchemaChangeType::from_sql("CREATE INDEX idx_email ON users(email)"),
            SchemaChangeType::CreateIndex
        );
        assert_eq!(
            SchemaChangeType::from_sql("-- comment"),
            SchemaChangeType::Other
        );
    }

    #[test]
    fn test_column_definition() {
        let col = ColumnDefinition::new("id", "integer", 1)
            .with_type_id(23)
            .with_nullable(false)
            .with_primary_key(true);

        assert_eq!(col.name, "id");
        assert_eq!(col.type_name, "integer");
        assert_eq!(col.type_id, Some(23));
        assert!(!col.nullable);
        assert!(col.primary_key);
    }

    #[test]
    fn test_schema_change_event() {
        let event = SchemaChangeEvent::new("postgres", "mydb", "public", SchemaChangeType::Create)
            .with_table("users")
            .with_ddl("CREATE TABLE users (id INT)")
            .with_relation_id(12345);

        assert_eq!(event.source_type, "postgres");
        assert_eq!(event.database, "mydb");
        assert_eq!(event.schema, "public");
        assert_eq!(event.table.as_deref(), Some("users"));
        assert_eq!(event.change_type, SchemaChangeType::Create);
        assert_eq!(event.relation_id, Some(12345));
    }

    #[test]
    fn test_qualified_table_name() {
        let event = SchemaChangeEvent::new("postgres", "mydb", "public", SchemaChangeType::Create)
            .with_table("users");

        assert_eq!(
            event.qualified_table_name(),
            Some("public.users".to_string())
        );
    }

    #[test]
    fn test_topic_name() {
        let event = SchemaChangeEvent::new("postgres", "mydb", "public", SchemaChangeType::Create);

        assert_eq!(event.topic_name("mydb"), "mydb.schema_changes");
        assert_eq!(event.topic_name(""), "schema_changes");
    }

    #[test]
    fn test_column_changes() {
        let prev = vec![
            ColumnDefinition::new("id", "integer", 1),
            ColumnDefinition::new("name", "varchar(100)", 2),
            ColumnDefinition::new("old_col", "text", 3),
        ];

        let curr = vec![
            ColumnDefinition::new("id", "bigint", 1), // type changed
            ColumnDefinition::new("name", "varchar(100)", 2), // unchanged
            ColumnDefinition::new("new_col", "text", 3), // added (old_col removed)
        ];

        let event = SchemaChangeEvent::new("postgres", "mydb", "public", SchemaChangeType::Alter)
            .with_previous_columns(prev)
            .with_columns(curr);

        let changes = event.get_column_changes();

        assert_eq!(changes.added.len(), 1);
        assert_eq!(changes.added[0].name, "new_col");

        assert_eq!(changes.removed.len(), 1);
        assert_eq!(changes.removed[0].name, "old_col");

        assert_eq!(changes.modified.len(), 1);
        assert_eq!(changes.modified[0].column.name, "id");
        assert_eq!(changes.modified[0].previous.type_name, "integer");
        assert_eq!(changes.modified[0].column.type_name, "bigint");
    }

    #[test]
    fn test_config_builder() {
        let config = SchemaChangeConfig::builder()
            .enabled(true)
            .topic_prefix("mydb")
            .include_ddl(false)
            .exclude_tables(vec!["temp_*".to_string()])
            .build();

        assert!(config.enabled);
        assert_eq!(config.topic_prefix, "mydb");
        assert!(!config.include_ddl);
        assert_eq!(config.exclude_tables, vec!["temp_*"]);
    }

    #[test]
    fn test_table_exclusion() {
        let config = SchemaChangeConfig::builder()
            .exclude_tables(vec!["temp_*".to_string(), "public.audit_*".to_string()])
            .build();

        assert!(config.is_table_excluded("public", "temp_data"));
        assert!(config.is_table_excluded("public", "audit_log"));
        assert!(!config.is_table_excluded("public", "users"));
    }

    #[test]
    fn test_type_exclusion() {
        let config = SchemaChangeConfig::builder()
            .exclude_types(vec![
                SchemaChangeType::CreateIndex,
                SchemaChangeType::DropIndex,
            ])
            .build();

        assert!(config.is_type_excluded(SchemaChangeType::CreateIndex));
        assert!(config.is_type_excluded(SchemaChangeType::DropIndex));
        assert!(!config.is_type_excluded(SchemaChangeType::Create));
    }

    #[test]
    fn test_extract_table_from_ddl() {
        assert_eq!(
            SchemaChangeEmitter::extract_table_from_ddl("CREATE TABLE users (id INT)"),
            Some("users".to_string())
        );

        assert_eq!(
            SchemaChangeEmitter::extract_table_from_ddl(
                "CREATE TABLE IF NOT EXISTS users (id INT)"
            ),
            Some("users".to_string())
        );

        assert_eq!(
            SchemaChangeEmitter::extract_table_from_ddl(
                "ALTER TABLE `users` ADD COLUMN email VARCHAR(255)"
            ),
            Some("users".to_string())
        );

        assert_eq!(
            SchemaChangeEmitter::extract_table_from_ddl("DROP TABLE IF EXISTS \"Users\""),
            Some("Users".to_string())
        );

        assert_eq!(
            SchemaChangeEmitter::extract_table_from_ddl("TRUNCATE users"),
            Some("users".to_string())
        );
    }

    #[test]
    fn test_stats() {
        let stats = SchemaChangeStats::new();

        stats.record_change(SchemaChangeType::Create, true);
        stats.record_change(SchemaChangeType::Alter, true);
        stats.record_change(SchemaChangeType::Alter, false);
        stats.record_change(SchemaChangeType::Drop, true);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.changes_detected, 4);
        assert_eq!(snapshot.changes_published, 3);
        assert_eq!(snapshot.changes_filtered, 1);
        assert_eq!(snapshot.creates, 1);
        assert_eq!(snapshot.alters, 2);
        assert_eq!(snapshot.drops, 1);
    }

    #[tokio::test]
    async fn test_emitter_cache() {
        let config = SchemaChangeConfig::builder().topic_prefix("test").build();
        let emitter = SchemaChangeEmitter::new(config);

        let columns = vec![
            ColumnDefinition::new("id", "integer", 1),
            ColumnDefinition::new("name", "text", 2),
        ];

        // First detection should emit CREATE
        let event = emitter
            .detect_postgres_change("testdb", "public", "users", 12345, columns.clone(), None)
            .await;

        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.change_type, SchemaChangeType::Create);
        assert_eq!(event.columns.len(), 2);

        // Same columns should not emit
        let event = emitter
            .detect_postgres_change("testdb", "public", "users", 12345, columns.clone(), None)
            .await;

        assert!(event.is_none());

        // Changed columns should emit ALTER
        let new_columns = vec![
            ColumnDefinition::new("id", "bigint", 1), // changed type
            ColumnDefinition::new("name", "text", 2),
            ColumnDefinition::new("email", "text", 3), // added
        ];

        let event = emitter
            .detect_postgres_change("testdb", "public", "users", 12345, new_columns, None)
            .await;

        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.change_type, SchemaChangeType::Alter);
        assert_eq!(event.columns.len(), 3);
        assert_eq!(event.previous_columns.len(), 2);
    }

    #[tokio::test]
    async fn test_emitter_disabled() {
        let config = SchemaChangeConfig::builder().enabled(false).build();
        let emitter = SchemaChangeEmitter::new(config);

        let columns = vec![ColumnDefinition::new("id", "integer", 1)];

        let event = emitter
            .detect_postgres_change("testdb", "public", "users", 12345, columns, None)
            .await;

        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_emitter_table_exclusion() {
        let config = SchemaChangeConfig::builder()
            .exclude_tables(vec!["temp_*".to_string()])
            .build();
        let emitter = SchemaChangeEmitter::new(config);

        let columns = vec![ColumnDefinition::new("id", "integer", 1)];

        let event = emitter
            .detect_postgres_change(
                "testdb",
                "public",
                "temp_data",
                12345,
                columns.clone(),
                None,
            )
            .await;

        assert!(event.is_none());

        let event = emitter
            .detect_postgres_change("testdb", "public", "users", 12346, columns, None)
            .await;

        assert!(event.is_some());
    }

    #[tokio::test]
    async fn test_mysql_ddl_detection() {
        let config = SchemaChangeConfig::builder().topic_prefix("mysql").build();
        let emitter = SchemaChangeEmitter::new(config);

        let event = emitter
            .detect_mysql_change(
                "testdb",
                "CREATE TABLE users (id INT)",
                Some("mysql-bin.000001:12345"),
            )
            .await;

        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.source_type, "mysql");
        assert_eq!(event.change_type, SchemaChangeType::Create);
        assert_eq!(event.table.as_deref(), Some("users"));
        assert!(event.ddl.is_some());
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let emitter = SchemaChangeEmitter::default_config();

        let columns = vec![ColumnDefinition::new("id", "integer", 1)];

        // Add to cache
        emitter
            .detect_postgres_change("testdb", "public", "users", 12345, columns.clone(), None)
            .await;

        assert!(emitter.get_cached_schema("public", "users").await.is_some());

        // Clear cache
        emitter.clear_cache("public", "users").await;

        assert!(emitter.get_cached_schema("public", "users").await.is_none());
    }

    #[test]
    fn test_event_to_json() {
        let event = SchemaChangeEvent::new("postgres", "mydb", "public", SchemaChangeType::Create)
            .with_table("users")
            .with_columns(vec![ColumnDefinition::new("id", "integer", 1)]);

        let json = event.to_json();

        assert!(json.is_object());
        assert_eq!(json["source_type"], "postgres");
        assert_eq!(json["change_type"], "CREATE");
        assert_eq!(json["table"], "users");
    }

    #[test]
    fn test_column_changes_summary() {
        let changes = ColumnChanges {
            added: vec![ColumnDefinition::new("a", "int", 1)],
            removed: vec![ColumnDefinition::new("b", "int", 2)],
            modified: vec![],
        };

        assert!(changes.has_changes());
        assert!(changes.summary().contains("+1 columns"));
        assert!(changes.summary().contains("-1 columns"));
    }

    #[test]
    fn test_change_type_display() {
        assert_eq!(format!("{}", SchemaChangeType::Create), "CREATE TABLE");
        assert_eq!(format!("{}", SchemaChangeType::Alter), "ALTER TABLE");
        assert_eq!(format!("{}", SchemaChangeType::Drop), "DROP TABLE");
    }
}
