//! # Tombstone Configuration
//!
//! Configuration for tombstone event emission in CDC pipelines.
//!
//! ## Overview
//!
//! Tombstones are special events emitted after DELETE operations with the
//! same key but null payload. They signal to Kafka log compaction that the
//! key should be removed from the compacted log.
//!
//! ## Behavior
//!
//! - Tombstone is emitted after each DELETE event
//! - Tombstone has same key as DELETE event
//! - Tombstone has null value (before and after are null)
//!
//! ## Usage
//!
//! ```rust
//! use rivven_cdc::common::tombstone::{TombstoneConfig, TombstoneEmitter};
//! use rivven_cdc::CdcEvent;
//! use serde_json::json;
//!
//! // Enable tombstones (default)
//! let config = TombstoneConfig::default();
//! let emitter = TombstoneEmitter::new(config);
//!
//! // Process a DELETE event
//! let delete = CdcEvent::delete("pg", "db", "s", "t", json!({"id": 1}), 1000);
//! let events = emitter.process(delete);
//!
//! // Returns [DELETE, TOMBSTONE]
//! assert_eq!(events.len(), 2);
//! ```

use super::pattern::pattern_match;

use crate::common::{CdcEvent, CdcOp};
use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for tombstone emission.
#[derive(Debug, Clone)]
pub struct TombstoneConfig {
    /// Whether to emit tombstone events after DELETE operations.
    ///
    /// When enabled (default), each DELETE event is followed by a tombstone
    /// event with the same key but null payload. This is required for Kafka
    /// log compaction to properly remove deleted records.
    pub emit_tombstones: bool,

    /// Include key data in the tombstone's `before` field.
    ///
    /// When enabled, the primary key columns from the DELETE event are
    /// copied to the tombstone's `before` field for easier key extraction
    /// in downstream consumers.
    ///
    /// Default: false (pure null tombstone as per Kafka spec)
    pub include_key_in_tombstone: bool,

    /// Tables to exclude from tombstone emission.
    ///
    /// Use glob patterns: `["audit.*", "logs.*"]`
    pub exclude_tables: Vec<String>,
}

impl Default for TombstoneConfig {
    fn default() -> Self {
        Self {
            emit_tombstones: true,
            include_key_in_tombstone: false,
            exclude_tables: Vec::new(),
        }
    }
}

impl TombstoneConfig {
    /// Create a new builder for TombstoneConfig.
    pub fn builder() -> TombstoneConfigBuilder {
        TombstoneConfigBuilder::default()
    }

    /// Disable tombstone emission.
    pub fn disabled() -> Self {
        Self {
            emit_tombstones: false,
            ..Default::default()
        }
    }

    /// Check if tombstones are enabled.
    pub fn is_enabled(&self) -> bool {
        self.emit_tombstones
    }

    /// Check if a table is excluded from tombstone emission.
    pub fn is_excluded(&self, schema: &str, table: &str) -> bool {
        let qualified = format!("{}.{}", schema, table);
        self.exclude_tables.iter().any(|pattern| {
            if pattern.contains('*') || pattern.contains('?') {
                pattern_match(pattern, &qualified)
            } else {
                pattern == &qualified
            }
        })
    }
}

/// Builder for TombstoneConfig.
#[derive(Default)]
pub struct TombstoneConfigBuilder {
    config: TombstoneConfig,
}

impl TombstoneConfigBuilder {
    /// Enable or disable tombstone emission.
    pub fn emit_tombstones(mut self, enabled: bool) -> Self {
        self.config.emit_tombstones = enabled;
        self
    }

    /// Include key data in tombstone's before field.
    pub fn include_key_in_tombstone(mut self, enabled: bool) -> Self {
        self.config.include_key_in_tombstone = enabled;
        self
    }

    /// Add tables to exclude from tombstone emission.
    pub fn exclude_tables(mut self, patterns: Vec<String>) -> Self {
        self.config.exclude_tables = patterns;
        self
    }

    /// Add a single table exclusion pattern.
    pub fn exclude_table(mut self, pattern: impl Into<String>) -> Self {
        self.config.exclude_tables.push(pattern.into());
        self
    }

    /// Build the configuration.
    pub fn build(self) -> TombstoneConfig {
        self.config
    }
}

/// Statistics for tombstone emission.
#[derive(Debug, Default)]
pub struct TombstoneStats {
    /// Total DELETE events processed
    pub deletes_processed: AtomicU64,
    /// Tombstones emitted
    pub tombstones_emitted: AtomicU64,
    /// Tombstones skipped (excluded tables)
    pub tombstones_skipped: AtomicU64,
}

impl TombstoneStats {
    /// Create new stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total deletes processed.
    pub fn deletes(&self) -> u64 {
        self.deletes_processed.load(Ordering::Relaxed)
    }

    /// Get tombstones emitted.
    pub fn emitted(&self) -> u64 {
        self.tombstones_emitted.load(Ordering::Relaxed)
    }

    /// Get tombstones skipped.
    pub fn skipped(&self) -> u64 {
        self.tombstones_skipped.load(Ordering::Relaxed)
    }

    fn record_delete(&self) {
        self.deletes_processed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_tombstone(&self) {
        self.tombstones_emitted.fetch_add(1, Ordering::Relaxed);
    }

    fn record_skip(&self) {
        self.tombstones_skipped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Tombstone emitter that processes CDC events and emits tombstones.
pub struct TombstoneEmitter {
    config: TombstoneConfig,
    stats: TombstoneStats,
}

impl TombstoneEmitter {
    /// Create a new tombstone emitter.
    pub fn new(config: TombstoneConfig) -> Self {
        Self {
            config,
            stats: TombstoneStats::new(),
        }
    }

    /// Process a CDC event, potentially emitting a tombstone.
    ///
    /// Returns a vector of events:
    /// - For DELETE: [DELETE, TOMBSTONE] if tombstones enabled
    /// - For other operations: [original event]
    pub fn process(&self, event: CdcEvent) -> Vec<CdcEvent> {
        if event.op != CdcOp::Delete {
            return vec![event];
        }

        self.stats.record_delete();

        if !self.config.emit_tombstones {
            return vec![event];
        }

        if self.config.is_excluded(&event.schema, &event.table) {
            self.stats.record_skip();
            return vec![event];
        }

        let tombstone = if self.config.include_key_in_tombstone {
            // Copy key data to tombstone's before field
            CdcEvent {
                source_type: event.source_type.clone(),
                database: event.database.clone(),
                schema: event.schema.clone(),
                table: event.table.clone(),
                op: CdcOp::Tombstone,
                before: event.before.clone(),
                after: None,
                timestamp: event.timestamp,
                transaction: event.transaction.clone(),
            }
        } else {
            // Pure null tombstone
            CdcEvent::tombstone(&event)
        };

        self.stats.record_tombstone();

        vec![event, tombstone]
    }

    /// Process a batch of events.
    pub fn process_batch(&self, events: Vec<CdcEvent>) -> Vec<CdcEvent> {
        let mut result = Vec::with_capacity(events.len() * 2);
        for event in events {
            result.extend(self.process(event));
        }
        result
    }

    /// Get emission statistics.
    pub fn stats(&self) -> &TombstoneStats {
        &self.stats
    }

    /// Get configuration.
    pub fn config(&self) -> &TombstoneConfig {
        &self.config
    }
}

impl Default for TombstoneEmitter {
    fn default() -> Self {
        Self::new(TombstoneConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_config() {
        let config = TombstoneConfig::default();
        assert!(config.emit_tombstones);
        assert!(!config.include_key_in_tombstone);
        assert!(config.exclude_tables.is_empty());
    }

    #[test]
    fn test_disabled_config() {
        let config = TombstoneConfig::disabled();
        assert!(!config.emit_tombstones);
    }

    #[test]
    fn test_builder() {
        let config = TombstoneConfig::builder()
            .emit_tombstones(true)
            .include_key_in_tombstone(true)
            .exclude_tables(vec!["audit.*".to_string()])
            .build();

        assert!(config.emit_tombstones);
        assert!(config.include_key_in_tombstone);
        assert_eq!(config.exclude_tables.len(), 1);
    }

    #[test]
    fn test_emitter_delete_with_tombstone() {
        let emitter = TombstoneEmitter::default();
        let delete = CdcEvent::delete("pg", "db", "public", "users", json!({"id": 1}), 1000);

        let events = emitter.process(delete);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].op, CdcOp::Delete);
        assert_eq!(events[1].op, CdcOp::Tombstone);
        assert!(events[1].before.is_none());
        assert!(events[1].after.is_none());

        assert_eq!(emitter.stats().deletes(), 1);
        assert_eq!(emitter.stats().emitted(), 1);
    }

    #[test]
    fn test_emitter_disabled() {
        let config = TombstoneConfig::disabled();
        let emitter = TombstoneEmitter::new(config);
        let delete = CdcEvent::delete("pg", "db", "public", "users", json!({"id": 1}), 1000);

        let events = emitter.process(delete);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, CdcOp::Delete);
    }

    #[test]
    fn test_emitter_insert_passthrough() {
        let emitter = TombstoneEmitter::default();
        let insert = CdcEvent::insert("pg", "db", "public", "users", json!({"id": 1}), 1000);

        let events = emitter.process(insert);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].op, CdcOp::Insert);
    }

    #[test]
    fn test_emitter_excluded_table() {
        let config = TombstoneConfig::builder().exclude_table("audit.*").build();
        let emitter = TombstoneEmitter::new(config);
        let delete = CdcEvent::delete("pg", "db", "audit", "log", json!({"id": 1}), 1000);

        let events = emitter.process(delete);

        assert_eq!(events.len(), 1);
        assert_eq!(emitter.stats().skipped(), 1);
    }

    #[test]
    fn test_emitter_include_key() {
        let config = TombstoneConfig::builder()
            .include_key_in_tombstone(true)
            .build();
        let emitter = TombstoneEmitter::new(config);
        let delete = CdcEvent::delete("pg", "db", "s", "t", json!({"id": 42}), 1000);

        let events = emitter.process(delete);

        assert_eq!(events.len(), 2);
        let tombstone = &events[1];
        assert_eq!(tombstone.op, CdcOp::Tombstone);
        // Key is preserved in before field
        assert_eq!(tombstone.before, Some(json!({"id": 42})));
    }

    #[test]
    fn test_batch_processing() {
        let emitter = TombstoneEmitter::default();
        let events = vec![
            CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 0),
            CdcEvent::delete("pg", "db", "s", "t", json!({"id": 2}), 0),
            CdcEvent::update("pg", "db", "s", "t", None, json!({"id": 3}), 0),
        ];

        let result = emitter.process_batch(events);

        // Insert(1) + Delete(1) + Tombstone(1) + Update(1) = 4
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].op, CdcOp::Insert);
        assert_eq!(result[1].op, CdcOp::Delete);
        assert_eq!(result[2].op, CdcOp::Tombstone);
        assert_eq!(result[3].op, CdcOp::Update);
    }

    #[test]
    fn test_is_excluded() {
        let config = TombstoneConfig::builder()
            .exclude_tables(vec!["audit.*".to_string(), "logs.system".to_string()])
            .build();

        assert!(config.is_excluded("audit", "log"));
        assert!(config.is_excluded("audit", "events"));
        assert!(config.is_excluded("logs", "system"));
        assert!(!config.is_excluded("logs", "app"));
        assert!(!config.is_excluded("public", "users"));
    }
}
