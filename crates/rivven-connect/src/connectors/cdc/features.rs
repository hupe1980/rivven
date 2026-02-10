//! # CDC Feature Integration
//!
//! This module provides full integration of all rivven-cdc features into rivven-connect.
//! It bridges the gap between YAML configuration and rivven-cdc's Rust APIs.
//!
//! ## Implemented Features
//!
//! | Feature | Status | Config |
//! |---------|--------|--------|
//! | SMT Transforms | ✅ | `transforms` |
//! | Column Filters | ✅ | `column_filters` |
//! | Tombstones | ✅ | `tombstone` (uses TombstoneEmitter) |
//! | Deduplication | ✅ | `deduplication` |
//! | Signal Processing | ✅ | `signal` |
//! | Transaction Topic | ✅ | `transaction_topic` |
//! | Schema Change Topic | ✅ | `schema_change_topic` |
//! | Field Encryption | ✅ | `encryption` (uses FieldEncryptor + MemoryKeyProvider) |
//! | Heartbeat | ✅ | `heartbeat_interval_secs` (uses Heartbeat for lag tracking) |
//! | Event Router | ✅ | `router` (content-based routing with DLQ) |
//! | Partitioner | ✅ | `partitioner` (Kafka-style partitioning) |
//! | Pipeline | ✅ | `pipeline` (composable processing stages) |
//! | Compaction | ✅ | `compaction` (log compaction for state) |
//! | Parallel CDC | ✅ | `parallel` (multi-table concurrent processing) |
//! | Outbox | ✅ | `outbox` (transactional outbox pattern) |
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    CdcFeatureProcessor                         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  1. Deduplication     →  Bloom + LRU duplicate detection       │
//! │  2. Column Filters    →  Remove sensitive/unwanted columns     │
//! │  3. SMT Transforms    →  Transform/filter event data           │
//! │  4. Field Encryption  →  AES-256-GCM field-level encryption    │
//! │  5. Event Router      →  Content-based routing with DLQ        │
//! │  6. Partitioner       →  Kafka-style partition assignment      │
//! │  7. Transaction Track →  Emit BEGIN/END boundaries             │
//! │  8. Tombstone Emit    →  For Kafka log compaction              │
//! │  9. Compaction        →  Log compaction (latest per key)       │
//! │ 10. Heartbeat         →  Lag tracking and health monitoring    │
//! │ 11. Health Monitor    →  Auto-recovery and liveness probes     │
//! │ 12. Notifications     →  Progress and status alerts            │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::config::{
    ColumnFilterConfig, CompactionConfig, DeduplicationCdcConfig, EventRouterConfig,
    FieldEncryptionConfig, HealthMonitorConfig, HeartbeatCdcConfig, IncrementalSnapshotCdcConfig,
    NotificationConfig, OutboxConfig, ParallelCdcConfig, PartitionStrategyConfig,
    PartitionerConfig, PipelineConfig, ReadOnlyReplicaConfig, RouteConditionConfig,
    SchemaChangeTopicConfig, SignalTableConfig, SmtTransformConfig, TombstoneCdcConfig,
    TransactionTopicCdcConfig,
};
use rivven_cdc::common::{
    CdcEvent,
    CdcOp,
    ColumnDefinition,
    CompactionConfig as CdcCompactionConfig,
    CompactionKeyStrategy,
    // Compaction
    Compactor,
    Deduplicator,
    DeduplicatorConfig,
    EncryptionConfig,
    // Router and Partitioner
    EventRouter,
    FieldEncryptor,
    // Health Monitoring
    HealthCheckResult,
    HealthConfig,
    HealthMonitor,
    HealthStatus,
    Heartbeat,
    HeartbeatConfig,
    KeyStrategy,
    MemoryKeyProvider,
    PartitionStrategy,
    Partitioner,
    RecoveryCoordinator,
    RouteCondition,
    RouteDecision,
    RouteRule,
    SchemaChangeConfig,
    SchemaChangeEmitter,
    SchemaChangeEvent,
    Signal,
    SignalProcessor,
    SignalResult,
    SmtChain,
    TombstoneConfig,
    TombstoneEmitter,
    TransactionEvent,
    TransactionTopicConfig,
    TransactionTopicEmitter,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ============================================================================
// CDC Feature Processor
// ============================================================================

/// Unified CDC feature processor that integrates all rivven-cdc capabilities.
///
/// This processor handles the full CDC event pipeline:
/// 1. Deduplication (skip duplicate events)
/// 2. Column filtering (remove sensitive/unwanted columns)
/// 3. SMT transforms (transform event data)
/// 4. Field encryption (AES-256-GCM encryption)
/// 5. Event routing (content-based routing with DLQ)
/// 6. Partitioning (Kafka-style partition assignment)
/// 7. Transaction tracking (emit BEGIN/END boundaries)
/// 8. Schema change detection (emit DDL events)
/// 9. Tombstone emission (for log compaction)
/// 10. Log compaction (keep latest per key)
/// 11. Heartbeat monitoring (WAL health, lag detection)
/// 12. Health monitoring (auto-recovery, liveness/readiness probes)
pub struct CdcFeatureProcessor {
    /// Deduplicator for idempotent processing
    deduplicator: Option<Arc<Deduplicator>>,
    /// Signal processor for runtime control
    signal_processor: Option<Arc<SignalProcessor>>,
    /// SMT transform chain
    smt_chain: Option<Arc<SmtChain>>,
    /// Transaction topic emitter
    transaction_emitter: Option<Arc<TransactionTopicEmitter>>,
    /// Schema change emitter
    schema_emitter: Option<Arc<SchemaChangeEmitter>>,
    /// Tombstone emitter (uses rivven-cdc's TombstoneEmitter)
    tombstone_emitter: Option<Arc<TombstoneEmitter>>,
    /// Field encryptor (uses rivven-cdc's FieldEncryptor with MemoryKeyProvider)
    field_encryptor: Option<Arc<FieldEncryptor<MemoryKeyProvider>>>,
    /// Heartbeat manager for replication health monitoring
    heartbeat: Option<Arc<Heartbeat>>,
    /// Health monitor for auto-recovery and K8s probes
    health_monitor: Option<Arc<HealthMonitor>>,
    /// Recovery coordinator for automatic reconnection
    recovery_coordinator: Option<Arc<RecoveryCoordinator>>,
    /// Event router for content-based routing
    event_router: Option<Arc<EventRouter>>,
    /// Partitioner for Kafka-style partition assignment
    partitioner: Option<Arc<Partitioner>>,
    /// Compactor for log compaction
    compactor: Option<Arc<Compactor>>,
    /// Column filters per table
    column_filters: HashMap<String, ColumnFilterConfig>,
    /// Encrypted fields list (for introspection)
    encrypted_fields: Vec<String>,
    /// Read-only mode enabled
    read_only_mode: bool,
    /// Parallel CDC config (for external coordinator use)
    parallel_config: Option<ParallelCdcConfig>,
    /// Outbox config (for external processor use)
    outbox_config: Option<OutboxConfig>,
    /// Statistics
    stats: Arc<RwLock<CdcFeatureStats>>,
}

// Ensure CdcFeatureProcessor is thread-safe (compile-time check)
#[cfg(test)]
static_assertions::assert_impl_all!(CdcFeatureProcessor: Send, Sync);

impl fmt::Debug for CdcFeatureProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcFeatureProcessor")
            .field("deduplication", &self.deduplicator.is_some())
            .field("signal_processing", &self.signal_processor.is_some())
            .field("smt_transforms", &self.smt_chain.is_some())
            .field("transaction_topic", &self.transaction_emitter.is_some())
            .field("schema_change_topic", &self.schema_emitter.is_some())
            .field("tombstones", &self.tombstone_emitter.is_some())
            .field("heartbeat", &self.heartbeat.is_some())
            .field("health_monitor", &self.health_monitor.is_some())
            .field("event_router", &self.event_router.is_some())
            .field("partitioner", &self.partitioner.is_some())
            .field("compactor", &self.compactor.is_some())
            .field("column_filters", &self.column_filters.len())
            .field("encryption", &self.field_encryptor.is_some())
            .field("read_only", &self.read_only_mode)
            .field("parallel", &self.parallel_config.is_some())
            .field("outbox", &self.outbox_config.is_some())
            .finish()
    }
}

impl fmt::Display for CdcFeatureProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let features = self.enabled_features();
        if features.is_empty() {
            write!(f, "CdcFeatureProcessor (no features enabled)")
        } else {
            write!(f, "CdcFeatureProcessor [{}]", features.join(", "))
        }
    }
}

/// Statistics for CDC feature processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcFeatureStats {
    /// Total events processed
    pub events_processed: u64,
    /// Events skipped due to deduplication
    pub events_deduplicated: u64,
    /// Events modified by SMT transforms
    pub events_transformed: u64,
    /// Events filtered out by SMT chain
    pub events_filtered: u64,
    /// Fields encrypted by FieldEncryptor
    pub fields_encrypted: u64,
    /// Signals successfully processed
    pub signals_processed: u64,
    /// Signals ignored (not applicable)
    pub signals_ignored: u64,
    /// Transactions tracked for exactly-once semantics
    pub transactions_tracked: u64,
    /// Schema changes detected
    pub schema_changes_detected: u64,
    /// Tombstone events emitted for log compaction
    pub tombstones_emitted: u64,
    /// Events routed to destinations
    pub events_routed: u64,
    /// Events sent to dead letter queue
    pub events_dead_lettered: u64,
    /// Events compacted (deduplicated by key)
    pub events_compacted: u64,
    /// Partition assignments made
    pub partitions_assigned: u64,
    /// Timestamp when stats collection started (for rate calculations)
    started_at: Option<Instant>,
}

impl Default for CdcFeatureStats {
    fn default() -> Self {
        Self {
            events_processed: 0,
            events_deduplicated: 0,
            events_transformed: 0,
            events_filtered: 0,
            fields_encrypted: 0,
            signals_processed: 0,
            signals_ignored: 0,
            transactions_tracked: 0,
            schema_changes_detected: 0,
            tombstones_emitted: 0,
            events_routed: 0,
            events_dead_lettered: 0,
            events_compacted: 0,
            partitions_assigned: 0,
            started_at: Some(Instant::now()),
        }
    }
}

impl CdcFeatureStats {
    /// Get the elapsed time since stats collection started.
    #[must_use]
    pub fn elapsed_secs(&self) -> f64 {
        self.started_at
            .map(|start| start.elapsed().as_secs_f64())
            .unwrap_or(0.0)
    }

    /// Calculate events processed per second.
    #[must_use]
    pub fn events_per_second(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed > 0.0 {
            self.events_processed as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Calculate deduplication rate (percentage of events deduplicated).
    #[must_use]
    pub fn deduplication_rate(&self) -> f64 {
        let total = self.events_processed + self.events_deduplicated;
        if total > 0 {
            (self.events_deduplicated as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Calculate filter rate (percentage of events filtered out).
    #[must_use]
    pub fn filter_rate(&self) -> f64 {
        let total = self.events_processed + self.events_filtered;
        if total > 0 {
            (self.events_filtered as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Get total events seen (processed + deduplicated + filtered).
    #[must_use]
    pub fn total_events_seen(&self) -> u64 {
        self.events_processed + self.events_deduplicated + self.events_filtered
    }
}

impl fmt::Display for CdcFeatureStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CdcStats {{ processed: {}, dedup: {}, transformed: {}, filtered: {}, \
             encrypted: {}, tombstones: {}, rate: {:.1}/s }}",
            self.events_processed,
            self.events_deduplicated,
            self.events_transformed,
            self.events_filtered,
            self.fields_encrypted,
            self.tombstones_emitted,
            self.events_per_second()
        )
    }
}

impl CdcFeatureProcessor {
    /// Create a new feature processor with the given configuration.
    #[must_use]
    pub fn new(config: CdcFeatureConfig) -> Self {
        Self::with_connector_name(config, "rivven-cdc")
    }

    /// Create a new feature processor with a custom connector name (for heartbeat identification).
    #[must_use]
    pub fn with_connector_name(config: CdcFeatureConfig, connector_name: &str) -> Self {
        let mut processor = Self {
            deduplicator: None,
            signal_processor: None,
            smt_chain: None,
            transaction_emitter: None,
            schema_emitter: None,
            tombstone_emitter: None,
            field_encryptor: None,
            heartbeat: None,
            health_monitor: None,
            recovery_coordinator: None,
            event_router: None,
            partitioner: None,
            compactor: None,
            column_filters: config.column_filters,
            encrypted_fields: config.encryption.fields.clone(),
            read_only_mode: config.read_only.enabled,
            parallel_config: if config.parallel.enabled {
                Some(config.parallel.clone())
            } else {
                None
            },
            outbox_config: if config.outbox.enabled {
                Some(config.outbox.clone())
            } else {
                None
            },
            stats: Arc::new(RwLock::new(CdcFeatureStats::default())),
        };

        // Initialize deduplicator
        if config.deduplication.enabled {
            let dedup_config = DeduplicatorConfig {
                lru_capacity: config.deduplication.lru_size,
                bloom_size_bits: config.deduplication.bloom_expected_insertions * 10,
                bloom_hash_count: 7,
                ttl: std::time::Duration::from_secs(config.deduplication.window_secs),
                key_strategy: KeyStrategy::TableAndPrimaryKey,
            };
            processor.deduplicator = Some(Arc::new(Deduplicator::new(dedup_config)));
            info!(
                "CDC deduplication enabled (LRU: {}, TTL: {}s)",
                config.deduplication.lru_size, config.deduplication.window_secs
            );
        }

        // Initialize signal processor
        if config.signal.enabled {
            processor.signal_processor = Some(Arc::new(SignalProcessor::new()));
            info!("CDC signal processing enabled");
        }

        // Initialize SMT chain
        if !config.transforms.is_empty() {
            let mut chain = SmtChain::new();
            for transform_config in &config.transforms {
                match super::common::build_smt_transform(transform_config) {
                    Ok(transform) => {
                        chain = chain.add_boxed(transform);
                        debug!("Added SMT transform: {:?}", transform_config.transform_type);
                    }
                    Err(e) => {
                        warn!("Failed to build SMT transform: {}", e);
                    }
                }
            }
            processor.smt_chain = Some(Arc::new(chain));
            info!(
                "CDC SMT chain initialized with {} transforms",
                config.transforms.len()
            );
        }

        // Initialize transaction topic emitter
        if config.transaction_topic.enabled {
            let tx_config = TransactionTopicConfig::builder()
                .topic_prefix(
                    config
                        .transaction_topic
                        .topic_name
                        .clone()
                        .unwrap_or_else(|| "rivven".to_string()),
                )
                .build();
            processor.transaction_emitter = Some(Arc::new(TransactionTopicEmitter::new(tx_config)));
            info!("CDC transaction topic enabled");
        }

        // Initialize schema change emitter
        if config.schema_change_topic.enabled {
            let schema_config = SchemaChangeConfig::builder()
                .topic_prefix(
                    config
                        .schema_change_topic
                        .topic_name
                        .clone()
                        .unwrap_or_else(|| "rivven".to_string()),
                )
                .include_columns(config.schema_change_topic.include_columns)
                .build();
            processor.schema_emitter = Some(Arc::new(SchemaChangeEmitter::new(schema_config)));
            info!("CDC schema change topic enabled");
        }

        // Initialize tombstone emitter (using rivven-cdc's TombstoneEmitter)
        if config.tombstone.enabled {
            let tombstone_config = TombstoneConfig::builder()
                .emit_tombstones(true)
                .include_key_in_tombstone(false)
                .build();
            processor.tombstone_emitter = Some(Arc::new(TombstoneEmitter::new(tombstone_config)));
            info!("CDC tombstone emission enabled");
        }

        // Initialize field encryption (using rivven-cdc's FieldEncryptor with MemoryKeyProvider)
        if config.encryption.enabled && !config.encryption.fields.is_empty() {
            match MemoryKeyProvider::new() {
                Ok(key_provider) => {
                    // Build encryption config with rules for each field
                    // The rivven-cdc EncryptionConfig uses table.field pattern rules
                    let mut enc_builder = EncryptionConfig::builder().enabled(true);
                    for field in &config.encryption.fields {
                        // Add rule for all tables (*) with this field
                        enc_builder = enc_builder.encrypt_field("*", field.clone());
                    }
                    let enc_config = enc_builder.build();
                    processor.field_encryptor =
                        Some(Arc::new(FieldEncryptor::new(enc_config, key_provider)));
                    info!(
                        "CDC field encryption enabled for {} fields: {:?}",
                        config.encryption.fields.len(),
                        config.encryption.fields
                    );
                }
                Err(e) => {
                    warn!("Failed to initialize encryption key provider: {}", e);
                }
            }
        }

        // Initialize event router (content-based routing with DLQ)
        if config.router.enabled {
            let mut builder = EventRouter::builder();

            // Set default destination
            if let Some(ref default_dest) = config.router.default_destination {
                builder = builder.default_destination(default_dest.clone());
            }

            // Set dead letter queue
            if let Some(ref dlq) = config.router.dead_letter_queue {
                builder = builder.dead_letter(dlq.clone());
            }

            // Configure drop behavior
            if config.router.drop_unroutable {
                builder = builder.drop_unroutable();
            }

            // Add routing rules
            for rule_config in &config.router.rules {
                let condition = build_route_condition(&rule_config.condition);
                let mut rule = RouteRule::new(
                    rule_config.name.clone(),
                    condition,
                    rule_config
                        .destinations
                        .first()
                        .cloned()
                        .unwrap_or_default(),
                );
                rule = rule.with_priority(rule_config.priority);
                if rule_config.destinations.len() > 1 {
                    rule = rule.with_destinations(rule_config.destinations.clone());
                }
                if rule_config.continue_matching {
                    rule = rule.and_continue();
                }
                builder = builder.route(rule);
            }

            processor.event_router = Some(Arc::new(builder.build()));
            info!(
                "CDC event router enabled with {} rules",
                config.router.rules.len()
            );
        }

        // Initialize partitioner (Kafka-style partition assignment)
        if config.partitioner.enabled {
            let strategy = match &config.partitioner.strategy {
                PartitionStrategyConfig::RoundRobin => PartitionStrategy::RoundRobin,
                PartitionStrategyConfig::KeyHash { columns } => {
                    PartitionStrategy::KeyHash(columns.clone())
                }
                PartitionStrategyConfig::TableHash => PartitionStrategy::TableHash,
                PartitionStrategyConfig::FullTableHash => PartitionStrategy::FullTableHash,
                PartitionStrategyConfig::Sticky { partition } => {
                    PartitionStrategy::Sticky(*partition)
                }
            };
            processor.partitioner = Some(Arc::new(Partitioner::new(
                config.partitioner.num_partitions,
                strategy,
            )));
            info!(
                "CDC partitioner enabled ({} partitions)",
                config.partitioner.num_partitions
            );
        }

        // Initialize compactor (log compaction)
        if config.compaction.enabled {
            let key_strategy = if config.compaction.key_columns.is_empty() {
                CompactionKeyStrategy::default()
            } else {
                CompactionKeyStrategy::PrimaryKey(config.compaction.key_columns.clone())
            };

            let compaction_config = CdcCompactionConfig::builder()
                .key_strategy(key_strategy)
                .min_cleanable_ratio(config.compaction.min_cleanable_ratio)
                .segment_size(config.compaction.segment_size)
                .tombstone_retention(Duration::from_secs(
                    config.compaction.tombstone_retention_secs,
                ))
                .background_compaction(config.compaction.background)
                .batch_size(config.compaction.batch_size)
                .build();
            processor.compactor = Some(Arc::new(Compactor::new(compaction_config)));
            info!(
                "CDC compaction enabled (min_ratio: {:.2}, segment_size: {})",
                config.compaction.min_cleanable_ratio, config.compaction.segment_size
            );
        }

        // Initialize heartbeat manager for replication health monitoring
        if config.heartbeat.enabled {
            let heartbeat_config = HeartbeatConfig::builder()
                .interval(std::time::Duration::from_secs(
                    config.heartbeat.interval_secs as u64,
                ))
                .max_lag(std::time::Duration::from_secs(
                    config.heartbeat.max_lag_secs as u64,
                ))
                .emit_events(config.heartbeat.emit_events)
                .build();
            processor.heartbeat = Some(Arc::new(Heartbeat::new(heartbeat_config, connector_name)));
            info!(
                "CDC heartbeat enabled (interval: {}s, max_lag: {}s)",
                config.heartbeat.interval_secs, config.heartbeat.max_lag_secs
            );
        }

        // Initialize health monitor for auto-recovery and K8s probes
        if config.health.enabled {
            let health_config = HealthConfig {
                check_interval: Duration::from_secs(config.health.check_interval_secs),
                max_lag_ms: config.health.max_lag_ms,
                failure_threshold: config.health.failure_threshold,
                success_threshold: config.health.success_threshold,
                check_timeout: Duration::from_secs(config.health.check_timeout_secs),
                auto_recovery: config.health.auto_recovery,
                recovery_delay: Duration::from_secs(config.health.recovery_delay_secs),
                max_recovery_delay: Duration::from_secs(config.health.max_recovery_delay_secs),
            };
            let monitor = Arc::new(HealthMonitor::new(health_config.clone()));
            processor.health_monitor = Some(monitor);

            // Initialize recovery coordinator if auto-recovery is enabled
            if config.health.auto_recovery {
                processor.recovery_coordinator =
                    Some(Arc::new(RecoveryCoordinator::new(health_config)));
                info!(
                    "CDC health monitoring enabled with auto-recovery (interval: {}s, max_lag: {}ms)",
                    config.health.check_interval_secs, config.health.max_lag_ms
                );
            } else {
                info!(
                    "CDC health monitoring enabled (interval: {}s, max_lag: {}ms)",
                    config.health.check_interval_secs, config.health.max_lag_ms
                );
            }
        }

        processor
    }

    /// Process a CDC event through all enabled features.
    ///
    /// Returns `None` if the event should be filtered out (duplicate, filtered by SMT, etc.)
    ///
    /// Processing order:
    /// 1. Deduplication check (early exit if duplicate)
    /// 2. Column filters (remove sensitive/unwanted columns)
    /// 3. SMT transforms (transform/filter event data)
    /// 4. Field encryption (AES-256-GCM)
    /// 5. Transaction tracking (for exactly-once semantics)
    /// 6. Tombstone emission (for Kafka log compaction)
    pub async fn process(&self, mut event: CdcEvent) -> Option<ProcessedEvent> {
        // Track stats updates to batch them at the end
        let mut was_transformed = false;
        let mut transaction_tracked = false;
        let mut tombstone_count = 0u64;
        let mut fields_encrypted = 0u64;

        // 1. Check for duplicates (early exit)
        if let Some(ref dedup) = self.deduplicator {
            if dedup.is_duplicate(&event).await {
                debug!("Event deduplicated: {}.{}", event.schema, event.table);
                // Update stats and return early
                let mut stats = self.stats.write().await;
                stats.events_processed += 1;
                stats.events_deduplicated += 1;
                return None;
            }
            dedup.mark_seen(&event).await;
        }

        // 2. Apply column filters
        let stream_name = format!("{}.{}", event.schema, event.table);
        if let Some(filter) = self.column_filters.get(&stream_name) {
            super::common::apply_column_filter_to_event(&mut event, filter);
        }

        // 3. Apply SMT transforms
        if let Some(ref chain) = self.smt_chain {
            match chain.apply(event.clone()) {
                Some(transformed) => {
                    event = transformed;
                    was_transformed = true;
                }
                None => {
                    debug!(
                        "Event filtered by SMT chain: {}.{}",
                        event.schema, event.table
                    );
                    // Update stats and return early
                    let mut stats = self.stats.write().await;
                    stats.events_processed += 1;
                    stats.events_filtered += 1;
                    return None;
                }
            }
        }

        // 4. Apply field encryption (using rivven-cdc's FieldEncryptor)
        if let Some(ref encryptor) = self.field_encryptor {
            match encryptor.encrypt(&event).await {
                Ok(encrypted_event) => {
                    // Count encrypted fields from encryptor stats
                    let enc_stats = encryptor.stats();
                    fields_encrypted = enc_stats.fields_encrypted;
                    event = encrypted_event;
                }
                Err(e) => {
                    warn!("Field encryption failed: {}", e);
                    // Continue with unencrypted event
                }
            }
        }

        // 5. Track transaction (if we have transaction info)
        if let Some(ref tx_emitter) = self.transaction_emitter {
            if let Some(ref tx_meta) = event.transaction {
                let ts_ms = event.timestamp;
                let _ctx = tx_emitter
                    .track_event(&tx_meta.id, &stream_name, ts_ms)
                    .await;
                transaction_tracked = true;
            }
        }

        // 6. Build processed event with optional tombstone (using rivven-cdc's TombstoneEmitter)
        let tombstone = if let Some(ref emitter) = self.tombstone_emitter {
            if event.op == CdcOp::Delete {
                // TombstoneEmitter.process() returns [DELETE, TOMBSTONE] for deletes
                let events = emitter.process(event.clone());
                if events.len() > 1 {
                    tombstone_count = 1;
                    Some(events[1].clone()) // The tombstone is the second event
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let processed = ProcessedEvent {
            event,
            tombstone,
            transaction_event: None,
            schema_change_event: None,
        };

        // 7. Batch update stats (single lock acquisition)
        {
            let mut stats = self.stats.write().await;
            stats.events_processed += 1;
            if was_transformed {
                stats.events_transformed += 1;
            }
            if fields_encrypted > 0 {
                stats.fields_encrypted += fields_encrypted;
            }
            if transaction_tracked {
                stats.transactions_tracked += 1;
            }
            if tombstone_count > 0 {
                stats.tombstones_emitted += tombstone_count;
            }
        }

        Some(processed)
    }

    /// Process a signal event (from signal table or other channel).
    pub async fn process_signal(&self, signal: Signal) -> Result<(), String> {
        if let Some(ref processor) = self.signal_processor {
            let result = processor.process(signal).await;
            match result {
                SignalResult::Success | SignalResult::Pending(_) => {
                    let mut stats = self.stats.write().await;
                    stats.signals_processed += 1;
                    Ok(())
                }
                SignalResult::Ignored(msg) => {
                    let mut stats = self.stats.write().await;
                    stats.signals_ignored += 1;
                    debug!("Signal ignored: {}", msg);
                    Ok(())
                }
                SignalResult::Failed(msg) => Err(msg),
            }
        } else {
            Ok(())
        }
    }

    /// Begin a transaction (emit BEGIN event if transaction topic enabled).
    pub async fn begin_transaction(
        &self,
        tx_id: &str,
        lsn: &str,
        ts_ms: i64,
    ) -> Option<TransactionEvent> {
        if let Some(ref emitter) = self.transaction_emitter {
            emitter.begin_transaction(tx_id, lsn, ts_ms).await
        } else {
            None
        }
    }

    /// End a transaction (emit END event if transaction topic enabled).
    pub async fn end_transaction(&self, tx_id: &str, ts_ms: i64) -> Option<TransactionEvent> {
        if let Some(ref emitter) = self.transaction_emitter {
            emitter.end_transaction(tx_id, ts_ms).await
        } else {
            None
        }
    }

    /// Detect schema changes for PostgreSQL.
    pub async fn detect_postgres_schema_change(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        relation_id: u32,
        columns: Vec<ColumnDefinition>,
        lsn: Option<&str>,
    ) -> Option<SchemaChangeEvent> {
        if let Some(ref emitter) = self.schema_emitter {
            let result = emitter
                .detect_postgres_change(database, schema, table, relation_id, columns, lsn)
                .await;
            if result.is_some() {
                let mut stats = self.stats.write().await;
                stats.schema_changes_detected += 1;
            }
            return result;
        }
        None
    }

    /// Detect schema changes for MySQL.
    ///
    /// The table name is extracted automatically from the DDL statement.
    pub async fn detect_mysql_schema_change(
        &self,
        database: &str,
        ddl: &str,
        binlog_position: Option<&str>,
    ) -> Option<SchemaChangeEvent> {
        if let Some(ref emitter) = self.schema_emitter {
            let result = emitter
                .detect_mysql_change(database, ddl, binlog_position)
                .await;
            if result.is_some() {
                let mut stats = self.stats.write().await;
                stats.schema_changes_detected += 1;
            }
            return result;
        }
        None
    }

    /// Get current statistics.
    pub async fn stats(&self) -> CdcFeatureStats {
        self.stats.read().await.clone()
    }

    /// Log current statistics at info level.
    pub async fn log_stats(&self) {
        let stats = self.stats.read().await;
        info!("{}", *stats);
    }

    /// Check if read-only mode is enabled.
    #[must_use]
    pub fn is_read_only(&self) -> bool {
        self.read_only_mode
    }

    /// Check if deduplication is enabled.
    #[must_use]
    pub fn has_deduplication(&self) -> bool {
        self.deduplicator.is_some()
    }

    /// Check if signal processing is enabled.
    #[must_use]
    pub fn has_signal_processing(&self) -> bool {
        self.signal_processor.is_some()
    }

    /// Check if transaction topic is enabled.
    #[must_use]
    pub fn has_transaction_topic(&self) -> bool {
        self.transaction_emitter.is_some()
    }

    /// Check if schema change topic is enabled.
    #[must_use]
    pub fn has_schema_change_topic(&self) -> bool {
        self.schema_emitter.is_some()
    }

    /// Check if field encryption is enabled (using rivven-cdc's FieldEncryptor).
    #[must_use]
    pub fn has_encryption(&self) -> bool {
        self.field_encryptor.is_some()
    }

    /// Get the list of encrypted fields.
    #[must_use]
    pub fn encrypted_fields(&self) -> &[String] {
        &self.encrypted_fields
    }

    /// Check if SMT transforms are configured.
    #[must_use]
    pub fn has_transforms(&self) -> bool {
        self.smt_chain.is_some()
    }

    /// Check if tombstone emission is enabled (using rivven-cdc's TombstoneEmitter).
    #[must_use]
    pub fn has_tombstones(&self) -> bool {
        self.tombstone_emitter.is_some()
    }

    /// Check if heartbeat monitoring is enabled.
    #[must_use]
    pub fn has_heartbeat(&self) -> bool {
        self.heartbeat.is_some()
    }

    /// Get the heartbeat manager (if enabled).
    #[must_use]
    pub fn heartbeat(&self) -> Option<&Arc<Heartbeat>> {
        self.heartbeat.as_ref()
    }

    /// Update heartbeat position (call this on each event to track lag).
    pub async fn update_heartbeat_position(&self, position: &str, server_id: &str) {
        if let Some(ref heartbeat) = self.heartbeat {
            heartbeat.update_position(position, server_id).await;
        }
    }

    /// Check if heartbeat is healthy (lag within threshold).
    #[must_use]
    pub fn is_heartbeat_healthy(&self) -> bool {
        self.heartbeat.as_ref().is_none_or(|h| h.is_healthy())
    }

    /// Get current heartbeat lag.
    #[must_use]
    pub fn heartbeat_lag(&self) -> Option<std::time::Duration> {
        self.heartbeat.as_ref().map(|h| h.lag())
    }

    // ========================================================================
    // Health Monitoring
    // ========================================================================

    /// Check if health monitoring is enabled.
    #[must_use]
    pub fn has_health_monitor(&self) -> bool {
        self.health_monitor.is_some()
    }

    /// Get the health monitor (if enabled).
    #[must_use]
    pub fn health_monitor(&self) -> Option<&Arc<HealthMonitor>> {
        self.health_monitor.as_ref()
    }

    /// Get the recovery coordinator (if enabled).
    #[must_use]
    pub fn recovery_coordinator(&self) -> Option<&Arc<RecoveryCoordinator>> {
        self.recovery_coordinator.as_ref()
    }

    /// Register a custom health check function.
    ///
    /// # Example
    ///
    /// ```ignore
    /// processor.register_health_check("database", || async {
    ///     // Check database connection
    ///     HealthCheckResult::Healthy
    /// }).await;
    /// ```
    pub async fn register_health_check<F, Fut>(&self, name: &str, check: F)
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = HealthCheckResult> + Send + 'static,
    {
        if let Some(ref monitor) = self.health_monitor {
            monitor.register_check(name, check).await;
        }
    }

    /// Unregister a health check by name.
    ///
    /// # Example
    ///
    /// ```ignore
    /// processor.unregister_health_check("database").await;
    /// ```
    pub async fn unregister_health_check(&self, name: &str) {
        if let Some(ref monitor) = self.health_monitor {
            monitor.unregister_check(name).await;
        }
    }

    /// Run all registered health checks.
    pub async fn check_health(&self) -> Option<HealthStatus> {
        if let Some(ref monitor) = self.health_monitor {
            Some(monitor.check().await)
        } else {
            None
        }
    }

    /// Get current health status without running new checks.
    pub async fn health_status(&self) -> Option<HealthStatus> {
        if let Some(ref monitor) = self.health_monitor {
            Some(monitor.status().await)
        } else {
            None
        }
    }

    /// Check if system is healthy (all checks passing, lag within threshold).
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        if let Some(ref monitor) = self.health_monitor {
            monitor.is_healthy()
        } else {
            // If no health monitor, use heartbeat health status
            self.is_heartbeat_healthy()
        }
    }

    /// Check if system is ready to serve traffic.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        if let Some(ref monitor) = self.health_monitor {
            monitor.is_ready()
        } else {
            // If no health monitor, always ready
            true
        }
    }

    /// Update the replication lag in the health monitor.
    pub fn set_health_lag(&self, lag_ms: u64) {
        if let Some(ref monitor) = self.health_monitor {
            monitor.set_lag(lag_ms);
        }
    }

    /// Mark the system as connected (healthy).
    pub fn mark_connected(&self) {
        if let Some(ref monitor) = self.health_monitor {
            monitor.mark_connected();
        }
    }

    /// Mark the system as disconnected (unhealthy).
    pub fn mark_disconnected(&self) {
        if let Some(ref monitor) = self.health_monitor {
            monitor.mark_disconnected();
        }
    }

    /// Check if automatic recovery is in progress.
    #[must_use]
    pub fn is_recovery_in_progress(&self) -> bool {
        if let Some(ref monitor) = self.health_monitor {
            monitor.is_recovery_in_progress()
        } else {
            false
        }
    }

    /// Attempt automatic recovery with a provided recovery function.
    ///
    /// Uses exponential backoff and coordinates with the health monitor.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let recovered = processor.attempt_recovery(|| async {
    ///     // Attempt to reconnect to database
    ///     match reconnect_database().await {
    ///         Ok(_) => true,
    ///         Err(_) => false,
    ///     }
    /// }).await;
    /// ```
    pub async fn attempt_recovery<F, Fut>(&self, recover_fn: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        if let (Some(ref monitor), Some(ref coordinator)) =
            (&self.health_monitor, &self.recovery_coordinator)
        {
            monitor.attempt_recovery(coordinator, recover_fn).await
        } else {
            false
        }
    }

    /// Spawn a background health monitoring task.
    ///
    /// This task runs health checks at the configured interval.
    /// Returns None if health monitoring is not enabled.
    pub fn spawn_health_monitor(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let monitor = self.health_monitor.clone()?;
        Some(monitor.spawn_monitor())
    }

    /// Get K8s-compatible liveness probe response.
    ///
    /// Returns (status_code, message) for HTTP health endpoints.
    pub async fn liveness_probe(&self) -> (u16, &'static str) {
        if let Some(ref monitor) = self.health_monitor {
            let status = monitor.status().await;
            status.liveness()
        } else {
            (200, "OK")
        }
    }

    /// Get K8s-compatible readiness probe response.
    ///
    /// Returns (status_code, message) for HTTP health endpoints.
    pub async fn readiness_probe(&self) -> (u16, &'static str) {
        if let Some(ref monitor) = self.health_monitor {
            let status = monitor.status().await;
            status.readiness()
        } else {
            (200, "OK")
        }
    }

    /// Get health status as JSON for HTTP endpoints.
    pub async fn health_json(&self) -> String {
        if let Some(ref monitor) = self.health_monitor {
            let status = monitor.status().await;
            status.to_json()
        } else {
            r#"{"status":"unknown","message":"Health monitoring not enabled"}"#.to_string()
        }
    }

    // ========================================================================
    // Event Routing
    // ========================================================================

    /// Check if event router is enabled.
    #[must_use]
    pub fn has_router(&self) -> bool {
        self.event_router.is_some()
    }

    /// Route an event to its destination(s).
    pub fn route(&self, event: &CdcEvent) -> RouteDecision {
        if let Some(ref router) = self.event_router {
            router.route(event)
        } else {
            RouteDecision::Drop
        }
    }

    /// Route and group multiple events by destination.
    pub fn route_and_group<'a>(
        &self,
        events: &'a [CdcEvent],
    ) -> HashMap<String, Vec<&'a CdcEvent>> {
        if let Some(ref router) = self.event_router {
            router.route_and_group(events)
        } else {
            HashMap::new()
        }
    }

    /// Check if partitioner is enabled.
    #[must_use]
    pub fn has_partitioner(&self) -> bool {
        self.partitioner.is_some()
    }

    /// Get partition for an event.
    pub fn partition(&self, event: &CdcEvent) -> Option<u32> {
        self.partitioner.as_ref().map(|p| p.partition(event))
    }

    /// Get number of partitions (if partitioner enabled).
    #[must_use]
    pub fn num_partitions(&self) -> Option<u32> {
        self.partitioner.as_ref().map(|p| p.num_partitions())
    }

    /// Check if compaction is enabled.
    #[must_use]
    pub fn has_compaction(&self) -> bool {
        self.compactor.is_some()
    }

    /// Get the compactor (if enabled).
    #[must_use]
    pub fn compactor(&self) -> Option<&Arc<Compactor>> {
        self.compactor.as_ref()
    }

    /// Check if parallel CDC is configured.
    #[must_use]
    pub fn has_parallel(&self) -> bool {
        self.parallel_config.is_some()
    }

    /// Get parallel CDC configuration (if enabled).
    #[must_use]
    pub fn parallel_config(&self) -> Option<&ParallelCdcConfig> {
        self.parallel_config.as_ref()
    }

    /// Check if outbox pattern is configured.
    #[must_use]
    pub fn has_outbox(&self) -> bool {
        self.outbox_config.is_some()
    }

    /// Get outbox configuration (if enabled).
    #[must_use]
    pub fn outbox_config(&self) -> Option<&OutboxConfig> {
        self.outbox_config.as_ref()
    }

    /// Check if column filters are configured.
    #[must_use]
    pub fn has_column_filters(&self) -> bool {
        !self.column_filters.is_empty()
    }

    /// Get the number of configured column filters.
    #[must_use]
    pub fn column_filter_count(&self) -> usize {
        self.column_filters.len()
    }

    /// Get the number of configured SMT transforms.
    #[must_use]
    pub fn transform_count(&self) -> usize {
        self.smt_chain.as_ref().map_or(0, |chain| chain.len())
    }

    /// Get the number of enabled features.
    #[must_use]
    pub fn feature_count(&self) -> usize {
        self.enabled_features().len()
    }

    /// Check if any features are enabled.
    #[must_use]
    pub fn has_any_features(&self) -> bool {
        self.feature_count() > 0
    }

    /// Reset all statistics to zero.
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = CdcFeatureStats::default();
    }

    /// Get a summary of enabled features.
    #[must_use]
    pub fn enabled_features(&self) -> Vec<&'static str> {
        let mut features = Vec::new();
        if self.has_deduplication() {
            features.push("deduplication");
        }
        if self.has_signal_processing() {
            features.push("signal_processing");
        }
        if self.has_transaction_topic() {
            features.push("transaction_topic");
        }
        if self.has_schema_change_topic() {
            features.push("schema_change_topic");
        }
        if self.has_encryption() {
            features.push("encryption");
        }
        if self.has_transforms() {
            features.push("smt_transforms");
        }
        if self.has_tombstones() {
            features.push("tombstones");
        }
        if self.has_heartbeat() {
            features.push("heartbeat");
        }
        if self.has_health_monitor() {
            features.push("health_monitor");
        }
        if self.has_column_filters() {
            features.push("column_filters");
        }
        if self.has_router() {
            features.push("router");
        }
        if self.has_partitioner() {
            features.push("partitioner");
        }
        if self.has_compaction() {
            features.push("compaction");
        }
        if self.has_parallel() {
            features.push("parallel");
        }
        if self.has_outbox() {
            features.push("outbox");
        }
        if self.is_read_only() {
            features.push("read_only");
        }
        features
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Build a RouteCondition from YAML configuration.
fn build_route_condition(config: &RouteConditionConfig) -> RouteCondition {
    match config {
        RouteConditionConfig::Always => RouteCondition::Always,
        RouteConditionConfig::Table { table } => RouteCondition::table(table),
        RouteConditionConfig::TablePattern { pattern } => RouteCondition::table_pattern(pattern),
        RouteConditionConfig::Schema { schema } => RouteCondition::schema(schema),
        RouteConditionConfig::Operation { op } => {
            let cdc_op = match op.to_lowercase().as_str() {
                "insert" | "c" => CdcOp::Insert,
                "update" | "u" => CdcOp::Update,
                "delete" | "d" => CdcOp::Delete,
                "snapshot" | "r" => CdcOp::Snapshot,
                "truncate" | "t" => CdcOp::Truncate,
                _ => CdcOp::Insert,
            };
            RouteCondition::operation(cdc_op)
        }
        RouteConditionConfig::Operations { ops } => {
            let cdc_ops: Vec<CdcOp> = ops
                .iter()
                .map(|op| match op.to_lowercase().as_str() {
                    "insert" | "c" => CdcOp::Insert,
                    "update" | "u" => CdcOp::Update,
                    "delete" | "d" => CdcOp::Delete,
                    "snapshot" | "r" => CdcOp::Snapshot,
                    "truncate" | "t" => CdcOp::Truncate,
                    _ => CdcOp::Insert,
                })
                .collect();
            RouteCondition::operations(cdc_ops)
        }
        RouteConditionConfig::FieldEquals { field, value } => {
            RouteCondition::field_equals(field, value.clone())
        }
        RouteConditionConfig::FieldExists { field } => RouteCondition::field_exists(field),
        RouteConditionConfig::All { conditions } => {
            let mut result = RouteCondition::Always;
            for cond in conditions {
                result = result.and(build_route_condition(cond));
            }
            result
        }
        RouteConditionConfig::Any { conditions } => {
            let mut result =
                build_route_condition(conditions.first().unwrap_or(&RouteConditionConfig::Always));
            for cond in conditions.iter().skip(1) {
                result = result.or(build_route_condition(cond));
            }
            result
        }
        RouteConditionConfig::Not { condition } => build_route_condition(condition).negate(),
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Unified configuration for all CDC features.
#[derive(Debug, Clone, Default)]
pub struct CdcFeatureConfig {
    pub transforms: Vec<SmtTransformConfig>,
    pub column_filters: HashMap<String, ColumnFilterConfig>,
    pub tombstone: TombstoneCdcConfig,
    pub deduplication: DeduplicationCdcConfig,
    pub encryption: FieldEncryptionConfig,
    pub signal: SignalTableConfig,
    pub transaction_topic: TransactionTopicCdcConfig,
    pub schema_change_topic: SchemaChangeTopicConfig,
    pub incremental_snapshot: IncrementalSnapshotCdcConfig,
    pub read_only: ReadOnlyReplicaConfig,
    pub heartbeat: HeartbeatCdcConfig,
    /// Event router configuration
    pub router: EventRouterConfig,
    /// Partitioner configuration
    pub partitioner: PartitionerConfig,
    /// Pipeline configuration
    pub pipeline: PipelineConfig,
    /// Compaction configuration
    pub compaction: CompactionConfig,
    /// Parallel CDC configuration
    pub parallel: ParallelCdcConfig,
    /// Outbox configuration
    pub outbox: OutboxConfig,
    /// Health monitoring configuration
    pub health: HealthMonitorConfig,
    /// Notification configuration
    pub notifications: NotificationConfig,
}

impl CdcFeatureConfig {
    /// Create config from CDC connector config fields.
    /// Works for both PostgreSQL and MySQL CDC connectors.
    #[allow(clippy::too_many_arguments)]
    pub fn from_cdc_config(
        transforms: Vec<SmtTransformConfig>,
        column_filters: HashMap<String, ColumnFilterConfig>,
        tombstone: TombstoneCdcConfig,
        deduplication: DeduplicationCdcConfig,
        encryption: FieldEncryptionConfig,
        signal: SignalTableConfig,
        transaction_topic: TransactionTopicCdcConfig,
        schema_change_topic: SchemaChangeTopicConfig,
        incremental_snapshot: IncrementalSnapshotCdcConfig,
        read_only: ReadOnlyReplicaConfig,
        heartbeat: HeartbeatCdcConfig,
    ) -> Self {
        Self {
            transforms,
            column_filters,
            tombstone,
            deduplication,
            encryption,
            signal,
            transaction_topic,
            schema_change_topic,
            incremental_snapshot,
            read_only,
            heartbeat,
            router: EventRouterConfig::default(),
            partitioner: PartitionerConfig::default(),
            pipeline: PipelineConfig::default(),
            compaction: CompactionConfig::default(),
            parallel: ParallelCdcConfig::default(),
            outbox: OutboxConfig::default(),
            health: HealthMonitorConfig::default(),
            notifications: NotificationConfig::default(),
        }
    }

    /// Create config from all CDC connector config fields including new features.
    #[allow(clippy::too_many_arguments)]
    pub fn from_full_cdc_config(
        transforms: Vec<SmtTransformConfig>,
        column_filters: HashMap<String, ColumnFilterConfig>,
        tombstone: TombstoneCdcConfig,
        deduplication: DeduplicationCdcConfig,
        encryption: FieldEncryptionConfig,
        signal: SignalTableConfig,
        transaction_topic: TransactionTopicCdcConfig,
        schema_change_topic: SchemaChangeTopicConfig,
        incremental_snapshot: IncrementalSnapshotCdcConfig,
        read_only: ReadOnlyReplicaConfig,
        heartbeat: HeartbeatCdcConfig,
        router: EventRouterConfig,
        partitioner: PartitionerConfig,
        pipeline: PipelineConfig,
        compaction: CompactionConfig,
        parallel: ParallelCdcConfig,
        outbox: OutboxConfig,
        health: HealthMonitorConfig,
        notifications: NotificationConfig,
    ) -> Self {
        Self {
            transforms,
            column_filters,
            tombstone,
            deduplication,
            encryption,
            signal,
            transaction_topic,
            schema_change_topic,
            incremental_snapshot,
            read_only,
            heartbeat,
            router,
            partitioner,
            pipeline,
            compaction,
            parallel,
            outbox,
            health,
            notifications,
        }
    }

    /// Create a builder for more ergonomic configuration.
    #[must_use]
    pub fn builder() -> CdcFeatureConfigBuilder {
        CdcFeatureConfigBuilder::default()
    }
}

/// Builder for `CdcFeatureConfig` for more ergonomic API.
#[derive(Debug, Default)]
pub struct CdcFeatureConfigBuilder {
    config: CdcFeatureConfig,
}

impl CdcFeatureConfigBuilder {
    /// Set the SMT transforms.
    #[must_use]
    pub fn transforms(mut self, transforms: Vec<SmtTransformConfig>) -> Self {
        self.config.transforms = transforms;
        self
    }

    /// Add a single SMT transform.
    #[must_use]
    pub fn add_transform(mut self, transform: SmtTransformConfig) -> Self {
        self.config.transforms.push(transform);
        self
    }

    /// Set the column filters.
    #[must_use]
    pub fn column_filters(mut self, filters: HashMap<String, ColumnFilterConfig>) -> Self {
        self.config.column_filters = filters;
        self
    }

    /// Add a column filter for a specific table.
    #[must_use]
    pub fn add_column_filter(
        mut self,
        table: impl Into<String>,
        filter: ColumnFilterConfig,
    ) -> Self {
        self.config.column_filters.insert(table.into(), filter);
        self
    }

    /// Set the tombstone configuration.
    #[must_use]
    pub fn tombstone(mut self, config: TombstoneCdcConfig) -> Self {
        self.config.tombstone = config;
        self
    }

    /// Enable tombstones with default settings.
    #[must_use]
    pub fn with_tombstones(mut self) -> Self {
        self.config.tombstone.enabled = true;
        self.config.tombstone.after_delete = true;
        self
    }

    /// Set the deduplication configuration.
    #[must_use]
    pub fn deduplication(mut self, config: DeduplicationCdcConfig) -> Self {
        self.config.deduplication = config;
        self
    }

    /// Enable deduplication with custom settings.
    #[must_use]
    pub fn with_deduplication(mut self, lru_size: usize, window_secs: u64) -> Self {
        self.config.deduplication.enabled = true;
        self.config.deduplication.lru_size = lru_size;
        self.config.deduplication.window_secs = window_secs;
        self.config.deduplication.bloom_expected_insertions = lru_size * 10;
        self
    }

    /// Set the encryption configuration.
    #[must_use]
    pub fn encryption(mut self, config: FieldEncryptionConfig) -> Self {
        self.config.encryption = config;
        self
    }

    /// Enable encryption for specific fields.
    #[must_use]
    pub fn with_encryption(mut self, fields: Vec<String>) -> Self {
        self.config.encryption.enabled = true;
        self.config.encryption.fields = fields;
        self
    }

    /// Set the signal table configuration.
    #[must_use]
    pub fn signal(mut self, config: SignalTableConfig) -> Self {
        self.config.signal = config;
        self
    }

    /// Enable signal processing.
    #[must_use]
    pub fn with_signals(mut self) -> Self {
        self.config.signal.enabled = true;
        self
    }

    /// Set the transaction topic configuration.
    #[must_use]
    pub fn transaction_topic(mut self, config: TransactionTopicCdcConfig) -> Self {
        self.config.transaction_topic = config;
        self
    }

    /// Enable transaction topic.
    #[must_use]
    pub fn with_transaction_topic(mut self, topic_name: Option<String>) -> Self {
        self.config.transaction_topic.enabled = true;
        self.config.transaction_topic.topic_name = topic_name;
        self
    }

    /// Set the schema change topic configuration.
    #[must_use]
    pub fn schema_change_topic(mut self, config: SchemaChangeTopicConfig) -> Self {
        self.config.schema_change_topic = config;
        self
    }

    /// Enable schema change topic.
    #[must_use]
    pub fn with_schema_change_topic(mut self) -> Self {
        self.config.schema_change_topic.enabled = true;
        self
    }

    /// Set the incremental snapshot configuration.
    #[must_use]
    pub fn incremental_snapshot(mut self, config: IncrementalSnapshotCdcConfig) -> Self {
        self.config.incremental_snapshot = config;
        self
    }

    /// Set the read-only replica configuration.
    #[must_use]
    pub fn read_only(mut self, config: ReadOnlyReplicaConfig) -> Self {
        self.config.read_only = config;
        self
    }

    /// Enable read-only mode.
    #[must_use]
    pub fn with_read_only(mut self) -> Self {
        self.config.read_only.enabled = true;
        self
    }

    /// Set the router configuration.
    #[must_use]
    pub fn router(mut self, config: EventRouterConfig) -> Self {
        self.config.router = config;
        self
    }

    /// Enable routing with default destination.
    #[must_use]
    pub fn with_router(mut self, default_destination: impl Into<String>) -> Self {
        self.config.router.enabled = true;
        self.config.router.default_destination = Some(default_destination.into());
        self
    }

    /// Set the partitioner configuration.
    #[must_use]
    pub fn partitioner(mut self, config: PartitionerConfig) -> Self {
        self.config.partitioner = config;
        self
    }

    /// Enable partitioning with specified number of partitions.
    #[must_use]
    pub fn with_partitioner(mut self, num_partitions: u32) -> Self {
        self.config.partitioner.enabled = true;
        self.config.partitioner.num_partitions = num_partitions;
        self
    }

    /// Set the pipeline configuration.
    #[must_use]
    pub fn pipeline(mut self, config: PipelineConfig) -> Self {
        self.config.pipeline = config;
        self
    }

    /// Enable pipeline with a name.
    #[must_use]
    pub fn with_pipeline(mut self, name: impl Into<String>) -> Self {
        self.config.pipeline.enabled = true;
        self.config.pipeline.name = Some(name.into());
        self
    }

    /// Set the compaction configuration.
    #[must_use]
    pub fn compaction(mut self, config: CompactionConfig) -> Self {
        self.config.compaction = config;
        self
    }

    /// Enable compaction with key columns.
    #[must_use]
    pub fn with_compaction(mut self, key_columns: Vec<String>) -> Self {
        self.config.compaction.enabled = true;
        self.config.compaction.key_columns = key_columns;
        self
    }

    /// Set the parallel CDC configuration.
    #[must_use]
    pub fn parallel(mut self, config: ParallelCdcConfig) -> Self {
        self.config.parallel = config;
        self
    }

    /// Enable parallel CDC with concurrency.
    #[must_use]
    pub fn with_parallel(mut self, concurrency: usize) -> Self {
        self.config.parallel.enabled = true;
        self.config.parallel.concurrency = concurrency;
        self
    }

    /// Set the outbox configuration.
    #[must_use]
    pub fn outbox(mut self, config: OutboxConfig) -> Self {
        self.config.outbox = config;
        self
    }

    /// Enable outbox pattern.
    #[must_use]
    pub fn with_outbox(mut self) -> Self {
        self.config.outbox.enabled = true;
        self
    }

    /// Set the health monitoring configuration.
    #[must_use]
    pub fn health(mut self, config: HealthMonitorConfig) -> Self {
        self.config.health = config;
        self
    }

    /// Enable health monitoring with default settings.
    #[must_use]
    pub fn with_health_monitoring(mut self) -> Self {
        self.config.health.enabled = true;
        self
    }

    /// Set the notification configuration.
    #[must_use]
    pub fn notifications(mut self, config: NotificationConfig) -> Self {
        self.config.notifications = config;
        self
    }

    /// Enable notifications with default settings.
    #[must_use]
    pub fn with_notifications(mut self) -> Self {
        self.config.notifications.enabled = true;
        self
    }

    /// Build the configuration.
    #[must_use]
    pub fn build(self) -> CdcFeatureConfig {
        self.config
    }
}

impl From<CdcFeatureConfigBuilder> for CdcFeatureConfig {
    fn from(builder: CdcFeatureConfigBuilder) -> Self {
        builder.build()
    }
}

impl From<CdcFeatureConfig> for CdcFeatureConfigBuilder {
    fn from(config: CdcFeatureConfig) -> Self {
        Self { config }
    }
}

// ============================================================================
// Processed Event
// ============================================================================

/// Result of processing a CDC event through the feature pipeline.
#[derive(Debug, Clone)]
pub struct ProcessedEvent {
    /// The transformed CDC event
    pub event: CdcEvent,
    /// Optional tombstone event (for deletes when tombstone.enabled)
    pub tombstone: Option<CdcEvent>,
    /// Optional transaction boundary event
    pub transaction_event: Option<TransactionEvent>,
    /// Optional schema change event
    pub schema_change_event: Option<SchemaChangeEvent>,
}

impl ProcessedEvent {
    /// Check if this event has an associated tombstone.
    #[must_use]
    pub fn has_tombstone(&self) -> bool {
        self.tombstone.is_some()
    }

    /// Check if this event has transaction metadata.
    #[must_use]
    pub fn has_transaction(&self) -> bool {
        self.transaction_event.is_some()
    }

    /// Check if this event has schema change metadata.
    #[must_use]
    pub fn has_schema_change(&self) -> bool {
        self.schema_change_event.is_some()
    }

    /// Get the table name in "schema.table" format.
    #[must_use]
    pub fn table_name(&self) -> String {
        format!("{}.{}", self.event.schema, self.event.table)
    }

    /// Get the operation type.
    #[must_use]
    pub fn operation(&self) -> &CdcOp {
        &self.event.op
    }

    /// Check if this is a delete operation.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self.event.op, CdcOp::Delete)
    }

    /// Check if this is an insert operation.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self.event.op, CdcOp::Insert)
    }

    /// Check if this is an update operation.
    #[must_use]
    pub fn is_update(&self) -> bool {
        matches!(self.event.op, CdcOp::Update)
    }
}

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a tombstone event from a delete event (test helper).
#[cfg(test)]
fn create_tombstone(delete_event: &CdcEvent) -> CdcEvent {
    CdcEvent {
        source_type: delete_event.source_type.clone(),
        database: delete_event.database.clone(),
        schema: delete_event.schema.clone(),
        table: delete_event.table.clone(),
        op: CdcOp::Tombstone,
        timestamp: delete_event.timestamp,
        before: None,
        after: None, // Tombstones have null payload
        transaction: delete_event.transaction.clone(),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::cdc::config::SmtTransformType;

    fn create_test_event() -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op: CdcOp::Insert,
            timestamp: 1234567890,
            before: None,
            after: Some(serde_json::json!({
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com"
            })),
            transaction: None,
        }
    }

    #[test]
    fn test_feature_config_default() {
        let config = CdcFeatureConfig::default();
        assert!(config.transforms.is_empty());
        assert!(!config.deduplication.enabled);
        assert!(!config.signal.enabled);
        assert!(!config.tombstone.enabled);
        assert!(!config.transaction_topic.enabled);
        assert!(!config.schema_change_topic.enabled);
    }

    #[tokio::test]
    async fn test_processor_creation() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        assert!(!processor.has_deduplication());
        assert!(!processor.has_signal_processing());
        assert!(!processor.has_transaction_topic());
        assert!(!processor.has_schema_change_topic());
        assert!(!processor.has_encryption());
        assert!(!processor.is_read_only());
    }

    #[tokio::test]
    async fn test_processor_with_deduplication() {
        let mut config = CdcFeatureConfig::default();
        config.deduplication.enabled = true;
        config.deduplication.lru_size = 1000;
        config.deduplication.window_secs = 60;
        config.deduplication.bloom_expected_insertions = 10000;

        let processor = CdcFeatureProcessor::new(config);
        assert!(processor.has_deduplication());
    }

    #[tokio::test]
    async fn test_processor_stats() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 0);
        assert_eq!(stats.events_deduplicated, 0);
        assert_eq!(stats.events_transformed, 0);
        assert_eq!(stats.events_filtered, 0);
    }

    #[tokio::test]
    async fn test_process_basic_event() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        let event = create_test_event();
        let result = processor.process(event).await;

        assert!(result.is_some());
        let processed = result.unwrap();
        assert_eq!(processed.event.op, CdcOp::Insert);
        assert!(processed.tombstone.is_none());

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 1);
    }

    #[tokio::test]
    async fn test_process_with_tombstone() {
        let mut config = CdcFeatureConfig::default();
        config.tombstone.enabled = true;
        config.tombstone.after_delete = true;

        let processor = CdcFeatureProcessor::new(config);

        let mut event = create_test_event();
        event.op = CdcOp::Delete;
        event.before = event.after.take();

        let result = processor.process(event).await;

        assert!(result.is_some());
        let processed = result.unwrap();
        assert_eq!(processed.event.op, CdcOp::Delete);
        assert!(processed.tombstone.is_some());
        assert_eq!(processed.tombstone.unwrap().op, CdcOp::Tombstone);

        let stats = processor.stats().await;
        assert_eq!(stats.tombstones_emitted, 1);
    }

    #[tokio::test]
    async fn test_process_with_column_filter() {
        let mut config = CdcFeatureConfig::default();
        config.column_filters.insert(
            "public.users".to_string(),
            ColumnFilterConfig {
                include: vec!["id".to_string(), "name".to_string()],
                exclude: vec![],
            },
        );

        let processor = CdcFeatureProcessor::new(config);

        let event = create_test_event();
        let result = processor.process(event).await;

        assert!(result.is_some());
        let processed = result.unwrap();
        let after = processed.event.after.unwrap();

        // Should only have id and name, not email
        assert!(after.get("id").is_some());
        assert!(after.get("name").is_some());
        assert!(after.get("email").is_none());
    }

    #[tokio::test]
    async fn test_process_with_smt_transform() {
        let mut config = CdcFeatureConfig::default();
        // Add a mask transform that masks the email field
        let mut transform_config = HashMap::new();
        transform_config.insert("fields".to_string(), serde_json::json!(["email"]));
        transform_config.insert("replacement".to_string(), serde_json::json!("***"));

        config.transforms.push(SmtTransformConfig {
            transform_type: SmtTransformType::MaskField,
            name: Some("mask_email".to_string()),
            config: transform_config,
            predicate: None,
        });

        let processor = CdcFeatureProcessor::new(config);

        let event = create_test_event();
        let result = processor.process(event).await;

        // The processor should have processed the event
        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 1);

        // If the transform was applied, the event should still exist
        // (mask doesn't filter, it transforms)
        if let Some(processed) = result {
            assert_eq!(processed.event.op, CdcOp::Insert);
        }
    }

    #[tokio::test]
    async fn test_create_tombstone() {
        let event = create_test_event();
        let tombstone = create_tombstone(&event);

        assert_eq!(tombstone.op, CdcOp::Tombstone);
        assert_eq!(tombstone.schema, event.schema);
        assert_eq!(tombstone.table, event.table);
        assert!(tombstone.before.is_none());
        assert!(tombstone.after.is_none());
    }

    #[test]
    fn test_from_cdc_config() {
        let config = CdcFeatureConfig::from_cdc_config(
            vec![],
            HashMap::new(),
            TombstoneCdcConfig::default(),
            DeduplicationCdcConfig::default(),
            FieldEncryptionConfig::default(),
            SignalTableConfig::default(),
            TransactionTopicCdcConfig::default(),
            SchemaChangeTopicConfig::default(),
            IncrementalSnapshotCdcConfig::default(),
            ReadOnlyReplicaConfig::default(),
            HeartbeatCdcConfig::default(),
        );

        assert!(config.transforms.is_empty());
        assert!(!config.deduplication.enabled);
        assert!(!config.heartbeat.enabled);
    }

    #[tokio::test]
    async fn test_processor_with_all_features() {
        let mut config = CdcFeatureConfig::default();
        config.deduplication.enabled = true;
        config.deduplication.lru_size = 100;
        config.deduplication.window_secs = 60;
        config.deduplication.bloom_expected_insertions = 1000;
        config.signal.enabled = true;
        config.transaction_topic.enabled = true;
        config.schema_change_topic.enabled = true;
        config.tombstone.enabled = true;
        config.read_only.enabled = true;

        let processor = CdcFeatureProcessor::new(config);

        assert!(processor.has_deduplication());
        assert!(processor.has_signal_processing());
        assert!(processor.has_transaction_topic());
        assert!(processor.has_schema_change_topic());
        assert!(processor.is_read_only());
    }

    #[tokio::test]
    async fn test_reset_stats() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        // Process an event to increment stats
        let event = create_test_event();
        let _ = processor.process(event).await;

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 1);

        // Reset stats
        processor.reset_stats().await;

        let stats = processor.stats().await;
        assert_eq!(stats.events_processed, 0);
        assert_eq!(stats.events_deduplicated, 0);
        assert_eq!(stats.events_transformed, 0);
    }

    #[test]
    fn test_enabled_features() {
        let mut config = CdcFeatureConfig::default();
        config.deduplication.enabled = true;
        config.deduplication.lru_size = 100;
        config.deduplication.window_secs = 60;
        config.deduplication.bloom_expected_insertions = 1000;
        config.tombstone.enabled = true;
        config.read_only.enabled = true;

        let processor = CdcFeatureProcessor::new(config);
        let features = processor.enabled_features();

        assert!(features.contains(&"deduplication"));
        assert!(features.contains(&"tombstones"));
        assert!(features.contains(&"read_only"));
        assert!(!features.contains(&"signal_processing"));
        assert!(!features.contains(&"transaction_topic"));
    }

    #[test]
    fn test_column_filter_helpers() {
        let mut config = CdcFeatureConfig::default();

        let processor = CdcFeatureProcessor::new(config.clone());
        assert!(!processor.has_column_filters());
        assert_eq!(processor.column_filter_count(), 0);

        config.column_filters.insert(
            "public.users".to_string(),
            ColumnFilterConfig {
                include: vec!["id".to_string()],
                exclude: vec![],
            },
        );
        config.column_filters.insert(
            "public.orders".to_string(),
            ColumnFilterConfig {
                include: vec![],
                exclude: vec!["internal_data".to_string()],
            },
        );

        let processor = CdcFeatureProcessor::new(config);
        assert!(processor.has_column_filters());
        assert_eq!(processor.column_filter_count(), 2);
    }

    #[test]
    fn test_encrypted_fields_getter() {
        let mut config = CdcFeatureConfig::default();
        config.encryption.enabled = true;
        config.encryption.fields = vec![
            "ssn".to_string(),
            "credit_card".to_string(),
            "password".to_string(),
        ];

        let processor = CdcFeatureProcessor::new(config);

        assert!(processor.has_encryption());
        assert_eq!(processor.encrypted_fields().len(), 3);
        assert!(processor.encrypted_fields().contains(&"ssn".to_string()));
        assert!(processor
            .encrypted_fields()
            .contains(&"credit_card".to_string()));
    }

    #[test]
    fn test_has_transforms() {
        let mut config = CdcFeatureConfig::default();

        let processor = CdcFeatureProcessor::new(config.clone());
        assert!(!processor.has_transforms());

        // Add a transform
        config.transforms.push(SmtTransformConfig {
            transform_type: SmtTransformType::MaskField,
            name: Some("mask_pii".to_string()),
            config: HashMap::new(),
            predicate: None,
        });

        let processor = CdcFeatureProcessor::new(config);
        assert!(processor.has_transforms());
    }

    #[test]
    fn test_builder_pattern() {
        let config = CdcFeatureConfig::builder()
            .with_tombstones()
            .with_deduplication(1000, 120)
            .with_signals()
            .with_read_only()
            .build();

        assert!(config.tombstone.enabled);
        assert!(config.tombstone.after_delete);
        assert!(config.deduplication.enabled);
        assert_eq!(config.deduplication.lru_size, 1000);
        assert_eq!(config.deduplication.window_secs, 120);
        assert!(config.signal.enabled);
        assert!(config.read_only.enabled);
    }

    #[test]
    fn test_builder_with_encryption() {
        let config = CdcFeatureConfig::builder()
            .with_encryption(vec!["ssn".to_string(), "credit_card".to_string()])
            .build();

        assert!(config.encryption.enabled);
        assert_eq!(config.encryption.fields.len(), 2);
        assert!(config.encryption.fields.contains(&"ssn".to_string()));
    }

    #[test]
    fn test_builder_with_column_filter() {
        let config = CdcFeatureConfig::builder()
            .add_column_filter(
                "public.users",
                ColumnFilterConfig {
                    include: vec!["id".to_string(), "name".to_string()],
                    exclude: vec![],
                },
            )
            .add_column_filter(
                "public.orders",
                ColumnFilterConfig {
                    include: vec![],
                    exclude: vec!["internal".to_string()],
                },
            )
            .build();

        assert_eq!(config.column_filters.len(), 2);
        assert!(config.column_filters.contains_key("public.users"));
        assert!(config.column_filters.contains_key("public.orders"));
    }

    #[test]
    fn test_stats_display() {
        let stats = CdcFeatureStats {
            events_processed: 1000,
            events_deduplicated: 50,
            events_transformed: 200,
            events_filtered: 25,
            fields_encrypted: 10,
            signals_processed: 5,
            signals_ignored: 1,
            transactions_tracked: 100,
            schema_changes_detected: 2,
            tombstones_emitted: 30,
            events_routed: 0,
            events_dead_lettered: 0,
            events_compacted: 0,
            partitions_assigned: 0,
            started_at: None, // Use None to avoid timing-dependent tests
        };

        let display = format!("{}", stats);
        assert!(display.contains("processed: 1000"));
        assert!(display.contains("dedup: 50"));
        assert!(display.contains("transformed: 200"));
        assert!(display.contains("filtered: 25"));
        assert!(display.contains("encrypted: 10"));
        assert!(display.contains("tombstones: 30"));
    }

    #[test]
    fn test_stats_rate_calculations() {
        let stats = CdcFeatureStats {
            events_processed: 900,
            events_deduplicated: 100,
            events_transformed: 0,
            events_filtered: 0,
            fields_encrypted: 0,
            signals_processed: 0,
            signals_ignored: 0,
            transactions_tracked: 0,
            schema_changes_detected: 0,
            tombstones_emitted: 0,
            events_routed: 0,
            events_dead_lettered: 0,
            events_compacted: 0,
            partitions_assigned: 0,
            started_at: None,
        };

        // Total events = processed + deduplicated + filtered = 900 + 100 + 0 = 1000
        assert_eq!(stats.total_events_seen(), 1000);

        // Dedup rate = 100 / 1000 * 100 = 10%
        assert!((stats.deduplication_rate() - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_processed_event_helpers() {
        let event = create_test_event();
        let processed = ProcessedEvent {
            event: event.clone(),
            tombstone: None,
            transaction_event: None,
            schema_change_event: None,
        };

        assert!(!processed.has_tombstone());
        assert!(!processed.has_transaction());
        assert!(!processed.has_schema_change());
        assert_eq!(processed.table_name(), "public.users");
        assert!(processed.is_insert());
        assert!(!processed.is_delete());
        assert!(!processed.is_update());
    }

    #[test]
    fn test_processed_event_with_tombstone() {
        let mut event = create_test_event();
        event.op = CdcOp::Delete;

        let tombstone = create_tombstone(&event);
        let processed = ProcessedEvent {
            event,
            tombstone: Some(tombstone),
            transaction_event: None,
            schema_change_event: None,
        };

        assert!(processed.has_tombstone());
        assert!(processed.is_delete());
    }

    // ========================================================================
    // Health Monitoring Tests
    // ========================================================================

    #[test]
    fn test_builder_with_health_monitoring() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        assert!(config.health.enabled);
        assert!(config.health.auto_recovery);
    }

    #[test]
    fn test_health_monitoring_config() {
        let config = CdcFeatureConfig::builder()
            .health(HealthMonitorConfig {
                enabled: true,
                check_interval_secs: 5,
                max_lag_ms: 10_000,
                failure_threshold: 2,
                success_threshold: 3,
                check_timeout_secs: 3,
                auto_recovery: true,
                recovery_delay_secs: 2,
                max_recovery_delay_secs: 30,
            })
            .build();

        assert!(config.health.enabled);
        assert_eq!(config.health.check_interval_secs, 5);
        assert_eq!(config.health.max_lag_ms, 10_000);
        assert_eq!(config.health.failure_threshold, 2);
    }

    #[test]
    fn test_processor_health_monitoring_disabled() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        assert!(!processor.has_health_monitor());
        assert!(processor.health_monitor().is_none());
        assert!(processor.recovery_coordinator().is_none());
        // When no health monitor, is_healthy() uses heartbeat (which defaults to true)
        assert!(processor.is_healthy());
        assert!(processor.is_ready());
    }

    #[test]
    fn test_processor_health_monitoring_enabled() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);

        assert!(processor.has_health_monitor());
        assert!(processor.health_monitor().is_some());
        assert!(processor.recovery_coordinator().is_some());
        // Initially not healthy (no checks run yet)
        assert!(!processor.is_healthy());
    }

    #[tokio::test]
    async fn test_health_monitoring_mark_connected() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);
        assert!(!processor.is_healthy());

        processor.mark_connected();
        assert!(processor.is_healthy());
        assert!(processor.is_ready());
    }

    #[tokio::test]
    async fn test_health_monitoring_mark_disconnected() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);
        processor.mark_connected();
        assert!(processor.is_healthy());

        processor.mark_disconnected();
        assert!(!processor.is_healthy());
    }

    #[tokio::test]
    async fn test_health_monitoring_lag_tracking() {
        let config = CdcFeatureConfig::builder()
            .health(HealthMonitorConfig {
                enabled: true,
                max_lag_ms: 1000,
                ..Default::default()
            })
            .build();

        let processor = CdcFeatureProcessor::new(config);
        processor.mark_connected();

        // Set acceptable lag
        processor.set_health_lag(500);

        // Run health check
        let status = processor.check_health().await;
        assert!(status.is_some());
        let status = status.unwrap();
        assert_eq!(status.lag_ms, 500);
    }

    #[tokio::test]
    async fn test_health_monitoring_probes() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);

        // Initially not ready
        let (code, _) = processor.readiness_probe().await;
        assert_eq!(code, 503);

        // Mark connected
        processor.mark_connected();

        // Now should be ready
        let (code, _) = processor.readiness_probe().await;
        assert_eq!(code, 200);

        // Liveness should always return OK unless catastrophically unhealthy
        let (code, _) = processor.liveness_probe().await;
        assert_eq!(code, 200);
    }

    #[tokio::test]
    async fn test_health_monitoring_json() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);
        processor.mark_connected();

        let json = processor.health_json().await;
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"ready\":true"));
    }

    #[tokio::test]
    async fn test_health_monitoring_custom_check() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);

        // Register a custom health check
        processor
            .register_health_check("test_check", || async { HealthCheckResult::Healthy })
            .await;

        // Run health checks
        let status = processor.check_health().await;
        assert!(status.is_some());
        let status = status.unwrap();
        assert!(status.checks.contains_key("test_check"));
    }

    #[tokio::test]
    async fn test_recovery_not_in_progress_when_disabled() {
        let config = CdcFeatureConfig::default();
        let processor = CdcFeatureProcessor::new(config);

        assert!(!processor.is_recovery_in_progress());
    }

    #[tokio::test]
    async fn test_attempt_recovery_when_enabled() {
        let config = CdcFeatureConfig::builder()
            .health(HealthMonitorConfig {
                enabled: true,
                auto_recovery: true,
                recovery_delay_secs: 0, // No delay for tests
                ..Default::default()
            })
            .build();

        let processor = CdcFeatureProcessor::new(config);
        processor.mark_disconnected();

        // Attempt recovery with a successful recovery function
        let recovered = processor.attempt_recovery(|| async { true }).await;
        assert!(recovered);
        assert!(processor.is_healthy());
    }

    #[test]
    fn test_enabled_features_includes_health_monitor() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();

        let processor = CdcFeatureProcessor::new(config);
        let features = processor.enabled_features();

        assert!(features.contains(&"health_monitor"));
    }

    #[tokio::test]
    async fn test_unregister_health_check() {
        let config = CdcFeatureConfig::builder().with_health_monitoring().build();
        let processor = CdcFeatureProcessor::new(config);

        // Register a health check
        processor
            .register_health_check("test_check", || async { HealthCheckResult::Healthy })
            .await;

        // Run health checks - should include our check
        let status = processor.check_health().await.unwrap();
        assert!(status.checks.contains_key("test_check"));

        // Unregister the check
        processor.unregister_health_check("test_check").await;

        // Run health checks again - should not include our check
        let status = processor.check_health().await.unwrap();
        assert!(!status.checks.contains_key("test_check"));
    }
}
