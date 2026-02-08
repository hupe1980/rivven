//! # Common CDC Types and Traits
//!
//! This module contains database-agnostic abstractions for Change Data Capture:
//!
//! - [`CdcEvent`] - Unified change event representation
//! - [`TransactionMetadata`] - Transaction boundary tracking
//! - [`CdcSource`] - Trait for CDC implementations
//! - [`CdcConnector`] - Event routing to Rivven topics
//! - [`CdcFilter`] - Table/column filtering
//! - Resilience primitives ([`RateLimiter`], [`CircuitBreaker`])
//! - [`Validator`] - Connection parameter validation
//! - [`CdcMetrics`] - Production-grade observability
//! - [`CheckpointStore`] - Persistent offset tracking
//! - [`EventBatcher`] - Configurable batch processing
//! - [`HealthMonitor`] - Connection monitoring and auto-recovery
//! - [`Pipeline`] - Composable CDC processing pipelines
//! - [`Deduplicator`] - Idempotent event processing
//! - [`ParallelCoordinator`] - Multi-table parallel CDC
//! - [`Compactor`] - Log compaction (keep latest per key)
//! - [`FieldEncryptor`] - Column-level encryption
//! - [`TransactionGrouper`] - Atomic transaction processing
//! - [`Heartbeat`] - Replication slot health/WAL advancement
//! - [`SignalProcessor`] - Control signals (snapshot, pause/resume)
//! - [`Notifier`] - Progress and status notifications
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Common Module                            │
//! ├─────────────────────────────────────────────────────────────┤
//! │  CdcSource trait ←─── PostgresCdc, MySqlCdc implement       │
//! │  CdcEvent      ←─── Unified event format                    │
//! │  Transaction   ←─── Transaction boundary grouping           │
//! │  CdcConnector  ←─── Routes events to topics                 │
//! │  CdcFilter     ←─── Include/exclude tables/columns          │
//! │  Resilience    ←─── Rate limiting, circuit breaker          │
//! │  CdcMetrics    ←─── Prometheus-compatible metrics           │
//! │  Checkpoint    ←─── Resumable CDC with offset tracking      │
//! │  EventBatcher  ←─── Throughput-optimized batching           │
//! │  HealthMonitor ←─── Auto-recovery, liveness probes          │
//! │  Pipeline      ←─── Transform, filter, route events         │
//! │  Deduplicator  ←─── Bloom + LRU duplicate detection         │
//! │  Parallel      ←─── Multi-table concurrent processing       │
//! │  Outbox        ←─── Transactional outbox pattern            │
//! │  Compaction    ←─── Log compaction, tombstones              │
//! │  Encryption    ←─── Field-level encryption (AES-256-GCM)    │
//! │  Heartbeat     ←─── Replication slot health, WAL advance    │
//! │  Signal        ←─── Control signals (snapshot, pause)       │
//! │  Notification  ←─── Progress, status, error alerts          │
//! │  SMT           ←─── Single Message Transforms               │
//! │  Tombstone     ←─── Log compaction tombstone emission       │
//! └─────────────────────────────────────────────────────────────┘
//! ```

mod batch;
mod checkpoint;
mod compaction;
mod connector;
mod dedup;
mod encryption;
mod error;
mod event;
mod extended_metrics;
mod filter;
mod health;
mod heartbeat;
pub mod incremental_snapshot;
mod metrics;
mod notification;
mod outbox;
mod parallel;
mod partitioner;
pub mod pattern;
mod pipeline;
pub mod progress_stores;
pub mod read_only;
pub mod replica_identity;
mod resilience;
mod router;
pub mod schema_change;
mod serialization;
mod signal;
mod smt;
mod snapshot;
pub mod tls;
pub mod tombstone;
mod traits;
mod transaction;
mod transaction_topic;
mod validation;

pub use batch::*;
pub use checkpoint::*;
pub use compaction::*;
pub use connector::*;
pub use dedup::*;
pub use encryption::*;
pub use error::*;
pub use event::*;
pub use extended_metrics::*;
pub use filter::*;
pub use health::*;
pub use heartbeat::*;
pub use incremental_snapshot::{
    AdditionalCondition, AggregateBufferStats, ChunkBoundary, ChunkBuffer, ChunkBufferStats,
    ChunkQueryBuilder, ChunkState, ChunkingStrategy, IncrementalSnapshotConfig,
    IncrementalSnapshotConfigBuilder, IncrementalSnapshotContext, IncrementalSnapshotCoordinator,
    IncrementalSnapshotRequest, IncrementalSnapshotState, IncrementalSnapshotStats,
    IncrementalSnapshotStatsSnapshot, KeyRange, QuerySpec, SnapshotChunk, SnapshotKey,
    SnapshotTable, WatermarkSignal, WatermarkStrategy, WatermarkType,
};
pub use metrics::*;
pub use notification::*;
pub use outbox::*;
pub use parallel::*;
pub use partitioner::*;
pub use pattern::{
    matches_any_pattern, matches_qualified_name, pattern_match, pattern_match_case_sensitive,
    PatternError, PatternMatcher, PatternSet, PatternSyntax, SharedPatternSet,
};
pub use pipeline::*;
pub use progress_stores::FileProgressStore;
#[cfg(feature = "postgres")]
pub use progress_stores::PostgresProgressStore;
pub use read_only::{
    DeduplicationResult, ReadOnlyConfig, ReadOnlyConfigBuilder, ReadOnlyError, ReadOnlyFeature,
    ReadOnlyGuard, ReadOnlyWatermarkStats, ReadOnlyWatermarkTracker, WatermarkSource,
};
pub use replica_identity::{
    CheckResult, EnforcementMode, ReplicaIdentity, ReplicaIdentityConfig, ReplicaIdentityEnforcer,
    ReplicaIdentityStats,
};
pub use resilience::*;
pub use router::*;
pub use schema_change::{
    ColumnChanges, ColumnDefinition, ColumnModification, SchemaChangeConfig,
    SchemaChangeConfigBuilder, SchemaChangeEmitter, SchemaChangeEvent, SchemaChangeStats,
    SchemaChangeStatsSnapshot, SchemaChangeType,
};
pub use serialization::{
    CdcEventSerializer, SerializationConfig, SerializationError, SerializationFormat,
};
pub use signal::*;
pub use smt::*;
pub use snapshot::*;
// Note: Database-specific snapshot sources are in their respective modules:
// - PostgresSnapshotSource, discover_primary_key, etc. -> rivven_cdc::postgres::*
// - MySqlSnapshotSource -> rivven_cdc::mysql::*
pub use tls::{SslMode, TlsConfig};
pub use tombstone::{TombstoneConfig, TombstoneEmitter, TombstoneStats};
pub use traits::*;
pub use transaction::*;
pub use transaction_topic::{
    DataCollectionEventCount, TransactionContext, TransactionEvent, TransactionStatus,
    TransactionTopicConfig, TransactionTopicConfigBuilder, TransactionTopicEmitter,
    TransactionTopicStats, TransactionTopicStatsSnapshot,
};
pub use validation::*;
