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
//! - [`SchemaTracker`] - Schema evolution tracking
//! - [`OutboxProcessor`] - Transactional outbox pattern
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
//! │  SchemaTracker ←─── Evolution tracking, compatibility       │
//! │  Outbox        ←─── Transactional outbox pattern            │
//! │  Compaction    ←─── Log compaction, tombstones              │
//! │  Encryption    ←─── Field-level encryption (AES-256-GCM)    │
//! │  Heartbeat     ←─── Replication slot health, WAL advance    │
//! │  Signal        ←─── Control signals (snapshot, pause)       │
//! │  Notification  ←─── Progress, status, error alerts          │
//! │  SMT           ←─── Single Message Transforms (Debezium)    │
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
mod pipeline;
pub mod read_only;
pub mod replica_identity;
mod resilience;
mod router;
pub mod schema_change;
mod schema_evolution;
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
    AdditionalCondition, ChunkBuffer, ChunkState, IncrementalSnapshotConfig,
    IncrementalSnapshotConfigBuilder, IncrementalSnapshotContext, IncrementalSnapshotCoordinator,
    IncrementalSnapshotRequest, IncrementalSnapshotState, IncrementalSnapshotStats,
    IncrementalSnapshotStatsSnapshot, SnapshotChunk, SnapshotTable, WatermarkSignal,
    WatermarkStrategy, WatermarkType,
};
pub use metrics::*;
pub use notification::*;
pub use outbox::*;
pub use parallel::*;
pub use partitioner::*;
pub use pipeline::*;
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
pub use schema_evolution::*;
pub use signal::*;
pub use smt::*;
pub use snapshot::*;
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
