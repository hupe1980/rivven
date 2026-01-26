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
mod filter;
mod health;
mod heartbeat;
mod metrics;
mod notification;
mod outbox;
mod parallel;
mod partitioner;
mod pipeline;
mod resilience;
mod router;
mod schema_evolution;
mod signal;
mod smt;
mod snapshot;
pub mod tls;
mod traits;
mod transaction;
mod validation;

pub use batch::*;
pub use checkpoint::*;
pub use compaction::*;
pub use connector::*;
pub use dedup::*;
pub use encryption::*;
pub use error::*;
pub use event::*;
pub use filter::*;
pub use health::*;
pub use heartbeat::*;
pub use metrics::*;
pub use notification::*;
pub use outbox::*;
pub use parallel::*;
pub use partitioner::*;
pub use pipeline::*;
pub use resilience::*;
pub use router::*;
pub use schema_evolution::*;
pub use signal::*;
pub use smt::*;
pub use snapshot::*;
pub use tls::{SslMode, TlsConfig};
pub use traits::*;
pub use transaction::*;
pub use validation::*;
