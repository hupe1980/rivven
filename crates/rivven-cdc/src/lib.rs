//! # rivven-cdc - Change Data Capture for Rivven
//!
//! Production-grade CDC support for PostgreSQL, MySQL, MariaDB, and SQL Server.
//!
//! ## Features
//!
//! - `postgres` - PostgreSQL logical replication via pgoutput
//! - `mysql` - MySQL binlog replication
//! - `mariadb` - MariaDB binlog replication (enables `mysql`)
//! - `sqlserver` - SQL Server CDC via poll-based CDC tables
//! - `full` - All CDC sources
//!
//! ## Architecture
//!
//! ```text
//! ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
//! │PostgreSQL │   │  MySQL    │   │ MariaDB   │   │SQL Server │
//! │   WAL     │   │  Binlog   │   │  Binlog   │   │CDC Tables │
//! └─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
//!       │               │               │               │
//!       ▼               ▼               ▼               ▼
//! ┌──────────────────────────────────────────────────────────┐
//! │                    CdcSource Trait                       │
//! └──────────────────────────────────────────────────────────┘
//!       │               │               │               │
//!       ▼               ▼               ▼               ▼
//! ┌──────────────────────────────────────────────────────────┐
//! │                     CdcEvent                             │
//! │    { op: Insert/Update/Delete, before, after, ... }     │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! # #[cfg(feature = "postgres")]
//! # async fn example() -> anyhow::Result<()> {
//! use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
//! use rivven_cdc::CdcSource;
//!
//! let config = PostgresCdcConfig::builder()
//!     .connection_string("postgres://user:pass@localhost/mydb")
//!     .slot_name("rivven_slot")
//!     .publication_name("rivven_pub")
//!     .build()?;
//!
//! let mut cdc = PostgresCdc::new(config);
//! cdc.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Public API Organization
//!
//! This crate exposes types in three tiers:
//!
//! ### Tier 1: Core Types (crate root)
//! Essential types for basic CDC operations - `CdcEvent`, `CdcOp`, `CdcSource`.
//!
//! ### Tier 2: Feature Types (crate root)
//! Optional features for production use - SMT transforms, deduplication, encryption.
//!
//! ### Tier 3: Advanced Types (`common` module)
//! Internal/advanced types for custom implementations - accessed via `common::*`.

// Common module - always available (contains advanced/internal types)
pub mod common;

// =============================================================================
// TIER 1: Core CDC Types - Essential for basic operations
// =============================================================================

pub use common::{
    // Error handling
    CdcError,
    // Core event and operation types
    CdcEvent,
    // Filtering
    CdcFilter,
    CdcFilterConfig,
    CdcOp,
    CdcSource,
    ErrorCategory,
    Result,
    TableColumnConfig,
};

// =============================================================================
// TIER 2: Production Features - Optional but commonly used
// =============================================================================

// SMT (Single Message Transforms)
pub use common::{
    Cast, CastType, ComputeField, ContentRouter, ExtractNewRecordState, Filter, FilterCondition,
    Flatten, HeaderSource, HeaderToValue, InsertField, MaskField, NullCondition, RegexRouter,
    ReplaceField, SetNull, Smt, SmtChain, TimestampConverter, TimestampFormat, TimezoneConverter,
    Unwrap, ValueToKey,
};

// Deduplication
pub use common::{Deduplicator, DeduplicatorConfig, KeyStrategy};

// Schema Change Detection
pub use common::{ColumnDefinition, SchemaChangeConfig, SchemaChangeEmitter, SchemaChangeEvent};

// Transaction Topic
pub use common::{TransactionEvent, TransactionTopicConfig, TransactionTopicEmitter};

// Tombstone Emission
pub use common::{TombstoneConfig, TombstoneEmitter};

// Signal Processing
pub use common::{Signal, SignalProcessor, SignalResult};

// Field-Level Encryption
pub use common::{EncryptionConfig, FieldEncryptor, MemoryKeyProvider};

// =============================================================================
// TIER 3: Advanced Types - Available via `common::` module
// =============================================================================
// The following are NOT re-exported at crate root but accessible via `common::`:
//
// Resilience (rivven-connect has its own implementations):
//   - common::CircuitBreaker, CircuitState
//   - common::RateLimiter
//   - common::ExponentialBackoff
//   - common::RetryConfig, RetryConfigBuilder
//   - common::GuardrailsConfig, GuardrailsConfigBuilder
//
// Routing (for custom event routing):
//   - common::EventRouter, RouteRule, RouteCondition, RouteDecision
//
// Snapshot (for custom snapshot implementations):
//   - common::SnapshotCoordinator, SnapshotConfig, SnapshotMode
//   - common::SnapshotProgress, SnapshotState, ProgressStore
//
// Validation (for custom validators):
//   - common::Validator
//
// Constants (for protocol-level customization):
//   - common::CONNECTION_TIMEOUT_SECS
//   - common::IO_TIMEOUT_SECS
//   - common::MAX_MESSAGE_SIZE

// PostgreSQL CDC - feature-gated
#[cfg(feature = "postgres")]
pub mod postgres;

// MySQL/MariaDB CDC - feature-gated
#[cfg(feature = "mysql")]
pub mod mysql;

// SQL Server CDC - feature-gated
#[cfg(feature = "sqlserver")]
pub mod sqlserver;
