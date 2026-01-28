//! # rivven-cdc - Change Data Capture for Rivven
//!
//! Production-grade CDC support for PostgreSQL, MySQL, and MariaDB.
//!
//! ## Features
//!
//! - `postgres` - PostgreSQL logical replication via pgoutput
//! - `mysql` - MySQL binlog replication
//! - `mariadb` - MariaDB binlog replication (enables `mysql`)
//! - `full` - All CDC sources
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ PostgreSQL  │     │   MySQL     │     │  MariaDB    │
//! │    WAL      │     │   Binlog    │     │   Binlog    │
//! └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
//!        │                   │                   │
//!        ▼                   ▼                   ▼
//! ┌──────────────────────────────────────────────────────┐
//! │                    CdcSource Trait                   │
//! └──────────────────────────────────────────────────────┘
//!        │                   │                   │
//!        ▼                   ▼                   ▼
//! ┌──────────────────────────────────────────────────────┐
//! │                     CdcEvent                         │
//! │  { op: Insert/Update/Delete, before, after, ... }   │
//! └──────────────────────────────────────────────────────┘
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

// Common module - always available
pub mod common;

// Re-export common types at crate root
pub use common::{
    CdcError,
    CdcEvent,
    CdcFilter,
    CdcFilterConfig,
    CdcOp,
    CdcSource,
    CircuitBreaker,
    CircuitState,
    CompatibilityMode,
    ErrorCategory,
    // Phase 3.6: Routing
    EventRouter,
    ExponentialBackoff,
    // Phase 4: Guardrails & Retry
    GuardrailAction,
    GuardrailCheck,
    GuardrailsConfig,
    GuardrailsConfigBuilder,
    ProgressStore,
    RateLimiter,
    Result,
    RetriableErrorType,
    RetryConfig,
    RetryConfigBuilder,
    RouteCondition,
    RouteDecision,
    RouteRule,
    // Schema Evolution
    SchemaTracker,
    SchemaVersion,
    SnapshotConfig,
    // Phase 3.5: Snapshot Support
    SnapshotCoordinator,
    SnapshotMode,
    SnapshotProgress,
    SnapshotState,
    TableColumnConfig,
    Validator,
    CONNECTION_TIMEOUT_SECS,
    IO_TIMEOUT_SECS,
    MAX_MESSAGE_SIZE,
};

// PostgreSQL CDC - feature-gated
#[cfg(feature = "postgres")]
pub mod postgres;

// MySQL/MariaDB CDC - feature-gated
#[cfg(feature = "mysql")]
pub mod mysql;
