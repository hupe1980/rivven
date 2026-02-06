//! CDC (Change Data Capture) Connectors
//!
//! This module provides CDC source connectors for capturing database changes
//! in real-time using native replication protocols.
//!
//! # Supported Databases
//!
//! | Database | Protocol | Feature Flag |
//! |----------|----------|--------------|
//! | PostgreSQL | Logical Replication | `postgres` (default) |
//! | MySQL | Binary Log | `mysql` |
//! | SQL Server | Change Tracking | `sqlserver` |
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     CDC Connector Architecture                   │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                  │
//! │  ┌──────────┐    ┌──────────┐    ┌──────────────┐              │
//! │  │ Database │───▶│ Protocol │───▶│ CDC Processor │             │
//! │  │ Changes  │    │ Decoder  │    │ (SMT, Filter) │             │
//! │  └──────────┘    └──────────┘    └──────┬───────┘              │
//! │                                          │                       │
//! │  ┌──────────────────────────────────────▼───────────────────┐  │
//! │  │                    Shared CDC Features                     │  │
//! │  │  • Deduplication     • Signals       • Heartbeat          │  │
//! │  │  • Snapshots         • Encryption    • Metrics            │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Initial Snapshots**: Full table capture before streaming
//! - **Incremental Snapshots**: Ad-hoc re-snapshotting via signals
//! - **Single Message Transforms (SMT)**: Filter, route, mask, compute fields
//! - **Column Filtering**: Include/exclude columns per table
//! - **Deduplication**: Exactly-once delivery with configurable stores
//! - **Signal Tables**: Runtime control (snapshots, pause, resume)
//! - **Heartbeats**: Connection monitoring with configurable actions
//! - **Encryption**: Field-level encryption for sensitive data

// Shared CDC configuration types
pub mod config;

// Shared CDC utilities (SMT, column filters, event conversion)
pub mod common;

// Full CDC feature integration (dedup, signals, encryption, etc.)
pub mod features;

// Snapshot management (initial + incremental snapshots)
pub mod snapshot;

// Database-specific CDC connectors
#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "sqlserver")]
pub mod sqlserver;

// Re-exports for convenience
pub use common::*;
pub use config::*;

#[cfg(feature = "postgres")]
pub use postgres::{PostgresCdcConfig, PostgresCdcSource, PostgresCdcSourceFactory};

#[cfg(feature = "mysql")]
pub use mysql::{MySqlCdcConfig, MySqlCdcSource, MySqlCdcSourceFactory};

#[cfg(feature = "sqlserver")]
pub use sqlserver::{SqlServerCdcConfig, SqlServerCdcSource, SqlServerCdcSourceFactory};
