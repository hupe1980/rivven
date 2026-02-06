//! RDBC (Relational Database Connectivity) Query-Based Connectors
//!
//! This module provides query-based source and sink connectors for relational
//! databases using the `rivven-rdbc` crate. Unlike CDC connectors which use
//! native replication protocols, RDBC connectors use polling queries.
//!
//! # Use Cases
//!
//! | Use Case | Connector | Notes |
//! |----------|-----------|-------|
//! | Periodic data extraction | `rdbc-source` | Bulk, incrementing, timestamp modes |
//! | Data warehouse loading | `rdbc-sink` | Batch upsert with transactions |
//! | Legacy database migration | Both | No CDC infrastructure required |
//! | Cross-database replication | Both | Any → Any database copy |
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   RDBC Connector Architecture                    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                  │
//! │  ┌──────────────────────────────────────────────────────────┐   │
//! │  │                     Connection Pool                       │   │
//! │  │  • LIFO idle stack (cache locality)                      │   │
//! │  │  • Semaphore-controlled concurrency                      │   │
//! │  │  • Health checks with recycling                          │   │
//! │  │  • Lifecycle tracking (created_at, age, idle_time)       │   │
//! │  └──────────────────────────────────────────────────────────┘   │
//! │                              │                                   │
//! │              ┌───────────────┴───────────────┐                  │
//! │              ▼                               ▼                  │
//! │  ┌──────────────────────┐       ┌──────────────────────┐       │
//! │  │    RDBC Source       │       │     RDBC Sink        │       │
//! │  ├──────────────────────┤       ├──────────────────────┤       │
//! │  │ • Bulk mode          │       │ • Insert             │       │
//! │  │ • Incrementing mode  │       │ • Update             │       │
//! │  │ • Timestamp mode     │       │ • Upsert             │       │
//! │  │ • Combined mode      │       │ • Delete             │       │
//! │  │ • Offset tracking    │       │ • Transactional      │       │
//! │  └──────────────────────┘       └──────────────────────┘       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Supported Databases
//!
//! | Database | Feature Flag | Driver |
//! |----------|--------------|--------|
//! | PostgreSQL | `rdbc-postgres` | `tokio-postgres` |
//! | MySQL | `rdbc-mysql` | `mysql_async` |
//! | SQL Server | `rdbc-sqlserver` | `tiberius` |
//!
//! # Hot Path Optimizations
//!
//! | Optimization | Impact |
//! |--------------|--------|
//! | `execute_batch()` | Single round-trip per batch (10-100x vs per-row) |
//! | LIFO connection reuse | Better CPU cache locality |
//! | Lock-free stats | Atomic counters with `Relaxed` ordering |
//! | `min_pool_size` warm-up | Avoid cold-start latency |

// Query-based source connector
pub mod source;

// High-performance batch sink connector
pub mod sink;

// Re-exports for convenience
pub use sink::{RdbcSink, RdbcSinkConfig, RdbcSinkFactory, RdbcWriteMode};
pub use source::{RdbcQueryMode, RdbcSource, RdbcSourceConfig, RdbcSourceFactory};
