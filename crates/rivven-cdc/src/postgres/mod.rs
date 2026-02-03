//! PostgreSQL CDC implementation using logical replication
//!
//! # Modules
//!
//! - **protocol**: WAL protocol handling (pgoutput decoder)
//! - **source**: CDC streaming source (`PostgresCdc`)
//! - **snapshot**: Snapshot source for initial data load
//!
//! # Features
//!
//! ## CDC Streaming
//!
//! - PostgreSQL 10+ with logical replication
//! - pgoutput output plugin
//! - Transaction boundaries (BEGIN/COMMIT)
//! - Table/column filtering
//!
//! ## Snapshot
//!
//! - Efficient keyset pagination
//! - Table discovery with primary keys
//! - Row count estimation via pg_class
//! - WAL LSN watermarks for consistency
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL WAL → ReplicationClient → PgOutputDecoder → CdcEvent
//! PostgreSQL Tables → PostgresSnapshotSource → CdcEvent (snapshot)
//! ```
//!
//! # Example
//!
//! ## CDC Streaming
//!
//! ```rust,no_run
//! use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
//! use rivven_cdc::common::CdcSource; // Required for .start()
//!
//! # async fn example() -> anyhow::Result<()> {
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
//! ## Snapshot
//!
//! ```rust,ignore
//! use rivven_cdc::postgres::PostgresSnapshotSource;
//!
//! let source = PostgresSnapshotSource::connect("postgresql://localhost/mydb").await?;
//! let watermark = source.get_watermark().await?;
//! ```

mod protocol;
mod snapshot;
mod source;

pub use protocol::*;
pub use snapshot::*;
pub use source::*;
