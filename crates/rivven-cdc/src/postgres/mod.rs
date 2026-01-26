//! PostgreSQL CDC implementation using logical replication
//!
//! Supports:
//! - PostgreSQL 10+ with logical replication
//! - pgoutput output plugin
//! - Transaction boundaries (BEGIN/COMMIT)
//! - Schema inference (20+ PostgreSQL types → Avro)
//! - Table/column filtering
//!
//! # Architecture
//!
//! ```text
//! PostgreSQL WAL → ReplicationClient → PgOutputDecoder → CdcEvent
//! ```
//!
//! # Example
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

mod protocol;
mod source;
mod type_mapper;

pub use protocol::*;
pub use source::*;
pub use type_mapper::*;
