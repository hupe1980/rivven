//! MySQL/MariaDB CDC implementation using binary log replication
//!
//! Supports:
//! - MySQL 5.7+, 8.0+
//! - MariaDB 10.2+
//! - Row-based replication (binlog_format=ROW)
//! - GTID-based replication
//! - Table filtering
//! - Schema inference
//!
//! # Architecture
//!
//! ```text
//! MySQL Binlog → MySqlBinlogClient → BinlogDecoder → CdcEvent
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = MySqlCdcConfig::new("localhost", "root")
//!     .with_password("password")
//!     .with_database("mydb")
//!     .with_server_id(1001);
//!
//! let cdc = MySqlCdc::new(config);
//! # Ok(())
//! # }
//! ```

pub mod protocol;
pub mod decoder;
pub mod source;
pub mod type_mapper;

pub use protocol::*;
pub use decoder::*;
pub use source::*;
pub use type_mapper::*;
