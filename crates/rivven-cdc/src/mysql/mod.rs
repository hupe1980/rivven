//! MySQL/MariaDB CDC implementation using binary log replication
//!
//! # Modules
//!
//! - **protocol**: Binlog protocol handling
//! - **decoder**: Binlog event decoding
//! - **source**: CDC streaming source (`MySqlCdc`)
//! - **snapshot**: Snapshot source for initial data load (`MySqlSnapshotSource`)
//!
//! # Features
//!
//! ## CDC Streaming
//!
//! - MySQL 5.7+, 8.0+
//! - MariaDB 10.2+
//! - Row-based replication (binlog_format=ROW)
//! - GTID-based replication
//! - Table filtering
//!
//! ## Snapshot
//!
//! - Keyset pagination
//! - Table discovery with primary keys
//! - Row count estimation via INFORMATION_SCHEMA
//! - GTID watermarks for consistency
//!
//! # Architecture
//!
//! ```text
//! MySQL Binlog → MySqlBinlogClient → BinlogDecoder → CdcEvent
//! MySQL Tables → MySqlSnapshotSource → CdcEvent (snapshot)
//! ```
//!
//! # Example
//!
//! ## CDC Streaming
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
//!
//! ## Snapshot
//!
//! ```rust,ignore
//! use rivven_cdc::mysql::MySqlSnapshotSource;
//!
//! let source = MySqlSnapshotSource::connect("mysql://localhost/mydb").await?;
//! let watermark = source.get_watermark().await?;
//! ```

pub mod decoder;
pub mod protocol;
pub mod snapshot;
pub mod source;

pub use decoder::*;
pub use protocol::*;
pub use snapshot::*;
pub use source::*;
