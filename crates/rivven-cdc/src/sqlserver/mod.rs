//! # SQL Server CDC Source
//!
//! Change Data Capture connector for Microsoft SQL Server using poll-based
//! CDC table queries (not log streaming).
//!
//! ## Architecture
//!
//! Unlike PostgreSQL (logical replication) and MySQL (binlog streaming), SQL Server
//! CDC uses a **poll-based** approach that queries CDC tables populated by the
//! SQL Server Agent capture job:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     SQL Server CDC Flow                         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌──────────┐    ┌──────────────┐    ┌─────────────────────┐   │
//! │  │ DML Ops  │───▶│ Transaction  │───▶│ CDC Capture Job     │   │
//! │  │ (I/U/D)  │    │ Log          │    │ (SQL Agent)         │   │
//! │  └──────────┘    └──────────────┘    └──────────┬──────────┘   │
//! │                                                  │              │
//! │                                                  ▼              │
//! │                                       ┌─────────────────────┐   │
//! │                                       │ cdc.<schema>_<table>│   │
//! │                                       │ Change Tables       │   │
//! │                                       └──────────┬──────────┘   │
//! │                                                  │              │
//! │     ┌────────────────────────────────────────────┘              │
//! │     ▼                                                           │
//! │  ┌────────────────────────────────────────────────────────────┐ │
//! │  │ Rivven CDC Connector (Poll-based)                          │ │
//! │  │                                                            │ │
//! │  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │ │
//! │  │  │ LSN Tracker  │◀───│ CDC Poller   │───▶│ Event        │  │ │
//! │  │  │ (Resume Pos) │    │ (fn_cdc_*)   │    │ Transformer  │  │ │
//! │  │  └──────────────┘    └──────────────┘    └──────────────┘  │ │
//! │  └────────────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Differences from PostgreSQL/MySQL
//!
//! | Aspect | PostgreSQL | MySQL | SQL Server |
//! |--------|------------|-------|------------|
//! | Mechanism | Logical replication | Binlog streaming | CDC table polling |
//! | Latency | Sub-second | Sub-second | Poll interval (100ms-5s) |
//! | Position | LSN (u64) | Binlog pos/GTID | commit_lsn + change_lsn |
//! | Server load | Push-based | Push-based | Query-based (minimal) |
//!
//! ## CDC Table Structure
//!
//! For each enabled table, SQL Server creates a change table with:
//!
//! | Column | Type | Description |
//! |--------|------|-------------|
//! | `__$start_lsn` | `binary(10)` | Commit LSN of the transaction |
//! | `__$seqval` | `binary(10)` | Sequence within transaction |
//! | `__$operation` | `int` | 1=Delete, 2=Insert, 3=Update(before), 4=Update(after) |
//! | `__$update_mask` | `varbinary` | Bitmask of updated columns |
//! | `<columns>` | varies | Original table columns |
//!
//! ## LSN Tracking
//!
//! We track position using `commit_lsn` + `change_lsn` (sequence value):
//!
//! ```text
//! Position = (commit_lsn, change_lsn)
//!
//! Query for changes:
//!   SELECT * FROM cdc.fn_cdc_get_all_changes_<capture_instance>(
//!       @from_lsn, @to_lsn, 'all update old'
//!   )
//!   WHERE __$start_lsn > @last_commit_lsn
//!      OR (__$start_lsn = @last_commit_lsn AND __$seqval > @last_change_lsn)
//! ```
//!
//! ## Features
//!
//! - **Poll-based CDC**: Queries CDC tables at configurable intervals
//! - **LSN tracking**: Precise resume from last position
//! - **Snapshot support**: Initial table sync before CDC streaming
//! - **Transaction grouping**: Events grouped by commit_lsn
//! - **Schema evolution**: Handles column additions/drops
//! - **TLS encryption**: Secure connections via rustls
//!
//! ## Example
//!
//! ```rust,ignore
//! use rivven_cdc::sqlserver::{SqlServerCdc, SqlServerCdcConfig};
//!
//! let config = SqlServerCdcConfig::builder()
//!     .host("localhost")
//!     .port(1433)
//!     .username("sa")
//!     .password("YourPassword123!")
//!     .database("mydb")
//!     .poll_interval_ms(500)
//!     .include_table("dbo", "users")
//!     .include_table("dbo", "orders")
//!     .build()?;
//!
//! let mut cdc = SqlServerCdc::new(config);
//! let rx = cdc.take_event_receiver().unwrap();
//!
//! cdc.start().await?;
//!
//! while let Some(event) = rx.recv().await {
//!     println!("Change: {:?}", event);
//! }
//! ```
//!
//! ## SQL Server Requirements
//!
//! 1. **Enable CDC on database**:
//!    ```sql
//!    EXEC sys.sp_cdc_enable_db;
//!    ```
//!
//! 2. **Enable CDC on tables**:
//!    ```sql
//!    EXEC sys.sp_cdc_enable_table
//!        @source_schema = N'dbo',
//!        @source_name = N'users',
//!        @role_name = NULL,
//!        @supports_net_changes = 1;
//!    ```
//!
//! 3. **SQL Server Agent must be running** (captures changes to CDC tables)
//!
//! ## Supported SQL Server Versions
//!
//! - SQL Server 2016+ (recommended: 2019 or 2022)
//! - Azure SQL Database (with CDC enabled)
//! - Azure SQL Managed Instance

mod error;
mod protocol;
mod snapshot;
mod source;

pub use error::SqlServerError;
pub use protocol::{CdcChangeRecord, SqlServerClient};
pub use snapshot::{SnapshotState, SqlServerSnapshotExecutor, TableSnapshotProgress};
pub use source::{
    CaptureInstance, CdcPosition, Lsn, SqlServerCdc, SqlServerCdcConfig, SqlServerCdcConfigBuilder,
    SqlServerMetrics, SqlServerMetricsSnapshot,
};

// Re-export common types
pub use crate::common::{CdcEvent, CdcOp, CdcSource, SnapshotMode};
