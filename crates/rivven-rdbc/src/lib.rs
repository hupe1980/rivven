//! # rivven-rdbc
//!
//! Production-grade relational database connectivity for the Rivven event streaming platform.
//!
//! This crate provides a unified interface for connecting to relational databases,
//! with feature parity to Kafka Connect JDBC connector and Debezium's relational support.
//!
//! ## Features
//!
//! - **Multi-Database Support**: PostgreSQL, MySQL, SQL Server with unified API
//! - **Connection Pooling**: High-performance connection pooling with health checks
//! - **SQL Dialect Abstraction**: Vendor-agnostic SQL generation using sea-query
//! - **Schema Discovery**: Automatic schema introspection and metadata
//! - **Table Source**: Query-based CDC with incrementing/timestamp modes
//! - **Table Sink**: High-performance batch writes with upsert support
//! - **Type Safety**: Comprehensive value types matching Debezium's type system
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use rivven_rdbc::prelude::*;
//!
//! // Create a connection pool
//! let pool = PoolBuilder::new("postgres://user:pass@localhost/db")
//!     .min_size(2)
//!     .max_size(10)
//!     .build();
//!
//! // Use connection for queries
//! let conn = pool.get().await?;
//! let rows = conn.query("SELECT * FROM users WHERE id = $1", &[Value::Int32(1)]).await?;
//!
//! // Configure a table source
//! let source = TableSourceConfig::incrementing("users", "id")
//!     .with_schema("public")
//!     .with_poll_interval(Duration::from_secs(1));
//!
//! // Configure a table sink
//! let sink = TableSinkBuilder::new()
//!     .batch_size(1000)
//!     .write_mode(WriteMode::Upsert)
//!     .auto_ddl(AutoDdlMode::Create)
//!     .build();
//! ```
//!
//! ## Feature Flags
//!
//! - `postgres` - PostgreSQL support via tokio-postgres
//! - `mysql` - MySQL/MariaDB support via mysql_async
//! - `sqlserver` - SQL Server support via tiberius
//! - `tls` - TLS support for secure connections
//! - `full` - All features enabled

#![warn(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]

pub mod connection;
pub mod dialect;
pub mod error;
pub mod pool;
pub mod schema;
pub mod security;
pub mod sink;
pub mod source;
pub mod types;

// Backend implementations (conditionally compiled)
#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "sqlserver")]
pub mod sqlserver;

/// Prelude module for convenient imports
pub mod prelude {
    // Error types
    pub use crate::error::{Error, ErrorCategory, Result};

    // Value and type system
    pub use crate::types::{ColumnMetadata, Row, TableMetadata, Value};

    // Connection traits and config
    pub use crate::connection::{
        Connection, ConnectionConfig, ConnectionFactory, ConnectionLifecycle, DatabaseType,
        IsolationLevel, PreparedStatement, RowStream, Transaction,
    };

    // Pool types
    pub use crate::pool::{
        create_pool, create_pool_with_config, AtomicPoolStats, ConnectionPool, PoolBuilder,
        PoolConfig, PoolStats, PooledConnection, RecycleReason, SimpleConnectionPool,
    };

    // Dialect types
    pub use crate::dialect::{
        dialect_for, MariaDbDialect, MySqlDialect, PostgresDialect, SqlDialect, SqlServerDialect,
    };

    // Schema types
    pub use crate::schema::{
        AutoDdlMode, ForeignKeyAction, ForeignKeyMetadata, IdentityNaming, IndexMetadata,
        PrefixNaming, SchemaEvolutionMode, SchemaEvolutionResult, SchemaManager, SchemaMapping,
        SchemaProvider, SuffixNaming, TableNamingStrategy,
    };

    // Source types
    pub use crate::source::{
        AtomicSourceStats, PollResult, QueryMode, SourceOffset, SourceQueryBuilder, SourceRecord,
        SourceStats, TableSource, TableSourceConfig,
    };

    // Sink types
    pub use crate::sink::{
        AtomicSinkStats, BatchConfig, BatchResult, BufferedSink, FailedRecord, SinkRecord,
        SinkStats, TableSink, TableSinkBuilder, TableSinkConfig, WriteMode,
    };
}

// Re-export commonly used items at crate root
pub use error::{Error, Result};
pub use types::Value;

#[cfg(test)]
mod tests {
    use super::prelude::*;

    #[test]
    fn test_prelude_imports() {
        // Ensure common types are accessible
        let _value = Value::Int32(42);
        let _config = ConnectionConfig::new("postgres://localhost/test");
        let _batch = BatchConfig::default();
        let _mode = WriteMode::Upsert;
    }

    #[test]
    fn test_error_types() {
        let err = Error::connection("test error");
        assert!(err.is_retriable());
        assert_eq!(err.category(), ErrorCategory::Connection);
    }

    #[test]
    fn test_value_types() {
        let v = Value::from(42_i32);
        assert!(!v.is_null());
        assert_eq!(v.as_i64(), Some(42));

        let v = Value::from("hello");
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_table_metadata() {
        let mut table = TableMetadata::new("users");
        table.schema = Some("public".into());

        assert_eq!(table.qualified_name(), "public.users");
        assert!(table.columns.is_empty());
    }

    #[test]
    fn test_query_modes() {
        assert!(!QueryMode::Bulk.is_incremental());
        assert!(QueryMode::incrementing("id").is_incremental());
        assert!(QueryMode::timestamp("updated_at").is_incremental());
    }

    #[test]
    fn test_write_modes() {
        assert_eq!(WriteMode::default(), WriteMode::Upsert);
    }

    #[test]
    fn test_dialect_selection() {
        let pg = dialect_for("postgres");
        assert_eq!(pg.name(), "PostgreSQL");

        let mysql = dialect_for("mysql");
        assert_eq!(mysql.name(), "MySQL");

        let mssql = dialect_for("sqlserver");
        assert_eq!(mssql.name(), "SQL Server");
    }

    #[test]
    fn test_source_config() {
        let config = TableSourceConfig::incrementing("events", "id")
            .with_schema("public")
            .with_batch_size(500);

        assert_eq!(config.table, "events");
        assert_eq!(config.schema, Some("public".into()));
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn test_sink_config() {
        let config = TableSinkBuilder::new()
            .batch_size(2000)
            .write_mode(WriteMode::Insert)
            .auto_ddl(AutoDdlMode::CreateAndEvolve)
            .build();

        assert_eq!(config.batch.max_size, 2000);
        assert_eq!(config.write_mode, WriteMode::Insert);
        assert_eq!(config.auto_ddl, AutoDdlMode::CreateAndEvolve);
    }
}
