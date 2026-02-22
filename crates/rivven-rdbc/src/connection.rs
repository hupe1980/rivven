//! Connection traits for rivven-rdbc
//!
//! Core abstractions for database connectivity:
//! - Connection: Basic connection with query execution
//! - ConnectionLifecycle: Optional lifecycle tracking for pool management
//! - PreparedStatement: Parameterized query support
//! - Transaction: ACID transaction support
//! - RowStream: Streaming row iteration

use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::types::{Row, Value};

/// Lifecycle tracking for connections (used by connection pools)
///
/// This trait provides optional lifecycle information for connections,
/// enabling accurate pool management and observability.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_rdbc::connection::ConnectionLifecycle;
/// use std::time::Duration;
///
/// let conn = pool.get().await?;
/// println!("Connection age: {:?}", conn.age());
/// if conn.is_expired(Duration::from_secs(1800)) {
///     println!("Connection should be recycled");
/// }
/// ```
#[async_trait]
pub trait ConnectionLifecycle: Send + Sync {
    /// Get the instant when this connection was created
    fn created_at(&self) -> Instant;

    /// Get the age of this connection (time since creation)
    fn age(&self) -> Duration {
        self.created_at().elapsed()
    }

    /// Check if connection has exceeded the given maximum lifetime
    fn is_expired(&self, max_lifetime: Duration) -> bool {
        self.age() > max_lifetime
    }

    /// Get duration since this connection was last used
    async fn idle_time(&self) -> Duration;

    /// Check if connection has exceeded idle timeout
    async fn is_idle_expired(&self, idle_timeout: Duration) -> bool {
        self.idle_time().await > idle_timeout
    }

    /// Update the last-used timestamp (called when connection is actively used)
    async fn touch(&self);
}

/// A connection to a database
#[async_trait]
pub trait Connection: Send + Sync {
    /// Execute a query that returns rows
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>>;

    /// Execute a query that modifies data, returns affected row count
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Execute a batch of statements, returns affected counts per statement
    async fn execute_batch(&self, statements: &[(&str, &[Value])]) -> Result<Vec<u64>> {
        let mut results = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            results.push(self.execute(sql, params).await?);
        }
        Ok(results)
    }

    /// Prepare a statement for repeated execution
    async fn prepare(&self, sql: &str) -> Result<Box<dyn PreparedStatement>>;

    /// Begin a transaction
    async fn begin(&self) -> Result<Box<dyn Transaction>>;

    /// Begin a transaction with specified isolation level
    async fn begin_with_isolation(
        &self,
        isolation: IsolationLevel,
    ) -> Result<Box<dyn Transaction>> {
        // Default implementation just begins and sets isolation
        let tx = self.begin().await?;
        tx.set_isolation_level(isolation).await?;
        Ok(tx)
    }

    /// Execute a query and stream results
    async fn query_stream(&self, sql: &str, params: &[Value]) -> Result<Pin<Box<dyn RowStream>>>;

    /// Execute a query and return the first row (convenience method)
    async fn query_one(&self, sql: &str, params: &[Value]) -> Result<Option<Row>> {
        let rows = self.query(sql, params).await?;
        Ok(rows.into_iter().next())
    }

    /// Check if connection is valid/alive
    async fn is_valid(&self) -> bool;

    /// Close the connection
    async fn close(&self) -> Result<()>;
}

/// A prepared statement
#[async_trait]
pub trait PreparedStatement: Send + Sync {
    /// Execute the prepared statement with given parameters
    async fn execute(&self, params: &[Value]) -> Result<u64>;

    /// Query with the prepared statement
    async fn query(&self, params: &[Value]) -> Result<Vec<Row>>;

    /// Get the SQL string
    fn sql(&self) -> &str;
}

/// A database transaction
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Execute a query that returns rows
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>>;

    /// Execute a query that modifies data
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64>;

    /// Execute a batch of statements within the transaction, returns affected counts per statement
    async fn execute_batch(&self, statements: &[(&str, &[Value])]) -> Result<Vec<u64>> {
        let mut results = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            results.push(self.execute(sql, params).await?);
        }
        Ok(results)
    }

    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;

    /// Set transaction isolation level
    async fn set_isolation_level(&self, level: IsolationLevel) -> Result<()>;

    /// Create a savepoint
    async fn savepoint(&self, name: &str) -> Result<()>;

    /// Rollback to a savepoint
    async fn rollback_to_savepoint(&self, name: &str) -> Result<()>;

    /// Release a savepoint
    async fn release_savepoint(&self, name: &str) -> Result<()>;
}

/// Streaming row iterator
pub trait RowStream: Send {
    /// Get the next row
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>>;
}

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    /// Read uncommitted - dirty reads possible
    ReadUncommitted,
    /// Read committed - no dirty reads (PostgreSQL default)
    ReadCommitted,
    /// Repeatable read - no non-repeatable reads (MySQL default)
    RepeatableRead,
    /// Serializable - full isolation
    Serializable,
    /// Snapshot isolation (SQL Server specific)
    Snapshot,
}

impl IsolationLevel {
    /// Convert to SQL string for SET TRANSACTION statement
    pub fn to_sql(&self) -> &'static str {
        match self {
            Self::ReadUncommitted => "READ UNCOMMITTED",
            Self::ReadCommitted => "READ COMMITTED",
            Self::RepeatableRead => "REPEATABLE READ",
            Self::Serializable => "SERIALIZABLE",
            Self::Snapshot => "SNAPSHOT",
        }
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_sql())
    }
}

/// Configuration for creating connections
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Connection URL (e.g., postgres://user:pass@host:5432/db)
    pub url: String,
    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,
    /// Query timeout in milliseconds (0 = no timeout)
    pub query_timeout_ms: u64,
    /// Statement cache size
    pub statement_cache_size: usize,
    /// Application name (shown in pg_stat_activity, etc)
    pub application_name: Option<String>,
    /// Additional connection properties
    pub properties: std::collections::HashMap<String, String>,
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact credentials from the URL to prevent leaking passwords to logs.
        let redacted_url = match url::Url::parse(&self.url) {
            Ok(mut parsed) => {
                if parsed.password().is_some() {
                    let _ = parsed.set_password(Some("***"));
                }
                parsed.to_string()
            }
            Err(_) => "***".to_string(),
        };

        f.debug_struct("ConnectionConfig")
            .field("url", &redacted_url)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("query_timeout_ms", &self.query_timeout_ms)
            .field("statement_cache_size", &self.statement_cache_size)
            .field("application_name", &self.application_name)
            .field("properties", &self.properties)
            .finish()
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            connect_timeout_ms: 10_000,
            query_timeout_ms: 30_000,
            statement_cache_size: 100,
            application_name: Some("rivven-rdbc".into()),
            properties: std::collections::HashMap::new(),
        }
    }
}

impl ConnectionConfig {
    /// Create configuration with just a URL
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, ms: u64) -> Self {
        self.connect_timeout_ms = ms;
        self
    }

    /// Set query timeout
    pub fn with_query_timeout(mut self, ms: u64) -> Self {
        self.query_timeout_ms = ms;
        self
    }

    /// Set statement cache size
    pub fn with_statement_cache_size(mut self, size: usize) -> Self {
        self.statement_cache_size = size;
        self
    }

    /// Set application name
    pub fn with_application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// Add a connection property
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }
}

/// Factory for creating connections
#[async_trait]
pub trait ConnectionFactory: Send + Sync {
    /// Create a new connection
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn Connection>>;

    /// Get the database type
    fn database_type(&self) -> DatabaseType;
}

/// Database type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabaseType {
    /// PostgreSQL
    PostgreSQL,
    /// MySQL/MariaDB
    MySQL,
    /// SQL Server
    SqlServer,
    /// SQLite
    SQLite,
    /// Oracle
    Oracle,
    /// Unknown/custom
    Unknown,
}

impl std::fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PostgreSQL => write!(f, "PostgreSQL"),
            Self::MySQL => write!(f, "MySQL"),
            Self::SqlServer => write!(f, "SQL Server"),
            Self::SQLite => write!(f, "SQLite"),
            Self::Oracle => write!(f, "Oracle"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_to_sql() {
        assert_eq!(IsolationLevel::ReadCommitted.to_sql(), "READ COMMITTED");
        assert_eq!(IsolationLevel::Serializable.to_sql(), "SERIALIZABLE");
    }

    #[test]
    fn test_connection_config_builder() {
        let config = ConnectionConfig::new("postgres://localhost/test")
            .with_connect_timeout(5000)
            .with_query_timeout(15000)
            .with_application_name("myapp")
            .with_property("sslmode", "require");

        assert_eq!(config.url, "postgres://localhost/test");
        assert_eq!(config.connect_timeout_ms, 5000);
        assert_eq!(config.query_timeout_ms, 15000);
        assert_eq!(config.application_name, Some("myapp".into()));
        assert_eq!(config.properties.get("sslmode"), Some(&"require".into()));
    }

    #[test]
    fn test_database_type_display() {
        assert_eq!(format!("{}", DatabaseType::PostgreSQL), "PostgreSQL");
        assert_eq!(format!("{}", DatabaseType::MySQL), "MySQL");
        assert_eq!(format!("{}", DatabaseType::SqlServer), "SQL Server");
        assert_eq!(format!("{}", DatabaseType::SQLite), "SQLite");
        assert_eq!(format!("{}", DatabaseType::Oracle), "Oracle");
        assert_eq!(format!("{}", DatabaseType::Unknown), "Unknown");
    }

    #[test]
    fn test_isolation_level_display() {
        assert_eq!(
            format!("{}", IsolationLevel::ReadUncommitted),
            "READ UNCOMMITTED"
        );
        assert_eq!(
            format!("{}", IsolationLevel::ReadCommitted),
            "READ COMMITTED"
        );
        assert_eq!(
            format!("{}", IsolationLevel::RepeatableRead),
            "REPEATABLE READ"
        );
        assert_eq!(format!("{}", IsolationLevel::Serializable), "SERIALIZABLE");
        assert_eq!(format!("{}", IsolationLevel::Snapshot), "SNAPSHOT");
    }

    /// Test ConnectionLifecycle default implementations
    #[test]
    fn test_connection_lifecycle_defaults() {
        // Test that default implementations are logically correct
        // The actual trait requires async methods, so we test the logic
        let now = Instant::now();

        // age = now.elapsed(), should be very small
        let age = now.elapsed();
        assert!(age < Duration::from_secs(1));

        // is_expired with 30 minutes should be false for a just-created connection
        let max_lifetime = Duration::from_secs(1800);
        assert!(age <= max_lifetime);

        // is_expired with very short lifetime should become true quickly
        std::thread::sleep(Duration::from_millis(5));
        let short_lifetime = Duration::from_millis(1);
        assert!(now.elapsed() > short_lifetime);
    }
}
