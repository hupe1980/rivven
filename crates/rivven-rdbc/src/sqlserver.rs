//! SQL Server backend implementation for rivven-rdbc
//!
//! Provides Microsoft SQL Server-specific implementations:
//! - Connection and prepared statements
//! - Transaction support with savepoints
//! - Streaming row iteration
//! - Connection pooling
//! - Schema provider for introspection

use async_trait::async_trait;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::connection::{
    Connection, ConnectionConfig, ConnectionFactory, ConnectionLifecycle, DatabaseType,
    IsolationLevel, PreparedStatement, RowStream, Transaction,
};
use crate::error::{Error, Result};
use crate::types::{Row, Value};

/// SQL Server connection
pub struct SqlServerConnection {
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
    in_transaction: AtomicBool,
    created_at: Instant,
    last_used: Mutex<Instant>,
}

impl SqlServerConnection {
    /// Get the age of this connection (time since creation)
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Check if connection is older than the specified max lifetime
    pub fn is_expired(&self, max_lifetime: std::time::Duration) -> bool {
        self.age() > max_lifetime
    }

    /// Get time since last use
    pub async fn idle_time(&self) -> std::time::Duration {
        self.last_used.lock().await.elapsed()
    }

    /// Create a new SQL Server connection from config
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        // Parse URL: sqlserver://user:pass@host:port/database
        let url = url::Url::parse(&config.url)
            .map_err(|e| Error::config(format!("Invalid SQL Server URL: {}", e)))?;

        let mut tib_config = Config::new();

        tib_config.host(url.host_str().unwrap_or("localhost"));
        tib_config.port(url.port().unwrap_or(1433));
        tib_config.database(url.path().trim_start_matches('/'));

        // Authentication from URL
        let username = if url.username().is_empty() {
            "sa"
        } else {
            url.username()
        };
        let password = url.password().unwrap_or("");
        tib_config.authentication(AuthMethod::sql_server(username, password));

        // TLS settings from properties
        if config
            .properties
            .get("trust_cert")
            .map(|s| s == "true")
            .unwrap_or(false)
        {
            tib_config.trust_cert();
        }

        let tcp = TcpStream::connect(tib_config.get_addr())
            .await
            .map_err(|e| Error::connection(format!("Failed to connect: {}", e)))?;

        tcp.set_nodelay(true).ok();

        let client = Client::connect(tib_config, tcp.compat_write())
            .await
            .map_err(|e| Error::connection(format!("Failed to authenticate: {}", e)))?;

        let now = Instant::now();
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            in_transaction: AtomicBool::new(false),
            created_at: now,
            last_used: Mutex::new(now),
        })
    }

    /// Create connection from URL
    pub async fn from_url(url: &str) -> Result<Self> {
        let config = ConnectionConfig::new(url);
        Self::connect(&config).await
    }

    async fn update_last_used(&self) {
        *self.last_used.lock().await = Instant::now();
    }
}

#[async_trait]
impl ConnectionLifecycle for SqlServerConnection {
    fn created_at(&self) -> Instant {
        self.created_at
    }

    async fn idle_time(&self) -> std::time::Duration {
        self.last_used.lock().await.elapsed()
    }

    async fn touch(&self) {
        self.update_last_used().await;
    }
}

/// Convert rivven Value to SQL Server parameter format
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::Int8(n) => n.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float32(n) => n.to_string(),
        Value::Float64(n) => n.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => format!("N'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!("0x{}", hex::encode(b)),
        Value::Date(d) => format!("'{}'", d),
        Value::Time(t) => format!("'{}'", t),
        Value::DateTime(dt) => format!("'{}'", dt),
        Value::DateTimeTz(dt) => format!("'{}'", dt),
        Value::Uuid(u) => format!("'{}'", u),
        Value::Json(j) => format!("N'{}'", j.to_string().replace('\'', "''")),
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_default();
            format!("N'{}'", json.replace('\'', "''"))
        }
        Value::Interval(micros) => micros.to_string(),
        Value::Bit(bits) => format!("0x{}", hex::encode(bits)),
        Value::Enum(s) => format!("N'{}'", s.replace('\'', "''")),
        Value::Geometry(wkb) | Value::Geography(wkb) => format!("0x{}", hex::encode(wkb)),
        Value::Range { .. } => "NULL".to_string(),
        Value::Composite(map) => {
            let json = serde_json::to_string(map).unwrap_or_default();
            format!("N'{}'", json.replace('\'', "''"))
        }
        Value::Custom { data, .. } => format!("0x{}", hex::encode(data)),
    }
}

/// Substitute parameters in SQL query
fn substitute_params(sql: &str, params: &[Value]) -> String {
    let mut result = sql.to_string();
    for (i, param) in params.iter().enumerate() {
        let placeholder = format!("@P{}", i + 1);
        result = result.replacen(&placeholder, &value_to_string(param), 1);
    }
    // Also handle ? placeholders
    for param in params {
        result = result.replacen('?', &value_to_string(param), 1);
    }
    result
}

/// Convert tiberius column value to rivven Value
fn tiberius_to_value(_col: &tiberius::Column, row: &tiberius::Row, idx: usize) -> Value {
    if let Ok(Some(bytes)) = row.try_get::<&[u8], _>(idx) {
        return Value::Bytes(bytes.to_vec());
    }

    // Try different type conversions
    if let Ok(Some(v)) = row.try_get::<bool, _>(idx) {
        return Value::Bool(v);
    }
    if let Ok(Some(v)) = row.try_get::<i16, _>(idx) {
        return Value::Int16(v);
    }
    if let Ok(Some(v)) = row.try_get::<i32, _>(idx) {
        return Value::Int32(v);
    }
    if let Ok(Some(v)) = row.try_get::<i64, _>(idx) {
        return Value::Int64(v);
    }
    if let Ok(Some(v)) = row.try_get::<f32, _>(idx) {
        return Value::Float32(v);
    }
    if let Ok(Some(v)) = row.try_get::<f64, _>(idx) {
        return Value::Float64(v);
    }
    if let Ok(Some(v)) = row.try_get::<&str, _>(idx) {
        return Value::String(v.to_string());
    }
    if let Ok(Some(v)) = row.try_get::<uuid::Uuid, _>(idx) {
        return Value::Uuid(v);
    }

    Value::Null
}

/// Convert tiberius Row to rivven Row
fn tiberius_row_to_row(tib_row: &tiberius::Row) -> Row {
    let columns: Vec<String> = tib_row
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    let values: Vec<Value> = tib_row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| tiberius_to_value(col, tib_row, i))
        .collect();

    Row::new(columns, values)
}

#[async_trait]
impl Connection for SqlServerConnection {
    async fn execute(&self, query: &str, params: &[Value]) -> Result<u64> {
        self.update_last_used().await;

        let sql = substitute_params(query, params);
        let mut client = self.client.lock().await;

        let result = client
            .execute(sql, &[])
            .await
            .map_err(|e| Error::execution(format!("Execute failed: {}", e)))?;

        Ok(result.total() as u64)
    }

    async fn query(&self, query: &str, params: &[Value]) -> Result<Vec<Row>> {
        self.update_last_used().await;

        let sql = substitute_params(query, params);
        let mut client = self.client.lock().await;

        let stream = client
            .query(sql, &[])
            .await
            .map_err(|e| Error::execution(format!("Query failed: {}", e)))?;

        let tib_rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::execution(format!("Failed to fetch rows: {}", e)))?;

        Ok(tib_rows.iter().map(tiberius_row_to_row).collect())
    }

    async fn prepare(&self, sql: &str) -> Result<Box<dyn PreparedStatement>> {
        // SQL Server uses parameterized queries, not server-side prepared statements
        Ok(Box::new(SqlServerPreparedStatement {
            sql: sql.to_string(),
        }))
    }

    async fn begin(&self) -> Result<Box<dyn Transaction>> {
        self.update_last_used().await;

        {
            let mut client = self.client.lock().await;
            client
                .execute("BEGIN TRANSACTION", &[])
                .await
                .map_err(|e| Error::transaction(format!("Failed to begin transaction: {}", e)))?;
        }

        self.in_transaction.store(true, Ordering::SeqCst);

        Ok(Box::new(SqlServerTransaction {
            client: Arc::clone(&self.client),
            committed: AtomicBool::new(false),
            rolled_back: AtomicBool::new(false),
        }))
    }

    async fn query_stream(
        &self,
        _query: &str,
        _params: &[Value],
    ) -> Result<Pin<Box<dyn RowStream>>> {
        // SQL Server streaming needs different approach with async-stream
        Err(Error::unsupported(
            "Streaming not yet implemented for SQL Server",
        ))
    }

    async fn is_valid(&self) -> bool {
        let mut client = self.client.lock().await;
        client.execute("SELECT 1", &[]).await.is_ok()
    }

    async fn close(&self) -> Result<()> {
        // Connection closes when dropped
        Ok(())
    }
}

/// SQL Server prepared statement
pub struct SqlServerPreparedStatement {
    sql: String,
}

#[async_trait]
impl PreparedStatement for SqlServerPreparedStatement {
    async fn execute(&self, _params: &[Value]) -> Result<u64> {
        // This requires a connection reference - for now return error
        Err(Error::unsupported(
            "Use Connection::execute with the query string directly",
        ))
    }

    async fn query(&self, _params: &[Value]) -> Result<Vec<Row>> {
        Err(Error::unsupported(
            "Use Connection::query with the query string directly",
        ))
    }

    fn sql(&self) -> &str {
        &self.sql
    }
}

/// SQL Server transaction
pub struct SqlServerTransaction {
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
    committed: AtomicBool,
    rolled_back: AtomicBool,
}

#[async_trait]
impl Transaction for SqlServerTransaction {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let query = substitute_params(sql, params);
        let mut client = self.client.lock().await;

        let stream = client
            .query(query, &[])
            .await
            .map_err(|e| Error::execution(format!("Query failed: {}", e)))?;

        let tib_rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::execution(format!("Failed to fetch rows: {}", e)))?;

        Ok(tib_rows.iter().map(tiberius_row_to_row).collect())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let query = substitute_params(sql, params);
        let mut client = self.client.lock().await;

        let result = client
            .execute(query, &[])
            .await
            .map_err(|e| Error::execution(format!("Execute failed: {}", e)))?;

        Ok(result.total() as u64)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        if self.rolled_back.load(Ordering::SeqCst) {
            return Err(Error::transaction("Transaction already rolled back"));
        }
        if self.committed.load(Ordering::SeqCst) {
            return Err(Error::transaction("Transaction already committed"));
        }

        let mut client = self.client.lock().await;
        client
            .execute("COMMIT TRANSACTION", &[])
            .await
            .map_err(|e| Error::transaction(format!("Failed to commit: {}", e)))?;

        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        if self.committed.load(Ordering::SeqCst) {
            return Err(Error::transaction("Transaction already committed"));
        }
        if self.rolled_back.load(Ordering::SeqCst) {
            return Ok(()); // Idempotent rollback
        }

        let mut client = self.client.lock().await;
        client
            .execute("ROLLBACK TRANSACTION", &[])
            .await
            .map_err(|e| Error::transaction(format!("Failed to rollback: {}", e)))?;

        self.rolled_back.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn set_isolation_level(&self, level: IsolationLevel) -> Result<()> {
        let level_sql = match level {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::Snapshot => "SNAPSHOT",
        };

        let mut client = self.client.lock().await;
        client
            .execute(
                format!("SET TRANSACTION ISOLATION LEVEL {}", level_sql),
                &[],
            )
            .await
            .map_err(|e| Error::transaction(format!("Failed to set isolation level: {}", e)))?;

        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .execute(format!("SAVE TRANSACTION {}", name), &[])
            .await
            .map_err(|e| Error::transaction(format!("Failed to create savepoint: {}", e)))?;
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .execute(format!("ROLLBACK TRANSACTION {}", name), &[])
            .await
            .map_err(|e| Error::transaction(format!("Failed to rollback to savepoint: {}", e)))?;
        Ok(())
    }

    async fn release_savepoint(&self, _name: &str) -> Result<()> {
        // SQL Server doesn't have RELEASE SAVEPOINT - savepoints are released on commit
        Ok(())
    }
}

/// SQL Server connection factory
pub struct SqlServerConnectionFactory {
    config: ConnectionConfig,
}

impl SqlServerConnectionFactory {
    /// Create a new SQL Server connection factory
    pub fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ConnectionFactory for SqlServerConnectionFactory {
    async fn connect(&self, _config: &ConnectionConfig) -> Result<Box<dyn Connection>> {
        let conn = SqlServerConnection::connect(&self.config).await?;
        Ok(Box::new(conn))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::SqlServer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_params() {
        let sql = "SELECT * FROM users WHERE id = @P1 AND name = @P2";
        let params = vec![Value::Int32(1), Value::String("test".to_string())];
        let result = substitute_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 1 AND name = N'test'"
        );
    }

    #[test]
    fn test_substitute_params_question_marks() {
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let params = vec![Value::Int32(1), Value::String("test".to_string())];
        let result = substitute_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM users WHERE id = 1 AND name = N'test'"
        );
    }

    #[test]
    fn test_value_to_string() {
        assert_eq!(value_to_string(&Value::Null), "NULL");
        assert_eq!(value_to_string(&Value::Bool(true)), "1");
        assert_eq!(value_to_string(&Value::Bool(false)), "0");
        assert_eq!(value_to_string(&Value::Int32(42)), "42");
        assert_eq!(
            value_to_string(&Value::String("hello".to_string())),
            "N'hello'"
        );
        assert_eq!(
            value_to_string(&Value::String("don't".to_string())),
            "N'don''t'"
        );
    }

    #[test]
    fn test_connection_config() {
        use crate::connection::ConnectionConfig;
        let config = ConnectionConfig::new("sqlserver://user:pass@localhost:1433/mydb");
        assert_eq!(config.url, "sqlserver://user:pass@localhost:1433/mydb");
    }
}
