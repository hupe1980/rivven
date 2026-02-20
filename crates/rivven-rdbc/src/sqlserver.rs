//! SQL Server backend implementation for rivven-rdbc
//!
//! Provides Microsoft SQL Server-specific implementations:
//! - Connection and prepared statements
//! - Transaction support with savepoints
//! - Streaming row iteration
//! - Connection pooling
//! - Schema provider for introspection

use async_trait::async_trait;
use std::future::Future;
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
use crate::security::validate_sql_identifier;
use crate::types::{Row, Value};

/// Streaming row iterator backed by a Vec.
///
/// tiberius processes results in result-set granularity; this adapter
/// yields rows one-by-one from a materialized result. Preserves the
/// `RowStream` contract for callers while keeping memory behaviour
/// explicit and documented.
struct VecRowStream {
    rows: std::vec::IntoIter<Row>,
}

impl VecRowStream {
    fn new(rows: Vec<Row>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl RowStream for VecRowStream {
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<Option<Row>>> + Send + '_>> {
        Box::pin(async move { Ok(self.rows.next()) })
    }
}

/// SQL Server connection
pub struct SqlServerConnection {
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
    in_transaction: Arc<AtomicBool>,
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
            in_transaction: Arc::new(AtomicBool::new(false)),
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

/// Owned parameter wrapper for safe, native tiberius parameter binding (C-1 fix).
///
/// Converts rivven `Value` to tiberius `ColumnData` for typed TDS protocol binding.
/// Parameters are **never interpolated into SQL text** — they are sent as typed
/// protocol-level parameters, making SQL injection impossible regardless of content.
///
/// # Security
///
/// This replaces the previous `substitute_params()` + `value_to_string()` approach
/// which performed client-side string interpolation, allowing SQL injection on any
/// Value that contained SQL metacharacters.
struct SqlParam(Value);

impl tiberius::ToSql for SqlParam {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        use std::borrow::Cow;
        use tiberius::ColumnData;
        use Value::*;

        match &self.0 {
            Null => ColumnData::String(None),
            Bool(b) => ColumnData::Bit(Some(*b)),
            Int8(n) => ColumnData::I16(Some(*n as i16)), // TDS has no i8
            Int16(n) => ColumnData::I16(Some(*n)),
            Int32(n) => ColumnData::I32(Some(*n)),
            Int64(n) => ColumnData::I64(Some(*n)),
            Float32(n) => ColumnData::F32(Some(*n)),
            Float64(n) => ColumnData::F64(Some(*n)),
            String(s) => ColumnData::String(Some(Cow::Borrowed(s.as_str()))),
            Bytes(b) => ColumnData::Binary(Some(Cow::Borrowed(b.as_slice()))),
            Uuid(u) => ColumnData::Guid(Some(*u)),
            // chrono types → ISO 8601 string representation (SQL Server parses these natively)
            // Using string parameters is safe: the value is bound as a typed TDS parameter,
            // not interpolated into SQL text. SQL Server handles the implicit conversion.
            Date(d) => ColumnData::String(Some(Cow::Owned(d.format("%Y-%m-%d").to_string()))),
            Time(t) => ColumnData::String(Some(Cow::Owned(t.format("%H:%M:%S%.f").to_string()))),
            DateTime(dt) => ColumnData::String(Some(Cow::Owned(
                dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
            ))),
            DateTimeTz(dt) => ColumnData::String(Some(Cow::Owned(
                dt.format("%Y-%m-%dT%H:%M:%S%.f%:z").to_string(),
            ))),
            // Types without direct TDS mapping → string representation
            Decimal(d) => ColumnData::String(Some(Cow::Owned(d.to_string()))),
            Json(j) => ColumnData::String(Some(Cow::Owned(j.to_string()))),
            Enum(s) => ColumnData::String(Some(Cow::Borrowed(s.as_str()))),
            Array(arr) => {
                let json = serde_json::to_string(arr).unwrap_or_else(|e| {
                    tracing::warn!(
                        "Failed to serialize Array to JSON for SQL Server param: {}",
                        e
                    );
                    "[]".to_string()
                });
                ColumnData::String(Some(Cow::Owned(json)))
            }
            Composite(map) => {
                let json = serde_json::to_string(map).unwrap_or_else(|e| {
                    tracing::warn!(
                        "Failed to serialize Composite to JSON for SQL Server param: {}",
                        e
                    );
                    "{}".to_string()
                });
                ColumnData::String(Some(Cow::Owned(json)))
            }
            Interval(micros) => ColumnData::I64(Some(*micros)),
            Bit(bits) => ColumnData::Binary(Some(Cow::Borrowed(bits.as_slice()))),
            Geometry(wkb) | Geography(wkb) => {
                ColumnData::Binary(Some(Cow::Borrowed(wkb.as_slice())))
            }
            Range { .. } => ColumnData::String(None), // ranges not natively supported in SQL Server
            Custom { data, .. } => ColumnData::Binary(Some(Cow::Borrowed(data.as_slice()))),
        }
    }
}

/// Build a slice of tiberius parameter references from owned SqlParams.
///
/// Returns the param_refs vector. Caller must keep `tib_params` alive
/// for the duration of the query call since `param_refs` borrows from them.
#[inline]
fn param_refs(tib_params: &[SqlParam]) -> Vec<&dyn tiberius::ToSql> {
    tib_params
        .iter()
        .map(|p| p as &dyn tiberius::ToSql)
        .collect()
}

/// Convert tiberius column value to rivven Value
fn tiberius_to_value(_col: &tiberius::Column, row: &tiberius::Row, idx: usize) -> Value {
    // probe typed columns before raw bytes to avoid BIT returning as Bytes.
    // Order: bool first, then numeric, then string, then bytes (catch-all).
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
    if let Ok(Some(bytes)) = row.try_get::<&[u8], _>(idx) {
        return Value::Bytes(bytes.to_vec());
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

        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let result = client
            .execute(query, &refs)
            .await
            .map_err(|e| Error::execution(format!("Execute failed: {}", e)))?;

        Ok(result.total() as u64)
    }

    async fn query(&self, query: &str, params: &[Value]) -> Result<Vec<Row>> {
        self.update_last_used().await;

        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let stream = client
            .query(query, &refs)
            .await
            .map_err(|e| Error::execution(format!("Query failed: {}", e)))?;

        let tib_rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::execution(format!("Failed to fetch rows: {}", e)))?;

        Ok(tib_rows.iter().map(tiberius_row_to_row).collect())
    }

    async fn prepare(&self, sql: &str) -> Result<Box<dyn PreparedStatement>> {
        // SQL Server uses parameterized queries (sp_executesql), not server-side
        // prepared statements. The connection reference enables execute/query.
        Ok(Box::new(SqlServerPreparedStatement {
            sql: sql.to_string(),
            client: Arc::clone(&self.client),
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
            in_transaction: Arc::clone(&self.in_transaction),
        }))
    }

    /// SQL Server requires SET TRANSACTION ISOLATION LEVEL *before*
    /// BEGIN TRANSACTION for it to take effect for that transaction.
    async fn begin_with_isolation(
        &self,
        isolation: IsolationLevel,
    ) -> Result<Box<dyn Transaction>> {
        self.update_last_used().await;

        let level_sql = match isolation {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
            IsolationLevel::Snapshot => "SNAPSHOT",
        };

        {
            let mut client = self.client.lock().await;
            client
                .execute(
                    format!("SET TRANSACTION ISOLATION LEVEL {}", level_sql),
                    &[],
                )
                .await
                .map_err(|e| Error::transaction(format!("Failed to set isolation level: {}", e)))?;
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
            in_transaction: Arc::clone(&self.in_transaction),
        }))
    }

    async fn query_stream(&self, query: &str, params: &[Value]) -> Result<Pin<Box<dyn RowStream>>> {
        // Fetch all rows and wrap in a streaming adapter.
        // tiberius's TDS protocol pipeline processes results in result-set
        // granularity; this adapter exposes them as a RowStream for callers.
        // For large result sets, callers should use OFFSET/FETCH pagination
        // or the TableSource incremental mode.
        let rows = self.query(query, params).await?;
        Ok(Box::pin(VecRowStream::new(rows)))
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

/// SQL Server prepared statement backed by a shared connection.
///
/// SQL Server uses parameterized queries (sp_executesql) rather than
/// server-side prepared statements. This wrapper stores the query text
/// and a reference to the parent connection for execute/query delegation.
pub struct SqlServerPreparedStatement {
    sql: String,
    client: Arc<Mutex<Client<Compat<TcpStream>>>>,
}

#[async_trait]
impl PreparedStatement for SqlServerPreparedStatement {
    async fn execute(&self, params: &[Value]) -> Result<u64> {
        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let result = client
            .execute(&*self.sql, &refs)
            .await
            .map_err(|e| Error::execution(format!("Prepared execute failed: {}", e)))?;

        Ok(result.total() as u64)
    }

    async fn query(&self, params: &[Value]) -> Result<Vec<Row>> {
        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let stream = client
            .query(&*self.sql, &refs)
            .await
            .map_err(|e| Error::execution(format!("Prepared query failed: {}", e)))?;

        let tib_rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::execution(format!("Failed to fetch rows: {}", e)))?;

        Ok(tib_rows.iter().map(tiberius_row_to_row).collect())
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
    in_transaction: Arc<AtomicBool>,
}

#[async_trait]
impl Transaction for SqlServerTransaction {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let stream = client
            .query(sql, &refs)
            .await
            .map_err(|e| Error::execution(format!("Query failed: {}", e)))?;

        let tib_rows = stream
            .into_first_result()
            .await
            .map_err(|e| Error::execution(format!("Failed to fetch rows: {}", e)))?;

        Ok(tib_rows.iter().map(tiberius_row_to_row).collect())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let tib_params: Vec<SqlParam> = params.iter().cloned().map(SqlParam).collect();
        let refs = param_refs(&tib_params);
        let mut client = self.client.lock().await;

        let result = client
            .execute(sql, &refs)
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
        self.in_transaction.store(false, Ordering::SeqCst);
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
        self.in_transaction.store(false, Ordering::SeqCst);
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
        validate_sql_identifier(name)?;
        let mut client = self.client.lock().await;
        client
            .execute(format!("SAVE TRANSACTION {}", name), &[])
            .await
            .map_err(|e| Error::transaction(format!("Failed to create savepoint: {}", e)))?;
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        validate_sql_identifier(name)?;
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

// Drop impl to prevent abandoned transactions. If the transaction was neither
// committed nor rolled back, issue a best-effort ROLLBACK and reset the
// parent connection's in_transaction flag.
impl Drop for SqlServerTransaction {
    fn drop(&mut self) {
        if !self.committed.load(Ordering::SeqCst) && !self.rolled_back.load(Ordering::SeqCst) {
            let client = self.client.clone();
            let in_tx = self.in_transaction.clone();
            tokio::task::block_in_place(|| {
                let rt = tokio::runtime::Handle::current();
                rt.block_on(async {
                    let mut guard = client.lock().await;
                    if let Err(e) = guard.execute("ROLLBACK TRANSACTION", &[]).await {
                        tracing::warn!("Auto-rollback on SqlServerTransaction drop failed: {}", e);
                    } else {
                        tracing::debug!("SqlServerTransaction auto-rolled back on drop");
                    }
                    in_tx.store(false, Ordering::SeqCst);
                });
            });
        }
    }
}

/// SQL Server connection factory
pub struct SqlServerConnectionFactory {
    #[allow(dead_code)] // Stored for future use; connect() uses passed config
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
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn Connection>> {
        // Use passed config (consistent with ConnectionFactory contract)
        let conn = SqlServerConnection::connect(config).await?;
        Ok(Box::new(conn))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::SqlServer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiberius::ToSql;

    #[test]
    fn test_sql_param_null() {
        let p = SqlParam(Value::Null);
        let cd = p.to_sql();
        // Null string → None variant
        assert!(matches!(cd, tiberius::ColumnData::String(None)));
    }

    #[test]
    fn test_sql_param_bool() {
        let p = SqlParam(Value::Bool(true));
        let cd = p.to_sql();
        assert!(matches!(cd, tiberius::ColumnData::Bit(Some(true))));
    }

    #[test]
    fn test_sql_param_integers() {
        assert!(matches!(
            SqlParam(Value::Int8(42)).to_sql(),
            tiberius::ColumnData::I16(Some(42))
        ));
        assert!(matches!(
            SqlParam(Value::Int16(1000)).to_sql(),
            tiberius::ColumnData::I16(Some(1000))
        ));
        assert!(matches!(
            SqlParam(Value::Int32(100_000)).to_sql(),
            tiberius::ColumnData::I32(Some(100_000))
        ));
        assert!(matches!(
            SqlParam(Value::Int64(1_000_000_000)).to_sql(),
            tiberius::ColumnData::I64(Some(1_000_000_000))
        ));
    }

    #[test]
    fn test_sql_param_string() {
        let p = SqlParam(Value::String("hello".into()));
        if let tiberius::ColumnData::String(Some(cow)) = p.to_sql() {
            assert_eq!(&*cow, "hello");
        } else {
            panic!("Expected String ColumnData");
        }
    }

    #[test]
    fn test_sql_param_string_with_injection_chars() {
        // SQL metacharacters are harmless because the value is bound as a typed
        // parameter — never interpolated into SQL text
        let p = SqlParam(Value::String("x'; DROP TABLE users--".into()));
        if let tiberius::ColumnData::String(Some(cow)) = p.to_sql() {
            assert_eq!(&*cow, "x'; DROP TABLE users--");
        } else {
            panic!("Expected String ColumnData");
        }
    }

    #[test]
    fn test_sql_param_bytes() {
        let p = SqlParam(Value::Bytes(vec![0xDE, 0xAD]));
        if let tiberius::ColumnData::Binary(Some(cow)) = p.to_sql() {
            assert_eq!(&*cow, &[0xDE, 0xAD]);
        } else {
            panic!("Expected Binary ColumnData");
        }
    }

    #[test]
    fn test_sql_param_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let p = SqlParam(Value::Uuid(uuid));
        assert!(matches!(p.to_sql(), tiberius::ColumnData::Guid(Some(_))));
    }

    #[test]
    fn test_sql_param_chrono_types() {
        use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};

        let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        let _cd = SqlParam(Value::Date(d)).to_sql();

        let t = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let _cd = SqlParam(Value::Time(t)).to_sql();

        let dt = NaiveDateTime::new(d, t);
        let _cd = SqlParam(Value::DateTime(dt)).to_sql();

        let dtz = Utc::now();
        let _cd = SqlParam(Value::DateTimeTz(dtz)).to_sql();
    }

    #[test]
    fn test_sql_param_json() {
        let j = serde_json::json!({"key": "value"});
        let p = SqlParam(Value::Json(j));
        if let tiberius::ColumnData::String(Some(cow)) = p.to_sql() {
            assert!(cow.contains("key"));
        } else {
            panic!("Expected String ColumnData for JSON");
        }
    }

    #[test]
    fn test_savepoint_name_validation() {
        // Valid names succeed
        assert!(validate_sql_identifier("sp1").is_ok());
        assert!(validate_sql_identifier("my_savepoint").is_ok());

        // Injection attempts are rejected
        assert!(validate_sql_identifier("x; DROP TABLE users--").is_err());
        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("x' OR '1'='1").is_err());
    }

    #[test]
    fn test_connection_config() {
        use crate::connection::ConnectionConfig;
        let config = ConnectionConfig::new("sqlserver://user:pass@localhost:1433/mydb");
        assert_eq!(config.url, "sqlserver://user:pass@localhost:1433/mydb");
    }
}
