//! MySQL backend implementation for rivven-rdbc
//!
//! Provides MySQL-specific implementations:
//! - Connection and prepared statements
//! - Transaction support
//! - Streaming row iteration
//! - Connection pooling
//! - Schema provider for introspection

use async_trait::async_trait;
use chrono::{Datelike, Timelike};
use mysql_async::prelude::*;
use mysql_async::{Conn, OptsBuilder};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::connection::{
    Connection, ConnectionConfig, ConnectionFactory, ConnectionLifecycle, DatabaseType,
    IsolationLevel, PreparedStatement, RowStream, Transaction,
};
use crate::error::{Error, Result};
use crate::schema::{
    ForeignKeyAction, ForeignKeyMetadata, IndexMetadata, SchemaEvolutionMode,
    SchemaEvolutionResult, SchemaManager, SchemaProvider,
};
use crate::types::{ColumnMetadata, Row, TableMetadata, Value};

/// Convert a rivven Value to a MySQL compatible parameter
fn value_to_sql(value: &Value) -> mysql_async::Value {
    match value {
        Value::Null => mysql_async::Value::NULL,
        Value::Bool(b) => mysql_async::Value::from(*b),
        Value::Int8(n) => mysql_async::Value::from(*n),
        Value::Int16(n) => mysql_async::Value::from(*n),
        Value::Int32(n) => mysql_async::Value::from(*n),
        Value::Int64(n) => mysql_async::Value::from(*n),
        Value::Float32(n) => mysql_async::Value::from(*n),
        Value::Float64(n) => mysql_async::Value::from(*n),
        Value::Decimal(d) => {
            // Convert to string for MySQL DECIMAL
            mysql_async::Value::from(d.to_string())
        }
        Value::String(s) => mysql_async::Value::from(s.clone()),
        Value::Bytes(b) => mysql_async::Value::from(b.clone()),
        Value::Date(d) => {
            let (year, month, day) = (d.year(), d.month() as u8, d.day() as u8);
            mysql_async::Value::Date(year as u16, month, day, 0, 0, 0, 0)
        }
        Value::Time(t) => {
            let (hour, min, sec, micro) = (
                t.hour() as u8,
                t.minute() as u8,
                t.second() as u8,
                t.nanosecond() / 1000,
            );
            mysql_async::Value::Time(false, 0, hour, min, sec, micro)
        }
        Value::DateTime(dt) => {
            let date = dt.date();
            let time = dt.time();
            mysql_async::Value::Date(
                date.year() as u16,
                date.month() as u8,
                date.day() as u8,
                time.hour() as u8,
                time.minute() as u8,
                time.second() as u8,
                time.nanosecond() / 1000,
            )
        }
        Value::DateTimeTz(dt) => {
            let naive = dt.naive_utc();
            let date = naive.date();
            let time = naive.time();
            mysql_async::Value::Date(
                date.year() as u16,
                date.month() as u8,
                date.day() as u8,
                time.hour() as u8,
                time.minute() as u8,
                time.second() as u8,
                time.nanosecond() / 1000,
            )
        }
        Value::Uuid(u) => mysql_async::Value::from(u.to_string()),
        Value::Json(j) => mysql_async::Value::from(j.to_string()),
        Value::Array(arr) => {
            // Serialize as JSON
            let json = serde_json::to_string(arr).unwrap_or_default();
            mysql_async::Value::from(json)
        }
        Value::Interval(micros) => mysql_async::Value::from(*micros),
        Value::Bit(bits) => mysql_async::Value::from(bits.clone()),
        Value::Enum(s) => mysql_async::Value::from(s.clone()),
        Value::Geometry(wkb) | Value::Geography(wkb) => mysql_async::Value::from(wkb.clone()),
        Value::Range { .. } => mysql_async::Value::NULL,
        Value::Composite(map) => {
            let json = serde_json::to_string(map).unwrap_or_default();
            mysql_async::Value::from(json)
        }
        Value::Custom { data, .. } => mysql_async::Value::from(data.clone()),
    }
}

/// Convert a MySQL value to a rivven Value
fn mysql_value_to_value(val: mysql_async::Value) -> Value {
    match val {
        mysql_async::Value::NULL => Value::Null,
        mysql_async::Value::Bytes(b) => {
            // Try to convert to string, otherwise keep as bytes
            match String::from_utf8(b.clone()) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Bytes(b),
            }
        }
        mysql_async::Value::Int(n) => Value::Int64(n),
        mysql_async::Value::UInt(n) => Value::Int64(n as i64),
        mysql_async::Value::Float(f) => Value::Float32(f),
        mysql_async::Value::Double(d) => Value::Float64(d),
        mysql_async::Value::Date(year, month, day, hour, min, sec, micro) => {
            if hour == 0 && min == 0 && sec == 0 && micro == 0 {
                // Date only
                if let Some(date) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                {
                    Value::Date(date)
                } else {
                    Value::Null
                }
            } else {
                // DateTime
                if let Some(date) =
                    chrono::NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
                {
                    if let Some(time) = chrono::NaiveTime::from_hms_micro_opt(
                        hour as u32,
                        min as u32,
                        sec as u32,
                        micro,
                    ) {
                        Value::DateTime(chrono::NaiveDateTime::new(date, time))
                    } else {
                        Value::Null
                    }
                } else {
                    Value::Null
                }
            }
        }
        mysql_async::Value::Time(neg, days, hour, min, sec, micro) => {
            // Convert to time (ignoring days for simplicity)
            let total_hours = days * 24 + hour as u32;
            if let Some(time) = chrono::NaiveTime::from_hms_micro_opt(
                total_hours % 24,
                min as u32,
                sec as u32,
                micro,
            ) {
                if neg {
                    // Negative time - store as interval
                    let micros = -((total_hours as i64 * 3600 + min as i64 * 60 + sec as i64)
                        * 1_000_000
                        + micro as i64);
                    Value::Interval(micros)
                } else {
                    Value::Time(time)
                }
            } else {
                Value::Null
            }
        }
    }
}

/// MySQL connection implementation
pub struct MySqlConnection {
    conn: Arc<Mutex<Option<Conn>>>,
    database: String,
    in_transaction: Arc<AtomicBool>,
    created_at: Instant,
    last_used: Mutex<Instant>,
}

impl MySqlConnection {
    /// Get the database name this connection is connected to
    pub fn database(&self) -> &str {
        &self.database
    }

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

    /// Create a new MySQL connection from an existing connection
    pub fn new(conn: Conn, database: String) -> Self {
        let now = Instant::now();
        Self {
            conn: Arc::new(Mutex::new(Some(conn))),
            database,
            in_transaction: Arc::new(AtomicBool::new(false)),
            created_at: now,
            last_used: Mutex::new(now),
        }
    }

    /// Create a new connection from configuration
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        // Parse URL: mysql://user:pass@host:port/database
        let url = url::Url::parse(&config.url)
            .map_err(|e| Error::config(format!("Invalid MySQL URL: {}", e)))?;

        let opts = OptsBuilder::from_opts(
            mysql_async::Opts::from_url(&config.url)
                .map_err(|e| Error::config(format!("Invalid MySQL connection string: {}", e)))?,
        );

        let database = url.path().trim_start_matches('/').to_string();

        let conn = Conn::new(opts)
            .await
            .map_err(|e| Error::connection(format!("Failed to connect to MySQL: {}", e)))?;

        Ok(Self::new(conn, database))
    }

    /// Take the inner connection
    async fn take_conn(&self) -> Option<Conn> {
        let mut guard = self.conn.lock().await;
        guard.take()
    }

    /// Put back the connection
    async fn put_conn(&self, conn: Conn) {
        let mut guard = self.conn.lock().await;
        *guard = Some(conn);
        *self.last_used.lock().await = Instant::now();
    }
}

#[async_trait]
impl ConnectionLifecycle for MySqlConnection {
    fn created_at(&self) -> Instant {
        self.created_at
    }

    async fn idle_time(&self) -> std::time::Duration {
        self.last_used.lock().await.elapsed()
    }

    async fn touch(&self) {
        *self.last_used.lock().await = Instant::now();
    }
}

#[async_trait]
impl Connection for MySqlConnection {
    async fn execute(&self, query: &str, params: &[Value]) -> Result<u64> {
        let mut conn = self
            .take_conn()
            .await
            .ok_or_else(|| Error::connection("Connection not available"))?;

        let mysql_params: Vec<mysql_async::Value> = params.iter().map(value_to_sql).collect();

        // For mysql_async, we need to convert the query to use ? placeholders
        // (it already uses ? so we should be fine)
        conn.exec_drop(query, mysql_params)
            .await
            .map_err(|e| Error::execution(format!("Failed to execute query: {}", e)))?;

        let affected = conn.affected_rows();
        self.put_conn(conn).await;

        Ok(affected)
    }

    async fn query(&self, query: &str, params: &[Value]) -> Result<Vec<Row>> {
        let mut conn = self
            .take_conn()
            .await
            .ok_or_else(|| Error::connection("Connection not available"))?;

        let mysql_params: Vec<mysql_async::Value> = params.iter().map(value_to_sql).collect();

        let result: Vec<mysql_async::Row> = conn
            .exec(query, mysql_params)
            .await
            .map_err(|e| Error::execution(format!("Failed to execute query: {}", e)))?;

        let rows: Vec<Row> = result
            .into_iter()
            .map(|row| {
                let columns: Vec<String> = row
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().to_string())
                    .collect();

                let values: Vec<Value> = (0..row.len())
                    .map(|i| {
                        let val: mysql_async::Value =
                            row.get(i).unwrap_or(mysql_async::Value::NULL);
                        mysql_value_to_value(val)
                    })
                    .collect();

                Row::new(columns, values)
            })
            .collect();

        self.put_conn(conn).await;
        Ok(rows)
    }

    async fn query_stream(
        &self,
        _query: &str,
        _params: &[Value],
    ) -> Result<Pin<Box<dyn RowStream>>> {
        // MySQL streaming is more complex with mysql_async
        Err(Error::unsupported(
            "Streaming not yet implemented for MySQL",
        ))
    }

    async fn prepare(&self, query: &str) -> Result<Box<dyn PreparedStatement>> {
        Ok(Box::new(MySqlPreparedStatement {
            query: query.to_string(),
        }))
    }

    async fn begin(&self) -> Result<Box<dyn Transaction>> {
        let mut conn = self
            .take_conn()
            .await
            .ok_or_else(|| Error::connection("Connection not available"))?;

        conn.query_drop("BEGIN")
            .await
            .map_err(|e| Error::transaction(format!("Failed to begin transaction: {}", e)))?;

        self.in_transaction.store(true, Ordering::SeqCst);

        Ok(Box::new(MySqlTransaction {
            conn: Arc::new(Mutex::new(Some(conn))),
            parent_conn: self.conn.clone(),
            committed: AtomicBool::new(false),
            in_transaction: Arc::clone(&self.in_transaction),
        }))
    }

    async fn begin_with_isolation(&self, level: IsolationLevel) -> Result<Box<dyn Transaction>> {
        let mut conn = self
            .take_conn()
            .await
            .ok_or_else(|| Error::connection("Connection not available"))?;

        let isolation_sql = match level {
            IsolationLevel::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            IsolationLevel::Snapshot => {
                return Err(Error::unsupported(
                    "Snapshot isolation not supported in MySQL",
                ))
            }
        };

        conn.query_drop(isolation_sql)
            .await
            .map_err(|e| Error::transaction(format!("Failed to set isolation level: {}", e)))?;

        conn.query_drop("BEGIN")
            .await
            .map_err(|e| Error::transaction(format!("Failed to begin transaction: {}", e)))?;

        self.in_transaction.store(true, Ordering::SeqCst);

        Ok(Box::new(MySqlTransaction {
            conn: Arc::new(Mutex::new(Some(conn))),
            parent_conn: self.conn.clone(),
            committed: AtomicBool::new(false),
            in_transaction: Arc::clone(&self.in_transaction),
        }))
    }

    async fn is_valid(&self) -> bool {
        if let Some(conn) = self.conn.lock().await.as_mut() {
            conn.ping().await.is_ok()
        } else {
            false
        }
    }

    async fn close(&self) -> Result<()> {
        if let Some(conn) = self.take_conn().await {
            conn.disconnect()
                .await
                .map_err(|e| Error::connection(format!("Failed to close connection: {}", e)))?;
        }
        Ok(())
    }
}

/// MySQL prepared statement (simplified - MySQL driver handles caching)
pub struct MySqlPreparedStatement {
    query: String,
}

#[async_trait]
impl PreparedStatement for MySqlPreparedStatement {
    async fn execute(&self, _params: &[Value]) -> Result<u64> {
        // MySQL driver manages statement caching internally
        // For now, return unsupported
        Err(Error::unsupported(
            "Use Connection::execute with the query string directly",
        ))
    }

    async fn query(&self, _params: &[Value]) -> Result<Vec<Row>> {
        // MySQL driver manages statement caching internally
        Err(Error::unsupported(
            "Use Connection::query with the query string directly",
        ))
    }

    fn sql(&self) -> &str {
        &self.query
    }
}

/// MySQL transaction
///
/// Uses manual BEGIN/COMMIT/ROLLBACK commands instead of mysql_async::Transaction
/// to avoid lifetime issues with borrowed connections.
pub struct MySqlTransaction {
    conn: Arc<Mutex<Option<Conn>>>,
    parent_conn: Arc<Mutex<Option<Conn>>>,
    committed: AtomicBool,
    in_transaction: Arc<AtomicBool>,
}

#[async_trait]
impl Transaction for MySqlTransaction {
    async fn execute(&self, query: &str, params: &[Value]) -> Result<u64> {
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| Error::transaction("Transaction not available"))?;

        let mysql_params: Vec<mysql_async::Value> = params.iter().map(value_to_sql).collect();

        conn.exec_drop(query, mysql_params)
            .await
            .map_err(|e| Error::execution(format!("Failed to execute in transaction: {}", e)))?;

        Ok(conn.affected_rows())
    }

    async fn query(&self, query: &str, params: &[Value]) -> Result<Vec<Row>> {
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| Error::transaction("Transaction not available"))?;

        let mysql_params: Vec<mysql_async::Value> = params.iter().map(value_to_sql).collect();

        let result: Vec<mysql_async::Row> = conn
            .exec(query, mysql_params)
            .await
            .map_err(|e| Error::execution(format!("Failed to query in transaction: {}", e)))?;

        let rows: Vec<Row> = result
            .into_iter()
            .map(|row| {
                let columns: Vec<String> = row
                    .columns_ref()
                    .iter()
                    .map(|c| c.name_str().to_string())
                    .collect();

                let values: Vec<Value> = (0..row.len())
                    .map(|i| {
                        let val: mysql_async::Value =
                            row.get(i).unwrap_or(mysql_async::Value::NULL);
                        mysql_value_to_value(val)
                    })
                    .collect();

                Row::new(columns, values)
            })
            .collect();

        Ok(rows)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        {
            let mut guard = self.conn.lock().await;
            let conn = guard
                .as_mut()
                .ok_or_else(|| Error::transaction("Transaction already completed"))?;

            conn.query_drop("COMMIT")
                .await
                .map_err(|e| Error::transaction(format!("Failed to commit transaction: {}", e)))?;
        }

        self.committed.store(true, Ordering::SeqCst);
        self.in_transaction.store(false, Ordering::SeqCst);

        // Return connection to parent
        if let Some(c) = self.conn.lock().await.take() {
            *self.parent_conn.lock().await = Some(c);
        }

        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        {
            let mut guard = self.conn.lock().await;
            if let Some(conn) = guard.as_mut() {
                conn.query_drop("ROLLBACK").await.map_err(|e| {
                    Error::transaction(format!("Failed to rollback transaction: {}", e))
                })?;
            }
        }

        self.in_transaction.store(false, Ordering::SeqCst);

        // Return connection to parent
        if let Some(c) = self.conn.lock().await.take() {
            *self.parent_conn.lock().await = Some(c);
        }

        Ok(())
    }

    async fn set_isolation_level(&self, level: IsolationLevel) -> Result<()> {
        let sql = match level {
            IsolationLevel::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
            IsolationLevel::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            IsolationLevel::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            IsolationLevel::Snapshot => {
                return Err(Error::unsupported(
                    "Snapshot isolation not supported in MySQL",
                ))
            }
        };
        self.execute(sql, &[]).await?;
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<()> {
        self.execute(&format!("SAVEPOINT {}", name), &[]).await?;
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        self.execute(&format!("ROLLBACK TO SAVEPOINT {}", name), &[])
            .await?;
        Ok(())
    }

    async fn release_savepoint(&self, name: &str) -> Result<()> {
        self.execute(&format!("RELEASE SAVEPOINT {}", name), &[])
            .await?;
        Ok(())
    }
}

/// MySQL connection factory
pub struct MySqlConnectionFactory {
    config: ConnectionConfig,
}

impl MySqlConnectionFactory {
    /// Create a new MySQL connection factory
    pub fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ConnectionFactory for MySqlConnectionFactory {
    async fn connect(&self, _config: &ConnectionConfig) -> Result<Box<dyn Connection>> {
        // Use stored config
        let conn = MySqlConnection::connect(&self.config).await?;
        Ok(Box::new(conn))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::MySQL
    }
}

/// MySQL schema provider for introspection
pub struct MySqlSchemaProvider {
    config: ConnectionConfig,
}

impl MySqlSchemaProvider {
    /// Create a new MySQL schema provider
    pub fn new(config: ConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl SchemaProvider for MySqlSchemaProvider {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let rows = conn.query("SHOW DATABASES", &[]).await?;
        conn.close().await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| r.get_index(0).and_then(|v| v.as_string()))
            .collect())
    }

    async fn get_table(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<TableMetadata>> {
        let conn = MySqlConnection::connect(&self.config).await?;
        // Parse database from URL
        let url = url::Url::parse(&self.config.url).ok();
        let default_db = url
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_else(|| "mysql".to_string());
        let schema_name = schema.unwrap_or(&default_db);

        // Query column information
        let columns: Vec<Row> = conn
            .query(
                r#"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    COLUMN_KEY,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
                "#,
                &[
                    Value::String(schema_name.to_string()),
                    Value::String(table_name.to_string()),
                ],
            )
            .await?;

        if columns.is_empty() {
            conn.close().await?;
            return Ok(None);
        }

        let column_metadata: Vec<ColumnMetadata> = columns
            .into_iter()
            .map(|row| {
                let name: String = row
                    .get_by_name("COLUMN_NAME")
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                let data_type: String = row
                    .get_by_name("DATA_TYPE")
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                let nullable: bool = row
                    .get_by_name("IS_NULLABLE")
                    .and_then(|v| v.as_string())
                    .map(|s| s == "YES")
                    .unwrap_or(true);
                let column_key: String = row
                    .get_by_name("COLUMN_KEY")
                    .and_then(|v| v.as_string())
                    .unwrap_or_default();
                let ordinal: u32 = row
                    .get_by_name("ORDINAL_POSITION")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as u32)
                    .unwrap_or(0);

                let mut meta = ColumnMetadata::new(&name, mysql_type_to_value_type(&data_type));
                meta.nullable = nullable;
                meta.ordinal = ordinal;
                meta.primary_key_ordinal = if column_key == "PRI" { Some(1) } else { None };
                meta.max_length = row
                    .get_by_name("CHARACTER_MAXIMUM_LENGTH")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as u32);
                meta.precision = row
                    .get_by_name("NUMERIC_PRECISION")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as u32);
                meta.scale = row
                    .get_by_name("NUMERIC_SCALE")
                    .and_then(|v| v.as_i64())
                    .map(|n| n as u32);
                meta.default_value = row
                    .get_by_name("COLUMN_DEFAULT")
                    .and_then(|v| v.as_string());
                meta
            })
            .collect();

        let _primary_keys: Vec<String> = column_metadata
            .iter()
            .filter(|c| c.is_primary_key())
            .map(|c| c.name.clone())
            .collect();

        conn.close().await?;

        let mut table = TableMetadata::new(table_name);
        table.schema = Some(schema_name.to_string());
        table.columns = column_metadata;

        Ok(Some(table))
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let url = url::Url::parse(&self.config.url).ok();
        let default_db = url
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_else(|| "mysql".to_string());
        let schema_name = schema.unwrap_or(&default_db);

        let rows: Vec<Row> = conn
            .query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
                &[Value::String(schema_name.to_string())],
            )
            .await?;

        let tables: Vec<String> = rows
            .into_iter()
            .filter_map(|row| row.get_by_name("TABLE_NAME").and_then(|v| v.as_string()))
            .collect();

        conn.close().await?;
        Ok(tables)
    }

    async fn list_indexes(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<IndexMetadata>> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let url = url::Url::parse(&self.config.url).ok();
        let default_db = url
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_else(|| "mysql".to_string());
        let schema_name = schema.unwrap_or(&default_db);

        let rows: Vec<Row> = conn
            .query(
                r#"
                SELECT 
                    INDEX_NAME,
                    COLUMN_NAME,
                    NON_UNIQUE,
                    INDEX_TYPE
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
                "#,
                &[
                    Value::String(schema_name.to_string()),
                    Value::String(table_name.to_string()),
                ],
            )
            .await?;

        // Group by index name
        let mut index_map: HashMap<String, IndexMetadata> = HashMap::new();

        for row in rows {
            let index_name: String = row
                .get_by_name("INDEX_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let column_name: String = row
                .get_by_name("COLUMN_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let non_unique: bool = row
                .get_by_name("NON_UNIQUE")
                .and_then(|v| v.as_i64())
                .map(|n| n != 0)
                .unwrap_or(true);

            index_map
                .entry(index_name.clone())
                .or_insert_with(|| {
                    let mut idx = IndexMetadata::new(table_name, &index_name, Vec::new());
                    idx.unique = !non_unique;
                    idx.schema = Some(schema_name.to_string());
                    idx
                })
                .columns
                .push(column_name);
        }

        // Mark PRIMARY index
        if let Some(idx) = index_map.get_mut("PRIMARY") {
            idx.primary = true;
            idx.unique = true;
        }

        conn.close().await?;
        Ok(index_map.into_values().collect())
    }

    async fn list_foreign_keys(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Vec<ForeignKeyMetadata>> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let url = url::Url::parse(&self.config.url).ok();
        let default_db = url
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_else(|| "mysql".to_string());
        let schema_name = schema.unwrap_or(&default_db);

        let rows: Vec<Row> = conn
            .query(
                r#"
                SELECT 
                    kcu.CONSTRAINT_NAME,
                    kcu.COLUMN_NAME,
                    kcu.REFERENCED_TABLE_SCHEMA,
                    kcu.REFERENCED_TABLE_NAME,
                    kcu.REFERENCED_COLUMN_NAME,
                    rc.UPDATE_RULE,
                    rc.DELETE_RULE
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                    ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
                WHERE kcu.TABLE_SCHEMA = ? 
                    AND kcu.TABLE_NAME = ?
                    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
                "#,
                &[
                    Value::String(schema_name.to_string()),
                    Value::String(table_name.to_string()),
                ],
            )
            .await?;

        // Group by constraint name
        let mut fk_map: HashMap<String, ForeignKeyMetadata> = HashMap::new();

        for row in rows {
            let constraint_name: String = row
                .get_by_name("CONSTRAINT_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let column_name: String = row
                .get_by_name("COLUMN_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let ref_schema: Option<String> = row
                .get_by_name("REFERENCED_TABLE_SCHEMA")
                .and_then(|v| v.as_string());
            let ref_table: String = row
                .get_by_name("REFERENCED_TABLE_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let ref_column: String = row
                .get_by_name("REFERENCED_COLUMN_NAME")
                .and_then(|v| v.as_string())
                .unwrap_or_default();
            let on_update: String = row
                .get_by_name("UPDATE_RULE")
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "NO ACTION".to_string());
            let on_delete: String = row
                .get_by_name("DELETE_RULE")
                .and_then(|v| v.as_string())
                .unwrap_or_else(|| "NO ACTION".to_string());

            let entry =
                fk_map
                    .entry(constraint_name.clone())
                    .or_insert_with(|| ForeignKeyMetadata {
                        name: constraint_name,
                        source_schema: Some(schema_name.to_string()),
                        source_table: table_name.to_string(),
                        source_columns: Vec::new(),
                        target_schema: ref_schema,
                        target_table: ref_table,
                        target_columns: Vec::new(),
                        on_update: parse_fk_action(&on_update),
                        on_delete: parse_fk_action(&on_delete),
                    });

            entry.source_columns.push(column_name);
            entry.target_columns.push(ref_column);
        }

        conn.close().await?;
        Ok(fk_map.into_values().collect())
    }
}

/// MySQL schema manager
pub struct MySqlSchemaManager {
    provider: MySqlSchemaProvider,
    config: ConnectionConfig,
    evolution_mode: SchemaEvolutionMode,
}

impl MySqlSchemaManager {
    /// Create a new MySQL schema manager
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            provider: MySqlSchemaProvider::new(config.clone()),
            config,
            evolution_mode: SchemaEvolutionMode::None,
        }
    }

    /// Set the schema evolution mode
    pub fn with_evolution_mode(mut self, mode: SchemaEvolutionMode) -> Self {
        self.evolution_mode = mode;
        self
    }
}

#[async_trait]
impl SchemaProvider for MySqlSchemaManager {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        self.provider.list_schemas().await
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        self.provider.list_tables(schema).await
    }

    async fn get_table(&self, schema: Option<&str>, table: &str) -> Result<Option<TableMetadata>> {
        self.provider.get_table(schema, table).await
    }

    async fn list_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexMetadata>> {
        self.provider.list_indexes(schema, table).await
    }

    async fn list_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyMetadata>> {
        self.provider.list_foreign_keys(schema, table).await
    }
}

#[async_trait]
impl SchemaManager for MySqlSchemaManager {
    async fn create_table(&self, table: &TableMetadata) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;

        // Build CREATE TABLE statement
        let columns: Vec<String> = table
            .columns
            .iter()
            .map(|col| {
                let mut def = format!("`{}` {}", col.name, &col.type_name);

                if !col.nullable {
                    def.push_str(" NOT NULL");
                }

                if col.auto_increment {
                    def.push_str(" AUTO_INCREMENT");
                }

                if let Some(ref default) = col.default_value {
                    def.push_str(&format!(" DEFAULT '{}'", default));
                }

                def
            })
            .collect();

        let mut sql = format!(
            "CREATE TABLE `{}` (\n  {}\n",
            table.name,
            columns.join(",\n  ")
        );

        let pk_cols = table.primary_key_columns();
        if !pk_cols.is_empty() {
            sql.push_str(&format!(
                ",\n  PRIMARY KEY ({})",
                pk_cols
                    .iter()
                    .map(|c| format!("`{}`", c.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        sql.push_str("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

        conn.execute(&sql, &[]).await?;
        conn.close().await?;
        Ok(())
    }

    async fn drop_table(&self, schema: Option<&str>, table: &str) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, table)
        } else {
            format!("`{}`", table)
        };
        conn.execute(&format!("DROP TABLE {}", table_ref), &[])
            .await?;
        conn.close().await?;
        Ok(())
    }

    async fn add_column(
        &self,
        schema: Option<&str>,
        table: &str,
        column: &ColumnMetadata,
    ) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, table)
        } else {
            format!("`{}`", table)
        };
        let sql = format!(
            "ALTER TABLE {} ADD COLUMN `{}` {} {}",
            table_ref,
            column.name,
            &column.type_name,
            if column.nullable { "NULL" } else { "NOT NULL" }
        );
        conn.execute(&sql, &[]).await?;
        conn.close().await?;
        Ok(())
    }

    async fn drop_column(&self, schema: Option<&str>, table: &str, column: &str) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, table)
        } else {
            format!("`{}`", table)
        };
        conn.execute(
            &format!("ALTER TABLE {} DROP COLUMN `{}`", table_ref, column),
            &[],
        )
        .await?;
        conn.close().await?;
        Ok(())
    }

    async fn alter_column(
        &self,
        schema: Option<&str>,
        table: &str,
        column: &ColumnMetadata,
    ) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, table)
        } else {
            format!("`{}`", table)
        };
        let sql = format!(
            "ALTER TABLE {} MODIFY COLUMN `{}` {} {}",
            table_ref,
            column.name,
            &column.type_name,
            if column.nullable { "NULL" } else { "NOT NULL" }
        );
        conn.execute(&sql, &[]).await?;
        conn.close().await?;
        Ok(())
    }

    async fn rename_table(
        &self,
        schema: Option<&str>,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let old_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, old_name)
        } else {
            format!("`{}`", old_name)
        };
        let new_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, new_name)
        } else {
            format!("`{}`", new_name)
        };
        conn.execute(&format!("RENAME TABLE {} TO {}", old_ref, new_ref), &[])
            .await?;
        conn.close().await?;
        Ok(())
    }

    async fn rename_column(
        &self,
        schema: Option<&str>,
        table: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(s) = schema {
            format!("`{}`.`{}`", s, table)
        } else {
            format!("`{}`", table)
        };
        // MySQL 8.0+ syntax
        conn.execute(
            &format!(
                "ALTER TABLE {} RENAME COLUMN `{}` TO `{}`",
                table_ref, old_name, new_name
            ),
            &[],
        )
        .await?;
        conn.close().await?;
        Ok(())
    }

    async fn create_index(&self, index: &IndexMetadata) -> Result<()> {
        let conn = MySqlConnection::connect(&self.config).await?;
        let table_ref = if let Some(ref s) = index.schema {
            format!("`{}`.`{}`", s, index.table)
        } else {
            format!("`{}`", index.table)
        };
        let unique = if index.unique { "UNIQUE " } else { "" };
        let columns: String = index
            .columns
            .iter()
            .map(|c| format!("`{}`", c))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute(
            &format!(
                "CREATE {}INDEX `{}` ON {} ({})",
                unique, index.name, table_ref, columns
            ),
            &[],
        )
        .await?;
        conn.close().await?;
        Ok(())
    }

    async fn drop_index(&self, _schema: Option<&str>, _index_name: &str) -> Result<()> {
        // MySQL requires table name for DROP INDEX - we'll need a workaround
        // For now, assume index_name is unique enough across the schema
        Err(Error::unsupported(
            "MySQL DROP INDEX requires table name - use ALTER TABLE ... DROP INDEX instead",
        ))
    }

    async fn evolve_schema(
        &self,
        target: &TableMetadata,
        mode: SchemaEvolutionMode,
    ) -> Result<SchemaEvolutionResult> {
        match mode {
            SchemaEvolutionMode::None => Ok(SchemaEvolutionResult::default()),
            SchemaEvolutionMode::AddColumnsOnly => {
                let current = self
                    .provider
                    .get_table(target.schema.as_deref(), &target.name)
                    .await?;
                if current.is_none() {
                    return Ok(SchemaEvolutionResult::default());
                }
                let current = current.unwrap();
                let current_cols: std::collections::HashSet<_> =
                    current.columns.iter().map(|c| &c.name).collect();

                let new_cols: Vec<_> = target
                    .columns
                    .iter()
                    .filter(|c| !current_cols.contains(&c.name))
                    .collect();

                if new_cols.is_empty() {
                    return Ok(SchemaEvolutionResult::default());
                }

                let mut result = SchemaEvolutionResult::default();
                for col in new_cols {
                    self.add_column(target.schema.as_deref(), &target.name, col)
                        .await?;
                    result.columns_added.push(col.name.clone());
                }
                result.changed = true;

                Ok(result)
            }
            SchemaEvolutionMode::AddAndWiden | SchemaEvolutionMode::Full => {
                // Full evolution: add/modify columns
                self.evolve_schema(target, SchemaEvolutionMode::AddColumnsOnly)
                    .await
            }
        }
    }
}

/// Convert MySQL data type to rivven Value type descriptor
fn mysql_type_to_value_type(mysql_type: &str) -> String {
    match mysql_type.to_lowercase().as_str() {
        "tinyint" => "Int8".to_string(),
        "smallint" => "Int16".to_string(),
        "int" | "integer" | "mediumint" => "Int32".to_string(),
        "bigint" => "Int64".to_string(),
        "float" => "Float32".to_string(),
        "double" | "real" => "Float64".to_string(),
        "decimal" | "numeric" => "Decimal".to_string(),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" => {
            "String".to_string()
        }
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            "Bytes".to_string()
        }
        "date" => "Date".to_string(),
        "time" => "Time".to_string(),
        "datetime" | "timestamp" => "DateTime".to_string(),
        "json" => "Json".to_string(),
        "enum" => "Enum".to_string(),
        "bit" => "Bit".to_string(),
        "geometry" | "point" | "linestring" | "polygon" => "Geometry".to_string(),
        _ => "String".to_string(),
    }
}

/// Convert rivven Value type to MySQL data type
#[allow(dead_code)]
fn value_type_to_mysql_type(value_type: &str) -> String {
    match value_type {
        "Bool" => "TINYINT(1)".to_string(),
        "Int8" => "TINYINT".to_string(),
        "Int16" => "SMALLINT".to_string(),
        "Int32" => "INT".to_string(),
        "Int64" => "BIGINT".to_string(),
        "Float32" => "FLOAT".to_string(),
        "Float64" => "DOUBLE".to_string(),
        "Decimal" => "DECIMAL(38, 9)".to_string(),
        "String" => "TEXT".to_string(),
        "Bytes" => "LONGBLOB".to_string(),
        "Date" => "DATE".to_string(),
        "Time" => "TIME".to_string(),
        "DateTime" | "DateTimeTz" => "DATETIME(6)".to_string(),
        "Uuid" => "CHAR(36)".to_string(),
        "Json" => "JSON".to_string(),
        "Enum" => "VARCHAR(255)".to_string(),
        "Bit" => "BIT(64)".to_string(),
        "Geometry" | "Geography" => "GEOMETRY".to_string(),
        _ => "TEXT".to_string(),
    }
}

/// Parse foreign key action from MySQL string
fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        "RESTRICT" => ForeignKeyAction::Restrict,
        _ => ForeignKeyAction::NoAction,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_type_conversions() {
        assert_eq!(mysql_type_to_value_type("int"), "Int32");
        assert_eq!(mysql_type_to_value_type("varchar"), "String");
        assert_eq!(mysql_type_to_value_type("bigint"), "Int64");
        assert_eq!(mysql_type_to_value_type("json"), "Json");
    }

    #[test]
    fn test_value_type_to_mysql() {
        assert_eq!(value_type_to_mysql_type("Int32"), "INT");
        assert_eq!(value_type_to_mysql_type("String"), "TEXT");
        assert_eq!(value_type_to_mysql_type("Uuid"), "CHAR(36)");
    }

    #[test]
    fn test_fk_action_parsing() {
        assert!(matches!(
            parse_fk_action("CASCADE"),
            ForeignKeyAction::Cascade
        ));
        assert!(matches!(
            parse_fk_action("SET NULL"),
            ForeignKeyAction::SetNull
        ));
        assert!(matches!(
            parse_fk_action("NO ACTION"),
            ForeignKeyAction::NoAction
        ));
    }
}
