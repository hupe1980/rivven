//! PostgreSQL backend implementation for rivven-rdbc
//!
//! Provides PostgreSQL-specific implementations:
//! - Connection and prepared statements
//! - Transaction support with savepoints
//! - Streaming row iteration
//! - Connection pooling via deadpool-postgres
//! - Schema provider for introspection

use async_trait::async_trait;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::connection::{
    Connection, ConnectionConfig, ConnectionFactory, ConnectionLifecycle, DatabaseType,
    IsolationLevel, PreparedStatement, RowStream, Transaction,
};
use crate::dialect::{PostgresDialect, SqlDialect};
use crate::error::{Error, Result};
use crate::schema::{ForeignKeyAction, ForeignKeyMetadata, IndexMetadata, SchemaProvider};
use crate::security::validate_sql_identifier;
use crate::types::{ColumnMetadata, Row, TableMetadata, Value};

/// Convert a `Value` to its string representation for use in PostgreSQL range literals.
fn value_to_range_bound(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int8(n) => n.to_string(),
        Value::Int16(n) => n.to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float32(n) => n.to_string(),
        Value::Float64(n) => n.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.clone(),
        Value::Date(d) => d.to_string(),
        Value::Time(t) => t.to_string(),
        Value::DateTime(dt) => dt.to_string(),
        Value::DateTimeTz(dt) => dt.to_rfc3339(),
        Value::Uuid(u) => u.to_string(),
        // Fallback: Debug representation for complex types
        other => format!("{other:?}"),
    }
}

/// Convert a rivven Value to a tokio-postgres compatible parameter
fn value_to_sql(value: &Value) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
    match value {
        Value::Null => Box::new(Option::<i32>::None),
        Value::Bool(b) => Box::new(*b),
        Value::Int8(n) => Box::new(i16::from(*n)), // PostgreSQL doesn't have int8
        Value::Int16(n) => Box::new(*n),
        Value::Int32(n) => Box::new(*n),
        Value::Int64(n) => Box::new(*n),
        Value::Float32(n) => Box::new(*n),
        Value::Float64(n) => Box::new(*n),
        Value::Decimal(d) => Box::new(*d),
        Value::String(s) => Box::new(s.clone()),
        Value::Bytes(b) => Box::new(b.clone()),
        Value::Date(d) => Box::new(*d),
        Value::Time(t) => Box::new(*t),
        Value::DateTime(dt) => Box::new(*dt),
        Value::DateTimeTz(dt) => Box::new(*dt),
        Value::Uuid(u) => Box::new(*u),
        Value::Json(j) => Box::new(j.clone()),
        // Complex types - serialize to JSON or handle specially
        Value::Array(arr) => {
            let json = serde_json::to_value(arr).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize Array to JSON, using null");
                serde_json::Value::Null
            });
            Box::new(json)
        }
        Value::Interval(micros) => {
            // PostgreSQL interval as microseconds
            Box::new(*micros)
        }
        Value::Bit(bits) => Box::new(bits.clone()),
        Value::Enum(s) => Box::new(s.clone()),
        Value::Geometry(wkb) | Value::Geography(wkb) => Box::new(wkb.clone()),
        Value::Range {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => {
            // Serialize ranges as a JSON string representation
            // instead of silently mapping to NULL. PostgreSQL's native range
            // types aren't supported by tokio-postgres parameterized queries,
            // so we emit a human-readable range literal that can be cast server-side.
            let lb = if *lower_inclusive { "[" } else { "(" };
            let ub = if *upper_inclusive { "]" } else { ")" };
            let lower_str = lower
                .as_ref()
                .map(|v| value_to_range_bound(v))
                .unwrap_or_default();
            let upper_str = upper
                .as_ref()
                .map(|v| value_to_range_bound(v))
                .unwrap_or_default();
            let range_str = format!("{lb}{lower_str},{upper_str}{ub}");
            Box::new(range_str)
        }
        Value::Composite(map) => {
            let json = serde_json::to_value(map).unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to serialize Composite to JSON, using null");
                serde_json::Value::Null
            });
            Box::new(json)
        }
        Value::Custom { data, .. } => Box::new(data.clone()),
    }
}

/// Convert a tokio-postgres row to a rivven Row
fn pg_row_to_row(pg_row: &tokio_postgres::Row) -> Row {
    let columns: Vec<String> = pg_row
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    let values: Vec<Value> = pg_row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| pg_value_to_value(pg_row, i, col.type_()))
        .collect();

    Row::new(columns, values)
}

/// Convert a PostgreSQL value to a rivven Value
fn pg_value_to_value(
    row: &tokio_postgres::Row,
    idx: usize,
    pg_type: &tokio_postgres::types::Type,
) -> Value {
    use tokio_postgres::types::Type;

    // no early NULL check — each per-type arm handles None correctly
    // via `try_get::<_, Option<T>>` returning Ok(None) for SQL NULL.

    match *pg_type {
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(Value::Int16)
            .unwrap_or(Value::Null),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(Value::Int32)
            .unwrap_or(Value::Null),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(Value::Int64)
            .unwrap_or(Value::Null),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .map(Value::Float32)
            .unwrap_or(Value::Null),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .map(Value::Float64)
            .unwrap_or(Value::Null),
        Type::NUMERIC => row
            .try_get::<_, Option<rust_decimal::Decimal>>(idx)
            .ok()
            .flatten()
            .map(Value::Decimal)
            .unwrap_or(Value::Null),
        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
        Type::BYTEA => row
            .try_get::<_, Option<Vec<u8>>>(idx)
            .ok()
            .flatten()
            .map(Value::Bytes)
            .unwrap_or(Value::Null),
        Type::DATE => row
            .try_get::<_, Option<chrono::NaiveDate>>(idx)
            .ok()
            .flatten()
            .map(Value::Date)
            .unwrap_or(Value::Null),
        Type::TIME => row
            .try_get::<_, Option<chrono::NaiveTime>>(idx)
            .ok()
            .flatten()
            .map(Value::Time)
            .unwrap_or(Value::Null),
        Type::TIMESTAMP => row
            .try_get::<_, Option<chrono::NaiveDateTime>>(idx)
            .ok()
            .flatten()
            .map(Value::DateTime)
            .unwrap_or(Value::Null),
        Type::TIMESTAMPTZ => row
            .try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
            .ok()
            .flatten()
            .map(Value::DateTimeTz)
            .unwrap_or(Value::Null),
        Type::UUID => row
            .try_get::<_, Option<uuid::Uuid>>(idx)
            .ok()
            .flatten()
            .map(Value::Uuid)
            .unwrap_or(Value::Null),
        Type::JSON | Type::JSONB => row
            .try_get::<_, Option<serde_json::Value>>(idx)
            .ok()
            .flatten()
            .map(Value::Json)
            .unwrap_or(Value::Null),
        _ => {
            // Try to get as string for unknown types
            row.try_get::<_, Option<String>>(idx)
                .ok()
                .flatten()
                .map(Value::String)
                .unwrap_or(Value::Null)
        }
    }
}

/// PostgreSQL connection implementation
pub struct PgConnection {
    client: Arc<tokio_postgres::Client>,
    closed: AtomicBool,
    created_at: Instant,
    last_used: Mutex<Instant>,
    /// Handle to the background connection task so we can abort it on close.
    connection_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set to `true` when the background connection task terminates with an
    /// error. Subsequent queries will return [`Error::connection`] immediately
    /// instead of hanging or producing confusing tokio-postgres errors.
    bg_task_failed: Arc<AtomicBool>,
    /// Set to `true` when a [`PgTransaction`] is dropped without being
    /// committed or rolled back. The next operation on this connection will
    /// issue a synchronous `ROLLBACK` before proceeding, ensuring the
    /// connection is never handed to the next pool consumer in a dirty
    /// transaction state.
    needs_rollback: Arc<AtomicBool>,
}

impl PgConnection {
    /// Create a new connection from a tokio-postgres client
    pub fn new(client: tokio_postgres::Client) -> Self {
        let now = Instant::now();
        Self {
            client: Arc::new(client),
            closed: AtomicBool::new(false),
            created_at: now,
            last_used: Mutex::new(now),
            connection_task: Mutex::new(None),
            bg_task_failed: Arc::new(AtomicBool::new(false)),
            needs_rollback: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create a new connection with the background connection task handle
    pub fn with_task(client: tokio_postgres::Client, task: tokio::task::JoinHandle<()>) -> Self {
        let now = Instant::now();
        let bg_task_failed = Arc::new(AtomicBool::new(false));
        let bg_flag = bg_task_failed.clone();

        // Wrap the original task in a monitoring task that sets the flag on error
        let monitored_task = tokio::spawn(async move {
            let result = task.await;
            match result {
                Ok(()) => {}
                Err(e) => {
                    // JoinError means the task panicked or was cancelled
                    tracing::error!("PostgreSQL background connection task failed: {}", e);
                    bg_flag.store(true, Ordering::Release);
                }
            }
        });

        Self {
            client: Arc::new(client),
            closed: AtomicBool::new(false),
            created_at: now,
            last_used: Mutex::new(now),
            connection_task: Mutex::new(Some(monitored_task)),
            bg_task_failed,
            needs_rollback: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check whether the background connection task has failed, marking the
    /// connection as unusable.
    fn check_bg_task(&self) -> Result<()> {
        if self.bg_task_failed.load(Ordering::Acquire) {
            return Err(Error::connection(
                "PostgreSQL background connection task has terminated; connection is unusable",
            ));
        }
        Ok(())
    }

    /// If a previous transaction was dropped without commit/rollback, issue a
    /// synchronous `ROLLBACK` to clean the connection state before the next
    /// operation. This prevents dirty transaction state from leaking to pool
    /// consumers.
    async fn ensure_clean_state(&self) -> Result<()> {
        if self.needs_rollback.swap(false, Ordering::AcqRel) {
            tracing::debug!("Issuing deferred ROLLBACK for dirty connection state");
            self.client.execute("ROLLBACK", &[]).await.map_err(|e| {
                Error::connection(format!("Failed to rollback dirty transaction state: {e}"))
            })?;
        }
        Ok(())
    }

    /// Get the underlying client
    pub fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    /// Get the age of this connection (time since creation)
    #[inline]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Check if connection is older than the specified max lifetime
    #[inline]
    pub fn is_expired(&self, max_lifetime: std::time::Duration) -> bool {
        self.age() > max_lifetime
    }

    /// Get time since last use
    pub async fn idle_time(&self) -> std::time::Duration {
        self.last_used.lock().await.elapsed()
    }

    /// Update last used timestamp (called internally after queries)
    async fn update_last_used(&self) {
        *self.last_used.lock().await = Instant::now();
    }
}

#[async_trait]
impl ConnectionLifecycle for PgConnection {
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

#[async_trait]
impl Connection for PgConnection {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::connection("connection is closed"));
        }
        self.check_bg_task()?;
        self.ensure_clean_state().await?;

        // Build boxed parameters
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let pg_rows = self
            .client
            .query(sql, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(pg_rows.iter().map(pg_row_to_row).collect())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::connection("connection is closed"));
        }
        self.check_bg_task()?;
        self.ensure_clean_state().await?;

        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let affected = self
            .client
            .execute(sql, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(affected)
    }

    async fn prepare(&self, sql: &str) -> Result<Box<dyn PreparedStatement>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::connection("connection is closed"));
        }
        self.check_bg_task()?;
        self.ensure_clean_state().await?;

        let stmt = self
            .client
            .prepare(sql)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(Box::new(PgPreparedStatement {
            client: Arc::clone(&self.client),
            statement: stmt,
            sql: sql.to_string(),
        }))
    }

    async fn begin(&self) -> Result<Box<dyn Transaction>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::connection("connection is closed"));
        }
        self.check_bg_task()?;
        self.ensure_clean_state().await?;

        self.client
            .execute("BEGIN", &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;

        Ok(Box::new(PgTransaction {
            client: Arc::clone(&self.client),
            committed: AtomicBool::new(false),
            rolled_back: AtomicBool::new(false),
            needs_rollback: Arc::clone(&self.needs_rollback),
        }))
    }

    async fn query_stream(&self, sql: &str, params: &[Value]) -> Result<Pin<Box<dyn RowStream>>> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(Error::connection("connection is closed"));
        }
        self.check_bg_task()?;
        self.ensure_clean_state().await?;

        // Use query_raw for true incremental row streaming from PostgreSQL.
        // Unlike query() which collects all rows into a Vec first, query_raw
        // returns a tokio_postgres::RowStream that yields rows one-by-one as
        // they arrive over the wire — bounded memory regardless of result size.
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let pg_stream = self
            .client
            .query_raw(sql, param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(Box::pin(PgRowStream {
            inner: Box::pin(pg_stream),
            _client: Arc::clone(&self.client),
        }))
    }

    async fn is_valid(&self) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        if self.bg_task_failed.load(Ordering::Acquire) {
            return false;
        }
        // Clean up any dirty transaction state before the health check.
        // This covers the pool's test-on-borrow path.
        if self.needs_rollback.swap(false, Ordering::AcqRel) {
            tracing::debug!("Issuing deferred ROLLBACK during health check");
            if self.client.execute("ROLLBACK", &[]).await.is_err() {
                return false;
            }
        }
        self.client.simple_query("SELECT 1").await.is_ok()
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Relaxed);
        // Abort the background connection task to actually close the TCP connection.
        //
        // NOTE: This does NOT send a PostgreSQL Terminate ('X') message to the
        // server. The server will detect the closed socket via TCP and clean up
        // the backend process, but the connection will appear as an unexpected
        // disconnect in the PostgreSQL server logs rather than a graceful close.
        // Sending Terminate would require ownership of the client or a raw socket
        // write, which tokio-postgres does not expose. The TCP RST from the abort
        // is sufficient for correctness — the server releases all resources
        // associated with the backend.
        if let Some(task) = self.connection_task.lock().await.take() {
            task.abort();
        }
        Ok(())
    }
}

/// True incremental row stream backed by tokio_postgres::RowStream.
///
/// Rows are pulled one-by-one from the database connection as the caller
/// advances the stream — no full-result materialization, bounded memory.
/// Holds an Arc<Client> to guarantee the underlying connection stays alive
/// for the lifetime of the stream, even if the parent PgConnection is dropped.
struct PgRowStream {
    inner: Pin<Box<tokio_postgres::RowStream>>,
    /// Keep the client alive so the background connection task continues
    /// processing row data for this stream.
    _client: Arc<tokio_postgres::Client>,
}

impl RowStream for PgRowStream {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Box::pin(async move {
            use futures_util::TryStreamExt;
            match self.inner.as_mut().try_next().await {
                Ok(Some(pg_row)) => Ok(Some(pg_row_to_row(&pg_row))),
                Ok(None) => Ok(None),
                Err(e) => Err(Error::query(e.to_string())),
            }
        })
    }
}

/// PostgreSQL prepared statement
pub struct PgPreparedStatement {
    client: Arc<tokio_postgres::Client>,
    statement: tokio_postgres::Statement,
    sql: String,
}

#[async_trait]
impl PreparedStatement for PgPreparedStatement {
    async fn execute(&self, params: &[Value]) -> Result<u64> {
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let affected = self
            .client
            .execute(&self.statement, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), &self.sql))?;

        Ok(affected)
    }

    async fn query(&self, params: &[Value]) -> Result<Vec<Row>> {
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let pg_rows = self
            .client
            .query(&self.statement, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), &self.sql))?;

        Ok(pg_rows.iter().map(pg_row_to_row).collect())
    }

    fn sql(&self) -> &str {
        &self.sql
    }
}

/// PostgreSQL transaction
pub struct PgTransaction {
    client: Arc<tokio_postgres::Client>,
    committed: AtomicBool,
    rolled_back: AtomicBool,
    /// Shared flag with the parent [`PgConnection`]. Set on drop when the
    /// transaction was not explicitly committed or rolled back, so the
    /// connection can issue a deferred `ROLLBACK` before its next operation.
    needs_rollback: Arc<AtomicBool>,
}

#[async_trait]
impl Transaction for PgTransaction {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>> {
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let pg_rows = self
            .client
            .query(sql, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(pg_rows.iter().map(pg_row_to_row).collect())
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        let boxed_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
            params.iter().map(value_to_sql).collect();

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = boxed_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        let affected = self
            .client
            .execute(sql, &param_refs)
            .await
            .map_err(|e| Error::query_with_sql(e.to_string(), sql))?;

        Ok(affected)
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.client
            .execute("COMMIT", &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        self.committed.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        self.client
            .execute("ROLLBACK", &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        self.rolled_back.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn set_isolation_level(&self, level: IsolationLevel) -> Result<()> {
        if matches!(level, IsolationLevel::Snapshot) {
            return Err(Error::unsupported(
                "Snapshot isolation is SQL Server specific; PostgreSQL supports SERIALIZABLE instead",
            ));
        }
        let sql = format!("SET TRANSACTION ISOLATION LEVEL {}", level.to_sql());
        self.client
            .execute(&sql, &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn savepoint(&self, name: &str) -> Result<()> {
        crate::security::validate_sql_identifier(name)?;
        let sql = format!("SAVEPOINT {}", name);
        self.client
            .execute(&sql, &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn rollback_to_savepoint(&self, name: &str) -> Result<()> {
        crate::security::validate_sql_identifier(name)?;
        let sql = format!("ROLLBACK TO SAVEPOINT {}", name);
        self.client
            .execute(&sql, &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }

    async fn release_savepoint(&self, name: &str) -> Result<()> {
        crate::security::validate_sql_identifier(name)?;
        let sql = format!("RELEASE SAVEPOINT {}", name);
        self.client
            .execute(&sql, &[])
            .await
            .map_err(|e| Error::Transaction {
                message: e.to_string(),
                source: Some(Box::new(e)),
            })?;
        Ok(())
    }
}

impl Drop for PgTransaction {
    fn drop(&mut self) {
        // If the transaction wasn't explicitly committed or rolled back, set
        // the shared `needs_rollback` flag on the parent connection. The next
        // operation on the connection (or the pool's health-check) will issue
        // a synchronous `ROLLBACK` before proceeding, preventing a race
        // with pool connection reuse.
        if !self.committed.load(Ordering::Relaxed) && !self.rolled_back.load(Ordering::Relaxed) {
            tracing::debug!(
                "PgTransaction dropped without commit/rollback; \
                 marking connection for deferred rollback"
            );
            self.needs_rollback.store(true, Ordering::Release);
        }
    }
}

/// PostgreSQL connection factory
#[derive(Debug, Clone, Default)]
pub struct PgConnectionFactory;

#[async_trait]
impl ConnectionFactory for PgConnectionFactory {
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn Connection>> {
        let (client, connection) = Self::connect_with_tls(config).await?;

        // Spawn the connection handler and store the handle so
        // PgConnection::close() can abort it to close the TCP connection.
        let bg_flag = Arc::new(AtomicBool::new(false));
        let bg_flag_clone = bg_flag.clone();
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("PostgreSQL connection error: {}", e);
                bg_flag_clone.store(true, Ordering::Release);
            }
        });

        let mut conn = PgConnection::with_task(client, task);
        conn.bg_task_failed = bg_flag;
        Ok(Box::new(conn))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::PostgreSQL
    }
}

impl PgConnectionFactory {
    /// Connect using TLS when the `postgres-tls` feature is enabled and the
    /// connection URL or properties request it (e.g. `sslmode=require`).
    /// Falls back to `NoTls` when TLS is not compiled in or not requested.
    #[cfg(feature = "postgres-tls")]
    async fn connect_with_tls(
        config: &ConnectionConfig,
    ) -> Result<(
        tokio_postgres::Client,
        impl std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>,
    )> {
        let wants_tls = Self::wants_tls(config);

        if wants_tls {
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(Self::root_cert_store())
                .with_no_client_auth();
            let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
            tokio_postgres::connect(&config.url, tls)
                .await
                .map_err(|e| Error::connection_with_source("failed to connect (TLS)", e))
        } else {
            tokio_postgres::connect(
                &config.url,
                tokio_postgres_rustls::MakeRustlsConnect::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(Self::root_cert_store())
                        .with_no_client_auth(),
                ),
            )
            .await
            .map_err(|e| Error::connection_with_source("failed to connect", e))
        }
    }

    #[cfg(not(feature = "postgres-tls"))]
    async fn connect_with_tls(
        config: &ConnectionConfig,
    ) -> Result<(
        tokio_postgres::Client,
        impl std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>,
    )> {
        tokio_postgres::connect(&config.url, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::connection_with_source("failed to connect", e))
    }

    /// Check whether the config requests TLS (via URL params or properties).
    #[cfg(feature = "postgres-tls")]
    fn wants_tls(config: &ConnectionConfig) -> bool {
        // Check properties map
        if let Some(mode) = config.properties.get("sslmode") {
            return mode != "disable";
        }
        // Check URL query parameters
        if let Ok(url) = url::Url::parse(&config.url) {
            for (k, v) in url.query_pairs() {
                if k == "sslmode" {
                    return v != "disable";
                }
            }
        }
        // Default: prefer TLS when the feature is compiled in
        true
    }

    #[cfg(feature = "postgres-tls")]
    fn root_cert_store() -> rustls::RootCertStore {
        let mut store = rustls::RootCertStore::empty();
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        store
    }
}

/// PostgreSQL schema provider
pub struct PgSchemaProvider {
    conn: Arc<dyn Connection>,
    dialect: PostgresDialect,
}

impl PgSchemaProvider {
    /// Create a new schema provider
    pub fn new(conn: Arc<dyn Connection>) -> Self {
        Self {
            conn,
            dialect: PostgresDialect,
        }
    }
}

#[async_trait]
impl SchemaProvider for PgSchemaProvider {
    async fn list_schemas(&self) -> Result<Vec<String>> {
        let rows = self
            .conn
            .query(
                "SELECT schema_name FROM information_schema.schemata \
                 WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
                 ORDER BY schema_name",
                &[],
            )
            .await?;

        Ok(rows
            .iter()
            .filter_map(|r| {
                r.get_by_name("schema_name")
                    .and_then(|v| v.as_str().map(String::from))
            })
            .collect())
    }

    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>> {
        let schema = schema.unwrap_or("public");
        let rows = self
            .conn
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_type = 'BASE TABLE' \
                 ORDER BY table_name",
                &[Value::String(schema.to_string())],
            )
            .await?;

        Ok(rows
            .iter()
            .filter_map(|r| {
                r.get_by_name("table_name")
                    .and_then(|v| v.as_str().map(String::from))
            })
            .collect())
    }

    async fn get_table(&self, schema: Option<&str>, table: &str) -> Result<Option<TableMetadata>> {
        let schema = schema.unwrap_or("public");
        validate_sql_identifier(table)?;
        validate_sql_identifier(schema)?;
        let sql = self.dialect.list_columns_sql(Some(schema), table);
        let rows = self.conn.query(&sql, &[]).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut table_meta = TableMetadata::new(table);
        table_meta.schema = Some(schema.to_string());

        for row in &rows {
            let name = row
                .get_by_name("column_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let type_name = row
                .get_by_name("data_type")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let nullable = row
                .get_by_name("nullable")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            let ordinal = row
                .get_by_name("ordinal_position")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as u32;

            let max_length = row
                .get_by_name("character_maximum_length")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32);

            let precision = row
                .get_by_name("numeric_precision")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32);

            let scale = row
                .get_by_name("numeric_scale")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32);

            let pk_ordinal = row
                .get_by_name("pk_ordinal")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32);

            let default_value = row
                .get_by_name("column_default")
                .and_then(|v| v.as_str())
                .map(String::from);

            let col = ColumnMetadata {
                name,
                type_name,
                nullable,
                primary_key_ordinal: pk_ordinal,
                ordinal,
                max_length,
                precision,
                scale,
                default_value,
                auto_increment: false, // Would need separate query
                comment: None,
            };

            table_meta.columns.push(col);
        }

        Ok(Some(table_meta))
    }

    async fn list_indexes(&self, schema: Option<&str>, table: &str) -> Result<Vec<IndexMetadata>> {
        let schema = schema.unwrap_or("public");
        let rows = self
            .conn
            .query(
                r#"SELECT 
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary,
                    am.amname as index_type
                FROM pg_class t
                JOIN pg_namespace n ON t.relnamespace = n.oid
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON ix.indexrelid = i.oid
                JOIN pg_am am ON i.relam = am.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE n.nspname = $1 AND t.relname = $2
                ORDER BY i.relname, a.attnum"#,
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        let mut indexes: HashMap<String, IndexMetadata> = HashMap::new();

        for row in &rows {
            let index_name = row
                .get_by_name("index_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let column_name = row
                .get_by_name("column_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let is_unique = row
                .get_by_name("is_unique")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let is_primary = row
                .get_by_name("is_primary")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let index_type = row
                .get_by_name("index_type")
                .and_then(|v| v.as_str())
                .map(String::from);

            let entry = indexes
                .entry(index_name.to_string())
                .or_insert_with(|| IndexMetadata {
                    schema: Some(schema.to_string()),
                    table: table.to_string(),
                    name: index_name.to_string(),
                    columns: vec![],
                    unique: is_unique,
                    primary: is_primary,
                    index_type,
                    predicate: None,
                });

            entry.columns.push(column_name.to_string());
        }

        Ok(indexes.into_values().collect())
    }

    async fn list_foreign_keys(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<ForeignKeyMetadata>> {
        let schema = schema.unwrap_or("public");
        let rows = self
            .conn
            .query(
                r#"SELECT 
                    tc.constraint_name,
                    kcu.column_name as source_column,
                    ccu.table_schema as target_schema,
                    ccu.table_name as target_table,
                    ccu.column_name as target_column,
                    rc.delete_rule,
                    rc.update_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.table_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = $1
                    AND tc.table_name = $2
                ORDER BY tc.constraint_name, kcu.ordinal_position"#,
                &[
                    Value::String(schema.to_string()),
                    Value::String(table.to_string()),
                ],
            )
            .await?;

        let mut fks: HashMap<String, ForeignKeyMetadata> = HashMap::new();

        for row in &rows {
            let constraint_name = row
                .get_by_name("constraint_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let source_column = row
                .get_by_name("source_column")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let target_schema = row
                .get_by_name("target_schema")
                .and_then(|v| v.as_str())
                .map(String::from);

            let target_table = row
                .get_by_name("target_table")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let target_column = row
                .get_by_name("target_column")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            let delete_rule = row
                .get_by_name("delete_rule")
                .and_then(|v| v.as_str())
                .map(parse_fk_action)
                .unwrap_or_default();

            let update_rule = row
                .get_by_name("update_rule")
                .and_then(|v| v.as_str())
                .map(parse_fk_action)
                .unwrap_or_default();

            let entry =
                fks.entry(constraint_name.to_string())
                    .or_insert_with(|| ForeignKeyMetadata {
                        name: constraint_name.to_string(),
                        source_schema: Some(schema.to_string()),
                        source_table: table.to_string(),
                        source_columns: vec![],
                        target_schema,
                        target_table: target_table.to_string(),
                        target_columns: vec![],
                        on_delete: delete_rule,
                        on_update: update_rule,
                    });

            entry.source_columns.push(source_column.to_string());
            entry.target_columns.push(target_column.to_string());
        }

        Ok(fks.into_values().collect())
    }
}

fn parse_fk_action(action: &str) -> ForeignKeyAction {
    match action.to_uppercase().as_str() {
        "CASCADE" => ForeignKeyAction::Cascade,
        "RESTRICT" => ForeignKeyAction::Restrict,
        "SET NULL" => ForeignKeyAction::SetNull,
        "SET DEFAULT" => ForeignKeyAction::SetDefault,
        _ => ForeignKeyAction::NoAction,
    }
}

/// Connect to PostgreSQL database
pub async fn connect(url: &str) -> Result<Box<dyn Connection>> {
    PgConnectionFactory
        .connect(&ConnectionConfig::new(url))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversion() {
        // Test various value types
        let _ = value_to_sql(&Value::Int32(42));
        let _ = value_to_sql(&Value::String("hello".into()));
        let _ = value_to_sql(&Value::Null);
        let _ = value_to_sql(&Value::Bool(true));
    }

    #[test]
    fn test_pg_connection_factory_type() {
        let factory = PgConnectionFactory;
        assert_eq!(factory.database_type(), DatabaseType::PostgreSQL);
    }

    #[test]
    fn test_parse_fk_action() {
        assert_eq!(parse_fk_action("CASCADE"), ForeignKeyAction::Cascade);
        assert_eq!(parse_fk_action("SET NULL"), ForeignKeyAction::SetNull);
        assert_eq!(parse_fk_action("NO ACTION"), ForeignKeyAction::NoAction);
    }
}
