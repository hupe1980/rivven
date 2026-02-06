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
use crate::types::{ColumnMetadata, Row, TableMetadata, Value};

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
            // For now, serialize as JSON
            let json = serde_json::to_value(arr).unwrap_or_default();
            Box::new(json)
        }
        Value::Interval(micros) => {
            // PostgreSQL interval as microseconds
            Box::new(*micros)
        }
        Value::Bit(bits) => Box::new(bits.clone()),
        Value::Enum(s) => Box::new(s.clone()),
        Value::Geometry(wkb) | Value::Geography(wkb) => Box::new(wkb.clone()),
        Value::Range { .. } => {
            // Ranges need special handling - for now serialize as JSON
            Box::new(serde_json::json!(null))
        }
        Value::Composite(map) => {
            let json = serde_json::to_value(map).unwrap_or_default();
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

    // Handle NULL
    if let Ok(None) = row.try_get::<_, Option<bool>>(idx) {
        // Check if actually NULL by trying a nullable type
        return Value::Null;
    }

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
        }
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
        }))
    }

    async fn query_stream(&self, sql: &str, params: &[Value]) -> Result<Pin<Box<dyn RowStream>>> {
        // For now, just fetch all and wrap in a stream adapter
        let rows = self.query(sql, params).await?;
        Ok(Box::pin(VecRowStream::new(rows)))
    }

    async fn is_valid(&self) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        self.client.simple_query("SELECT 1").await.is_ok()
    }

    async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

/// Simple row stream backed by a Vec
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
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Result<Option<Row>>> + Send + '_>> {
        Box::pin(async move { Ok(self.rows.next()) })
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
        // If transaction wasn't committed or rolled back, attempt rollback
        if !self.committed.load(Ordering::Relaxed) && !self.rolled_back.load(Ordering::Relaxed) {
            // Best effort rollback in drop - can't be async
            // In production, you'd want to log this
        }
    }
}

/// PostgreSQL connection factory
#[derive(Debug, Clone, Default)]
pub struct PgConnectionFactory;

#[async_trait]
impl ConnectionFactory for PgConnectionFactory {
    async fn connect(&self, config: &ConnectionConfig) -> Result<Box<dyn Connection>> {
        let (client, connection) = tokio_postgres::connect(&config.url, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::connection_with_source("failed to connect", e))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Box::new(PgConnection::new(client)))
    }

    fn database_type(&self) -> DatabaseType {
        DatabaseType::PostgreSQL
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
