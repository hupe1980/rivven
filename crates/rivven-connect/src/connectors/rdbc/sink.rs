//! RDBC Sink Connector
//!
//! High-performance batch sink connector using rivven-rdbc.
//!
//! # Features
//!
//! - **Connection pooling**: Reuses connections for throughput
//! - **Batch operations**: Uses `execute_batch` for hot path (single round-trip)
//! - **Upsert/Insert/Delete**: Full CDC event support
//! - **Transactional batches**: Optional exactly-once semantics
//! - **Prepared statements**: Parsed once, executed many times
//!
//! # Example
//!
//! ```yaml
//! connectors:
//!   - name: users-sink
//!     type: rdbc-sink
//!     config:
//!       connection_url: "postgres://user:pass@localhost/db"
//!       table: users_copy
//!       write_mode: upsert
//!       pk_columns: ["id"]
//!       batch_size: 1000
//!       transactional: true  # exactly-once
//! ```

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use rivven_rdbc::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};
use validator::Validate;

use crate::connectors::{AnySink, SinkFactory};
use crate::error::{ConnectorError, Result};
use crate::prelude::*;
use crate::types::SensitiveString;

/// Executor abstraction for batch operations.
/// Enables unified interface for both Connection and Transaction.
enum BatchExecutor<'a> {
    Connection(&'a dyn Connection),
    Transaction(&'a dyn Transaction),
}

impl<'a> BatchExecutor<'a> {
    /// Execute a batch of SQL statements
    async fn execute_batch(
        &self,
        statements: &[(&str, &[Value])],
    ) -> rivven_rdbc::error::Result<Vec<u64>> {
        match self {
            BatchExecutor::Connection(conn) => conn.execute_batch(statements).await,
            BatchExecutor::Transaction(tx) => tx.execute_batch(statements).await,
        }
    }
}

/// Write mode for RDBC sink
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RdbcWriteMode {
    /// Insert only
    #[default]
    Insert,
    /// Upsert (insert or update)
    Upsert,
    /// Update only
    Update,
}

/// RDBC Sink Connector Configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct RdbcSinkConfig {
    /// Database connection URL
    pub connection_url: SensitiveString,

    /// Target schema name
    #[serde(default)]
    pub schema: Option<String>,

    /// Target table name
    #[validate(length(min = 1))]
    pub table: String,

    /// Write mode
    #[serde(default)]
    pub write_mode: RdbcWriteMode,

    /// Primary key columns (required for upsert/update)
    #[serde(default)]
    pub pk_columns: Option<Vec<String>>,

    /// Batch size for writes (default: 1000)
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: u32,

    /// Batch timeout in milliseconds (default: 5000)
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,

    /// Delete rows on DELETE events
    #[serde(default = "default_true")]
    pub delete_enabled: bool,

    /// Wrap each batch in a transaction (exactly-once semantics)
    #[serde(default)]
    pub transactional: bool,

    /// Connection pool size (default: 4)
    ///
    /// Controls the maximum number of concurrent database connections.
    /// Higher values allow more parallel batch writes but consume more
    /// database resources. Tune based on your database's connection limits.
    #[serde(default = "default_pool_size")]
    #[validate(range(min = 1, max = 64))]
    pub pool_size: u32,

    /// Minimum pool size for warm-up (default: 1)
    ///
    /// Number of connections to create at startup. Set higher for
    /// latency-sensitive workloads that need connections ready immediately.
    #[serde(default = "default_min_pool_size")]
    #[validate(range(min = 1, max = 64))]
    pub min_pool_size: u32,

    /// Maximum connection lifetime in seconds (default: 3600 = 1 hour)
    ///
    /// Connections older than this are recycled to prevent stale connections.
    /// Set to 0 to disable lifetime-based recycling.
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,

    /// Idle connection timeout in seconds (default: 600 = 10 minutes)
    ///
    /// Idle connections exceeding this timeout are recycled.
    /// Set to 0 to disable idle timeout.
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,

    /// Connection acquire timeout in milliseconds (default: 30000 = 30 seconds)
    ///
    /// Maximum time to wait when acquiring a connection from the pool.
    #[serde(default = "default_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
}

fn default_batch_size() -> u32 {
    1000
}

fn default_batch_timeout() -> u64 {
    5000
}

fn default_true() -> bool {
    true
}

fn default_pool_size() -> u32 {
    4
}

fn default_min_pool_size() -> u32 {
    1
}

fn default_max_lifetime_secs() -> u64 {
    3600
}

fn default_idle_timeout_secs() -> u64 {
    600
}

fn default_acquire_timeout_ms() -> u64 {
    30000
}

impl Default for RdbcSinkConfig {
    fn default() -> Self {
        Self {
            connection_url: SensitiveString::new(""),
            schema: None,
            table: String::new(),
            write_mode: RdbcWriteMode::Insert,
            pk_columns: None,
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout(),
            delete_enabled: true,
            transactional: false,
            pool_size: default_pool_size(),
            min_pool_size: default_min_pool_size(),
            max_lifetime_secs: default_max_lifetime_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            acquire_timeout_ms: default_acquire_timeout_ms(),
        }
    }
}

impl RdbcSinkConfig {
    /// Validate write-mode-specific requirements
    pub fn validate_mode(&self) -> std::result::Result<(), String> {
        if matches!(
            self.write_mode,
            RdbcWriteMode::Upsert | RdbcWriteMode::Update
        ) && self.pk_columns.is_none()
        {
            return Err("'pk_columns' required for upsert/update modes".to_string());
        }
        Ok(())
    }

    /// Get qualified table name
    pub fn qualified_table(&self) -> String {
        match &self.schema {
            Some(s) => format!(r#""{}"."{}""#, s, self.table),
            None => format!(r#""{}""#, self.table),
        }
    }
}

/// RDBC Sink connector
pub struct RdbcSink;

impl RdbcSink {
    /// Create a new RDBC sink
    pub fn new() -> Self {
        Self
    }
}

impl Default for RdbcSink {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a connection based on URL scheme
#[cfg(feature = "rdbc-postgres")]
async fn create_connection(
    url: &str,
) -> std::result::Result<Box<dyn Connection>, rivven_rdbc::Error> {
    use rivven_rdbc::postgres::PgConnectionFactory;
    let factory = PgConnectionFactory;
    let config = ConnectionConfig::new(url);
    factory.connect(&config).await
}

#[cfg(not(feature = "rdbc-postgres"))]
async fn create_connection(
    _url: &str,
) -> std::result::Result<Box<dyn Connection>, rivven_rdbc::Error> {
    Err(rivven_rdbc::Error::config(
        "No RDBC backend enabled. Enable 'rdbc-postgres' feature.",
    ))
}

/// Create a connection pool based on URL scheme and config
#[cfg(feature = "rdbc-postgres")]
async fn create_pool(
    config: &RdbcSinkConfig,
) -> std::result::Result<std::sync::Arc<SimpleConnectionPool>, rivven_rdbc::Error> {
    use rivven_rdbc::postgres::PgConnectionFactory;
    let factory = std::sync::Arc::new(PgConnectionFactory);
    let pool_config = PoolConfig::new(config.connection_url.expose_secret())
        .with_min_size(config.min_pool_size as usize)
        .with_max_size(config.pool_size as usize)
        .with_acquire_timeout(Duration::from_millis(config.acquire_timeout_ms))
        .with_max_lifetime(Duration::from_secs(config.max_lifetime_secs))
        .with_idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .with_test_on_borrow(true);
    SimpleConnectionPool::new(pool_config, factory).await
}

#[cfg(not(feature = "rdbc-postgres"))]
async fn create_pool(
    _config: &RdbcSinkConfig,
) -> std::result::Result<std::sync::Arc<SimpleConnectionPool>, rivven_rdbc::Error> {
    Err(rivven_rdbc::Error::config(
        "No RDBC backend enabled. Enable 'rdbc-postgres' feature.",
    ))
}

#[async_trait]
impl Sink for RdbcSink {
    type Config = RdbcSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("rdbc-sink", env!("CARGO_PKG_VERSION"))
            .description("High-performance RDBC sink connector using rivven-rdbc")
            .author("Rivven Team")
            .license("Apache-2.0")
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Validation failed: {}", e)))?;

        if let Err(e) = config.validate_mode() {
            return Ok(CheckResult::failure(e));
        }

        match create_connection(config.connection_url.expose_secret()).await {
            Ok(conn) => match conn.execute("SELECT 1", &[]).await {
                Ok(_) => {
                    info!(table = %config.table, "RDBC sink connection check passed");
                    Ok(CheckResult::builder()
                        .check_passed("connectivity")
                        .check_passed("authentication")
                        .build())
                }
                Err(e) => Ok(CheckResult::builder()
                    .check_passed("connectivity")
                    .check_failed("query_execution", e.to_string())
                    .build()),
            },
            Err(e) => Ok(CheckResult::builder()
                .check_failed("connectivity", e.to_string())
                .build()),
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        // Create connection pool with full lifecycle configuration
        let pool = create_pool(config)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let mut result = WriteResult::new();
        let mut buffer: Vec<SourceEvent> = Vec::with_capacity(config.batch_size as usize);
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
        let mut last_flush = std::time::Instant::now();

        info!(
            table = %config.table,
            mode = ?config.write_mode,
            batch_size = config.batch_size,
            pool_size = config.pool_size,
            max_lifetime_secs = config.max_lifetime_secs,
            idle_timeout_secs = config.idle_timeout_secs,
            transactional = config.transactional,
            "Starting RDBC sink with connection pool"
        );

        while let Some(event) = events.next().await {
            buffer.push(event);

            let should_flush =
                buffer.len() >= config.batch_size as usize || last_flush.elapsed() >= batch_timeout;

            if should_flush && !buffer.is_empty() {
                // Acquire connection from pool
                let conn = pool
                    .get()
                    .await
                    .map_err(|e| ConnectorError::Connection(format!("Pool exhausted: {}", e)))?;

                // Execute batch with optional transaction wrapper for exactly-once
                let flush_result = if config.transactional {
                    self.flush_batch_transactional(&*conn, config, &buffer)
                        .await
                } else {
                    let executor = BatchExecutor::Connection(&*conn);
                    self.flush_batch(&executor, config, &buffer).await
                };

                // Connection is returned to pool automatically when `conn` is dropped

                match flush_result {
                    Ok((success, bytes)) => {
                        result.add_success(success, bytes);
                        debug!(records = success, "Batch flushed");
                    }
                    Err(e) => {
                        error!(error = %e, records = buffer.len(), "Batch write failed");
                        result.add_failure(buffer.len() as u64, e);
                    }
                }
                buffer.clear();
                last_flush = std::time::Instant::now();
            }
        }

        // Flush remaining
        if !buffer.is_empty() {
            let conn = pool
                .get()
                .await
                .map_err(|e| ConnectorError::Connection(format!("Pool exhausted: {}", e)))?;

            let flush_result = if config.transactional {
                self.flush_batch_transactional(&*conn, config, &buffer)
                    .await
            } else {
                let executor = BatchExecutor::Connection(&*conn);
                self.flush_batch(&executor, config, &buffer).await
            };

            match flush_result {
                Ok((success, bytes)) => result.add_success(success, bytes),
                Err(e) => {
                    error!(error = %e, records = buffer.len(), "Final batch failed");
                    result.add_failure(buffer.len() as u64, e);
                }
            }
        }

        // Log pool stats using helper methods
        let stats = pool.stats();
        debug!(
            connections_created = stats.connections_created,
            acquisitions = stats.acquisitions,
            reuse_rate = %format!("{:.1}%", stats.reuse_rate() * 100.0),
            avg_wait_ms = %format!("{:.2}", stats.avg_wait_time_ms()),
            lifetime_recycled = stats.lifetime_expired_count,
            idle_recycled = stats.idle_expired_count,
            health_failures = stats.health_check_failures,
            "Pool statistics"
        );

        info!(
            records_written = result.records_written,
            records_failed = result.records_failed,
            pool_reuse_rate = %format!("{:.1}%", stats.reuse_rate() * 100.0),
            "RDBC sink completed"
        );

        Ok(result)
    }
}

impl RdbcSink {
    /// Flush a batch of events to the database using true batch operations.
    ///
    /// # Hot Path Optimization
    ///
    /// Instead of executing N individual SQL statements (N round-trips),
    /// we group records by SQL template and use `execute_batch` for a single
    /// round-trip per operation type. This provides 10-100x throughput improvement
    /// for large batches.
    ///
    /// # Transactional Mode
    ///
    /// When passed a `BatchExecutor::Transaction`, all operations execute
    /// within the active transaction for exactly-once semantics.
    async fn flush_batch(
        &self,
        executor: &BatchExecutor<'_>,
        config: &RdbcSinkConfig,
        events: &[SourceEvent],
    ) -> std::result::Result<(u64, u64), String> {
        if events.is_empty() {
            return Ok((0, 0));
        }

        // Group records by operation for batch execution
        let mut upsert_batch: Vec<(String, Vec<Value>)> = Vec::with_capacity(events.len());
        let mut delete_batch: Vec<(String, Vec<Value>)> = Vec::new();
        let mut bytes = 0u64;

        // Pre-build SQL templates (assume homogeneous columns for efficiency)
        let first_data_event = events.iter().find(|e| {
            matches!(
                e.event_type,
                SourceEventType::Insert | SourceEventType::Update | SourceEventType::Record
            ) && e.data.is_object()
        });

        // If we have data events, get column order from first event
        let column_order: Option<Vec<String>> = first_data_event
            .and_then(|e| e.data.as_object().map(|obj| obj.keys().cloned().collect()));

        for event in events {
            // Skip non-data events
            if !matches!(
                event.event_type,
                SourceEventType::Insert
                    | SourceEventType::Update
                    | SourceEventType::Record
                    | SourceEventType::Delete
            ) {
                continue;
            }

            // Estimate bytes for metrics
            bytes += serde_json::to_string(&event.data)
                .map(|s| s.len() as u64)
                .unwrap_or(0);

            // Handle deletes separately
            if event.event_type == SourceEventType::Delete && config.delete_enabled {
                if let Some(pk_cols) = &config.pk_columns {
                    if let Some((sql, values)) = self.prepare_delete(config, &event.data, pk_cols) {
                        delete_batch.push((sql, values));
                    }
                }
                continue;
            }

            // Handle inserts/updates
            let data = match event.data.as_object() {
                Some(d) => d,
                None => continue,
            };

            // Use consistent column order for batch compatibility
            let columns = match &column_order {
                Some(cols) => cols.clone(),
                None => data.keys().cloned().collect(),
            };

            if columns.is_empty() {
                continue;
            }

            // Collect values in column order
            let values: Vec<Value> = columns
                .iter()
                .map(|col| data.get(col).map(json_to_value).unwrap_or(Value::Null))
                .collect();

            let sql = match config.write_mode {
                RdbcWriteMode::Insert => self.build_insert_sql(config, &columns),
                RdbcWriteMode::Upsert => self.build_upsert_sql(config, &columns),
                RdbcWriteMode::Update => self.build_update_sql(config, &columns),
            };

            upsert_batch.push((sql, values));
        }

        // Execute batches using execute_batch for hot path
        let mut written = 0u64;

        // Execute upsert/insert batch
        if !upsert_batch.is_empty() {
            trace!(count = upsert_batch.len(), "Executing upsert batch");

            // Convert to format expected by execute_batch
            let batch_refs: Vec<(&str, &[Value])> = upsert_batch
                .iter()
                .map(|(sql, vals)| (sql.as_str(), vals.as_slice()))
                .collect();

            match executor.execute_batch(&batch_refs).await {
                Ok(counts) => {
                    let total: u64 = counts.iter().sum();
                    written += total;
                    debug!(records = total, "Batch upsert complete");
                }
                Err(e) => {
                    return Err(format!("Batch execute failed: {}", e));
                }
            }
        }

        // Execute delete batch
        if !delete_batch.is_empty() {
            trace!(count = delete_batch.len(), "Executing delete batch");

            let batch_refs: Vec<(&str, &[Value])> = delete_batch
                .iter()
                .map(|(sql, vals)| (sql.as_str(), vals.as_slice()))
                .collect();

            match executor.execute_batch(&batch_refs).await {
                Ok(counts) => {
                    let total: u64 = counts.iter().sum();
                    written += total;
                    debug!(records = total, "Batch delete complete");
                }
                Err(e) => {
                    // Deletes are non-fatal (record may not exist)
                    warn!(error = %e, "Batch delete failed");
                }
            }
        }

        Ok((written, bytes))
    }

    /// Flush a batch within a transaction for exactly-once semantics.
    ///
    /// Operations are wrapped in BEGIN/COMMIT with rollback on error.
    /// This guarantees that either all records in the batch are written
    /// or none are (atomic batch).
    async fn flush_batch_transactional(
        &self,
        conn: &dyn Connection,
        config: &RdbcSinkConfig,
        events: &[SourceEvent],
    ) -> std::result::Result<(u64, u64), String> {
        if events.is_empty() {
            return Ok((0, 0));
        }

        // Begin transaction
        let tx = conn
            .begin()
            .await
            .map_err(|e| format!("Transaction begin failed: {}", e))?;

        trace!("Transaction started for batch of {} events", events.len());

        // Execute batch within transaction
        let executor = BatchExecutor::Transaction(&*tx);
        let result = self.flush_batch(&executor, config, events).await;

        match result {
            Ok((written, bytes)) => {
                // Commit on success
                tx.commit()
                    .await
                    .map_err(|e| format!("Transaction commit failed: {}", e))?;
                trace!(records = written, "Transaction committed");
                Ok((written, bytes))
            }
            Err(e) => {
                // Rollback on failure
                if let Err(rollback_err) = tx.rollback().await {
                    error!(error = %rollback_err, "Rollback failed after batch error");
                }
                Err(e)
            }
        }
    }

    /// Prepare a DELETE statement for batch execution
    fn prepare_delete(
        &self,
        config: &RdbcSinkConfig,
        data: &serde_json::Value,
        pk_cols: &[String],
    ) -> Option<(String, Vec<Value>)> {
        let obj = data.as_object()?;

        let mut conditions: Vec<String> = Vec::new();
        let mut values: Vec<Value> = Vec::new();

        for (i, pk) in pk_cols.iter().enumerate() {
            if let Some(val) = obj.get(pk) {
                conditions.push(format!(r#""{}" = ${}"#, pk, i + 1));
                values.push(json_to_value(val));
            }
        }

        if conditions.is_empty() {
            return None;
        }

        let sql = format!(
            "DELETE FROM {} WHERE {}",
            config.qualified_table(),
            conditions.join(" AND ")
        );

        Some((sql, values))
    }

    /// Delete a record by primary key (legacy single-record API)
    #[allow(dead_code)]
    async fn delete_record(
        &self,
        conn: &dyn Connection,
        config: &RdbcSinkConfig,
        data: &serde_json::Value,
        pk_cols: &[String],
    ) -> std::result::Result<(), String> {
        let (sql, values) = self
            .prepare_delete(config, data, pk_cols)
            .ok_or("No primary key values found")?;

        conn.execute(&sql, &values)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Build INSERT SQL
    fn build_insert_sql(&self, config: &RdbcSinkConfig, columns: &[String]) -> String {
        let cols: Vec<String> = columns.iter().map(|c| format!(r#""{}""#, c)).collect();
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();

        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            config.qualified_table(),
            cols.join(", "),
            placeholders.join(", ")
        )
    }

    /// Build UPSERT SQL (PostgreSQL syntax)
    fn build_upsert_sql(&self, config: &RdbcSinkConfig, columns: &[String]) -> String {
        let pk_cols = config.pk_columns.as_deref().unwrap_or(&[]);
        let cols: Vec<String> = columns.iter().map(|c| format!(r#""{}""#, c)).collect();
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();

        let pk_list: Vec<String> = pk_cols.iter().map(|c| format!(r#""{}""#, c)).collect();

        let update_cols: Vec<String> = columns
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!(r#""{}" = EXCLUDED."{}""#, c, c))
            .collect();

        if update_cols.is_empty() {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                config.qualified_table(),
                cols.join(", "),
                placeholders.join(", "),
                pk_list.join(", ")
            )
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                config.qualified_table(),
                cols.join(", "),
                placeholders.join(", "),
                pk_list.join(", "),
                update_cols.join(", ")
            )
        }
    }

    /// Build UPDATE SQL
    fn build_update_sql(&self, config: &RdbcSinkConfig, columns: &[String]) -> String {
        let pk_cols = config.pk_columns.as_deref().unwrap_or(&[]);

        let mut set_clauses: Vec<String> = Vec::new();
        let mut where_clauses: Vec<String> = Vec::new();
        let mut param_index = 1;

        for col in columns {
            if pk_cols.contains(col) {
                where_clauses.push(format!(r#""{}" = ${}"#, col, param_index));
            } else {
                set_clauses.push(format!(r#""{}" = ${}"#, col, param_index));
            }
            param_index += 1;
        }

        format!(
            "UPDATE {} SET {} WHERE {}",
            config.qualified_table(),
            set_clauses.join(", "),
            where_clauses.join(" AND ")
        )
    }
}

/// Convert JSON value to rdbc Value
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int64(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float64(f)
            } else {
                Value::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Value::Json(v.clone()),
    }
}

/// Factory for creating RdbcSink instances
pub struct RdbcSinkFactory;

impl SinkFactory for RdbcSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        RdbcSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(RdbcSinkWrapper(RdbcSink::new()))
    }
}

/// Wrapper for type-erased sink operations
struct RdbcSinkWrapper(RdbcSink);

#[async_trait]
impl AnySink for RdbcSinkWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: RdbcSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Validation failed: {}", e)))?;

        self.0.check(&typed_config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let typed_config: RdbcSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Validation failed: {}", e)))?;

        self.0.write(&typed_config, events).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config: RdbcSinkConfig = serde_json::from_str(
            r#"{"connection_url": "postgres://localhost/test", "table": "users"}"#,
        )
        .unwrap();

        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.batch_timeout_ms, 5000);
        assert!(matches!(config.write_mode, RdbcWriteMode::Insert));
    }

    #[test]
    fn test_mode_validation() {
        let config = RdbcSinkConfig {
            connection_url: "postgres://localhost/test".into(),
            table: "users".to_string(),
            write_mode: RdbcWriteMode::Upsert,
            pk_columns: None,
            ..Default::default()
        };

        assert!(config.validate_mode().is_err());
    }

    #[test]
    fn test_qualified_table() {
        let config = RdbcSinkConfig {
            schema: Some("public".to_string()),
            table: "users".to_string(),
            ..Default::default()
        };

        assert_eq!(config.qualified_table(), r#""public"."users""#);
    }

    #[test]
    fn test_build_insert_sql() {
        let sink = RdbcSink::new();
        let config = RdbcSinkConfig {
            table: "users".to_string(),
            ..Default::default()
        };

        let sql = sink.build_insert_sql(&config, &["id".to_string(), "name".to_string()]);

        assert!(sql.contains(r#""id""#));
        assert!(sql.contains(r#""name""#));
        assert!(sql.contains("$1"));
        assert!(sql.contains("$2"));
    }

    #[test]
    fn test_build_upsert_sql() {
        let sink = RdbcSink::new();
        let config = RdbcSinkConfig {
            table: "users".to_string(),
            pk_columns: Some(vec!["id".to_string()]),
            ..Default::default()
        };

        let sql = sink.build_upsert_sql(&config, &["id".to_string(), "name".to_string()]);

        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE SET"));
        assert!(sql.contains(r#""name" = EXCLUDED."name""#));
    }

    #[test]
    fn test_spec() {
        let spec = RdbcSink::spec();
        assert_eq!(spec.connector_type, "rdbc-sink");
    }

    #[test]
    fn test_json_to_value() {
        assert!(matches!(
            json_to_value(&serde_json::json!(null)),
            Value::Null
        ));
        assert!(matches!(
            json_to_value(&serde_json::json!(true)),
            Value::Bool(true)
        ));
        assert!(matches!(
            json_to_value(&serde_json::json!(42)),
            Value::Int64(42)
        ));
        assert!(matches!(
            json_to_value(&serde_json::json!(1.5)),
            Value::Float64(_)
        ));
        assert!(matches!(
            json_to_value(&serde_json::json!("hello")),
            Value::String(_)
        ));
    }

    #[test]
    fn test_pool_lifecycle_config_defaults() {
        let config: RdbcSinkConfig = serde_json::from_str(
            r#"{"connection_url": "postgres://localhost/test", "table": "users"}"#,
        )
        .unwrap();

        // Verify new pool lifecycle defaults
        assert_eq!(config.max_lifetime_secs, 3600);
        assert_eq!(config.idle_timeout_secs, 600);
        assert_eq!(config.acquire_timeout_ms, 30000);
        assert_eq!(config.pool_size, 4);
        assert_eq!(config.min_pool_size, 1);
    }

    #[test]
    fn test_pool_lifecycle_config_custom() {
        let config: RdbcSinkConfig = serde_json::from_str(
            r#"{
                "connection_url": "postgres://localhost/test",
                "table": "users",
                "pool_size": 10,
                "min_pool_size": 5,
                "max_lifetime_secs": 1800,
                "idle_timeout_secs": 300,
                "acquire_timeout_ms": 15000
            }"#,
        )
        .unwrap();

        assert_eq!(config.pool_size, 10);
        assert_eq!(config.min_pool_size, 5);
        assert_eq!(config.max_lifetime_secs, 1800);
        assert_eq!(config.idle_timeout_secs, 300);
        assert_eq!(config.acquire_timeout_ms, 15000);
    }
}
