//! RDBC Source Connector
//!
//! Query-based source connector using rivven-rdbc for database connectivity.
//!
//! # Features
//!
//! - **Polling-based**: Efficient incremental data capture
//! - **Multiple modes**: Bulk, incrementing, timestamp, or combined
//! - **Connection pooling**: Configurable pool for high throughput
//! - **Offset tracking**: Resume from last position
//! - **Prepared statements**: Query parsed once for hot path
//!
//! # Example
//!
//! ```yaml
//! connectors:
//!   - name: users-source
//!     type: rdbc-source
//!     config:
//!       connection_url: "postgres://user:pass@localhost/db"
//!       table: users
//!       mode: incrementing
//!       incrementing_column: id
//!       poll_interval_ms: 1000
//!       batch_size: 1000
//! ```

use async_trait::async_trait;
use futures::stream::BoxStream;
use rivven_rdbc::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error, info};
use validator::Validate;

use crate::connectors::{AnySource, SourceFactory};
use crate::error::{ConnectorError, Result};
use crate::prelude::*;
use crate::types::SensitiveString;

/// Query mode for RDBC source
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum RdbcQueryMode {
    /// Full table scan
    #[default]
    Bulk,
    /// Incrementing column mode
    Incrementing,
    /// Timestamp column mode
    Timestamp,
    /// Combined mode
    TimestampIncrementing,
}

/// RDBC Source Connector Configuration
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct RdbcSourceConfig {
    /// Database connection URL
    pub connection_url: SensitiveString,

    /// Schema name (e.g., "public")
    #[serde(default)]
    pub schema: Option<String>,

    /// Table name to read from
    #[validate(length(min = 1))]
    pub table: String,

    /// Query mode for tracking changes
    #[serde(default)]
    pub mode: RdbcQueryMode,

    /// Column name for incrementing mode
    #[serde(default)]
    pub incrementing_column: Option<String>,

    /// Column name for timestamp mode
    #[serde(default)]
    pub timestamp_column: Option<String>,

    /// Poll interval in milliseconds (default: 5000)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,

    /// Maximum rows per poll (default: 1000)
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: u32,

    /// Topic/stream name (default: table name)
    #[serde(default)]
    pub topic: Option<String>,

    /// Connection pool size (default: 1)
    ///
    /// For sources, a single connection is typically sufficient since reads
    /// are sequential. Set higher for parallel read scenarios.
    #[serde(default = "default_pool_size")]
    #[validate(range(min = 1, max = 16))]
    pub pool_size: u32,

    /// Minimum pool size for warm-up (default: 1)
    ///
    /// Number of connections to create at startup. Useful for reducing
    /// latency on first poll.
    #[serde(default = "default_min_pool_size")]
    #[validate(range(min = 1, max = 16))]
    pub min_pool_size: u32,

    /// Maximum connection lifetime in seconds (default: 3600 = 1 hour)
    ///
    /// Connections older than this are recycled to prevent stale connections.
    #[serde(default = "default_max_lifetime_secs")]
    pub max_lifetime_secs: u64,

    /// Idle connection timeout in seconds (default: 600 = 10 minutes)
    ///
    /// Idle connections exceeding this timeout are recycled.
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,

    /// Connection acquire timeout in milliseconds (default: 30000 = 30 seconds)
    #[serde(default = "default_acquire_timeout_ms")]
    pub acquire_timeout_ms: u64,
}

fn default_poll_interval() -> u64 {
    5000
}

fn default_batch_size() -> u32 {
    1000
}

fn default_pool_size() -> u32 {
    1
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

impl Default for RdbcSourceConfig {
    fn default() -> Self {
        Self {
            connection_url: SensitiveString::new(""),
            schema: None,
            table: String::new(),
            mode: RdbcQueryMode::Bulk,
            incrementing_column: None,
            timestamp_column: None,
            poll_interval_ms: default_poll_interval(),
            batch_size: default_batch_size(),
            topic: None,
            pool_size: default_pool_size(),
            min_pool_size: default_min_pool_size(),
            max_lifetime_secs: default_max_lifetime_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            acquire_timeout_ms: default_acquire_timeout_ms(),
        }
    }
}

impl RdbcSourceConfig {
    /// Get the topic name
    pub fn topic_name(&self) -> &str {
        self.topic.as_deref().unwrap_or(&self.table)
    }

    /// Validate mode-specific requirements
    pub fn validate_mode(&self) -> std::result::Result<(), String> {
        match self.mode {
            RdbcQueryMode::Incrementing if self.incrementing_column.is_none() => {
                Err("'incrementing_column' required for incrementing mode".to_string())
            }
            RdbcQueryMode::Timestamp if self.timestamp_column.is_none() => {
                Err("'timestamp_column' required for timestamp mode".to_string())
            }
            RdbcQueryMode::TimestampIncrementing
                if self.incrementing_column.is_none() || self.timestamp_column.is_none() =>
            {
                Err("Both columns required for timestamp_incrementing mode".to_string())
            }
            _ => Ok(()),
        }
    }
}

/// RDBC Source connector
pub struct RdbcSource;

impl RdbcSource {
    /// Create a new RDBC source
    pub fn new() -> Self {
        Self
    }
}

impl Default for RdbcSource {
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
    config: &RdbcSourceConfig,
) -> std::result::Result<std::sync::Arc<SimpleConnectionPool>, rivven_rdbc::Error> {
    use rivven_rdbc::postgres::PgConnectionFactory;
    use std::time::Duration;
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
    _config: &RdbcSourceConfig,
) -> std::result::Result<std::sync::Arc<SimpleConnectionPool>, rivven_rdbc::Error> {
    Err(rivven_rdbc::Error::config(
        "No RDBC backend enabled. Enable 'rdbc-postgres' feature.",
    ))
}

#[async_trait]
impl Source for RdbcSource {
    type Config = RdbcSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("rdbc-source", env!("CARGO_PKG_VERSION"))
            .description("Query-based RDBC source connector using rivven-rdbc")
            .author("Rivven Team")
            .license("Apache-2.0")
            .incremental(true)
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
                    info!(table = %config.table, "RDBC source connection check passed");
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

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let schema = serde_json::json!({
            "type": "object",
            "description": format!("Records from table {}", config.table)
        });

        let stream = Stream::new(&config.table, schema)
            .with_sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

        Ok(Catalog::new().add_stream(stream))
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config = config.clone();

        // Create connection pool with full lifecycle configuration
        let pool = create_pool(&config)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        // Parse initial offset from state
        let initial_offset = state
            .as_ref()
            .and_then(|s| s.get_stream(config.topic_name()))
            .and_then(|ss| ss.cursor_value.clone());

        let stream = async_stream::stream! {
            let topic = config.topic_name().to_string();
            let poll_interval = Duration::from_millis(config.poll_interval_ms);

            info!(
                table = %config.table,
                mode = ?config.mode,
                pool_size = config.pool_size,
                max_lifetime_secs = config.max_lifetime_secs,
                idle_timeout_secs = config.idle_timeout_secs,
                "Starting RDBC source with connection pool"
            );

            // Track current offset
            let mut current_inc: Option<i64> = initial_offset
                .as_ref()
                .and_then(|v| v.get("incrementing"))
                .and_then(|v| v.as_i64());

            let mut current_ts: Option<String> = initial_offset
                .as_ref()
                .and_then(|v| v.get("timestamp"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            loop {
                // Acquire connection from pool for each poll iteration
                let conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!(error = %e, "Failed to acquire connection from pool");
                        yield Err(ConnectorError::Connection(e.to_string()).into());
                        tokio::time::sleep(poll_interval).await;
                        continue;
                    }
                };

                // Build query
                let (sql, params) = build_query(&config, current_inc, current_ts.as_deref());
                debug!(sql = %sql, "Executing poll");

                match conn.query(&sql, &params).await {
                    Ok(rows) => {
                        let count = rows.len();
                        debug!(rows = count, "Poll returned rows");

                        for row in rows {
                            let mut data = serde_json::Map::new();
                            for (col, val) in row.columns().iter().zip(row.values().iter()) {
                                data.insert(col.clone(), value_to_json(val));
                            }

                            // Update offsets
                            if let Some(col) = &config.incrementing_column {
                                if let Some(v) = data.get(col).and_then(|v| v.as_i64()) {
                                    current_inc = Some(v);
                                }
                            }
                            if let Some(col) = &config.timestamp_column {
                                if let Some(v) = data.get(col).and_then(|v| v.as_str()) {
                                    current_ts = Some(v.to_string());
                                }
                            }

                            let event = SourceEvent::record(&topic, serde_json::Value::Object(data));
                            yield Ok(event);
                        }

                        // Emit state checkpoint if we got data
                        if count > 0 {
                            let mut cursor = serde_json::Map::new();
                            if let Some(inc) = current_inc {
                                cursor.insert("incrementing".to_string(), serde_json::json!(inc));
                            }
                            if let Some(ref ts) = current_ts {
                                cursor.insert("timestamp".to_string(), serde_json::json!(ts));
                            }
                            let state_event = SourceEvent::state(serde_json::Value::Object(cursor));
                            yield Ok(state_event);
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Query failed");
                        yield Err(ConnectorError::Connection(e.to_string()).into());
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Build SQL query based on mode and current offset
fn build_query(
    config: &RdbcSourceConfig,
    inc_offset: Option<i64>,
    ts_offset: Option<&str>,
) -> (String, Vec<Value>) {
    let table = match &config.schema {
        Some(s) => format!(r#""{}"."{}""#, s, config.table),
        None => format!(r#""{}""#, config.table),
    };

    let mut conditions = Vec::new();
    let mut params = Vec::new();

    match config.mode {
        RdbcQueryMode::Incrementing => {
            if let (Some(col), Some(val)) = (&config.incrementing_column, inc_offset) {
                conditions.push(format!(r#""{}" > $1"#, col));
                params.push(Value::Int64(val));
            }
        }
        RdbcQueryMode::Timestamp => {
            if let (Some(col), Some(val)) = (&config.timestamp_column, ts_offset) {
                conditions.push(format!(r#""{}" > $1"#, col));
                params.push(Value::String(val.to_string()));
            }
        }
        RdbcQueryMode::TimestampIncrementing => {
            if let (Some(ts_col), Some(inc_col), Some(ts_val), Some(inc_val)) = (
                &config.timestamp_column,
                &config.incrementing_column,
                ts_offset,
                inc_offset,
            ) {
                conditions.push(format!(
                    r#"("{}" > $1 OR ("{}" = $1 AND "{}" > $2))"#,
                    ts_col, ts_col, inc_col
                ));
                params.push(Value::String(ts_val.to_string()));
                params.push(Value::Int64(inc_val));
            }
        }
        RdbcQueryMode::Bulk => {}
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let order_by = match config.mode {
        RdbcQueryMode::Incrementing => config
            .incrementing_column
            .as_ref()
            .map(|c| format!(r#" ORDER BY "{}" ASC"#, c))
            .unwrap_or_default(),
        RdbcQueryMode::Timestamp => config
            .timestamp_column
            .as_ref()
            .map(|c| format!(r#" ORDER BY "{}" ASC"#, c))
            .unwrap_or_default(),
        RdbcQueryMode::TimestampIncrementing => {
            let ts = config.timestamp_column.as_deref().unwrap_or("updated_at");
            let inc = config.incrementing_column.as_deref().unwrap_or("id");
            format!(r#" ORDER BY "{}", "{}" ASC"#, ts, inc)
        }
        RdbcQueryMode::Bulk => String::new(),
    };

    let sql = format!(
        "SELECT * FROM {}{}{} LIMIT {}",
        table, where_clause, order_by, config.batch_size
    );

    (sql, params)
}

/// Convert Value to JSON
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::json!(b),
        Value::Int8(n) => serde_json::json!(n),
        Value::Int16(n) => serde_json::json!(n),
        Value::Int32(n) => serde_json::json!(n),
        Value::Int64(n) => serde_json::json!(n),
        Value::Float32(n) => serde_json::json!(n),
        Value::Float64(n) => serde_json::json!(n),
        Value::Decimal(d) => serde_json::json!(d.to_string()),
        Value::String(s) => serde_json::json!(s),
        Value::Bytes(b) => serde_json::json!(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b
        )),
        Value::Date(d) => serde_json::json!(d.to_string()),
        Value::Time(t) => serde_json::json!(t.to_string()),
        Value::DateTime(dt) => serde_json::json!(dt.to_string()),
        Value::DateTimeTz(dt) => serde_json::json!(dt.to_rfc3339()),
        Value::Uuid(u) => serde_json::json!(u.to_string()),
        Value::Json(j) => j.clone(),
        Value::Array(arr) => serde_json::Value::Array(arr.iter().map(value_to_json).collect()),
        Value::Interval(i) => serde_json::json!(i),
        Value::Bit(b) => serde_json::json!(hex::encode(b)),
        Value::Enum(e) => serde_json::json!(e),
        Value::Geometry(g) | Value::Geography(g) => serde_json::json!(hex::encode(g)),
        Value::Range {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
        } => serde_json::json!({
            "lower": lower.as_ref().map(|v| value_to_json(v)),
            "upper": upper.as_ref().map(|v| value_to_json(v)),
            "lower_inclusive": lower_inclusive,
            "upper_inclusive": upper_inclusive
        }),
        Value::Composite(m) => {
            let obj: serde_json::Map<String, serde_json::Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Custom { type_name, data } => serde_json::json!({
            "type_name": type_name,
            "data": hex::encode(data)
        }),
    }
}

/// Factory for creating RdbcSource instances
pub struct RdbcSourceFactory;

impl SourceFactory for RdbcSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        RdbcSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(RdbcSourceWrapper(RdbcSource::new()))
    }
}

/// Wrapper for type-erased source operations
struct RdbcSourceWrapper(RdbcSource);

#[async_trait]
impl AnySource for RdbcSourceWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: RdbcSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Validation failed: {}", e)))?;

        self.0.check(&typed_config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let typed_config: RdbcSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.discover(&typed_config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let typed_config: RdbcSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;

        typed_config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Validation failed: {}", e)))?;

        self.0.read(&typed_config, catalog, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config: RdbcSourceConfig = serde_json::from_str(
            r#"{"connection_url": "postgres://localhost/test", "table": "users"}"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_ms, 5000);
        assert_eq!(config.batch_size, 1000);
        assert!(matches!(config.mode, RdbcQueryMode::Bulk));
    }

    #[test]
    fn test_mode_validation() {
        let config = RdbcSourceConfig {
            connection_url: "postgres://localhost/test".into(),
            table: "users".to_string(),
            mode: RdbcQueryMode::Incrementing,
            incrementing_column: None,
            ..Default::default()
        };

        assert!(config.validate_mode().is_err());
    }

    #[test]
    fn test_build_query_bulk() {
        let config = RdbcSourceConfig {
            connection_url: "postgres://localhost/test".into(),
            schema: Some("public".to_string()),
            table: "users".to_string(),
            batch_size: 100,
            ..Default::default()
        };

        let (sql, params) = build_query(&config, None, None);

        assert!(sql.contains(r#""public"."users""#));
        assert!(sql.contains("LIMIT 100"));
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_query_incrementing() {
        let config = RdbcSourceConfig {
            connection_url: "postgres://localhost/test".into(),
            table: "users".to_string(),
            mode: RdbcQueryMode::Incrementing,
            incrementing_column: Some("id".to_string()),
            batch_size: 100,
            ..Default::default()
        };

        let (sql, params) = build_query(&config, Some(42), None);

        assert!(sql.contains(r#""id" > $1"#));
        assert!(sql.contains(r#"ORDER BY "id" ASC"#));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_spec() {
        let spec = RdbcSource::spec();
        assert_eq!(spec.connector_type, "rdbc-source");
        assert!(spec.supports_incremental);
    }

    #[test]
    fn test_pool_lifecycle_config_defaults() {
        let config: RdbcSourceConfig = serde_json::from_str(
            r#"{"connection_url": "postgres://localhost/test", "table": "users"}"#,
        )
        .unwrap();

        // Verify new pool lifecycle defaults
        assert_eq!(config.max_lifetime_secs, 3600);
        assert_eq!(config.idle_timeout_secs, 600);
        assert_eq!(config.acquire_timeout_ms, 30000);
        assert_eq!(config.pool_size, 1); // Sources default to 1
        assert_eq!(config.min_pool_size, 1);
    }

    #[test]
    fn test_pool_lifecycle_config_custom() {
        let config: RdbcSourceConfig = serde_json::from_str(
            r#"{
                "connection_url": "postgres://localhost/test",
                "table": "users",
                "pool_size": 4,
                "min_pool_size": 2,
                "max_lifetime_secs": 1800,
                "idle_timeout_secs": 300,
                "acquire_timeout_ms": 15000
            }"#,
        )
        .unwrap();

        assert_eq!(config.pool_size, 4);
        assert_eq!(config.min_pool_size, 2);
        assert_eq!(config.max_lifetime_secs, 1800);
        assert_eq!(config.idle_timeout_secs, 300);
        assert_eq!(config.acquire_timeout_ms, 15000);
    }
}
