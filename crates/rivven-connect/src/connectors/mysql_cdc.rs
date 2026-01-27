//! MySQL/MariaDB CDC Source connector
//!
//! Implements the rivven-connect-sdk Source trait for MySQL/MariaDB
//! Change Data Capture using binary log replication.

use super::super::prelude::*;
use crate::connectors::{AnySource, SensitiveString, SourceFactory};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use validator::Validate;

/// MySQL CDC source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct MySqlCdcConfig {
    /// MySQL/MariaDB host
    #[validate(length(min = 1))]
    pub host: String,

    /// MySQL/MariaDB port (default: 3306)
    #[serde(default = "default_port")]
    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    /// Database name (optional, for filtering)
    #[serde(default)]
    pub database: Option<String>,

    /// Username
    #[validate(length(min = 1))]
    pub user: String,

    /// Password (redacted in logs)
    pub password: SensitiveString,

    /// Server ID for replication (must be unique among all replicas)
    #[serde(default = "default_server_id")]
    #[validate(range(min = 1))]
    pub server_id: u32,

    /// Starting binlog filename (empty = current)
    #[serde(default)]
    pub binlog_filename: String,

    /// Starting binlog position (4 = start of file)
    #[serde(default = "default_binlog_position")]
    pub binlog_position: u32,

    /// Use GTID-based replication
    #[serde(default)]
    pub use_gtid: bool,

    /// GTID set for GTID-based replication
    #[serde(default)]
    pub gtid_set: String,

    /// Tables to include (schema.table patterns, empty = all)
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude
    #[serde(default)]
    pub exclude_tables: Vec<String>,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub connect_timeout_secs: u32,
}

fn default_port() -> u16 {
    3306
}

fn default_server_id() -> u32 {
    1001
}

fn default_binlog_position() -> u32 {
    4
}

fn default_connect_timeout() -> u32 {
    10
}

/// MySQL CDC source
pub struct MySqlCdcSource;

impl MySqlCdcSource {
    pub fn new() -> Self {
        Self
    }

    /// Build a rivven-cdc MySqlCdcConfig from our config
    fn build_cdc_config(config: &MySqlCdcConfig) -> rivven_cdc::mysql::MySqlCdcConfig {
        let mut cdc_config = rivven_cdc::mysql::MySqlCdcConfig::new(&config.host, &config.user)
            .with_port(config.port)
            .with_password(config.password.expose())
            .with_server_id(config.server_id);

        if let Some(ref db) = config.database {
            cdc_config = cdc_config.with_database(db);
        }

        if !config.binlog_filename.is_empty() {
            cdc_config =
                cdc_config.with_binlog_position(&config.binlog_filename, config.binlog_position);
        }

        if config.use_gtid && !config.gtid_set.is_empty() {
            cdc_config = cdc_config.with_gtid(&config.gtid_set);
        }

        // Add table filters
        for table in &config.include_tables {
            cdc_config = cdc_config.include_table(table);
        }
        for table in &config.exclude_tables {
            cdc_config = cdc_config.exclude_table(table);
        }

        cdc_config
    }
}

impl Default for MySqlCdcSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for MySqlCdcSource {
    type Config = MySqlCdcConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("mysql-cdc", env!("CARGO_PKG_VERSION"))
            .description("MySQL/MariaDB CDC source using binary log replication")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/mysql-cdc")
            .incremental(true)
            .config_schema::<MySqlCdcConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Validate config first
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!(
            "Checking MySQL connection to {}:{}",
            config.host, config.port
        );

        // Try to connect with timeout
        let timeout = tokio::time::Duration::from_secs(config.connect_timeout_secs as u64);

        match tokio::time::timeout(timeout, async {
            // The MySqlBinlogClient::connect function validates connection
            rivven_cdc::mysql::MySqlBinlogClient::connect(
                &config.host,
                config.port,
                &config.user,
                Some(config.password.expose()),
                config.database.as_deref(),
            )
            .await
        })
        .await
        {
            Ok(Ok(_client)) => {
                info!("MySQL connection check passed");
                Ok(CheckResult::success())
            }
            Ok(Err(e)) => {
                let msg = format!("MySQL connection failed: {}", e);
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
            Err(_) => {
                let msg = format!(
                    "MySQL connection timed out after {} seconds",
                    config.connect_timeout_secs
                );
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
        }
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        info!(
            "Discovering MySQL streams for {}:{}",
            config.host, config.port
        );

        // Schema discovery for MySQL CDC requires querying INFORMATION_SCHEMA,
        // which would need a general-purpose MySQL query client.
        //
        // For CDC use cases, the catalog can be:
        // 1. Empty - CDC captures all table changes and infers schema from binlog events
        // 2. User-provided - specify tables in include_tables configuration
        //
        // The binlog replication protocol provides TABLE_MAP_EVENT with column types,
        // so the connector can work without upfront schema discovery.

        let mut catalog = Catalog::new();

        // If include_tables is configured, create streams for those tables
        for table_pattern in &config.include_tables {
            // Parse schema.table pattern
            let parts: Vec<&str> = table_pattern.splitn(2, '.').collect();
            let (namespace, name) = if parts.len() == 2 {
                (Some(parts[0].to_string()), parts[1].to_string())
            } else {
                (None, table_pattern.clone())
            };

            // Create minimal schema - actual schema comes from binlog events
            let json_schema = serde_json::json!({
                "type": "object",
                "additionalProperties": true
            });

            let mut stream = Stream::new(name, json_schema).sync_modes(vec![SyncMode::Incremental]);

            if let Some(ns) = namespace {
                stream = stream.namespace(ns);
            }

            catalog = catalog.add_stream(stream);
        }

        debug!("Discovered {} streams", catalog.streams.len());
        Ok(catalog)
    }

    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        use rivven_cdc::common::CdcSource;

        info!("Starting MySQL CDC stream");

        let cdc_config = Self::build_cdc_config(config);

        // Create channel for CDC events
        let (tx, event_rx) = tokio::sync::mpsc::channel::<rivven_cdc::common::CdcEvent>(1000);

        let mut cdc = rivven_cdc::mysql::MySqlCdc::new(cdc_config).with_event_channel(tx);

        // Start CDC in background
        cdc.start()
            .await
            .map_err(|e| ConnectorError::connection(format!("Failed to start MySQL CDC: {}", e)))?;

        // Get configured streams for filtering
        let configured_streams: std::collections::HashSet<String> = catalog
            .streams
            .iter()
            .map(|s| s.stream.name.clone())
            .collect();

        // Convert CDC events to SourceEvents
        let stream = tokio_stream::wrappers::ReceiverStream::new(event_rx)
            .filter_map(move |cdc_event| {
                use rivven_cdc::common::CdcOp;

                let stream_name = format!("{}.{}", cdc_event.schema, cdc_event.table);

                // Filter to configured streams (empty = all)
                if !configured_streams.is_empty() && !configured_streams.contains(&stream_name) {
                    return std::future::ready(None);
                }

                let source_event = match cdc_event.op {
                    CdcOp::Insert | CdcOp::Snapshot => SourceEvent::insert(
                        &stream_name,
                        cdc_event.after.clone().unwrap_or_default(),
                    ),
                    CdcOp::Update => SourceEvent::update(
                        &stream_name,
                        cdc_event.before.clone(),
                        cdc_event.after.clone().unwrap_or_default(),
                    ),
                    CdcOp::Delete => SourceEvent::delete(
                        &stream_name,
                        cdc_event.before.clone().unwrap_or_default(),
                    ),
                    // Tombstone events have null payload - emit as delete with empty data
                    CdcOp::Tombstone => SourceEvent::delete(&stream_name, serde_json::Value::Null),
                    // Schema and Truncate events are not row-level data events
                    CdcOp::Truncate | CdcOp::Schema => return std::future::ready(None),
                };

                // Add position for checkpointing (using timestamp as fallback)
                let event = source_event
                    .namespace(&cdc_event.schema)
                    .position(format!("{}", cdc_event.timestamp));

                std::future::ready(Some(Ok(event)))
            })
            .boxed();

        Ok(stream)
    }
}

// ============================================================================
// Factory and AnySource implementation for registry pattern
// ============================================================================

/// Factory for creating MySQL CDC source instances
pub struct MySqlCdcSourceFactory;

impl SourceFactory for MySqlCdcSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        MySqlCdcSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(MySqlCdcSourceWrapper(MySqlCdcSource::new()))
    }
}

/// Wrapper for type-erased source operations
#[allow(dead_code)] // Used by SourceFactory for dynamic dispatch
struct MySqlCdcSourceWrapper(MySqlCdcSource);

#[async_trait]
impl AnySource for MySqlCdcSourceWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: MySqlCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::config(format!("Invalid config: {}", e)))?;
        self.0.check(&typed_config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let typed_config: MySqlCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::config(format!("Invalid config: {}", e)))?;
        self.0.discover(&typed_config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let typed_config: MySqlCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::config(format!("Invalid config: {}", e)))?;
        self.0.read(&typed_config, catalog, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            host: localhost
            user: root
            password: secret
        "#;

        let config: MySqlCdcConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 3306);
        assert_eq!(config.user, "root");
        assert_eq!(config.server_id, 1001);
        assert_eq!(config.binlog_position, 4);
        assert!(!config.use_gtid);
    }

    #[test]
    fn test_config_with_gtid() {
        let yaml = r#"
            host: db.example.com
            port: 3307
            user: replicator
            password: repl_pass
            server_id: 2001
            use_gtid: true
            gtid_set: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
        "#;

        let config: MySqlCdcConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 3307);
        assert_eq!(config.server_id, 2001);
        assert!(config.use_gtid);
        assert!(!config.gtid_set.is_empty());
    }

    #[test]
    fn test_spec() {
        let spec = MySqlCdcSource::spec();
        assert_eq!(spec.connector_type, "mysql-cdc");
    }
}
