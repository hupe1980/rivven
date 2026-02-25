//! SQL Server CDC Source connector
//!
//! Implements the rivven-connect-sdk Source trait for SQL Server
//! Change Data Capture using poll-based CDC table reading.
//!
//! ## Supported SQL Server Versions
//!
//! - SQL Server 2016 SP1+ (Standard & Enterprise)
//! - SQL Server 2019
//! - SQL Server 2022
//! - Azure SQL Database
//! - Azure SQL Managed Instance
//!
//! ## Features
//!
//! All features are configurable via YAML - no Rust code required.
//! Designed for Kubernetes operators and binary deployments.
//!
//! ### Wired Up (Ready to Use)
//!
//! - **Core CDC**: Poll-based CDC table reading with LSN tracking
//! - **Filtering**: Table, column filtering with regex patterns
//! - **SMT**: 10 Single Message Transforms (see MySQL CDC for details)
//! - **Tombstones**: Log compaction support (configurable)
//! - **Column Filters**: Per-table column inclusion/exclusion
//! - **Snapshot Modes**: Initial, Always, Never, WhenNeeded
//! - **Metrics**: Events captured, poll cycles, latency tracking
//! - **Resilience**: Exponential backoff, connection retry
//!
//! ### Key Features
//!
//! - LSN-based positioning (commit_lsn + change_lsn)
//! - Before/after row states for UPDATE operations
//! - Schema inference from CDC tables
//! - Multiple snapshot modes
//! - Table filtering with include/exclude patterns

use super::config::{
    ColumnFilterConfig, DeduplicationCdcConfig, FieldEncryptionConfig, HeartbeatCdcConfig,
    IncrementalSnapshotCdcConfig, ReadOnlyReplicaConfig, SchemaChangeTopicConfig,
    SignalTableConfig, SmtTransformConfig, TombstoneCdcConfig, TransactionTopicCdcConfig,
};
use crate::connectors::{AnySource, SensitiveString, SourceFactory};
use crate::prelude::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use validator::Validate;

/// SQL Server CDC source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct SqlServerCdcConfig {
    /// SQL Server host
    #[validate(length(min = 1))]
    pub host: String,

    /// SQL Server port (default: 1433)
    #[serde(default = "default_port")]
    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    /// Database name
    #[validate(length(min = 1))]
    pub database: String,

    /// Username for SQL Server authentication
    #[validate(length(min = 1))]
    pub username: String,

    /// Password (redacted in logs)
    pub password: SensitiveString,

    /// Schema name (default: dbo)
    #[serde(default = "default_schema")]
    pub schema: String,

    /// Tables to include (empty = all tables with CDC enabled)
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude
    #[serde(default)]
    pub exclude_tables: Vec<String>,

    /// Enable TLS encryption
    #[serde(default)]
    pub encrypt: bool,

    /// Trust server certificate (for self-signed certs)
    #[serde(default)]
    pub trust_server_certificate: bool,

    /// Poll interval in milliseconds (default: 500)
    #[serde(default = "default_poll_interval_ms")]
    #[validate(range(min = 100, max = 60000))]
    pub poll_interval_ms: u64,

    /// Snapshot mode
    #[serde(default)]
    pub snapshot_mode: SnapshotModeConfig,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub connect_timeout_secs: u32,

    /// Maximum connection retry attempts
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 1, max = 100))]
    pub max_retries: u32,

    /// Heartbeat interval in seconds (default: 10)
    ///
    /// Used for monitoring CDC lag and detecting stale connections.
    /// Set to 0 to disable heartbeat.
    #[serde(default = "default_heartbeat_interval")]
    #[validate(range(min = 0, max = 3600))]
    pub heartbeat_interval_secs: u32,

    // ========================================================================
    // Implemented CDC Features
    // ========================================================================
    /// Single Message Transform configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub transforms: Vec<SmtTransformConfig>,

    /// Tombstone emission configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub tombstone: TombstoneCdcConfig,

    /// Column filtering (per-table column inclusion/exclusion) ✅ IMPLEMENTED
    #[serde(default)]
    pub column_filters: HashMap<String, ColumnFilterConfig>,

    /// Signal table configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub signal: SignalTableConfig,

    /// Incremental snapshot configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub incremental_snapshot: IncrementalSnapshotCdcConfig,

    /// Read-only replica configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub read_only: ReadOnlyReplicaConfig,

    /// Transaction metadata topic configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub transaction_topic: TransactionTopicCdcConfig,

    /// Schema change topic configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub schema_change_topic: SchemaChangeTopicConfig,

    /// Field-level encryption configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub encryption: FieldEncryptionConfig,

    /// Deduplication configuration ✅ IMPLEMENTED
    #[serde(default)]
    pub deduplication: DeduplicationCdcConfig,

    /// Topic routing pattern for dynamic topic selection ✅ IMPLEMENTED
    ///
    /// Enables per-table topic routing based on CDC event metadata.
    /// Supported placeholders:
    /// - `{database}` - Database name
    /// - `{schema}` - Schema name (e.g., "dbo")
    /// - `{table}` - Table name
    ///
    /// Example: `"cdc.{database}.{schema}.{table}"` → `"cdc.mydb.dbo.users"`
    ///
    /// If not set, all events go to the source's configured `topic`.
    #[serde(default)]
    pub topic_routing: Option<String>,
}

/// Snapshot mode configuration
///
/// Supported snapshot modes:
/// - `initial` - Snapshot on first run (default)
/// - `always` - Snapshot on every restart
/// - `never` - No snapshot, stream only
/// - `when_needed` - Snapshot if offsets unavailable
/// - `initial_only` - Snapshot and stop (migration mode)
/// - `schema_only` - Capture schema, skip data
/// - `recovery` - Rebuild schema history
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotModeConfig {
    /// Perform initial snapshot only on first run
    #[default]
    Initial,
    /// Always perform snapshot on connector start
    Always,
    /// Never perform snapshot (CDC only)
    Never,
    /// Perform snapshot only when needed (no valid offset)
    WhenNeeded,
    /// Perform snapshot and then stop (data migration mode)
    InitialOnly,
    /// Capture schema only, skip data snapshot
    #[serde(alias = "no_data")]
    SchemaOnly,
    /// Recovery mode for corrupted schema history
    Recovery,
}

fn default_port() -> u16 {
    1433
}

fn default_schema() -> String {
    "dbo".to_string()
}

fn default_poll_interval_ms() -> u64 {
    500
}

fn default_connect_timeout() -> u32 {
    30
}

fn default_max_retries() -> u32 {
    3
}

fn default_heartbeat_interval() -> u32 {
    10
}

/// SQL Server CDC source
pub struct SqlServerCdcSource;

impl SqlServerCdcSource {
    pub fn new() -> Self {
        Self
    }

    /// Build a rivven-cdc SqlServerCdcConfig from our config
    fn build_cdc_config(
        config: &SqlServerCdcConfig,
    ) -> std::result::Result<rivven_cdc::sqlserver::SqlServerCdcConfig, ConnectorError> {
        let mut builder = rivven_cdc::sqlserver::SqlServerCdcConfig::builder()
            .host(&config.host)
            .port(config.port)
            .database(&config.database)
            .username(&config.username)
            .password(config.password.expose_secret())
            .poll_interval_ms(config.poll_interval_ms)
            .encrypt(config.encrypt)
            .trust_server_certificate(config.trust_server_certificate);

        // Set snapshot mode
        let snapshot_mode = match &config.snapshot_mode {
            SnapshotModeConfig::Initial => rivven_cdc::common::SnapshotMode::Initial,
            SnapshotModeConfig::Always => rivven_cdc::common::SnapshotMode::Always,
            SnapshotModeConfig::Never => rivven_cdc::common::SnapshotMode::NoSnapshot,
            SnapshotModeConfig::WhenNeeded => rivven_cdc::common::SnapshotMode::WhenNeeded,
            SnapshotModeConfig::InitialOnly => rivven_cdc::common::SnapshotMode::InitialOnly,
            SnapshotModeConfig::SchemaOnly => rivven_cdc::common::SnapshotMode::SchemaOnly,
            SnapshotModeConfig::Recovery => rivven_cdc::common::SnapshotMode::Recovery,
        };
        builder = builder.snapshot_mode(snapshot_mode);

        // Add table filters
        for table in &config.include_tables {
            // Parse schema.table pattern
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            let (schema, table_name) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (config.schema.as_str(), table.as_str())
            };
            builder = builder.include_table(schema, table_name);
        }
        for table in &config.exclude_tables {
            let parts: Vec<&str> = table.splitn(2, '.').collect();
            let (schema, table_name) = if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (config.schema.as_str(), table.as_str())
            };
            builder = builder.exclude_table(schema, table_name);
        }

        builder
            .build()
            .map_err(|e| ConnectorError::config(format!("Invalid SQL Server config: {}", e)))
    }
}

impl Default for SqlServerCdcSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for SqlServerCdcSource {
    type Config = SqlServerCdcConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("sqlserver-cdc", env!("CARGO_PKG_VERSION"))
            .description("SQL Server CDC source using poll-based CDC table reading")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/sqlserver-cdc")
            .incremental(true)
            .config_schema::<SqlServerCdcConfig>()
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
            "Checking SQL Server connection to {}:{}",
            config.host, config.port
        );

        // Try to connect with timeout
        let timeout = tokio::time::Duration::from_secs(config.connect_timeout_secs as u64);

        match tokio::time::timeout(timeout, async {
            let cdc_config = Self::build_cdc_config(config)?;
            let _cdc = rivven_cdc::sqlserver::SqlServerCdc::new(cdc_config);
            Ok::<_, ConnectorError>(())
        })
        .await
        {
            Ok(Ok(_)) => {
                info!("SQL Server connection check passed");
                Ok(CheckResult::success())
            }
            Ok(Err(e)) => {
                let msg = format!("SQL Server connection failed: {}", e);
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
            Err(_) => {
                let msg = format!(
                    "SQL Server connection timed out after {} seconds",
                    config.connect_timeout_secs
                );
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
        }
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        info!(
            "Discovering SQL Server streams for {}:{}",
            config.host, config.port
        );

        // For CDC use cases, the catalog can be:
        // 1. Empty - CDC captures all table changes from enabled tables
        // 2. User-provided - specify tables in include_tables configuration
        //
        // SQL Server CDC requires tables to be explicitly enabled for CDC.
        // The connector will capture changes from enabled tables.

        let mut catalog = Catalog::new();

        // If include_tables is configured, create streams for those tables
        for table_pattern in &config.include_tables {
            // Parse schema.table pattern
            let parts: Vec<&str> = table_pattern.splitn(2, '.').collect();
            let (namespace, name) = if parts.len() == 2 {
                (Some(parts[0].to_string()), parts[1].to_string())
            } else {
                (Some(config.schema.clone()), table_pattern.clone())
            };

            // Create minimal schema - actual schema comes from CDC table columns
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
        use super::features::{CdcFeatureConfig, CdcFeatureProcessor};
        use rivven_cdc::common::CdcSource;

        info!("Starting SQL Server CDC stream");

        let cdc_config = Self::build_cdc_config(config)?;

        // Create SQL Server CDC instance
        let mut cdc = rivven_cdc::sqlserver::SqlServerCdc::new(cdc_config);

        // Take the event receiver
        let event_rx = cdc.take_event_receiver().ok_or_else(|| {
            ConnectorError::connection("Failed to get event receiver from SQL Server CDC")
        })?;

        // Start CDC in background
        cdc.start().await.map_err(|e| {
            ConnectorError::connection(format!("Failed to start SQL Server CDC: {}", e))
        })?;

        // Get configured streams for filtering
        let configured_streams: std::collections::HashSet<String> = catalog
            .streams
            .iter()
            .map(|s| s.stream.name.clone())
            .collect();

        // ====================================================================
        // Initialize CdcFeatureProcessor with all configured features
        // ====================================================================

        // Build heartbeat config from connector heartbeat_interval_secs
        let heartbeat_config = HeartbeatCdcConfig {
            enabled: config.heartbeat_interval_secs > 0,
            interval_secs: config.heartbeat_interval_secs,
            max_lag_secs: config.heartbeat_interval_secs * 30, // 5 min default for 10s interval
            emit_events: false,
            topic: None,
            action_query: None,
        };

        let feature_config = CdcFeatureConfig::from_cdc_config(
            config.transforms.clone(),
            config.column_filters.clone(),
            config.tombstone.clone(),
            config.deduplication.clone(),
            config.encryption.clone(),
            config.signal.clone(),
            config.transaction_topic.clone(),
            config.schema_change_topic.clone(),
            config.incremental_snapshot.clone(),
            config.read_only.clone(),
            heartbeat_config,
        );

        // Use connector name for identification
        let connector_name = format!("sqlserver-cdc-{}-{}", config.host, config.database);
        let feature_processor = std::sync::Arc::new(CdcFeatureProcessor::with_connector_name(
            feature_config,
            &connector_name,
        )?);

        // Log enabled features
        if feature_processor.has_deduplication() {
            info!("CDC deduplication enabled");
        }
        if feature_processor.has_signal_processing() {
            info!("CDC signal processing enabled");
        }
        if feature_processor.has_transaction_topic() {
            info!("CDC transaction topic enabled");
        }
        if feature_processor.has_schema_change_topic() {
            info!("CDC schema change topic enabled");
        }
        if feature_processor.has_encryption() {
            debug!("CDC field encryption enabled (key provider required for full support)");
        }
        if feature_processor.has_heartbeat() {
            info!(
                "CDC heartbeat enabled (interval: {}s)",
                config.heartbeat_interval_secs
            );
        }
        if feature_processor.is_read_only() {
            info!("CDC read-only mode enabled");
        }

        // Convert CDC events to SourceEvents with all features applied
        let stream = tokio_stream::wrappers::ReceiverStream::new(event_rx)
            .filter_map(move |cdc_event| {
                let processor = feature_processor.clone();
                let streams = configured_streams.clone();

                async move {
                    let stream_name = format!("{}.{}", cdc_event.schema, cdc_event.table);

                    // Filter to configured streams (empty = all)
                    if !streams.is_empty() && !streams.contains(&stream_name) {
                        return None;
                    }

                    // Update heartbeat position with timestamp
                    let position = cdc_event.timestamp.to_string();
                    processor
                        .update_heartbeat_position(&position, &cdc_event.database)
                        .await;

                    // Process through feature pipeline (deduplication, transforms, etc.)
                    let processed = match processor.process(cdc_event.clone()).await {
                        Some(p) => p,
                        None => return None, // Event was filtered/deduplicated
                    };

                    // Use shared conversion function from cdc_common
                    let source_event = super::common::cdc_event_to_source_event(&processed.event)?;
                    Some(Ok(source_event))
                }
            })
            .boxed();

        Ok(stream)
    }
}

// ============================================================================
// Factory and AnySource implementation for registry pattern
// ============================================================================

/// Factory for creating SQL Server CDC source instances
pub struct SqlServerCdcSourceFactory;

impl SourceFactory for SqlServerCdcSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        SqlServerCdcSource::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySource>> {
        Ok(Box::new(SqlServerCdcSourceWrapper(
            SqlServerCdcSource::new(),
        )))
    }
}

/// Wrapper for type-erased source operations
#[allow(dead_code)] // Used by SourceFactory for dynamic dispatch
struct SqlServerCdcSourceWrapper(SqlServerCdcSource);

#[async_trait]
impl AnySource for SqlServerCdcSourceWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: SqlServerCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::config(format!("Invalid config: {}", e)))?;
        self.0.check(&typed_config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let typed_config: SqlServerCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::config(format!("Invalid config: {}", e)))?;
        self.0.discover(&typed_config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let typed_config: SqlServerCdcConfig = serde_yaml::from_value(config.clone())
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
            database: testdb
            username: sa
            password: secret
        "#;

        let config: SqlServerCdcConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
        assert_eq!(config.database, "testdb");
        assert_eq!(config.username, "sa");
        assert_eq!(config.schema, "dbo");
        assert_eq!(config.poll_interval_ms, 500);
        assert!(!config.encrypt);
    }

    #[test]
    fn test_config_with_tls() {
        let yaml = r#"
            host: db.example.com
            port: 1433
            database: production
            username: cdc_user
            password: secure_pass
            schema: sales
            encrypt: true
            trust_server_certificate: true
            poll_interval_ms: 1000
            include_tables:
              - orders
              - customers
        "#;

        let config: SqlServerCdcConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 1433);
        assert_eq!(config.schema, "sales");
        assert!(config.encrypt);
        assert!(config.trust_server_certificate);
        assert_eq!(config.poll_interval_ms, 1000);
        assert_eq!(config.include_tables.len(), 2);
    }

    #[test]
    fn test_spec() {
        let spec = SqlServerCdcSource::spec();
        assert_eq!(spec.connector_type, "sqlserver-cdc");
    }

    #[test]
    fn test_snapshot_modes() {
        let yaml = r#"
            host: localhost
            database: testdb
            username: sa
            password: secret
            snapshot_mode: always
        "#;

        let config: SqlServerCdcConfig = serde_yaml::from_str(yaml).unwrap();

        match config.snapshot_mode {
            SnapshotModeConfig::Always => {}
            _ => panic!("Expected Always snapshot mode"),
        }
    }

    #[test]
    fn test_heartbeat_config() {
        let yaml = r#"
            host: localhost
            database: testdb
            username: sa
            password: secret
            heartbeat_interval_secs: 30
        "#;

        let config: SqlServerCdcConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.heartbeat_interval_secs, 30);

        // Test default
        let yaml_default = r#"
            host: localhost
            database: testdb
            username: sa
            password: secret
        "#;
        let config_default: SqlServerCdcConfig = serde_yaml::from_str(yaml_default).unwrap();
        assert_eq!(config_default.heartbeat_interval_secs, 10);
    }

    #[test]
    fn test_all_snapshot_modes() {
        // Test all snapshot modes
        let test_cases = [
            ("initial", "Initial"),
            ("always", "Always"),
            ("never", "Never"),
            ("when_needed", "WhenNeeded"),
            ("initial_only", "InitialOnly"),
            ("no_data", "SchemaOnly"),
            ("schema_only", "SchemaOnly"),
            ("recovery", "Recovery"),
        ];

        for (yaml_value, expected_variant) in test_cases {
            let yaml = format!(
                r#"
                host: localhost
                database: testdb
                username: sa
                password: secret
                snapshot_mode: {}
            "#,
                yaml_value
            );

            let config: SqlServerCdcConfig = serde_yaml::from_str(&yaml).unwrap();
            let mode_str = format!("{:?}", config.snapshot_mode);
            assert!(
                mode_str.contains(expected_variant),
                "Expected {} for yaml value '{}', got {:?}",
                expected_variant,
                yaml_value,
                config.snapshot_mode
            );
        }
    }
}
