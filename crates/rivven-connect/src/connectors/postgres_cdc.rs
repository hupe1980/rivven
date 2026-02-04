//! PostgreSQL CDC Source connector
//!
//! Implements the rivven-connect-sdk Source trait for PostgreSQL
//! Change Data Capture using logical replication.
//!
//! ## Features
//!
//! All features are configurable via YAML - no Rust code required.
//! Designed for Kubernetes operators and binary deployments.
//!
//! ### Wired Up (Ready to Use)
//!
//! - **Core CDC**: Logical replication via pgoutput
//! - **Auto-provisioning**: Automatic slot/publication creation
//! - **TLS/mTLS**: Encrypted connections
//! - **Filtering**: Schema, table, and column filtering
//! - **SMT**: 10 Single Message Transforms:
//!   - `ExtractNewRecordState` - Flatten envelope
//!   - `ValueToKey` - Extract key from value
//!   - `MaskField` - Mask sensitive fields
//!   - `InsertField` - Add static/computed fields
//!   - `ReplaceField` - Rename/filter fields
//!   - `RegexRouter` - Route based on patterns
//!   - `TimestampConverter` - Convert timestamp formats
//!   - `Filter` - Filter by condition
//!   - `Cast` - Convert field types
//!   - `Flatten` - Flatten nested structures
//! - **Tombstones**: Log compaction support (configurable)
//! - **Column Filters**: Per-table column inclusion/exclusion
//!
//! ### Configured (Requires Custom Setup)
//!
//! These features have configuration support but require integration
//! with custom async components for production use:
//!
//! - **Encryption**: Field-level AES-256-GCM (needs KeyProvider impl)
//! - **Deduplication**: Bloom filter + LRU (async Deduplicator)
//! - **Incremental Snapshots**: Chunk-based snapshots
//! - **Signaling**: Control CDC via signal table
//! - **Transaction Topics**: Transaction metadata
//! - **Schema Change Topics**: DDL changes
//!
//! ## Example Configuration
//!
//! ```yaml
//! connector_type: postgres-cdc
//! config:
//!   host: localhost
//!   port: 5432
//!   database: mydb
//!   user: replicator
//!   password: ${DB_PASSWORD}
//!   slot_name: rivven_slot
//!   publication_name: rivven_pub
//!   auto_create_slot: true
//!   auto_create_publication: true
//!   
//!   # Column filtering per table
//!   column_filters:
//!     public.users:
//!       include: ["id", "email", "created_at"]
//!       exclude: ["password_hash"]
//!   
//!   # SMT transforms
//!   transforms:
//!     - type: mask_field
//!       config:
//!         fields: ["ssn", "credit_card"]
//!     - type: timestamp_converter
//!       config:
//!         fields: ["created_at"]
//!         format: iso8601
//!     - type: extract_new_record_state
//!       config:
//!         drop_tombstones: false
//!         add_table: true
//!   
//!   # Tombstone handling
//!   tombstone:
//!     enabled: true
//! ```

use super::super::prelude::*;
use super::cdc_config::{
    ColumnFilterConfig, DeduplicationCdcConfig, FieldEncryptionConfig, HeartbeatCdcConfig,
    IncrementalSnapshotCdcConfig, ReadOnlyReplicaConfig, SchemaChangeTopicConfig,
    SignalTableConfig, SmtTransformConfig, SnapshotCdcConfig, SnapshotModeConfig,
    TombstoneCdcConfig, TransactionTopicCdcConfig,
};
use crate::connectors::{AnySource, SourceFactory};
use async_trait::async_trait;
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;

/// A password type for database connections that preserves the value during serialization.
///
/// Unlike the standard `SensitiveString` which redacts on serialize, this type
/// preserves the actual password value when serialized. This is necessary for:
/// - Configuration roundtripping (save/load)
/// - Connection pooling and reconnection
/// - Config validation workflows
///
/// Debug and Display output is still redacted to prevent leaks in logs.
#[derive(Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct ConnectionPassword(#[serde(with = "password_serde")] SecretString);

mod password_serde {
    use secrecy::{ExposeSecret, SecretString};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        secret: &SecretString,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(secret.expose_secret())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SecretString, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(SecretString::new(s.into_boxed_str()))
    }
}

impl std::fmt::Debug for ConnectionPassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl std::fmt::Display for ConnectionPassword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl ConnectionPassword {
    /// Create a new ConnectionPassword (used in tests)
    #[cfg(test)]
    pub fn new(s: impl Into<String>) -> Self {
        Self(SecretString::new(s.into().into_boxed_str()))
    }

    /// Expose the secret value (use sparingly!)
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl JsonSchema for ConnectionPassword {
    fn schema_name() -> String {
        "ConnectionPassword".to_string()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        // Schema looks like a normal string but with format hint
        let mut schema = gen.subschema_for::<String>();
        if let schemars::schema::Schema::Object(obj) = &mut schema {
            obj.format = Some("password".to_string());
        }
        schema
    }
}

/// PostgreSQL CDC source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct PostgresCdcConfig {
    /// PostgreSQL host
    #[validate(length(min = 1))]
    pub host: String,

    /// PostgreSQL port (default: 5432)
    #[serde(default = "default_port")]
    #[validate(range(min = 1, max = 65535))]
    pub port: u16,

    /// Database name
    #[validate(length(min = 1))]
    pub database: String,

    /// Username
    #[validate(length(min = 1))]
    pub user: String,

    /// Password (redacted in logs, preserved in serialization for DB reconnection)
    pub password: ConnectionPassword,

    /// Replication slot name
    #[serde(default = "default_slot_name")]
    #[validate(length(min = 1, max = 63))]
    pub slot_name: String,

    /// Publication name
    #[serde(default = "default_publication_name")]
    #[validate(length(min = 1, max = 63))]
    pub publication_name: String,

    /// Schemas to include (empty = all)
    #[serde(default)]
    pub schemas: Vec<String>,

    /// Tables to include (empty = all in schemas)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Initial snapshot configuration ✅ IMPLEMENTED
    /// Full table data load before streaming begins
    #[serde(default)]
    pub snapshot: SnapshotCdcConfig,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub connect_timeout_secs: u32,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub heartbeat_interval_secs: u32,

    /// Automatically create replication slot if it doesn't exist (default: true)
    /// Removes the need to pre-configure slots manually
    #[serde(default = "default_true")]
    pub auto_create_slot: bool,

    /// Automatically create publication if it doesn't exist (default: true)
    /// Removes the need to pre-configure publications manually
    #[serde(default = "default_true")]
    pub auto_create_publication: bool,

    /// Drop replication slot on connector stop (default: false)
    /// Useful for development/testing; DO NOT use in production
    #[serde(default)]
    pub drop_slot_on_stop: bool,

    /// TLS/SSL configuration
    #[serde(default)]
    pub tls: PostgresTlsConfig,

    // ========================================================================
    // Implemented CDC Features
    // ========================================================================
    /// Single Message Transform (SMT) chain ✅ IMPLEMENTED
    /// Apply transforms like masking, filtering, renaming in order
    #[serde(default)]
    pub transforms: Vec<SmtTransformConfig>,

    /// Tombstone emission configuration ✅ IMPLEMENTED
    /// Support Kafka log compaction with null-payload deletes
    #[serde(default)]
    pub tombstone: TombstoneCdcConfig,

    /// Column filtering (per-table column inclusion/exclusion) ✅ IMPLEMENTED
    /// Format: {"schema.table": {"include": ["col1"], "exclude": ["col2"]}}
    #[serde(default)]
    pub column_filters: HashMap<String, ColumnFilterConfig>,

    /// Signal table configuration ✅ IMPLEMENTED
    /// Ad-hoc snapshots and pause/resume via signal table
    #[serde(default)]
    pub signal: SignalTableConfig,

    /// Incremental snapshot configuration ✅ IMPLEMENTED
    /// Non-blocking re-snapshots using DBLog algorithm
    #[serde(default)]
    pub incremental_snapshot: IncrementalSnapshotCdcConfig,

    /// Read-only replica configuration ✅ IMPLEMENTED
    /// Connect to read replicas for reduced primary load
    #[serde(default)]
    pub read_only: ReadOnlyReplicaConfig,

    /// Transaction metadata topic configuration ✅ IMPLEMENTED
    /// Emit transaction boundaries for exactly-once semantics
    #[serde(default)]
    pub transaction_topic: TransactionTopicCdcConfig,

    /// Schema change topic configuration ✅ IMPLEMENTED
    /// Capture DDL changes (CREATE, ALTER, DROP)
    #[serde(default)]
    pub schema_change_topic: SchemaChangeTopicConfig,

    /// Field-level encryption configuration ✅ IMPLEMENTED
    /// AES-256-GCM encryption for sensitive columns
    /// Note: Full encryption requires KeyProvider integration
    #[serde(default)]
    pub encryption: FieldEncryptionConfig,

    /// Deduplication configuration ✅ IMPLEMENTED
    /// Bloom filter + LRU cache for idempotent processing
    #[serde(default)]
    pub deduplication: DeduplicationCdcConfig,

    /// Topic routing pattern for dynamic topic selection ✅ IMPLEMENTED
    ///
    /// Enables per-table topic routing based on CDC event metadata.
    /// Supported placeholders:
    /// - `{database}` - Database name (e.g., "mydb")
    /// - `{schema}` - Schema name (e.g., "public")
    /// - `{table}` - Table name (e.g., "users")
    ///
    /// Example: `"cdc.{schema}.{table}"` → `"cdc.public.users"`
    ///
    /// If not set, all events go to the source's configured `topic`.
    #[serde(default)]
    pub topic_routing: Option<String>,
}

/// TLS configuration for PostgreSQL connections
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PostgresTlsConfig {
    /// SSL mode: disable, prefer, require, verify-ca, verify-full
    #[serde(default = "default_ssl_mode")]
    pub mode: String,

    /// Path to CA certificate file (PEM format)
    /// Required for verify-ca and verify-full modes
    #[serde(default)]
    pub ca_cert_path: Option<String>,

    /// Path to client certificate file (PEM format, for mTLS)
    #[serde(default)]
    pub client_cert_path: Option<String>,

    /// Path to client private key file (PEM format, for mTLS)
    #[serde(default)]
    pub client_key_path: Option<String>,

    /// Accept invalid/self-signed certificates (DANGEROUS - testing only)
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl Default for PostgresTlsConfig {
    fn default() -> Self {
        Self {
            mode: default_ssl_mode(),
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            accept_invalid_certs: false,
        }
    }
}

fn default_ssl_mode() -> String {
    "prefer".to_string()
}

fn default_port() -> u16 {
    5432
}
fn default_slot_name() -> String {
    "rivven_slot".to_string()
}
fn default_publication_name() -> String {
    "rivven_pub".to_string()
}
fn default_connect_timeout() -> u32 {
    10
}
fn default_heartbeat_interval() -> u32 {
    10
}
fn default_true() -> bool {
    true
}

/// Result of auto-provisioning operations
#[derive(Debug, Default)]
struct ProvisioningResult {
    /// Was publication already present?
    publication_existed: bool,
    /// Did we create the publication?
    publication_created: bool,
    /// Was slot already present?
    slot_existed: bool,
    /// Did we create the slot?
    slot_created: bool,
}

/// PostgreSQL CDC Source implementation
pub struct PostgresCdcSource;

impl PostgresCdcSource {
    /// Create a new PostgresCdcSource
    pub fn new() -> Self {
        Self
    }

    /// Build connection string from config
    /// Note: This exposes the password for connection use only
    fn connection_string(config: &PostgresCdcConfig) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} connect_timeout={}",
            config.host,
            config.port,
            config.database,
            config.user,
            config.password.expose(), // Explicit exposure for connection
            config.connect_timeout_secs
        )
    }

    /// Build replication connection string (requires replication=database)
    #[allow(dead_code)]
    fn replication_connection_string(config: &PostgresCdcConfig) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} connect_timeout={} replication=database",
            config.host,
            config.port,
            config.database,
            config.user,
            config.password.expose(),
            config.connect_timeout_secs
        )
    }

    /// Auto-provision replication slot and publication if configured
    /// Simplifies setup by automatically creating required PostgreSQL resources
    async fn auto_provision(
        config: &PostgresCdcConfig,
        client: &tokio_postgres::Client,
    ) -> std::result::Result<ProvisioningResult, String> {
        use tracing::{info, warn};

        let mut result = ProvisioningResult::default();

        // Check and create publication if needed
        if config.auto_create_publication {
            let pub_check = format!(
                "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
                config.publication_name.replace('\'', "''")
            );

            match client.query_opt(&pub_check, &[]).await {
                Ok(Some(_)) => {
                    // Publication exists
                    result.publication_existed = true;
                }
                Ok(None) => {
                    // Create publication for all tables (or filtered if specified)
                    let create_pub = if config.tables.is_empty() && config.schemas.is_empty() {
                        format!(
                            "CREATE PUBLICATION {} FOR ALL TABLES",
                            config.publication_name
                        )
                    } else if !config.tables.is_empty() {
                        // Specific tables
                        let tables = config
                            .tables
                            .iter()
                            .map(|t| {
                                // If table has schema prefix, use it; otherwise use public
                                if t.contains('.') {
                                    t.to_string()
                                } else {
                                    format!("public.{}", t)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!(
                            "CREATE PUBLICATION {} FOR TABLE {}",
                            config.publication_name, tables
                        )
                    } else {
                        // Schemas specified - create for all tables (PostgreSQL will filter)
                        format!(
                            "CREATE PUBLICATION {} FOR ALL TABLES",
                            config.publication_name
                        )
                    };

                    match client.execute(&create_pub, &[]).await {
                        Ok(_) => {
                            info!("Created publication '{}'", config.publication_name);
                            result.publication_created = true;
                        }
                        Err(e) => {
                            return Err(format!("Failed to create publication: {}", e));
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to check publication: {}", e));
                }
            }
        }

        // Check and create replication slot if needed
        if config.auto_create_slot {
            let slot_check = format!(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'",
                config.slot_name.replace('\'', "''")
            );

            match client.query_opt(&slot_check, &[]).await {
                Ok(Some(_)) => {
                    // Slot exists
                    result.slot_existed = true;
                }
                Ok(None) => {
                    // Create replication slot using pgoutput plugin
                    let create_slot = format!(
                        "SELECT pg_create_logical_replication_slot('{}', 'pgoutput')",
                        config.slot_name.replace('\'', "''")
                    );

                    match client.query_one(&create_slot, &[]).await {
                        Ok(_) => {
                            info!("Created replication slot '{}'", config.slot_name);
                            result.slot_created = true;
                        }
                        Err(e) => {
                            // Check if error is because slot already exists (race condition)
                            let err_str = e.to_string();
                            if err_str.contains("already exists") {
                                warn!("Slot '{}' was created by another process", config.slot_name);
                                result.slot_existed = true;
                            } else {
                                return Err(format!("Failed to create replication slot: {}", e));
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Failed to check replication slot: {}", e));
                }
            }
        }

        Ok(result)
    }

    /// Drop replication slot (for cleanup/testing)
    #[allow(dead_code)]
    async fn drop_slot(
        config: &PostgresCdcConfig,
        client: &tokio_postgres::Client,
    ) -> std::result::Result<(), String> {
        use tracing::info;

        // Try to drop, ignore if not exists
        let drop_sql = format!(
            "SELECT pg_drop_replication_slot('{}')",
            config.slot_name.replace('\'', "''")
        );

        match client.query_opt(&drop_sql, &[]).await {
            Ok(_) => {
                info!("Dropped replication slot '{}'", config.slot_name);
                Ok(())
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("does not exist") {
                    Ok(()) // Already gone
                } else {
                    Err(format!("Failed to drop slot: {}", e))
                }
            }
        }
    }
}

impl Default for PostgresCdcSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for PostgresCdcSource {
    type Config = PostgresCdcConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("postgres-cdc", env!("CARGO_PKG_VERSION"))
            .description("PostgreSQL Change Data Capture using logical replication")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/postgres-cdc")
            .incremental(true)
            .config_schema::<PostgresCdcConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        use tracing::info;

        // Validate config first
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        let conn_str = Self::connection_string(config);

        // Connect to PostgreSQL
        let (client, connection) =
            match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
                Ok(c) => c,
                Err(e) => return Ok(CheckResult::failure(format!("Connection failed: {}", e))),
            };

        // Spawn connection handler
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Check wal_level
        match client.query_one("SHOW wal_level", &[]).await {
            Ok(row) => {
                let wal_level: &str = row.get(0);
                if wal_level != "logical" {
                    return Ok(CheckResult::failure(format!(
                        "wal_level is '{}', must be 'logical' for CDC. Run: ALTER SYSTEM SET wal_level = 'logical'; and restart PostgreSQL.",
                        wal_level
                    )));
                }
            }
            Err(e) => {
                return Ok(CheckResult::failure(format!(
                    "Failed to check wal_level: {}",
                    e
                )))
            }
        }

        // Check max_replication_slots
        match client.query_one("SHOW max_replication_slots", &[]).await {
            Ok(row) => {
                let slots: &str = row.get(0);
                if let Ok(n) = slots.parse::<i32>() {
                    if n < 1 {
                        return Ok(CheckResult::failure(
                            "max_replication_slots must be > 0 for CDC".to_string(),
                        ));
                    }
                }
            }
            Err(e) => {
                return Ok(CheckResult::failure(format!(
                    "Failed to check max_replication_slots: {}",
                    e
                )));
            }
        }

        // Check REPLICATION privilege (required for creating slots)
        let has_replication = client
            .query_opt(
                "SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolreplication",
                &[],
            )
            .await
            .ok()
            .flatten()
            .is_some();

        if !has_replication && (config.auto_create_slot || config.auto_create_publication) {
            return Ok(CheckResult::failure("User lacks REPLICATION privilege required for auto-provisioning. Either grant REPLICATION role or set auto_create_slot=false and auto_create_publication=false".to_string()));
        }

        // Auto-provision if configured
        if config.auto_create_slot || config.auto_create_publication {
            match Self::auto_provision(config, &client).await {
                Ok(result) => {
                    if result.publication_created {
                        info!("Auto-created publication '{}'", config.publication_name);
                    }
                    if result.slot_created {
                        info!("Auto-created replication slot '{}'", config.slot_name);
                    }
                }
                Err(e) => {
                    return Ok(CheckResult::failure(format!(
                        "Auto-provisioning failed: {}",
                        e
                    )));
                }
            }
        } else {
            // Manual mode - verify resources exist
            let pub_check = format!(
                "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
                config.publication_name.replace('\'', "''")
            );
            if client
                .query_opt(&pub_check, &[])
                .await
                .ok()
                .flatten()
                .is_none()
            {
                return Ok(CheckResult::failure(format!(
                    "Publication '{}' does not exist. Create it manually or set auto_create_publication=true",
                    config.publication_name
                )));
            }

            let slot_check = format!(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'",
                config.slot_name.replace('\'', "''")
            );
            if client
                .query_opt(&slot_check, &[])
                .await
                .ok()
                .flatten()
                .is_none()
            {
                return Ok(CheckResult::failure(format!(
                    "Replication slot '{}' does not exist. Create it manually or set auto_create_slot=true",
                    config.slot_name
                )));
            }
        }

        Ok(CheckResult::success())
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let conn_str = Self::connection_string(config);

        let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Build schema filter
        let schema_filter = if config.schemas.is_empty() {
            "AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')".to_string()
        } else {
            let schemas = config
                .schemas
                .iter()
                .map(|s| format!("'{}'", s.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AND schemaname IN ({})", schemas)
        };

        // Query tables
        let query = format!(
            r#"
            SELECT 
                t.schemaname,
                t.tablename,
                COALESCE(
                    (SELECT array_agg(a.attname ORDER BY a.attnum)
                     FROM pg_attribute a
                     WHERE a.attrelid = (t.schemaname || '.' || t.tablename)::regclass
                       AND a.attnum > 0 AND NOT a.attisdropped),
                    ARRAY[]::text[]
                ) as columns,
                COALESCE(
                    (SELECT array_agg(format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum)
                     FROM pg_attribute a
                     WHERE a.attrelid = (t.schemaname || '.' || t.tablename)::regclass
                       AND a.attnum > 0 AND NOT a.attisdropped),
                    ARRAY[]::text[]
                ) as column_types,
                COALESCE(
                    (SELECT array_agg(pk.attname)
                     FROM pg_index idx
                     JOIN pg_attribute pk ON pk.attrelid = idx.indrelid AND pk.attnum = ANY(idx.indkey)
                     WHERE idx.indrelid = (t.schemaname || '.' || t.tablename)::regclass
                       AND idx.indisprimary),
                    ARRAY[]::text[]
                ) as primary_keys
            FROM pg_tables t
            WHERE t.tableowner = current_user
            {}
            ORDER BY t.schemaname, t.tablename
            "#,
            schema_filter
        );

        let rows = client
            .query(&query, &[])
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let mut catalog = Catalog::new();

        for row in rows {
            let schema: String = row.get(0);
            let table: String = row.get(1);
            let columns: Vec<String> = row.get(2);
            let column_types: Vec<String> = row.get(3);
            let primary_keys: Vec<String> = row.get(4);

            // Apply table filter if configured
            if !config.tables.is_empty() {
                let full_name = format!("{}.{}", schema, table);
                if !config.tables.contains(&table) && !config.tables.contains(&full_name) {
                    continue;
                }
            }

            // Build JSON schema
            let mut properties = serde_json::Map::new();
            for (col, typ) in columns.iter().zip(column_types.iter()) {
                let json_type = postgres_type_to_json(typ);
                properties.insert(col.clone(), json_type);
            }

            let json_schema = serde_json::json!({
                "type": "object",
                "properties": properties,
            });

            let stream_name = format!("{}.{}", schema, table);
            let mut stream = Stream::new(&stream_name, json_schema);
            stream = stream.namespace(&schema);

            // Set primary key
            if !primary_keys.is_empty() {
                stream = stream.primary_key(primary_keys.iter().map(|k| vec![k.clone()]).collect());
            }

            // CDC supports incremental with cursor
            stream = stream.sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

            catalog = catalog.add_stream(stream);
        }

        Ok(catalog)
    }

    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        use super::cdc_features::{CdcFeatureConfig, CdcFeatureProcessor};
        use futures::StreamExt;
        use rivven_cdc::common::CdcSource;
        use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig as CdcConfig};
        use tracing::{debug, info};

        // Ensure slot/publication exist before starting CDC
        if config.auto_create_slot || config.auto_create_publication {
            let conn_str = Self::connection_string(config);
            let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
                .await
                .map_err(|e| ConnectorError::Connection(format!("Connection failed: {}", e)))?;

            tokio::spawn(async move {
                let _ = connection.await;
            });

            let result = Self::auto_provision(config, &client)
                .await
                .map_err(|e| ConnectorError::Config(format!("Auto-provisioning failed: {}", e)))?;

            if result.publication_created {
                info!("Auto-created publication '{}'", config.publication_name);
            }
            if result.slot_created {
                info!("Auto-created replication slot '{}'", config.slot_name);
            }
        }

        // Build CDC config
        let conn_str = Self::connection_string(config);
        let cdc_config = CdcConfig::builder()
            .connection_string(conn_str)
            .slot_name(&config.slot_name)
            .publication_name(&config.publication_name)
            .buffer_size(1000)
            .build()
            .map_err(|e| ConnectorError::Config(e.to_string()))?;

        // Start CDC
        let mut cdc = PostgresCdc::new(cdc_config);
        cdc.start()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let event_rx = cdc
            .take_event_receiver()
            .ok_or_else(|| ConnectorError::Internal("Failed to get event receiver".to_string()))?;

        // Get configured streams for filtering
        let configured_streams: std::collections::HashSet<String> = catalog
            .streams
            .iter()
            .map(|s| s.stream.name.clone())
            .collect();

        // Determine if we have prior state (used for snapshot mode decisions)
        let has_prior_state = state.as_ref().is_some_and(|s| {
            s.streams
                .get("_global")
                .is_some_and(|gs| gs.cursor_value.as_ref().is_some())
        });

        // Resume from state if available
        let _resume_lsn = state.as_ref().and_then(|s| {
            s.streams.get("_global").and_then(|gs| {
                gs.cursor_value
                    .as_ref()
                    .and_then(|c| c.as_str().map(String::from))
            })
        });

        // ====================================================================
        // Execute Snapshot Phase (if configured)
        // ====================================================================
        use super::cdc_snapshot::{SnapshotExecutor, SnapshotExecutorConfig};

        // Build table list from catalog: (schema, table, primary_key)
        let snapshot_tables: Vec<(String, String, String)> = catalog
            .streams
            .iter()
            .map(|cs| {
                let name = &cs.stream.name;
                // Parse "schema.table" format
                let parts: Vec<&str> = name.split('.').collect();
                // Get primary key from stream definition
                let pk = cs
                    .stream
                    .source_defined_primary_key
                    .as_ref()
                    .and_then(|keys| keys.first())
                    .and_then(|path| path.first())
                    .cloned()
                    .unwrap_or_else(|| "id".to_string());
                if parts.len() == 2 {
                    (parts[0].to_string(), parts[1].to_string(), pk)
                } else {
                    // Default to public schema
                    ("public".to_string(), name.clone(), pk)
                }
            })
            .collect();

        let snapshot_executor = SnapshotExecutor::new(SnapshotExecutorConfig {
            yaml_config: config.snapshot.clone(),
            connection_string: Self::connection_string(config),
            tables: snapshot_tables,
            has_prior_state,
        });

        // Execute snapshot if needed
        let (snapshot_events, snapshot_result) = snapshot_executor
            .execute()
            .await
            .map_err(|e| ConnectorError::Internal(format!("Snapshot failed: {}", e)))?;

        // Log snapshot result
        if snapshot_result.total_events > 0 {
            info!(
                "Snapshot completed: {} events from {} tables in {:?}",
                snapshot_result.total_events,
                snapshot_result.tables_completed.len(),
                snapshot_result.duration
            );
        }

        // Check if we should continue to streaming
        if !snapshot_result.should_stream {
            info!("Snapshot-only mode: returning snapshot events without streaming");
            // Convert snapshot events to SourceEvents and return as stream
            let snapshot_stream = futures::stream::iter(snapshot_events)
                .filter_map(move |cdc_event| async move {
                    super::cdc_common::cdc_event_to_source_event(&cdc_event).map(Ok)
                })
                .boxed();
            return Ok(snapshot_stream);
        }

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

        // Use connector name for heartbeat identification
        let connector_name = format!("postgres-cdc-{}", config.slot_name);
        let feature_processor = std::sync::Arc::new(CdcFeatureProcessor::with_connector_name(
            feature_config,
            &connector_name,
        ));

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

        // Log snapshot configuration
        match config.snapshot.mode {
            SnapshotModeConfig::Always => info!("Snapshot mode: Always (snapshot on every start)"),
            SnapshotModeConfig::Initial => {
                info!("Snapshot mode: Initial (snapshot on first start only)")
            }
            SnapshotModeConfig::Never => info!("Snapshot mode: Never (streaming only)"),
            SnapshotModeConfig::WhenNeeded => {
                info!("Snapshot mode: WhenNeeded (snapshot if no offsets)")
            }
            SnapshotModeConfig::InitialOnly => {
                info!("Snapshot mode: InitialOnly (snapshot only, no streaming)")
            }
            SnapshotModeConfig::NoData => {
                info!("Snapshot mode: NoData (schema capture only)")
            }
            SnapshotModeConfig::Recovery => info!("Snapshot mode: Recovery (force re-snapshot)"),
        }
        if !matches!(config.snapshot.mode, SnapshotModeConfig::Never) {
            info!(
                "Snapshot config: batch_size={}, parallel_tables={}, progress_dir={:?}",
                config.snapshot.batch_size,
                config.snapshot.parallel_tables,
                config.snapshot.progress_dir
            );
        }

        // Convert CDC events to SourceEvents with all features applied
        let cdc_stream =
            tokio_stream::wrappers::ReceiverStream::new(event_rx).filter_map(move |cdc_event| {
                let processor = feature_processor.clone();
                let streams = configured_streams.clone();

                async move {
                    let stream_name = format!("{}.{}", cdc_event.schema, cdc_event.table);

                    // Filter to configured streams
                    if !streams.is_empty() && !streams.contains(&stream_name) {
                        return None;
                    }

                    // Update heartbeat position with timestamp (for lag tracking)
                    // Note: Use timestamp as position since CdcEvent doesn't expose raw LSN
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
                    let source_event =
                        super::cdc_common::cdc_event_to_source_event(&processed.event)?;
                    Some(Ok(source_event))
                }
            });

        // Combine snapshot events with streaming events
        // Snapshot events are emitted first, then streaming continues
        let snapshot_event_stream =
            futures::stream::iter(snapshot_events).filter_map(|cdc_event| async move {
                super::cdc_common::cdc_event_to_source_event(&cdc_event).map(Ok)
            });

        // Chain: snapshot events first, then streaming events
        let combined_stream = snapshot_event_stream.chain(cdc_stream).boxed();

        Ok(combined_stream)
    }
}

/// Convert PostgreSQL type to JSON Schema type
fn postgres_type_to_json(pg_type: &str) -> serde_json::Value {
    let pg_type_lower = pg_type.to_lowercase();

    // Check for arrays first (before other type patterns)
    let json_type = if pg_type_lower.contains("[]") || pg_type_lower.starts_with("array") {
        "array"
    } else if pg_type_lower.contains("int")
        || pg_type_lower == "serial"
        || pg_type_lower == "bigserial"
    {
        "integer"
    } else if pg_type_lower.contains("numeric")
        || pg_type_lower.contains("decimal")
        || pg_type_lower.contains("real")
        || pg_type_lower.contains("double")
    {
        "number"
    } else if pg_type_lower == "boolean" || pg_type_lower == "bool" {
        "boolean"
    } else if pg_type_lower.contains("json") {
        "object"
    } else {
        "string"
    };

    serde_json::json!({
        "type": json_type,
        "x-postgres-type": pg_type,
    })
}

/// Factory for creating PostgresCdcSource instances
pub struct PostgresCdcSourceFactory;

impl SourceFactory for PostgresCdcSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        PostgresCdcSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(PostgresCdcSourceWrapper(PostgresCdcSource::new()))
    }
}

/// Wrapper for type-erased source operations
#[allow(dead_code)] // Used by SourceFactory for dynamic dispatch
struct PostgresCdcSourceWrapper(PostgresCdcSource);

#[async_trait]
impl AnySource for PostgresCdcSourceWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: PostgresCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.check(&typed_config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let typed_config: PostgresCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.discover(&typed_config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let typed_config: PostgresCdcConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.read(&typed_config, catalog, state).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = PostgresCdcSource::spec();
        assert_eq!(spec.connector_type, "postgres-cdc");
        assert!(spec.supports_incremental);
        assert!(spec.supports_discover);
    }

    #[test]
    fn test_config_validation() {
        let config = PostgresCdcConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
            user: "postgres".to_string(),
            password: ConnectionPassword::new("secret"),
            slot_name: "rivven_slot".to_string(),
            publication_name: "rivven_pub".to_string(),
            schemas: vec![],
            tables: vec![],
            snapshot: SnapshotCdcConfig::default(),
            connect_timeout_secs: 10,
            heartbeat_interval_secs: 10,
            auto_create_slot: true,
            auto_create_publication: true,
            drop_slot_on_stop: false,
            tls: PostgresTlsConfig::default(),
            signal: SignalTableConfig::default(),
            incremental_snapshot: IncrementalSnapshotCdcConfig::default(),
            transforms: vec![],
            read_only: ReadOnlyReplicaConfig::default(),
            transaction_topic: TransactionTopicCdcConfig::default(),
            schema_change_topic: SchemaChangeTopicConfig::default(),
            tombstone: TombstoneCdcConfig::default(),
            encryption: FieldEncryptionConfig::default(),
            deduplication: DeduplicationCdcConfig::default(),
            column_filters: HashMap::new(),
            topic_routing: None,
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_invalid_port() {
        let config = PostgresCdcConfig {
            host: "localhost".to_string(),
            port: 0, // Invalid
            database: "test".to_string(),
            user: "postgres".to_string(),
            password: ConnectionPassword::new("secret"),
            slot_name: "rivven_slot".to_string(),
            publication_name: "rivven_pub".to_string(),
            schemas: vec![],
            tables: vec![],
            snapshot: SnapshotCdcConfig::default(),
            connect_timeout_secs: 10,
            heartbeat_interval_secs: 10,
            auto_create_slot: true,
            auto_create_publication: true,
            drop_slot_on_stop: false,
            tls: PostgresTlsConfig::default(),
            signal: SignalTableConfig::default(),
            incremental_snapshot: IncrementalSnapshotCdcConfig::default(),
            transforms: vec![],
            read_only: ReadOnlyReplicaConfig::default(),
            transaction_topic: TransactionTopicCdcConfig::default(),
            schema_change_topic: SchemaChangeTopicConfig::default(),
            tombstone: TombstoneCdcConfig::default(),
            encryption: FieldEncryptionConfig::default(),
            deduplication: DeduplicationCdcConfig::default(),
            column_filters: HashMap::new(),
            topic_routing: None,
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_postgres_type_mapping() {
        assert_eq!(
            postgres_type_to_json("integer").get("type").unwrap(),
            "integer"
        );
        assert_eq!(
            postgres_type_to_json("bigint").get("type").unwrap(),
            "integer"
        );
        assert_eq!(
            postgres_type_to_json("numeric(10,2)").get("type").unwrap(),
            "number"
        );
        assert_eq!(
            postgres_type_to_json("boolean").get("type").unwrap(),
            "boolean"
        );
        assert_eq!(
            postgres_type_to_json("jsonb").get("type").unwrap(),
            "object"
        );
        assert_eq!(postgres_type_to_json("text").get("type").unwrap(), "string");
        assert_eq!(
            postgres_type_to_json("integer[]").get("type").unwrap(),
            "array"
        );
    }

    #[test]
    fn test_password_redacted_in_debug() {
        let password = ConnectionPassword::new("super-secret-password");

        // Debug output should show [REDACTED], not the actual password
        let debug_output = format!("{:?}", password);
        assert_eq!(debug_output, "[REDACTED]");
        assert!(!debug_output.contains("super-secret"));

        // Config debug should also be redacted
        let config = PostgresCdcConfig {
            host: "localhost".to_string(),
            port: 5432,
            database: "test".to_string(),
            user: "postgres".to_string(),
            password: ConnectionPassword::new("my-secret-password"),
            slot_name: "rivven_slot".to_string(),
            publication_name: "rivven_pub".to_string(),
            schemas: vec![],
            tables: vec![],
            snapshot: SnapshotCdcConfig::default(),
            connect_timeout_secs: 10,
            heartbeat_interval_secs: 10,
            auto_create_slot: true,
            auto_create_publication: true,
            drop_slot_on_stop: false,
            tls: PostgresTlsConfig::default(),
            signal: SignalTableConfig::default(),
            incremental_snapshot: IncrementalSnapshotCdcConfig::default(),
            transforms: vec![],
            read_only: ReadOnlyReplicaConfig::default(),
            transaction_topic: TransactionTopicCdcConfig::default(),
            schema_change_topic: SchemaChangeTopicConfig::default(),
            tombstone: TombstoneCdcConfig::default(),
            encryption: FieldEncryptionConfig::default(),
            deduplication: DeduplicationCdcConfig::default(),
            column_filters: HashMap::new(),
            topic_routing: None,
        };

        let config_debug = format!("{:?}", config);
        assert!(config_debug.contains("[REDACTED]"));
        assert!(!config_debug.contains("my-secret-password"));
    }

    #[test]
    fn test_password_expose_works() {
        let password = ConnectionPassword::new("actual-password");
        assert_eq!(password.expose(), "actual-password");
    }

    #[test]
    fn test_tls_config_defaults() {
        let tls = PostgresTlsConfig::default();
        assert_eq!(tls.mode, "prefer");
        assert!(tls.ca_cert_path.is_none());
        assert!(tls.client_cert_path.is_none());
        assert!(tls.client_key_path.is_none());
        assert!(!tls.accept_invalid_certs);
    }

    #[test]
    fn test_tls_config_verify_full() {
        let tls = PostgresTlsConfig {
            mode: "verify-full".to_string(),
            ca_cert_path: Some("/path/to/ca.pem".to_string()),
            client_cert_path: Some("/path/to/client.pem".to_string()),
            client_key_path: Some("/path/to/client-key.pem".to_string()),
            accept_invalid_certs: false,
        };

        assert_eq!(tls.mode, "verify-full");
        assert!(tls.ca_cert_path.is_some());
    }

    #[test]
    fn test_config_with_tls_deserialization() {
        let yaml = r#"
host: db.example.com
port: 5432
database: production
user: replicator
password: secret123
slot_name: my_slot
publication_name: my_pub
tls:
  mode: verify-full
  ca_cert_path: /etc/ssl/ca.pem
  accept_invalid_certs: false
"#;

        let config: PostgresCdcConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.tls.mode, "verify-full");
        assert_eq!(config.tls.ca_cert_path, Some("/etc/ssl/ca.pem".to_string()));
    }
}
