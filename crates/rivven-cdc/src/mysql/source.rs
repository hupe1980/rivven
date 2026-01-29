//! MySQL CDC source implementation
//!
//! Captures Change Data Capture events from MySQL/MariaDB using binlog replication.

#[cfg(feature = "mysql-tls")]
use crate::common::TlsConfig;
use crate::common::{CdcEvent, CdcOp, CdcSource, Result};
use anyhow::Context;
use async_trait::async_trait;
use mysql_async::prelude::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::decoder::{BinlogDecoder, BinlogEvent, ColumnValue, RowsEvent, TableMapEvent};
use super::protocol::MySqlBinlogClient;

/// MySQL CDC configuration
///
/// # Security Note
///
/// This struct implements a custom Debug that redacts the password field
/// to prevent accidental leakage to logs.
///
/// # TLS Support
///
/// TLS encryption is strongly recommended for production deployments.
/// Enable it via the `tls_config` field with `mysql-tls` feature.
#[derive(Clone)]
pub struct MySqlCdcConfig {
    /// MySQL host
    pub host: String,
    /// MySQL port (default: 3306)
    pub port: u16,
    /// Username for authentication
    pub user: String,
    /// Password for authentication
    pub password: Option<String>,
    /// Database to connect to (optional, for filtering)
    pub database: Option<String>,
    /// Server ID for replication (must be unique among all replicas)
    pub server_id: u32,
    /// Starting binlog filename (empty for current)
    pub binlog_filename: String,
    /// Starting binlog position (4 = start of file)
    pub binlog_position: u32,
    /// Use GTID-based replication
    pub use_gtid: bool,
    /// GTID set for GTID-based replication
    pub gtid_set: String,
    /// Tables to include (schema.table patterns, empty = all)
    pub include_tables: Vec<String>,
    /// Tables to exclude
    pub exclude_tables: Vec<String>,
    /// TLS configuration (requires `mysql-tls` feature)
    #[cfg(feature = "mysql-tls")]
    pub tls_config: Option<TlsConfig>,
}

impl std::fmt::Debug for MySqlCdcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut builder = f.debug_struct("MySqlCdcConfig");
        builder
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("database", &self.database)
            .field("server_id", &self.server_id)
            .field("binlog_filename", &self.binlog_filename)
            .field("binlog_position", &self.binlog_position)
            .field("use_gtid", &self.use_gtid)
            .field("gtid_set", &self.gtid_set)
            .field("include_tables", &self.include_tables)
            .field("exclude_tables", &self.exclude_tables);

        #[cfg(feature = "mysql-tls")]
        {
            let tls_enabled = self
                .tls_config
                .as_ref()
                .map(|c| c.is_enabled())
                .unwrap_or(false);
            builder.field("tls_enabled", &tls_enabled);
        }

        builder.finish()
    }
}

impl Default for MySqlCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: None,
            database: None,
            server_id: 1001, // Arbitrary default, should be unique
            binlog_filename: String::new(),
            binlog_position: 4,
            use_gtid: false,
            gtid_set: String::new(),
            include_tables: vec![],
            exclude_tables: vec![],
            #[cfg(feature = "mysql-tls")]
            tls_config: None,
        }
    }
}

impl MySqlCdcConfig {
    pub fn new(host: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            user: user.into(),
            ..Default::default()
        }
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn with_binlog_position(mut self, filename: impl Into<String>, position: u32) -> Self {
        self.binlog_filename = filename.into();
        self.binlog_position = position;
        self
    }

    pub fn with_gtid(mut self, gtid_set: impl Into<String>) -> Self {
        self.use_gtid = true;
        self.gtid_set = gtid_set.into();
        self
    }

    pub fn include_table(mut self, pattern: impl Into<String>) -> Self {
        self.include_tables.push(pattern.into());
        self
    }

    pub fn exclude_table(mut self, pattern: impl Into<String>) -> Self {
        self.exclude_tables.push(pattern.into());
        self
    }

    /// Set TLS configuration for encrypted connections
    ///
    /// Requires the `mysql-tls` feature.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rivven_cdc::common::{TlsConfig, SslMode};
    ///
    /// let config = MySqlCdcConfig::new("localhost", "root")
    ///     .with_password("secret")
    ///     .with_tls(TlsConfig::new(SslMode::Require));
    /// ```
    #[cfg(feature = "mysql-tls")]
    pub fn with_tls(mut self, tls_config: TlsConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
    }
}

/// Schema metadata cache for resolving column names
///
/// MySQL binlog events don't include column names, only types and values.
/// This cache stores column names queried from INFORMATION_SCHEMA.
#[derive(Default)]
pub struct SchemaCache {
    /// Map of (schema, table) -> column names in order
    tables: HashMap<(String, String), Vec<String>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Get column names for a table, or None if not cached
    pub fn get_columns(&self, schema: &str, table: &str) -> Option<Vec<String>> {
        self.tables
            .get(&(schema.to_string(), table.to_string()))
            .cloned()
    }

    /// Cache column names for a table
    pub fn set_columns(&mut self, schema: &str, table: &str, columns: Vec<String>) {
        self.tables
            .insert((schema.to_string(), table.to_string()), columns);
    }

    /// Check if a table is cached
    pub fn has_table(&self, schema: &str, table: &str) -> bool {
        self.tables
            .contains_key(&(schema.to_string(), table.to_string()))
    }
}

/// MySQL CDC source
pub struct MySqlCdc {
    config: MySqlCdcConfig,
    running: Arc<AtomicBool>,
    event_sender: Option<mpsc::Sender<CdcEvent>>,
    schema_cache: Arc<RwLock<SchemaCache>>,
}

impl MySqlCdc {
    pub fn new(config: MySqlCdcConfig) -> Self {
        Self {
            config,
            running: Arc::new(AtomicBool::new(false)),
            event_sender: None,
            schema_cache: Arc::new(RwLock::new(SchemaCache::new())),
        }
    }

    /// Set an event channel for receiving CDC events
    pub fn with_event_channel(mut self, sender: mpsc::Sender<CdcEvent>) -> Self {
        self.event_sender = Some(sender);
        self
    }

    /// Get the configuration
    pub fn config(&self) -> &MySqlCdcConfig {
        &self.config
    }

    /// Check if a table should be captured based on include/exclude filters
    #[allow(dead_code)]
    fn should_capture_table(&self, schema: &str, table: &str) -> bool {
        let full_name = format!("{}.{}", schema, table);

        // If exclude list is not empty, check for exclusion first
        for pattern in &self.config.exclude_tables {
            if pattern_matches(pattern, &full_name) {
                return false;
            }
        }

        // If include list is empty, include all non-excluded tables
        if self.config.include_tables.is_empty() {
            return true;
        }

        // Check if table matches any include pattern
        for pattern in &self.config.include_tables {
            if pattern_matches(pattern, &full_name) {
                return true;
            }
        }

        false
    }
}

#[async_trait]
impl CdcSource for MySqlCdc {
    async fn start(&mut self) -> Result<()> {
        if self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!(
            "Starting MySQL CDC from {}:{} (server_id={})",
            self.config.host, self.config.port, self.config.server_id
        );

        self.running.store(true, Ordering::SeqCst);

        let config = self.config.clone();
        let running = self.running.clone();
        let event_sender = self.event_sender.clone();
        let schema_cache = self.schema_cache.clone();

        tokio::spawn(async move {
            if let Err(e) =
                run_mysql_cdc_loop(config, running.clone(), event_sender, schema_cache).await
            {
                error!("MySQL CDC loop failed: {:?}", e);
                running.store(false, Ordering::SeqCst);
            }
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!("Stopping MySQL CDC");
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
}

/// Main CDC loop
async fn run_mysql_cdc_loop(
    config: MySqlCdcConfig,
    running: Arc<AtomicBool>,
    event_sender: Option<mpsc::Sender<CdcEvent>>,
    schema_cache: Arc<RwLock<SchemaCache>>,
) -> anyhow::Result<()> {
    // Create metadata connection URL for schema queries
    let metadata_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        config.user,
        config.password.as_deref().unwrap_or(""),
        config.host,
        config.port,
        config.database.as_deref().unwrap_or("mysql")
    );
    let metadata_pool = mysql_async::Pool::new(metadata_url.as_str());

    // Connect to MySQL with TLS if configured
    #[cfg(feature = "mysql-tls")]
    let mut client = {
        if let Some(ref tls_config) = config.tls_config {
            if tls_config.is_enabled() {
                info!("Connecting to MySQL with TLS (mode: {})", tls_config.mode);
                MySqlBinlogClient::connect_with_tls(
                    &config.host,
                    config.port,
                    &config.user,
                    config.password.as_deref(),
                    config.database.as_deref(),
                    tls_config,
                )
                .await
                .context("Failed to connect to MySQL with TLS")?
            } else {
                MySqlBinlogClient::connect(
                    &config.host,
                    config.port,
                    &config.user,
                    config.password.as_deref(),
                    config.database.as_deref(),
                )
                .await
                .context("Failed to connect to MySQL")?
            }
        } else {
            MySqlBinlogClient::connect(
                &config.host,
                config.port,
                &config.user,
                config.password.as_deref(),
                config.database.as_deref(),
            )
            .await
            .context("Failed to connect to MySQL")?
        }
    };

    #[cfg(not(feature = "mysql-tls"))]
    let mut client = MySqlBinlogClient::connect(
        &config.host,
        config.port,
        &config.user,
        config.password.as_deref(),
        config.database.as_deref(),
    )
    .await
    .context("Failed to connect to MySQL")?;

    info!(
        "Connected to MySQL {} (connection_id={}{})",
        client.server_version(),
        client.connection_id(),
        if client.is_tls() { ", TLS" } else { "" }
    );

    // Detect if server is MariaDB
    let is_mariadb = client.server_version().contains("MariaDB");

    // Set binlog checksum acknowledgment for MySQL 5.6.5+ and MariaDB 10+
    // This tells the master we can handle CRC32 checksums in binlog events
    // For MariaDB, this MUST be set BEFORE @mariadb_slave_capability
    if is_mariadb {
        // MariaDB requires explicit CRC32 setting
        if let Err(e) = client.query("SET @master_binlog_checksum = 'CRC32'").await {
            debug!("MariaDB binlog checksum set failed: {}", e);
        }
    } else if let Err(e) = client
        .query("SET @source_binlog_checksum = @@global.binlog_checksum")
        .await
    {
        // Try the older variable name for MySQL < 8.0.26
        if let Err(e2) = client
            .query("SET @master_binlog_checksum = @@global.binlog_checksum")
            .await
        {
            debug!(
                "Binlog checksum negotiation failed (may be MySQL < 5.6.5): {} / {}",
                e, e2
            );
        }
    }

    // Set MariaDB-specific slave capability flags
    // This is required for MariaDB 10.x+ to support checksums and other features
    if is_mariadb {
        // MariaDB slave capability bits:
        // Bit 0 (1) = Supports binlog checksums
        // Bit 1 (2) = Supports semi-sync replication
        // Bit 2 (4) = Start position is beyond the ignorable events
        // We set 1 | 4 = 5 to indicate checksum support
        if let Err(e) = client.query("SET @mariadb_slave_capability=5").await {
            debug!("MariaDB slave capability set failed: {}", e);
        }
    }

    // Get binlog position if not specified
    let (binlog_file, binlog_pos) = if config.binlog_filename.is_empty() {
        get_current_binlog_position(&mut client).await?
    } else {
        (config.binlog_filename.clone(), config.binlog_position)
    };

    info!(
        "Starting binlog replication from {}:{}",
        binlog_file, binlog_pos
    );

    // Register as replica
    client.register_slave(config.server_id).await?;

    // Start binlog dump
    let mut stream = if config.use_gtid && !config.gtid_set.is_empty() {
        client
            .binlog_dump_gtid(config.server_id, &config.gtid_set)
            .await?
    } else {
        client
            .binlog_dump(config.server_id, &binlog_file, binlog_pos)
            .await?
    };

    let mut decoder = BinlogDecoder::new();
    let mut event_buffer: Vec<CdcEvent> = Vec::new();
    let mut current_gtid: Option<String> = None;
    let mut current_binlog_file = binlog_file;
    let mut current_binlog_pos = binlog_pos;

    while running.load(Ordering::SeqCst) {
        let event_data = match stream.next_event().await {
            Ok(Some(data)) => data,
            Ok(None) => {
                // Connection closed
                warn!("Binlog stream closed");
                break;
            }
            Err(e) => {
                error!("Error reading binlog event: {:?}", e);
                // Try to reconnect
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let event = match decoder.decode(&event_data) {
            Ok(ev) => ev,
            Err(e) => {
                warn!("Failed to decode binlog event: {:?}", e);
                continue;
            }
        };

        match event {
            BinlogEvent::FormatDescription(fde) => {
                info!(
                    "Binlog format: version={}, server={}",
                    fde.binlog_version, fde.server_version
                );
            }

            BinlogEvent::Rotate(rotate) => {
                info!(
                    "Rotating to binlog file: {} at position {}",
                    rotate.next_binlog, rotate.position
                );
                current_binlog_file = rotate.next_binlog;
                current_binlog_pos = rotate.position as u32;
            }

            BinlogEvent::Gtid(gtid) => {
                current_gtid = Some(gtid.gtid_string());
                debug!("GTID: {}", current_gtid.as_ref().unwrap());
            }

            BinlogEvent::TableMap(table_map) => {
                debug!(
                    "Table map: {}.{} (table_id={})",
                    table_map.schema_name, table_map.table_name, table_map.table_id
                );

                // Query column names from INFORMATION_SCHEMA if not already cached
                let schema = table_map.schema_name.clone();
                let table = table_map.table_name.clone();

                if !schema_cache.read().unwrap().has_table(&schema, &table) {
                    let query = r#"
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
                        ORDER BY ORDINAL_POSITION
                    "#;

                    match metadata_pool.get_conn().await {
                        Ok(mut conn) => {
                            let result: std::result::Result<Vec<String>, _> =
                                conn.exec(query, (&schema, &table)).await;

                            match result {
                                Ok(columns) => {
                                    debug!(
                                        "Cached {} column names for {}.{}: {:?}",
                                        columns.len(),
                                        schema,
                                        table,
                                        columns
                                    );
                                    schema_cache
                                        .write()
                                        .unwrap()
                                        .set_columns(&schema, &table, columns);
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to query columns for {}.{}: {:?}",
                                        schema, table, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Failed to get metadata connection: {:?}", e);
                        }
                    }
                }
            }

            BinlogEvent::WriteRows(rows) => {
                debug!("Processing WriteRows for table_id={}", rows.table_id);
                if let Some(table_map) = decoder.get_table(rows.table_id) {
                    debug!(
                        "Found table: {}.{}",
                        table_map.schema_name, table_map.table_name
                    );
                    process_row_event(
                        CdcOp::Insert,
                        &rows,
                        table_map,
                        &config,
                        &current_gtid,
                        &current_binlog_file,
                        current_binlog_pos,
                        &mut event_buffer,
                        &schema_cache,
                    );
                    debug!("Event buffer size after processing: {}", event_buffer.len());
                } else {
                    warn!("No table map found for table_id={}", rows.table_id);
                }
            }

            BinlogEvent::UpdateRows(rows) => {
                if let Some(table_map) = decoder.get_table(rows.table_id) {
                    process_row_event(
                        CdcOp::Update,
                        &rows,
                        table_map,
                        &config,
                        &current_gtid,
                        &current_binlog_file,
                        current_binlog_pos,
                        &mut event_buffer,
                        &schema_cache,
                    );
                }
            }

            BinlogEvent::DeleteRows(rows) => {
                if let Some(table_map) = decoder.get_table(rows.table_id) {
                    process_row_event(
                        CdcOp::Delete,
                        &rows,
                        table_map,
                        &config,
                        &current_gtid,
                        &current_binlog_file,
                        current_binlog_pos,
                        &mut event_buffer,
                        &schema_cache,
                    );
                }
            }

            BinlogEvent::Xid(xid) => {
                debug!(
                    "Transaction commit: XID={}, buffer_len={}",
                    xid.xid,
                    event_buffer.len()
                );

                // Flush event buffer
                if !event_buffer.is_empty() {
                    // Send to event channel
                    if let Some(sender) = &event_sender {
                        debug!("Sending {} events to channel", event_buffer.len());
                        for event in event_buffer.drain(..) {
                            if sender.send(event).await.is_err() {
                                warn!("Event channel closed");
                                break;
                            }
                        }
                    } else {
                        debug!("No event sender, clearing buffer");
                        event_buffer.clear();
                    }
                }

                current_gtid = None;
            }

            BinlogEvent::Query(query) => {
                // Handle DDL statements
                let sql_upper = query.query.to_uppercase();
                if sql_upper.contains("CREATE TABLE")
                    || sql_upper.contains("ALTER TABLE")
                    || sql_upper.contains("DROP TABLE")
                    || sql_upper.contains("TRUNCATE")
                {
                    debug!("DDL: {}", query.query);

                    if sql_upper.contains("TRUNCATE") {
                        // Generate truncate event
                        // Would need to parse table name from query
                    }
                }
            }

            BinlogEvent::Heartbeat => {
                debug!("Heartbeat received");
            }

            BinlogEvent::Unknown(event_type) => {
                trace!("Unknown event type: {:?}", event_type);
            }
        }
    }

    info!("MySQL CDC loop stopped");
    Ok(())
}

/// Get current binlog position from MySQL
async fn get_current_binlog_position(
    _client: &mut MySqlBinlogClient,
) -> anyhow::Result<(String, u32)> {
    // Execute SHOW MASTER STATUS
    // For now, use a default
    // A real implementation would parse the result
    Ok(("mysql-bin.000001".to_string(), 4))
}

/// Process a rows event and convert to CDC events
#[allow(clippy::too_many_arguments)]
fn process_row_event(
    op: CdcOp,
    rows: &RowsEvent,
    table_map: &TableMapEvent,
    config: &MySqlCdcConfig,
    _gtid: &Option<String>,
    _binlog_file: &str,
    _binlog_pos: u32,
    buffer: &mut Vec<CdcEvent>,
    schema_cache: &Arc<RwLock<SchemaCache>>,
) {
    // Filter check
    // Note: MySqlCdc methods aren't available here, so we inline the check
    let full_name = format!("{}.{}", table_map.schema_name, table_map.table_name);

    // Check exclude patterns
    for pattern in &config.exclude_tables {
        if pattern_matches(pattern, &full_name) {
            return;
        }
    }

    // Check include patterns
    if !config.include_tables.is_empty() {
        let mut matched = false;
        for pattern in &config.include_tables {
            if pattern_matches(pattern, &full_name) {
                matched = true;
                break;
            }
        }
        if !matched {
            return;
        }
    }

    // Filter by database if configured
    if let Some(db) = &config.database {
        if !db.is_empty() && table_map.schema_name != *db {
            return;
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Get column names from cache
    let column_names = schema_cache
        .read()
        .unwrap()
        .get_columns(&table_map.schema_name, &table_map.table_name);

    for row in &rows.rows {
        let before = match op {
            CdcOp::Update | CdcOp::Delete => row
                .before
                .as_ref()
                .map(|cols| columns_to_json(cols, column_names.as_ref())),
            _ => None,
        };

        let after = match op {
            CdcOp::Insert | CdcOp::Update => row
                .after
                .as_ref()
                .map(|cols| columns_to_json(cols, column_names.as_ref())),
            _ => None,
        };

        let event = CdcEvent {
            source_type: "mysql".into(),
            database: table_map.schema_name.clone(),
            schema: table_map.schema_name.clone(), // MySQL uses database as schema
            table: table_map.table_name.clone(),
            op,
            before,
            after,
            timestamp,
            transaction: None,
        };

        buffer.push(event);
    }
}

/// Convert column values to JSON using actual column names
fn columns_to_json(
    columns: &[ColumnValue],
    column_names: Option<&Vec<String>>,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for (i, value) in columns.iter().enumerate() {
        // Use actual column name if available, otherwise fall back to generic name
        let col_name = column_names
            .and_then(|names| names.get(i).cloned())
            .unwrap_or_else(|| format!("col{}", i));

        let json_value = column_value_to_json(value);
        map.insert(col_name, json_value);
    }

    serde_json::Value::Object(map)
}

/// Convert a column value to JSON
fn column_value_to_json(value: &ColumnValue) -> serde_json::Value {
    match value {
        ColumnValue::Null => serde_json::Value::Null,
        ColumnValue::SignedInt(v) => serde_json::json!(*v),
        ColumnValue::UnsignedInt(v) => serde_json::json!(*v),
        ColumnValue::Float(v) => serde_json::json!(*v),
        ColumnValue::Double(v) => serde_json::json!(*v),
        ColumnValue::Decimal(v) => serde_json::json!(v),
        ColumnValue::String(v) => serde_json::json!(v),
        ColumnValue::Bytes(v) => {
            // Base64 encode bytes
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(v);
            serde_json::json!(encoded)
        }
        ColumnValue::Date { year, month, day } => {
            serde_json::json!(format!("{:04}-{:02}-{:02}", year, month, day))
        }
        ColumnValue::Time {
            hours,
            minutes,
            seconds,
            microseconds,
            negative,
        } => {
            let sign = if *negative { "-" } else { "" };
            if *microseconds > 0 {
                serde_json::json!(format!(
                    "{}{:02}:{:02}:{:02}.{:06}",
                    sign, hours, minutes, seconds, microseconds
                ))
            } else {
                serde_json::json!(format!(
                    "{}{:02}:{:02}:{:02}",
                    sign, hours, minutes, seconds
                ))
            }
        }
        ColumnValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        } => {
            if *microsecond > 0 {
                serde_json::json!(format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                    year, month, day, hour, minute, second, microsecond
                ))
            } else {
                serde_json::json!(format!(
                    "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                    year, month, day, hour, minute, second
                ))
            }
        }
        ColumnValue::Timestamp(v) => serde_json::json!(*v),
        ColumnValue::Year(v) => serde_json::json!(*v),
        ColumnValue::Json(v) => v.clone(),
        ColumnValue::Enum(v) => serde_json::json!(*v),
        ColumnValue::Set(v) => serde_json::json!(*v),
        ColumnValue::Bit(v) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(v);
            serde_json::json!(encoded)
        }
    }
}

/// Simple pattern matching for table filtering
/// Supports wildcards: * matches any characters
fn pattern_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" || pattern == "*.*" {
        return true;
    }

    if !pattern.contains('*') {
        return pattern == value;
    }

    // Convert glob pattern to simple matching
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.len() == 2 {
        // Pattern like "schema.*" or "*.table"
        let (prefix, suffix) = (parts[0], parts[1]);
        return value.starts_with(prefix) && value.ends_with(suffix);
    }

    // More complex patterns - use simple contains for now
    parts.iter().all(|part| value.contains(part))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matches() {
        assert!(pattern_matches("*", "test.users"));
        assert!(pattern_matches("*.*", "test.users"));
        assert!(pattern_matches("test.*", "test.users"));
        assert!(pattern_matches("test.*", "test.orders"));
        assert!(!pattern_matches("test.*", "prod.users"));
        assert!(pattern_matches("*.users", "test.users"));
        assert!(pattern_matches("*.users", "prod.users"));
        assert!(!pattern_matches("*.users", "test.orders"));
        assert!(pattern_matches("test.users", "test.users"));
        assert!(!pattern_matches("test.users", "test.orders"));
    }

    #[test]
    fn test_config_builder() {
        let config = MySqlCdcConfig::new("localhost", "admin")
            .with_password("secret")
            .with_port(3307)
            .with_database("mydb")
            .with_server_id(12345)
            .include_table("mydb.*")
            .exclude_table("mydb.temp_*");

        assert_eq!(config.host, "localhost");
        assert_eq!(config.user, "admin");
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.port, 3307);
        assert_eq!(config.database, Some("mydb".to_string()));
        assert_eq!(config.server_id, 12345);
        assert_eq!(config.include_tables, vec!["mydb.*"]);
        assert_eq!(config.exclude_tables, vec!["mydb.temp_*"]);
    }

    #[test]
    fn test_column_value_to_json() {
        assert_eq!(
            column_value_to_json(&ColumnValue::Null),
            serde_json::Value::Null
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::SignedInt(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::String("hello".to_string())),
            serde_json::json!("hello")
        );
        assert_eq!(
            column_value_to_json(&ColumnValue::Date {
                year: 2024,
                month: 1,
                day: 15
            }),
            serde_json::json!("2024-01-15")
        );
    }

    #[test]
    fn test_config_debug_redacts_password() {
        let config =
            MySqlCdcConfig::new("localhost", "admin").with_password("super_secret_password");

        let debug_output = format!("{:?}", config);

        // Should contain REDACTED for password
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]"
        );

        // Should NOT contain the actual password
        assert!(
            !debug_output.contains("super_secret_password"),
            "Debug output should not contain the password"
        );

        // Should still show non-sensitive fields
        assert!(
            debug_output.contains("localhost"),
            "Debug output should show host"
        );
        assert!(
            debug_output.contains("admin"),
            "Debug output should show user"
        );
    }

    #[test]
    fn test_config_debug_shows_none_for_missing_password() {
        let config = MySqlCdcConfig::new("localhost", "admin");

        let debug_output = format!("{:?}", config);

        // When password is None, should show None (not REDACTED)
        assert!(
            debug_output.contains("None"),
            "Debug output should show None for missing password"
        );
    }
}
