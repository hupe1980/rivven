//! PostgreSQL CDC source implementation
//!
//! Implements the CdcSource trait for PostgreSQL logical replication.

#[cfg(feature = "postgres-tls")]
use crate::common::TlsConfig;
use crate::common::{
    CdcConfig, CdcError, CdcEvent, CdcSource, Result, SignalConfig, SignalProcessor,
};
use crate::postgres::protocol::{
    PgOutputDecoder, RelationBody, ReplicationMessage, SecureReplicationClient, Tuple, TupleData,
};
use async_trait::async_trait;
use bytes::Buf;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
use url::Url;

/// PostgreSQL CDC configuration
///
/// # Security Note
///
/// This struct implements a custom Debug that redacts credentials from
/// the connection string to prevent accidental leakage to logs.
///
/// # TLS Support
///
/// TLS encryption is strongly recommended for production deployments.
/// Enable it via the `tls_config` field with `postgres-tls` feature.
///
/// # Signal Table Support
///
/// Enable signal table support with `signal_config`. When configured,
/// the CDC connector will detect signals from the specified table and
/// process them (execute-snapshot, pause/resume, etc.).
#[derive(Clone)]
pub struct PostgresCdcConfig {
    /// PostgreSQL connection string
    pub connection_string: String,
    /// Replication slot name
    pub slot_name: String,
    /// Publication name
    pub publication_name: String,
    /// Start LSN (0 for beginning)
    pub start_lsn: u64,
    /// Event buffer size
    pub buffer_size: usize,
    /// TLS configuration (requires `postgres-tls` feature)
    #[cfg(feature = "postgres-tls")]
    pub tls_config: Option<TlsConfig>,
    /// Signal configuration for runtime control
    pub signal_config: Option<SignalConfig>,
}

impl std::fmt::Debug for PostgresCdcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact credentials from connection string
        let redacted_conn = redact_connection_string(&self.connection_string);
        let mut builder = f.debug_struct("PostgresCdcConfig");
        builder
            .field("connection_string", &redacted_conn)
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("start_lsn", &self.start_lsn)
            .field("buffer_size", &self.buffer_size);

        #[cfg(feature = "postgres-tls")]
        {
            let tls_enabled = self
                .tls_config
                .as_ref()
                .map(|c| c.is_enabled())
                .unwrap_or(false);
            builder.field("tls_enabled", &tls_enabled);
        }

        if let Some(ref sig_config) = self.signal_config {
            builder.field("signal_data_collection", &sig_config.signal_data_collection);
        }

        builder.finish()
    }
}

/// Redact password from a connection string for safe logging
fn redact_connection_string(conn_str: &str) -> String {
    // Try URL format first: postgresql://user:password@host:port/db
    if let Ok(url) = Url::parse(conn_str) {
        if url.password().is_some() {
            let mut redacted = url.clone();
            let _ = redacted.set_password(Some("[REDACTED]"));
            return redacted.to_string();
        }
        return conn_str.to_string();
    }

    // Handle key=value format: host=localhost password=secret user=postgres
    let mut result = String::new();
    let mut in_password = false;
    let mut skip_until_space = false;

    for (i, c) in conn_str.char_indices() {
        if skip_until_space {
            if c.is_whitespace() {
                skip_until_space = false;
                result.push(c);
            }
            continue;
        }

        // Check for password= pattern
        let remaining = &conn_str[i..];
        if remaining.to_lowercase().starts_with("password=") {
            result.push_str("password=[REDACTED]");
            in_password = true;
            skip_until_space = true;
            continue;
        }

        if !in_password {
            result.push(c);
        }
    }

    result
}

impl PostgresCdcConfig {
    /// Create a new configuration builder
    pub fn builder() -> PostgresCdcConfigBuilder {
        PostgresCdcConfigBuilder::default()
    }
}

impl CdcConfig for PostgresCdcConfig {
    fn source_type(&self) -> &'static str {
        "postgres"
    }

    fn connection_string(&self) -> &str {
        &self.connection_string
    }

    fn validate(&self) -> Result<()> {
        if self.connection_string.is_empty() {
            return Err(CdcError::config("Connection string is required"));
        }
        if self.slot_name.is_empty() {
            return Err(CdcError::config("Slot name is required"));
        }
        if self.publication_name.is_empty() {
            return Err(CdcError::config("Publication name is required"));
        }
        Ok(())
    }
}

/// Builder for PostgresCdcConfig
#[derive(Default)]
pub struct PostgresCdcConfigBuilder {
    connection_string: Option<String>,
    slot_name: Option<String>,
    publication_name: Option<String>,
    start_lsn: u64,
    buffer_size: usize,
    #[cfg(feature = "postgres-tls")]
    tls_config: Option<TlsConfig>,
    signal_config: Option<SignalConfig>,
}

impl PostgresCdcConfigBuilder {
    /// Set the connection string
    pub fn connection_string(mut self, s: impl Into<String>) -> Self {
        self.connection_string = Some(s.into());
        self
    }

    /// Set the replication slot name
    pub fn slot_name(mut self, s: impl Into<String>) -> Self {
        self.slot_name = Some(s.into());
        self
    }

    /// Set the publication name
    pub fn publication_name(mut self, s: impl Into<String>) -> Self {
        self.publication_name = Some(s.into());
        self
    }

    /// Set the starting LSN
    pub fn start_lsn(mut self, lsn: u64) -> Self {
        self.start_lsn = lsn;
        self
    }

    /// Set the event buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set TLS configuration for encrypted connections
    ///
    /// Requires the `postgres-tls` feature.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rivven_cdc::common::{TlsConfig, SslMode};
    ///
    /// let config = PostgresCdcConfig::builder()
    ///     .connection_string("postgres://user:pass@localhost/db")
    ///     .slot_name("my_slot")
    ///     .publication_name("my_publication")
    ///     .tls_config(TlsConfig::new(SslMode::Require))
    ///     .build()?;
    /// ```
    #[cfg(feature = "postgres-tls")]
    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Set signal configuration for runtime control
    ///
    /// Enables the signal table feature for runtime commands like
    /// `execute-snapshot`, `pause-snapshot`, `resume-snapshot`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rivven_cdc::common::SignalConfig;
    ///
    /// let config = PostgresCdcConfig::builder()
    ///     .connection_string("postgres://user:pass@localhost/db")
    ///     .slot_name("my_slot")
    ///     .publication_name("my_publication")
    ///     .signal_config(
    ///         SignalConfig::builder()
    ///             .signal_data_collection("public.debezium_signal")
    ///             .build()
    ///     )
    ///     .build()?;
    /// ```
    pub fn signal_config(mut self, config: SignalConfig) -> Self {
        self.signal_config = Some(config);
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<PostgresCdcConfig> {
        let config = PostgresCdcConfig {
            connection_string: self
                .connection_string
                .ok_or_else(|| CdcError::config("Connection string is required"))?,
            slot_name: self
                .slot_name
                .ok_or_else(|| CdcError::config("Slot name is required"))?,
            publication_name: self
                .publication_name
                .ok_or_else(|| CdcError::config("Publication name is required"))?,
            start_lsn: self.start_lsn,
            buffer_size: if self.buffer_size == 0 {
                1000
            } else {
                self.buffer_size
            },
            #[cfg(feature = "postgres-tls")]
            tls_config: self.tls_config,
            signal_config: self.signal_config,
        };
        config.validate()?;
        Ok(config)
    }
}

/// Shared state for signal table detection in CDC loop.
#[derive(Clone)]
struct SignalTableState {
    /// Fully qualified signal table name (schema.table)
    signal_table: Option<String>,
    /// Pending signals from CDC stream (written by CDC loop, read by SignalManager)
    pending_signals: Arc<RwLock<Vec<crate::common::SignalRecord>>>,
}

impl SignalTableState {
    fn new(config: &Option<SignalConfig>) -> Self {
        Self {
            signal_table: config
                .as_ref()
                .and_then(|c| c.signal_data_collection.clone()),
            pending_signals: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Check if this is a signal table event.
    fn is_signal_table(&self, schema: &str, table: &str) -> bool {
        if let Some(ref fqn) = self.signal_table {
            let expected = format!("{}.{}", schema, table);
            fqn == &expected || fqn == table
        } else {
            false
        }
    }

    /// Handle a signal table insert.
    async fn handle_signal_insert(&self, json: &serde_json::Value) {
        // Extract id, type, data from JSON row
        let id = json.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let signal_type = json.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let data = json.get("data").and_then(|v| v.as_str());

        if id.is_empty() || signal_type.is_empty() {
            warn!("Signal table row missing id or type: {:?}", json);
            return;
        }

        let record = crate::common::SignalRecord {
            id: id.to_string(),
            signal_type: signal_type.to_string(),
            data: data.map(|s| s.to_string()),
            offset: None,
        };

        debug!(
            "Signal table INSERT detected: id={}, type={}",
            id, signal_type
        );
        self.pending_signals.write().await.push(record);
    }
}

/// PostgreSQL CDC source
pub struct PostgresCdc {
    config: PostgresCdcConfig,
    active: bool,
    event_tx: Option<mpsc::Sender<CdcEvent>>,
    event_rx: Option<mpsc::Receiver<CdcEvent>>,
    /// Signal processor for handling signals
    signal_processor: Arc<SignalProcessor>,
    /// Signal state shared with CDC loop
    signal_state: SignalTableState,
}

impl PostgresCdc {
    /// Create a new PostgreSQL CDC source
    pub fn new(config: PostgresCdcConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let signal_state = SignalTableState::new(&config.signal_config);
        Self {
            config,
            active: false,
            event_tx: Some(tx),
            event_rx: Some(rx),
            signal_processor: Arc::new(SignalProcessor::new()),
            signal_state,
        }
    }

    /// Take the event receiver (can only be called once)
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<CdcEvent>> {
        self.event_rx.take()
    }

    /// Get configuration
    pub fn config(&self) -> &PostgresCdcConfig {
        &self.config
    }

    /// Get the signal processor for registering custom handlers
    pub fn signal_processor(&self) -> &Arc<SignalProcessor> {
        &self.signal_processor
    }

    /// Check if the connector is paused by a signal
    pub fn is_paused(&self) -> bool {
        self.signal_processor.is_paused()
    }
}

#[async_trait]
impl CdcSource for PostgresCdc {
    async fn start(&mut self) -> Result<()> {
        info!("Starting PostgreSQL CDC on slot {}", self.config.slot_name);

        if self.config.signal_config.is_some() {
            info!(
                "Signal table enabled: {:?}",
                self.config
                    .signal_config
                    .as_ref()
                    .and_then(|c| c.signal_data_collection.as_ref())
            );
        }

        let config = self.config.clone();
        let event_tx = self
            .event_tx
            .clone()
            .ok_or_else(|| CdcError::InvalidState("Event sender not available".into()))?;
        let signal_state = self.signal_state.clone();
        let signal_processor = Arc::clone(&self.signal_processor);

        // Spawn the CDC loop
        tokio::spawn(async move {
            match run_cdc_loop(&config, event_tx, signal_state, signal_processor).await {
                Ok(_) => info!("PostgreSQL CDC loop finished gracefully"),
                Err(e) => error!("PostgreSQL CDC loop failed: {:?}", e),
            }
        });

        self.active = true;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!("Stopping PostgreSQL CDC");
        self.active = false;
        // Drop the sender to signal the loop to stop
        self.event_tx = None;
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        self.active
    }
}

/// Main CDC loop
async fn run_cdc_loop(
    config: &PostgresCdcConfig,
    event_tx: mpsc::Sender<CdcEvent>,
    signal_state: SignalTableState,
    signal_processor: Arc<SignalProcessor>,
) -> anyhow::Result<()> {
    // Parse connection string
    let url = Url::parse(&config.connection_string)?;
    let host = url.host_str().unwrap_or("localhost");
    let port = url.port().unwrap_or(5432);
    let user = url.username();
    let password = url.password();
    let database = url.path().trim_start_matches('/').to_string();
    let database = if database.is_empty() {
        "postgres"
    } else {
        &database
    };

    // Connect with TLS if configured, otherwise plain secure connection
    #[cfg(feature = "postgres-tls")]
    let client = {
        if let Some(ref tls_config) = config.tls_config {
            if tls_config.is_enabled() {
                info!("Connecting with TLS (mode: {})", tls_config.mode);
                SecureReplicationClient::connect_with_tls(
                    host, port, user, database, password, tls_config,
                )
                .await?
            } else {
                SecureReplicationClient::connect(host, port, user, database, password).await?
            }
        } else {
            SecureReplicationClient::connect(host, port, user, database, password).await?
        }
    };

    #[cfg(not(feature = "postgres-tls"))]
    let client = SecureReplicationClient::connect(host, port, user, database, password).await?;

    let mut stream = client
        .start_replication(
            &config.slot_name,
            config.start_lsn,
            &config.publication_name,
        )
        .await?;

    let mut relations: HashMap<u32, RelationBody> = HashMap::new();
    let mut event_buffer: Vec<CdcEvent> = Vec::new();
    const BATCH_SIZE: usize = 100;

    loop {
        // Process any pending signals from the CDC stream
        {
            let mut pending = signal_state.pending_signals.write().await;
            for record in pending.drain(..) {
                match record.to_signal(crate::common::SignalSource::Source) {
                    Ok(signal) => {
                        let result = signal_processor.process(signal).await;
                        debug!("Processed signal {}: {:?}", record.id, result);
                    }
                    Err(e) => {
                        warn!("Failed to parse signal {}: {}", record.id, e);
                    }
                }
            }
        }

        // Check if paused by signal
        if signal_processor.is_paused() {
            debug!("CDC paused by signal, waiting...");
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }

        let msg_opt = stream.next_message().await?;

        match msg_opt {
            Some(mut bytes) => {
                if !bytes.has_remaining() {
                    continue;
                }
                let type_code = bytes.get_u8();

                match type_code {
                    b'w' => {
                        // XLogData
                        let _wal_start = bytes.get_u64();
                        let _wal_end = bytes.get_u64();
                        let _ts = bytes.get_i64();

                        match PgOutputDecoder::decode(&mut bytes) {
                            Ok(msg) => match msg {
                                ReplicationMessage::Relation(rel) => {
                                    relations.insert(rel.id, rel);
                                }
                                ReplicationMessage::Insert(ins) => {
                                    if let Some(rel) = relations.get(&ins.relation_id) {
                                        let json = tuple_to_json(&ins.tuple, rel);

                                        // Check if this is a signal table INSERT
                                        if signal_state.is_signal_table(&rel.namespace, &rel.name) {
                                            signal_state.handle_signal_insert(&json).await;
                                            // Don't emit signal table changes as CDC events
                                            continue;
                                        }

                                        let event = CdcEvent::insert(
                                            "postgres",
                                            database,
                                            &rel.namespace,
                                            &rel.name,
                                            json,
                                            current_timestamp(),
                                        );
                                        event_buffer.push(event);
                                    }
                                }
                                ReplicationMessage::Update(upd) => {
                                    if let Some(rel) = relations.get(&upd.relation_id) {
                                        // Skip signal table updates
                                        if signal_state.is_signal_table(&rel.namespace, &rel.name) {
                                            continue;
                                        }

                                        let after = tuple_to_json(&upd.new_tuple, rel);
                                        let before =
                                            upd.key_tuple.as_ref().map(|t| tuple_to_json(t, rel));
                                        let event = CdcEvent::update(
                                            "postgres",
                                            database,
                                            &rel.namespace,
                                            &rel.name,
                                            before,
                                            after,
                                            current_timestamp(),
                                        );
                                        event_buffer.push(event);
                                    }
                                }
                                ReplicationMessage::Delete(del) => {
                                    if let Some(rel) = relations.get(&del.relation_id) {
                                        // Skip signal table deletes
                                        if signal_state.is_signal_table(&rel.namespace, &rel.name) {
                                            continue;
                                        }

                                        if let Some(key_tuple) = &del.key_tuple {
                                            let before = tuple_to_json(key_tuple, rel);
                                            let event = CdcEvent::delete(
                                                "postgres",
                                                database,
                                                &rel.namespace,
                                                &rel.name,
                                                before,
                                                current_timestamp(),
                                            );
                                            event_buffer.push(event);
                                        }
                                    }
                                }
                                ReplicationMessage::Commit(_) => {
                                    // Flush buffer on commit
                                    for event in event_buffer.drain(..) {
                                        if event_tx.send(event).await.is_err() {
                                            info!("Event receiver dropped, stopping");
                                            return Ok(());
                                        }
                                    }
                                }
                                ReplicationMessage::Begin(_) => {
                                    event_buffer.clear();
                                }
                                _ => {}
                            },
                            Err(e) => {
                                warn!("Decoder error: {}", e);
                            }
                        }
                    }
                    b'k' => {
                        // PrimaryKeepAlive
                        let wal_end = bytes.get_u64();
                        let _ts = bytes.get_i64();
                        let reply_requested = bytes.get_u8();

                        if reply_requested == 1 {
                            debug!("Sending KeepAlive response for LSN {}", wal_end);
                            stream.send_status_update(wal_end).await?;
                        }
                    }
                    _ => {
                        debug!("Unknown stream message: {}", type_code);
                    }
                }
            }
            None => {
                info!("Replication stream ended");
                break;
            }
        }

        // Flush large buffers
        if event_buffer.len() >= BATCH_SIZE {
            for event in event_buffer.drain(..) {
                if event_tx.send(event).await.is_err() {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

fn tuple_to_json(tuple: &Tuple, schema: &RelationBody) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (i, col_data) in tuple.0.iter().enumerate() {
        if let Some(col_def) = schema.columns.get(i) {
            let value = match col_data {
                TupleData::Null => serde_json::Value::Null,
                TupleData::Toast => serde_json::Value::String("<toast>".to_string()),
                TupleData::Text(bytes) => {
                    let s = String::from_utf8_lossy(bytes);
                    serde_json::Value::String(s.to_string())
                }
            };
            map.insert(col_def.name.clone(), value);
        }
    }
    serde_json::Value::Object(map)
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_debug_redacts_url_password() {
        let config = PostgresCdcConfig::builder()
            .connection_string("postgresql://user:secret_password@localhost:5432/mydb")
            .slot_name("test_slot")
            .publication_name("test_pub")
            .build()
            .unwrap();

        let debug_output = format!("{:?}", config);

        // URL encoding may encode brackets, so check for either [REDACTED] or %5BREDACTED%5D
        let has_redacted =
            debug_output.contains("[REDACTED]") || debug_output.contains("%5BREDACTED%5D");
        assert!(
            has_redacted,
            "Debug output should contain REDACTED marker: {}",
            debug_output
        );

        // Should NOT contain the actual password
        assert!(
            !debug_output.contains("secret_password"),
            "Debug output should not contain the password"
        );

        // Should still show non-sensitive parts
        assert!(
            debug_output.contains("localhost"),
            "Debug output should show host"
        );
        assert!(
            debug_output.contains("user"),
            "Debug output should show user"
        );
    }

    #[test]
    fn test_config_debug_redacts_keyword_password() {
        let config = PostgresCdcConfig::builder()
            .connection_string(
                "host=localhost port=5432 user=admin password=super_secret dbname=mydb",
            )
            .slot_name("test_slot")
            .publication_name("test_pub")
            .build()
            .unwrap();

        let debug_output = format!("{:?}", config);

        // Should contain REDACTED for password in key=value format
        assert!(
            debug_output.contains("[REDACTED]"),
            "Debug output should contain [REDACTED]: {}",
            debug_output
        );

        // Should NOT contain the actual password
        assert!(
            !debug_output.contains("super_secret"),
            "Debug output should not contain the password"
        );
    }

    #[test]
    fn test_config_debug_shows_no_password_connection() {
        let config = PostgresCdcConfig::builder()
            .connection_string("postgresql://user@localhost:5432/mydb")
            .slot_name("test_slot")
            .publication_name("test_pub")
            .build()
            .unwrap();

        let debug_output = format!("{:?}", config);

        // Should not show [REDACTED] when there's no password
        let has_redacted =
            debug_output.contains("[REDACTED]") || debug_output.contains("%5BREDACTED%5D");
        assert!(
            !has_redacted,
            "Debug output should not contain REDACTED when no password: {}",
            debug_output
        );
    }

    #[test]
    fn test_redact_connection_string_url_format() {
        let redacted = redact_connection_string("postgresql://user:password123@localhost:5432/db");
        // URL encoding may encode brackets
        let has_redacted = redacted.contains("[REDACTED]") || redacted.contains("%5BREDACTED%5D");
        assert!(has_redacted);
        assert!(!redacted.contains("password123"));
    }

    #[test]
    fn test_redact_connection_string_keyword_format() {
        let redacted = redact_connection_string("host=localhost password=mysecret user=admin");
        assert!(redacted.contains("[REDACTED]"));
        assert!(!redacted.contains("mysecret"));
    }

    #[test]
    fn test_redact_connection_string_no_password() {
        let conn = "host=localhost user=admin dbname=mydb";
        let redacted = redact_connection_string(conn);
        assert!(!redacted.contains("[REDACTED]"));
        assert_eq!(redacted, conn);
    }
}
