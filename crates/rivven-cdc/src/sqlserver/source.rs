//! SQL Server CDC source implementation
//!
//! Captures Change Data Capture events from SQL Server using poll-based CDC table queries.

use crate::common::{CdcConfig, CdcError, CdcEvent, CdcSource, Result, TransactionMetadata};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use super::error::SqlServerError;

// ============================================================================
// Metrics
// ============================================================================

/// SQL Server CDC metrics
///
/// Thread-safe metrics collection for observability.
/// Use [`SqlServerCdc::metrics()`] to get a snapshot of current metrics.
#[derive(Default)]
pub struct SqlServerMetrics {
    /// Total events captured
    events_captured: AtomicU64,
    /// Total poll cycles
    poll_cycles: AtomicU64,
    /// Empty poll cycles (no changes)
    empty_polls: AtomicU64,
    /// Total polling time in milliseconds
    total_poll_time_ms: AtomicU64,
    /// Last poll duration in milliseconds
    last_poll_duration_ms: AtomicU64,
    /// Current capture instances count
    capture_instances: AtomicU64,
}

impl SqlServerMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record_event(&self) {
        self.events_captured.fetch_add(1, Ordering::Relaxed);
    }

    fn record_poll(&self, duration: Duration, events: usize) {
        self.poll_cycles.fetch_add(1, Ordering::Relaxed);
        let ms = duration.as_millis() as u64;
        self.total_poll_time_ms.fetch_add(ms, Ordering::Relaxed);
        self.last_poll_duration_ms.store(ms, Ordering::Relaxed);
        if events == 0 {
            self.empty_polls.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn set_capture_instances(&self, count: usize) {
        self.capture_instances
            .store(count as u64, Ordering::Relaxed);
    }

    /// Export metrics snapshot
    pub fn snapshot(&self) -> SqlServerMetricsSnapshot {
        SqlServerMetricsSnapshot {
            events_captured: self.events_captured.load(Ordering::Relaxed),
            poll_cycles: self.poll_cycles.load(Ordering::Relaxed),
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            avg_poll_time_ms: {
                let cycles = self.poll_cycles.load(Ordering::Relaxed);
                if cycles > 0 {
                    self.total_poll_time_ms.load(Ordering::Relaxed) / cycles
                } else {
                    0
                }
            },
            last_poll_duration_ms: self.last_poll_duration_ms.load(Ordering::Relaxed),
            capture_instances: self.capture_instances.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot for external export
#[derive(Debug, Clone)]
pub struct SqlServerMetricsSnapshot {
    pub events_captured: u64,
    pub poll_cycles: u64,
    pub empty_polls: u64,
    pub avg_poll_time_ms: u64,
    pub last_poll_duration_ms: u64,
    pub capture_instances: u64,
}

/// SQL Server CDC configuration
///
/// # Security Note
///
/// This struct implements a custom Debug that redacts the password field
/// to prevent accidental leakage to logs.
///
/// # TLS Support
///
/// TLS encryption is supported via the `sqlserver-tls` feature.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::sqlserver::SqlServerCdcConfig;
///
/// let config = SqlServerCdcConfig::builder()
///     .host("localhost")
///     .port(1433)
///     .username("sa")
///     .password("YourPassword123!")
///     .database("mydb")
///     .poll_interval_ms(500)
///     .include_table("dbo", "users")
///     .build()?;
/// ```
#[derive(Clone)]
pub struct SqlServerCdcConfig {
    /// SQL Server host
    pub host: String,
    /// SQL Server port (default: 1433)
    pub port: u16,
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: Option<String>,
    /// Database name (required for CDC)
    pub database: String,
    /// Application name for connection identification
    pub application_name: String,
    /// Poll interval in milliseconds (default: 500ms)
    pub poll_interval_ms: u64,
    /// Tables to include (schema, table pairs; empty = all CDC-enabled tables)
    pub include_tables: Vec<(String, String)>,
    /// Tables to exclude
    pub exclude_tables: Vec<(String, String)>,
    /// Starting LSN (hex string, empty = start from current position)
    pub start_lsn: String,
    /// Maximum batch size per poll (default: 1000)
    pub batch_size: u32,
    /// Event buffer size (default: 10000)
    pub buffer_size: usize,
    /// Connection timeout in seconds (default: 30)
    pub connect_timeout_secs: u64,
    /// Whether to trust server certificate (for self-signed certs)
    pub trust_server_certificate: bool,
    /// Whether to enable encryption
    pub encrypt: bool,
    /// Snapshot mode (Initial, Always, Never, WhenNeeded)
    pub snapshot_mode: crate::common::SnapshotMode,
    /// Cached redacted connection string for trait compliance
    redacted_conn_str: String,
}

impl std::fmt::Debug for SqlServerCdcConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlServerCdcConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("database", &self.database)
            .field("application_name", &self.application_name)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field("include_tables", &self.include_tables)
            .field("exclude_tables", &self.exclude_tables)
            .field("start_lsn", &self.start_lsn)
            .field("batch_size", &self.batch_size)
            .field("buffer_size", &self.buffer_size)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("trust_server_certificate", &self.trust_server_certificate)
            .field("encrypt", &self.encrypt)
            .field("snapshot_mode", &self.snapshot_mode)
            .field("redacted_conn_str", &self.redacted_conn_str)
            .finish()
    }
}

impl Default for SqlServerCdcConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 1433,
            username: String::new(),
            password: None,
            database: String::new(),
            application_name: "rivven-cdc".to_string(),
            poll_interval_ms: 500,
            include_tables: Vec::new(),
            exclude_tables: Vec::new(),
            start_lsn: String::new(),
            batch_size: 1000,
            buffer_size: 10_000,
            connect_timeout_secs: 30,
            trust_server_certificate: false,
            encrypt: true,
            snapshot_mode: crate::common::SnapshotMode::Initial,
            redacted_conn_str: String::new(),
        }
    }
}

impl SqlServerCdcConfig {
    /// Create a new builder for SqlServerCdcConfig
    pub fn builder() -> SqlServerCdcConfigBuilder {
        SqlServerCdcConfigBuilder::default()
    }

    /// Build ADO.NET style connection string for Tiberius
    pub fn connection_string(&self) -> String {
        let mut parts = vec![
            format!("Server={},{}", self.host, self.port),
            format!("Database={}", self.database),
            format!("User Id={}", self.username),
        ];

        if let Some(ref pwd) = self.password {
            parts.push(format!("Password={}", pwd));
        }

        parts.push(format!("Application Name={}", self.application_name));
        // safe cast — clamp to i32::MAX instead of silent truncation
        let timeout_secs = i32::try_from(self.connect_timeout_secs).unwrap_or(i32::MAX);
        parts.push(format!("Connect Timeout={}", timeout_secs));

        if self.trust_server_certificate {
            parts.push("TrustServerCertificate=true".to_string());
        }

        if self.encrypt {
            parts.push("Encrypt=true".to_string());
        } else {
            parts.push("Encrypt=false".to_string());
        }

        parts.join(";")
    }

    /// Redact password from connection string for logging
    pub fn redacted_connection_string(&self) -> String {
        let conn_str = self.connection_string();
        // Redact password in connection string
        if let Some(start) = conn_str.find("Password=") {
            if let Some(end) = conn_str[start..].find(';') {
                let before = &conn_str[..start];
                let after = &conn_str[start + end..];
                return format!("{}Password=[REDACTED]{}", before, after);
            } else {
                // Password is at the end
                return format!("{}Password=[REDACTED]", &conn_str[..start]);
            }
        }
        conn_str
    }
}

impl CdcConfig for SqlServerCdcConfig {
    fn source_type(&self) -> &'static str {
        "sqlserver"
    }

    fn connection_string(&self) -> &str {
        &self.redacted_conn_str
    }

    fn validate(&self) -> Result<()> {
        if self.host.is_empty() {
            return Err(CdcError::config("Host is required"));
        }
        if self.username.is_empty() {
            return Err(CdcError::config("Username is required"));
        }
        if self.database.is_empty() {
            return Err(CdcError::config("Database is required"));
        }
        if self.poll_interval_ms == 0 {
            return Err(CdcError::config("Poll interval must be > 0"));
        }
        if self.poll_interval_ms < 50 {
            return Err(CdcError::config(
                "Poll interval must be >= 50ms to avoid excessive load",
            ));
        }
        if self.batch_size == 0 {
            return Err(CdcError::config("Batch size must be > 0"));
        }
        Ok(())
    }
}

/// Builder for SqlServerCdcConfig
#[derive(Default)]
pub struct SqlServerCdcConfigBuilder {
    config: SqlServerCdcConfig,
}

impl SqlServerCdcConfigBuilder {
    /// Set the SQL Server host
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    /// Set the SQL Server port (default: 1433)
    pub fn port(mut self, port: u16) -> Self {
        self.config.port = port;
        self
    }

    /// Set the username for authentication
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.config.username = username.into();
        self
    }

    /// Set the password for authentication
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.config.password = Some(password.into());
        self
    }

    /// Set the database name
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.config.database = database.into();
        self
    }

    /// Set the application name for connection identification
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.config.application_name = name.into();
        self
    }

    /// Set the poll interval in milliseconds (default: 500ms)
    ///
    /// Lower values = lower latency but higher CPU/database load.
    /// Recommended: 100-1000ms for most use cases.
    pub fn poll_interval_ms(mut self, ms: u64) -> Self {
        self.config.poll_interval_ms = ms;
        self
    }

    /// Include a table for CDC capture
    ///
    /// If no tables are included, all CDC-enabled tables will be captured.
    pub fn include_table(mut self, schema: impl Into<String>, table: impl Into<String>) -> Self {
        self.config
            .include_tables
            .push((schema.into(), table.into()));
        self
    }

    /// Exclude a table from CDC capture
    pub fn exclude_table(mut self, schema: impl Into<String>, table: impl Into<String>) -> Self {
        self.config
            .exclude_tables
            .push((schema.into(), table.into()));
        self
    }

    /// Set the starting LSN (hex string)
    ///
    /// If empty, starts from the current maximum LSN.
    pub fn start_lsn(mut self, lsn: impl Into<String>) -> Self {
        self.config.start_lsn = lsn.into();
        self
    }

    /// Set the maximum batch size per poll (default: 1000)
    pub fn batch_size(mut self, size: u32) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the event buffer size (default: 10000)
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set connection timeout in seconds (default: 30)
    pub fn connect_timeout_secs(mut self, secs: u64) -> Self {
        self.config.connect_timeout_secs = secs;
        self
    }

    /// Trust server certificate (for self-signed certs)
    ///
    /// **Security warning**: Only use in development/testing.
    pub fn trust_server_certificate(mut self, trust: bool) -> Self {
        self.config.trust_server_certificate = trust;
        self
    }

    /// Enable/disable encryption (default: true)
    pub fn encrypt(mut self, encrypt: bool) -> Self {
        self.config.encrypt = encrypt;
        self
    }

    /// Set snapshot mode (Initial, Always, Never, WhenNeeded)
    ///
    /// - `Initial`: Take snapshot on first run, then stream changes
    /// - `Always`: Take snapshot on every restart
    /// - `Never`: Skip snapshot, stream changes only
    /// - `WhenNeeded`: Snapshot if no valid LSN position exists
    pub fn snapshot_mode(mut self, mode: crate::common::SnapshotMode) -> Self {
        self.config.snapshot_mode = mode;
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<SqlServerCdcConfig> {
        let mut config = self.config;
        config.validate()?;
        // Compute and cache the redacted connection string
        config.redacted_conn_str = config.redacted_connection_string();
        Ok(config)
    }
}

/// LSN (Log Sequence Number) for SQL Server
///
/// SQL Server uses 10-byte binary LSNs consisting of:
/// - VLF sequence number (4 bytes)
/// - Log block offset (4 bytes)
/// - Slot number (2 bytes)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    /// Raw LSN bytes (10 bytes)
    pub bytes: [u8; 10],
}

impl Lsn {
    /// Create a new LSN from bytes
    pub fn new(bytes: [u8; 10]) -> Self {
        Self { bytes }
    }

    /// Create an LSN from hex string (20 characters)
    pub fn from_hex(hex: &str) -> std::result::Result<Self, SqlServerError> {
        if hex.len() != 20 {
            return Err(SqlServerError::InvalidLsn(format!(
                "LSN hex must be 20 characters, got {}",
                hex.len()
            )));
        }
        let bytes = hex::decode(hex)
            .map_err(|e| SqlServerError::InvalidLsn(format!("Invalid hex: {}", e)))?;
        let arr: [u8; 10] = bytes
            .try_into()
            .map_err(|_| SqlServerError::InvalidLsn("Invalid LSN length".to_string()))?;
        Ok(Self::new(arr))
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.bytes)
    }

    /// Create minimum LSN (all zeros)
    pub fn min() -> Self {
        Self::new([0u8; 10])
    }

    /// Create maximum LSN (all ones)
    pub fn max() -> Self {
        Self::new([0xFF; 10])
    }

    /// Check if this is the minimum LSN
    pub fn is_min(&self) -> bool {
        self.bytes.iter().all(|&b| b == 0)
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format as SQL Server style: VLF:Offset:Slot
        let vlf = u32::from_be_bytes([self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3]]);
        let offset =
            u32::from_be_bytes([self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7]]);
        let slot = u16::from_be_bytes([self.bytes[8], self.bytes[9]]);
        write!(f, "{:08X}:{:08X}:{:04X}", vlf, offset, slot)
    }
}

/// CDC change position for tracking
///
/// Combines commit LSN and sequence value for precise positioning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcPosition {
    /// Commit LSN (transaction commit position)
    pub commit_lsn: Lsn,
    /// Change sequence within transaction
    pub change_lsn: Lsn,
}

impl CdcPosition {
    /// Create a new position
    pub fn new(commit_lsn: Lsn, change_lsn: Lsn) -> Self {
        Self {
            commit_lsn,
            change_lsn,
        }
    }

    /// Create a position from hex strings
    pub fn from_hex(
        commit_hex: &str,
        change_hex: &str,
    ) -> std::result::Result<Self, SqlServerError> {
        Ok(Self {
            commit_lsn: Lsn::from_hex(commit_hex)?,
            change_lsn: Lsn::from_hex(change_hex)?,
        })
    }

    /// Deserialize position from storage
    pub fn from_string(s: &str) -> std::result::Result<Self, SqlServerError> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(SqlServerError::InvalidLsn(format!(
                "Invalid position format: {}",
                s
            )));
        }
        Self::from_hex(parts[0], parts[1])
    }
}

impl std::fmt::Display for CdcPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}",
            self.commit_lsn.to_hex(),
            self.change_lsn.to_hex()
        )
    }
}

impl std::cmp::PartialOrd for CdcPosition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for CdcPosition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.commit_lsn.cmp(&other.commit_lsn) {
            std::cmp::Ordering::Equal => self.change_lsn.cmp(&other.change_lsn),
            ord => ord,
        }
    }
}

/// Capture instance information from sys.sp_cdc_help_change_data_capture
#[derive(Debug, Clone)]
pub struct CaptureInstance {
    /// Source schema
    pub source_schema: String,
    /// Source table
    pub source_table: String,
    /// Capture instance name (e.g., "dbo_users")
    pub capture_instance: String,
    /// Object ID of the source table
    pub object_id: i32,
    /// Column list
    pub columns: Vec<String>,
    /// Primary key columns
    pub primary_key_columns: Vec<String>,
    /// Whether net changes function is available
    pub supports_net_changes: bool,
}

/// SQL Server CDC source
///
/// Captures changes from SQL Server using poll-based CDC table queries.
pub struct SqlServerCdc {
    config: SqlServerCdcConfig,
    active: Arc<AtomicBool>,
    event_tx: Option<mpsc::Sender<CdcEvent>>,
    event_rx: Option<mpsc::Receiver<CdcEvent>>,
    /// Current position for tracking
    current_position: Option<CdcPosition>,
    /// Discovered capture instances
    capture_instances: Vec<CaptureInstance>,
    /// Metrics
    metrics: Arc<SqlServerMetrics>,
}

impl SqlServerCdc {
    /// Create a new SQL Server CDC source
    pub fn new(config: SqlServerCdcConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        Self {
            config,
            active: Arc::new(AtomicBool::new(false)),
            event_tx: Some(tx),
            event_rx: Some(rx),
            current_position: None,
            capture_instances: Vec::new(),
            metrics: SqlServerMetrics::new(),
        }
    }

    /// Take the event receiver (can only be called once)
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<CdcEvent>> {
        self.event_rx.take()
    }

    /// Get configuration
    pub fn config(&self) -> &SqlServerCdcConfig {
        &self.config
    }

    /// Get current position
    pub fn current_position(&self) -> Option<&CdcPosition> {
        self.current_position.as_ref()
    }

    /// Get discovered capture instances
    pub fn capture_instances(&self) -> &[CaptureInstance] {
        &self.capture_instances
    }

    /// Get current metrics snapshot
    pub fn metrics(&self) -> SqlServerMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Check if a table should be captured based on include/exclude config
    #[cfg(test)]
    fn should_capture_table(&self, schema: &str, table: &str) -> bool {
        // Check excludes first
        for (ex_schema, ex_table) in &self.config.exclude_tables {
            if ex_schema == schema && ex_table == table {
                return false;
            }
        }

        // If includes are specified, table must be in the list
        if !self.config.include_tables.is_empty() {
            for (inc_schema, inc_table) in &self.config.include_tables {
                if inc_schema == schema && inc_table == table {
                    return true;
                }
            }
            return false;
        }

        // No includes specified = capture all
        true
    }
}

#[async_trait]
impl CdcSource for SqlServerCdc {
    async fn start(&mut self) -> Result<()> {
        info!(
            "Starting SQL Server CDC on {}:{}/{}",
            self.config.host, self.config.port, self.config.database
        );

        if self.active.load(Ordering::SeqCst) {
            return Err(CdcError::config("CDC source already started"));
        }

        self.active.store(true, Ordering::SeqCst);

        // Spawn the polling task
        let config = self.config.clone();
        let active = self.active.clone();
        let metrics = self.metrics.clone();
        let event_tx = self
            .event_tx
            .clone()
            .ok_or_else(|| CdcError::config("Event sender not available"))?;
        let start_lsn = self.config.start_lsn.clone();
        let include_tables = self.config.include_tables.clone();
        let exclude_tables = self.config.exclude_tables.clone();

        tokio::spawn(async move {
            if let Err(e) = run_cdc_poll_loop(
                config,
                active,
                metrics,
                event_tx,
                start_lsn,
                include_tables,
                exclude_tables,
            )
            .await
            {
                error!("CDC poll loop error: {:?}", e);
            }
        });

        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!("Stopping SQL Server CDC");
        self.active.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
}

/// Run the CDC polling loop
async fn run_cdc_poll_loop(
    config: SqlServerCdcConfig,
    active: Arc<AtomicBool>,
    metrics: Arc<SqlServerMetrics>,
    event_tx: mpsc::Sender<CdcEvent>,
    start_lsn: String,
    include_tables: Vec<(String, String)>,
    exclude_tables: Vec<(String, String)>,
) -> Result<()> {
    info!("Connecting to SQL Server...");

    // Connect to SQL Server with retry
    let mut client = connect_with_retry(&config, 3).await?;

    // Verify CDC is enabled
    client.verify_cdc_enabled().await?;

    // Discover capture instances
    let instances = client.discover_capture_instances().await?;
    info!("Discovered {} capture instances", instances.len());

    // Filter to tables we want to capture
    let filtered_instances: Vec<_> = instances
        .into_iter()
        .filter(|inst| {
            // Check excludes
            for (schema, table) in &exclude_tables {
                if inst.source_schema == *schema && inst.source_table == *table {
                    return false;
                }
            }
            // Check includes
            if include_tables.is_empty() {
                true
            } else {
                include_tables.iter().any(|(schema, table)| {
                    inst.source_schema == *schema && inst.source_table == *table
                })
            }
        })
        .collect();

    if filtered_instances.is_empty() {
        warn!("No capture instances match filter criteria");
        return Ok(());
    }

    metrics.set_capture_instances(filtered_instances.len());

    info!(
        "Capturing changes from {} tables: {:?}",
        filtered_instances.len(),
        filtered_instances
            .iter()
            .map(|i| format!("{}.{}", i.source_schema, i.source_table))
            .collect::<Vec<_>>()
    );

    // Initialize position
    let mut current_position = if start_lsn.is_empty() {
        let max_lsn = client.get_max_lsn().await?;
        info!("Starting from current max LSN: {}", max_lsn);
        CdcPosition::new(max_lsn.clone(), max_lsn)
    } else {
        CdcPosition::from_string(&start_lsn).map_err(|e| CdcError::config(e.to_string()))?
    };

    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    // Main polling loop
    while active.load(Ordering::SeqCst) {
        let poll_start = Instant::now();
        let mut events_this_poll = 0;

        trace!("Polling for changes since {}", current_position.commit_lsn);

        // Get current max LSN
        let max_lsn = client.get_max_lsn().await?;

        // Skip if no new changes
        if max_lsn <= current_position.commit_lsn {
            metrics.record_poll(poll_start.elapsed(), 0);
            trace!("No new changes, sleeping...");
            tokio::time::sleep(poll_interval).await;
            continue;
        }

        // Query each capture instance for changes
        for instance in &filtered_instances {
            let changes = client
                .get_changes(
                    &instance.capture_instance,
                    &current_position.commit_lsn,
                    &max_lsn,
                    config.batch_size,
                )
                .await?;

            for change in changes {
                events_this_poll += 1;
                metrics.record_event();

                // Convert to CdcEvent
                let event = CdcEvent {
                    source_type: "sqlserver".to_string(),
                    database: config.database.clone(),
                    schema: instance.source_schema.clone(),
                    table: instance.source_table.clone(),
                    op: change.operation,
                    before: change.before.map(serde_json::Value::Object),
                    after: change.after.map(serde_json::Value::Object),
                    // unwrap_or_default instead of unwrap to handle pre-epoch clocks
                    timestamp: change.commit_time.unwrap_or_else(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64
                    }),
                    transaction: Some(TransactionMetadata {
                        id: change.commit_lsn.to_hex(),
                        lsn: change.change_lsn.to_hex(),
                        sequence: u16::from_be_bytes([
                            change.change_lsn.bytes[8],
                            change.change_lsn.bytes[9],
                        ]) as u64,
                        total_events: 0,
                        commit_ts: change.commit_time,
                        is_last: false,
                    }),
                };

                // Send event
                if event_tx.send(event).await.is_err() {
                    warn!("Event receiver dropped, stopping CDC");
                    return Ok(());
                }
            }
        }

        // Record poll metrics
        metrics.record_poll(poll_start.elapsed(), events_this_poll);

        if events_this_poll > 0 {
            debug!(
                "Processed {} events in {:?}",
                events_this_poll,
                poll_start.elapsed()
            );
        }

        // Update position
        current_position = CdcPosition::new(max_lsn.clone(), max_lsn);

        // Sleep before next poll
        tokio::time::sleep(poll_interval).await;
    }

    info!("CDC poll loop stopped");
    Ok(())
}

/// Connect to SQL Server with retry logic
async fn connect_with_retry(
    config: &SqlServerCdcConfig,
    max_retries: u32,
) -> Result<super::protocol::SqlServerClient> {
    use super::protocol::SqlServerClient;

    let mut last_error = None;
    let mut delay = Duration::from_millis(500);

    for attempt in 1..=max_retries {
        match SqlServerClient::connect(config).await {
            Ok(client) => return Ok(client),
            Err(e) => {
                warn!(
                    "Connection attempt {}/{} failed: {:?}",
                    attempt, max_retries, e
                );
                last_error = Some(e);

                if attempt < max_retries {
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, Duration::from_secs(30));
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| CdcError::ConnectionRefused("Max retries exceeded".to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = SqlServerCdcConfig::builder()
            .host("localhost")
            .port(1433)
            .username("sa")
            .password("TestPassword123!")
            .database("testdb")
            .poll_interval_ms(500)
            .include_table("dbo", "users")
            .build()
            .unwrap();

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 1433);
        assert_eq!(config.username, "sa");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.poll_interval_ms, 500);
        assert_eq!(config.include_tables.len(), 1);
    }

    #[test]
    fn test_config_validation() {
        // Missing username
        let result = SqlServerCdcConfig::builder()
            .host("localhost")
            .database("testdb")
            .build();
        assert!(result.is_err());

        // Missing database
        let result = SqlServerCdcConfig::builder()
            .host("localhost")
            .username("sa")
            .build();
        assert!(result.is_err());

        // Poll interval too low
        let result = SqlServerCdcConfig::builder()
            .host("localhost")
            .username("sa")
            .database("testdb")
            .poll_interval_ms(10)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_debug_redacts_password() {
        let config = SqlServerCdcConfig::builder()
            .host("localhost")
            .username("sa")
            .password("secret")
            .database("testdb")
            .build()
            .unwrap();

        let debug_str = format!("{:?}", config);
        assert!(!debug_str.contains("secret"));
        assert!(debug_str.contains("REDACTED"));
    }

    #[test]
    fn test_connection_string() {
        let config = SqlServerCdcConfig::builder()
            .host("myserver")
            .port(1433)
            .username("myuser")
            .password("mypass")
            .database("mydb")
            .build()
            .unwrap();

        let conn_str = config.connection_string();
        assert!(conn_str.contains("Server=myserver,1433"));
        assert!(conn_str.contains("Database=mydb"));
        assert!(conn_str.contains("User Id=myuser"));
        // Password should be present in raw connection string
        // but redacted_connection_string should redact it
        let redacted = config.redacted_connection_string();
        assert!(redacted.contains("[REDACTED]"));
        assert!(!redacted.contains("mypass"));
    }

    #[test]
    fn test_lsn_operations() {
        // Create from hex
        let lsn = Lsn::from_hex("00000001000000010001").unwrap();
        assert_eq!(lsn.to_hex(), "00000001000000010001");

        // Min/max
        assert!(Lsn::min().is_min());
        assert!(Lsn::min() < Lsn::max());

        // Display format
        let display = format!("{}", lsn);
        assert!(display.contains(":"));
    }

    #[test]
    fn test_position_ordering() {
        let pos1 = CdcPosition::from_hex("00000001000000010001", "00000001000000010001").unwrap();
        let pos2 = CdcPosition::from_hex("00000001000000010002", "00000001000000010001").unwrap();
        let pos3 = CdcPosition::from_hex("00000001000000010001", "00000001000000010002").unwrap();

        assert!(pos1 < pos2);
        assert!(pos1 < pos3);
        assert!(pos3 < pos2);
    }

    #[test]
    fn test_table_filtering() {
        let config = SqlServerCdcConfig::builder()
            .host("localhost")
            .username("sa")
            .database("testdb")
            .include_table("dbo", "users")
            .exclude_table("dbo", "logs")
            .build()
            .unwrap();

        let cdc = SqlServerCdc::new(config);

        assert!(cdc.should_capture_table("dbo", "users"));
        assert!(!cdc.should_capture_table("dbo", "logs"));
        assert!(!cdc.should_capture_table("dbo", "other")); // Not in include list
    }

    #[test]
    fn test_table_filtering_all_tables() {
        let config = SqlServerCdcConfig::builder()
            .host("localhost")
            .username("sa")
            .database("testdb")
            .exclude_table("dbo", "logs")
            .build()
            .unwrap();

        let cdc = SqlServerCdc::new(config);

        // No include list = capture all except excluded
        assert!(cdc.should_capture_table("dbo", "users"));
        assert!(!cdc.should_capture_table("dbo", "logs"));
        assert!(cdc.should_capture_table("dbo", "orders"));
    }
}
