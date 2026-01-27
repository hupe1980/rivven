//! # CDC Signaling
//!
//! Multi-channel control system for CDC connectors - Debezium-compatible signaling.
//!
//! ## Features
//!
//! - **Ad-hoc Snapshots**: Trigger snapshots on demand
//! - **Pause/Resume**: Control streaming without restarting
//! - **Incremental Snapshots**: Chunk-based table re-snapshots
//! - **Custom Signals**: Application-defined signal handlers
//! - **Multi-Channel**: Source table, Kafka/Topic, File, API channels
//!
//! ## Signal Channels (Debezium Compatible)
//!
//! Rivven supports multiple signal channels, matching Debezium's architecture:
//!
//! | Channel | Description | Use Case |
//! |---------|-------------|----------|
//! | `source` | Signal table captured via CDC stream | Default, required for incremental snapshots |
//! | `topic` | Signals from a Rivven/Kafka topic | Avoids database writes |
//! | `file` | Signals from a JSON file | Simple deployments |
//! | `api` | HTTP/gRPC API calls | Programmatic control |
//!
//! The **source channel is enabled by default** because it implements the watermarking
//! mechanism for incremental snapshot deduplication. Signals flow through the CDC stream,
//! so no separate database connection is required.
//!
//! ## Debezium-Compatible Signal Table
//!
//! ```sql
//! CREATE TABLE debezium_signal (
//!     id VARCHAR(42) PRIMARY KEY,
//!     type VARCHAR(32) NOT NULL,
//!     data VARCHAR(2048) NULL
//! );
//! ```
//!
//! ## Configuration
//!
//! ```rust,ignore
//! use rivven_cdc::common::signal::{SignalConfig, SignalChannel as ChannelType};
//!
//! let config = SignalConfig::builder()
//!     .enabled_channels(vec![ChannelType::Source, ChannelType::Topic])
//!     .signal_data_collection("public.debezium_signal")  // Source channel table
//!     .signal_topic("cdc-signals")                        // Topic channel
//!     .build();
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::signal::{Signal, SignalProcessor, SignalAction};
//!
//! let processor = SignalProcessor::new();
//!
//! // Register custom handler
//! processor.register_handler("custom-action", |signal| {
//!     println!("Custom signal: {:?}", signal);
//!     Ok(())
//! });
//!
//! // Process a signal
//! let signal = Signal::execute_snapshot(vec!["public.orders".to_string()]);
//! processor.process(signal).await?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Signal action types - compatible with Debezium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SignalAction {
    /// Execute a snapshot for specified tables
    ExecuteSnapshot,
    /// Stop an in-progress snapshot
    StopSnapshot,
    /// Pause streaming
    PauseSnapshot,
    /// Resume streaming
    ResumeSnapshot,
    /// Log a message (diagnostic)
    Log,
    /// Custom action
    Custom(String),
}

impl SignalAction {
    /// Get the action name as string.
    pub fn as_str(&self) -> &str {
        match self {
            SignalAction::ExecuteSnapshot => "execute-snapshot",
            SignalAction::StopSnapshot => "stop-snapshot",
            SignalAction::PauseSnapshot => "pause-snapshot",
            SignalAction::ResumeSnapshot => "resume-snapshot",
            SignalAction::Log => "log",
            SignalAction::Custom(name) => name,
        }
    }

    /// Parse action from string.
    pub fn parse(s: &str) -> Self {
        match s {
            "execute-snapshot" => SignalAction::ExecuteSnapshot,
            "stop-snapshot" => SignalAction::StopSnapshot,
            "pause-snapshot" => SignalAction::PauseSnapshot,
            "resume-snapshot" => SignalAction::ResumeSnapshot,
            "log" => SignalAction::Log,
            other => SignalAction::Custom(other.to_string()),
        }
    }
}

/// Signal data payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalData {
    /// Tables to snapshot (for snapshot actions)
    #[serde(default, rename = "data-collections")]
    pub data_collections: Vec<String>,
    /// Snapshot type (blocking, incremental)
    #[serde(default, rename = "type")]
    pub snapshot_type: Option<String>,
    /// Additional properties
    #[serde(default, flatten)]
    pub properties: HashMap<String, serde_json::Value>,
}

impl SignalData {
    /// Create empty signal data.
    pub fn empty() -> Self {
        Self {
            data_collections: Vec::new(),
            snapshot_type: None,
            properties: HashMap::new(),
        }
    }

    /// Create signal data for snapshot.
    pub fn for_snapshot(tables: Vec<String>, snapshot_type: &str) -> Self {
        Self {
            data_collections: tables,
            snapshot_type: Some(snapshot_type.to_string()),
            properties: HashMap::new(),
        }
    }

    /// Create signal data for log message.
    pub fn for_log(message: &str) -> Self {
        let mut properties = HashMap::new();
        properties.insert(
            "message".to_string(),
            serde_json::Value::String(message.to_string()),
        );
        Self {
            data_collections: Vec::new(),
            snapshot_type: None,
            properties,
        }
    }

    /// Add a property.
    pub fn with_property(mut self, key: &str, value: serde_json::Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }

    /// Get a property value.
    pub fn get_property(&self, key: &str) -> Option<&serde_json::Value> {
        self.properties.get(key)
    }

    /// Get log message if present.
    pub fn log_message(&self) -> Option<&str> {
        self.properties.get("message")?.as_str()
    }
}

/// A CDC signal - Debezium compatible format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// Unique signal identifier
    pub id: String,
    /// Signal action type
    #[serde(rename = "type")]
    pub action: SignalAction,
    /// Signal data/payload
    #[serde(default)]
    pub data: SignalData,
    /// Timestamp when signal was created
    #[serde(default = "default_timestamp")]
    pub timestamp: i64,
    /// Source of the signal
    #[serde(default)]
    pub source: SignalSource,
}

fn default_timestamp() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Signal {
    /// Create a new signal.
    pub fn new(id: impl Into<String>, action: SignalAction, data: SignalData) -> Self {
        Self {
            id: id.into(),
            action,
            data,
            timestamp: chrono::Utc::now().timestamp_millis(),
            source: SignalSource::Api,
        }
    }

    /// Create an execute-snapshot signal.
    pub fn execute_snapshot(tables: Vec<String>) -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::ExecuteSnapshot,
            SignalData::for_snapshot(tables, "incremental"),
        )
    }

    /// Create a blocking snapshot signal.
    pub fn blocking_snapshot(tables: Vec<String>) -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::ExecuteSnapshot,
            SignalData::for_snapshot(tables, "blocking"),
        )
    }

    /// Create a stop-snapshot signal.
    pub fn stop_snapshot() -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::StopSnapshot,
            SignalData::empty(),
        )
    }

    /// Create a pause signal.
    pub fn pause() -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::PauseSnapshot,
            SignalData::empty(),
        )
    }

    /// Create a resume signal.
    pub fn resume() -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::ResumeSnapshot,
            SignalData::empty(),
        )
    }

    /// Create a log signal.
    pub fn log(message: &str) -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::Log,
            SignalData::for_log(message),
        )
    }

    /// Create a custom signal.
    pub fn custom(action: &str, data: SignalData) -> Self {
        Self::new(
            Uuid::new_v4().to_string(),
            SignalAction::Custom(action.to_string()),
            data,
        )
    }

    /// Set signal source.
    pub fn with_source(mut self, source: SignalSource) -> Self {
        self.source = source;
        self
    }

    /// Check if this is a snapshot action.
    pub fn is_snapshot_action(&self) -> bool {
        matches!(
            self.action,
            SignalAction::ExecuteSnapshot | SignalAction::StopSnapshot
        )
    }

    /// Check if this is a pause/resume action.
    pub fn is_control_action(&self) -> bool {
        matches!(
            self.action,
            SignalAction::PauseSnapshot | SignalAction::ResumeSnapshot
        )
    }
}

/// Source of the signal.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SignalSource {
    /// Signal from API call
    #[default]
    Api,
    /// Signal from database table
    Source,
    /// Signal from Kafka topic
    Kafka,
    /// Signal from file
    File,
    /// Signal from JMX
    Jmx,
}

/// Result of processing a signal.
#[derive(Debug, Clone)]
pub enum SignalResult {
    /// Signal processed successfully
    Success,
    /// Signal acknowledged but pending
    Pending(String),
    /// Signal ignored (not applicable)
    Ignored(String),
    /// Signal failed
    Failed(String),
}

impl SignalResult {
    /// Check if successful.
    pub fn is_success(&self) -> bool {
        matches!(self, SignalResult::Success | SignalResult::Pending(_))
    }

    /// Get error message if failed.
    pub fn error_message(&self) -> Option<&str> {
        match self {
            SignalResult::Failed(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Signal handler trait.
pub trait SignalHandler: Send + Sync {
    /// Handle a signal.
    fn handle(&self, signal: &Signal) -> impl std::future::Future<Output = SignalResult> + Send;

    /// Get supported action types.
    fn supported_actions(&self) -> Vec<SignalAction>;
}

/// Signal processing statistics.
#[derive(Debug, Default)]
pub struct SignalStats {
    /// Total signals received
    signals_received: AtomicU64,
    /// Signals processed successfully
    signals_processed: AtomicU64,
    /// Signals that failed
    signals_failed: AtomicU64,
    /// Signals ignored
    signals_ignored: AtomicU64,
    /// Snapshot signals
    snapshot_signals: AtomicU64,
    /// Control signals (pause/resume)
    control_signals: AtomicU64,
}

impl SignalStats {
    /// Record signal received.
    pub fn record_received(&self) {
        self.signals_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record signal processed.
    pub fn record_processed(&self) {
        self.signals_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record signal failed.
    pub fn record_failed(&self) {
        self.signals_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record signal ignored.
    pub fn record_ignored(&self) {
        self.signals_ignored.fetch_add(1, Ordering::Relaxed);
    }

    /// Record snapshot signal.
    pub fn record_snapshot(&self) {
        self.snapshot_signals.fetch_add(1, Ordering::Relaxed);
    }

    /// Record control signal.
    pub fn record_control(&self) {
        self.control_signals.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total received.
    pub fn received(&self) -> u64 {
        self.signals_received.load(Ordering::Relaxed)
    }

    /// Get total processed.
    pub fn processed(&self) -> u64 {
        self.signals_processed.load(Ordering::Relaxed)
    }

    /// Get total failed.
    pub fn failed(&self) -> u64 {
        self.signals_failed.load(Ordering::Relaxed)
    }
}

/// Boxed async handler function.
type BoxedHandler = Box<
    dyn Fn(&Signal) -> std::pin::Pin<Box<dyn std::future::Future<Output = SignalResult> + Send>>
        + Send
        + Sync,
>;

/// Signal processor for handling CDC signals.
pub struct SignalProcessor {
    /// Custom handlers keyed by action name
    handlers: RwLock<HashMap<String, BoxedHandler>>,
    /// Statistics
    stats: Arc<SignalStats>,
    /// Whether processing is paused
    paused: AtomicBool,
    /// Enabled signal sources
    enabled_sources: RwLock<Vec<SignalSource>>,
}

impl Default for SignalProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl SignalProcessor {
    /// Create a new signal processor.
    pub fn new() -> Self {
        Self {
            handlers: RwLock::new(HashMap::new()),
            stats: Arc::new(SignalStats::default()),
            paused: AtomicBool::new(false),
            enabled_sources: RwLock::new(vec![SignalSource::Api, SignalSource::Source]),
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> &Arc<SignalStats> {
        &self.stats
    }

    /// Check if processing is paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Pause processing.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        info!("Signal processor paused");
    }

    /// Resume processing.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        info!("Signal processor resumed");
    }

    /// Register a custom handler for an action.
    pub async fn register_handler<F, Fut>(&self, action: &str, handler: F)
    where
        F: Fn(&Signal) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = SignalResult> + Send + 'static,
    {
        let boxed: BoxedHandler = Box::new(move |signal| Box::pin(handler(signal)));
        self.handlers
            .write()
            .await
            .insert(action.to_string(), boxed);
        debug!("Registered handler for action: {}", action);
    }

    /// Set enabled signal sources.
    pub async fn set_enabled_sources(&self, sources: Vec<SignalSource>) {
        *self.enabled_sources.write().await = sources;
    }

    /// Check if signal source is enabled.
    pub async fn is_source_enabled(&self, source: &SignalSource) -> bool {
        self.enabled_sources.read().await.contains(source)
    }

    /// Process a signal.
    pub async fn process(&self, signal: Signal) -> SignalResult {
        self.stats.record_received();

        // Check if source is enabled
        if !self.is_source_enabled(&signal.source).await {
            debug!(
                "Signal source {:?} not enabled, ignoring: {}",
                signal.source, signal.id
            );
            self.stats.record_ignored();
            return SignalResult::Ignored(format!("Source {:?} not enabled", signal.source));
        }

        info!(
            "Processing signal: id={}, action={:?}, source={:?}",
            signal.id, signal.action, signal.source
        );

        // Track signal types
        if signal.is_snapshot_action() {
            self.stats.record_snapshot();
        }
        if signal.is_control_action() {
            self.stats.record_control();
        }

        // Handle built-in control actions
        let result = match &signal.action {
            SignalAction::PauseSnapshot => {
                self.pause();
                SignalResult::Success
            }
            SignalAction::ResumeSnapshot => {
                self.resume();
                SignalResult::Success
            }
            SignalAction::Log => {
                if let Some(msg) = signal.data.log_message() {
                    info!("Signal log message: {}", msg);
                }
                SignalResult::Success
            }
            _ => {
                // Check for custom handler
                let handlers = self.handlers.read().await;
                if let Some(handler) = handlers.get(signal.action.as_str()) {
                    handler(&signal).await
                } else {
                    // No handler - return pending for snapshot actions
                    if signal.is_snapshot_action() {
                        SignalResult::Pending(format!(
                            "Snapshot signal {} queued for processing",
                            signal.id
                        ))
                    } else {
                        warn!("No handler for action: {:?}", signal.action);
                        SignalResult::Ignored(format!("No handler for action: {:?}", signal.action))
                    }
                }
            }
        };

        // Update stats based on result
        match &result {
            SignalResult::Success | SignalResult::Pending(_) => {
                self.stats.record_processed();
            }
            SignalResult::Failed(_) => {
                self.stats.record_failed();
            }
            SignalResult::Ignored(_) => {
                self.stats.record_ignored();
            }
        }

        result
    }

    /// Parse signal from Debezium-compatible table row.
    pub fn parse_from_row(
        id: &str,
        signal_type: &str,
        data: Option<&str>,
    ) -> Result<Signal, String> {
        let action = SignalAction::parse(signal_type);

        let signal_data = if let Some(data_str) = data {
            serde_json::from_str(data_str)
                .map_err(|e| format!("Failed to parse signal data: {}", e))?
        } else {
            SignalData::empty()
        };

        Ok(Signal::new(id, action, signal_data).with_source(SignalSource::Source))
    }
}

/// Signal channel for communication between components.
#[derive(Clone)]
pub struct SignalChannel {
    sender: tokio::sync::mpsc::Sender<Signal>,
}

impl SignalChannel {
    /// Create a new signal channel.
    pub fn new(buffer_size: usize) -> (Self, tokio::sync::mpsc::Receiver<Signal>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(buffer_size);
        (Self { sender }, receiver)
    }

    /// Send a signal.
    pub async fn send(&self, signal: Signal) -> Result<(), String> {
        self.sender
            .send(signal)
            .await
            .map_err(|e| format!("Failed to send signal: {}", e))
    }

    /// Try to send a signal without blocking.
    pub fn try_send(&self, signal: Signal) -> Result<(), String> {
        self.sender
            .try_send(signal)
            .map_err(|e| format!("Failed to send signal: {}", e))
    }
}

// ============================================================================
// Signal Channel Configuration
// ============================================================================

/// Signal channel types - Debezium compatible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum SignalChannelType {
    /// Source channel - signal table captured via CDC stream (default)
    #[default]
    Source,
    /// Topic channel - signals from Rivven/Kafka topic
    Topic,
    /// File channel - signals from a JSON file
    File,
    /// API channel - signals from HTTP/gRPC
    Api,
    /// JMX channel (for compatibility, maps to API)
    Jmx,
}

impl SignalChannelType {
    /// Get the channel name as string (Debezium compatible).
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalChannelType::Source => "source",
            SignalChannelType::Topic => "kafka", // Debezium uses "kafka"
            SignalChannelType::File => "file",
            SignalChannelType::Api => "api",
            SignalChannelType::Jmx => "jmx",
        }
    }

    /// Parse from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "source" => Some(SignalChannelType::Source),
            "kafka" | "topic" => Some(SignalChannelType::Topic),
            "file" => Some(SignalChannelType::File),
            "api" => Some(SignalChannelType::Api),
            "jmx" => Some(SignalChannelType::Jmx),
            _ => None,
        }
    }
}

/// Configuration for CDC signaling - Debezium compatible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    /// Enabled signal channels (default: source, kafka)
    #[serde(default = "default_enabled_channels")]
    pub enabled_channels: Vec<SignalChannelType>,

    /// Fully-qualified name of signal data collection (for source channel)
    /// Format: schema.table (PostgreSQL) or database.table (MySQL)
    /// Default: None (disabled)
    #[serde(default)]
    pub signal_data_collection: Option<String>,

    /// Topic for signal messages (for topic channel)
    /// Default: None (disabled)
    #[serde(default)]
    pub signal_topic: Option<String>,

    /// Path to signal file (for file channel)
    /// Default: None (disabled)
    #[serde(default)]
    pub signal_file: Option<String>,

    /// Poll interval for file channel
    #[serde(default = "default_poll_interval_ms")]
    pub signal_poll_interval_ms: u64,

    /// Consumer properties for topic channel
    #[serde(default)]
    pub signal_consumer_properties: HashMap<String, String>,
}

fn default_enabled_channels() -> Vec<SignalChannelType> {
    vec![SignalChannelType::Source, SignalChannelType::Topic]
}

fn default_poll_interval_ms() -> u64 {
    1000 // 1 second
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            enabled_channels: default_enabled_channels(),
            signal_data_collection: None,
            signal_topic: None,
            signal_file: None,
            signal_poll_interval_ms: default_poll_interval_ms(),
            signal_consumer_properties: HashMap::new(),
        }
    }
}

impl SignalConfig {
    /// Create a new builder.
    pub fn builder() -> SignalConfigBuilder {
        SignalConfigBuilder::default()
    }

    /// Check if a channel is enabled.
    pub fn is_channel_enabled(&self, channel: SignalChannelType) -> bool {
        self.enabled_channels.contains(&channel)
    }

    /// Get the signal table name (without schema).
    pub fn signal_table_name(&self) -> Option<&str> {
        self.signal_data_collection
            .as_ref()
            .and_then(|s| s.split('.').next_back())
    }

    /// Get the signal schema name.
    pub fn signal_schema_name(&self) -> Option<&str> {
        self.signal_data_collection.as_ref().and_then(|s| {
            let parts: Vec<&str> = s.split('.').collect();
            if parts.len() >= 2 {
                Some(parts[0])
            } else {
                None
            }
        })
    }

    /// Parse enabled channels from comma-separated string (Debezium format).
    pub fn parse_enabled_channels(s: &str) -> Vec<SignalChannelType> {
        s.split(',')
            .filter_map(|c| SignalChannelType::parse(c.trim()))
            .collect()
    }
}

/// Builder for SignalConfig.
#[derive(Debug, Default)]
pub struct SignalConfigBuilder {
    enabled_channels: Option<Vec<SignalChannelType>>,
    signal_data_collection: Option<String>,
    signal_topic: Option<String>,
    signal_file: Option<String>,
    signal_poll_interval_ms: Option<u64>,
    signal_consumer_properties: HashMap<String, String>,
}

impl SignalConfigBuilder {
    /// Set enabled channels.
    pub fn enabled_channels(mut self, channels: Vec<SignalChannelType>) -> Self {
        self.enabled_channels = Some(channels);
        self
    }

    /// Enable specific channel.
    pub fn enable_channel(mut self, channel: SignalChannelType) -> Self {
        self.enabled_channels
            .get_or_insert_with(Vec::new)
            .push(channel);
        self
    }

    /// Set signal data collection (table name).
    pub fn signal_data_collection(mut self, collection: impl Into<String>) -> Self {
        self.signal_data_collection = Some(collection.into());
        self
    }

    /// Set signal topic.
    pub fn signal_topic(mut self, topic: impl Into<String>) -> Self {
        self.signal_topic = Some(topic.into());
        self
    }

    /// Set signal file path.
    pub fn signal_file(mut self, path: impl Into<String>) -> Self {
        self.signal_file = Some(path.into());
        self
    }

    /// Set poll interval for file channel.
    pub fn signal_poll_interval_ms(mut self, ms: u64) -> Self {
        self.signal_poll_interval_ms = Some(ms);
        self
    }

    /// Add consumer property for topic channel.
    pub fn consumer_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.signal_consumer_properties
            .insert(key.into(), value.into());
        self
    }

    /// Build the configuration.
    pub fn build(self) -> SignalConfig {
        SignalConfig {
            enabled_channels: self
                .enabled_channels
                .unwrap_or_else(default_enabled_channels),
            signal_data_collection: self.signal_data_collection,
            signal_topic: self.signal_topic,
            signal_file: self.signal_file,
            signal_poll_interval_ms: self
                .signal_poll_interval_ms
                .unwrap_or_else(default_poll_interval_ms),
            signal_consumer_properties: self.signal_consumer_properties,
        }
    }
}

// ============================================================================
// Signal Channel Reader Trait (Debezium SPI Compatible)
// ============================================================================

/// Signal record from a channel - minimal data needed.
#[derive(Debug, Clone)]
pub struct SignalRecord {
    /// Signal ID
    pub id: String,
    /// Signal type (action)
    pub signal_type: String,
    /// Signal data (JSON string)
    pub data: Option<String>,
    /// Source offset (for acknowledgment)
    pub offset: Option<String>,
}

impl SignalRecord {
    /// Create a new signal record.
    pub fn new(id: impl Into<String>, signal_type: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            signal_type: signal_type.into(),
            data: None,
            offset: None,
        }
    }

    /// Set data.
    pub fn with_data(mut self, data: impl Into<String>) -> Self {
        self.data = Some(data.into());
        self
    }

    /// Set offset.
    pub fn with_offset(mut self, offset: impl Into<String>) -> Self {
        self.offset = Some(offset.into());
        self
    }

    /// Convert to Signal.
    pub fn to_signal(&self, source: SignalSource) -> Result<Signal, String> {
        let action = SignalAction::parse(&self.signal_type);
        let signal_data = if let Some(data_str) = &self.data {
            serde_json::from_str(data_str)
                .map_err(|e| format!("Failed to parse signal data: {}", e))?
        } else {
            SignalData::empty()
        };
        Ok(Signal::new(&self.id, action, signal_data).with_source(source))
    }
}

/// Trait for reading signals from a channel.
///
/// This is the Rust equivalent of Debezium's `SignalChannelReader` SPI.
/// Implementations provide different signal sources (database, topic, file, etc.).
#[async_trait::async_trait]
pub trait SignalChannelReader: Send + Sync {
    /// Get the channel name.
    fn name(&self) -> &str;

    /// Initialize the channel reader.
    async fn init(&mut self) -> Result<(), String>;

    /// Read available signals. Returns empty vec if none available.
    async fn read(&mut self) -> Result<Vec<SignalRecord>, String>;

    /// Acknowledge a signal (optional - for channels that track consumption).
    async fn acknowledge(&mut self, _signal_id: &str) -> Result<(), String> {
        Ok(()) // Default: no-op
    }

    /// Close the channel reader.
    async fn close(&mut self) -> Result<(), String>;
}

// ============================================================================
// Source Signal Channel (CDC Stream)
// ============================================================================

/// Source signal channel - detects signals from CDC stream.
///
/// This channel watches for changes to the signal table through the normal
/// CDC replication stream. No separate database connection is required.
///
/// When a row is inserted into the signal table:
/// 1. The INSERT flows through logical replication like any other change
/// 2. The CDC connector detects it's from the signal table
/// 3. The signal is extracted and processed
///
/// This is the default and recommended channel because:
/// - Signal ordering is guaranteed (part of the change stream)
/// - Watermarking for incremental snapshots works correctly
/// - No additional connections or polling required
/// - Works with read replicas (signals replicate like other data)
pub struct SourceSignalChannel {
    /// Signal table name (fully qualified)
    signal_table: String,
    /// Pending signals detected from CDC stream
    pending: Arc<RwLock<Vec<SignalRecord>>>,
    /// Whether the channel is initialized
    initialized: bool,
}

impl SourceSignalChannel {
    /// Create a new source signal channel.
    pub fn new(signal_table: impl Into<String>) -> Self {
        Self {
            signal_table: signal_table.into(),
            pending: Arc::new(RwLock::new(Vec::new())),
            initialized: false,
        }
    }

    /// Get a reference to pending signals for CDC integration.
    pub fn pending_signals(&self) -> Arc<RwLock<Vec<SignalRecord>>> {
        Arc::clone(&self.pending)
    }

    /// Check if a CDC event is from the signal table.
    pub fn is_signal_event(&self, schema: &str, table: &str) -> bool {
        let expected = format!("{}.{}", schema, table);
        self.signal_table == expected || self.signal_table == table
    }

    /// Extract signal from CDC event (called by CDC connector).
    pub async fn handle_cdc_event(
        &self,
        id: &str,
        signal_type: &str,
        data: Option<&str>,
    ) -> Result<(), String> {
        let record = SignalRecord {
            id: id.to_string(),
            signal_type: signal_type.to_string(),
            data: data.map(|s| s.to_string()),
            offset: None,
        };
        self.pending.write().await.push(record);
        debug!(
            "Source channel: detected signal {} of type {}",
            id, signal_type
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl SignalChannelReader for SourceSignalChannel {
    fn name(&self) -> &str {
        "source"
    }

    async fn init(&mut self) -> Result<(), String> {
        info!(
            "Source signal channel initialized for table: {}",
            self.signal_table
        );
        self.initialized = true;
        Ok(())
    }

    async fn read(&mut self) -> Result<Vec<SignalRecord>, String> {
        // Drain pending signals detected from CDC stream
        let mut pending = self.pending.write().await;
        let signals = std::mem::take(&mut *pending);
        if !signals.is_empty() {
            debug!("Source channel: returning {} signals", signals.len());
        }
        Ok(signals)
    }

    async fn close(&mut self) -> Result<(), String> {
        info!("Source signal channel closed");
        self.initialized = false;
        Ok(())
    }
}

// ============================================================================
// File Signal Channel
// ============================================================================

/// File signal channel - reads signals from a JSON file.
///
/// The file should contain JSON signal objects, one per line:
/// ```json
/// {"id":"sig-1","type":"execute-snapshot","data":{"data-collections":["public.users"]}}
/// ```
///
/// Processed signals are tracked to avoid reprocessing.
pub struct FileSignalChannel {
    /// Path to signal file
    path: std::path::PathBuf,
    /// Processed signal IDs
    processed: std::collections::HashSet<String>,
    /// Last file modification time
    last_modified: Option<std::time::SystemTime>,
    /// Whether initialized
    initialized: bool,
}

impl FileSignalChannel {
    /// Create a new file signal channel.
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            processed: std::collections::HashSet::new(),
            last_modified: None,
            initialized: false,
        }
    }
}

#[async_trait::async_trait]
impl SignalChannelReader for FileSignalChannel {
    fn name(&self) -> &str {
        "file"
    }

    async fn init(&mut self) -> Result<(), String> {
        if !self.path.exists() {
            // Create empty file if it doesn't exist
            tokio::fs::write(&self.path, "")
                .await
                .map_err(|e| format!("Failed to create signal file: {}", e))?;
        }
        info!("File signal channel initialized: {:?}", self.path);
        self.initialized = true;
        Ok(())
    }

    async fn read(&mut self) -> Result<Vec<SignalRecord>, String> {
        // Check if file has been modified
        let metadata = tokio::fs::metadata(&self.path)
            .await
            .map_err(|e| format!("Failed to read signal file metadata: {}", e))?;

        let modified = metadata
            .modified()
            .map_err(|e| format!("Failed to get file modification time: {}", e))?;

        if self.last_modified == Some(modified) {
            return Ok(Vec::new()); // No changes
        }
        self.last_modified = Some(modified);

        // Read and parse file
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| format!("Failed to read signal file: {}", e))?;

        let mut signals = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Parse JSON line
            #[derive(Deserialize)]
            struct FileSignal {
                id: String,
                #[serde(rename = "type")]
                signal_type: String,
                data: Option<serde_json::Value>,
            }

            match serde_json::from_str::<FileSignal>(line) {
                Ok(fs) => {
                    if !self.processed.contains(&fs.id) {
                        let record = SignalRecord {
                            id: fs.id.clone(),
                            signal_type: fs.signal_type,
                            data: fs.data.map(|v| v.to_string()),
                            offset: None,
                        };
                        signals.push(record);
                        self.processed.insert(fs.id);
                    }
                }
                Err(e) => {
                    warn!("Failed to parse signal line: {} - {}", line, e);
                }
            }
        }

        if !signals.is_empty() {
            debug!("File channel: read {} new signals", signals.len());
        }

        Ok(signals)
    }

    async fn close(&mut self) -> Result<(), String> {
        info!("File signal channel closed");
        self.initialized = false;
        Ok(())
    }
}

// ============================================================================
// Multi-Channel Signal Manager
// ============================================================================

/// Manages multiple signal channels.
pub struct SignalManager {
    /// Active channel readers
    channels: Vec<Box<dyn SignalChannelReader>>,
    /// Signal processor
    processor: Arc<SignalProcessor>,
    /// Configuration
    config: SignalConfig,
    /// Running flag
    running: Arc<AtomicBool>,
}

impl SignalManager {
    /// Create a new signal manager.
    pub fn new(config: SignalConfig, processor: Arc<SignalProcessor>) -> Self {
        Self {
            channels: Vec::new(),
            processor,
            config,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Add a channel reader.
    pub fn add_channel(&mut self, channel: Box<dyn SignalChannelReader>) {
        info!("Adding signal channel: {}", channel.name());
        self.channels.push(channel);
    }

    /// Initialize all channels.
    pub async fn init(&mut self) -> Result<(), String> {
        for channel in &mut self.channels {
            channel.init().await?;
        }
        self.running.store(true, Ordering::SeqCst);
        info!(
            "Signal manager initialized with {} channels",
            self.channels.len()
        );
        Ok(())
    }

    /// Poll all channels and process signals.
    pub async fn poll(&mut self) -> Result<usize, String> {
        let mut total = 0;

        for channel in &mut self.channels {
            let records = channel.read().await?;
            for record in records {
                let source = match channel.name() {
                    "source" => SignalSource::Source,
                    "file" => SignalSource::File,
                    "kafka" | "topic" => SignalSource::Kafka,
                    _ => SignalSource::Api,
                };

                match record.to_signal(source) {
                    Ok(signal) => {
                        let result = self.processor.process(signal).await;
                        if result.is_success() {
                            // Acknowledge successful processing
                            if let Err(e) = channel.acknowledge(&record.id).await {
                                warn!("Failed to acknowledge signal {}: {}", record.id, e);
                            }
                        }
                        total += 1;
                    }
                    Err(e) => {
                        warn!("Failed to parse signal {}: {}", record.id, e);
                    }
                }
            }
        }

        Ok(total)
    }

    /// Close all channels.
    pub async fn close(&mut self) -> Result<(), String> {
        self.running.store(false, Ordering::SeqCst);
        for channel in &mut self.channels {
            if let Err(e) = channel.close().await {
                warn!("Failed to close channel {}: {}", channel.name(), e);
            }
        }
        info!("Signal manager closed");
        Ok(())
    }

    /// Check if running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get config reference.
    pub fn config(&self) -> &SignalConfig {
        &self.config
    }

    /// Get processor reference.
    pub fn processor(&self) -> &Arc<SignalProcessor> {
        &self.processor
    }

    /// Create a source channel for CDC integration.
    pub fn create_source_channel(&self) -> Option<SourceSignalChannel> {
        if self.config.is_channel_enabled(SignalChannelType::Source) {
            self.config
                .signal_data_collection
                .as_ref()
                .map(SourceSignalChannel::new)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_action_str() {
        assert_eq!(SignalAction::ExecuteSnapshot.as_str(), "execute-snapshot");
        assert_eq!(SignalAction::StopSnapshot.as_str(), "stop-snapshot");
        assert_eq!(SignalAction::PauseSnapshot.as_str(), "pause-snapshot");
        assert_eq!(SignalAction::ResumeSnapshot.as_str(), "resume-snapshot");
        assert_eq!(SignalAction::Log.as_str(), "log");
        assert_eq!(
            SignalAction::Custom("my-action".to_string()).as_str(),
            "my-action"
        );
    }

    #[test]
    fn test_signal_action_parse() {
        assert_eq!(
            SignalAction::parse("execute-snapshot"),
            SignalAction::ExecuteSnapshot
        );
        assert_eq!(
            SignalAction::parse("pause-snapshot"),
            SignalAction::PauseSnapshot
        );
        assert_eq!(
            SignalAction::parse("unknown"),
            SignalAction::Custom("unknown".to_string())
        );
    }

    #[test]
    fn test_signal_data_empty() {
        let data = SignalData::empty();
        assert!(data.data_collections.is_empty());
        assert!(data.snapshot_type.is_none());
        assert!(data.properties.is_empty());
    }

    #[test]
    fn test_signal_data_for_snapshot() {
        let data = SignalData::for_snapshot(
            vec!["public.users".to_string(), "public.orders".to_string()],
            "incremental",
        );
        assert_eq!(data.data_collections.len(), 2);
        assert_eq!(data.snapshot_type, Some("incremental".to_string()));
    }

    #[test]
    fn test_signal_data_for_log() {
        let data = SignalData::for_log("Test message");
        assert_eq!(data.log_message(), Some("Test message"));
    }

    #[test]
    fn test_signal_data_properties() {
        let data = SignalData::empty()
            .with_property("key1", serde_json::json!("value1"))
            .with_property("key2", serde_json::json!(42));

        assert_eq!(
            data.get_property("key1"),
            Some(&serde_json::json!("value1"))
        );
        assert_eq!(data.get_property("key2"), Some(&serde_json::json!(42)));
        assert_eq!(data.get_property("key3"), None);
    }

    #[test]
    fn test_signal_execute_snapshot() {
        let signal = Signal::execute_snapshot(vec!["public.users".to_string()]);

        assert_eq!(signal.action, SignalAction::ExecuteSnapshot);
        assert_eq!(signal.data.data_collections, vec!["public.users"]);
        assert_eq!(signal.data.snapshot_type, Some("incremental".to_string()));
        assert!(signal.is_snapshot_action());
        assert!(!signal.is_control_action());
    }

    #[test]
    fn test_signal_blocking_snapshot() {
        let signal = Signal::blocking_snapshot(vec!["public.orders".to_string()]);

        assert_eq!(signal.data.snapshot_type, Some("blocking".to_string()));
    }

    #[test]
    fn test_signal_stop_snapshot() {
        let signal = Signal::stop_snapshot();

        assert_eq!(signal.action, SignalAction::StopSnapshot);
        assert!(signal.is_snapshot_action());
    }

    #[test]
    fn test_signal_pause() {
        let signal = Signal::pause();

        assert_eq!(signal.action, SignalAction::PauseSnapshot);
        assert!(signal.is_control_action());
        assert!(!signal.is_snapshot_action());
    }

    #[test]
    fn test_signal_resume() {
        let signal = Signal::resume();

        assert_eq!(signal.action, SignalAction::ResumeSnapshot);
        assert!(signal.is_control_action());
    }

    #[test]
    fn test_signal_log() {
        let signal = Signal::log("Hello, CDC!");

        assert_eq!(signal.action, SignalAction::Log);
        assert_eq!(signal.data.log_message(), Some("Hello, CDC!"));
    }

    #[test]
    fn test_signal_custom() {
        let data =
            SignalData::empty().with_property("custom_field", serde_json::json!("custom_value"));
        let signal = Signal::custom("my-custom-action", data);

        assert_eq!(
            signal.action,
            SignalAction::Custom("my-custom-action".to_string())
        );
    }

    #[test]
    fn test_signal_with_source() {
        let signal = Signal::pause().with_source(SignalSource::Kafka);
        assert_eq!(signal.source, SignalSource::Kafka);
    }

    #[test]
    fn test_signal_result() {
        assert!(SignalResult::Success.is_success());
        assert!(SignalResult::Pending("waiting".to_string()).is_success());
        assert!(!SignalResult::Failed("error".to_string()).is_success());
        assert!(!SignalResult::Ignored("skipped".to_string()).is_success());

        assert_eq!(
            SignalResult::Failed("error msg".to_string()).error_message(),
            Some("error msg")
        );
        assert_eq!(SignalResult::Success.error_message(), None);
    }

    #[test]
    fn test_signal_stats() {
        let stats = SignalStats::default();

        stats.record_received();
        stats.record_received();
        assert_eq!(stats.received(), 2);

        stats.record_processed();
        assert_eq!(stats.processed(), 1);

        stats.record_failed();
        assert_eq!(stats.failed(), 1);

        stats.record_snapshot();
        stats.record_control();
    }

    #[tokio::test]
    async fn test_signal_processor_new() {
        let processor = SignalProcessor::new();

        assert!(!processor.is_paused());
        assert_eq!(processor.stats().received(), 0);
    }

    #[tokio::test]
    async fn test_signal_processor_pause_resume() {
        let processor = SignalProcessor::new();

        assert!(!processor.is_paused());

        processor.pause();
        assert!(processor.is_paused());

        processor.resume();
        assert!(!processor.is_paused());
    }

    #[tokio::test]
    async fn test_signal_processor_process_pause() {
        let processor = SignalProcessor::new();

        let result = processor.process(Signal::pause()).await;

        assert!(result.is_success());
        assert!(processor.is_paused());
    }

    #[tokio::test]
    async fn test_signal_processor_process_resume() {
        let processor = SignalProcessor::new();
        processor.pause();

        let result = processor.process(Signal::resume()).await;

        assert!(result.is_success());
        assert!(!processor.is_paused());
    }

    #[tokio::test]
    async fn test_signal_processor_process_log() {
        let processor = SignalProcessor::new();

        let result = processor.process(Signal::log("Test log")).await;

        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_signal_processor_custom_handler() {
        let processor = SignalProcessor::new();

        processor
            .register_handler("custom-action", |_signal| async { SignalResult::Success })
            .await;

        let signal = Signal::custom("custom-action", SignalData::empty());
        let result = processor.process(signal).await;

        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_signal_processor_source_filtering() {
        let processor = SignalProcessor::new();
        processor.set_enabled_sources(vec![SignalSource::Api]).await;

        // API source should work
        let api_signal = Signal::pause().with_source(SignalSource::Api);
        let result = processor.process(api_signal).await;
        assert!(result.is_success());

        // Kafka source should be ignored
        let kafka_signal = Signal::pause().with_source(SignalSource::Kafka);
        let result = processor.process(kafka_signal).await;
        assert!(matches!(result, SignalResult::Ignored(_)));
    }

    #[tokio::test]
    async fn test_signal_processor_snapshot_pending() {
        let processor = SignalProcessor::new();

        let signal = Signal::execute_snapshot(vec!["public.users".to_string()]);
        let result = processor.process(signal).await;

        // Without handler, snapshot signals should be pending
        assert!(matches!(result, SignalResult::Pending(_)));
    }

    #[tokio::test]
    async fn test_signal_processor_stats() {
        let processor = SignalProcessor::new();

        processor.process(Signal::log("msg1")).await;
        processor.process(Signal::pause()).await;
        processor.process(Signal::resume()).await;

        assert_eq!(processor.stats().received(), 3);
        assert_eq!(processor.stats().processed(), 3);
    }

    #[test]
    fn test_parse_from_row() {
        let signal = SignalProcessor::parse_from_row(
            "sig-1",
            "execute-snapshot",
            Some(r#"{"data-collections": ["public.users"]}"#),
        )
        .unwrap();

        assert_eq!(signal.id, "sig-1");
        assert_eq!(signal.action, SignalAction::ExecuteSnapshot);
        assert_eq!(signal.source, SignalSource::Source);
    }

    #[test]
    fn test_parse_from_row_no_data() {
        let signal = SignalProcessor::parse_from_row("sig-2", "pause-snapshot", None).unwrap();

        assert_eq!(signal.id, "sig-2");
        assert_eq!(signal.action, SignalAction::PauseSnapshot);
    }

    #[test]
    fn test_parse_from_row_invalid_json() {
        let result = SignalProcessor::parse_from_row("sig-3", "log", Some("not valid json"));

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_signal_channel() {
        let (channel, mut receiver) = SignalChannel::new(16);

        channel.send(Signal::pause()).await.unwrap();
        channel.send(Signal::resume()).await.unwrap();

        let sig1 = receiver.recv().await.unwrap();
        let sig2 = receiver.recv().await.unwrap();

        assert_eq!(sig1.action, SignalAction::PauseSnapshot);
        assert_eq!(sig2.action, SignalAction::ResumeSnapshot);
    }

    #[tokio::test]
    async fn test_signal_channel_try_send() {
        let (channel, _receiver) = SignalChannel::new(2);

        assert!(channel.try_send(Signal::pause()).is_ok());
        assert!(channel.try_send(Signal::resume()).is_ok());
        // Buffer full
        assert!(channel.try_send(Signal::log("overflow")).is_err());
    }

    #[test]
    fn test_signal_serialization() {
        let signal = Signal::execute_snapshot(vec!["public.users".to_string()]);
        let json = serde_json::to_string(&signal).unwrap();

        assert!(json.contains("execute-snapshot"));
        assert!(json.contains("public.users"));

        let parsed: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.action, SignalAction::ExecuteSnapshot);
    }

    // ========================================================================
    // Signal Channel Tests
    // ========================================================================

    #[test]
    fn test_signal_channel_type_str() {
        assert_eq!(SignalChannelType::Source.as_str(), "source");
        assert_eq!(SignalChannelType::Topic.as_str(), "kafka");
        assert_eq!(SignalChannelType::File.as_str(), "file");
        assert_eq!(SignalChannelType::Api.as_str(), "api");
        assert_eq!(SignalChannelType::Jmx.as_str(), "jmx");
    }

    #[test]
    fn test_signal_channel_type_parse() {
        assert_eq!(
            SignalChannelType::parse("source"),
            Some(SignalChannelType::Source)
        );
        assert_eq!(
            SignalChannelType::parse("kafka"),
            Some(SignalChannelType::Topic)
        );
        assert_eq!(
            SignalChannelType::parse("topic"),
            Some(SignalChannelType::Topic)
        );
        assert_eq!(
            SignalChannelType::parse("file"),
            Some(SignalChannelType::File)
        );
        assert_eq!(SignalChannelType::parse("unknown"), None);
    }

    #[test]
    fn test_signal_config_default() {
        let config = SignalConfig::default();

        assert!(config.is_channel_enabled(SignalChannelType::Source));
        assert!(config.is_channel_enabled(SignalChannelType::Topic));
        assert!(!config.is_channel_enabled(SignalChannelType::File));
        assert!(config.signal_data_collection.is_none());
        assert!(config.signal_topic.is_none());
    }

    #[test]
    fn test_signal_config_builder() {
        let config = SignalConfig::builder()
            .enabled_channels(vec![SignalChannelType::Source, SignalChannelType::File])
            .signal_data_collection("public.debezium_signal")
            .signal_file("/tmp/signals.json")
            .signal_poll_interval_ms(500)
            .consumer_property("bootstrap.servers", "localhost:9092")
            .build();

        assert!(config.is_channel_enabled(SignalChannelType::Source));
        assert!(config.is_channel_enabled(SignalChannelType::File));
        assert!(!config.is_channel_enabled(SignalChannelType::Topic));
        assert_eq!(
            config.signal_data_collection,
            Some("public.debezium_signal".to_string())
        );
        assert_eq!(config.signal_file, Some("/tmp/signals.json".to_string()));
        assert_eq!(config.signal_poll_interval_ms, 500);
    }

    #[test]
    fn test_signal_config_table_name() {
        let config = SignalConfig::builder()
            .signal_data_collection("public.debezium_signal")
            .build();

        assert_eq!(config.signal_table_name(), Some("debezium_signal"));
        assert_eq!(config.signal_schema_name(), Some("public"));
    }

    #[test]
    fn test_signal_config_parse_channels() {
        let channels = SignalConfig::parse_enabled_channels("source, kafka, file");

        assert_eq!(channels.len(), 3);
        assert!(channels.contains(&SignalChannelType::Source));
        assert!(channels.contains(&SignalChannelType::Topic));
        assert!(channels.contains(&SignalChannelType::File));
    }

    #[test]
    fn test_signal_record_to_signal() {
        let record = SignalRecord::new("sig-1", "execute-snapshot")
            .with_data(r#"{"data-collections": ["public.users"]}"#);

        let signal = record.to_signal(SignalSource::Source).unwrap();

        assert_eq!(signal.id, "sig-1");
        assert_eq!(signal.action, SignalAction::ExecuteSnapshot);
        assert_eq!(signal.source, SignalSource::Source);
        assert_eq!(signal.data.data_collections, vec!["public.users"]);
    }

    #[test]
    fn test_signal_record_to_signal_no_data() {
        let record = SignalRecord::new("sig-2", "pause-snapshot");

        let signal = record.to_signal(SignalSource::File).unwrap();

        assert_eq!(signal.id, "sig-2");
        assert_eq!(signal.action, SignalAction::PauseSnapshot);
        assert_eq!(signal.source, SignalSource::File);
    }

    #[test]
    fn test_signal_record_invalid_json() {
        let record = SignalRecord::new("sig-3", "log").with_data("not valid json");

        assert!(record.to_signal(SignalSource::Api).is_err());
    }

    #[tokio::test]
    async fn test_source_signal_channel() {
        let mut channel = SourceSignalChannel::new("public.debezium_signal");

        // Initialize
        channel.init().await.unwrap();
        assert_eq!(channel.name(), "source");

        // No signals initially
        let signals = channel.read().await.unwrap();
        assert!(signals.is_empty());

        // Simulate CDC event
        channel
            .handle_cdc_event(
                "sig-1",
                "execute-snapshot",
                Some(r#"{"data-collections": ["public.orders"]}"#),
            )
            .await
            .unwrap();

        // Read signal
        let signals = channel.read().await.unwrap();
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].id, "sig-1");
        assert_eq!(signals[0].signal_type, "execute-snapshot");

        // Should be empty after read
        let signals = channel.read().await.unwrap();
        assert!(signals.is_empty());

        // Close
        channel.close().await.unwrap();
    }

    #[test]
    fn test_source_signal_channel_is_signal_event() {
        let channel = SourceSignalChannel::new("public.debezium_signal");

        assert!(channel.is_signal_event("public", "debezium_signal"));
        assert!(!channel.is_signal_event("public", "users"));
        assert!(!channel.is_signal_event("other", "debezium_signal"));
    }

    #[tokio::test]
    async fn test_file_signal_channel() {
        // Create a temporary file
        let temp_dir = std::env::temp_dir();
        let signal_file = temp_dir.join(format!("rivven_signals_{}.json", uuid::Uuid::new_v4()));

        // Write test signals
        let content = r#"{"id":"sig-1","type":"execute-snapshot","data":{"data-collections":["public.users"]}}
{"id":"sig-2","type":"pause-snapshot"}
# This is a comment
{"id":"sig-3","type":"log","data":{"message":"Hello"}}"#;
        tokio::fs::write(&signal_file, content).await.unwrap();

        let mut channel = FileSignalChannel::new(&signal_file);

        // Initialize
        channel.init().await.unwrap();
        assert_eq!(channel.name(), "file");

        // Read signals
        let signals = channel.read().await.unwrap();
        assert_eq!(signals.len(), 3);
        assert_eq!(signals[0].id, "sig-1");
        assert_eq!(signals[1].id, "sig-2");
        assert_eq!(signals[2].id, "sig-3");

        // Second read should return empty (already processed)
        let signals = channel.read().await.unwrap();
        assert!(signals.is_empty());

        // Close and cleanup
        channel.close().await.unwrap();
        let _ = tokio::fs::remove_file(&signal_file).await;
    }

    #[tokio::test]
    async fn test_signal_manager() {
        let config = SignalConfig::builder()
            .enabled_channels(vec![SignalChannelType::Source])
            .signal_data_collection("public.debezium_signal")
            .build();

        let processor = Arc::new(SignalProcessor::new());
        let mut manager = SignalManager::new(config, processor.clone());

        // Create and add source channel
        let source_channel = manager.create_source_channel().unwrap();
        let pending = source_channel.pending_signals();
        manager.add_channel(Box::new(source_channel));

        // Initialize
        manager.init().await.unwrap();
        assert!(manager.is_running());

        // Simulate CDC signal
        pending
            .write()
            .await
            .push(SignalRecord::new("sig-1", "pause-snapshot"));

        // Poll and process
        let count = manager.poll().await.unwrap();
        assert_eq!(count, 1);

        // Processor should have received the signal
        assert!(processor.is_paused());

        // Close
        manager.close().await.unwrap();
        assert!(!manager.is_running());
    }
}
