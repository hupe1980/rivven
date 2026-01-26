//! # CDC Signaling
//!
//! Control channel for CDC connectors - Debezium-compatible signaling system.
//!
//! ## Features
//!
//! - **Ad-hoc Snapshots**: Trigger snapshots on demand
//! - **Pause/Resume**: Control streaming without restarting
//! - **Incremental Snapshots**: Chunk-based table re-snapshots
//! - **Custom Signals**: Application-defined signal handlers
//!
//! ## Debezium Compatibility
//!
//! Compatible with Debezium signaling table format:
//! ```sql
//! CREATE TABLE debezium_signal (
//!     id VARCHAR(42) PRIMARY KEY,
//!     type VARCHAR(32) NOT NULL,
//!     data VARCHAR(2048) NULL
//! );
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
}
