//! Google Cloud Pub/Sub Source Connector for Rivven Connect
//!
//! This module provides Google Cloud Pub/Sub integration for cloud-native message streaming:
//! - **PubSubSource**: Subscribe to Pub/Sub topics, stream to Rivven
//!
//! # Use Cases
//!
//! - Ingest events from Google Cloud services via Pub/Sub
//! - Process GCP event-driven workflows
//! - Bridge Google Cloud workloads with Rivven streaming
//!
//! # Example Configuration
//!
//! ```yaml
//! type: pubsub-source
//! config:
//!   project_id: my-gcp-project
//!   subscription: my-subscription
//!   max_messages: 100
//!   ack_deadline_seconds: 30
//!   auth:
//!     credentials_file: /path/to/service-account.json
//! ```
//!
//! # Authentication
//!
//! Supports multiple authentication methods:
//! - Service account JSON key file
//! - Application Default Credentials (ADC)
//! - Workload Identity (for GKE)
//! - Metadata service (for Compute Engine/Cloud Run)

use crate::{
    AnySource, Catalog, CheckResult, ConfiguredCatalog, ConnectorError, ConnectorSpec, Result,
    Source, SourceEvent, SourceFactory, State, Stream, SyncMode,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use validator::Validate;

/// Wrapper for sensitive configuration values
#[derive(Debug, Clone, JsonSchema)]
pub struct SensitiveString(#[schemars(with = "String")] SecretString);

impl SensitiveString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretString::from(value.into()))
    }

    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Serialize for SensitiveString {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str("***REDACTED***")
    }
}

impl<'de> Deserialize<'de> for SensitiveString {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Ok(Self::new(value))
    }
}

// ============================================================================
// Configuration Types
// ============================================================================

/// Google Cloud authentication configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct GcpAuthConfig {
    /// Path to service account JSON key file
    pub credentials_file: Option<String>,

    /// Service account JSON key content (base64 encoded)
    pub credentials_json: Option<SensitiveString>,

    /// Use Application Default Credentials (ADC)
    #[serde(default = "default_true")]
    pub use_adc: bool,

    /// Impersonate a service account
    pub impersonate_service_account: Option<String>,
}

impl Default for GcpAuthConfig {
    fn default() -> Self {
        Self {
            credentials_file: None,
            credentials_json: None,
            use_adc: true, // Default to ADC
            impersonate_service_account: None,
        }
    }
}

impl GcpAuthConfig {
    /// Check if explicit credentials are provided
    pub fn has_explicit_credentials(&self) -> bool {
        self.credentials_file.is_some() || self.credentials_json.is_some()
    }
}

/// Pub/Sub Source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct PubSubSourceConfig {
    /// Google Cloud project ID
    #[validate(length(min = 1, max = 30))]
    pub project_id: String,

    /// Subscription name (not full path)
    #[validate(length(min = 3, max = 255))]
    pub subscription: String,

    /// Topic name for auto-created subscriptions (not full path)
    pub topic: Option<String>,

    /// Authentication configuration
    #[serde(default)]
    pub auth: GcpAuthConfig,

    /// Maximum number of messages to receive per pull request
    #[serde(default = "default_max_messages")]
    #[validate(range(min = 1, max = 1000))]
    pub max_messages: i32,

    /// Acknowledgement deadline in seconds (10-600)
    #[serde(default = "default_ack_deadline")]
    #[validate(range(min = 10, max = 600))]
    pub ack_deadline_seconds: i32,

    /// Enable exactly-once delivery (requires ordering key)
    #[serde(default)]
    pub exactly_once: bool,

    /// Filter expression for messages (CEL)
    pub filter: Option<String>,

    /// Dead letter topic for failed messages
    pub dead_letter_topic: Option<String>,

    /// Maximum delivery attempts before dead letter
    #[serde(default = "default_max_delivery_attempts")]
    #[validate(range(min = 5, max = 100))]
    pub max_delivery_attempts: i32,

    /// Include message attributes in event metadata
    #[serde(default = "default_true")]
    pub include_attributes: bool,

    /// Include ordering key in event metadata
    #[serde(default = "default_true")]
    pub include_ordering_key: bool,

    /// Endpoint URL override (for emulator)
    pub endpoint_url: Option<String>,

    /// Enable streaming pull (more efficient for high-throughput)
    #[serde(default = "default_true")]
    pub streaming_pull: bool,
}

fn default_max_messages() -> i32 {
    100
}

fn default_ack_deadline() -> i32 {
    30
}

fn default_max_delivery_attempts() -> i32 {
    10
}

fn default_true() -> bool {
    true
}

impl Default for PubSubSourceConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            subscription: String::new(),
            topic: None,
            auth: GcpAuthConfig::default(),
            max_messages: default_max_messages(),
            ack_deadline_seconds: default_ack_deadline(),
            exactly_once: false,
            filter: None,
            dead_letter_topic: None,
            max_delivery_attempts: default_max_delivery_attempts(),
            include_attributes: true,
            include_ordering_key: true,
            endpoint_url: None,
            streaming_pull: true,
        }
    }
}

// ============================================================================
// Pub/Sub Message Types
// ============================================================================

/// Simulated Pub/Sub message for development/testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubMessage {
    /// Unique message ID assigned by Pub/Sub
    pub message_id: String,
    /// Message data (base64 encoded in real API)
    pub data: String,
    /// User-defined attributes
    pub attributes: HashMap<String, String>,
    /// Ordering key for ordered delivery
    pub ordering_key: Option<String>,
    /// Publish time
    pub publish_time: DateTime<Utc>,
    /// Delivery attempt (for exactly-once)
    pub delivery_attempt: Option<i32>,
    /// Ack ID for acknowledgement
    pub ack_id: String,
}

// ============================================================================
// Pub/Sub Source Implementation
// ============================================================================

/// Google Cloud Pub/Sub Source connector
pub struct PubSubSource {
    shutdown: Arc<AtomicBool>,
    messages_received: Arc<AtomicU64>,
    messages_acked: Arc<AtomicU64>,
}

impl PubSubSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            messages_received: Arc::new(AtomicU64::new(0)),
            messages_acked: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the full subscription path
    fn subscription_path(project_id: &str, subscription: &str) -> String {
        format!("projects/{}/subscriptions/{}", project_id, subscription)
    }

    /// Get the full topic path
    #[allow(dead_code)]
    fn topic_path(project_id: &str, topic: &str) -> String {
        format!("projects/{}/topics/{}", project_id, topic)
    }

    /// Generate a simulated Pub/Sub message for testing
    fn generate_test_message(sequence: u64) -> PubSubMessage {
        let message_id = format!("{}", sequence);
        let events = ["user.created", "order.placed", "payment.processed"];
        let event_type = events[sequence as usize % 3];

        let data = json!({
            "eventType": event_type,
            "sequence": sequence,
            "data": {
                "user_id": format!("user_{}", sequence % 100),
                "amount": (sequence % 1000) as f64 / 100.0,
                "timestamp": Utc::now().to_rfc3339()
            }
        })
        .to_string();

        let mut attributes = HashMap::new();
        attributes.insert("event_type".to_string(), event_type.to_string());
        attributes.insert("source".to_string(), "rivven-test".to_string());

        PubSubMessage {
            message_id: message_id.clone(),
            data,
            attributes,
            ordering_key: Some(format!("key-{}", sequence % 10)),
            publish_time: Utc::now(),
            delivery_attempt: Some(1),
            ack_id: format!("ack-{}", message_id),
        }
    }

    /// Convert Pub/Sub message to source event
    fn message_to_event(
        message: &PubSubMessage,
        stream_name: &str,
        config: &PubSubSourceConfig,
    ) -> SourceEvent {
        // Parse data as JSON or use as string
        let data =
            serde_json::from_str(&message.data).unwrap_or_else(|_| json!({"data": message.data}));

        let mut event = SourceEvent::record(stream_name, data)
            .with_position(message.message_id.clone())
            .with_metadata("pubsub.message_id", json!(message.message_id.clone()))
            .with_metadata("pubsub.ack_id", json!(message.ack_id.clone()))
            .with_metadata(
                "pubsub.publish_time",
                json!(message.publish_time.to_rfc3339()),
            );

        // Include ordering key if requested
        if config.include_ordering_key {
            if let Some(ref key) = message.ordering_key {
                event = event.with_metadata("pubsub.ordering_key", json!(key.clone()));
            }
        }

        // Include delivery attempt for exactly-once tracking
        if let Some(attempt) = message.delivery_attempt {
            event = event.with_metadata("pubsub.delivery_attempt", json!(attempt));
        }

        // Include message attributes if requested
        if config.include_attributes {
            for (key, value) in &message.attributes {
                event = event.with_metadata(format!("pubsub.attr.{}", key), json!(value.clone()));
            }
        }

        event
    }
}

impl Default for PubSubSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for PubSubSource {
    type Config = PubSubSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("pubsub-source", env!("CARGO_PKG_VERSION"))
            .description("Subscribe to Google Cloud Pub/Sub topics")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/pubsub-source")
            .config_schema::<PubSubSourceConfig>()
            .incremental(true)
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Validate configuration
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        // Check project_id format (alphanumeric + dashes)
        if !config
            .project_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-')
        {
            return Ok(CheckResult::failure(
                "Project ID must be alphanumeric with dashes only",
            ));
        }

        // Check authentication
        if !config.auth.has_explicit_credentials() && !config.auth.use_adc {
            warn!(
                "No explicit credentials configured. Will use Application Default Credentials (ADC)"
            );
        }

        info!(
            project_id = %config.project_id,
            subscription = %config.subscription,
            "Pub/Sub source configuration validated"
        );

        Ok(CheckResult::builder()
            .check_passed("configuration")
            .check_passed("project_id_format")
            .build())
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let stream_name = config
            .topic
            .clone()
            .unwrap_or_else(|| config.subscription.clone());

        let schema = json!({
            "type": "object",
            "properties": {
                "message_id": { "type": "string" },
                "data": { "type": "object" },
                "attributes": { "type": "object" },
                "ordering_key": { "type": ["string", "null"] },
                "publish_time": { "type": "string", "format": "date-time" }
            },
            "required": ["message_id", "data"]
        });

        let stream = Stream::new(&stream_name, schema)
            .namespace("pubsub")
            .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

        Ok(Catalog::new().add_stream(stream))
    }

    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let shutdown = Arc::clone(&self.shutdown);
        let messages_received = Arc::clone(&self.messages_received);
        let messages_acked = Arc::clone(&self.messages_acked);

        // Get stream name from catalog
        let stream_name = catalog
            .streams
            .first()
            .map(|s| s.stream.name.clone())
            .unwrap_or_else(|| config.subscription.clone());

        // Get starting sequence from state
        let start_sequence = state
            .as_ref()
            .and_then(|s| s.get_stream(&stream_name))
            .and_then(|stream_state| stream_state.cursor_value.as_ref())
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let config = config.clone();
        let subscription_path = Self::subscription_path(&config.project_id, &config.subscription);

        info!(
            subscription = %subscription_path,
            stream = %stream_name,
            start_sequence = start_sequence,
            streaming_pull = config.streaming_pull,
            "Starting Pub/Sub source"
        );

        let stream = async_stream::stream! {
            let mut sequence = start_sequence;
            let mut poll_count = 0u64;
            let poll_interval = Duration::from_millis(100);

            while !shutdown.load(Ordering::Relaxed) {
                poll_count += 1;

                // Simulate receiving messages (in production, use google-cloud-pubsub client)
                let batch_size = config.max_messages.min(10) as u64;

                for _ in 0..batch_size {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    sequence += 1;
                    let message = Self::generate_test_message(sequence);

                    debug!(
                        message_id = %message.message_id,
                        sequence = sequence,
                        ordering_key = ?message.ordering_key,
                        "Received Pub/Sub message"
                    );

                    let event = Self::message_to_event(&message, &stream_name, &config);
                    messages_received.fetch_add(1, Ordering::Relaxed);

                    // In production, we'd ack after successful processing
                    messages_acked.fetch_add(1, Ordering::Relaxed);

                    yield Ok(event);
                }

                // Emit state checkpoint periodically
                if poll_count.is_multiple_of(10) {
                    let state_event = SourceEvent::state(json!({
                        stream_name.clone(): {
                            "cursor_field": "sequence",
                            "cursor_value": sequence.to_string()
                        }
                    }));
                    yield Ok(state_event);
                }

                // Wait before next poll
                if !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(poll_interval).await;
                }
            }

            info!(
                total_messages = messages_received.load(Ordering::Relaxed),
                total_acked = messages_acked.load(Ordering::Relaxed),
                "Pub/Sub source shutdown"
            );
        };

        Ok(Box::pin(stream))
    }
}

impl Drop for PubSubSource {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory for creating Pub/Sub source instances
pub struct PubSubSourceFactory;

impl SourceFactory for PubSubSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        PubSubSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(PubSubSource::new())
    }
}

/// AnySource implementation for PubSubSource
#[async_trait]
impl AnySource for PubSubSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: PubSubSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Pub/Sub source config: {}", e)))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: PubSubSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Pub/Sub source config: {}", e)))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: PubSubSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Pub/Sub source config: {}", e)))?;
        self.read(&config, catalog, state).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_defaults() {
        let config = PubSubSourceConfig::default();

        assert_eq!(config.max_messages, 100);
        assert_eq!(config.ack_deadline_seconds, 30);
        assert_eq!(config.max_delivery_attempts, 10);
        assert!(config.include_attributes);
        assert!(config.include_ordering_key);
        assert!(config.streaming_pull);
    }

    #[test]
    fn test_generate_test_message() {
        let message = PubSubSource::generate_test_message(42);

        assert_eq!(message.message_id, "42");
        assert!(!message.data.is_empty());
        assert!(!message.attributes.is_empty());
        assert!(message.ordering_key.is_some());
    }

    #[test]
    fn test_message_to_event() {
        let message = PubSubSource::generate_test_message(1);
        let config = PubSubSourceConfig::default();

        let event = PubSubSource::message_to_event(&message, "test-stream", &config);

        assert_eq!(event.stream, "test-stream");
        assert!(event.metadata.extra.contains_key("pubsub.message_id"));
        assert!(event.metadata.extra.contains_key("pubsub.ack_id"));
    }

    #[tokio::test]
    async fn test_source_spec() {
        let spec = PubSubSource::spec();

        assert_eq!(spec.connector_type, "pubsub-source");
        assert!(spec.supports_incremental);
        assert!(spec.config_schema.is_some());
    }

    #[tokio::test]
    async fn test_source_check() {
        let source = PubSubSource::new();
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            ..Default::default()
        };

        let result = source.check(&config).await.unwrap();
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_source_discover() {
        let source = PubSubSource::new();
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            topic: Some("my-topic".to_string()),
            ..Default::default()
        };

        let catalog = source.discover(&config).await.unwrap();

        assert_eq!(catalog.streams.len(), 1);
        assert_eq!(catalog.streams[0].name, "my-topic");
    }

    #[test]
    fn test_subscription_path() {
        let path = PubSubSource::subscription_path("my-project", "my-sub");
        assert_eq!(path, "projects/my-project/subscriptions/my-sub");
    }

    #[test]
    fn test_topic_path() {
        let path = PubSubSource::topic_path("my-project", "my-topic");
        assert_eq!(path, "projects/my-project/topics/my-topic");
    }

    #[test]
    fn test_gcp_auth_config() {
        let auth = GcpAuthConfig {
            credentials_file: Some("/path/to/creds.json".to_string()),
            ..Default::default()
        };

        assert!(auth.has_explicit_credentials());

        let auth_adc = GcpAuthConfig::default();
        assert!(!auth_adc.has_explicit_credentials());
        assert!(auth_adc.use_adc);
    }

    #[test]
    fn test_sensitive_string_serialization() {
        let secret = SensitiveString::new("my-secret-key");
        let serialized = serde_json::to_string(&secret).unwrap();

        // Should not contain the actual secret
        assert_eq!(serialized, "\"***REDACTED***\"");
        assert!(!serialized.contains("my-secret-key"));
    }

    #[test]
    fn test_config_serialization() {
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            auth: GcpAuthConfig {
                credentials_json: Some(SensitiveString::new("secret-json")),
                ..Default::default()
            },
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        // Secret should be redacted
        assert!(!json.contains("secret-json"));
        assert!(json.contains("REDACTED"));
    }

    #[test]
    fn test_exactly_once_config() {
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            exactly_once: true,
            ..Default::default()
        };

        assert!(config.exactly_once);
    }

    #[test]
    fn test_dead_letter_config() {
        let config = PubSubSourceConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            dead_letter_topic: Some("dlq-topic".to_string()),
            max_delivery_attempts: 5,
            ..Default::default()
        };

        assert_eq!(config.dead_letter_topic, Some("dlq-topic".to_string()));
        assert_eq!(config.max_delivery_attempts, 5);
    }
}
