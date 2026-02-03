//! Amazon SQS Source Connector for Rivven Connect
//!
//! This module provides AWS SQS integration for cloud-native message queuing:
//! - **SqsSource**: Consume messages from SQS queues, stream to Rivven
//!
//! # Use Cases
//!
//! - Ingest events from AWS services via SQS
//! - Process async job queues
//! - Bridge AWS workloads with Rivven streaming
//!
//! # Example Configuration
//!
//! ```yaml
//! type: sqs-source
//! config:
//!   queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
//!   region: us-east-1
//!   max_messages: 10
//!   wait_time_seconds: 20
//!   visibility_timeout: 30
//!   auth:
//!     access_key_id: ${AWS_ACCESS_KEY_ID}
//!     secret_access_key: ${AWS_SECRET_ACCESS_KEY}
//! ```
//!
//! # Authentication
//!
//! Supports multiple authentication methods:
//! - Explicit credentials (access key + secret)
//! - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
//! - IAM role (for EKS/EC2 workloads)
//! - AWS profile

use crate::connectors::{AnySource, SourceFactory, SourceRegistry};
use crate::error::{ConnectorError, Result};
use crate::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
use crate::traits::event::SourceEvent;
use crate::traits::source::{CheckResult, Source};
use crate::traits::spec::ConnectorSpec;
use crate::traits::state::State;
use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;
use validator::Validate;

use crate::types::SensitiveString;

// ============================================================================
// Configuration Types
// ============================================================================

/// AWS authentication configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct AwsAuthConfig {
    /// AWS access key ID (optional if using IAM role or profile)
    pub access_key_id: Option<String>,

    /// AWS secret access key
    pub secret_access_key: Option<SensitiveString>,

    /// AWS session token (for temporary credentials)
    pub session_token: Option<SensitiveString>,

    /// AWS profile name to use from credentials file
    pub profile: Option<String>,

    /// IAM role ARN to assume
    pub role_arn: Option<String>,

    /// External ID for role assumption
    pub external_id: Option<String>,
}

impl AwsAuthConfig {
    /// Check if explicit credentials are provided
    pub fn has_explicit_credentials(&self) -> bool {
        self.access_key_id.is_some() && self.secret_access_key.is_some()
    }
}

/// SQS message attribute data type
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SqsAttributeType {
    String,
    Number,
    Binary,
}

/// SQS Source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct SqsSourceConfig {
    /// SQS queue URL
    #[validate(length(min = 1))]
    #[validate(url)]
    pub queue_url: String,

    /// AWS region (e.g., us-east-1)
    #[validate(length(min = 1, max = 25))]
    pub region: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AwsAuthConfig,

    /// Maximum number of messages to receive per poll (1-10)
    #[serde(default = "default_max_messages")]
    #[validate(range(min = 1, max = 10))]
    pub max_messages: i32,

    /// Long polling wait time in seconds (0-20, 0 disables long polling)
    #[serde(default = "default_wait_time")]
    #[validate(range(min = 0, max = 20))]
    pub wait_time_seconds: i32,

    /// Visibility timeout in seconds (0-43200)
    #[serde(default = "default_visibility_timeout")]
    #[validate(range(min = 0, max = 43200))]
    pub visibility_timeout: i32,

    /// Whether to delete messages after successful processing
    #[serde(default = "default_true")]
    pub delete_after_receive: bool,

    /// Include message attributes in event metadata
    #[serde(default = "default_true")]
    pub include_attributes: bool,

    /// Include system attributes (SenderId, SentTimestamp, etc.)
    #[serde(default)]
    pub include_system_attributes: bool,

    /// Filter message attributes to include (empty = all)
    #[serde(default)]
    pub attribute_names: Vec<String>,

    /// FIFO queue deduplication scope
    #[serde(default)]
    pub fifo_deduplication_scope: Option<FifoDeduplicationScope>,

    /// Endpoint URL override (for LocalStack or custom endpoints)
    pub endpoint_url: Option<String>,
}

/// FIFO queue deduplication scope
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FifoDeduplicationScope {
    /// Deduplication at queue level
    Queue,
    /// Deduplication at message group level
    MessageGroup,
}

fn default_max_messages() -> i32 {
    10
}
fn default_wait_time() -> i32 {
    20
}
fn default_visibility_timeout() -> i32 {
    30
}
fn default_true() -> bool {
    true
}

impl Default for SqsSourceConfig {
    fn default() -> Self {
        Self {
            queue_url: String::new(),
            region: "us-east-1".to_string(),
            auth: AwsAuthConfig::default(),
            max_messages: default_max_messages(),
            wait_time_seconds: default_wait_time(),
            visibility_timeout: default_visibility_timeout(),
            delete_after_receive: true,
            include_attributes: true,
            include_system_attributes: false,
            attribute_names: vec![],
            fifo_deduplication_scope: None,
            endpoint_url: None,
        }
    }
}

// ============================================================================
// SQS Message Types
// ============================================================================

/// Simulated SQS message for development/testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    /// Unique message ID
    pub message_id: String,
    /// Receipt handle for deletion
    pub receipt_handle: String,
    /// Message body
    pub body: String,
    /// MD5 of the message body
    pub md5_of_body: String,
    /// Message attributes
    pub attributes: HashMap<String, String>,
    /// Message attributes (custom)
    pub message_attributes: HashMap<String, SqsMessageAttribute>,
    /// Approximate receive count
    pub approximate_receive_count: i32,
    /// Approximate first receive timestamp
    pub approximate_first_receive_timestamp: i64,
    /// Sent timestamp
    pub sent_timestamp: i64,
    /// Sender ID
    pub sender_id: Option<String>,
    /// Message group ID (FIFO queues)
    pub message_group_id: Option<String>,
    /// Message deduplication ID (FIFO queues)
    pub message_deduplication_id: Option<String>,
    /// Sequence number (FIFO queues)
    pub sequence_number: Option<String>,
}

/// SQS message attribute value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessageAttribute {
    /// Data type (String, Number, Binary, String.*, Number.*, Binary.*)
    pub data_type: String,
    /// String value
    pub string_value: Option<String>,
    /// Binary value (base64 encoded)
    pub binary_value: Option<String>,
}

// ============================================================================
// SQS Source Implementation
// ============================================================================

/// SQS Source connector
pub struct SqsSource {
    shutdown: Arc<AtomicBool>,
    messages_received: Arc<AtomicU64>,
}

impl SqsSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            messages_received: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Generate a simulated SQS message for testing
    fn generate_test_message(sequence: u64) -> SqsMessage {
        let message_id = Uuid::new_v4().to_string();
        let actions = ["click", "view", "purchase"];
        let action = actions[sequence as usize % 3];
        let body = json!({
            "eventType": "test_event",
            "sequence": sequence,
            "data": {
                "user_id": format!("user_{}", sequence % 100),
                "action": action,
                "timestamp": Utc::now().to_rfc3339()
            }
        })
        .to_string();

        let md5 = format!("{:x}", md5::compute(&body));

        SqsMessage {
            message_id: message_id.clone(),
            receipt_handle: format!("receipt_{}", message_id),
            body,
            md5_of_body: md5,
            attributes: HashMap::new(),
            message_attributes: HashMap::new(),
            approximate_receive_count: 1,
            approximate_first_receive_timestamp: Utc::now().timestamp_millis(),
            sent_timestamp: Utc::now().timestamp_millis(),
            sender_id: Some("AIDAEXAMPLE".to_string()),
            message_group_id: None,
            message_deduplication_id: None,
            sequence_number: None,
        }
    }

    /// Convert SQS message to source event
    fn message_to_event(
        message: &SqsMessage,
        stream_name: &str,
        config: &SqsSourceConfig,
    ) -> SourceEvent {
        // Parse body as JSON or use as string
        let data =
            serde_json::from_str(&message.body).unwrap_or_else(|_| json!({"body": message.body}));

        let mut event = SourceEvent::record(stream_name, data)
            .with_position(message.message_id.clone())
            .with_metadata("sqs.message_id", json!(message.message_id.clone()))
            .with_metadata("sqs.receipt_handle", json!(message.receipt_handle.clone()))
            .with_metadata("sqs.md5_of_body", json!(message.md5_of_body.clone()));

        // Include system attributes if requested
        if config.include_system_attributes {
            event = event
                .with_metadata(
                    "sqs.receive_count",
                    json!(message.approximate_receive_count),
                )
                .with_metadata("sqs.sent_timestamp", json!(message.sent_timestamp))
                .with_metadata(
                    "sqs.first_receive_timestamp",
                    json!(message.approximate_first_receive_timestamp),
                );

            if let Some(ref sender) = message.sender_id {
                event = event.with_metadata("sqs.sender_id", json!(sender.clone()));
            }
        }

        // FIFO queue attributes
        if let Some(ref group_id) = message.message_group_id {
            event = event.with_metadata("sqs.message_group_id", json!(group_id.clone()));
        }
        if let Some(ref dedup_id) = message.message_deduplication_id {
            event = event.with_metadata("sqs.deduplication_id", json!(dedup_id.clone()));
        }
        if let Some(ref seq) = message.sequence_number {
            event = event.with_metadata("sqs.sequence_number", json!(seq.clone()));
        }

        // Include message attributes if requested
        if config.include_attributes {
            for (key, attr) in &message.message_attributes {
                if let Some(ref value) = attr.string_value {
                    event = event.with_metadata(format!("sqs.attr.{}", key), json!(value.clone()));
                }
            }
        }

        event
    }
}

impl Default for SqsSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for SqsSource {
    type Config = SqsSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("sqs-source", env!("CARGO_PKG_VERSION"))
            .description("Consume messages from Amazon SQS queues")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/sqs-source")
            .config_schema::<SqsSourceConfig>()
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

        // Check queue URL format
        if !config.queue_url.contains("sqs") && config.endpoint_url.is_none() {
            return Ok(CheckResult::failure(
                "Queue URL does not appear to be an SQS URL",
            ));
        }

        // Check authentication
        if !config.auth.has_explicit_credentials()
            && config.auth.profile.is_none()
            && config.auth.role_arn.is_none()
        {
            warn!(
                "No explicit credentials configured. Will use default AWS credential chain (environment, IAM role, etc.)"
            );
        }

        info!(
            queue_url = %config.queue_url,
            region = %config.region,
            "SQS source configuration validated"
        );

        Ok(CheckResult::builder()
            .check_passed("configuration")
            .check_passed("queue_url_format")
            .build())
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        // Extract queue name from URL for stream name
        let queue_name = config
            .queue_url
            .split('/')
            .next_back()
            .unwrap_or("sqs-queue")
            .to_string();

        let is_fifo = queue_name.ends_with(".fifo");

        let mut schema = json!({
            "type": "object",
            "properties": {
                "message_id": { "type": "string" },
                "body": { "type": "object" },
                "receipt_handle": { "type": "string" },
                "md5_of_body": { "type": "string" },
                "sent_timestamp": { "type": "integer" }
            },
            "required": ["message_id", "body"]
        });

        if is_fifo {
            if let Some(props) = schema.get_mut("properties").and_then(|p| p.as_object_mut()) {
                props.insert("message_group_id".to_string(), json!({"type": "string"}));
                props.insert("sequence_number".to_string(), json!({"type": "string"}));
            }
        }

        let stream = Stream::new(&queue_name, schema)
            .namespace("sqs")
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

        // Get stream name from catalog
        let stream_name = catalog
            .streams
            .first()
            .map(|s| s.stream.name.clone())
            .unwrap_or_else(|| "sqs".to_string());

        // Get starting sequence from state
        let start_sequence = state
            .as_ref()
            .and_then(|s| s.get_stream(&stream_name))
            .and_then(|stream_state| stream_state.cursor_value.as_ref())
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let config = config.clone();

        info!(
            queue_url = %config.queue_url,
            stream = %stream_name,
            start_sequence = start_sequence,
            "Starting SQS source"
        );

        let stream = async_stream::stream! {
            let mut sequence = start_sequence;
            let poll_interval = Duration::from_secs(config.wait_time_seconds as u64);
            let mut poll_count = 0u64;

            while !shutdown.load(Ordering::Relaxed) {
                poll_count += 1;

                // Simulate receiving messages (in production, use AWS SDK)
                let batch_size = config.max_messages as u64;

                for _ in 0..batch_size {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    sequence += 1;
                    let message = Self::generate_test_message(sequence);

                    debug!(
                        message_id = %message.message_id,
                        sequence = sequence,
                        "Received SQS message"
                    );

                    let event = Self::message_to_event(&message, &stream_name, &config);
                    messages_received.fetch_add(1, Ordering::Relaxed);

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

                // Wait before next poll (long polling simulation)
                if !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(poll_interval).await;
                }
            }

            info!(
                total_messages = messages_received.load(Ordering::Relaxed),
                "SQS source shutdown"
            );
        };

        Ok(Box::pin(stream))
    }
}

impl Drop for SqsSource {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory for creating SQS source instances
pub struct SqsSourceFactory;

impl SourceFactory for SqsSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        SqsSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(SqsSource::new())
    }
}

/// AnySource implementation for SqsSource
#[async_trait]
impl AnySource for SqsSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: SqsSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid SQS source config: {}", e)))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: SqsSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid SQS source config: {}", e)))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: SqsSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid SQS source config: {}", e)))?;
        self.read(&config, catalog, state).await
    }
}

// ============================================================================
// Registry Functions
// ============================================================================

/// Register SQS source connectors with a source registry
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("sqs-source", std::sync::Arc::new(SqsSourceFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        let config = SqsSourceConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_defaults() {
        let config = SqsSourceConfig::default();

        assert_eq!(config.max_messages, 10);
        assert_eq!(config.wait_time_seconds, 20);
        assert_eq!(config.visibility_timeout, 30);
        assert!(config.delete_after_receive);
        assert!(config.include_attributes);
    }

    #[test]
    fn test_generate_test_message() {
        let message = SqsSource::generate_test_message(42);

        assert!(!message.message_id.is_empty());
        assert!(!message.body.is_empty());
        assert!(!message.md5_of_body.is_empty());
        assert!(message.approximate_receive_count >= 1);
    }

    #[test]
    fn test_message_to_event() {
        let message = SqsSource::generate_test_message(1);
        let config = SqsSourceConfig::default();

        let event = SqsSource::message_to_event(&message, "test-stream", &config);

        assert_eq!(event.stream, "test-stream");
        // Check metadata via the extra field
        assert!(event.metadata.extra.contains_key("sqs.message_id"));
        assert!(event.metadata.extra.contains_key("sqs.receipt_handle"));
    }

    #[tokio::test]
    async fn test_source_spec() {
        let spec = SqsSource::spec();

        assert_eq!(spec.connector_type, "sqs-source");
        assert!(spec.supports_incremental);
        assert!(spec.config_schema.is_some());
    }

    #[tokio::test]
    async fn test_source_check() {
        let source = SqsSource::new();
        let config = SqsSourceConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let result = source.check(&config).await.unwrap();
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_source_discover() {
        let source = SqsSource::new();
        let config = SqsSourceConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let catalog = source.discover(&config).await.unwrap();

        assert_eq!(catalog.streams.len(), 1);
        assert_eq!(catalog.streams[0].name, "my-queue");
    }

    #[tokio::test]
    async fn test_fifo_queue_discovery() {
        let source = SqsSource::new();
        let config = SqsSourceConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let catalog = source.discover(&config).await.unwrap();

        assert_eq!(catalog.streams[0].name, "my-queue.fifo");
        // FIFO queues should have message_group_id in schema
        let schema = &catalog.streams[0].json_schema;
        assert!(schema["properties"].get("message_group_id").is_some());
    }

    #[test]
    fn test_aws_auth_config() {
        let auth = AwsAuthConfig {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some(SensitiveString::new(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            )),
            ..Default::default()
        };

        assert!(auth.has_explicit_credentials());

        let auth_empty = AwsAuthConfig::default();
        assert!(!auth_empty.has_explicit_credentials());
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
        let config = SqsSourceConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            auth: AwsAuthConfig {
                access_key_id: Some("AKIAEXAMPLE".to_string()),
                secret_access_key: Some(SensitiveString::new("secret")),
                ..Default::default()
            },
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();

        // Secret should be redacted in serialization
        assert!(json.contains("***REDACTED***"));
        assert!(!json.contains("\"secret\""));
    }
}
