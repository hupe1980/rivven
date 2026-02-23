//! Amazon SQS Source Connector for Rivven Connect
//!
//! Production-grade AWS SQS integration using the official AWS SDK:
//! - **SqsSource**: Consume messages from SQS queues, stream to Rivven
//!
//! # Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | AWS SDK v1 | Official `aws-sdk-sqs` for real SQS operations |
//! | Lock-free metrics | Atomic counters with zero contention |
//! | Batch processing | Receive up to 10 messages per poll |
//! | Batch deletion | `DeleteMessageBatch` for efficient cleanup |
//! | Visibility timeout | Configurable per-queue message hide time |
//! | Long polling | Native SQS long-poll (up to 20s) |
//! | FIFO support | Message group ordering + deduplication |
//! | DLQ awareness | Tracks approximate receive count for redrive |
//! | Exponential backoff | Configurable retry with multiplier |
//! | Prometheus export | `to_prometheus_format()` for scraping |
//! | JSON serialization | Serde derives on MetricsSnapshot |
//! | IAM / Profile / STS | Full AWS credential chain support |
//!
//! # Use Cases
//!
//! - Ingest events from AWS services via SQS
//! - Process async job queues with at-least-once delivery
//! - Bridge AWS workloads with Rivven streaming
//! - Fan-out from SNS → SQS → Rivven pipelines
//!
//! # Authentication
//!
//! Supports the full AWS credential chain:
//! - Explicit credentials (access key + secret + optional session token)
//! - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
//! - IAM instance profile (EC2/ECS/EKS)
//! - AWS SSO / SSO-OIDC
//! - STS `AssumeRole` with optional external ID
//! - Named profiles from `~/.aws/credentials`
//!
//! Enable with the `sqs` feature:
//!
//! ```toml
//! rivven-connect = { version = "0.0.20", features = ["sqs"] }
//! ```
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
//!   delete_after_receive: true
//!   auth:
//!     access_key_id: ${AWS_ACCESS_KEY_ID}
//!     secret_access_key: ${AWS_SECRET_ACCESS_KEY}
//! ```
//!
//! # Observability
//!
//! ```rust,ignore
//! let source = SqsSource::new();
//! let snapshot = source.metrics().snapshot();
//! println!("Messages received: {}", snapshot.messages_received);
//! println!("Avg poll latency: {:.2}ms", snapshot.avg_poll_latency_ms());
//!
//! // Export to Prometheus
//! let prometheus_output = snapshot.to_prometheus_format("myapp");
//! ```

use crate::connectors::{AnySource, SourceFactory, SourceRegistry};
use crate::error::{ConnectorError, Result};
use crate::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
use crate::traits::event::SourceEvent;
use crate::traits::source::{CheckResult, Source};
use crate::traits::spec::ConnectorSpec;
use crate::traits::state::State;
use crate::types::SensitiveString;
use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use validator::Validate;

#[cfg(any(not(feature = "sqs"), test))]
use uuid::Uuid;

#[cfg(feature = "sqs")]
use aws_sdk_sqs::{
    config::{Credentials, Region},
    types::QueueAttributeName,
    Client as SqsClient,
};

// ============================================================================
// SQS Source Metrics
// ============================================================================

/// Point-in-time snapshot of SQS source metrics.
///
/// Captures all metrics at a specific moment for export, alerting, or dashboards.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqsSourceMetricsSnapshot {
    /// Total messages successfully received
    pub messages_received: u64,
    /// Total bytes received (body size)
    pub bytes_received: u64,
    /// Total messages deleted after processing
    pub messages_deleted: u64,
    /// Total delete failures (message may be redelivered)
    pub delete_failures: u64,
    /// Total receive polls executed
    pub polls: u64,
    /// Total empty polls (no messages returned)
    pub empty_polls: u64,
    /// Total errors (API failures, timeouts, etc.)
    pub errors: u64,
    /// Cumulative poll latency in microseconds
    pub poll_latency_us: u64,
    /// Minimum batch size received — 0 if no batches
    pub batch_size_min: u64,
    /// Maximum batch size received
    pub batch_size_max: u64,
    /// Sum of all batch sizes (for average calculation)
    pub batch_size_sum: u64,
}

impl SqsSourceMetricsSnapshot {
    /// Get average poll latency in milliseconds.
    #[inline]
    pub fn avg_poll_latency_ms(&self) -> f64 {
        if self.polls == 0 {
            return 0.0;
        }
        (self.poll_latency_us as f64 / self.polls as f64) / 1000.0
    }

    /// Get average batch size per non-empty poll.
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        let non_empty = self.polls.saturating_sub(self.empty_polls);
        if non_empty == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / non_empty as f64
    }

    /// Get message throughput in messages per second.
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.messages_received as f64 / elapsed_secs
    }

    /// Get bytes throughput in bytes per second.
    #[inline]
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_received as f64 / elapsed_secs
    }

    /// Get error rate as percentage of total polls + errors.
    #[inline]
    pub fn error_rate_percent(&self) -> f64 {
        let total = self.polls + self.errors;
        if total == 0 {
            return 0.0;
        }
        (self.errors as f64 / total as f64) * 100.0
    }

    /// Get empty poll rate as percentage of total polls.
    #[inline]
    pub fn empty_poll_rate_percent(&self) -> f64 {
        if self.polls == 0 {
            return 0.0;
        }
        (self.empty_polls as f64 / self.polls as f64) * 100.0
    }

    /// Export metrics in Prometheus text format.
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut out = String::with_capacity(2048);

        macro_rules! prom {
            ($name:expr, $type:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP {}_{} {}\n# TYPE {}_{} {}\n{}_{} {}\n",
                    prefix, $name, $help, prefix, $name, $type, prefix, $name, $val
                ));
            };
        }

        prom!(
            "sqs_source_messages_received_total",
            "counter",
            "Total messages received from SQS",
            self.messages_received
        );
        prom!(
            "sqs_source_bytes_received_total",
            "counter",
            "Total bytes received from SQS",
            self.bytes_received
        );
        prom!(
            "sqs_source_messages_deleted_total",
            "counter",
            "Total messages deleted from SQS",
            self.messages_deleted
        );
        prom!(
            "sqs_source_delete_failures_total",
            "counter",
            "Total delete failures",
            self.delete_failures
        );
        prom!(
            "sqs_source_polls_total",
            "counter",
            "Total receive polls",
            self.polls
        );
        prom!(
            "sqs_source_empty_polls_total",
            "counter",
            "Total empty polls (no messages)",
            self.empty_polls
        );
        prom!(
            "sqs_source_errors_total",
            "counter",
            "Total errors",
            self.errors
        );
        prom!(
            "sqs_source_poll_latency_avg_ms",
            "gauge",
            "Average poll latency in milliseconds",
            format!("{:.3}", self.avg_poll_latency_ms())
        );

        out
    }
}

/// Lock-free metrics for SQS source connector.
///
/// All counters use `AtomicU64` with `Ordering::Relaxed` for maximum performance
/// on the hot path. Thread-safe without locks.
#[derive(Debug, Default)]
pub struct SqsSourceMetrics {
    messages_received: AtomicU64,
    bytes_received: AtomicU64,
    messages_deleted: AtomicU64,
    delete_failures: AtomicU64,
    polls: AtomicU64,
    empty_polls: AtomicU64,
    errors: AtomicU64,
    poll_latency_us: AtomicU64,
    batch_size_min: AtomicU64,
    batch_size_max: AtomicU64,
    batch_size_sum: AtomicU64,
}

impl SqsSourceMetrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        Self {
            batch_size_min: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    /// Record a batch size for min/max/avg tracking.
    /// Uses lock-free CAS operations for thread-safe updates.
    #[inline(always)]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);

        // Update min using CAS loop
        let mut current = self.batch_size_min.load(Ordering::Relaxed);
        while size < current {
            match self.batch_size_min.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }

        // Update max using CAS loop
        let mut current = self.batch_size_max.load(Ordering::Relaxed);
        while size > current {
            match self.batch_size_max.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    #[inline]
    pub fn snapshot(&self) -> SqsSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        SqsSourceMetricsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            messages_deleted: self.messages_deleted.load(Ordering::Relaxed),
            delete_failures: self.delete_failures.load(Ordering::Relaxed),
            polls: self.polls.load(Ordering::Relaxed),
            empty_polls: self.empty_polls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            poll_latency_us: self.poll_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.messages_received.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.messages_deleted.store(0, Ordering::Relaxed);
        self.delete_failures.store(0, Ordering::Relaxed);
        self.polls.store(0, Ordering::Relaxed);
        self.empty_polls.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.poll_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    pub fn snapshot_and_reset(&self) -> SqsSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        SqsSourceMetricsSnapshot {
            messages_received: self.messages_received.swap(0, Ordering::Relaxed),
            bytes_received: self.bytes_received.swap(0, Ordering::Relaxed),
            messages_deleted: self.messages_deleted.swap(0, Ordering::Relaxed),
            delete_failures: self.delete_failures.swap(0, Ordering::Relaxed),
            polls: self.polls.swap(0, Ordering::Relaxed),
            empty_polls: self.empty_polls.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            poll_latency_us: self.poll_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Configuration Types
// ============================================================================

/// AWS authentication configuration.
///
/// Supports the full AWS credential chain — if no fields are set, the connector
/// falls back to the default provider chain (env vars → profile → IMDS/EKS).
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct AwsAuthConfig {
    /// AWS access key ID (optional if using IAM role or profile)
    pub access_key_id: Option<String>,

    /// AWS secret access key
    pub secret_access_key: Option<SensitiveString>,

    /// AWS session token (for temporary credentials from STS)
    pub session_token: Option<SensitiveString>,

    /// AWS profile name to use from `~/.aws/credentials`
    pub profile: Option<String>,

    /// IAM role ARN to assume via STS
    pub role_arn: Option<String>,

    /// External ID for cross-account role assumption
    pub external_id: Option<String>,
}

impl AwsAuthConfig {
    /// Check if explicit credentials are provided.
    pub fn has_explicit_credentials(&self) -> bool {
        self.access_key_id.is_some() && self.secret_access_key.is_some()
    }
}

/// FIFO queue deduplication scope.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FifoDeduplicationScope {
    /// Deduplication at queue level
    Queue,
    /// Deduplication at message group level
    MessageGroup,
}

/// SQS Source configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct SqsSourceConfig {
    /// SQS queue URL (e.g., `https://sqs.us-east-1.amazonaws.com/123456789/my-queue`)
    #[validate(length(min = 1))]
    #[validate(url)]
    pub queue_url: String,

    /// AWS region (e.g., `us-east-1`)
    #[validate(length(min = 1, max = 25))]
    pub region: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AwsAuthConfig,

    /// Maximum number of messages to receive per poll (1–10)
    #[serde(default = "default_max_messages")]
    #[validate(range(min = 1, max = 10))]
    pub max_messages: i32,

    /// Long polling wait time in seconds (0–20, 0 disables long polling)
    #[serde(default = "default_wait_time")]
    #[validate(range(min = 0, max = 20))]
    pub wait_time_seconds: i32,

    /// Visibility timeout in seconds (0–43200). Time a message is hidden from
    /// other consumers after being received.
    #[serde(default = "default_visibility_timeout")]
    #[validate(range(min = 0, max = 43200))]
    pub visibility_timeout: i32,

    /// Whether to delete messages after successful processing (default: true).
    /// Disable for peek/inspect workloads.
    #[serde(default = "default_true")]
    pub delete_after_receive: bool,

    /// Include message attributes in event metadata (default: true)
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

    /// Endpoint URL override (for LocalStack, ElasticMQ, or other SQS-compatible services)
    pub endpoint_url: Option<String>,

    /// Initial retry delay in milliseconds on transient errors (default: 200)
    #[serde(default = "default_retry_initial_ms")]
    #[validate(range(min = 10, max = 60000))]
    pub retry_initial_ms: u64,

    /// Maximum retry delay in milliseconds (default: 30000)
    #[serde(default = "default_retry_max_ms")]
    #[validate(range(min = 100, max = 300000))]
    pub retry_max_ms: u64,

    /// Backoff multiplier for retries (default: 2.0)
    #[serde(default = "default_retry_multiplier")]
    #[validate(range(min = 1.0, max = 10.0))]
    pub retry_multiplier: f64,

    /// State checkpoint interval — emit state event every N polls (default: 10)
    #[serde(default = "default_checkpoint_interval")]
    #[validate(range(min = 1, max = 10000))]
    pub checkpoint_interval: u64,
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
fn default_retry_initial_ms() -> u64 {
    200
}
fn default_retry_max_ms() -> u64 {
    30_000
}
fn default_retry_multiplier() -> f64 {
    2.0
}
fn default_checkpoint_interval() -> u64 {
    10
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
            retry_initial_ms: default_retry_initial_ms(),
            retry_max_ms: default_retry_max_ms(),
            retry_multiplier: default_retry_multiplier(),
            checkpoint_interval: default_checkpoint_interval(),
        }
    }
}

// ============================================================================
// SQS Source Implementation
// ============================================================================

/// SQS Source connector — consumes messages from an Amazon SQS queue.
///
/// When the `sqs` feature is enabled, uses the official `aws-sdk-sqs` client.
/// Falls back to simulation mode (test message generation) when compiled
/// without the feature — useful for integration testing.
pub struct SqsSource {
    shutdown: Arc<AtomicBool>,
    metrics: Arc<SqsSourceMetrics>,
}

impl SqsSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(SqsSourceMetrics::new()),
        }
    }

    /// Get metrics for this source.
    #[inline]
    pub fn metrics(&self) -> &SqsSourceMetrics {
        &self.metrics
    }

    /// Signal the source to shut down gracefully.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if the source is shutting down.
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Extract queue name from a full SQS queue URL.
    fn queue_name_from_url(queue_url: &str) -> &str {
        queue_url.rsplit('/').next().unwrap_or("sqs-queue")
    }

    /// Build an SQS client from config.
    #[cfg(feature = "sqs")]
    async fn build_client(config: &SqsSourceConfig) -> Result<SqsClient> {
        use aws_sdk_sqs::config::Builder as SqsConfigBuilder;

        let region = Region::new(config.region.clone());
        let mut builder = SqsConfigBuilder::new()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(region);

        // Explicit credentials take precedence
        if config.auth.has_explicit_credentials() {
            let access_key = config.auth.access_key_id.clone().unwrap_or_default();
            let secret_key = config
                .auth
                .secret_access_key
                .as_ref()
                .map(|s| s.expose_secret().to_string())
                .unwrap_or_default();
            let session_token = config
                .auth
                .session_token
                .as_ref()
                .map(|s| s.expose_secret().to_string());

            let credentials =
                Credentials::new(access_key, secret_key, session_token, None, "rivven-sqs");
            builder = builder.credentials_provider(credentials);
        } else {
            // Fall back to default credential chain (env vars, profile, IMDS)
            let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new(config.region.clone()))
                .load()
                .await;
            let creds_provider = aws_config
                .credentials_provider()
                .ok_or_else(|| {
                    ConnectorError::Config(
                        "No AWS credentials found. Set access_key_id/secret_access_key, \
                         configure AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars, \
                         or ensure IAM role is available."
                            .into(),
                    )
                })?
                .clone();
            builder = builder.credentials_provider(creds_provider);
        }

        // Custom endpoint (LocalStack, ElasticMQ, etc.)
        if let Some(ref endpoint) = config.endpoint_url {
            builder = builder.endpoint_url(endpoint);
        }

        Ok(SqsClient::from_conf(builder.build()))
    }

    /// Convert an AWS SDK SQS Message to a SourceEvent.
    #[cfg(feature = "sqs")]
    fn sdk_message_to_event(
        message: &aws_sdk_sqs::types::Message,
        stream_name: &str,
        config: &SqsSourceConfig,
        sequence: u64,
    ) -> SourceEvent {
        let body = message.body().unwrap_or("");
        let data = serde_json::from_str(body).unwrap_or_else(|_| json!({"body": body}));

        let message_id = message.message_id().unwrap_or("unknown").to_string();

        let mut event = SourceEvent::record(stream_name, data)
            .with_position(format!("{}:{}", message_id, sequence))
            .with_metadata("sqs.message_id", json!(message_id))
            .with_metadata(
                "sqs.receipt_handle",
                json!(message.receipt_handle().unwrap_or("")),
            );

        // System attributes
        if config.include_system_attributes {
            if let Some(attrs) = message.attributes() {
                for (key, value) in attrs {
                    event = event.with_metadata(format!("sqs.sys.{}", key.as_str()), json!(value));
                }
            }
        }

        // Message attributes
        if config.include_attributes {
            if let Some(msg_attrs) = message.message_attributes() {
                for (key, attr) in msg_attrs {
                    if let Some(sv) = attr.string_value() {
                        event = event.with_metadata(format!("sqs.attr.{}", key), json!(sv));
                    }
                }
            }
        }

        event
    }

    /// Generate a simulated SQS message for fallback/testing mode.
    #[cfg(any(not(feature = "sqs"), test))]
    fn generate_test_message(sequence: u64) -> (String, String, String) {
        let message_id = Uuid::new_v4().to_string();
        let actions = ["click", "view", "purchase"];
        let action = actions[sequence as usize % 3];
        let body = json!({
            "eventType": "test_event",
            "sequence": sequence,
            "data": {
                "user_id": format!("user_{}", sequence % 100),
                "action": action,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }
        })
        .to_string();
        let receipt = format!("receipt_{}", message_id);
        (message_id, body, receipt)
    }

    /// Convert a simulated test message to a SourceEvent (fallback mode).
    #[cfg(any(not(feature = "sqs"), test))]
    fn test_message_to_event(
        message_id: &str,
        body: &str,
        receipt: &str,
        stream_name: &str,
        config: &SqsSourceConfig,
        sequence: u64,
    ) -> SourceEvent {
        let data = serde_json::from_str(body).unwrap_or_else(|_| json!({"body": body}));

        let mut event = SourceEvent::record(stream_name, data)
            .with_position(format!("{}:{}", message_id, sequence))
            .with_metadata("sqs.message_id", json!(message_id))
            .with_metadata("sqs.receipt_handle", json!(receipt));

        if config.include_system_attributes {
            event = event
                .with_metadata("sqs.sys.ApproximateReceiveCount", json!("1"))
                .with_metadata(
                    "sqs.sys.SentTimestamp",
                    json!(chrono::Utc::now().timestamp_millis().to_string()),
                );
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
        // 1. Validate configuration schema
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking SQS connectivity to {}", config.queue_url);

        #[cfg(feature = "sqs")]
        {
            // 2. Build client and call GetQueueAttributes to verify connectivity + permissions
            let client = match Self::build_client(config).await {
                Ok(c) => c,
                Err(e) => {
                    return Ok(CheckResult::failure(format!(
                        "Failed to build SQS client: {}",
                        e
                    )))
                }
            };

            let attrs_result = tokio::time::timeout(Duration::from_secs(10), async {
                client
                    .get_queue_attributes()
                    .queue_url(&config.queue_url)
                    .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
                    .attribute_names(QueueAttributeName::QueueArn)
                    .send()
                    .await
            })
            .await;

            match attrs_result {
                Ok(Ok(resp)) => {
                    let empty_attrs = std::collections::HashMap::new();
                    let attrs = resp.attributes().unwrap_or(&empty_attrs);
                    let arn = attrs
                        .get(&QueueAttributeName::QueueArn)
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");
                    let approx_msgs = attrs
                        .get(&QueueAttributeName::ApproximateNumberOfMessages)
                        .map(|s| s.as_str())
                        .unwrap_or("0");

                    info!(
                        queue_arn = %arn,
                        approximate_messages = %approx_msgs,
                        "SQS queue accessible"
                    );

                    Ok(CheckResult::builder()
                        .check_passed("configuration")
                        .check_passed("aws_credentials")
                        .check_passed("queue_accessibility")
                        .build())
                }
                Ok(Err(e)) => {
                    let msg = format!("SQS API error: {}", e);
                    error!("{}", msg);
                    Ok(CheckResult::builder()
                        .check_passed("configuration")
                        .check_failed("queue_accessibility", &msg)
                        .build())
                }
                Err(_) => Ok(CheckResult::failure(format!(
                    "SQS connection timeout for queue: {}",
                    config.queue_url
                ))),
            }
        }

        #[cfg(not(feature = "sqs"))]
        {
            // Fallback: validate config only (no AWS SDK available)
            if !config.auth.has_explicit_credentials()
                && config.auth.profile.is_none()
                && config.auth.role_arn.is_none()
            {
                warn!(
                    "No explicit credentials configured. Will use default AWS credential \
                     chain (environment, IAM role, etc.)"
                );
            }

            Ok(CheckResult::builder()
                .check_passed("configuration")
                .check_passed("queue_url_format")
                .build())
        }
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        let queue_name = Self::queue_name_from_url(&config.queue_url).to_string();
        let is_fifo = queue_name.ends_with(".fifo");

        let mut schema = json!({
            "type": "object",
            "properties": {
                "message_id": { "type": "string" },
                "body": { "type": "object" },
                "receipt_handle": { "type": "string" },
                "sent_timestamp": { "type": "integer" },
                "approximate_receive_count": { "type": "integer" }
            },
            "required": ["message_id", "body"]
        });

        if is_fifo {
            if let Some(props) = schema.get_mut("properties").and_then(|p| p.as_object_mut()) {
                props.insert("message_group_id".into(), json!({"type": "string"}));
                props.insert("sequence_number".into(), json!({"type": "string"}));
                props.insert("message_deduplication_id".into(), json!({"type": "string"}));
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
        let metrics = Arc::clone(&self.metrics);

        let stream_name = catalog
            .streams
            .first()
            .map(|s| s.stream.name.clone())
            .unwrap_or_else(|| Self::queue_name_from_url(&config.queue_url).to_string());

        // Restore sequence from state
        let start_sequence = state
            .as_ref()
            .and_then(|s| s.get_stream(&stream_name))
            .and_then(|ss| ss.cursor_value.as_ref())
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

        #[cfg(feature = "sqs")]
        {
            let client = Self::build_client(&config).await?;

            let stream = async_stream::stream! {
                let mut sequence = start_sequence;
                let mut poll_count = 0u64;
                let mut current_backoff_ms = config.retry_initial_ms;
                let checkpoint_interval = config.checkpoint_interval;

                while !shutdown.load(Ordering::Relaxed) {
                    poll_count += 1;
                    let poll_start = std::time::Instant::now();

                    // Build receive request
                    let mut req = client
                        .receive_message()
                        .queue_url(&config.queue_url)
                        .max_number_of_messages(config.max_messages)
                        .wait_time_seconds(config.wait_time_seconds)
                        .visibility_timeout(config.visibility_timeout);

                    // Request all message attributes
                    if config.include_attributes {
                        if config.attribute_names.is_empty() {
                            req = req.message_attribute_names("All");
                        } else {
                            for attr in &config.attribute_names {
                                req = req.message_attribute_names(attr);
                            }
                        }
                    }

                    // Request system attributes
                    if config.include_system_attributes {
                        req = req.message_system_attribute_names(
                            aws_sdk_sqs::types::MessageSystemAttributeName::All,
                        );
                    }

                    match req.send().await {
                        Ok(output) => {
                            // Reset backoff on success
                            current_backoff_ms = config.retry_initial_ms;

                            let messages = output.messages();
                            let batch_len = messages.len() as u64;

                            // Update poll metrics
                            let poll_elapsed = poll_start.elapsed().as_micros() as u64;
                            metrics.polls.fetch_add(1, Ordering::Relaxed);
                            metrics.poll_latency_us.fetch_add(poll_elapsed, Ordering::Relaxed);

                            if batch_len == 0 {
                                metrics.empty_polls.fetch_add(1, Ordering::Relaxed);
                                debug!("Empty poll ({}ms)", poll_elapsed / 1000);
                                continue;
                            }

                            metrics.record_batch_size(batch_len);
                            debug!(batch_size = batch_len, "Received SQS batch");

                            // Collect receipt handles for batch deletion
                            let mut receipt_handles: Vec<(String, String)> =
                                Vec::with_capacity(messages.len());

                            for message in messages {
                                sequence += 1;
                                let body_len =
                                    message.body().map(|b| b.len()).unwrap_or(0) as u64;
                                metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                                metrics.bytes_received.fetch_add(body_len, Ordering::Relaxed);

                                let event = Self::sdk_message_to_event(
                                    message,
                                    &stream_name,
                                    &config,
                                    sequence,
                                );

                                if let (Some(id), Some(handle)) =
                                    (message.message_id(), message.receipt_handle())
                                {
                                    receipt_handles
                                        .push((id.to_string(), handle.to_string()));
                                }

                                yield Ok(event);
                            }

                            // Batch delete if configured
                            if config.delete_after_receive && !receipt_handles.is_empty() {
                                let entries: std::result::Result<Vec<_>, _> = receipt_handles
                                    .iter()
                                    .enumerate()
                                    .map(|(i, (_id, handle))| {
                                        aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                                            .id(format!("del_{}", i))
                                            .receipt_handle(handle)
                                            .build()
                                    })
                                    .collect();

                                let entries = match entries {
                                    Ok(e) => e,
                                    Err(e) => {
                                        warn!("Failed to build SQS delete entries: {}", e);
                                        continue;
                                    }
                                };

                                match client
                                    .delete_message_batch()
                                    .queue_url(&config.queue_url)
                                    .set_entries(Some(entries))
                                    .send()
                                    .await
                                {
                                    Ok(resp) => {
                                        let successful = resp.successful().len() as u64;
                                        let failed = resp.failed().len() as u64;
                                        metrics
                                            .messages_deleted
                                            .fetch_add(successful, Ordering::Relaxed);
                                        if failed > 0 {
                                            metrics
                                                .delete_failures
                                                .fetch_add(failed, Ordering::Relaxed);
                                            warn!(
                                                successful = successful,
                                                failed = failed,
                                                "Partial batch delete failure"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        let count = receipt_handles.len() as u64;
                                        metrics
                                            .delete_failures
                                            .fetch_add(count, Ordering::Relaxed);
                                        warn!("Batch delete failed ({}): {}", count, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            metrics.errors.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                backoff_ms = current_backoff_ms,
                                "SQS receive error: {}", e
                            );

                            // Exponential backoff
                            tokio::time::sleep(Duration::from_millis(current_backoff_ms)).await;
                            current_backoff_ms =
                                ((current_backoff_ms as f64 * config.retry_multiplier) as u64)
                                    .min(config.retry_max_ms);
                            continue;
                        }
                    }

                    // Emit state checkpoint periodically
                    if poll_count.is_multiple_of(checkpoint_interval) {
                        let state_event = SourceEvent::state(json!({
                            stream_name.clone(): {
                                "cursor_field": "sequence",
                                "cursor_value": sequence.to_string()
                            }
                        }));
                        yield Ok(state_event);
                    }
                }

                info!(
                    total_messages = metrics.messages_received.load(Ordering::Relaxed),
                    total_deleted = metrics.messages_deleted.load(Ordering::Relaxed),
                    "SQS source shutdown"
                );
            };

            Ok(Box::pin(stream))
        }

        #[cfg(not(feature = "sqs"))]
        {
            // Fallback simulation mode (no AWS SDK)
            let stream = async_stream::stream! {
                let mut sequence = start_sequence;
                let mut poll_count = 0u64;
                let poll_interval =
                    Duration::from_secs(config.wait_time_seconds.max(1) as u64);
                let batch_size = config.max_messages as u64;
                let checkpoint_interval = config.checkpoint_interval;

                while !shutdown.load(Ordering::Relaxed) {
                    poll_count += 1;
                    let poll_start = std::time::Instant::now();
                    metrics.polls.fetch_add(1, Ordering::Relaxed);

                    for _ in 0..batch_size {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }

                        sequence += 1;
                        let (message_id, body, receipt) =
                            Self::generate_test_message(sequence);
                        let body_len = body.len() as u64;

                        let event = Self::test_message_to_event(
                            &message_id,
                            &body,
                            &receipt,
                            &stream_name,
                            &config,
                            sequence,
                        );

                        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                        metrics.bytes_received.fetch_add(body_len, Ordering::Relaxed);

                        if config.delete_after_receive {
                            metrics.messages_deleted.fetch_add(1, Ordering::Relaxed);
                        }

                        yield Ok(event);
                    }

                    metrics.record_batch_size(batch_size);
                    let poll_elapsed = poll_start.elapsed().as_micros() as u64;
                    metrics
                        .poll_latency_us
                        .fetch_add(poll_elapsed, Ordering::Relaxed);

                    // Emit state checkpoint periodically
                    if poll_count.is_multiple_of(checkpoint_interval) {
                        let state_event = SourceEvent::state(json!({
                            stream_name.clone(): {
                                "cursor_field": "sequence",
                                "cursor_value": sequence.to_string()
                            }
                        }));
                        yield Ok(state_event);
                    }

                    if !shutdown.load(Ordering::Relaxed) {
                        tokio::time::sleep(poll_interval).await;
                    }
                }

                info!(
                    total_messages = metrics.messages_received.load(Ordering::Relaxed),
                    "SQS source shutdown (simulation)"
                );
            };

            Ok(Box::pin(stream))
        }
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

/// Factory for creating SQS source instances.
pub struct SqsSourceFactory;

impl SourceFactory for SqsSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        SqsSource::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySource>> {
        Ok(Box::new(SqsSource::new()))
    }
}

/// AnySource implementation for SqsSource — type-erased dispatch.
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
// SQS Sink Metrics
// ============================================================================

/// Point-in-time snapshot of SQS sink metrics.
///
/// Captures all metrics at a specific moment for export, alerting, or dashboards.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqsSinkMetricsSnapshot {
    /// Total messages successfully sent
    pub messages_sent: u64,
    /// Total bytes sent (body size)
    pub bytes_sent: u64,
    /// Total messages failed to send
    pub messages_failed: u64,
    /// Total send batches executed
    pub batches_sent: u64,
    /// Total partial batch failures (some messages in batch failed)
    pub partial_failures: u64,
    /// Total errors (API failures, timeouts, etc.)
    pub errors: u64,
    /// Total retries
    pub retries: u64,
    /// Cumulative send latency in microseconds
    pub send_latency_us: u64,
    /// Minimum batch size sent — 0 if no batches
    pub batch_size_min: u64,
    /// Maximum batch size sent
    pub batch_size_max: u64,
    /// Sum of all batch sizes (for average calculation)
    pub batch_size_sum: u64,
    /// Total oversized messages (exceeded 256KB limit)
    pub oversized_messages: u64,
    /// Total skipped messages (due to oversized behavior or circuit breaker)
    pub skipped_messages: u64,
    /// Total bytes saved by compression
    pub compression_savings_bytes: u64,
    /// Total circuit breaker trips
    pub circuit_breaker_trips: u64,
}

impl SqsSinkMetricsSnapshot {
    /// Get average send latency in milliseconds.
    #[inline]
    pub fn avg_send_latency_ms(&self) -> f64 {
        if self.batches_sent == 0 {
            return 0.0;
        }
        (self.send_latency_us as f64 / self.batches_sent as f64) / 1000.0
    }

    /// Get average batch size.
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        if self.batches_sent == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / self.batches_sent as f64
    }

    /// Get message throughput in messages per second.
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.messages_sent as f64 / elapsed_secs
    }

    /// Get bytes throughput in bytes per second.
    #[inline]
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_sent as f64 / elapsed_secs
    }

    /// Get success rate as percentage.
    #[inline]
    pub fn success_rate_percent(&self) -> f64 {
        let total = self.messages_sent + self.messages_failed;
        if total == 0 {
            return 100.0;
        }
        (self.messages_sent as f64 / total as f64) * 100.0
    }

    /// Get error rate as percentage of total batches + errors.
    #[inline]
    pub fn error_rate_percent(&self) -> f64 {
        let total = self.batches_sent + self.errors;
        if total == 0 {
            return 0.0;
        }
        (self.errors as f64 / total as f64) * 100.0
    }

    /// Get compression ratio (compressed size / original size).
    #[inline]
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_sent == 0 || self.compression_savings_bytes == 0 {
            return 1.0;
        }
        let original = self.bytes_sent + self.compression_savings_bytes;
        self.bytes_sent as f64 / original as f64
    }

    /// Export metrics in Prometheus text format.
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut out = String::with_capacity(3072);

        macro_rules! prom {
            ($name:expr, $type:expr, $help:expr, $val:expr) => {
                out.push_str(&format!(
                    "# HELP {}_{} {}\n# TYPE {}_{} {}\n{}_{} {}\n",
                    prefix, $name, $help, prefix, $name, $type, prefix, $name, $val
                ));
            };
        }

        prom!(
            "sqs_sink_messages_sent_total",
            "counter",
            "Total messages sent to SQS",
            self.messages_sent
        );
        prom!(
            "sqs_sink_bytes_sent_total",
            "counter",
            "Total bytes sent to SQS",
            self.bytes_sent
        );
        prom!(
            "sqs_sink_messages_failed_total",
            "counter",
            "Total messages failed to send",
            self.messages_failed
        );
        prom!(
            "sqs_sink_batches_sent_total",
            "counter",
            "Total send batches",
            self.batches_sent
        );
        prom!(
            "sqs_sink_partial_failures_total",
            "counter",
            "Total partial batch failures",
            self.partial_failures
        );
        prom!(
            "sqs_sink_errors_total",
            "counter",
            "Total errors",
            self.errors
        );
        prom!(
            "sqs_sink_retries_total",
            "counter",
            "Total retries",
            self.retries
        );
        prom!(
            "sqs_sink_oversized_messages_total",
            "counter",
            "Messages exceeding 256KB limit",
            self.oversized_messages
        );
        prom!(
            "sqs_sink_skipped_messages_total",
            "counter",
            "Messages skipped (oversized or circuit breaker)",
            self.skipped_messages
        );
        prom!(
            "sqs_sink_compression_savings_bytes_total",
            "counter",
            "Bytes saved by compression",
            self.compression_savings_bytes
        );
        prom!(
            "sqs_sink_circuit_breaker_trips_total",
            "counter",
            "Circuit breaker trip count",
            self.circuit_breaker_trips
        );
        prom!(
            "sqs_sink_send_latency_avg_ms",
            "gauge",
            "Average send latency in milliseconds",
            format!("{:.3}", self.avg_send_latency_ms())
        );
        prom!(
            "sqs_sink_success_rate",
            "gauge",
            "Message success rate",
            format!("{:.4}", self.success_rate_percent() / 100.0)
        );
        prom!(
            "sqs_sink_compression_ratio",
            "gauge",
            "Compression ratio (1.0 = no compression)",
            format!("{:.4}", self.compression_ratio())
        );

        out
    }
}

/// Lock-free metrics for SQS sink connector.
///
/// All counters use `AtomicU64` with `Ordering::Relaxed` for maximum performance
/// on the hot path. Thread-safe without locks.
#[derive(Debug, Default)]
pub struct SqsSinkMetrics {
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
    messages_failed: AtomicU64,
    batches_sent: AtomicU64,
    partial_failures: AtomicU64,
    errors: AtomicU64,
    retries: AtomicU64,
    send_latency_us: AtomicU64,
    batch_size_min: AtomicU64,
    batch_size_max: AtomicU64,
    batch_size_sum: AtomicU64,
    oversized_messages: AtomicU64,
    skipped_messages: AtomicU64,
    compression_savings_bytes: AtomicU64,
    circuit_breaker_trips: AtomicU64,
}

impl SqsSinkMetrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        Self {
            batch_size_min: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    /// Record a batch size for min/max/avg tracking.
    /// Uses lock-free CAS operations for thread-safe updates.
    #[inline(always)]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);

        // Update min using CAS loop
        let mut current = self.batch_size_min.load(Ordering::Relaxed);
        while size < current {
            match self.batch_size_min.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }

        // Update max using CAS loop
        let mut current = self.batch_size_max.load(Ordering::Relaxed);
        while size > current {
            match self.batch_size_max.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    #[inline]
    pub fn snapshot(&self) -> SqsSinkMetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        SqsSinkMetricsSnapshot {
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            messages_failed: self.messages_failed.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            partial_failures: self.partial_failures.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            send_latency_us: self.send_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
            oversized_messages: self.oversized_messages.load(Ordering::Relaxed),
            skipped_messages: self.skipped_messages.load(Ordering::Relaxed),
            compression_savings_bytes: self.compression_savings_bytes.load(Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.messages_sent.store(0, Ordering::Relaxed);
        self.bytes_sent.store(0, Ordering::Relaxed);
        self.messages_failed.store(0, Ordering::Relaxed);
        self.batches_sent.store(0, Ordering::Relaxed);
        self.partial_failures.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.retries.store(0, Ordering::Relaxed);
        self.send_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
        self.oversized_messages.store(0, Ordering::Relaxed);
        self.skipped_messages.store(0, Ordering::Relaxed);
        self.compression_savings_bytes.store(0, Ordering::Relaxed);
        self.circuit_breaker_trips.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    pub fn snapshot_and_reset(&self) -> SqsSinkMetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        SqsSinkMetricsSnapshot {
            messages_sent: self.messages_sent.swap(0, Ordering::Relaxed),
            bytes_sent: self.bytes_sent.swap(0, Ordering::Relaxed),
            messages_failed: self.messages_failed.swap(0, Ordering::Relaxed),
            batches_sent: self.batches_sent.swap(0, Ordering::Relaxed),
            partial_failures: self.partial_failures.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            retries: self.retries.swap(0, Ordering::Relaxed),
            send_latency_us: self.send_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
            oversized_messages: self.oversized_messages.swap(0, Ordering::Relaxed),
            skipped_messages: self.skipped_messages.swap(0, Ordering::Relaxed),
            compression_savings_bytes: self.compression_savings_bytes.swap(0, Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.swap(0, Ordering::Relaxed),
        }
    }
}

// ============================================================================
// SQS Sink Configuration
// ============================================================================

/// Message body format for SQS messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SqsBodyFormat {
    /// JSON serialization (default)
    #[default]
    Json,
    /// Raw string (event data must be a string)
    Raw,
    /// Base64-encoded binary
    Base64,
}

/// Compression codec for SQS message bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SqsCompression {
    /// No compression (default)
    #[default]
    None,
    /// Gzip compression (best for text/JSON)
    Gzip,
    /// Zstd compression (best ratio)
    Zstd,
}

/// Behavior when a message exceeds the 256KB SQS limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OversizedMessageBehavior {
    /// Fail the message (default) — increments oversized_messages counter
    #[default]
    Fail,
    /// Skip the message silently — increments skipped_messages counter
    Skip,
    /// Truncate to fit within limit (lossy)
    Truncate,
}

/// SQS Sink configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct SqsSinkConfig {
    /// SQS queue URL (e.g., `https://sqs.us-east-1.amazonaws.com/123456789/my-queue`)
    #[validate(length(min = 1))]
    #[validate(url)]
    pub queue_url: String,

    /// AWS region (e.g., `us-east-1`)
    #[validate(length(min = 1, max = 25))]
    pub region: String,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AwsAuthConfig,

    /// Batch size for SendMessageBatch (1–10, default: 10)
    #[serde(default = "default_sink_batch_size")]
    #[validate(range(min = 1, max = 10))]
    pub batch_size: usize,

    /// Maximum time to wait before flushing incomplete batch (milliseconds)
    #[serde(default = "default_batch_timeout_ms")]
    #[validate(range(min = 0, max = 60000))]
    pub batch_timeout_ms: u64,

    /// Message group ID for FIFO queues (required for .fifo queues)
    /// If not set, uses event key or a default group
    pub message_group_id: Option<String>,

    /// Field from event data to use as message group ID (for FIFO queues)
    pub message_group_id_field: Option<String>,

    /// Field from event data to use as deduplication ID (for FIFO queues)
    /// If not set and queue is FIFO, uses content-based deduplication
    pub deduplication_id_field: Option<String>,

    /// Enable content-based deduplication for FIFO queues.
    /// AWS will generate deduplication ID from message body hash.
    /// Only valid for FIFO queues with ContentBasedDeduplication enabled.
    #[serde(default)]
    pub content_based_deduplication: bool,

    /// Delay in seconds before message becomes visible (0–900)
    #[serde(default)]
    #[validate(range(min = 0, max = 900))]
    pub delay_seconds: i32,

    /// Include event metadata as SQS message attributes
    #[serde(default = "default_true")]
    pub include_metadata: bool,

    /// Custom message attributes to add to all messages
    #[serde(default)]
    pub custom_attributes: std::collections::HashMap<String, String>,

    /// Endpoint URL override (for LocalStack, ElasticMQ, or other SQS-compatible services)
    pub endpoint_url: Option<String>,

    /// Initial retry delay in milliseconds on transient errors (default: 200)
    #[serde(default = "default_retry_initial_ms")]
    #[validate(range(min = 10, max = 60000))]
    pub retry_initial_ms: u64,

    /// Maximum retry delay in milliseconds (default: 30000)
    #[serde(default = "default_retry_max_ms")]
    #[validate(range(min = 100, max = 300000))]
    pub retry_max_ms: u64,

    /// Backoff multiplier for retries (default: 2.0)
    #[serde(default = "default_retry_multiplier")]
    #[validate(range(min = 1.0, max = 10.0))]
    pub retry_multiplier: f64,

    /// Maximum number of retries before giving up (default: 3)
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_retries: u32,

    /// Message body format (json, raw, base64)
    #[serde(default)]
    pub body_format: SqsBodyFormat,

    /// Compression codec (none, gzip, zstd)
    #[serde(default)]
    pub compression: SqsCompression,

    /// Behavior when message exceeds 256KB limit
    #[serde(default)]
    pub oversized_behavior: OversizedMessageBehavior,

    /// Circuit breaker: consecutive failures before tripping (0 = disabled)
    #[serde(default)]
    #[validate(range(min = 0, max = 100))]
    pub circuit_breaker_threshold: u32,

    /// Circuit breaker: seconds to wait before attempting recovery
    #[serde(default = "default_circuit_breaker_recovery_secs")]
    #[validate(range(min = 1, max = 3600))]
    pub circuit_breaker_recovery_secs: u64,
}

fn default_circuit_breaker_recovery_secs() -> u64 {
    30
}

fn default_sink_batch_size() -> usize {
    10
}

fn default_batch_timeout_ms() -> u64 {
    1000
}

fn default_max_retries() -> u32 {
    3
}

impl Default for SqsSinkConfig {
    fn default() -> Self {
        Self {
            queue_url: String::new(),
            region: "us-east-1".to_string(),
            auth: AwsAuthConfig::default(),
            batch_size: default_sink_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            message_group_id: None,
            message_group_id_field: None,
            deduplication_id_field: None,
            content_based_deduplication: false,
            delay_seconds: 0,
            include_metadata: true,
            custom_attributes: std::collections::HashMap::new(),
            endpoint_url: None,
            retry_initial_ms: default_retry_initial_ms(),
            retry_max_ms: default_retry_max_ms(),
            retry_multiplier: default_retry_multiplier(),
            max_retries: default_max_retries(),
            body_format: SqsBodyFormat::default(),
            compression: SqsCompression::default(),
            oversized_behavior: OversizedMessageBehavior::default(),
            circuit_breaker_threshold: 0,
            circuit_breaker_recovery_secs: default_circuit_breaker_recovery_secs(),
        }
    }
}

// ============================================================================
// SQS Sink Implementation
// ============================================================================

use crate::connectors::{AnySink, SinkFactory, SinkRegistry};
use crate::traits::sink::{Sink, WriteResult};
use futures::StreamExt;

/// SQS Sink connector — produces messages to an Amazon SQS queue.
///
/// When the `sqs` feature is enabled, uses the official `aws-sdk-sqs` client.
/// Falls back to simulation mode (logs messages) when compiled without the feature.
pub struct SqsSink {
    metrics: Arc<SqsSinkMetrics>,
}

impl SqsSink {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(SqsSinkMetrics::new()),
        }
    }

    /// Get metrics for this sink.
    #[inline]
    pub fn metrics(&self) -> &SqsSinkMetrics {
        &self.metrics
    }

    /// Extract queue name from a full SQS queue URL.
    fn queue_name_from_url(queue_url: &str) -> &str {
        queue_url.rsplit('/').next().unwrap_or("sqs-queue")
    }

    /// Check if queue is a FIFO queue.
    fn is_fifo_queue(queue_url: &str) -> bool {
        Self::queue_name_from_url(queue_url).ends_with(".fifo")
    }

    /// Build an SQS client from config (reuses SqsSource logic).
    #[cfg(feature = "sqs")]
    async fn build_client(config: &SqsSinkConfig) -> Result<SqsClient> {
        use aws_sdk_sqs::config::Builder as SqsConfigBuilder;

        let region = Region::new(config.region.clone());
        let mut builder = SqsConfigBuilder::new()
            .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
            .region(region);

        // Explicit credentials take precedence
        if config.auth.has_explicit_credentials() {
            let access_key = config.auth.access_key_id.clone().unwrap_or_default();
            let secret_key = config
                .auth
                .secret_access_key
                .as_ref()
                .map(|s| s.expose_secret().to_string())
                .unwrap_or_default();
            let session_token = config
                .auth
                .session_token
                .as_ref()
                .map(|s| s.expose_secret().to_string());

            let credentials = Credentials::new(
                access_key,
                secret_key,
                session_token,
                None,
                "rivven-sqs-sink",
            );
            builder = builder.credentials_provider(credentials);
        } else {
            // Fall back to default credential chain (env vars, profile, IMDS)
            let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(Region::new(config.region.clone()))
                .load()
                .await;
            let creds_provider = aws_config
                .credentials_provider()
                .ok_or_else(|| {
                    ConnectorError::Config(
                        "No AWS credentials found. Set access_key_id/secret_access_key, \
                         configure AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars, \
                         or ensure IAM role is available."
                            .into(),
                    )
                })?
                .clone();
            builder = builder.credentials_provider(creds_provider);
        }

        // Custom endpoint (LocalStack, ElasticMQ, etc.)
        if let Some(ref endpoint) = config.endpoint_url {
            builder = builder.endpoint_url(endpoint);
        }

        Ok(SqsClient::from_conf(builder.build()))
    }

    /// Get message group ID for a FIFO queue message.
    fn get_message_group_id(config: &SqsSinkConfig, event: &SourceEvent) -> Option<String> {
        // Priority: explicit field → config value → event stream name
        if let Some(ref field) = config.message_group_id_field {
            if let Some(value) = event.data.get(field).and_then(|v| v.as_str()) {
                return Some(value.to_string());
            }
        }
        if let Some(ref group_id) = config.message_group_id {
            return Some(group_id.clone());
        }
        Some(event.stream.clone())
    }

    /// Get deduplication ID for a FIFO queue message.
    fn get_deduplication_id(
        config: &SqsSinkConfig,
        event: &SourceEvent,
        index: usize,
    ) -> Option<String> {
        if let Some(ref field) = config.deduplication_id_field {
            if let Some(value) = event.data.get(field).and_then(|v| v.as_str()) {
                return Some(value.to_string());
            }
        }
        // Generate a unique deduplication ID based on position or index
        let position = event.metadata.position.as_deref().unwrap_or("pos");
        Some(format!("{}:{}:{}", event.stream, position, index))
    }

    /// Maximum SQS message size (256KB).
    const MAX_MESSAGE_SIZE: usize = 262_144;

    /// Maximum SQS batch payload size (256KB total).
    /// Note: Not currently enforced, reserved for future batch size tracking.
    #[allow(dead_code)]
    const MAX_BATCH_SIZE: usize = 262_144;

    /// Format event data as message body.
    fn format_body(config: &SqsSinkConfig, event: &SourceEvent) -> String {
        match config.body_format {
            SqsBodyFormat::Json => {
                serde_json::to_string(&event.data).unwrap_or_else(|_| event.data.to_string())
            }
            SqsBodyFormat::Raw => event
                .data
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    serde_json::to_string(&event.data).unwrap_or_else(|_| event.data.to_string())
                }),
            SqsBodyFormat::Base64 => {
                use base64::{engine::general_purpose::STANDARD, Engine};
                let json = serde_json::to_vec(&event.data).unwrap_or_default();
                STANDARD.encode(&json)
            }
        }
    }

    /// Compress message body if compression is enabled.
    fn compress_body(config: &SqsSinkConfig, body: &str) -> (String, u64) {
        match config.compression {
            SqsCompression::None => (body.to_string(), 0),
            SqsCompression::Gzip => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
                if encoder.write_all(body.as_bytes()).is_ok() {
                    if let Ok(compressed) = encoder.finish() {
                        use base64::{engine::general_purpose::STANDARD, Engine};
                        let original_len = body.len() as u64;
                        let compressed_len = compressed.len() as u64;
                        // Only use compression if it actually reduces size
                        if compressed_len < original_len {
                            return (
                                STANDARD.encode(&compressed),
                                original_len.saturating_sub(compressed_len),
                            );
                        }
                    }
                }
                (body.to_string(), 0)
            }
            SqsCompression::Zstd => {
                if let Ok(compressed) = zstd::encode_all(body.as_bytes(), 3) {
                    use base64::{engine::general_purpose::STANDARD, Engine};
                    let original_len = body.len() as u64;
                    let compressed_len = compressed.len() as u64;
                    // Only use compression if it actually reduces size
                    if compressed_len < original_len {
                        return (
                            STANDARD.encode(&compressed),
                            original_len.saturating_sub(compressed_len),
                        );
                    }
                }
                (body.to_string(), 0)
            }
        }
    }

    /// Handle oversized message according to config.
    fn handle_oversized(
        config: &SqsSinkConfig,
        body: &str,
        metrics: &SqsSinkMetrics,
    ) -> Option<String> {
        metrics.oversized_messages.fetch_add(1, Ordering::Relaxed);

        match config.oversized_behavior {
            OversizedMessageBehavior::Fail => None,
            OversizedMessageBehavior::Skip => {
                metrics.skipped_messages.fetch_add(1, Ordering::Relaxed);
                None
            }
            OversizedMessageBehavior::Truncate => {
                // Truncate to MAX_MESSAGE_SIZE - some buffer for metadata
                let max_len = Self::MAX_MESSAGE_SIZE.saturating_sub(1024);
                Some(body.chars().take(max_len).collect())
            }
        }
    }
}

impl Default for SqsSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for SqsSink {
    type Config = SqsSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("sqs-sink", env!("CARGO_PKG_VERSION"))
            .description("Produce messages to Amazon SQS queues with batch sending")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/sqs-sink")
            .config_schema::<SqsSinkConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // 1. Validate configuration schema
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        // Check FIFO queue requirements
        let is_fifo = Self::is_fifo_queue(&config.queue_url);
        if is_fifo && config.message_group_id.is_none() && config.message_group_id_field.is_none() {
            warn!(
                "FIFO queue detected but no message_group_id or message_group_id_field configured. \
                 Will use stream name as message group ID."
            );
        }

        info!("Checking SQS connectivity to {}", config.queue_url);

        #[cfg(feature = "sqs")]
        {
            // 2. Build client and call GetQueueAttributes to verify connectivity + permissions
            let client = match Self::build_client(config).await {
                Ok(c) => c,
                Err(e) => {
                    return Ok(CheckResult::failure(format!(
                        "Failed to build SQS client: {}",
                        e
                    )))
                }
            };

            let attrs_result = tokio::time::timeout(Duration::from_secs(10), async {
                client
                    .get_queue_attributes()
                    .queue_url(&config.queue_url)
                    .attribute_names(QueueAttributeName::QueueArn)
                    .send()
                    .await
            })
            .await;

            match attrs_result {
                Ok(Ok(resp)) => {
                    let empty_attrs = std::collections::HashMap::new();
                    let attrs = resp.attributes().unwrap_or(&empty_attrs);
                    let arn = attrs
                        .get(&QueueAttributeName::QueueArn)
                        .map(|s| s.as_str())
                        .unwrap_or("unknown");

                    info!(
                        queue_arn = %arn,
                        is_fifo = is_fifo,
                        "SQS queue accessible for sending"
                    );

                    Ok(CheckResult::builder()
                        .check_passed("configuration")
                        .check_passed("aws_credentials")
                        .check_passed("queue_accessibility")
                        .build())
                }
                Ok(Err(e)) => {
                    let msg = format!("SQS API error: {}", e);
                    error!("{}", msg);
                    Ok(CheckResult::builder()
                        .check_passed("configuration")
                        .check_failed("queue_accessibility", &msg)
                        .build())
                }
                Err(_) => Ok(CheckResult::failure(format!(
                    "SQS connection timeout for queue: {}",
                    config.queue_url
                ))),
            }
        }

        #[cfg(not(feature = "sqs"))]
        {
            // Fallback: validate config only (no AWS SDK available)
            if !config.auth.has_explicit_credentials()
                && config.auth.profile.is_none()
                && config.auth.role_arn.is_none()
            {
                warn!(
                    "No explicit credentials configured. Will use default AWS credential \
                     chain (environment, IAM role, etc.)"
                );
            }

            Ok(CheckResult::builder()
                .check_passed("configuration")
                .check_passed("queue_url_format")
                .build())
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let metrics = Arc::clone(&self.metrics);
        let queue_name = Self::queue_name_from_url(&config.queue_url).to_string();
        let is_fifo = Self::is_fifo_queue(&config.queue_url);

        info!(
            queue_url = %config.queue_url,
            queue_name = %queue_name,
            is_fifo = is_fifo,
            batch_size = config.batch_size,
            "Starting SQS sink"
        );

        let mut result = WriteResult::new();
        let batch_size = config.batch_size.min(10); // SQS limit is 10
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);

        #[cfg(feature = "sqs")]
        {
            use aws_sdk_sqs::types::{MessageAttributeValue, SendMessageBatchRequestEntry};

            let client = Self::build_client(config).await?;

            // Batch accumulator
            let mut pending_entries: Vec<SendMessageBatchRequestEntry> =
                Vec::with_capacity(batch_size);
            let mut pending_bytes: u64 = 0;
            let mut message_index: usize = 0;
            let mut last_flush = std::time::Instant::now();

            // Circuit breaker state
            let circuit_breaker_threshold = config.circuit_breaker_threshold;
            let mut consecutive_failures: u32 = 0;
            let mut circuit_breaker_tripped_at: Option<std::time::Instant> = None;

            while let Some(event) = events.next().await {
                // Skip non-data events
                if !event.is_data() {
                    continue;
                }

                // Circuit breaker check
                if consecutive_failures >= circuit_breaker_threshold
                    && circuit_breaker_threshold > 0
                {
                    if circuit_breaker_tripped_at.is_none() {
                        metrics
                            .circuit_breaker_trips
                            .fetch_add(1, Ordering::Relaxed);
                        circuit_breaker_tripped_at = Some(std::time::Instant::now());
                        warn!(
                            "Circuit breaker tripped after {} consecutive failures",
                            consecutive_failures
                        );
                    }

                    // Check if recovery window has passed
                    if let Some(tripped) = circuit_breaker_tripped_at {
                        if tripped.elapsed().as_secs() < config.circuit_breaker_recovery_secs {
                            metrics.skipped_messages.fetch_add(1, Ordering::Relaxed);
                            continue;
                        } else {
                            info!("Circuit breaker recovery attempt");
                            circuit_breaker_tripped_at = None;
                            consecutive_failures = 0;
                        }
                    }
                }

                // Format message body
                let raw_body = Self::format_body(config, &event);

                // Apply compression if configured
                let (body, compression_savings) = Self::compress_body(config, &raw_body);
                if compression_savings > 0 {
                    metrics
                        .compression_savings_bytes
                        .fetch_add(compression_savings, Ordering::Relaxed);
                }

                // Check message size (SQS limit is 256KB)
                if body.len() > Self::MAX_MESSAGE_SIZE {
                    if let Some(truncated) = Self::handle_oversized(config, &body, &metrics) {
                        // Use truncated body
                        let body = truncated;
                        let body_len = body.len() as u64;

                        // Build entry with truncated body
                        let entry_id = format!("msg_{}", message_index);
                        let mut entry_builder = SendMessageBatchRequestEntry::builder()
                            .id(&entry_id)
                            .message_body(&body);

                        // Add delay if configured
                        if config.delay_seconds > 0 {
                            entry_builder = entry_builder.delay_seconds(config.delay_seconds);
                        }

                        // FIFO queue attributes
                        if is_fifo {
                            if let Some(group_id) = Self::get_message_group_id(config, &event) {
                                entry_builder = entry_builder.message_group_id(group_id);
                            }
                            // Content-based deduplication: don't set deduplication ID
                            if !config.content_based_deduplication {
                                if let Some(dedup_id) =
                                    Self::get_deduplication_id(config, &event, message_index)
                                {
                                    entry_builder =
                                        entry_builder.message_deduplication_id(dedup_id);
                                }
                            }
                        }

                        // Add metadata as message attributes if configured
                        if config.include_metadata {
                            let stream_attr = MessageAttributeValue::builder()
                                .data_type("String")
                                .string_value(&event.stream)
                                .build()
                                .expect("valid attribute");
                            entry_builder =
                                entry_builder.message_attributes("rivven.stream", stream_attr);

                            if let Some(ref ns) = event.namespace {
                                let ns_attr = MessageAttributeValue::builder()
                                    .data_type("String")
                                    .string_value(ns)
                                    .build()
                                    .expect("valid attribute");
                                entry_builder =
                                    entry_builder.message_attributes("rivven.namespace", ns_attr);
                            }

                            // Add compression indicator if compressed
                            if compression_savings > 0 {
                                let comp_attr = MessageAttributeValue::builder()
                                    .data_type("String")
                                    .string_value(match config.compression {
                                        SqsCompression::Gzip => "gzip",
                                        SqsCompression::Zstd => "zstd",
                                        SqsCompression::None => "none",
                                    })
                                    .build()
                                    .expect("valid attribute");
                                entry_builder = entry_builder
                                    .message_attributes("rivven.compression", comp_attr);
                            }
                        }

                        // Add custom attributes
                        for (key, value) in &config.custom_attributes {
                            let attr = MessageAttributeValue::builder()
                                .data_type("String")
                                .string_value(value)
                                .build()
                                .expect("valid attribute");
                            entry_builder = entry_builder.message_attributes(key, attr);
                        }

                        if let Ok(entry) = entry_builder.build() {
                            pending_entries.push(entry);
                            pending_bytes += body_len;
                            message_index += 1;
                        }
                    } else {
                        // Message was failed or skipped
                        if matches!(config.oversized_behavior, OversizedMessageBehavior::Fail) {
                            result.add_failure(
                                1,
                                format!("Message exceeds 256KB limit: {} bytes", body.len()),
                            );
                            metrics.messages_failed.fetch_add(1, Ordering::Relaxed);
                        }
                        continue;
                    }
                } else {
                    let body_len = body.len() as u64;

                    // Build entry
                    let entry_id = format!("msg_{}", message_index);
                    let mut entry_builder = SendMessageBatchRequestEntry::builder()
                        .id(&entry_id)
                        .message_body(&body);

                    // Add delay if configured
                    if config.delay_seconds > 0 {
                        entry_builder = entry_builder.delay_seconds(config.delay_seconds);
                    }

                    // FIFO queue attributes
                    if is_fifo {
                        if let Some(group_id) = Self::get_message_group_id(config, &event) {
                            entry_builder = entry_builder.message_group_id(group_id);
                        }
                        // Content-based deduplication: don't set deduplication ID
                        if !config.content_based_deduplication {
                            if let Some(dedup_id) =
                                Self::get_deduplication_id(config, &event, message_index)
                            {
                                entry_builder = entry_builder.message_deduplication_id(dedup_id);
                            }
                        }
                    }

                    // Add metadata as message attributes if configured
                    if config.include_metadata {
                        let stream_attr = MessageAttributeValue::builder()
                            .data_type("String")
                            .string_value(&event.stream)
                            .build()
                            .expect("valid attribute");
                        entry_builder =
                            entry_builder.message_attributes("rivven.stream", stream_attr);

                        if let Some(ref ns) = event.namespace {
                            let ns_attr = MessageAttributeValue::builder()
                                .data_type("String")
                                .string_value(ns)
                                .build()
                                .expect("valid attribute");
                            entry_builder =
                                entry_builder.message_attributes("rivven.namespace", ns_attr);
                        }

                        // Add compression indicator if compressed
                        if compression_savings > 0 {
                            let comp_attr = MessageAttributeValue::builder()
                                .data_type("String")
                                .string_value(match config.compression {
                                    SqsCompression::Gzip => "gzip",
                                    SqsCompression::Zstd => "zstd",
                                    SqsCompression::None => "none",
                                })
                                .build()
                                .expect("valid attribute");
                            entry_builder =
                                entry_builder.message_attributes("rivven.compression", comp_attr);
                        }
                    }

                    // Add custom attributes
                    for (key, value) in &config.custom_attributes {
                        let attr = MessageAttributeValue::builder()
                            .data_type("String")
                            .string_value(value)
                            .build()
                            .expect("valid attribute");
                        entry_builder = entry_builder.message_attributes(key, attr);
                    }

                    let entry = entry_builder.build().map_err(|e| {
                        ConnectorError::Serialization(format!(
                            "Failed to build SQS message entry: {}",
                            e
                        ))
                    })?;

                    pending_entries.push(entry);
                    pending_bytes += body_len;
                    message_index += 1;
                }

                // Check if we should flush
                let should_flush =
                    pending_entries.len() >= batch_size || last_flush.elapsed() >= batch_timeout;

                if should_flush && !pending_entries.is_empty() {
                    let batch_start = std::time::Instant::now();
                    let entries_to_send = std::mem::take(&mut pending_entries);
                    let entries_count = entries_to_send.len();
                    let bytes_in_batch = pending_bytes;

                    let mut current_backoff_ms = config.retry_initial_ms;
                    let mut retries = 0u32;
                    let mut send_success = false;

                    // Retry loop
                    while retries <= config.max_retries {
                        match client
                            .send_message_batch()
                            .queue_url(&config.queue_url)
                            .set_entries(Some(entries_to_send.clone()))
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                let successful = resp.successful().len() as u64;
                                let failed = resp.failed().len() as u64;

                                metrics
                                    .messages_sent
                                    .fetch_add(successful, Ordering::Relaxed);
                                metrics
                                    .bytes_sent
                                    .fetch_add(bytes_in_batch, Ordering::Relaxed);
                                metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                                metrics.record_batch_size(entries_count as u64);

                                result.add_success(successful, bytes_in_batch);

                                if failed > 0 {
                                    metrics.messages_failed.fetch_add(failed, Ordering::Relaxed);
                                    metrics.partial_failures.fetch_add(1, Ordering::Relaxed);
                                    for failure in resp.failed() {
                                        let error_msg = format!(
                                            "Message {} failed: {} - {}",
                                            failure.id(),
                                            failure.code(),
                                            failure.message().unwrap_or("unknown")
                                        );
                                        result.add_failure(1, error_msg.clone());
                                        warn!("{}", error_msg);
                                    }
                                }

                                debug!(
                                    batch_size = entries_count,
                                    successful = successful,
                                    failed = failed,
                                    "Sent SQS batch"
                                );

                                send_success = true;
                                // Reset circuit breaker on success
                                consecutive_failures = 0;
                                break;
                            }
                            Err(e) => {
                                retries += 1;
                                metrics.retries.fetch_add(1, Ordering::Relaxed);

                                if retries > config.max_retries {
                                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                                    metrics
                                        .messages_failed
                                        .fetch_add(entries_count as u64, Ordering::Relaxed);
                                    let error_msg = format!(
                                        "Failed to send batch after {} retries: {}",
                                        config.max_retries, e
                                    );
                                    error!("{}", error_msg);
                                    result.add_failure(entries_count as u64, error_msg);
                                    // Increment circuit breaker counter
                                    consecutive_failures += 1;
                                    break;
                                }

                                warn!(
                                    retry = retries,
                                    backoff_ms = current_backoff_ms,
                                    "SQS send error, retrying: {}",
                                    e
                                );

                                tokio::time::sleep(Duration::from_millis(current_backoff_ms)).await;
                                current_backoff_ms =
                                    ((current_backoff_ms as f64 * config.retry_multiplier) as u64)
                                        .min(config.retry_max_ms);
                            }
                        }
                    }

                    let batch_elapsed = batch_start.elapsed().as_micros() as u64;
                    metrics
                        .send_latency_us
                        .fetch_add(batch_elapsed, Ordering::Relaxed);

                    if send_success {
                        pending_bytes = 0;
                        last_flush = std::time::Instant::now();
                    }

                    pending_entries = Vec::with_capacity(batch_size);
                }
            }

            // Flush any remaining messages
            if !pending_entries.is_empty() {
                let batch_start = std::time::Instant::now();
                let entries_count = pending_entries.len();

                match client
                    .send_message_batch()
                    .queue_url(&config.queue_url)
                    .set_entries(Some(pending_entries))
                    .send()
                    .await
                {
                    Ok(resp) => {
                        let successful = resp.successful().len() as u64;
                        let failed = resp.failed().len() as u64;

                        metrics
                            .messages_sent
                            .fetch_add(successful, Ordering::Relaxed);
                        metrics
                            .bytes_sent
                            .fetch_add(pending_bytes, Ordering::Relaxed);
                        metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                        metrics.record_batch_size(entries_count as u64);

                        result.add_success(successful, pending_bytes);

                        if failed > 0 {
                            metrics.messages_failed.fetch_add(failed, Ordering::Relaxed);
                            for failure in resp.failed() {
                                result.add_failure(
                                    1,
                                    format!("Message {} failed: {}", failure.id(), failure.code()),
                                );
                            }
                        }
                    }
                    Err(e) => {
                        metrics.errors.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .messages_failed
                            .fetch_add(entries_count as u64, Ordering::Relaxed);
                        result.add_failure(
                            entries_count as u64,
                            format!("Final batch send failed: {}", e),
                        );
                    }
                }

                let batch_elapsed = batch_start.elapsed().as_micros() as u64;
                metrics
                    .send_latency_us
                    .fetch_add(batch_elapsed, Ordering::Relaxed);
            }
        }

        #[cfg(not(feature = "sqs"))]
        {
            // Simulation mode for testing without AWS SDK
            let mut message_count = 0u64;
            let mut total_bytes = 0u64;

            while let Some(event) = events.next().await {
                if !event.is_data() {
                    continue;
                }

                let body =
                    serde_json::to_string(&event.data).unwrap_or_else(|_| event.data.to_string());
                let body_len = body.len() as u64;

                debug!(
                    stream = %event.stream,
                    body_len = body_len,
                    is_fifo = is_fifo,
                    "Simulated SQS send"
                );

                message_count += 1;
                total_bytes += body_len;

                metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                metrics.bytes_sent.fetch_add(body_len, Ordering::Relaxed);
            }

            // Record final batch metrics
            if message_count > 0 {
                metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
                metrics.record_batch_size(message_count);
            }

            result.add_success(message_count, total_bytes);
        }

        info!(
            total_messages = result.records_written,
            total_bytes = result.bytes_written,
            failed = result.records_failed,
            "SQS sink completed"
        );

        Ok(result)
    }
}

// ============================================================================
// SQS Sink Factory
// ============================================================================

/// Factory for creating SQS sink instances.
pub struct SqsSinkFactory;

impl SinkFactory for SqsSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        SqsSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(SqsSink::new()))
    }
}

/// AnySink implementation for SqsSink — type-erased dispatch.
#[async_trait]
impl AnySink for SqsSink {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: SqsSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid SQS sink config: {}", e)))?;
        self.check(&config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let config: SqsSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid SQS sink config: {}", e)))?;
        self.write(&config, events).await
    }
}

// ============================================================================
// Registry Functions
// ============================================================================

/// Register SQS source connectors with a source registry.
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("sqs-source", std::sync::Arc::new(SqsSourceFactory));
}

/// Register SQS sink connectors with a sink registry.
pub fn register_sinks(registry: &mut SinkRegistry) {
    registry.register("sqs-sink", std::sync::Arc::new(SqsSinkFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Config tests ----

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
        assert!(!config.include_system_attributes);
        assert_eq!(config.retry_initial_ms, 200);
        assert_eq!(config.retry_max_ms, 30_000);
        assert_eq!(config.retry_multiplier, 2.0);
        assert_eq!(config.checkpoint_interval, 10);
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
        // Secret should be redacted
        assert!(json.contains("***REDACTED***"));
        assert!(!json.contains("\"secret\""));
    }

    #[test]
    fn test_config_deserialization() {
        let config: SqsSourceConfig = serde_json::from_str(
            r#"{
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
                "region": "us-east-1",
                "max_messages": 5,
                "wait_time_seconds": 10,
                "visibility_timeout": 60,
                "delete_after_receive": false,
                "endpoint_url": "http://localhost:4566"
            }"#,
        )
        .unwrap();

        assert_eq!(config.max_messages, 5);
        assert_eq!(config.wait_time_seconds, 10);
        assert_eq!(config.visibility_timeout, 60);
        assert!(!config.delete_after_receive);
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:4566")
        );
    }

    // ---- Auth tests ----

    #[test]
    fn test_aws_auth_config_explicit() {
        let auth = AwsAuthConfig {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            secret_access_key: Some(SensitiveString::new(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            )),
            ..Default::default()
        };
        assert!(auth.has_explicit_credentials());
    }

    #[test]
    fn test_aws_auth_config_empty() {
        let auth = AwsAuthConfig::default();
        assert!(!auth.has_explicit_credentials());
    }

    #[test]
    fn test_aws_auth_config_partial() {
        let auth = AwsAuthConfig {
            access_key_id: Some("AKIAEXAMPLE".to_string()),
            // No secret
            ..Default::default()
        };
        assert!(!auth.has_explicit_credentials());
    }

    #[test]
    fn test_sensitive_string_serialization() {
        let secret = SensitiveString::new("my-secret-key");
        let serialized = serde_json::to_string(&secret).unwrap();
        assert_eq!(serialized, "\"***REDACTED***\"");
        assert!(!serialized.contains("my-secret-key"));
    }

    // ---- Metrics tests ----

    #[test]
    fn test_metrics_new() {
        let metrics = SqsSourceMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_received, 0);
        assert_eq!(snap.batch_size_min, 0); // u64::MAX → 0 in snapshot
        assert_eq!(snap.batch_size_max, 0);
    }

    #[test]
    fn test_metrics_counter_increments() {
        let metrics = SqsSourceMetrics::new();
        metrics.messages_received.fetch_add(10, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(5000, Ordering::Relaxed);
        metrics.polls.fetch_add(3, Ordering::Relaxed);
        metrics.empty_polls.fetch_add(1, Ordering::Relaxed);
        metrics.errors.fetch_add(2, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.messages_received, 10);
        assert_eq!(snap.bytes_received, 5000);
        assert_eq!(snap.polls, 3);
        assert_eq!(snap.empty_polls, 1);
        assert_eq!(snap.errors, 2);
    }

    #[test]
    fn test_metrics_batch_size_tracking() {
        let metrics = SqsSourceMetrics::new();
        metrics.record_batch_size(3);
        metrics.record_batch_size(10);
        metrics.record_batch_size(7);

        let snap = metrics.snapshot();
        assert_eq!(snap.batch_size_min, 3);
        assert_eq!(snap.batch_size_max, 10);
        assert_eq!(snap.batch_size_sum, 20);
    }

    #[test]
    fn test_metrics_snapshot_and_reset() {
        let metrics = SqsSourceMetrics::new();
        metrics.messages_received.fetch_add(50, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(2000, Ordering::Relaxed);
        metrics.record_batch_size(5);

        let snap = metrics.snapshot_and_reset();
        assert_eq!(snap.messages_received, 50);
        assert_eq!(snap.bytes_received, 2000);
        assert_eq!(snap.batch_size_min, 5);
        assert_eq!(snap.batch_size_max, 5);

        // Verify reset
        let snap2 = metrics.snapshot();
        assert_eq!(snap2.messages_received, 0);
        assert_eq!(snap2.bytes_received, 0);
        assert_eq!(snap2.batch_size_min, 0);
    }

    #[test]
    fn test_metrics_derived_values() {
        let snap = SqsSourceMetricsSnapshot {
            messages_received: 1000,
            bytes_received: 50000,
            messages_deleted: 990,
            delete_failures: 10,
            polls: 100,
            empty_polls: 20,
            errors: 5,
            poll_latency_us: 5_000_000, // 5s total
            batch_size_min: 3,
            batch_size_max: 10,
            batch_size_sum: 700,
        };

        // Avg poll latency = 5_000_000us / 100 polls = 50000us = 50ms
        assert!((snap.avg_poll_latency_ms() - 50.0).abs() < 0.001);

        // Avg batch size = 700 / (100 - 20) = 8.75
        assert!((snap.avg_batch_size() - 8.75).abs() < 0.001);

        // Messages per second with 10s elapsed = 100/s
        assert!((snap.messages_per_second(10.0) - 100.0).abs() < 0.001);

        // Bytes per second
        assert!((snap.bytes_per_second(10.0) - 5000.0).abs() < 0.001);

        // Error rate = 5 / 105 ≈ 4.76%
        assert!((snap.error_rate_percent() - 4.762).abs() < 0.01);

        // Empty poll rate = 20 / 100 = 20%
        assert!((snap.empty_poll_rate_percent() - 20.0).abs() < 0.001);
    }

    #[test]
    fn test_metrics_edge_cases() {
        let snap = SqsSourceMetricsSnapshot::default();
        assert_eq!(snap.avg_poll_latency_ms(), 0.0);
        assert_eq!(snap.avg_batch_size(), 0.0);
        assert_eq!(snap.messages_per_second(0.0), 0.0);
        assert_eq!(snap.bytes_per_second(-1.0), 0.0);
        assert_eq!(snap.error_rate_percent(), 0.0);
        assert_eq!(snap.empty_poll_rate_percent(), 0.0);
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = SqsSourceMetrics::new();
        metrics.messages_received.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(5000, Ordering::Relaxed);
        metrics.errors.fetch_add(2, Ordering::Relaxed);
        metrics.polls.fetch_add(10, Ordering::Relaxed);

        let snap = metrics.snapshot();
        let prom = snap.to_prometheus_format("rivven");

        assert!(prom.contains("rivven_sqs_source_messages_received_total 100"));
        assert!(prom.contains("rivven_sqs_source_bytes_received_total 5000"));
        assert!(prom.contains("rivven_sqs_source_errors_total 2"));
        assert!(prom.contains("rivven_sqs_source_polls_total 10"));
        assert!(prom.contains("# HELP"));
        assert!(prom.contains("# TYPE"));
    }

    // ---- Source lifecycle tests ----

    #[test]
    fn test_queue_name_extraction() {
        assert_eq!(
            SqsSource::queue_name_from_url(
                "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
            ),
            "my-queue"
        );
        assert_eq!(
            SqsSource::queue_name_from_url(
                "https://sqs.us-east-1.amazonaws.com/123456789/events.fifo"
            ),
            "events.fifo"
        );
        assert_eq!(SqsSource::queue_name_from_url(""), "");
    }

    #[test]
    fn test_generate_test_message() {
        let (id, body, receipt) = SqsSource::generate_test_message(42);
        assert!(!id.is_empty());
        assert!(!body.is_empty());
        assert!(receipt.starts_with("receipt_"));

        // Body should be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["sequence"], 42);
    }

    #[test]
    fn test_test_message_to_event() {
        let config = SqsSourceConfig::default();
        let event = SqsSource::test_message_to_event(
            "msg-123",
            r#"{"key":"value"}"#,
            "receipt-abc",
            "test-stream",
            &config,
            1,
        );

        assert_eq!(event.stream, "test-stream");
        assert!(event.metadata.extra.contains_key("sqs.message_id"));
        assert!(event.metadata.extra.contains_key("sqs.receipt_handle"));
    }

    #[test]
    fn test_test_message_with_system_attrs() {
        let config = SqsSourceConfig {
            include_system_attributes: true,
            ..Default::default()
        };
        let event = SqsSource::test_message_to_event(
            "msg-123",
            r#"{"key":"value"}"#,
            "receipt-abc",
            "test-stream",
            &config,
            1,
        );

        assert!(event
            .metadata
            .extra
            .contains_key("sqs.sys.ApproximateReceiveCount"));
        assert!(event.metadata.extra.contains_key("sqs.sys.SentTimestamp"));
    }

    #[tokio::test]
    async fn test_source_spec() {
        let spec = SqsSource::spec();
        assert_eq!(spec.connector_type, "sqs-source");
        assert!(spec.supports_incremental);
        assert!(spec.config_schema.is_some());
        assert!(spec.description.as_ref().unwrap().contains("SQS"));
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
        // With the sqs feature, check fails because there are no valid AWS credentials.
        // Without the sqs feature, check succeeds with config-only validation.
        #[cfg(feature = "sqs")]
        assert!(!result.success);
        #[cfg(not(feature = "sqs"))]
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
        let schema = &catalog.streams[0].json_schema;
        assert!(schema["properties"].get("message_group_id").is_some());
        assert!(schema["properties"].get("sequence_number").is_some());
        assert!(schema["properties"]
            .get("message_deduplication_id")
            .is_some());
    }

    #[test]
    fn test_graceful_shutdown() {
        let source = SqsSource::new();
        assert!(!source.is_shutting_down());
        source.shutdown();
        assert!(source.is_shutting_down());
    }

    #[test]
    fn test_drop_triggers_shutdown() {
        let shutdown = {
            let source = SqsSource::new();
            Arc::clone(&source.shutdown)
        }; // source dropped here
        assert!(shutdown.load(Ordering::Relaxed));
    }

    // ========================================================================
    // SQS Sink Tests
    // ========================================================================

    // ---- Sink Config tests ----

    #[test]
    fn test_sink_config_validation() {
        let config = SqsSinkConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sink_config_defaults() {
        let config = SqsSinkConfig::default();
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.batch_timeout_ms, 1000);
        assert_eq!(config.delay_seconds, 0);
        assert!(config.include_metadata);
        assert!(config.message_group_id.is_none());
        assert_eq!(config.retry_initial_ms, 200);
        assert_eq!(config.retry_max_ms, 30_000);
        assert_eq!(config.retry_multiplier, 2.0);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_sink_config_serialization() {
        let config = SqsSinkConfig {
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
        // Secret should be redacted
        assert!(json.contains("***REDACTED***"));
        assert!(!json.contains("\"secret\""));
    }

    #[test]
    fn test_sink_config_deserialization() {
        let config: SqsSinkConfig = serde_json::from_str(
            r#"{
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo",
                "region": "us-east-1",
                "batch_size": 5,
                "batch_timeout_ms": 500,
                "message_group_id": "my-group",
                "delay_seconds": 10,
                "endpoint_url": "http://localhost:4566"
            }"#,
        )
        .unwrap();

        assert_eq!(config.batch_size, 5);
        assert_eq!(config.batch_timeout_ms, 500);
        assert_eq!(config.message_group_id.as_deref(), Some("my-group"));
        assert_eq!(config.delay_seconds, 10);
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:4566")
        );
    }

    #[test]
    fn test_sink_config_fifo_with_group_id_field() {
        let config: SqsSinkConfig = serde_json::from_str(
            r#"{
                "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/orders.fifo",
                "region": "us-east-1",
                "message_group_id_field": "customer_id",
                "deduplication_id_field": "order_id"
            }"#,
        )
        .unwrap();

        assert_eq!(
            config.message_group_id_field.as_deref(),
            Some("customer_id")
        );
        assert_eq!(config.deduplication_id_field.as_deref(), Some("order_id"));
    }

    // ---- Sink Metrics tests ----

    #[test]
    fn test_sink_metrics_new() {
        let metrics = SqsSinkMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.messages_sent, 0);
        assert_eq!(snap.batch_size_min, 0); // u64::MAX → 0 in snapshot
        assert_eq!(snap.batch_size_max, 0);
    }

    #[test]
    fn test_sink_metrics_counter_increments() {
        let metrics = SqsSinkMetrics::new();
        metrics.messages_sent.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(50000, Ordering::Relaxed);
        metrics.batches_sent.fetch_add(10, Ordering::Relaxed);
        metrics.messages_failed.fetch_add(5, Ordering::Relaxed);
        metrics.errors.fetch_add(2, Ordering::Relaxed);
        metrics.retries.fetch_add(3, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.messages_sent, 100);
        assert_eq!(snap.bytes_sent, 50000);
        assert_eq!(snap.batches_sent, 10);
        assert_eq!(snap.messages_failed, 5);
        assert_eq!(snap.errors, 2);
        assert_eq!(snap.retries, 3);
    }

    #[test]
    fn test_sink_metrics_batch_size_tracking() {
        let metrics = SqsSinkMetrics::new();
        metrics.record_batch_size(3);
        metrics.record_batch_size(10);
        metrics.record_batch_size(7);

        let snap = metrics.snapshot();
        assert_eq!(snap.batch_size_min, 3);
        assert_eq!(snap.batch_size_max, 10);
        assert_eq!(snap.batch_size_sum, 20);
    }

    #[test]
    fn test_sink_metrics_snapshot_and_reset() {
        let metrics = SqsSinkMetrics::new();
        metrics.messages_sent.fetch_add(50, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(2000, Ordering::Relaxed);
        metrics.record_batch_size(5);

        let snap = metrics.snapshot_and_reset();
        assert_eq!(snap.messages_sent, 50);
        assert_eq!(snap.bytes_sent, 2000);
        assert_eq!(snap.batch_size_min, 5);
        assert_eq!(snap.batch_size_max, 5);

        // Verify reset
        let snap2 = metrics.snapshot();
        assert_eq!(snap2.messages_sent, 0);
        assert_eq!(snap2.bytes_sent, 0);
        assert_eq!(snap2.batch_size_min, 0);
    }

    #[test]
    fn test_sink_metrics_derived_values() {
        let snap = SqsSinkMetricsSnapshot {
            messages_sent: 1000,
            bytes_sent: 50000,
            messages_failed: 50,
            batches_sent: 100,
            partial_failures: 5,
            errors: 2,
            retries: 10,
            send_latency_us: 5_000_000, // 5s total
            batch_size_min: 5,
            batch_size_max: 10,
            batch_size_sum: 750,
            oversized_messages: 0,
            skipped_messages: 0,
            compression_savings_bytes: 0,
            circuit_breaker_trips: 0,
        };

        // Avg send latency = 5_000_000us / 100 batches = 50000us = 50ms
        assert!((snap.avg_send_latency_ms() - 50.0).abs() < 0.001);

        // Avg batch size = 750 / 100 = 7.5
        assert!((snap.avg_batch_size() - 7.5).abs() < 0.001);

        // Messages per second with 10s elapsed = 100/s
        assert!((snap.messages_per_second(10.0) - 100.0).abs() < 0.001);

        // Bytes per second
        assert!((snap.bytes_per_second(10.0) - 5000.0).abs() < 0.001);

        // Success rate = 1000 / 1050 ≈ 95.24%
        assert!((snap.success_rate_percent() - 95.238).abs() < 0.01);

        // Error rate = 2 / 102 ≈ 1.96%
        assert!((snap.error_rate_percent() - 1.96).abs() < 0.1);
    }

    #[test]
    fn test_sink_metrics_edge_cases() {
        let snap = SqsSinkMetricsSnapshot::default();
        assert_eq!(snap.avg_send_latency_ms(), 0.0);
        assert_eq!(snap.avg_batch_size(), 0.0);
        assert_eq!(snap.messages_per_second(0.0), 0.0);
        assert_eq!(snap.bytes_per_second(-1.0), 0.0);
        assert_eq!(snap.success_rate_percent(), 100.0); // 100% when no messages
        assert_eq!(snap.error_rate_percent(), 0.0);
    }

    #[test]
    fn test_sink_prometheus_format() {
        let metrics = SqsSinkMetrics::new();
        metrics.messages_sent.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_sent.fetch_add(5000, Ordering::Relaxed);
        metrics.errors.fetch_add(2, Ordering::Relaxed);
        metrics.batches_sent.fetch_add(10, Ordering::Relaxed);

        let snap = metrics.snapshot();
        let prom = snap.to_prometheus_format("rivven");

        assert!(prom.contains("rivven_sqs_sink_messages_sent_total 100"));
        assert!(prom.contains("rivven_sqs_sink_bytes_sent_total 5000"));
        assert!(prom.contains("rivven_sqs_sink_errors_total 2"));
        assert!(prom.contains("rivven_sqs_sink_batches_sent_total 10"));
        assert!(prom.contains("# HELP"));
        assert!(prom.contains("# TYPE"));
    }

    #[test]
    fn test_sink_metrics_json_serialization() {
        let snap = SqsSinkMetricsSnapshot {
            messages_sent: 200,
            bytes_sent: 10000,
            messages_failed: 5,
            batches_sent: 20,
            partial_failures: 1,
            errors: 1,
            retries: 2,
            send_latency_us: 100000,
            batch_size_min: 10,
            batch_size_max: 10,
            batch_size_sum: 200,
            oversized_messages: 0,
            skipped_messages: 0,
            compression_savings_bytes: 0,
            circuit_breaker_trips: 0,
        };

        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"messages_sent\":200"));
        assert!(json.contains("\"bytes_sent\":10000"));

        let deserialized: SqsSinkMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, snap);
    }

    // ---- Sink lifecycle tests ----

    #[test]
    fn test_sink_queue_name_extraction() {
        assert_eq!(
            SqsSink::queue_name_from_url("https://sqs.us-east-1.amazonaws.com/123456789/my-queue"),
            "my-queue"
        );
        assert_eq!(
            SqsSink::queue_name_from_url(
                "https://sqs.us-east-1.amazonaws.com/123456789/events.fifo"
            ),
            "events.fifo"
        );
    }

    #[test]
    fn test_sink_is_fifo_queue() {
        assert!(!SqsSink::is_fifo_queue(
            "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
        ));
        assert!(SqsSink::is_fifo_queue(
            "https://sqs.us-east-1.amazonaws.com/123456789/events.fifo"
        ));
    }

    #[test]
    fn test_sink_get_message_group_id_explicit() {
        let config = SqsSinkConfig {
            message_group_id: Some("explicit-group".to_string()),
            ..Default::default()
        };
        let event = SourceEvent::record("test-stream", json!({"data": "test"}));

        let group_id = SqsSink::get_message_group_id(&config, &event);
        assert_eq!(group_id, Some("explicit-group".to_string()));
    }

    #[test]
    fn test_sink_get_message_group_id_from_field() {
        let config = SqsSinkConfig {
            message_group_id_field: Some("customer_id".to_string()),
            ..Default::default()
        };
        let event = SourceEvent::record(
            "test-stream",
            json!({"customer_id": "cust-123", "data": "test"}),
        );

        let group_id = SqsSink::get_message_group_id(&config, &event);
        assert_eq!(group_id, Some("cust-123".to_string()));
    }

    #[test]
    fn test_sink_get_message_group_id_fallback_to_stream() {
        let config = SqsSinkConfig::default();
        let event = SourceEvent::record("orders", json!({"data": "test"}));

        let group_id = SqsSink::get_message_group_id(&config, &event);
        assert_eq!(group_id, Some("orders".to_string()));
    }

    #[test]
    fn test_sink_get_deduplication_id_from_field() {
        let config = SqsSinkConfig {
            deduplication_id_field: Some("order_id".to_string()),
            ..Default::default()
        };
        let event = SourceEvent::record("orders", json!({"order_id": "ord-456", "data": "test"}));

        let dedup_id = SqsSink::get_deduplication_id(&config, &event, 0);
        assert_eq!(dedup_id, Some("ord-456".to_string()));
    }

    #[test]
    fn test_sink_get_deduplication_id_generated() {
        let config = SqsSinkConfig::default();
        let event = SourceEvent::record("orders", json!({"data": "test"}));

        let dedup_id = SqsSink::get_deduplication_id(&config, &event, 42);
        assert!(dedup_id.is_some());
        assert!(dedup_id.as_ref().unwrap().contains("orders"));
        assert!(dedup_id.as_ref().unwrap().contains("42"));
    }

    #[tokio::test]
    async fn test_sink_spec() {
        let spec = SqsSink::spec();
        assert_eq!(spec.connector_type, "sqs-sink");
        assert!(spec.config_schema.is_some());
        assert!(spec.description.as_ref().unwrap().contains("SQS"));
    }

    #[tokio::test]
    async fn test_sink_check() {
        let sink = SqsSink::new();
        let config = SqsSinkConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let result = sink.check(&config).await.unwrap();
        // With the sqs feature, check fails because there are no valid AWS credentials.
        // Without the sqs feature, check succeeds with config-only validation.
        #[cfg(feature = "sqs")]
        assert!(!result.success);
        #[cfg(not(feature = "sqs"))]
        assert!(result.success);
    }

    #[tokio::test]
    async fn test_sink_check_fifo_warning() {
        let sink = SqsSink::new();
        let config = SqsSinkConfig {
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo".to_string(),
            region: "us-east-1".to_string(),
            // No message_group_id set - should warn
            ..Default::default()
        };

        let _result = sink.check(&config).await.unwrap();
        // Check still passes (warning is logged)
        #[cfg(not(feature = "sqs"))]
        assert!(_result.success);
    }

    #[test]
    fn test_sink_metrics_access() {
        let sink = SqsSink::new();
        let metrics = sink.metrics();

        metrics.messages_sent.fetch_add(75, Ordering::Relaxed);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_sent, 75);
    }

    #[tokio::test]
    async fn test_sink_write_simulation_mode() {
        // Test simulation mode (without sqs feature)
        #[cfg(not(feature = "sqs"))]
        {
            use futures::stream;

            let sink = SqsSink::new();
            let config = SqsSinkConfig {
                queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            };

            let events = vec![
                SourceEvent::record("test", json!({"id": 1, "data": "message 1"})),
                SourceEvent::record("test", json!({"id": 2, "data": "message 2"})),
                SourceEvent::record("test", json!({"id": 3, "data": "message 3"})),
            ];
            let event_stream = Box::pin(stream::iter(events));

            let result = sink.write(&config, event_stream).await.unwrap();
            assert_eq!(result.records_written, 3);
            assert!(result.bytes_written > 0);
            assert_eq!(result.records_failed, 0);

            let snapshot = sink.metrics().snapshot();
            assert_eq!(snapshot.messages_sent, 3);
            assert_eq!(snapshot.batches_sent, 1);
        }
    }

    // ====== New Feature Tests ======

    #[test]
    fn test_sink_body_format_options() {
        // Test body format enum variants
        assert_eq!(SqsBodyFormat::default(), SqsBodyFormat::Json);

        let json_config: SqsBodyFormat = serde_json::from_str("\"json\"").unwrap();
        assert_eq!(json_config, SqsBodyFormat::Json);

        let raw_config: SqsBodyFormat = serde_json::from_str("\"raw\"").unwrap();
        assert_eq!(raw_config, SqsBodyFormat::Raw);

        let base64_config: SqsBodyFormat = serde_json::from_str("\"base64\"").unwrap();
        assert_eq!(base64_config, SqsBodyFormat::Base64);
    }

    #[test]
    fn test_sink_compression_options() {
        // Test compression enum variants
        assert_eq!(SqsCompression::default(), SqsCompression::None);

        let none_config: SqsCompression = serde_json::from_str("\"none\"").unwrap();
        assert_eq!(none_config, SqsCompression::None);

        let gzip_config: SqsCompression = serde_json::from_str("\"gzip\"").unwrap();
        assert_eq!(gzip_config, SqsCompression::Gzip);

        let zstd_config: SqsCompression = serde_json::from_str("\"zstd\"").unwrap();
        assert_eq!(zstd_config, SqsCompression::Zstd);
    }

    #[test]
    fn test_sink_oversized_behavior_options() {
        // Test oversized message behavior enum variants
        assert_eq!(
            OversizedMessageBehavior::default(),
            OversizedMessageBehavior::Fail
        );

        let fail_config: OversizedMessageBehavior = serde_json::from_str("\"fail\"").unwrap();
        assert_eq!(fail_config, OversizedMessageBehavior::Fail);

        let skip_config: OversizedMessageBehavior = serde_json::from_str("\"skip\"").unwrap();
        assert_eq!(skip_config, OversizedMessageBehavior::Skip);

        let truncate_config: OversizedMessageBehavior =
            serde_json::from_str("\"truncate\"").unwrap();
        assert_eq!(truncate_config, OversizedMessageBehavior::Truncate);
    }

    #[test]
    fn test_sink_config_new_fields_defaults() {
        let config = SqsSinkConfig::default();

        // New fields have correct defaults
        assert_eq!(config.body_format, SqsBodyFormat::Json);
        assert_eq!(config.compression, SqsCompression::None);
        assert_eq!(config.oversized_behavior, OversizedMessageBehavior::Fail);
        assert!(!config.content_based_deduplication);
        assert_eq!(config.circuit_breaker_threshold, 0);
        assert_eq!(config.circuit_breaker_recovery_secs, 30);
    }

    #[test]
    fn test_sink_config_new_fields_deserialization() {
        let yaml = r#"
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo"
            region: "us-east-1"
            body_format: "base64"
            compression: "gzip"
            oversized_behavior: "skip"
            content_based_deduplication: true
            circuit_breaker_threshold: 5
            circuit_breaker_recovery_secs: 60
        "#;

        let config: SqsSinkConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.body_format, SqsBodyFormat::Base64);
        assert_eq!(config.compression, SqsCompression::Gzip);
        assert_eq!(config.oversized_behavior, OversizedMessageBehavior::Skip);
        assert!(config.content_based_deduplication);
        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_recovery_secs, 60);
    }

    #[test]
    fn test_sink_format_body_json() {
        let config = SqsSinkConfig {
            body_format: SqsBodyFormat::Json,
            ..Default::default()
        };
        let event = SourceEvent::record("test", json!({"key": "value", "num": 42}));

        let body = SqsSink::format_body(&config, &event);
        assert!(body.contains("\"key\""));
        assert!(body.contains("\"value\""));
        assert!(body.contains("42"));
    }

    #[test]
    fn test_sink_format_body_raw() {
        let config = SqsSinkConfig {
            body_format: SqsBodyFormat::Raw,
            ..Default::default()
        };

        // Raw string data
        let event = SourceEvent::record("test", json!("plain text message"));
        let body = SqsSink::format_body(&config, &event);
        assert_eq!(body, "plain text message");

        // Non-string falls back to JSON
        let event2 = SourceEvent::record("test", json!({"key": "value"}));
        let body2 = SqsSink::format_body(&config, &event2);
        assert!(body2.contains("\"key\""));
    }

    #[test]
    fn test_sink_format_body_base64() {
        let config = SqsSinkConfig {
            body_format: SqsBodyFormat::Base64,
            ..Default::default()
        };
        let event = SourceEvent::record("test", json!({"key": "value"}));

        let body = SqsSink::format_body(&config, &event);
        // Base64 encoded JSON
        use base64::{engine::general_purpose::STANDARD, Engine};
        let decoded = STANDARD.decode(&body).unwrap();
        let json_str = String::from_utf8(decoded).unwrap();
        assert!(json_str.contains("\"key\""));
    }

    #[test]
    fn test_sink_compress_body_none() {
        let config = SqsSinkConfig {
            compression: SqsCompression::None,
            ..Default::default()
        };
        let body = "test message body";

        let (result, savings) = SqsSink::compress_body(&config, body);
        assert_eq!(result, body);
        assert_eq!(savings, 0);
    }

    #[test]
    fn test_sink_compress_body_gzip() {
        let config = SqsSinkConfig {
            compression: SqsCompression::Gzip,
            ..Default::default()
        };
        // Large text to ensure compression actually helps
        let body = "a".repeat(1000);

        let (result, savings) = SqsSink::compress_body(&config, &body);
        // Gzip should compress repetitive text well
        assert!(result.len() < body.len());
        assert!(savings > 0);
    }

    #[test]
    fn test_sink_compress_body_zstd() {
        let config = SqsSinkConfig {
            compression: SqsCompression::Zstd,
            ..Default::default()
        };
        // Large text to ensure compression actually helps
        let body = "b".repeat(1000);

        let (result, savings) = SqsSink::compress_body(&config, &body);
        // Zstd should compress repetitive text well
        assert!(result.len() < body.len());
        assert!(savings > 0);
    }

    #[test]
    fn test_sink_handle_oversized_fail() {
        let config = SqsSinkConfig {
            oversized_behavior: OversizedMessageBehavior::Fail,
            ..Default::default()
        };
        let metrics = SqsSinkMetrics::new();
        let body = "test";

        let result = SqsSink::handle_oversized(&config, body, &metrics);
        assert!(result.is_none());
        assert_eq!(metrics.oversized_messages.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.skipped_messages.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_sink_handle_oversized_skip() {
        let config = SqsSinkConfig {
            oversized_behavior: OversizedMessageBehavior::Skip,
            ..Default::default()
        };
        let metrics = SqsSinkMetrics::new();
        let body = "test";

        let result = SqsSink::handle_oversized(&config, body, &metrics);
        assert!(result.is_none());
        assert_eq!(metrics.oversized_messages.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.skipped_messages.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_sink_handle_oversized_truncate() {
        let config = SqsSinkConfig {
            oversized_behavior: OversizedMessageBehavior::Truncate,
            ..Default::default()
        };
        let metrics = SqsSinkMetrics::new();
        let body = "a".repeat(300_000);

        let result = SqsSink::handle_oversized(&config, &body, &metrics);
        assert!(result.is_some());
        let truncated = result.unwrap();
        assert!(truncated.len() < body.len());
        assert!(truncated.len() <= SqsSink::MAX_MESSAGE_SIZE);
        assert_eq!(metrics.oversized_messages.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_sink_metrics_new_fields() {
        let metrics = SqsSinkMetrics::new();

        // Increment new counters
        metrics.oversized_messages.fetch_add(5, Ordering::Relaxed);
        metrics.skipped_messages.fetch_add(3, Ordering::Relaxed);
        metrics
            .compression_savings_bytes
            .fetch_add(10000, Ordering::Relaxed);
        metrics
            .circuit_breaker_trips
            .fetch_add(2, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.oversized_messages, 5);
        assert_eq!(snap.skipped_messages, 3);
        assert_eq!(snap.compression_savings_bytes, 10000);
        assert_eq!(snap.circuit_breaker_trips, 2);
    }

    #[test]
    fn test_sink_metrics_compression_ratio() {
        let snap = SqsSinkMetricsSnapshot {
            bytes_sent: 8000,
            compression_savings_bytes: 2000, // Original was 10000
            ..Default::default()
        };

        // Ratio = 8000 / (8000 + 2000) = 0.8
        assert!((snap.compression_ratio() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_sink_metrics_prometheus_new_fields() {
        let metrics = SqsSinkMetrics::new();
        metrics.oversized_messages.fetch_add(10, Ordering::Relaxed);
        metrics
            .circuit_breaker_trips
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .compression_savings_bytes
            .fetch_add(50000, Ordering::Relaxed);

        let snap = metrics.snapshot();
        let prom = snap.to_prometheus_format("rivven");

        assert!(prom.contains("rivven_sqs_sink_oversized_messages_total 10"));
        assert!(prom.contains("rivven_sqs_sink_circuit_breaker_trips_total 1"));
        assert!(prom.contains("rivven_sqs_sink_compression_savings_bytes_total 50000"));
        assert!(prom.contains("rivven_sqs_sink_compression_ratio"));
    }

    #[test]
    fn test_sink_content_based_deduplication_config() {
        let yaml = r#"
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue.fifo"
            region: "us-east-1"
            content_based_deduplication: true
        "#;

        let config: SqsSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.content_based_deduplication);
    }

    #[test]
    fn test_sink_circuit_breaker_config() {
        let yaml = r#"
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
            region: "us-east-1"
            circuit_breaker_threshold: 10
            circuit_breaker_recovery_secs: 120
        "#;

        let config: SqsSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.circuit_breaker_threshold, 10);
        assert_eq!(config.circuit_breaker_recovery_secs, 120);

        // Validation should pass
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_sink_max_message_size_constant() {
        // Verify constant matches SQS limit
        assert_eq!(SqsSink::MAX_MESSAGE_SIZE, 262_144);
    }
}
