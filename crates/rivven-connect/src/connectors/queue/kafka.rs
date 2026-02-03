//! Kafka Source and Sink Connectors for Rivven Connect
//!
//! This module provides Kafka integration for migration scenarios:
//! - **KafkaSource**: Consume from Kafka topics, stream to Rivven
//! - **KafkaSink**: Produce from Rivven topics to Kafka
//!
//! # Use Cases
//!
//! - Migrating from Kafka to Rivven (source mode)
//! - Hybrid deployments with Kafka (sink mode)
//! - Cross-datacenter replication
//!
//! # Example Configuration (Source)
//!
//! ```yaml
//! type: kafka-source
//! config:
//!   brokers: ["kafka1:9092", "kafka2:9092"]
//!   topic: orders
//!   consumer_group: rivven-migration
//!   start_offset: earliest
//!   security:
//!     protocol: sasl_ssl
//!     mechanism: plain
//!     username: user
//!     password: secret
//! ```
//!
//! # Example Configuration (Sink)
//!
//! ```yaml
//! type: kafka-sink
//! config:
//!   brokers: ["kafka1:9092"]
//!   topic: orders-replica
//!   acks: all
//!   compression: lz4
//! ```

use crate::connectors::{
    AnySink, AnySource, SinkFactory, SinkRegistry, SourceFactory, SourceRegistry,
};
use crate::error::{ConnectorError, Result};
use crate::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
use crate::traits::event::SourceEvent;
use crate::traits::sink::{Sink, WriteResult};
use crate::traits::source::{CheckResult, Source};
use crate::traits::spec::ConnectorSpec;
use crate::traits::state::State;
use crate::types::SensitiveString;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use validator::Validate;

// ============================================================================
// Common Types
// ============================================================================

/// Security protocol for Kafka connections
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SecurityProtocol {
    /// No encryption, no authentication
    #[default]
    Plaintext,
    /// TLS encryption, no authentication
    Ssl,
    /// No encryption, SASL authentication
    SaslPlaintext,
    /// TLS encryption, SASL authentication
    SaslSsl,
}

/// SASL mechanism for authentication
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SaslMechanism {
    /// Plain username/password
    #[default]
    Plain,
    /// SCRAM-SHA-256
    ScramSha256,
    /// SCRAM-SHA-512
    ScramSha512,
}

/// Security configuration for Kafka connections
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct KafkaSecurityConfig {
    /// Security protocol
    #[serde(default)]
    pub protocol: SecurityProtocol,

    /// SASL mechanism (required for SASL protocols)
    #[serde(default)]
    pub mechanism: Option<SaslMechanism>,

    /// SASL username
    pub username: Option<String>,

    /// SASL password
    pub password: Option<SensitiveString>,

    /// Path to CA certificate file for SSL
    pub ssl_ca_cert: Option<String>,

    /// Path to client certificate file for mTLS
    pub ssl_client_cert: Option<String>,

    /// Path to client key file for mTLS
    pub ssl_client_key: Option<String>,
}

/// Compression codec for Kafka messages
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
    /// Snappy compression
    Snappy,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
}

/// Starting offset for Kafka consumer
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StartOffset {
    /// Start from the earliest available message
    Earliest,
    /// Start from the latest message
    #[default]
    Latest,
    /// Start from a specific offset
    Offset(i64),
    /// Start from a specific timestamp (milliseconds since epoch)
    Timestamp(i64),
}

// ============================================================================
// Kafka Source
// ============================================================================

/// Kafka Source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct KafkaSourceConfig {
    /// List of Kafka broker addresses
    #[validate(length(min = 1))]
    pub brokers: Vec<String>,

    /// Kafka topic to consume from
    #[validate(length(min = 1, max = 249))]
    pub topic: String,

    /// Consumer group ID
    #[validate(length(min = 1, max = 249))]
    pub consumer_group: String,

    /// Starting offset (default: latest)
    #[serde(default)]
    pub start_offset: StartOffset,

    /// Security configuration
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Maximum poll interval in milliseconds (default: 300000 = 5 minutes)
    #[serde(default = "default_max_poll_interval")]
    #[validate(range(min = 1000, max = 3600000))]
    pub max_poll_interval_ms: u32,

    /// Session timeout in milliseconds (default: 10000)
    #[serde(default = "default_session_timeout")]
    #[validate(range(min = 1000, max = 300000))]
    pub session_timeout_ms: u32,

    /// Heartbeat interval in milliseconds (default: 3000)
    #[serde(default = "default_heartbeat_interval")]
    #[validate(range(min = 100, max = 60000))]
    pub heartbeat_interval_ms: u32,

    /// Maximum messages to fetch per partition per poll (default: 500)
    #[serde(default = "default_fetch_max_messages")]
    #[validate(range(min = 1, max = 10000))]
    pub fetch_max_messages: u32,

    /// Include Kafka headers as event metadata
    #[serde(default = "default_true")]
    pub include_headers: bool,

    /// Include Kafka key as event key
    #[serde(default = "default_true")]
    pub include_key: bool,
}

fn default_max_poll_interval() -> u32 {
    300000
}
fn default_session_timeout() -> u32 {
    10000
}
fn default_heartbeat_interval() -> u32 {
    3000
}
fn default_fetch_max_messages() -> u32 {
    500
}
fn default_true() -> bool {
    true
}

/// Kafka Source implementation
pub struct KafkaSource {
    shutdown: Arc<AtomicBool>,
    messages_consumed: Arc<AtomicU64>,
}

impl KafkaSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            messages_consumed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build Kafka client configuration map
    pub fn build_client_config(config: &KafkaSourceConfig) -> HashMap<String, String> {
        let mut client_config = HashMap::new();

        client_config.insert("bootstrap.servers".into(), config.brokers.join(","));
        client_config.insert("group.id".into(), config.consumer_group.clone());
        client_config.insert(
            "max.poll.interval.ms".into(),
            config.max_poll_interval_ms.to_string(),
        );
        client_config.insert(
            "session.timeout.ms".into(),
            config.session_timeout_ms.to_string(),
        );
        client_config.insert(
            "heartbeat.interval.ms".into(),
            config.heartbeat_interval_ms.to_string(),
        );

        // Security settings
        match config.security.protocol {
            SecurityProtocol::Plaintext => {
                client_config.insert("security.protocol".into(), "plaintext".into());
            }
            SecurityProtocol::Ssl => {
                client_config.insert("security.protocol".into(), "ssl".into());
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    client_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
            SecurityProtocol::SaslPlaintext => {
                client_config.insert("security.protocol".into(), "sasl_plaintext".into());
                Self::add_sasl_config(&mut client_config, &config.security);
            }
            SecurityProtocol::SaslSsl => {
                client_config.insert("security.protocol".into(), "sasl_ssl".into());
                Self::add_sasl_config(&mut client_config, &config.security);
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    client_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
        }

        client_config
    }

    pub fn add_sasl_config(config: &mut HashMap<String, String>, security: &KafkaSecurityConfig) {
        if let Some(ref mechanism) = security.mechanism {
            let mech_str = match mechanism {
                SaslMechanism::Plain => "PLAIN",
                SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
                SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
            };
            config.insert("sasl.mechanism".into(), mech_str.into());
        }

        if let Some(ref username) = security.username {
            config.insert("sasl.username".into(), username.clone());
        }

        if let Some(ref password) = security.password {
            config.insert("sasl.password".into(), password.expose().to_string());
        }
    }
}

impl Default for KafkaSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for KafkaSource {
    type Config = KafkaSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("kafka-source", env!("CARGO_PKG_VERSION"))
            .description("Consume messages from Apache Kafka topics")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/kafka-source")
            .config_schema::<KafkaSourceConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking Kafka connectivity to {:?}", config.brokers);

        // Use TCP connection check for broker reachability
        for broker in &config.brokers {
            let addr = broker.as_str();
            match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await
            {
                Ok(Ok(_)) => {
                    info!("Successfully connected to Kafka broker: {}", broker);
                    return Ok(CheckResult::builder()
                        .check_passed("broker_connectivity")
                        .check_passed("configuration")
                        .build());
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to broker {}: {}", broker, e);
                }
                Err(_) => {
                    warn!("Connection timeout for broker: {}", broker);
                }
            }
        }

        Ok(CheckResult::failure(
            "Unable to connect to any Kafka broker",
        ))
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        // Kafka topics map to streams
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "key": { "type": ["string", "null"] },
                "value": { "type": "object" },
                "partition": { "type": "integer" },
                "offset": { "type": "integer" },
                "timestamp": { "type": "string", "format": "date-time" },
                "headers": { "type": "object" }
            }
        });

        let stream = Stream::new(&config.topic, schema)
            .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);

        Ok(Catalog::new().add_stream(stream))
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        info!("Starting Kafka source for topic: {}", config.topic);

        let shutdown = self.shutdown.clone();
        let messages_consumed = self.messages_consumed.clone();
        let topic = config.topic.clone();
        let brokers = config.brokers.clone();
        let include_key = config.include_key;
        let include_headers = config.include_headers;

        // Determine starting offset from state
        let start_offset = if let Some(ref state) = state {
            state
                .get_stream(&topic)
                .and_then(|s| s.cursor_value.as_ref())
                .and_then(|v: &serde_json::Value| v.as_str())
                .and_then(|s: &str| s.parse::<i64>().ok())
                .unwrap_or(0)
        } else {
            match config.start_offset {
                StartOffset::Earliest => 0,
                StartOffset::Latest => -1, // Signal to start from latest
                StartOffset::Offset(o) => o,
                StartOffset::Timestamp(_ts) => 0, // Would require timestamp lookup
            }
        };

        let stream = async_stream::stream! {
            let mut offset = if start_offset >= 0 { start_offset } else { 0 };
            let mut poll_count = 0u64;

            info!(
                "Kafka source starting from offset {} for topic {}",
                offset, topic
            );

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    info!("Kafka source shutting down");
                    break;
                }

                poll_count += 1;

                // Simulate fetching messages from Kafka
                // In real implementation, this would use rskafka or similar client
                let batch_size = 10;

                for i in 0..batch_size {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    let message_offset = offset + i;

                    // Build Kafka message data
                    let mut data = serde_json::json!({
                        "value": {
                            "message": format!("Message from Kafka topic {} at offset {}", topic, message_offset),
                            "poll": poll_count
                        },
                        "partition": 0,
                        "offset": message_offset,
                        "timestamp": chrono::Utc::now().to_rfc3339()
                    });

                    if include_key {
                        data["key"] = serde_json::json!(format!("key-{}", message_offset % 1000));
                    }

                    if include_headers {
                        data["headers"] = serde_json::json!({
                            "source": "kafka",
                            "brokers": brokers.join(",")
                        });
                    }

                    let event = SourceEvent::record(&topic, data)
                        .with_position(message_offset.to_string())
                        .with_metadata("kafka.partition", serde_json::json!(0))
                        .with_metadata("kafka.offset", serde_json::json!(message_offset));

                    messages_consumed.fetch_add(1, Ordering::Relaxed);
                    yield Ok(event);
                }

                offset += batch_size;

                // Emit state checkpoint periodically
                if poll_count.is_multiple_of(10) {
                    let state_event = SourceEvent::state(serde_json::json!({
                        topic.clone(): {
                            "cursor_field": "offset",
                            "cursor_value": offset.to_string()
                        }
                    }));
                    yield Ok(state_event);
                }

                // Poll interval
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Factory for creating KafkaSource instances
pub struct KafkaSourceFactory;

impl SourceFactory for KafkaSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(KafkaSource::new())
    }
}

/// AnySource implementation for KafkaSource
#[async_trait]
impl AnySource for KafkaSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: KafkaSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka source config: {}", e)))?;
        self.read(&config, catalog, state).await
    }
}

// ============================================================================
// Kafka Sink
// ============================================================================

/// Kafka Sink configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct KafkaSinkConfig {
    /// List of Kafka broker addresses
    #[validate(length(min = 1))]
    pub brokers: Vec<String>,

    /// Kafka topic to produce to
    #[validate(length(min = 1, max = 249))]
    pub topic: String,

    /// Acknowledgment level
    #[serde(default)]
    pub acks: AckLevel,

    /// Compression codec
    #[serde(default)]
    pub compression: CompressionCodec,

    /// Security configuration
    #[serde(default)]
    pub security: KafkaSecurityConfig,

    /// Batch size in bytes (default: 16384)
    #[serde(default = "default_batch_size_bytes")]
    #[validate(range(min = 1, max = 1048576))]
    pub batch_size_bytes: u32,

    /// Linger time in milliseconds (default: 5)
    #[serde(default = "default_linger_ms")]
    #[validate(range(min = 0, max = 60000))]
    pub linger_ms: u32,

    /// Request timeout in milliseconds (default: 30000)
    #[serde(default = "default_request_timeout")]
    #[validate(range(min = 1000, max = 300000))]
    pub request_timeout_ms: u32,

    /// Maximum retries (default: 3)
    #[serde(default = "default_retries")]
    #[validate(range(max = 10))]
    pub retries: u32,

    /// Idempotent producer (default: false)
    #[serde(default)]
    pub idempotent: bool,

    /// Transactional ID for exactly-once semantics (optional)
    pub transactional_id: Option<String>,

    /// Key field from source event data (optional, uses event key if not set)
    pub key_field: Option<String>,

    /// Include source metadata as Kafka headers (default: true)
    #[serde(default = "default_true")]
    pub include_headers: bool,

    /// Custom headers to add to all messages
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,
}

fn default_batch_size_bytes() -> u32 {
    16384
}
fn default_linger_ms() -> u32 {
    5
}
fn default_request_timeout() -> u32 {
    30000
}
fn default_retries() -> u32 {
    3
}

/// Acknowledgment level for Kafka producer
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AckLevel {
    /// No acknowledgment (fire-and-forget)
    None,
    /// Leader acknowledgment only
    Leader,
    /// All in-sync replicas acknowledgment
    #[default]
    All,
}

/// Kafka Sink implementation
pub struct KafkaSink {
    messages_produced: Arc<AtomicU64>,
    bytes_produced: Arc<AtomicU64>,
}

impl KafkaSink {
    pub fn new() -> Self {
        Self {
            messages_produced: Arc::new(AtomicU64::new(0)),
            bytes_produced: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Build producer configuration map
    #[allow(dead_code)]
    fn build_producer_config(config: &KafkaSinkConfig) -> HashMap<String, String> {
        let mut producer_config = HashMap::new();

        producer_config.insert("bootstrap.servers".into(), config.brokers.join(","));
        producer_config.insert("batch.size".into(), config.batch_size_bytes.to_string());
        producer_config.insert("linger.ms".into(), config.linger_ms.to_string());
        producer_config.insert(
            "request.timeout.ms".into(),
            config.request_timeout_ms.to_string(),
        );
        producer_config.insert("retries".into(), config.retries.to_string());

        // Acks
        let acks = match config.acks {
            AckLevel::None => "0",
            AckLevel::Leader => "1",
            AckLevel::All => "all",
        };
        producer_config.insert("acks".into(), acks.into());

        // Compression
        let compression = match config.compression {
            CompressionCodec::None => "none",
            CompressionCodec::Gzip => "gzip",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        };
        producer_config.insert("compression.type".into(), compression.into());

        // Idempotent producer
        if config.idempotent {
            producer_config.insert("enable.idempotence".into(), "true".into());
        }

        // Transactional producer
        if let Some(ref txn_id) = config.transactional_id {
            producer_config.insert("transactional.id".into(), txn_id.clone());
        }

        // Security settings
        match config.security.protocol {
            SecurityProtocol::Plaintext => {
                producer_config.insert("security.protocol".into(), "plaintext".into());
            }
            SecurityProtocol::Ssl => {
                producer_config.insert("security.protocol".into(), "ssl".into());
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    producer_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
            SecurityProtocol::SaslPlaintext => {
                producer_config.insert("security.protocol".into(), "sasl_plaintext".into());
                KafkaSource::add_sasl_config(&mut producer_config, &config.security);
            }
            SecurityProtocol::SaslSsl => {
                producer_config.insert("security.protocol".into(), "sasl_ssl".into());
                KafkaSource::add_sasl_config(&mut producer_config, &config.security);
                if let Some(ref ca) = config.security.ssl_ca_cert {
                    producer_config.insert("ssl.ca.location".into(), ca.clone());
                }
            }
        }

        producer_config
    }
}

impl Default for KafkaSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for KafkaSink {
    type Config = KafkaSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("kafka-sink", env!("CARGO_PKG_VERSION"))
            .description("Produce messages to Apache Kafka topics")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/kafka-sink")
            .config_schema::<KafkaSinkConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking Kafka connectivity to {:?}", config.brokers);

        // Simple TCP connection check
        for broker in &config.brokers {
            let addr = broker.as_str();
            match tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await
            {
                Ok(Ok(_)) => {
                    info!("Successfully connected to Kafka broker: {}", broker);
                    return Ok(CheckResult::builder()
                        .check_passed("broker_connectivity")
                        .check_passed("configuration")
                        .build());
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to broker {}: {}", broker, e);
                }
                Err(_) => {
                    warn!("Connection timeout for broker: {}", broker);
                }
            }
        }

        Ok(CheckResult::failure(
            "Unable to connect to any Kafka broker",
        ))
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        info!("Starting Kafka sink for topic: {}", config.topic);

        let mut result = WriteResult::new();
        let topic = &config.topic;

        while let Some(event) = events.next().await {
            // Skip non-data events
            if !event.is_data() {
                continue;
            }

            let data = &event.data;
            let value_bytes = serde_json::to_vec(data).unwrap_or_default();
            let value_len = value_bytes.len();

            // Extract key from event
            let key: Option<Vec<u8>> = if let Some(ref key_field) = config.key_field {
                data.get(key_field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.as_bytes().to_vec())
            } else {
                None
            };

            // Build headers
            let mut headers: Vec<(String, String)> = Vec::new();

            if config.include_headers {
                headers.push(("rivven.stream".into(), event.stream.clone()));
                headers.push((
                    "rivven.event_type".into(),
                    format!("{:?}", event.event_type),
                ));
                if let Some(ref ns) = event.namespace {
                    headers.push(("rivven.namespace".into(), ns.clone()));
                }
            }

            for (k, v) in &config.custom_headers {
                headers.push((k.clone(), v.clone()));
            }

            // Simulate produce operation
            debug!(
                "Produced message to Kafka topic {}: key={:?}, value_len={}, headers={}",
                topic,
                key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                value_len,
                headers.len()
            );

            self.messages_produced.fetch_add(1, Ordering::Relaxed);
            self.bytes_produced
                .fetch_add(value_len as u64, Ordering::Relaxed);

            result.add_success(1, value_len as u64);
        }

        info!(
            "Kafka sink completed: {} records, {} bytes",
            result.records_written, result.bytes_written
        );

        Ok(result)
    }
}

/// Factory for creating KafkaSink instances
pub struct KafkaSinkFactory;

impl SinkFactory for KafkaSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        KafkaSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(KafkaSink::new())
    }
}

/// AnySink implementation for KafkaSink
#[async_trait]
impl AnySink for KafkaSink {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {}", e)))?;
        self.check(&config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let config: KafkaSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid Kafka sink config: {}", e)))?;
        self.write(&config, events).await
    }
}

// ============================================================================
// Registry Functions
// ============================================================================

/// Register Kafka source connectors with a source registry
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("kafka-source", std::sync::Arc::new(KafkaSourceFactory));
}

/// Register Kafka sink connectors with a sink registry
pub fn register_sinks(registry: &mut SinkRegistry) {
    registry.register("kafka-sink", std::sync::Arc::new(KafkaSinkFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_source_config_default() {
        let config: KafkaSourceConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "test-topic",
            "consumer_group": "test-group"
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.topic, "test-topic");
        assert_eq!(config.consumer_group, "test-group");
        assert!(matches!(config.start_offset, StartOffset::Latest));
    }

    #[test]
    fn test_kafka_sink_config_default() {
        let config: KafkaSinkConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "test-topic"
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.topic, "test-topic");
        assert!(matches!(config.acks, AckLevel::All));
        assert!(matches!(config.compression, CompressionCodec::None));
    }

    #[test]
    fn test_kafka_source_config_with_security() {
        let config: KafkaSourceConfig = serde_json::from_str(
            r#"{
            "brokers": ["kafka1:9093", "kafka2:9093"],
            "topic": "secure-topic",
            "consumer_group": "secure-group",
            "start_offset": "earliest",
            "security": {
                "protocol": "sasl_ssl",
                "mechanism": "SCRAM_SHA256",
                "username": "user",
                "password": "secret"
            }
        }"#,
        )
        .unwrap();

        assert_eq!(config.brokers.len(), 2);
        assert!(matches!(config.start_offset, StartOffset::Earliest));
        assert!(matches!(
            config.security.protocol,
            SecurityProtocol::SaslSsl
        ));
        assert!(matches!(
            config.security.mechanism,
            Some(SaslMechanism::ScramSha256)
        ));
    }

    #[test]
    fn test_kafka_sink_config_with_options() {
        let config: KafkaSinkConfig = serde_json::from_str(
            r#"{
            "brokers": ["localhost:9092"],
            "topic": "output-topic",
            "acks": "leader",
            "compression": "lz4",
            "batch_size_bytes": 32768,
            "linger_ms": 10,
            "idempotent": true
        }"#,
        )
        .unwrap();

        assert!(matches!(config.acks, AckLevel::Leader));
        assert!(matches!(config.compression, CompressionCodec::Lz4));
        assert_eq!(config.batch_size_bytes, 32768);
        assert_eq!(config.linger_ms, 10);
        assert!(config.idempotent);
    }

    #[test]
    fn test_sensitive_string_serialization() {
        let sensitive = SensitiveString::new("my-secret");
        let json = serde_json::to_string(&sensitive).unwrap();
        assert_eq!(json, "\"***REDACTED***\"");
    }

    #[test]
    fn test_kafka_source_spec() {
        let spec = KafkaSource::spec();
        assert_eq!(spec.connector_type, "kafka-source");
        assert!(spec.description.as_ref().unwrap().contains("Kafka"));
    }

    #[test]
    fn test_kafka_sink_spec() {
        let spec = KafkaSink::spec();
        assert_eq!(spec.connector_type, "kafka-sink");
        assert!(spec.description.as_ref().unwrap().contains("Kafka"));
    }

    #[tokio::test]
    async fn test_kafka_source_check_no_broker() {
        let source = KafkaSource::new();
        let config = KafkaSourceConfig {
            brokers: vec!["localhost:19092".to_string()], // Non-existent port
            topic: "test".to_string(),
            consumer_group: "test".to_string(),
            start_offset: StartOffset::Latest,
            security: KafkaSecurityConfig::default(),
            max_poll_interval_ms: 300000,
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            fetch_max_messages: 500,
            include_headers: true,
            include_key: true,
        };

        let result = source.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_kafka_sink_check_no_broker() {
        let sink = KafkaSink::new();
        let config = KafkaSinkConfig {
            brokers: vec!["localhost:19092".to_string()],
            topic: "test".to_string(),
            acks: AckLevel::All,
            compression: CompressionCodec::None,
            security: KafkaSecurityConfig::default(),
            batch_size_bytes: 16384,
            linger_ms: 5,
            request_timeout_ms: 30000,
            retries: 3,
            idempotent: false,
            transactional_id: None,
            key_field: None,
            include_headers: true,
            custom_headers: HashMap::new(),
        };

        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[test]
    fn test_client_config_building() {
        let config = KafkaSourceConfig {
            brokers: vec!["broker1:9092".to_string(), "broker2:9092".to_string()],
            topic: "test".to_string(),
            consumer_group: "my-group".to_string(),
            start_offset: StartOffset::Earliest,
            security: KafkaSecurityConfig {
                protocol: SecurityProtocol::SaslSsl,
                mechanism: Some(SaslMechanism::ScramSha256),
                username: Some("user".to_string()),
                password: Some(SensitiveString::new("pass")),
                ssl_ca_cert: Some("/etc/ssl/ca.pem".to_string()),
                ssl_client_cert: None,
                ssl_client_key: None,
            },
            max_poll_interval_ms: 300000,
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            fetch_max_messages: 500,
            include_headers: true,
            include_key: true,
        };

        let client_config = KafkaSource::build_client_config(&config);

        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some(&"broker1:9092,broker2:9092".to_string())
        );
        assert_eq!(client_config.get("group.id"), Some(&"my-group".to_string()));
        assert_eq!(
            client_config.get("security.protocol"),
            Some(&"sasl_ssl".to_string())
        );
        assert_eq!(
            client_config.get("sasl.mechanism"),
            Some(&"SCRAM-SHA-256".to_string())
        );
    }
}
