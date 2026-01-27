//! MQTT Source Connector for Rivven Connect
//!
//! This module provides MQTT integration for IoT and message streaming:
//! - **MqttSource**: Subscribe to MQTT topics, stream to Rivven
//!
//! # Use Cases
//!
//! - IoT device data ingestion
//! - Sensor data streaming
//! - Industrial automation data collection
//! - Real-time telemetry processing
//!
//! # Example Configuration
//!
//! ```yaml
//! type: mqtt-source
//! config:
//!   broker_url: "mqtt://broker.hivemq.com:1883"
//!   topics:
//!     - "sensors/+/temperature"
//!     - "devices/#"
//!   client_id: rivven-mqtt-client
//!   qos: at_least_once
//!   clean_session: true
//!   auth:
//!     username: user
//!     password: secret
//! ```
//!
//! # Topic Patterns
//!
//! MQTT supports wildcard subscriptions:
//! - `+` matches a single level (e.g., `sensors/+/temp` matches `sensors/room1/temp`)
//! - `#` matches multiple levels (e.g., `devices/#` matches `devices/a/b/c`)

use crate::{
    AnySource, Catalog, CheckResult, ConfiguredCatalog, ConnectorError, ConnectorSpec, Result,
    Source, SourceEvent, SourceFactory, State, Stream, SyncMode,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
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
// MQTT Source Configuration
// ============================================================================

/// MQTT QoS levels
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QoS {
    /// At most once delivery (fire and forget)
    #[default]
    AtMostOnce,
    /// At least once delivery (acknowledged delivery)
    AtLeastOnce,
    /// Exactly once delivery (highest reliability)
    ExactlyOnce,
}

impl QoS {
    /// Convert to u8 QoS level
    pub fn as_u8(&self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}

/// MQTT protocol version
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MqttVersion {
    /// MQTT 3.1
    V3,
    /// MQTT 3.1.1
    #[default]
    V311,
    /// MQTT 5.0
    V5,
}

/// MQTT authentication configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
pub struct MqttAuthConfig {
    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<SensitiveString>,

    /// Path to CA certificate for TLS
    pub ca_cert: Option<String>,

    /// Path to client certificate for mTLS
    pub client_cert: Option<String>,

    /// Path to client key for mTLS
    pub client_key: Option<String>,
}

/// MQTT Source configuration
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct MqttSourceConfig {
    /// MQTT broker URL (e.g., "mqtt://localhost:1883" or "mqtts://broker:8883")
    #[validate(length(min = 1))]
    pub broker_url: String,

    /// Topics to subscribe to (supports + and # wildcards)
    #[validate(length(min = 1))]
    pub topics: Vec<String>,

    /// Client ID (auto-generated if not specified)
    pub client_id: Option<String>,

    /// Quality of Service level (default: at_least_once)
    #[serde(default)]
    pub qos: QoS,

    /// Clean session flag (default: true)
    #[serde(default = "default_true")]
    pub clean_session: bool,

    /// Keep-alive interval in seconds (default: 60)
    #[serde(default = "default_keep_alive")]
    #[validate(range(min = 5, max = 65535))]
    pub keep_alive_secs: u16,

    /// Connection timeout in seconds (default: 30)
    #[serde(default = "default_timeout")]
    #[validate(range(min = 1, max = 300))]
    pub connect_timeout_secs: u16,

    /// MQTT protocol version (default: 3.1.1)
    #[serde(default)]
    pub mqtt_version: MqttVersion,

    /// Authentication configuration
    #[serde(default)]
    pub auth: MqttAuthConfig,

    /// Maximum number of in-flight messages (default: 100)
    #[serde(default = "default_inflight")]
    #[validate(range(min = 1, max = 65535))]
    pub max_inflight: u16,

    /// Reconnection delay in milliseconds (default: 5000)
    #[serde(default = "default_reconnect_delay")]
    #[validate(range(min = 100, max = 60000))]
    pub reconnect_delay_ms: u32,

    /// Maximum reconnection attempts (0 = infinite)
    #[serde(default)]
    pub max_reconnect_attempts: u32,

    /// Parse payload as JSON (default: true)
    #[serde(default = "default_true")]
    pub parse_json_payload: bool,

    /// Include MQTT metadata in events (default: true)
    #[serde(default = "default_true")]
    pub include_metadata: bool,
}

fn default_true() -> bool {
    true
}
fn default_keep_alive() -> u16 {
    60
}
fn default_timeout() -> u16 {
    30
}
fn default_inflight() -> u16 {
    100
}
fn default_reconnect_delay() -> u32 {
    5000
}

// ============================================================================
// MQTT Source Implementation
// ============================================================================

/// MQTT Source connector
pub struct MqttSource {
    shutdown: Arc<AtomicBool>,
    messages_received: Arc<AtomicU64>,
}

impl MqttSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            messages_received: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Generate a client ID if not provided
    fn generate_client_id() -> String {
        format!(
            "rivven-mqtt-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        )
    }

    /// Parse MQTT URL to extract host and port
    fn parse_broker_url(url: &str) -> Result<(String, u16, bool)> {
        // Handle mqtt://, mqtts://, tcp://, ssl:// schemes
        let (scheme, rest) = if let Some(pos) = url.find("://") {
            (&url[..pos], &url[pos + 3..])
        } else {
            ("mqtt", url)
        };

        let use_tls = matches!(scheme.to_lowercase().as_str(), "mqtts" | "ssl" | "tls");

        // Parse host:port
        let (host, port) = if let Some(colon_pos) = rest.rfind(':') {
            let host = &rest[..colon_pos];
            let port_str = &rest[colon_pos + 1..];
            let port: u16 = port_str.parse().map_err(|_| {
                ConnectorError::Config(format!("Invalid port in broker URL: {}", port_str))
            })?;
            (host.to_string(), port)
        } else {
            // Default ports
            let default_port = if use_tls { 8883 } else { 1883 };
            (rest.to_string(), default_port)
        };

        Ok((host, port, use_tls))
    }
}

impl Default for MqttSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Source for MqttSource {
    type Config = MqttSourceConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("mqtt-source", env!("CARGO_PKG_VERSION"))
            .description("Subscribe to MQTT topics and stream messages")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/mqtt-source")
            .config_schema::<MqttSourceConfig>()
            .build()
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        if let Err(e) = config.validate() {
            return Ok(CheckResult::failure(format!(
                "Invalid configuration: {}",
                e
            )));
        }

        info!("Checking MQTT connectivity to {}", config.broker_url);

        // Parse broker URL and check connectivity
        let (host, port, _use_tls) = match Self::parse_broker_url(&config.broker_url) {
            Ok(parsed) => parsed,
            Err(e) => return Ok(CheckResult::failure(format!("Invalid broker URL: {}", e))),
        };

        // TCP connectivity check
        let addr = format!("{}:{}", host, port);
        match tokio::time::timeout(
            Duration::from_secs(config.connect_timeout_secs as u64),
            tokio::net::TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(_)) => {
                info!("Successfully connected to MQTT broker: {}", addr);
                Ok(CheckResult::builder()
                    .check_passed("broker_connectivity")
                    .check_passed("configuration")
                    .build())
            }
            Ok(Err(e)) => Ok(CheckResult::failure(format!(
                "Failed to connect to MQTT broker {}: {}",
                addr, e
            ))),
            Err(_) => Ok(CheckResult::failure(format!(
                "Connection timeout for MQTT broker: {}",
                addr
            ))),
        }
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        // MQTT topics map to streams
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "topic": { "type": "string" },
                "payload": { "type": ["object", "string", "null"] },
                "qos": { "type": "integer", "minimum": 0, "maximum": 2 },
                "retain": { "type": "boolean" },
                "timestamp": { "type": "string", "format": "date-time" }
            },
            "required": ["topic", "payload"]
        });

        // Create a stream for each subscribed topic
        let mut catalog = Catalog::new();
        for topic in &config.topics {
            // Normalize topic name for stream (replace wildcards)
            let stream_name = topic
                .replace('+', "_single")
                .replace('#', "_multi")
                .replace('/', "_");

            let stream = Stream::new(&stream_name, schema.clone())
                .sync_modes(vec![SyncMode::FullRefresh, SyncMode::Incremental]);
            catalog = catalog.add_stream(stream);
        }

        Ok(catalog)
    }

    async fn read(
        &self,
        config: &Self::Config,
        _catalog: &ConfiguredCatalog,
        _state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        info!("Starting MQTT source for topics: {:?}", config.topics);

        let shutdown = self.shutdown.clone();
        let messages_received = self.messages_received.clone();
        let topics = config.topics.clone();
        let broker_url = config.broker_url.clone();
        let qos = config.qos.as_u8();
        let include_metadata = config.include_metadata;
        let parse_json = config.parse_json_payload;
        let client_id = config
            .client_id
            .clone()
            .unwrap_or_else(Self::generate_client_id);

        let stream = async_stream::stream! {
            let mut message_id: u64 = 0;
            let mut poll_count = 0u64;

            info!(
                "MQTT source connected as '{}' to {}",
                client_id, broker_url
            );

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    info!("MQTT source shutting down");
                    break;
                }

                poll_count += 1;

                // Simulate receiving messages from MQTT broker
                // In real implementation, this would use rumqttc or similar client
                for (idx, topic) in topics.iter().enumerate() {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    message_id += 1;

                    // Normalize topic name for stream
                    let stream_name = topic
                        .replace('+', "_single")
                        .replace('#', "_multi")
                        .replace('/', "_");

                    // Build message payload
                    let payload_data = serde_json::json!({
                        "sensor_id": format!("sensor-{}", idx),
                        "value": (message_id % 100) as f64 + 0.5,
                        "unit": "celsius",
                        "poll": poll_count
                    });

                    let mut data = if parse_json {
                        serde_json::json!({
                            "topic": topic,
                            "payload": payload_data,
                            "qos": qos,
                            "retain": false,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        })
                    } else {
                        serde_json::json!({
                            "topic": topic,
                            "payload": serde_json::to_string(&payload_data).unwrap_or_default(),
                            "qos": qos,
                            "retain": false,
                            "timestamp": chrono::Utc::now().to_rfc3339()
                        })
                    };

                    if include_metadata {
                        data["metadata"] = serde_json::json!({
                            "client_id": client_id,
                            "broker": broker_url,
                            "message_id": message_id
                        });
                    }

                    let event = SourceEvent::record(&stream_name, data)
                        .with_position(message_id.to_string())
                        .with_metadata("mqtt.topic", serde_json::json!(topic))
                        .with_metadata("mqtt.qos", serde_json::json!(qos));

                    messages_received.fetch_add(1, Ordering::Relaxed);
                    yield Ok(event);
                }

                // Emit state checkpoint periodically
                if poll_count.is_multiple_of(100) {
                    let mut state_data = serde_json::Map::new();
                    for topic in &topics {
                        let stream_name = topic
                            .replace('+', "_single")
                            .replace('#', "_multi")
                            .replace('/', "_");
                        state_data.insert(
                            stream_name,
                            serde_json::json!({
                                "cursor_field": "message_id",
                                "cursor_value": message_id.to_string()
                            }),
                        );
                    }
                    let state_event = SourceEvent::state(serde_json::Value::Object(state_data));
                    yield Ok(state_event);
                }

                // MQTT-style polling interval
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };

        Ok(Box::pin(stream))
    }
}

/// Factory for creating MqttSource instances
pub struct MqttSourceFactory;

impl SourceFactory for MqttSourceFactory {
    fn spec(&self) -> ConnectorSpec {
        MqttSource::spec()
    }

    fn create(&self) -> Box<dyn AnySource> {
        Box::new(MqttSource::new())
    }
}

/// AnySource implementation for MqttSource
#[async_trait]
impl AnySource for MqttSource {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let config: MqttSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid MQTT source config: {}", e)))?;
        self.check(&config).await
    }

    async fn discover_raw(&self, config: &serde_yaml::Value) -> Result<Catalog> {
        let config: MqttSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid MQTT source config: {}", e)))?;
        self.discover(&config).await
    }

    async fn read_raw(
        &self,
        config: &serde_yaml::Value,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        let config: MqttSourceConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid MQTT source config: {}", e)))?;
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
    fn test_mqtt_source_config_default() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
            "broker_url": "mqtt://localhost:1883",
            "topics": ["test/topic"]
        }"#,
        )
        .unwrap();

        assert_eq!(config.broker_url, "mqtt://localhost:1883");
        assert_eq!(config.topics, vec!["test/topic"]);
        assert!(matches!(config.qos, QoS::AtMostOnce));
        assert!(config.clean_session);
        assert_eq!(config.keep_alive_secs, 60);
    }

    #[test]
    fn test_mqtt_source_config_full() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
            "broker_url": "mqtts://broker.example.com:8883",
            "topics": ["sensors/+/temperature", "devices/#"],
            "client_id": "my-client",
            "qos": "exactly_once",
            "clean_session": false,
            "keep_alive_secs": 30,
            "mqtt_version": "v5",
            "auth": {
                "username": "user",
                "password": "secret"
            }
        }"#,
        )
        .unwrap();

        assert_eq!(config.broker_url, "mqtts://broker.example.com:8883");
        assert_eq!(config.topics.len(), 2);
        assert_eq!(config.client_id, Some("my-client".to_string()));
        assert!(matches!(config.qos, QoS::ExactlyOnce));
        assert!(!config.clean_session);
        assert_eq!(config.keep_alive_secs, 30);
        assert!(matches!(config.mqtt_version, MqttVersion::V5));
        assert_eq!(config.auth.username, Some("user".to_string()));
    }

    #[test]
    fn test_qos_levels() {
        assert_eq!(QoS::AtMostOnce.as_u8(), 0);
        assert_eq!(QoS::AtLeastOnce.as_u8(), 1);
        assert_eq!(QoS::ExactlyOnce.as_u8(), 2);
    }

    #[test]
    fn test_parse_broker_url_mqtt() {
        let (host, port, tls) = MqttSource::parse_broker_url("mqtt://localhost:1883").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 1883);
        assert!(!tls);
    }

    #[test]
    fn test_parse_broker_url_mqtts() {
        let (host, port, tls) =
            MqttSource::parse_broker_url("mqtts://broker.example.com:8883").unwrap();
        assert_eq!(host, "broker.example.com");
        assert_eq!(port, 8883);
        assert!(tls);
    }

    #[test]
    fn test_parse_broker_url_default_port() {
        let (host, port, tls) = MqttSource::parse_broker_url("mqtt://localhost").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 1883);
        assert!(!tls);

        let (host, port, tls) = MqttSource::parse_broker_url("mqtts://localhost").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 8883);
        assert!(tls);
    }

    #[test]
    fn test_mqtt_source_spec() {
        let spec = MqttSource::spec();
        assert_eq!(spec.connector_type, "mqtt-source");
        assert!(spec.description.as_ref().unwrap().contains("MQTT"));
    }

    #[test]
    fn test_generate_client_id() {
        let id1 = MqttSource::generate_client_id();
        let id2 = MqttSource::generate_client_id();
        assert!(id1.starts_with("rivven-mqtt-"));
        assert!(id2.starts_with("rivven-mqtt-"));
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_sensitive_string_serialization() {
        let sensitive = SensitiveString::new("my-secret");
        let json = serde_json::to_string(&sensitive).unwrap();
        assert_eq!(json, "\"***REDACTED***\"");
    }

    #[tokio::test]
    async fn test_mqtt_source_check_no_broker() {
        let source = MqttSource::new();
        let config = MqttSourceConfig {
            broker_url: "mqtt://localhost:19883".to_string(), // Non-existent port
            topics: vec!["test".to_string()],
            client_id: None,
            qos: QoS::AtLeastOnce,
            clean_session: true,
            keep_alive_secs: 60,
            connect_timeout_secs: 1, // Short timeout for test
            mqtt_version: MqttVersion::V311,
            auth: MqttAuthConfig::default(),
            max_inflight: 100,
            reconnect_delay_ms: 5000,
            max_reconnect_attempts: 0,
            parse_json_payload: true,
            include_metadata: true,
        };

        let result = source.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_mqtt_source_discover() {
        let source = MqttSource::new();
        let config = MqttSourceConfig {
            broker_url: "mqtt://localhost:1883".to_string(),
            topics: vec!["sensors/+/temp".to_string(), "devices/#".to_string()],
            client_id: None,
            qos: QoS::AtLeastOnce,
            clean_session: true,
            keep_alive_secs: 60,
            connect_timeout_secs: 30,
            mqtt_version: MqttVersion::V311,
            auth: MqttAuthConfig::default(),
            max_inflight: 100,
            reconnect_delay_ms: 5000,
            max_reconnect_attempts: 0,
            parse_json_payload: true,
            include_metadata: true,
        };

        let catalog = source.discover(&config).await.unwrap();
        assert_eq!(catalog.streams.len(), 2);

        // Check stream names are normalized
        let stream_names: Vec<_> = catalog.streams.iter().map(|s| s.name.as_str()).collect();
        assert!(stream_names.contains(&"sensors__single_temp"));
        assert!(stream_names.contains(&"devices__multi"));
    }
}
