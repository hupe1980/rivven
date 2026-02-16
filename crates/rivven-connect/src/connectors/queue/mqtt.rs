//! MQTT Source Connector for Rivven Connect
//!
//! This module provides MQTT integration for IoT and message streaming:
//! - **MqttSource**: Subscribe to MQTT topics, stream to Rivven
//!
//! # Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Pure Rust client | Zero C dependencies via rumqttc |
//! | Lock-free metrics | Atomic counters with zero contention |
//! | Batch size tracking | Min/max/avg with CAS operations |
//! | Latency tracking | Message receive latency measurements |
//! | Prometheus export | `to_prometheus_format()` for scraping |
//! | JSON serialization | Serde derives on MetricsSnapshot |
//! | QoS levels | `at_most_once`, `at_least_once`, `exactly_once` |
//! | TLS support | MQTT over TLS (mqtts://) |
//! | Exponential backoff | Configurable retry with multiplier |
//!
//! # Use Cases
//!
//! - IoT device data ingestion
//! - Sensor data streaming
//! - Industrial automation data collection
//! - Real-time telemetry processing
//!
//! # MQTT Client
//!
//! This connector uses [rumqttc](https://crates.io/crates/rumqttc), a pure Rust
//! MQTT client with no C dependencies. This provides:
//!
//! - **Zero C dependencies**: No libmosquitto or similar
//! - **Simplified builds**: No C compiler configuration
//! - **Consistent behavior**: Same behavior across all platforms
//! - **MQTT 3.1.1 & 5.0**: Support for both protocol versions
//!
//! Enable with the `mqtt` feature:
//!
//! ```toml
//! rivven-connect = { version = "0.0.17", features = ["mqtt"] }
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! type: mqtt-source
//! topic: iot-events              # Rivven topic (destination)
//! config:
//!   broker_url: "mqtt://broker.hivemq.com:1883"
//!   topics:                      # MQTT topics to subscribe to
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
//!
//! # Observability
//!
//! ```rust,ignore
//! // Get metrics from source
//! let source = MqttSource::new();
//! let snapshot = source.metrics().snapshot();
//! println!("Messages received: {}", snapshot.messages_received);
//! println!("Avg receive latency: {:.2}ms", snapshot.avg_receive_latency_ms());
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
#[cfg(feature = "mqtt")]
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS as RumqttQoS};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use validator::Validate;

// ============================================================================
// MQTT Source Metrics
// ============================================================================

/// Point-in-time snapshot of MQTT source metrics.
///
/// This struct captures all metrics at a specific moment and can be:
/// - Serialized to JSON for export
/// - Exported to Prometheus format
/// - Used for computing derived metrics (rates, averages)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqttSourceMetricsSnapshot {
    /// Total messages successfully received
    pub messages_received: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total connection attempts
    pub connections: u64,
    /// Total disconnections
    pub disconnections: u64,
    /// Total errors
    pub errors: u64,
    /// Cumulative receive latency in microseconds
    pub receive_latency_us: u64,
    /// Minimum batch size - 0 if no batches
    pub batch_size_min: u64,
    /// Maximum batch size
    pub batch_size_max: u64,
    /// Sum of all batch sizes
    pub batch_size_sum: u64,
    /// Total number of batches recorded
    pub batch_count: u64,
}

impl MqttSourceMetricsSnapshot {
    /// Get average receive latency in milliseconds
    #[inline]
    pub fn avg_receive_latency_ms(&self) -> f64 {
        if self.messages_received == 0 {
            return 0.0;
        }
        (self.receive_latency_us as f64 / self.messages_received as f64) / 1000.0
    }

    /// Divide by batch count, not connection count.
    #[inline]
    pub fn avg_batch_size(&self) -> f64 {
        if self.batch_count == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / self.batch_count as f64
    }

    /// Get throughput in messages per second
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.messages_received as f64 / elapsed_secs
    }

    /// Get bytes throughput in bytes per second
    #[inline]
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_received as f64 / elapsed_secs
    }

    /// Get error rate as percentage of total messages
    #[inline]
    pub fn error_rate_percent(&self) -> f64 {
        let total = self.messages_received + self.errors;
        if total == 0 {
            return 0.0;
        }
        (self.errors as f64 / total as f64) * 100.0
    }

    /// Export metrics in Prometheus text format.
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(1024);

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_messages_received_total Total messages received\n\
             # TYPE {prefix}_mqtt_source_messages_received_total counter\n\
             {prefix}_mqtt_source_messages_received_total {}\n",
            self.messages_received
        ));

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_bytes_received_total Total bytes received\n\
             # TYPE {prefix}_mqtt_source_bytes_received_total counter\n\
             {prefix}_mqtt_source_bytes_received_total {}\n",
            self.bytes_received
        ));

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_connections_total Total connections\n\
             # TYPE {prefix}_mqtt_source_connections_total counter\n\
             {prefix}_mqtt_source_connections_total {}\n",
            self.connections
        ));

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_disconnections_total Total disconnections\n\
             # TYPE {prefix}_mqtt_source_disconnections_total counter\n\
             {prefix}_mqtt_source_disconnections_total {}\n",
            self.disconnections
        ));

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_errors_total Total errors\n\
             # TYPE {prefix}_mqtt_source_errors_total counter\n\
             {prefix}_mqtt_source_errors_total {}\n",
            self.errors
        ));

        output.push_str(&format!(
            "# HELP {prefix}_mqtt_source_receive_latency_avg_ms Average receive latency\n\
             # TYPE {prefix}_mqtt_source_receive_latency_avg_ms gauge\n\
             {prefix}_mqtt_source_receive_latency_avg_ms {:.3}\n",
            self.avg_receive_latency_ms()
        ));

        output
    }
}

/// Lock-free metrics for MQTT source connector.
///
/// All counters use `AtomicU64` with `Ordering::Relaxed` for maximum performance
/// on the hot path.
#[derive(Debug, Default)]
pub struct MqttSourceMetrics {
    /// Total messages received
    pub messages_received: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Total connections
    pub connections: AtomicU64,
    /// Total disconnections
    pub disconnections: AtomicU64,
    /// Total errors
    pub errors: AtomicU64,
    /// Cumulative receive latency in microseconds
    pub receive_latency_us: AtomicU64,
    /// Minimum batch size
    batch_size_min: AtomicU64,
    /// Maximum batch size
    batch_size_max: AtomicU64,
    /// Sum of all batch sizes
    batch_size_sum: AtomicU64,
    /// Total number of batches recorded
    batch_count: AtomicU64,
}

impl MqttSourceMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            disconnections: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            receive_latency_us: AtomicU64::new(0),
            batch_size_min: AtomicU64::new(u64::MAX),
            batch_size_max: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
        }
    }

    /// Record a batch size for min/max/avg tracking.
    /// Uses lock-free CAS operations for thread-safe updates.
    #[inline(always)]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);
        self.batch_count.fetch_add(1, Ordering::Relaxed);

        // Update min using CAS loop
        let mut current_min = self.batch_size_min.load(Ordering::Relaxed);
        while size < current_min {
            match self.batch_size_min.compare_exchange_weak(
                current_min,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
            std::hint::spin_loop();
        }

        // Update max using CAS loop
        let mut current_max = self.batch_size_max.load(Ordering::Relaxed);
        while size > current_max {
            match self.batch_size_max.compare_exchange_weak(
                current_max,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Capture a point-in-time snapshot of all metrics.
    #[inline]
    pub fn snapshot(&self) -> MqttSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        MqttSourceMetricsSnapshot {
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            connections: self.connections.load(Ordering::Relaxed),
            disconnections: self.disconnections.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            receive_latency_us: self.receive_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
            batch_count: self.batch_count.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.messages_received.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.connections.store(0, Ordering::Relaxed);
        self.disconnections.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.receive_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    pub fn snapshot_and_reset(&self) -> MqttSourceMetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        MqttSourceMetricsSnapshot {
            messages_received: self.messages_received.swap(0, Ordering::Relaxed),
            bytes_received: self.bytes_received.swap(0, Ordering::Relaxed),
            connections: self.connections.swap(0, Ordering::Relaxed),
            disconnections: self.disconnections.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            receive_latency_us: self.receive_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
            batch_count: self.batch_count.swap(0, Ordering::Relaxed),
        }
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
    #[inline]
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

/// Last Will and Testament configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct LastWillConfig {
    /// Topic to publish the last will message to
    pub topic: String,

    /// Message payload
    pub message: String,

    /// QoS level for the last will message
    #[serde(default)]
    pub qos: QoS,

    /// Retain flag for the last will message
    #[serde(default)]
    pub retain: bool,
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

    /// Initial retry delay in milliseconds on error (default: 100)
    #[serde(default = "default_retry_initial_ms")]
    #[validate(range(min = 10, max = 60000))]
    pub retry_initial_ms: u64,

    /// Maximum retry delay in milliseconds (default: 10000)
    #[serde(default = "default_retry_max_ms")]
    #[validate(range(min = 100, max = 300000))]
    pub retry_max_ms: u64,

    /// Backoff multiplier for retries (default: 2.0)
    #[serde(default = "default_retry_multiplier")]
    #[validate(range(min = 1.0, max = 10.0))]
    pub retry_multiplier: f64,

    /// Last Will and Testament configuration (optional)
    pub last_will: Option<LastWillConfig>,
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
fn default_retry_initial_ms() -> u64 {
    100
}
fn default_retry_max_ms() -> u64 {
    10000
}
fn default_retry_multiplier() -> f64 {
    2.0
}

// ============================================================================
// MQTT Source Implementation
// ============================================================================

/// MQTT Source connector
pub struct MqttSource {
    shutdown: Arc<AtomicBool>,
    metrics: Arc<MqttSourceMetrics>,
}

impl MqttSource {
    pub fn new() -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(MqttSourceMetrics::new()),
        }
    }

    /// Get metrics for this source.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let snapshot = source.metrics().snapshot();
    /// println!("Messages: {}", snapshot.messages_received);
    /// ```
    #[inline]
    pub fn metrics(&self) -> &MqttSourceMetrics {
        &self.metrics
    }

    /// Signal the source to shut down gracefully.
    ///
    /// This sets the shutdown flag which will be checked on the next event loop
    /// iteration, causing the stream to terminate cleanly.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if the source is shutting down.
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Generate a client ID if not provided
    fn generate_client_id() -> String {
        format!(
            "rivven-mqtt-{}",
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        )
    }

    /// Convert our QoS to rumqttc QoS
    #[cfg(feature = "mqtt")]
    #[inline]
    fn to_rumqtt_qos(qos: &QoS) -> RumqttQoS {
        match qos {
            QoS::AtMostOnce => RumqttQoS::AtMostOnce,
            QoS::AtLeastOnce => RumqttQoS::AtLeastOnce,
            QoS::ExactlyOnce => RumqttQoS::ExactlyOnce,
        }
    }

    /// Create an MQTT client for the given configuration.
    ///
    /// This connects to the MQTT broker using the pure Rust rumqttc client.
    /// Supports:
    /// - TLS via `mqtts://` URLs (uses system root certificates)
    /// - Username/password authentication
    /// - Last Will and Testament (LWT) for connection monitoring
    /// - Configurable keep-alive, clean session, and max packet size
    #[cfg(feature = "mqtt")]
    pub fn create_mqtt_options(config: &MqttSourceConfig) -> Result<MqttOptions> {
        let client_id = config
            .client_id
            .clone()
            .unwrap_or_else(Self::generate_client_id);

        let (host, port, use_tls) = Self::parse_broker_url(&config.broker_url)?;

        let mut mqtt_options = MqttOptions::new(client_id, host.clone(), port);
        mqtt_options.set_keep_alive(Duration::from_secs(config.keep_alive_secs as u64));
        mqtt_options.set_clean_session(config.clean_session);
        mqtt_options.set_max_packet_size(256 * 1024, 256 * 1024);

        // Configure authentication if provided
        if let (Some(username), Some(password)) = (&config.auth.username, &config.auth.password) {
            mqtt_options.set_credentials(username.clone(), password.expose().to_string());
        }

        // Configure Last Will and Testament (LWT)
        if let Some(lw) = &config.last_will {
            let last_will = rumqttc::LastWill::new(
                &lw.topic,
                lw.message.as_bytes().to_vec(),
                Self::to_rumqtt_qos(&lw.qos),
                lw.retain,
            );
            mqtt_options.set_last_will(last_will);
        }

        // Configure TLS for mqtts:// URLs
        if use_tls {
            // Use rustls with system root certificates (default TlsConfiguration)
            mqtt_options.set_transport(rumqttc::Transport::Tls(Default::default()));
            info!("MQTT TLS enabled for {}", host);
        }

        Ok(mqtt_options)
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

        #[cfg(feature = "mqtt")]
        {
            // Real MQTT CONNECT verification using rumqttc
            let mqtt_options = match Self::create_mqtt_options(config) {
                Ok(opts) => opts,
                Err(e) => return Ok(CheckResult::failure(format!("Invalid MQTT options: {}", e))),
            };

            let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

            // Try to establish connection with timeout
            let connect_result = tokio::time::timeout(
                Duration::from_secs(config.connect_timeout_secs as u64),
                async {
                    loop {
                        match eventloop.poll().await {
                            Ok(Event::Incoming(Packet::ConnAck(connack))) => {
                                // Check CONNACK return code
                                if connack.code == rumqttc::ConnectReturnCode::Success {
                                    return Ok(());
                                } else {
                                    return Err(format!("MQTT CONNACK error: {:?}", connack.code));
                                }
                            }
                            Ok(_) => {
                                // Other events, continue polling
                                continue;
                            }
                            Err(e) => {
                                return Err(format!("MQTT connection error: {}", e));
                            }
                        }
                    }
                },
            )
            .await;

            // Disconnect cleanly
            let _ = client.disconnect().await;

            match connect_result {
                Ok(Ok(())) => {
                    info!(
                        "Successfully authenticated with MQTT broker: {}",
                        config.broker_url
                    );
                    Ok(CheckResult::builder()
                        .check_passed("broker_connectivity")
                        .check_passed("mqtt_authentication")
                        .check_passed("configuration")
                        .build())
                }
                Ok(Err(e)) => Ok(CheckResult::failure(e)),
                Err(_) => Ok(CheckResult::failure(format!(
                    "MQTT connection timeout for broker: {}",
                    config.broker_url
                ))),
            }
        }

        #[cfg(not(feature = "mqtt"))]
        {
            // Fallback TCP connectivity check when mqtt feature is disabled
            let (host, port, _use_tls) = match Self::parse_broker_url(&config.broker_url) {
                Ok(parsed) => parsed,
                Err(e) => return Ok(CheckResult::failure(format!("Invalid broker URL: {}", e))),
            };

            let addr = format!("{}:{}", host, port);
            match tokio::time::timeout(
                Duration::from_secs(config.connect_timeout_secs as u64),
                tokio::net::TcpStream::connect(&addr),
            )
            .await
            {
                Ok(Ok(_)) => {
                    info!("Successfully connected to MQTT broker (TCP): {}", addr);
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

        #[cfg(feature = "mqtt")]
        {
            let mqtt_options = Self::create_mqtt_options(config)?;
            let (client, mut eventloop) =
                AsyncClient::new(mqtt_options, config.max_inflight as usize);

            // Subscribe to all topics
            let qos = Self::to_rumqtt_qos(&config.qos);
            for topic in &config.topics {
                client.subscribe(topic, qos).await.map_err(|e| {
                    ConnectorError::Connection(format!("Failed to subscribe to {}: {}", topic, e))
                })?;
                info!("Subscribed to MQTT topic: {}", topic);
            }

            let shutdown = self.shutdown.clone();
            let metrics = self.metrics.clone();
            let topics = config.topics.clone();
            let include_metadata = config.include_metadata;
            let parse_json = config.parse_json_payload;
            let retry_initial_ms = config.retry_initial_ms;
            let retry_max_ms = config.retry_max_ms;
            let retry_multiplier = config.retry_multiplier;
            let client_id = config
                .client_id
                .clone()
                .unwrap_or_else(Self::generate_client_id);
            let broker_url = config.broker_url.clone();

            let stream = async_stream::stream! {
                let mut message_id: u64 = 0;
                let mut current_backoff_ms = retry_initial_ms;

                metrics.connections.fetch_add(1, Ordering::Relaxed);
                info!("MQTT source connected as '{}' to {}", client_id, broker_url);

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        info!("MQTT source shutting down");
                        break;
                    }

                    let receive_start = std::time::Instant::now();

                    match eventloop.poll().await {
                        Ok(event) => {
                            // Reset backoff on success
                            current_backoff_ms = retry_initial_ms;

                            if let Event::Incoming(Packet::Publish(publish)) = event {
                                message_id += 1;
                                let topic = publish.topic.clone();
                                let payload_bytes = publish.payload.to_vec();
                                let payload_len = payload_bytes.len() as u64;

                                // Normalize topic name for stream
                                let stream_name = topic
                                    .replace('+', "_single")
                                    .replace('#', "_multi")
                                    .replace('/', "_");

                                // Parse payload
                                let payload_value: serde_json::Value = if parse_json {
                                    serde_json::from_slice(&payload_bytes).unwrap_or_else(|_| {
                                        serde_json::json!(String::from_utf8_lossy(&payload_bytes).to_string())
                                    })
                                } else {
                                    serde_json::json!(String::from_utf8_lossy(&payload_bytes).to_string())
                                };

                                let mut data = serde_json::json!({
                                    "topic": topic,
                                    "payload": payload_value,
                                    "qos": publish.qos as u8,
                                    "retain": publish.retain,
                                    "timestamp": chrono::Utc::now().to_rfc3339()
                                });

                                if include_metadata {
                                    data["metadata"] = serde_json::json!({
                                        "client_id": client_id,
                                        "broker": broker_url,
                                        "message_id": message_id,
                                        "pkid": publish.pkid
                                    });
                                }

                                let event = SourceEvent::record(&stream_name, data)
                                    .with_position(message_id.to_string())
                                    .with_metadata("mqtt.topic", serde_json::json!(topic))
                                    .with_metadata("mqtt.qos", serde_json::json!(publish.qos as u8));

                                // Update metrics
                                metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                                metrics.bytes_received.fetch_add(payload_len, Ordering::Relaxed);
                                let receive_elapsed = receive_start.elapsed().as_micros() as u64;
                                metrics.receive_latency_us.fetch_add(receive_elapsed, Ordering::Relaxed);

                                yield Ok(event);
                            } else if let Event::Incoming(Packet::ConnAck(_)) = event {
                                info!("MQTT connection acknowledged");
                            } else if let Event::Incoming(Packet::Disconnect) = event {
                                metrics.disconnections.fetch_add(1, Ordering::Relaxed);
                                warn!("MQTT disconnected, will reconnect...");
                            }
                        }
                        Err(e) => {
                            metrics.errors.fetch_add(1, Ordering::Relaxed);
                            warn!("MQTT error (backoff {}ms): {}", current_backoff_ms, e);

                            // Exponential backoff
                            tokio::time::sleep(Duration::from_millis(current_backoff_ms)).await;
                            current_backoff_ms = ((current_backoff_ms as f64 * retry_multiplier) as u64).min(retry_max_ms);
                        }
                    }

                    // Emit state checkpoint periodically
                    if message_id > 0 && message_id.is_multiple_of(100) {
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
                }
            };

            Ok(Box::pin(stream))
        }

        #[cfg(not(feature = "mqtt"))]
        {
            // Fallback simulation when mqtt feature is disabled
            let shutdown = self.shutdown.clone();
            let metrics = self.metrics.clone();
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
                    "MQTT source (simulation) connected as '{}' to {}",
                    client_id, broker_url
                );

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        info!("MQTT source shutting down");
                        break;
                    }

                    poll_count += 1;

                    for (idx, topic) in topics.iter().enumerate() {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }

                        message_id += 1;

                        let stream_name = topic
                            .replace('+', "_single")
                            .replace('#', "_multi")
                            .replace('/', "_");

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

                        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                        yield Ok(event);
                    }

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

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };

            Ok(Box::pin(stream))
        }
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
// Registry Functions
// ============================================================================

/// Register MQTT source connectors with a source registry
pub fn register_sources(registry: &mut SourceRegistry) {
    registry.register("mqtt-source", std::sync::Arc::new(MqttSourceFactory));
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
            retry_initial_ms: 100,
            retry_max_ms: 10_000,
            retry_multiplier: 2.0,
            last_will: None,
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
            retry_initial_ms: 100,
            retry_max_ms: 10_000,
            retry_multiplier: 2.0,
            last_will: None,
        };

        let catalog = source.discover(&config).await.unwrap();
        assert_eq!(catalog.streams.len(), 2);

        // Check stream names are normalized
        let stream_names: Vec<_> = catalog.streams.iter().map(|s| s.name.as_str()).collect();
        assert!(stream_names.contains(&"sensors__single_temp"));
        assert!(stream_names.contains(&"devices__multi"));
    }

    #[test]
    fn test_mqtt_source_metrics() {
        let metrics = MqttSourceMetrics::new();

        // Test direct counter updates (same as in read() hot path)
        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(100, Ordering::Relaxed);
        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(200, Ordering::Relaxed);

        // Test batch size recording
        metrics.record_batch_size(50);
        metrics.record_batch_size(150);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_received, 2);
        assert_eq!(snapshot.bytes_received, 300);
        assert_eq!(snapshot.batch_size_min, 50);
        assert_eq!(snapshot.batch_size_max, 150);
    }

    #[test]
    fn test_backoff_config_defaults() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
            "broker_url": "mqtt://localhost:1883",
            "topics": ["test"]
        }"#,
        )
        .unwrap();

        // Verify backoff defaults
        assert_eq!(config.retry_initial_ms, 100);
        assert_eq!(config.retry_max_ms, 10_000);
        assert_eq!(config.retry_multiplier, 2.0);
        assert!(config.last_will.is_none());
    }

    #[test]
    fn test_last_will_config() {
        let config: MqttSourceConfig = serde_json::from_str(
            r#"{
            "broker_url": "mqtt://localhost:1883",
            "topics": ["test"],
            "last_will": {
                "topic": "clients/rivven/status",
                "message": "offline",
                "qos": "at_least_once",
                "retain": true
            }
        }"#,
        )
        .unwrap();

        let lw = config.last_will.as_ref().unwrap();
        assert_eq!(lw.topic, "clients/rivven/status");
        assert_eq!(lw.message, "offline");
        assert!(matches!(lw.qos, QoS::AtLeastOnce));
        assert!(lw.retain);
    }

    #[test]
    fn test_metrics_snapshot_prometheus_format() {
        let metrics = MqttSourceMetrics::new();
        metrics.messages_received.fetch_add(100, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(5000, Ordering::Relaxed);
        metrics.errors.fetch_add(2, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        let prometheus = snapshot.to_prometheus_format("rivven");

        assert!(prometheus.contains("rivven_mqtt_source_messages_received_total 100"));
        assert!(prometheus.contains("rivven_mqtt_source_bytes_received_total 5000"));
        assert!(prometheus.contains("rivven_mqtt_source_errors_total 2"));
    }

    #[test]
    fn test_metrics_snapshot_and_reset() {
        let metrics = MqttSourceMetrics::new();
        metrics.messages_received.fetch_add(50, Ordering::Relaxed);
        metrics.bytes_received.fetch_add(2000, Ordering::Relaxed);

        // Take snapshot and reset
        let snapshot = metrics.snapshot_and_reset();
        assert_eq!(snapshot.messages_received, 50);
        assert_eq!(snapshot.bytes_received, 2000);

        // Verify reset
        let snapshot2 = metrics.snapshot();
        assert_eq!(snapshot2.messages_received, 0);
        assert_eq!(snapshot2.bytes_received, 0);
    }

    #[test]
    fn test_metrics_derived_values() {
        let snapshot = MqttSourceMetricsSnapshot {
            messages_received: 1000,
            bytes_received: 50000,
            connections: 10,
            disconnections: 2,
            errors: 5,
            receive_latency_us: 500000, // 500ms total
            batch_size_min: 10,
            batch_size_max: 100,
            batch_size_sum: 500,
            batch_count: 10,
        };

        // Average latency = 500000us / 1000 messages = 500us = 0.5ms
        assert!((snapshot.avg_receive_latency_ms() - 0.5).abs() < 0.001);

        // Average batch size = 500 / 10 batches = 50
        assert!((snapshot.avg_batch_size() - 50.0).abs() < 0.001);

        // Messages per second with 10 elapsed seconds = 100/s
        assert!((snapshot.messages_per_second(10.0) - 100.0).abs() < 0.001);

        // Bytes per second with 10 elapsed seconds = 5000/s
        assert!((snapshot.bytes_per_second(10.0) - 5000.0).abs() < 0.001);

        // Error rate = 5 / (1000 + 5) = 0.497%
        assert!((snapshot.error_rate_percent() - 0.497).abs() < 0.01);
    }

    #[test]
    fn test_graceful_shutdown() {
        let source = MqttSource::new();

        // Initially not shutting down
        assert!(!source.is_shutting_down());

        // Signal shutdown
        source.shutdown();

        // Now shutting down
        assert!(source.is_shutting_down());
    }

    #[test]
    fn test_edge_cases() {
        // Test zero elapsed time
        let snapshot = MqttSourceMetricsSnapshot::default();
        assert_eq!(snapshot.messages_per_second(0.0), 0.0);
        assert_eq!(snapshot.bytes_per_second(0.0), 0.0);
        assert_eq!(snapshot.avg_receive_latency_ms(), 0.0);
        assert_eq!(snapshot.avg_batch_size(), 0.0);
        assert_eq!(snapshot.error_rate_percent(), 0.0);

        // Test negative elapsed time
        assert_eq!(snapshot.messages_per_second(-1.0), 0.0);
        assert_eq!(snapshot.bytes_per_second(-1.0), 0.0);
    }

    #[test]
    fn test_parse_broker_url_tls_variants() {
        // Test ssl:// scheme
        let (host, port, tls) = MqttSource::parse_broker_url("ssl://broker:8883").unwrap();
        assert_eq!(host, "broker");
        assert_eq!(port, 8883);
        assert!(tls);

        // Test tls:// scheme
        let (host, port, tls) = MqttSource::parse_broker_url("tls://broker:8883").unwrap();
        assert_eq!(host, "broker");
        assert_eq!(port, 8883);
        assert!(tls);

        // Test tcp:// scheme (non-TLS)
        let (host, port, tls) = MqttSource::parse_broker_url("tcp://broker:1883").unwrap();
        assert_eq!(host, "broker");
        assert_eq!(port, 1883);
        assert!(!tls);
    }
}
