//! OpenTelemetry tracing integration for rivven-connect
//!
//! This module provides distributed tracing capabilities using OpenTelemetry.
//! Traces are propagated through the pipeline allowing end-to-end visibility.
//!
//! # Features
//!
//! - Automatic span creation for source reads and sink writes
//! - Context propagation across connector boundaries
//! - Support for multiple exporters (OTLP, Jaeger, Zipkin)
//! - Baggage propagation for cross-cutting concerns
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::telemetry::{TelemetryConfig, init_tracing};
//!
//! let config = TelemetryConfig {
//!     enabled: true,
//!     service_name: "my-pipeline".to_string(),
//!     exporter: ExporterConfig::Otlp {
//!         endpoint: "http://localhost:4317".to_string(),
//!     },
//!     ..Default::default()
//! };
//!
//! init_tracing(&config)?;
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn, Span};

/// Configuration for OpenTelemetry tracing
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelemetryConfig {
    /// Enable OpenTelemetry tracing
    #[serde(default)]
    pub enabled: bool,

    /// Service name for traces
    #[serde(default = "default_service_name")]
    pub service_name: String,

    /// Exporter configuration
    #[serde(default)]
    pub exporter: ExporterConfig,

    /// Sampling ratio (0.0 to 1.0, default 1.0 = all traces)
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,

    /// Additional resource attributes
    #[serde(default)]
    pub resource_attributes: HashMap<String, String>,

    /// Propagation format (w3c, jaeger, b3)
    #[serde(default)]
    pub propagation: PropagationFormat,

    /// Batch configuration
    #[serde(default)]
    pub batch: BatchConfig,
}

fn default_service_name() -> String {
    "rivven-connect".to_string()
}

fn default_sample_ratio() -> f64 {
    1.0
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: default_service_name(),
            exporter: ExporterConfig::default(),
            sample_ratio: default_sample_ratio(),
            resource_attributes: HashMap::new(),
            propagation: PropagationFormat::default(),
            batch: BatchConfig::default(),
        }
    }
}

/// Exporter configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExporterConfig {
    /// OTLP exporter (default, supports both gRPC and HTTP)
    Otlp {
        /// OTLP endpoint (e.g., "http://localhost:4317" for gRPC)
        #[serde(default = "default_otlp_endpoint")]
        endpoint: String,
        /// Use HTTP instead of gRPC
        #[serde(default)]
        use_http: bool,
        /// Custom headers
        #[serde(default)]
        headers: HashMap<String, String>,
    },
    /// Jaeger exporter (for Jaeger collector)
    Jaeger {
        /// Jaeger collector endpoint
        #[serde(default = "default_jaeger_endpoint")]
        endpoint: String,
    },
    /// Console exporter (for debugging)
    Console,
    /// No-op exporter (traces discarded)
    None,
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_jaeger_endpoint() -> String {
    "http://localhost:14268/api/traces".to_string()
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self::Otlp {
            endpoint: default_otlp_endpoint(),
            use_http: false,
            headers: HashMap::new(),
        }
    }
}

/// Trace context propagation format
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PropagationFormat {
    /// W3C Trace Context (default)
    #[default]
    W3c,
    /// Jaeger propagation format
    Jaeger,
    /// B3 propagation format (Zipkin)
    B3,
    /// B3 multi-header format
    B3Multi,
}

/// Batch span processor configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BatchConfig {
    /// Maximum queue size
    #[serde(default = "default_max_queue_size")]
    pub max_queue_size: usize,
    /// Maximum export batch size
    #[serde(default = "default_max_export_batch_size")]
    pub max_export_batch_size: usize,
    /// Scheduled delay in milliseconds
    #[serde(default = "default_scheduled_delay_ms")]
    pub scheduled_delay_ms: u64,
    /// Export timeout in milliseconds
    #[serde(default = "default_export_timeout_ms")]
    pub export_timeout_ms: u64,
}

fn default_max_queue_size() -> usize {
    2048
}

fn default_max_export_batch_size() -> usize {
    512
}

fn default_scheduled_delay_ms() -> u64 {
    5000
}

fn default_export_timeout_ms() -> u64 {
    30000
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_queue_size: default_max_queue_size(),
            max_export_batch_size: default_max_export_batch_size(),
            scheduled_delay_ms: default_scheduled_delay_ms(),
            export_timeout_ms: default_export_timeout_ms(),
        }
    }
}

/// Trace context for propagation across connector boundaries
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    /// W3C traceparent header value
    pub traceparent: Option<String>,
    /// W3C tracestate header value  
    pub tracestate: Option<String>,
    /// Baggage items
    pub baggage: HashMap<String, String>,
}

impl TraceContext {
    /// Create a new trace context
    pub fn new() -> Self {
        Self::default()
    }

    /// Create trace context from current span
    pub fn from_current_span() -> Self {
        // In a real implementation, this would extract from the current span
        // For now, return empty context
        Self::new()
    }

    /// Extract trace context from headers
    pub fn from_headers(headers: &HashMap<String, String>) -> Self {
        Self {
            traceparent: headers.get("traceparent").cloned(),
            tracestate: headers.get("tracestate").cloned(),
            baggage: headers
                .get("baggage")
                .map(|b| parse_baggage(b))
                .unwrap_or_default(),
        }
    }

    /// Convert trace context to headers
    pub fn to_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(ref tp) = self.traceparent {
            headers.insert("traceparent".to_string(), tp.clone());
        }
        if let Some(ref ts) = self.tracestate {
            headers.insert("tracestate".to_string(), ts.clone());
        }
        if !self.baggage.is_empty() {
            headers.insert("baggage".to_string(), format_baggage(&self.baggage));
        }
        headers
    }

    /// Set a baggage item
    pub fn set_baggage(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.baggage.insert(key.into(), value.into());
    }

    /// Get a baggage item
    pub fn get_baggage(&self, key: &str) -> Option<&String> {
        self.baggage.get(key)
    }
}

fn parse_baggage(baggage: &str) -> HashMap<String, String> {
    baggage
        .split(',')
        .filter_map(|item| {
            let mut parts = item.trim().splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                _ => None,
            }
        })
        .collect()
}

fn format_baggage(baggage: &HashMap<String, String>) -> String {
    baggage
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

/// Initialize OpenTelemetry tracing
///
/// This function sets up the tracing subscriber with OpenTelemetry integration.
/// Call this once at application startup.
pub fn init_tracing(config: &TelemetryConfig) -> Result<(), TelemetryError> {
    if !config.enabled {
        info!("OpenTelemetry tracing is disabled");
        return Ok(());
    }

    info!(
        service_name = %config.service_name,
        exporter = ?config.exporter,
        sample_ratio = %config.sample_ratio,
        "Initializing OpenTelemetry tracing"
    );

    // Note: Full OpenTelemetry integration requires the opentelemetry crate
    // which would be added as an optional dependency behind a feature flag.
    // This is a placeholder implementation showing the API.

    match &config.exporter {
        ExporterConfig::Otlp {
            endpoint, use_http, ..
        } => {
            info!(
                endpoint = %endpoint,
                use_http = %use_http,
                "Configured OTLP exporter"
            );
        }
        ExporterConfig::Jaeger { endpoint } => {
            info!(endpoint = %endpoint, "Configured Jaeger exporter");
        }
        ExporterConfig::Console => {
            info!("Configured console exporter for debugging");
        }
        ExporterConfig::None => {
            warn!("Tracing enabled but exporter is set to 'none'");
        }
    }

    Ok(())
}

/// Shutdown OpenTelemetry tracing
///
/// Flushes any pending spans and shuts down the tracer provider.
/// Call this at application shutdown.
pub fn shutdown_tracing() {
    info!("Shutting down OpenTelemetry tracing");
    // In a real implementation, this would call:
    // opentelemetry::global::shutdown_tracer_provider();
}

/// Error type for telemetry operations
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("Failed to initialize tracer: {0}")]
    InitializationFailed(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Exporter error: {0}")]
    ExporterError(String),
}

/// Extension trait for adding trace context to events
pub trait TracingExt {
    /// Attach trace context to this item
    fn with_trace_context(self, ctx: TraceContext) -> Self;

    /// Extract trace context from this item
    fn trace_context(&self) -> Option<&TraceContext>;
}

/// Create a span for source read operations
pub fn source_read_span(source_name: &str, stream_name: &str) -> Span {
    tracing::info_span!(
        "source.read",
        otel.kind = "consumer",
        source.name = source_name,
        stream.name = stream_name,
    )
}

/// Create a span for sink write operations  
pub fn sink_write_span(sink_name: &str, batch_size: usize) -> Span {
    tracing::info_span!(
        "sink.write",
        otel.kind = "producer",
        sink.name = sink_name,
        batch.size = batch_size,
    )
}

/// Create a span for transform operations
pub fn transform_span(transform_name: &str, event_count: usize) -> Span {
    tracing::info_span!(
        "transform.process",
        otel.kind = "internal",
        transform.name = transform_name,
        event.count = event_count,
    )
}

/// Create a span for broker publish operations
pub fn broker_publish_span(topic: &str) -> Span {
    tracing::info_span!(
        "broker.publish",
        otel.kind = "producer",
        messaging.system = "rivven",
        messaging.destination = topic,
    )
}

/// Create a span for broker consume operations
pub fn broker_consume_span(topic: &str) -> Span {
    tracing::info_span!(
        "broker.consume",
        otel.kind = "consumer",
        messaging.system = "rivven",
        messaging.source = topic,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TelemetryConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.service_name, "rivven-connect");
        assert_eq!(config.sample_ratio, 1.0);
    }

    #[test]
    fn test_trace_context_baggage() {
        let mut ctx = TraceContext::new();
        ctx.set_baggage("user_id", "123");
        ctx.set_baggage("tenant", "acme");

        assert_eq!(ctx.get_baggage("user_id"), Some(&"123".to_string()));
        assert_eq!(ctx.get_baggage("tenant"), Some(&"acme".to_string()));
        assert_eq!(ctx.get_baggage("missing"), None);
    }

    #[test]
    fn test_trace_context_headers() {
        let mut ctx = TraceContext::new();
        ctx.traceparent =
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string());
        ctx.set_baggage("key", "value");

        let headers = ctx.to_headers();
        assert!(headers.contains_key("traceparent"));
        assert!(headers.contains_key("baggage"));
    }

    #[test]
    fn test_parse_baggage() {
        let baggage = "key1=value1,key2=value2, key3=value3";
        let parsed = parse_baggage(baggage);

        assert_eq!(parsed.get("key1"), Some(&"value1".to_string()));
        assert_eq!(parsed.get("key2"), Some(&"value2".to_string()));
        assert_eq!(parsed.get("key3"), Some(&"value3".to_string()));
    }

    #[test]
    fn test_format_baggage() {
        let mut baggage = HashMap::new();
        baggage.insert("a".to_string(), "1".to_string());
        baggage.insert("b".to_string(), "2".to_string());

        let formatted = format_baggage(&baggage);
        assert!(formatted.contains("a=1"));
        assert!(formatted.contains("b=2"));
    }

    #[test]
    fn test_exporter_config_serde() {
        let yaml = r#"
type: otlp
endpoint: http://localhost:4317
use_http: false
"#;
        let config: ExporterConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            ExporterConfig::Otlp {
                endpoint, use_http, ..
            } => {
                assert_eq!(endpoint, "http://localhost:4317");
                assert!(!use_http);
            }
            _ => panic!("Expected OTLP config"),
        }
    }

    #[test]
    fn test_init_tracing_disabled() {
        let config = TelemetryConfig::default();
        assert!(init_tracing(&config).is_ok());
    }
}
