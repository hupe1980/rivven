//! Stdout Sink connector
//!
//! A simple sink that writes events to stdout for debugging and testing.

use super::super::prelude::*;
use crate::connectors::{AnySink, SinkFactory};
use crate::schema::avro::{AvroCodec, AvroSchema};
use async_trait::async_trait;
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Stdout sink configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, Validate, JsonSchema)]
pub struct StdoutSinkConfig {
    /// Output format
    #[serde(default)]
    pub format: OutputFormat,

    /// Include metadata in output
    #[serde(default = "default_true")]
    pub include_metadata: bool,

    /// Include timestamp in output
    #[serde(default = "default_true")]
    pub include_timestamp: bool,

    /// Color output (if terminal supports it)
    #[serde(default = "default_true")]
    pub color: bool,

    /// Rate limit output (events per second, 0 = unlimited)
    #[serde(default)]
    #[validate(range(max = 100000))]
    pub rate_limit: u32,

    /// Avro schema (required for Avro/AvroBinary/AvroHex formats)
    #[serde(default)]
    pub avro_schema: Option<String>,

    /// Include schema ID in Confluent wire format (for Avro formats)
    #[serde(default)]
    pub confluent_wire_format: bool,
}

fn default_true() -> bool {
    true
}

/// Output format for stdout sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Pretty-printed JSON
    #[default]
    Pretty,
    /// Compact JSON (one line per event)
    Json,
    /// Tab-separated values
    Tsv,
    /// Simple text format
    Text,
    /// Avro JSON (shows JSON representation of Avro-encoded data)
    AvroJson,
    /// Avro binary (base64-encoded)
    AvroBinary,
    /// Avro binary (hex-encoded)
    AvroHex,
}

/// Stdout Sink implementation
pub struct StdoutSink;

impl StdoutSink {
    /// Create a new StdoutSink
    pub fn new() -> Self {
        Self
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for StdoutSink {
    type Config = StdoutSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::builder("stdout", env!("CARGO_PKG_VERSION"))
            .description("Write events to stdout for debugging")
            .author("Rivven Team")
            .license("MIT/Apache-2.0")
            .documentation_url("https://rivven.dev/docs/connectors/stdout")
            .config_schema::<StdoutSinkConfig>()
            .build()
    }

    async fn check(&self, _config: &Self::Config) -> Result<CheckResult> {
        // Stdout is always available
        Ok(CheckResult::success())
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let mut result = WriteResult::new();

        // Parse Avro schema if configured
        let avro_codec = if let Some(ref schema_str) = config.avro_schema {
            match AvroSchema::parse(schema_str) {
                Ok(schema) => Some(AvroCodec::new(schema)),
                Err(e) => {
                    return Err(
                        ConnectorError::Config(format!("Invalid Avro schema: {}", e)).into(),
                    );
                }
            }
        } else {
            None
        };

        // Validate Avro schema is provided for Avro formats
        if matches!(
            config.format,
            OutputFormat::AvroJson | OutputFormat::AvroBinary | OutputFormat::AvroHex
        ) && avro_codec.is_none()
        {
            return Err(ConnectorError::Config(
                "Avro schema required for Avro output formats".to_string(),
            )
            .into());
        }

        // Rate limiting
        let rate_limit = if config.rate_limit > 0 {
            Some(std::time::Duration::from_secs_f64(
                1.0 / config.rate_limit as f64,
            ))
        } else {
            None
        };
        let mut last_event = std::time::Instant::now();

        while let Some(event) = events.next().await {
            // Apply rate limiting
            if let Some(interval) = rate_limit {
                let elapsed = last_event.elapsed();
                if elapsed < interval {
                    tokio::time::sleep(interval - elapsed).await;
                }
                last_event = std::time::Instant::now();
            }

            let output = format_event(&event, config, avro_codec.as_ref());
            println!("{}", output);

            let bytes = output.len() as u64;
            result.add_success(1, bytes);
        }

        Ok(result)
    }
}

/// Format an event for output
/// Format an event for output
pub fn format_event(
    event: &SourceEvent,
    config: &StdoutSinkConfig,
    avro_codec: Option<&AvroCodec>,
) -> String {
    match config.format {
        OutputFormat::Pretty => {
            let mut output = String::new();

            if config.include_timestamp {
                output.push_str(&format!(
                    "[{}] ",
                    event.timestamp.format("%Y-%m-%d %H:%M:%S%.3f")
                ));
            }

            let op = match event.event_type {
                SourceEventType::Insert => "INSERT",
                SourceEventType::Update => "UPDATE",
                SourceEventType::Delete => "DELETE",
                SourceEventType::Record => "RECORD",
                SourceEventType::State => "STATE",
                SourceEventType::Log => "LOG",
                SourceEventType::Schema => "SCHEMA",
            };

            output.push_str(&format!("{}: {}", op, event.stream));

            if let Some(ref ns) = event.namespace {
                output.push_str(&format!(" ({})", ns));
            }

            output.push('\n');

            if let Ok(pretty) = serde_json::to_string_pretty(&event.data) {
                output.push_str(&pretty);
            } else {
                output.push_str(&event.data.to_string());
            }

            if config.include_metadata && !event.metadata.extra.is_empty() {
                output.push_str("\n---metadata---\n");
                if let Ok(meta) = serde_json::to_string_pretty(&event.metadata.extra) {
                    output.push_str(&meta);
                }
            }

            output
        }
        OutputFormat::Json => {
            serde_json::to_string(event).unwrap_or_else(|_| event.data.to_string())
        }
        OutputFormat::Tsv => {
            format!(
                "{}\t{}\t{}\t{}",
                event.timestamp.to_rfc3339(),
                event.event_type.as_str(),
                event.stream,
                event.data.to_string().replace(['\t', '\n'], " ")
            )
        }
        OutputFormat::Text => {
            format!(
                "{} {} {}",
                event.event_type.as_str(),
                event.stream,
                event.data
            )
        }
        OutputFormat::AvroJson => {
            // For Avro JSON format, we just show the event data
            // The codec validates it against the schema
            if let Some(codec) = avro_codec {
                match codec.encode(&event.data) {
                    Ok(bytes) => {
                        // Re-decode to show what Avro sees
                        match codec.decode(&bytes) {
                            Ok(decoded) => serde_json::to_string_pretty(&decoded)
                                .unwrap_or_else(|_| decoded.to_string()),
                            Err(e) => format!("{{\"error\": \"Avro decode error: {}\"}}", e),
                        }
                    }
                    Err(e) => format!("{{\"error\": \"Avro encode error: {}\"}}", e),
                }
            } else {
                "{\"error\": \"No Avro schema configured\"}".to_string()
            }
        }
        OutputFormat::AvroBinary => {
            // Output binary Avro as base64
            if let Some(codec) = avro_codec {
                let schema_id = event.metadata.schema_id;
                let result = if config.confluent_wire_format {
                    codec.encode_with_schema_id(&event.data, schema_id.unwrap_or(0))
                } else {
                    codec.encode(&event.data)
                };
                match result {
                    Ok(bytes) => {
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes)
                    }
                    Err(e) => format!("error:{}", e),
                }
            } else {
                "error:no_avro_schema".to_string()
            }
        }
        OutputFormat::AvroHex => {
            // Output binary Avro as hex
            if let Some(codec) = avro_codec {
                let schema_id = event.metadata.schema_id;
                let result = if config.confluent_wire_format {
                    codec.encode_with_schema_id(&event.data, schema_id.unwrap_or(0))
                } else {
                    codec.encode(&event.data)
                };
                match result {
                    Ok(bytes) => hex::encode(&bytes),
                    Err(e) => format!("error:{}", e),
                }
            } else {
                "error:no_avro_schema".to_string()
            }
        }
    }
}

/// Factory for creating StdoutSink instances
pub struct StdoutSinkFactory;

impl SinkFactory for StdoutSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        StdoutSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(StdoutSinkWrapper(StdoutSink::new())))
    }
}

/// Wrapper for type-erased sink operations
#[allow(dead_code)] // Used by SinkFactory for dynamic dispatch
struct StdoutSinkWrapper(StdoutSink);

#[async_trait]
impl AnySink for StdoutSinkWrapper {
    async fn check_raw(&self, config: &serde_yaml::Value) -> Result<CheckResult> {
        let typed_config: StdoutSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.check(&typed_config).await
    }

    async fn write_raw(
        &self,
        config: &serde_yaml::Value,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let typed_config: StdoutSinkConfig = serde_yaml::from_value(config.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        self.0.write(&typed_config, events).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_spec() {
        let spec = StdoutSink::spec();
        assert_eq!(spec.connector_type, "stdout");
    }

    #[test]
    fn test_config_defaults() {
        // When using serde default (from_str), the defaults are properly applied
        // When using Default::default(), the struct derives default (false for bools, 0 for numbers)
        let config: StdoutSinkConfig = serde_json::from_str("{}").unwrap();
        assert!(config.include_metadata);
        assert!(config.include_timestamp);
        assert!(config.color);
        assert_eq!(config.rate_limit, 0);
    }

    #[test]
    fn test_format_event_pretty() {
        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "public.users".to_string(),
            namespace: Some("public".to_string()),
            timestamp: Utc::now(),
            data: serde_json::json!({"id": 1, "name": "Alice"}),
            metadata: Default::default(),
        };

        let config = StdoutSinkConfig::default();
        let output = format_event(&event, &config, None);

        assert!(output.contains("INSERT"));
        assert!(output.contains("public.users"));
        assert!(output.contains("Alice"));
    }

    #[test]
    fn test_format_event_json() {
        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "users".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({"id": 1}),
            metadata: Default::default(),
        };

        let config = StdoutSinkConfig {
            format: OutputFormat::Json,
            ..Default::default()
        };

        let output = format_event(&event, &config, None);

        // Should be valid JSON
        let parsed: std::result::Result<serde_json::Value, _> = serde_json::from_str(&output);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_format_event_avro() {
        let schema_str = r#"
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
        "#;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "users".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({"id": 1, "name": "Alice"}),
            metadata: Default::default(),
        };

        let schema = AvroSchema::parse(schema_str).unwrap();
        let codec = AvroCodec::new(schema);

        let config = StdoutSinkConfig {
            format: OutputFormat::AvroBinary,
            avro_schema: Some(schema_str.to_string()),
            ..Default::default()
        };

        let output = format_event(&event, &config, Some(&codec));
        // Should be valid base64
        assert!(!output.starts_with("error:"));
    }

    #[tokio::test]
    async fn test_check_always_succeeds() {
        let sink = StdoutSink::new();
        let config = StdoutSinkConfig::default();

        let result = sink.check(&config).await.unwrap();
        assert!(result.is_success());
    }
}
