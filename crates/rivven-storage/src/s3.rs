//! Amazon S3 Sink Connector
//!
//! This module provides an S3 sink that writes events to Amazon S3 or S3-compatible
//! storage systems (MinIO, LocalStack, etc.).
//!
//! # Features
//!
//! - JSON and JSONL output formats
//! - Configurable partitioning (none, daily, hourly)
//! - Gzip compression
//! - Batching with configurable size and flush interval
//! - Automatic credential resolution (AWS defaults or explicit)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_cloud::s3::S3SinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("s3", Arc::new(S3SinkFactory));
//! ```

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use rivven_connect::error::ConnectorError;
use rivven_connect::prelude::*;
use rivven_connect::{AnySink, SinkFactory};
use schemars::JsonSchema;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::io::Write;
use tracing::{debug, error, info, warn};
use validator::Validate;

/// Wrapper for sensitive configuration values
///
/// Prevents accidental logging of secrets while allowing access when needed.
#[derive(Debug, Clone, JsonSchema)]
pub struct SensitiveString(#[schemars(with = "String")] SecretString);

impl SensitiveString {
    /// Create a new sensitive string
    pub fn new(value: impl Into<String>) -> Self {
        Self(SecretString::from(value.into()))
    }

    /// Expose the secret value
    pub fn expose_secret(&self) -> &str {
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

/// Output format for S3 sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum S3Format {
    /// JSON array of events
    Json,
    /// Newline-delimited JSON (one event per line)
    #[default]
    Jsonl,
}

/// Partition strategy for S3 keys
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum S3Partitioning {
    /// No partitioning: prefix/file.json
    #[default]
    None,
    /// Daily partitioning: prefix/year=2024/month=01/day=15/file.json
    Day,
    /// Hourly partitioning: prefix/year=2024/month=01/day=15/hour=14/file.json
    Hour,
}

/// Compression for S3 objects
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum S3Compression {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
}

/// Configuration for the S3 sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct S3SinkConfig {
    /// S3 bucket name
    #[validate(length(min = 3, max = 63))]
    pub bucket: String,

    /// Optional key prefix (e.g., "cdc/events/")
    #[serde(default)]
    pub prefix: String,

    /// AWS region (default: us-east-1)
    #[serde(default = "default_region")]
    pub region: String,

    /// Custom S3 endpoint URL (for S3-compatible services like MinIO)
    #[serde(default)]
    pub endpoint_url: Option<String>,

    /// AWS access key ID (uses environment/instance role if not provided)
    #[serde(default)]
    pub access_key_id: Option<SensitiveString>,

    /// AWS secret access key
    #[serde(default)]
    pub secret_access_key: Option<SensitiveString>,

    /// Output format (json or jsonl)
    #[serde(default)]
    pub format: S3Format,

    /// Partition strategy for S3 keys
    #[serde(default)]
    pub partition_by: S3Partitioning,

    /// Number of events to batch before writing
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: usize,

    /// Compression to use for S3 objects
    #[serde(default)]
    pub compression: S3Compression,

    /// Flush interval in seconds (max time between writes)
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval() -> u64 {
    60
}

impl Default for S3SinkConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            region: default_region(),
            endpoint_url: None,
            access_key_id: None,
            secret_access_key: None,
            format: S3Format::default(),
            partition_by: S3Partitioning::default(),
            batch_size: default_batch_size(),
            compression: S3Compression::default(),
            flush_interval_secs: default_flush_interval(),
        }
    }
}

/// S3 Sink implementation
pub struct S3Sink;

impl S3Sink {
    /// Create a new S3 sink instance
    pub fn new() -> Self {
        Self
    }

    async fn create_client(config: &S3SinkConfig) -> rivven_connect::error::Result<S3Client> {
        let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()));

        // Use explicit credentials if provided
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let creds = aws_sdk_s3::config::Credentials::new(
                access_key.expose_secret(),
                secret_key.expose_secret(),
                None,
                None,
                "rivven-storage",
            );
            aws_config_loader = aws_config_loader.credentials_provider(creds);
        }

        let aws_config = aws_config_loader.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

        // Use custom endpoint for S3-compatible services
        if let Some(endpoint) = &config.endpoint_url {
            s3_config_builder = s3_config_builder
                .endpoint_url(endpoint)
                .force_path_style(true);
        }

        Ok(S3Client::from_conf(s3_config_builder.build()))
    }

    fn generate_key(config: &S3SinkConfig, timestamp: DateTime<Utc>, batch_id: u64) -> String {
        let extension = match (&config.format, &config.compression) {
            (S3Format::Json, S3Compression::None) => "json",
            (S3Format::Json, S3Compression::Gzip) => "json.gz",
            (S3Format::Jsonl, S3Compression::None) => "jsonl",
            (S3Format::Jsonl, S3Compression::Gzip) => "jsonl.gz",
        };

        let partition = match config.partition_by {
            S3Partitioning::None => String::new(),
            S3Partitioning::Day => {
                format!(
                    "year={}/month={:02}/day={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d")
                )
            }
            S3Partitioning::Hour => {
                format!(
                    "year={}/month={:02}/day={:02}/hour={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d"),
                    timestamp.format("%H")
                )
            }
        };

        let filename = format!(
            "events_{}_{}.{}",
            timestamp.format("%Y%m%d_%H%M%S"),
            batch_id,
            extension
        );

        let prefix = if config.prefix.is_empty() {
            String::new()
        } else if config.prefix.ends_with('/') {
            config.prefix.clone()
        } else {
            format!("{}/", config.prefix)
        };

        format!("{}{}{}", prefix, partition, filename)
    }

    fn serialize_batch(
        events: &[serde_json::Value],
        format: &S3Format,
        compression: &S3Compression,
    ) -> rivven_connect::ConnectorResult<Vec<u8>> {
        let json_data = match format {
            S3Format::Json => serde_json::to_vec_pretty(events)
                .map_err(|e| ConnectorError::Serialization(e.to_string()))?,
            S3Format::Jsonl => {
                let mut data = Vec::new();
                for event in events {
                    serde_json::to_writer(&mut data, event)
                        .map_err(|e| ConnectorError::Serialization(e.to_string()))?;
                    data.push(b'\n');
                }
                data
            }
        };

        match compression {
            S3Compression::None => Ok(json_data),
            S3Compression::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&json_data).map_err(ConnectorError::Io)?;
                encoder.finish().map_err(ConnectorError::Io)
            }
        }
    }
}

impl Default for S3Sink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for S3Sink {
    type Config = S3SinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("s3", env!("CARGO_PKG_VERSION"))
            .description("Amazon S3 sink - writes events to S3 in JSON/JSONL format")
            .documentation_url("https://rivven.dev/docs/connectors/s3-sink")
            .config_schema_from::<S3SinkConfig>()
            .metadata("format", "json,jsonl")
            .metadata("compression", "gzip,none")
            .metadata("partitioning", "none,day,hour")
    }

    async fn check(&self, config: &Self::Config) -> rivven_connect::error::Result<CheckResult> {
        info!("Checking S3 connectivity for bucket: {}", config.bucket);

        let client = Self::create_client(config).await?;

        // Try to head the bucket to verify access
        match client.head_bucket().bucket(&config.bucket).send().await {
            Ok(_) => {
                info!("Successfully connected to S3 bucket: {}", config.bucket);
                Ok(CheckResult::success())
            }
            Err(e) => {
                let msg = format!("Failed to access S3 bucket '{}': {}", config.bucket, e);
                warn!("{}", msg);
                Ok(CheckResult::failure(msg))
            }
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> rivven_connect::error::Result<WriteResult> {
        let client = Self::create_client(config).await?;
        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut batch_id = 0u64;
        let mut last_flush = std::time::Instant::now();
        let flush_interval = std::time::Duration::from_secs(config.flush_interval_secs);

        info!(
            "Starting S3 sink: bucket={}, prefix={}, format={:?}, batch_size={}",
            config.bucket, config.prefix, config.format, config.batch_size
        );

        while let Some(event) = events.next().await {
            // Convert event to JSON
            let json_event = serde_json::json!({
                "event_type": event.event_type,
                "stream": event.stream,
                "namespace": event.namespace,
                "timestamp": event.timestamp.to_rfc3339(),
                "data": event.data,
                "metadata": event.metadata,
            });
            batch.push(json_event);

            // Flush if batch is full or timeout exceeded
            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                let timestamp = Utc::now();
                let key = Self::generate_key(config, timestamp, batch_id);
                let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;

                debug!(
                    "Uploading {} events to s3://{}/{}",
                    batch.len(),
                    config.bucket,
                    key
                );

                client
                    .put_object()
                    .bucket(&config.bucket)
                    .key(&key)
                    .body(ByteStream::from(data))
                    .content_type(match (&config.format, &config.compression) {
                        (S3Format::Json, S3Compression::None) => "application/json",
                        (S3Format::Jsonl, S3Compression::None) => "application/x-ndjson",
                        (_, S3Compression::Gzip) => "application/gzip",
                    })
                    .send()
                    .await
                    .map_err(|e| {
                        error!("Failed to upload to S3: {}", e);
                        ConnectorError::Connection(format!("S3 upload failed: {}", e))
                    })?;

                info!(
                    "Uploaded {} events to s3://{}/{}",
                    batch.len(),
                    config.bucket,
                    key
                );

                total_written += batch.len() as u64;
                batch.clear();
                batch_id += 1;
                last_flush = std::time::Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            let timestamp = Utc::now();
            let key = Self::generate_key(config, timestamp, batch_id);
            let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;

            client
                .put_object()
                .bucket(&config.bucket)
                .key(&key)
                .body(ByteStream::from(data))
                .send()
                .await
                .map_err(|e| ConnectorError::Connection(format!("S3 upload failed: {}", e)))?;

            info!(
                "Uploaded final {} events to s3://{}/{}",
                batch.len(),
                config.bucket,
                key
            );

            total_written += batch.len() as u64;
        }

        info!("S3 sink completed: {} total events written", total_written);

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: 0,
            records_failed: 0,
            errors: vec![],
        })
    }
}

/// Factory for creating S3 sink instances
pub struct S3SinkFactory;

impl SinkFactory for S3SinkFactory {
    fn spec(&self) -> ConnectorSpec {
        S3Sink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(S3Sink::new())
    }
}

// Implement AnySink for S3Sink
rivven_connect::impl_any_sink!(S3Sink, S3SinkConfig);

/// Register the S3 sink with the given registry
pub fn register(registry: &mut rivven_connect::SinkRegistry) {
    use std::sync::Arc;
    registry.register("s3", Arc::new(S3SinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = S3Sink::spec();
        assert_eq!(spec.connector_type, "s3");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = S3SinkConfig::default();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_secs, 60);
        assert!(matches!(config.format, S3Format::Jsonl));
        assert!(matches!(config.compression, S3Compression::None));
    }

    #[test]
    fn test_generate_key_no_partition() {
        let config = S3SinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "data".to_string(),
            partition_by: S3Partitioning::None,
            format: S3Format::Jsonl,
            compression: S3Compression::None,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let key = S3Sink::generate_key(&config, timestamp, 1);

        assert!(key.starts_with("data/events_20240115_143000_1.jsonl"));
    }

    #[test]
    fn test_generate_key_day_partition() {
        let config = S3SinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "cdc".to_string(),
            partition_by: S3Partitioning::Day,
            format: S3Format::Json,
            compression: S3Compression::Gzip,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let key = S3Sink::generate_key(&config, timestamp, 0);

        assert!(key.contains("year=2024/month=01/day=15/"));
        assert!(key.ends_with(".json.gz"));
    }

    #[test]
    fn test_factory() {
        let factory = S3SinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "s3");
        let _sink = factory.create();
    }
}
