//! Google Cloud Storage (GCS) sink connector
//!
//! This module provides a sink connector for writing data to Google Cloud Storage.
//!
//! # Features
//!
//! - JSON and JSONL output formats
//! - Configurable partitioning (none, daily, hourly)
//! - Gzip compression
//! - Batching with configurable size and flush interval
//! - Automatic credential resolution (ADC, service account key, or explicit)
//!
//! # Authentication
//!
//! The connector supports multiple authentication methods:
//!
//! 1. **Application Default Credentials (ADC)** - Automatic (default)
//! 2. **Service Account Key File** - Via `credentials_file` config
//! 3. **Service Account JSON** - Via `credentials_json` config (for secrets managers)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_storage::gcs::GcsSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("gcs", Arc::new(GcsSinkFactory));
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use google_cloud_storage::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::buckets::get::GetBucketRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
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

/// Output format for GCS sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GcsFormat {
    /// JSON array of events
    Json,
    /// Newline-delimited JSON (one event per line)
    #[default]
    Jsonl,
}

/// Partition strategy for GCS object names
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum GcsPartitioning {
    /// No partitioning: prefix/file.json
    #[default]
    None,
    /// Daily partitioning: prefix/year=2024/month=01/day=15/file.json
    Day,
    /// Hourly partitioning: prefix/year=2024/month=01/day=15/hour=14/file.json
    Hour,
}

/// Compression for GCS objects
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum GcsCompression {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
}

/// Configuration for the GCS sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct GcsSinkConfig {
    /// GCS bucket name
    #[validate(length(min = 3, max = 222))]
    pub bucket: String,

    /// Optional object name prefix (e.g., "cdc/events/")
    #[serde(default)]
    pub prefix: String,

    /// GCP project ID (optional, uses default if not specified)
    #[serde(default)]
    pub project_id: Option<String>,

    /// Path to service account credentials JSON file
    /// If not provided, uses Application Default Credentials (ADC)
    #[serde(default)]
    pub credentials_file: Option<String>,

    /// Service account credentials as JSON string
    /// Useful when credentials are stored in a secrets manager
    #[serde(default)]
    pub credentials_json: Option<SensitiveString>,

    /// Output format (json or jsonl)
    #[serde(default)]
    pub format: GcsFormat,

    /// Partition strategy for object names
    #[serde(default)]
    pub partition_by: GcsPartitioning,

    /// Number of events to batch before writing
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: usize,

    /// Compression to use for GCS objects
    #[serde(default)]
    pub compression: GcsCompression,

    /// Flush interval in seconds (max time between writes)
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Storage class for uploaded objects (STANDARD, NEARLINE, COLDLINE, ARCHIVE)
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval() -> u64 {
    60
}

fn default_storage_class() -> String {
    "STANDARD".to_string()
}

impl Default for GcsSinkConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            project_id: None,
            credentials_file: None,
            credentials_json: None,
            format: GcsFormat::default(),
            partition_by: GcsPartitioning::default(),
            batch_size: default_batch_size(),
            compression: GcsCompression::default(),
            flush_interval_secs: default_flush_interval(),
            storage_class: default_storage_class(),
        }
    }
}

/// GCS Sink implementation
pub struct GcsSink;

impl GcsSink {
    /// Create a new GCS sink instance
    pub fn new() -> Self {
        Self
    }

    async fn create_client(config: &GcsSinkConfig) -> rivven_connect::error::Result<Client> {
        let client_config = if let Some(json) = &config.credentials_json {
            // Use credentials from JSON string
            let creds: CredentialsFile = serde_json::from_str(json.expose_secret())
                .map_err(|e| ConnectorError::Config(format!("Invalid credentials JSON: {}", e)))?;
            ClientConfig::default()
                .with_credentials(creds)
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!("Failed to load credentials: {}", e))
                })?
        } else if let Some(file_path) = &config.credentials_file {
            // Use credentials from file
            let creds = CredentialsFile::new_from_file(file_path.to_string())
                .await
                .map_err(|e| {
                    ConnectorError::Config(format!(
                        "Failed to load credentials from '{}': {}",
                        file_path, e
                    ))
                })?;
            ClientConfig::default()
                .with_credentials(creds)
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!("Failed to configure credentials: {}", e))
                })?
        } else {
            // Use Application Default Credentials
            ClientConfig::default().with_auth().await.map_err(|e| {
                ConnectorError::Connection(format!(
                    "Failed to initialize GCS client with ADC: {}",
                    e
                ))
            })?
        };

        Ok(Client::new(client_config))
    }

    fn generate_object_name(
        config: &GcsSinkConfig,
        timestamp: DateTime<Utc>,
        batch_id: u64,
    ) -> String {
        let extension = match (&config.format, &config.compression) {
            (GcsFormat::Json, GcsCompression::None) => "json",
            (GcsFormat::Json, GcsCompression::Gzip) => "json.gz",
            (GcsFormat::Jsonl, GcsCompression::None) => "jsonl",
            (GcsFormat::Jsonl, GcsCompression::Gzip) => "jsonl.gz",
        };

        let partition = match config.partition_by {
            GcsPartitioning::None => String::new(),
            GcsPartitioning::Day => {
                format!(
                    "year={}/month={:02}/day={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d")
                )
            }
            GcsPartitioning::Hour => {
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
        format: &GcsFormat,
        compression: &GcsCompression,
    ) -> rivven_connect::ConnectorResult<Vec<u8>> {
        let json_data = match format {
            GcsFormat::Json => serde_json::to_vec_pretty(events)
                .map_err(|e| ConnectorError::Serialization(e.to_string()))?,
            GcsFormat::Jsonl => {
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
            GcsCompression::None => Ok(json_data),
            GcsCompression::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&json_data).map_err(ConnectorError::Io)?;
                encoder.finish().map_err(ConnectorError::Io)
            }
        }
    }

    fn get_content_type(format: &GcsFormat, compression: &GcsCompression) -> &'static str {
        match (format, compression) {
            (GcsFormat::Json, GcsCompression::None) => "application/json",
            (GcsFormat::Jsonl, GcsCompression::None) => "application/x-ndjson",
            (_, GcsCompression::Gzip) => "application/gzip",
        }
    }
}

impl Default for GcsSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for GcsSink {
    type Config = GcsSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("gcs", env!("CARGO_PKG_VERSION"))
            .description("Google Cloud Storage sink - writes events to GCS in JSON/JSONL format")
            .documentation_url("https://rivven.dev/docs/connectors/gcs-sink")
            .config_schema_from::<GcsSinkConfig>()
            .metadata("format", "json,jsonl")
            .metadata("compression", "gzip,none")
            .metadata("partitioning", "none,day,hour")
            .metadata("storage_class", "STANDARD,NEARLINE,COLDLINE,ARCHIVE")
    }

    async fn check(&self, config: &Self::Config) -> rivven_connect::error::Result<CheckResult> {
        info!("Checking GCS connectivity for bucket: {}", config.bucket);

        let client = Self::create_client(config).await?;

        // Try to get bucket metadata to verify access
        let req = GetBucketRequest {
            bucket: config.bucket.clone(),
            ..Default::default()
        };

        match client.get_bucket(&req).await {
            Ok(bucket) => {
                info!(
                    "Successfully connected to GCS bucket: {} (location: {})",
                    config.bucket, bucket.location
                );
                Ok(CheckResult::success())
            }
            Err(e) => {
                let msg = format!("Failed to access GCS bucket '{}': {}", config.bucket, e);
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
            "Starting GCS sink: bucket={}, prefix={}, format={:?}, batch_size={}",
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
                let object_name = Self::generate_object_name(config, timestamp, batch_id);
                let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;
                let content_type = Self::get_content_type(&config.format, &config.compression);

                debug!(
                    "Uploading {} events to gs://{}/{}",
                    batch.len(),
                    config.bucket,
                    object_name
                );

                let upload_type = UploadType::Simple(Media::new(object_name.clone()));
                let req = UploadObjectRequest {
                    bucket: config.bucket.clone(),
                    ..Default::default()
                };

                client
                    .upload_object(&req, data, &upload_type)
                    .await
                    .map_err(|e| {
                        error!("Failed to upload to GCS: {}", e);
                        ConnectorError::Connection(format!("GCS upload failed: {}", e))
                    })?;

                info!(
                    "Uploaded {} events to gs://{}/{} (content-type: {})",
                    batch.len(),
                    config.bucket,
                    object_name,
                    content_type
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
            let object_name = Self::generate_object_name(config, timestamp, batch_id);
            let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;

            let upload_type = UploadType::Simple(Media::new(object_name.clone()));
            let req = UploadObjectRequest {
                bucket: config.bucket.clone(),
                ..Default::default()
            };

            client
                .upload_object(&req, data, &upload_type)
                .await
                .map_err(|e| ConnectorError::Connection(format!("GCS upload failed: {}", e)))?;

            info!(
                "Uploaded final {} events to gs://{}/{}",
                batch.len(),
                config.bucket,
                object_name
            );

            total_written += batch.len() as u64;
        }

        info!("GCS sink completed: {} total events written", total_written);

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: 0,
            records_failed: 0,
            errors: vec![],
        })
    }
}

/// Factory for creating GCS sink instances
pub struct GcsSinkFactory;

impl SinkFactory for GcsSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        GcsSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(GcsSink::new())
    }
}

// Implement AnySink for GcsSink
rivven_connect::impl_any_sink!(GcsSink, GcsSinkConfig);

/// Register the GCS sink with the given registry
pub fn register(registry: &mut rivven_connect::SinkRegistry) {
    use std::sync::Arc;
    registry.register("gcs", Arc::new(GcsSinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = GcsSink::spec();
        assert_eq!(spec.connector_type, "gcs");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = GcsSinkConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_secs, 60);
        assert_eq!(config.storage_class, "STANDARD");
        assert!(matches!(config.format, GcsFormat::Jsonl));
        assert!(matches!(config.compression, GcsCompression::None));
    }

    #[test]
    fn test_generate_object_name_no_partition() {
        let config = GcsSinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "data".to_string(),
            partition_by: GcsPartitioning::None,
            format: GcsFormat::Jsonl,
            compression: GcsCompression::None,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = GcsSink::generate_object_name(&config, timestamp, 1);

        assert!(name.starts_with("data/events_20240115_143000_1.jsonl"));
    }

    #[test]
    fn test_generate_object_name_day_partition() {
        let config = GcsSinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "cdc".to_string(),
            partition_by: GcsPartitioning::Day,
            format: GcsFormat::Json,
            compression: GcsCompression::Gzip,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = GcsSink::generate_object_name(&config, timestamp, 0);

        assert!(name.contains("year=2024/month=01/day=15/"));
        assert!(name.ends_with(".json.gz"));
    }

    #[test]
    fn test_generate_object_name_hour_partition() {
        let config = GcsSinkConfig {
            bucket: "test-bucket".to_string(),
            prefix: "".to_string(),
            partition_by: GcsPartitioning::Hour,
            format: GcsFormat::Jsonl,
            compression: GcsCompression::None,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-06-20T09:45:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = GcsSink::generate_object_name(&config, timestamp, 5);

        assert!(name.contains("year=2024/month=06/day=20/hour=09/"));
        assert!(name.ends_with(".jsonl"));
    }

    #[test]
    fn test_content_type() {
        assert_eq!(
            GcsSink::get_content_type(&GcsFormat::Json, &GcsCompression::None),
            "application/json"
        );
        assert_eq!(
            GcsSink::get_content_type(&GcsFormat::Jsonl, &GcsCompression::None),
            "application/x-ndjson"
        );
        assert_eq!(
            GcsSink::get_content_type(&GcsFormat::Json, &GcsCompression::Gzip),
            "application/gzip"
        );
        assert_eq!(
            GcsSink::get_content_type(&GcsFormat::Jsonl, &GcsCompression::Gzip),
            "application/gzip"
        );
    }

    #[test]
    fn test_factory() {
        let factory = GcsSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "gcs");
        let _sink = factory.create();
    }

    #[test]
    fn test_serialize_batch_jsonl() {
        let events = vec![
            serde_json::json!({"id": 1, "name": "test1"}),
            serde_json::json!({"id": 2, "name": "test2"}),
        ];

        let data =
            GcsSink::serialize_batch(&events, &GcsFormat::Jsonl, &GcsCompression::None).unwrap();
        let result = String::from_utf8(data).unwrap();

        assert!(result.contains(r#"{"id":1,"name":"test1"}"#));
        assert!(result.contains(r#"{"id":2,"name":"test2"}"#));
        assert_eq!(result.matches('\n').count(), 2);
    }

    #[test]
    fn test_serialize_batch_json() {
        let events = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];

        let data =
            GcsSink::serialize_batch(&events, &GcsFormat::Json, &GcsCompression::None).unwrap();
        let result = String::from_utf8(data).unwrap();

        // Should be valid JSON array
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_serialize_batch_gzip() {
        let events = vec![serde_json::json!({"id": 1, "data": "test data for compression"})];

        let data =
            GcsSink::serialize_batch(&events, &GcsFormat::Jsonl, &GcsCompression::Gzip).unwrap();

        // Gzip magic bytes
        assert!(data.len() >= 2);
        assert_eq!(data[0], 0x1f);
        assert_eq!(data[1], 0x8b);
    }
}
