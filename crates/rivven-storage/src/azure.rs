//! Azure Blob Storage sink connector
//!
//! This module provides a sink connector for writing data to Azure Blob Storage.
//!
//! # Features
//!
//! - JSON and JSONL output formats
//! - Configurable partitioning (none, daily, hourly)
//! - Gzip compression
//! - Batching with configurable size and flush interval
//! - Multiple authentication methods (connection string, SAS token, managed identity)
//!
//! # Authentication
//!
//! The connector supports multiple authentication methods:
//!
//! 1. **Connection String** - Full connection string with account key
//! 2. **SAS Token** - Shared Access Signature token
//! 3. **Account Key** - Storage account name + key
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_storage::azure::AzureBlobSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("azure-blob", Arc::new(AzureBlobSinkFactory));
//! ```

use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage::ConnectionString;
use azure_storage_blobs::prelude::*;
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

/// Output format for Azure Blob sink
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AzureBlobFormat {
    /// JSON array of events
    Json,
    /// Newline-delimited JSON (one event per line)
    #[default]
    Jsonl,
}

/// Partition strategy for Azure Blob names
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AzureBlobPartitioning {
    /// No partitioning: prefix/file.json
    #[default]
    None,
    /// Daily partitioning: prefix/year=2024/month=01/day=15/file.json
    Day,
    /// Hourly partitioning: prefix/year=2024/month=01/day=15/hour=14/file.json
    Hour,
}

/// Compression for Azure Blob objects
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AzureBlobCompression {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
}

/// Configuration for the Azure Blob Storage sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct AzureBlobSinkConfig {
    /// Azure storage account name
    #[validate(length(min = 3, max = 24))]
    pub account_name: String,

    /// Azure storage container name
    #[validate(length(min = 3, max = 63))]
    pub container: String,

    /// Optional blob name prefix (e.g., "cdc/events/")
    #[serde(default)]
    pub prefix: String,

    /// Azure storage account key (used with account_name)
    #[serde(default)]
    pub account_key: Option<SensitiveString>,

    /// Full connection string (alternative to account_name + account_key)
    #[serde(default)]
    pub connection_string: Option<SensitiveString>,

    /// SAS token for authentication (alternative to account_key)
    #[serde(default)]
    pub sas_token: Option<SensitiveString>,

    /// Output format (json or jsonl)
    #[serde(default)]
    pub format: AzureBlobFormat,

    /// Partition strategy for blob names
    #[serde(default)]
    pub partition_by: AzureBlobPartitioning,

    /// Number of events to batch before writing
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: usize,

    /// Compression to use for blobs
    #[serde(default)]
    pub compression: AzureBlobCompression,

    /// Flush interval in seconds (max time between writes)
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Access tier for uploaded blobs (Hot, Cool, Cold, Archive)
    #[serde(default = "default_access_tier")]
    pub access_tier: String,
}

fn default_batch_size() -> usize {
    1000
}

fn default_flush_interval() -> u64 {
    60
}

fn default_access_tier() -> String {
    "Hot".to_string()
}

impl Default for AzureBlobSinkConfig {
    fn default() -> Self {
        Self {
            account_name: String::new(),
            container: String::new(),
            prefix: String::new(),
            account_key: None,
            connection_string: None,
            sas_token: None,
            format: AzureBlobFormat::default(),
            partition_by: AzureBlobPartitioning::default(),
            batch_size: default_batch_size(),
            compression: AzureBlobCompression::default(),
            flush_interval_secs: default_flush_interval(),
            access_tier: default_access_tier(),
        }
    }
}

/// Azure Blob Storage Sink implementation
pub struct AzureBlobSink;

impl AzureBlobSink {
    /// Create a new Azure Blob Storage sink instance
    pub fn new() -> Self {
        Self
    }

    fn create_client(
        config: &AzureBlobSinkConfig,
    ) -> rivven_connect::error::Result<ContainerClient> {
        let container_client = if let Some(conn_str) = &config.connection_string {
            // Use connection string
            let conn = ConnectionString::new(conn_str.expose_secret())
                .map_err(|e| ConnectorError::Config(format!("Invalid connection string: {}", e)))?;

            let storage_credentials = conn.storage_credentials().map_err(|e| {
                ConnectorError::Config(format!(
                    "Failed to get credentials from connection string: {}",
                    e
                ))
            })?;

            let account_name = conn.account_name.ok_or_else(|| {
                ConnectorError::Config("Connection string missing AccountName".to_string())
            })?;

            let blob_service =
                BlobServiceClient::new(account_name.to_string(), storage_credentials);
            blob_service.container_client(&config.container)
        } else if let Some(account_key) = &config.account_key {
            // Use account name + key
            let storage_credentials = StorageCredentials::access_key(
                config.account_name.clone(),
                account_key.expose_secret().to_string(),
            );
            let blob_service =
                BlobServiceClient::new(config.account_name.clone(), storage_credentials);
            blob_service.container_client(&config.container)
        } else if let Some(sas_token) = &config.sas_token {
            // Use SAS token
            let storage_credentials = StorageCredentials::sas_token(sas_token.expose_secret())
                .map_err(|e| ConnectorError::Config(format!("Invalid SAS token: {}", e)))?;
            let blob_service =
                BlobServiceClient::new(config.account_name.clone(), storage_credentials);
            blob_service.container_client(&config.container)
        } else {
            return Err(ConnectorError::Config(
                "No authentication method provided. Use connection_string, account_key, or sas_token".to_string(),
            ).into());
        };

        Ok(container_client)
    }

    fn generate_blob_name(
        config: &AzureBlobSinkConfig,
        timestamp: DateTime<Utc>,
        batch_id: u64,
    ) -> String {
        let extension = match (&config.format, &config.compression) {
            (AzureBlobFormat::Json, AzureBlobCompression::None) => "json",
            (AzureBlobFormat::Json, AzureBlobCompression::Gzip) => "json.gz",
            (AzureBlobFormat::Jsonl, AzureBlobCompression::None) => "jsonl",
            (AzureBlobFormat::Jsonl, AzureBlobCompression::Gzip) => "jsonl.gz",
        };

        let partition = match config.partition_by {
            AzureBlobPartitioning::None => String::new(),
            AzureBlobPartitioning::Day => {
                format!(
                    "year={}/month={:02}/day={:02}/",
                    timestamp.format("%Y"),
                    timestamp.format("%m"),
                    timestamp.format("%d")
                )
            }
            AzureBlobPartitioning::Hour => {
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
        format: &AzureBlobFormat,
        compression: &AzureBlobCompression,
    ) -> rivven_connect::ConnectorResult<Vec<u8>> {
        let json_data = match format {
            AzureBlobFormat::Json => serde_json::to_vec_pretty(events)
                .map_err(|e| ConnectorError::Serialization(e.to_string()))?,
            AzureBlobFormat::Jsonl => {
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
            AzureBlobCompression::None => Ok(json_data),
            AzureBlobCompression::Gzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&json_data).map_err(ConnectorError::Io)?;
                encoder.finish().map_err(ConnectorError::Io)
            }
        }
    }

    fn get_content_type(
        format: &AzureBlobFormat,
        compression: &AzureBlobCompression,
    ) -> &'static str {
        match (format, compression) {
            (AzureBlobFormat::Json, AzureBlobCompression::None) => "application/json",
            (AzureBlobFormat::Jsonl, AzureBlobCompression::None) => "application/x-ndjson",
            (_, AzureBlobCompression::Gzip) => "application/gzip",
        }
    }
}

impl Default for AzureBlobSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for AzureBlobSink {
    type Config = AzureBlobSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("azure-blob", env!("CARGO_PKG_VERSION"))
            .description(
                "Azure Blob Storage sink - writes events to Azure Blob Storage in JSON/JSONL format",
            )
            .documentation_url("https://rivven.dev/docs/connectors/azure-blob-sink")
            .config_schema_from::<AzureBlobSinkConfig>()
            .metadata("format", "json,jsonl")
            .metadata("compression", "gzip,none")
            .metadata("partitioning", "none,day,hour")
            .metadata("access_tier", "Hot,Cool,Cold,Archive")
    }

    async fn check(&self, config: &Self::Config) -> rivven_connect::error::Result<CheckResult> {
        info!(
            "Checking Azure Blob Storage connectivity for container: {}",
            config.container
        );

        let container_client = Self::create_client(config)?;

        // Try to get container properties to verify access
        match container_client.get_properties().await {
            Ok(_) => {
                info!(
                    "Successfully connected to Azure Blob container: {}",
                    config.container
                );
                Ok(CheckResult::success())
            }
            Err(e) => {
                let msg = format!(
                    "Failed to access Azure Blob container '{}': {}",
                    config.container, e
                );
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
        let container_client = Self::create_client(config)?;
        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut batch_id = 0u64;
        let mut last_flush = std::time::Instant::now();
        let flush_interval = std::time::Duration::from_secs(config.flush_interval_secs);

        info!(
            "Starting Azure Blob sink: container={}, prefix={}, format={:?}, batch_size={}",
            config.container, config.prefix, config.format, config.batch_size
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
                let blob_name = Self::generate_blob_name(config, timestamp, batch_id);
                let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;
                let content_type = Self::get_content_type(&config.format, &config.compression);

                debug!(
                    "Uploading {} events to {}/{}",
                    batch.len(),
                    config.container,
                    blob_name
                );

                let blob_client = container_client.blob_client(&blob_name);
                blob_client
                    .put_block_blob(data)
                    .content_type(content_type)
                    .await
                    .map_err(|e| {
                        error!("Failed to upload to Azure Blob: {}", e);
                        ConnectorError::Connection(format!("Azure Blob upload failed: {}", e))
                    })?;

                info!(
                    "Uploaded {} events to {}/{} (content-type: {})",
                    batch.len(),
                    config.container,
                    blob_name,
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
            let blob_name = Self::generate_blob_name(config, timestamp, batch_id);
            let data = Self::serialize_batch(&batch, &config.format, &config.compression)?;

            let blob_client = container_client.blob_client(&blob_name);
            blob_client.put_block_blob(data).await.map_err(|e| {
                ConnectorError::Connection(format!("Azure Blob upload failed: {}", e))
            })?;

            info!(
                "Uploaded final {} events to {}/{}",
                batch.len(),
                config.container,
                blob_name
            );

            total_written += batch.len() as u64;
        }

        info!(
            "Azure Blob sink completed: {} total events written",
            total_written
        );

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: 0,
            records_failed: 0,
            errors: vec![],
        })
    }
}

/// Factory for creating Azure Blob Storage sink instances
pub struct AzureBlobSinkFactory;

impl SinkFactory for AzureBlobSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        AzureBlobSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(AzureBlobSink::new())
    }
}

// Implement AnySink for AzureBlobSink
rivven_connect::impl_any_sink!(AzureBlobSink, AzureBlobSinkConfig);

/// Register the Azure Blob Storage sink with the given registry
pub fn register(registry: &mut rivven_connect::SinkRegistry) {
    use std::sync::Arc;
    registry.register("azure-blob", Arc::new(AzureBlobSinkFactory));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = AzureBlobSink::spec();
        assert_eq!(spec.connector_type, "azure-blob");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = AzureBlobSinkConfig::default();
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_secs, 60);
        assert_eq!(config.access_tier, "Hot");
        assert!(matches!(config.format, AzureBlobFormat::Jsonl));
        assert!(matches!(config.compression, AzureBlobCompression::None));
    }

    #[test]
    fn test_generate_blob_name_no_partition() {
        let config = AzureBlobSinkConfig {
            account_name: "testaccount".to_string(),
            container: "test-container".to_string(),
            prefix: "data".to_string(),
            partition_by: AzureBlobPartitioning::None,
            format: AzureBlobFormat::Jsonl,
            compression: AzureBlobCompression::None,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = AzureBlobSink::generate_blob_name(&config, timestamp, 1);

        assert!(name.starts_with("data/events_20240115_143000_1.jsonl"));
    }

    #[test]
    fn test_generate_blob_name_day_partition() {
        let config = AzureBlobSinkConfig {
            account_name: "testaccount".to_string(),
            container: "test-container".to_string(),
            prefix: "cdc".to_string(),
            partition_by: AzureBlobPartitioning::Day,
            format: AzureBlobFormat::Json,
            compression: AzureBlobCompression::Gzip,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-01-15T14:30:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = AzureBlobSink::generate_blob_name(&config, timestamp, 0);

        assert!(name.contains("year=2024/month=01/day=15/"));
        assert!(name.ends_with(".json.gz"));
    }

    #[test]
    fn test_generate_blob_name_hour_partition() {
        let config = AzureBlobSinkConfig {
            account_name: "testaccount".to_string(),
            container: "test-container".to_string(),
            prefix: "".to_string(),
            partition_by: AzureBlobPartitioning::Hour,
            format: AzureBlobFormat::Jsonl,
            compression: AzureBlobCompression::None,
            ..Default::default()
        };

        let timestamp = DateTime::parse_from_rfc3339("2024-06-20T09:45:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let name = AzureBlobSink::generate_blob_name(&config, timestamp, 5);

        assert!(name.contains("year=2024/month=06/day=20/hour=09/"));
        assert!(name.ends_with(".jsonl"));
    }

    #[test]
    fn test_content_type() {
        assert_eq!(
            AzureBlobSink::get_content_type(&AzureBlobFormat::Json, &AzureBlobCompression::None),
            "application/json"
        );
        assert_eq!(
            AzureBlobSink::get_content_type(&AzureBlobFormat::Jsonl, &AzureBlobCompression::None),
            "application/x-ndjson"
        );
        assert_eq!(
            AzureBlobSink::get_content_type(&AzureBlobFormat::Json, &AzureBlobCompression::Gzip),
            "application/gzip"
        );
        assert_eq!(
            AzureBlobSink::get_content_type(&AzureBlobFormat::Jsonl, &AzureBlobCompression::Gzip),
            "application/gzip"
        );
    }

    #[test]
    fn test_factory() {
        let factory = AzureBlobSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "azure-blob");
        let _sink = factory.create();
    }

    #[test]
    fn test_serialize_batch_jsonl() {
        let events = vec![
            serde_json::json!({"id": 1, "name": "test1"}),
            serde_json::json!({"id": 2, "name": "test2"}),
        ];

        let data = AzureBlobSink::serialize_batch(
            &events,
            &AzureBlobFormat::Jsonl,
            &AzureBlobCompression::None,
        )
        .unwrap();
        let result = String::from_utf8(data).unwrap();

        assert!(result.contains(r#"{"id":1,"name":"test1"}"#));
        assert!(result.contains(r#"{"id":2,"name":"test2"}"#));
        assert_eq!(result.matches('\n').count(), 2);
    }

    #[test]
    fn test_serialize_batch_json() {
        let events = vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})];

        let data = AzureBlobSink::serialize_batch(
            &events,
            &AzureBlobFormat::Json,
            &AzureBlobCompression::None,
        )
        .unwrap();
        let result = String::from_utf8(data).unwrap();

        // Should be valid JSON array
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_serialize_batch_gzip() {
        let events = vec![serde_json::json!({"id": 1, "data": "test data for compression"})];

        let data = AzureBlobSink::serialize_batch(
            &events,
            &AzureBlobFormat::Jsonl,
            &AzureBlobCompression::Gzip,
        )
        .unwrap();

        // Gzip magic bytes
        assert!(data.len() >= 2);
        assert_eq!(data[0], 0x1f);
        assert_eq!(data[1], 0x8b);
    }

    #[test]
    fn test_create_client_no_auth() {
        let config = AzureBlobSinkConfig {
            account_name: "testaccount".to_string(),
            container: "test-container".to_string(),
            ..Default::default()
        };

        let result = AzureBlobSink::create_client(&config);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No authentication method"));
    }
}
