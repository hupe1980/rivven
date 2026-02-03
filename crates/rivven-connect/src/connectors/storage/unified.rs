//! Unified Object Storage Sink using the `object_store` crate
//!
//! This module provides a **single sink connector** that works with:
//! - Amazon S3 and S3-compatible storage (MinIO, Cloudflare R2, DigitalOcean Spaces)
//! - Google Cloud Storage (GCS)
//! - Azure Blob Storage
//! - Local filesystem (for testing)
//!
//! # Architecture
//!
//! Instead of maintaining three separate connectors (S3, GCS, Azure), we use the
//! `object_store` crate which provides a unified `ObjectStore` trait. This:
//! - Reduces code duplication by ~800 lines
//! - Ensures consistent behavior across providers
//! - Simplifies testing with local filesystem backend
//! - Is battle-tested (used by Apache Arrow, DataFusion, Delta Lake)
//!
//! # Example Configuration
//!
//! ```yaml
//! # S3
//! sinks:
//!   s3_events:
//!     connector: object-storage
//!     config:
//!       provider: s3
//!       bucket: my-bucket
//!       region: us-east-1
//!       prefix: events/
//!       format: jsonl
//!       compression: gzip
//!
//! # GCS
//! sinks:
//!   gcs_events:
//!     connector: object-storage
//!     config:
//!       provider: gcs
//!       bucket: my-bucket
//!       prefix: events/
//!       service_account_path: /path/to/sa.json
//!
//! # Azure
//! sinks:
//!   azure_events:
//!     connector: object-storage
//!     config:
//!       provider: azure
//!       account: mystorageaccount
//!       container: mycontainer
//!       access_key: ${AZURE_ACCESS_KEY}
//! ```

use crate::connectors::storage::common::{
    default_batch_size, default_flush_interval_secs, event_to_json, generate_object_key,
    get_content_type, serialize_batch, StorageCompression, StorageFormat, StoragePartitioning,
};
use crate::connectors::{AnySink, SinkFactory};
use crate::error::{ConnectorError, Result};
use crate::prelude::*;
use crate::types::SensitiveString;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use validator::Validate;

// ============================================================================
// Provider Configuration
// ============================================================================

/// Cloud storage provider
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageProvider {
    /// Amazon S3 or S3-compatible (MinIO, R2, DigitalOcean Spaces)
    #[default]
    S3,
    /// Google Cloud Storage
    Gcs,
    /// Azure Blob Storage
    Azure,
    /// Local filesystem (for testing)
    Local,
}

/// S3-specific configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct S3Config {
    /// AWS region (e.g., "us-east-1")
    #[serde(default = "default_region")]
    pub region: String,

    /// Custom endpoint URL (for MinIO, R2, etc.)
    #[serde(default)]
    pub endpoint: Option<String>,

    /// AWS access key ID
    #[serde(default)]
    pub access_key_id: Option<SensitiveString>,

    /// AWS secret access key
    #[serde(default)]
    pub secret_access_key: Option<SensitiveString>,

    /// Use path-style URLs (required for MinIO)
    #[serde(default)]
    pub force_path_style: bool,

    /// Skip signature for public buckets
    #[serde(default)]
    pub anonymous: bool,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// GCS-specific configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct GcsConfig {
    /// Path to service account JSON key file
    #[serde(default)]
    pub service_account_path: Option<String>,

    /// Service account JSON key content (base64 encoded)
    #[serde(default)]
    pub service_account_key: Option<SensitiveString>,

    /// Use Application Default Credentials
    #[serde(default = "default_true")]
    pub use_adc: bool,
}

fn default_true() -> bool {
    true
}

/// Azure-specific configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct AzureConfig {
    /// Azure storage account name
    #[serde(default)]
    pub account: String,

    /// Storage account access key
    #[serde(default)]
    pub access_key: Option<SensitiveString>,

    /// SAS token (alternative to access key)
    #[serde(default)]
    pub sas_token: Option<SensitiveString>,

    /// Use managed identity (for Azure VMs/AKS)
    #[serde(default)]
    pub use_managed_identity: bool,
}

/// Local filesystem configuration (for testing)
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct LocalConfig {
    /// Root directory for local storage
    #[serde(default = "default_local_root")]
    pub root: String,
}

fn default_local_root() -> String {
    "/tmp/rivven-storage".to_string()
}

// ============================================================================
// Main Configuration
// ============================================================================

/// Configuration for the unified object storage sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct ObjectStorageSinkConfig {
    /// Storage provider (s3, gcs, azure, local)
    #[serde(default)]
    pub provider: StorageProvider,

    /// Bucket name (S3/GCS) or container name (Azure)
    #[validate(length(min = 1, max = 255))]
    pub bucket: String,

    /// Object key prefix (e.g., "events/cdc/")
    #[serde(default)]
    pub prefix: String,

    /// Output format
    #[serde(default)]
    pub format: StorageFormat,

    /// Compression
    #[serde(default)]
    pub compression: StorageCompression,

    /// Partitioning strategy
    #[serde(default)]
    pub partitioning: StoragePartitioning,

    /// Batch size (number of events before flushing)
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100000))]
    pub batch_size: usize,

    /// Flush interval in seconds
    #[serde(default = "default_flush_interval_secs")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// S3-specific configuration
    #[serde(default)]
    pub s3: S3Config,

    /// GCS-specific configuration
    #[serde(default)]
    pub gcs: GcsConfig,

    /// Azure-specific configuration
    #[serde(default)]
    pub azure: AzureConfig,

    /// Local filesystem configuration (for testing)
    #[serde(default)]
    pub local: LocalConfig,
}

impl Default for ObjectStorageSinkConfig {
    fn default() -> Self {
        Self {
            provider: StorageProvider::S3,
            bucket: String::new(),
            prefix: String::new(),
            format: StorageFormat::default(),
            compression: StorageCompression::default(),
            partitioning: StoragePartitioning::default(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval_secs(),
            s3: S3Config::default(),
            gcs: GcsConfig::default(),
            azure: AzureConfig::default(),
            local: LocalConfig::default(),
        }
    }
}

// ============================================================================
// Sink Implementation
// ============================================================================

/// Unified object storage sink
pub struct ObjectStorageSink;

impl ObjectStorageSink {
    pub fn new() -> Self {
        Self
    }

    /// Create an ObjectStore client based on provider configuration
    fn create_store(config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        match config.provider {
            StorageProvider::S3 => Self::create_s3_store(config),
            StorageProvider::Gcs => Self::create_gcs_store(config),
            StorageProvider::Azure => Self::create_azure_store(config),
            StorageProvider::Local => Self::create_local_store(config),
        }
    }

    #[cfg(feature = "s3")]
    fn create_s3_store(config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.s3.region);

        if let Some(endpoint) = &config.s3.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if let (Some(key), Some(secret)) = (&config.s3.access_key_id, &config.s3.secret_access_key)
        {
            builder = builder
                .with_access_key_id(key.expose_secret())
                .with_secret_access_key(secret.expose_secret());
        }

        if config.s3.force_path_style {
            builder = builder.with_virtual_hosted_style_request(false);
        }

        if config.s3.anonymous {
            builder = builder.with_skip_signature(true);
        }

        let store = builder
            .build()
            .map_err(|e| ConnectorError::Config(format!("Failed to create S3 client: {}", e)))?;

        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "s3"))]
    fn create_s3_store(_config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config(
            "S3 support not enabled. Rebuild with 's3' feature.".to_string(),
        )
        .into())
    }

    #[cfg(feature = "gcs")]
    fn create_gcs_store(config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::gcp::GoogleCloudStorageBuilder;

        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);

        if let Some(path) = &config.gcs.service_account_path {
            builder = builder.with_service_account_path(path);
        }

        if let Some(key) = &config.gcs.service_account_key {
            builder = builder.with_service_account_key(key.expose_secret());
        }

        let store = builder
            .build()
            .map_err(|e| ConnectorError::Config(format!("Failed to create GCS client: {}", e)))?;

        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "gcs"))]
    fn create_gcs_store(_config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config(
            "GCS support not enabled. Rebuild with 'gcs' feature.".to_string(),
        )
        .into())
    }

    #[cfg(feature = "azure")]
    fn create_azure_store(config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::azure::MicrosoftAzureBuilder;

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&config.azure.account)
            .with_container_name(&config.bucket);

        if let Some(key) = &config.azure.access_key {
            builder = builder.with_access_key(key.expose_secret());
        }

        if let Some(sas) = &config.azure.sas_token {
            // Parse SAS token query string into key-value pairs
            let sas_str = sas.expose_secret();
            let sas_str = sas_str.trim_start_matches('?');
            let pairs: Vec<(String, String)> = sas_str
                .split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                        _ => None,
                    }
                })
                .collect();
            builder = builder.with_sas_authorization(pairs);
        }

        if config.azure.use_managed_identity {
            builder = builder.with_use_azure_cli(true);
        }

        let store = builder.build().map_err(|e| {
            ConnectorError::Config(format!("Failed to create Azure Blob client: {}", e))
        })?;

        Ok(Arc::new(store))
    }

    #[cfg(not(feature = "azure"))]
    fn create_azure_store(_config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        Err(ConnectorError::Config(
            "Azure support not enabled. Rebuild with 'azure' feature.".to_string(),
        )
        .into())
    }

    fn create_local_store(config: &ObjectStorageSinkConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::local::LocalFileSystem;

        // Create root directory if it doesn't exist
        let root = std::path::Path::new(&config.local.root);
        std::fs::create_dir_all(root).map_err(|e| {
            ConnectorError::Config(format!("Failed to create local storage directory: {}", e))
        })?;

        let store = LocalFileSystem::new_with_prefix(root).map_err(|e| {
            ConnectorError::Config(format!("Failed to create local filesystem store: {}", e))
        })?;

        Ok(Arc::new(store))
    }
}

impl Default for ObjectStorageSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for ObjectStorageSink {
    type Config = ObjectStorageSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("object-storage", env!("CARGO_PKG_VERSION"))
            .description("Unified object storage sink - S3, GCS, Azure Blob, and local filesystem")
            .documentation_url("https://rivven.dev/docs/connectors/object-storage-sink")
            .config_schema_from::<ObjectStorageSinkConfig>()
            .metadata("providers", "s3,gcs,azure,local")
            .metadata("formats", "json,jsonl,avro,csv,parquet")
            .metadata("compression", "none,gzip")
            .metadata("partitioning", "none,day,hour")
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        info!(
            "Checking {:?} connectivity for bucket: {}",
            config.provider, config.bucket
        );

        let store = Self::create_store(config)?;

        // Try to list objects with a prefix to verify access
        let prefix = ObjectPath::from(config.prefix.trim_end_matches('/'));

        match store.list_with_delimiter(Some(&prefix)).await {
            Ok(_) => {
                info!(
                    "Successfully connected to {:?} bucket: {}",
                    config.provider, config.bucket
                );
                Ok(CheckResult::success())
            }
            Err(e) => {
                // Some errors (like empty prefix) are acceptable
                if e.to_string().contains("NotFound") {
                    info!(
                        "Bucket {} accessible (prefix may not exist yet)",
                        config.bucket
                    );
                    Ok(CheckResult::success())
                } else {
                    let msg = format!(
                        "Failed to access {:?} bucket '{}': {}",
                        config.provider, config.bucket, e
                    );
                    warn!("{}", msg);
                    Ok(CheckResult::failure(msg))
                }
            }
        }
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let store = Self::create_store(config)?;
        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut batch_id = 0u64;
        let mut last_flush = std::time::Instant::now();
        let flush_interval = std::time::Duration::from_secs(config.flush_interval_secs);

        info!(
            "Starting {:?} sink: bucket={}, prefix={}, format={:?}, batch_size={}",
            config.provider, config.bucket, config.prefix, config.format, config.batch_size
        );

        while let Some(event) = events.next().await {
            batch.push(event_to_json(&event));

            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                let timestamp = Utc::now();
                let key = generate_object_key(
                    &config.prefix,
                    &config.partitioning,
                    &config.format,
                    &config.compression,
                    timestamp,
                    batch_id,
                );
                let data = serialize_batch(&batch, &config.format, &config.compression)?;
                let content_type = get_content_type(&config.format, &config.compression);
                let path = ObjectPath::from(key.as_str());

                debug!(
                    "Uploading {} events to {:?}://{}/{}",
                    batch.len(),
                    config.provider,
                    config.bucket,
                    key
                );

                store
                    .put(&path, PutPayload::from(data))
                    .await
                    .map_err(|e| {
                        error!("Failed to upload to {:?}: {}", config.provider, e);
                        ConnectorError::Connection(format!(
                            "{:?} upload failed: {}",
                            config.provider, e
                        ))
                    })?;

                info!(
                    "Uploaded {} events to {:?}://{}/{} (content-type: {})",
                    batch.len(),
                    config.provider,
                    config.bucket,
                    key,
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
            let key = generate_object_key(
                &config.prefix,
                &config.partitioning,
                &config.format,
                &config.compression,
                timestamp,
                batch_id,
            );
            let data = serialize_batch(&batch, &config.format, &config.compression)?;
            let path = ObjectPath::from(key.as_str());

            store
                .put(&path, PutPayload::from(data))
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!(
                        "{:?} upload failed: {}",
                        config.provider, e
                    ))
                })?;

            info!(
                "Uploaded final {} events to {:?}://{}/{}",
                batch.len(),
                config.provider,
                config.bucket,
                key
            );

            total_written += batch.len() as u64;
        }

        info!(
            "{:?} sink completed: {} total events written",
            config.provider, total_written
        );

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: 0,
            records_failed: 0,
            errors: vec![],
        })
    }
}

/// Factory for creating object storage sink instances
pub struct ObjectStorageSinkFactory;

impl SinkFactory for ObjectStorageSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        ObjectStorageSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(ObjectStorageSink::new())
    }
}

// Implement AnySink for ObjectStorageSink
crate::impl_any_sink!(ObjectStorageSink, ObjectStorageSinkConfig);

/// Register the unified object storage sink
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("object-storage", Arc::new(ObjectStorageSinkFactory));
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = ObjectStorageSink::spec();
        assert_eq!(spec.connector_type, "object-storage");
        assert!(spec.config_schema.is_some());
    }

    #[test]
    fn test_default_config() {
        let config = ObjectStorageSinkConfig::default();
        assert_eq!(config.provider, StorageProvider::S3);
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_secs, 60);
        assert!(matches!(config.format, StorageFormat::Json));
        assert!(matches!(config.compression, StorageCompression::None));
    }

    #[test]
    fn test_s3_config() {
        let yaml = r#"
provider: s3
bucket: my-bucket
prefix: events/
format: jsonl
compression: gzip
partitioning: day
s3:
  region: eu-west-1
  endpoint: https://s3.eu-west-1.amazonaws.com
  force_path_style: false
"#;

        let config: ObjectStorageSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, StorageProvider::S3);
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "events/");
        assert_eq!(config.s3.region, "eu-west-1");
        assert!(!config.s3.force_path_style);
    }

    #[test]
    fn test_gcs_config() {
        let yaml = r#"
provider: gcs
bucket: my-gcs-bucket
prefix: data/
gcs:
  service_account_path: /path/to/sa.json
  use_adc: false
"#;

        let config: ObjectStorageSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, StorageProvider::Gcs);
        assert_eq!(config.bucket, "my-gcs-bucket");
        assert_eq!(
            config.gcs.service_account_path,
            Some("/path/to/sa.json".to_string())
        );
        assert!(!config.gcs.use_adc);
    }

    #[test]
    fn test_azure_config() {
        let yaml = r#"
provider: azure
bucket: my-container
azure:
  account: mystorageaccount
  use_managed_identity: true
"#;

        let config: ObjectStorageSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, StorageProvider::Azure);
        assert_eq!(config.bucket, "my-container");
        assert_eq!(config.azure.account, "mystorageaccount");
        assert!(config.azure.use_managed_identity);
    }

    #[test]
    fn test_local_config() {
        let yaml = r#"
provider: local
bucket: test-bucket
prefix: events/
local:
  root: /tmp/test-storage
"#;

        let config: ObjectStorageSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.provider, StorageProvider::Local);
        assert_eq!(config.local.root, "/tmp/test-storage");
    }

    #[test]
    fn test_factory() {
        let factory = ObjectStorageSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "object-storage");
        let _sink = factory.create();
    }

    #[tokio::test]
    async fn test_local_storage_check() {
        let config = ObjectStorageSinkConfig {
            provider: StorageProvider::Local,
            bucket: "test".to_string(),
            prefix: "events/".to_string(),
            local: LocalConfig {
                root: "/tmp/rivven-test-storage".to_string(),
            },
            ..Default::default()
        };

        let sink = ObjectStorageSink::new();
        let result = sink.check(&config).await;
        assert!(result.is_ok());
    }
}
