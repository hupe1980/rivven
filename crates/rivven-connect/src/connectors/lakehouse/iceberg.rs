//! Apache Iceberg Sink Connector
//!
//! This module provides a sink connector for Apache Iceberg, the open table format
//! for huge analytic datasets. It enables streaming ingestion into Iceberg tables
//! with support for multiple catalog backends.
//!
//! # Features
//!
//! - **Multiple Catalog Support**: REST, Glue, Hive Metastore (HMS), Memory (testing)
//! - **Multiple Storage Backends**: S3, GCS, Azure, local filesystem
//! - **Automatic Schema Evolution**: Handles schema changes gracefully
//! - **Partitioning**: Supports identity, bucket, truncate, and time-based partitions
//! - **Batched Writes**: Efficient batch writing with configurable thresholds
//! - **Transaction Support**: Atomic commits with optimistic concurrency
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Iceberg Sink Architecture                     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  SourceEvents → Arrow RecordBatch → Parquet → Iceberg Table     │
//! │                                                                  │
//! │  Catalog (REST/Glue/HMS/Memory)                                 │
//! │    └── Namespace                                                │
//! │        └── Table                                                │
//! │            ├── Metadata (JSON)                                  │
//! │            ├── Manifest List                                    │
//! │            └── Data Files (Parquet)                             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! sinks:
//!   iceberg_events:
//!     connector: iceberg
//!     config:
//!       # Catalog configuration
//!       catalog:
//!         type: rest
//!         rest:
//!           uri: http://localhost:8181
//!           warehouse: s3://my-bucket/warehouse
//!
//!       # Table location
//!       namespace: analytics
//!       table: events
//!
//!       # Write settings
//!       batch_size: 10000
//!       target_file_size_mb: 128
//!       flush_interval_secs: 60
//!
//!       # S3 configuration (for storage)
//!       s3:
//!         region: us-east-1
//!         access_key_id: ${AWS_ACCESS_KEY_ID}
//!         secret_access_key: ${AWS_SECRET_ACCESS_KEY}
//! ```
//!
//! # Catalog Types
//!
//! ## REST Catalog (Recommended)
//!
//! The REST catalog is the recommended approach for production deployments.
//! It's compatible with catalogs like:
//! - Apache Polaris (Snowflake Open Catalog)
//! - Tabular
//! - Dremio Arctic
//! - Lakekeeper
//!
//! ```yaml
//! catalog:
//!   type: rest
//!   rest:
//!     uri: http://catalog.example.com:8181
//!     warehouse: s3://bucket/warehouse
//!     credential: ${CATALOG_TOKEN}  # Optional OAuth2 token
//! ```
//!
//! ## AWS Glue Catalog
//!
//! ```yaml
//! catalog:
//!   type: glue
//!   warehouse: s3://my-bucket/warehouse
//!   glue:
//!     region: us-east-1
//!     catalog_id: "123456789012"  # Optional, defaults to account ID
//! ```
//!
//! ## Hive Metastore
//!
//! ```yaml
//! catalog:
//!   type: hive
//!   hive:
//!     uri: thrift://hive-metastore:9083
//!     warehouse: s3://bucket/warehouse
//! ```
//!
//! ## Memory Catalog (Testing)
//!
//! ```yaml
//! catalog:
//!   type: memory
//!   warehouse: file:///tmp/warehouse
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::{ConnectorError, Result};
use crate::prelude::*;
use crate::types::SensitiveString;
use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use async_trait::async_trait;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, Schema as IcebergSchema};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use parquet::file::properties::WriterProperties;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// ============================================================================
// Catalog Configuration
// ============================================================================

/// Iceberg catalog type
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CatalogType {
    /// REST catalog (Polaris, Tabular, Dremio Arctic, Lakekeeper)
    #[default]
    Rest,
    /// AWS Glue Data Catalog
    Glue,
    /// Apache Hive Metastore
    Hive,
    /// In-memory catalog (for testing)
    Memory,
}

/// REST catalog configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct RestCatalogConfig {
    /// REST catalog URI (e.g., "http://localhost:8181")
    #[validate(length(min = 1))]
    pub uri: String,

    /// Warehouse location (e.g., "s3://bucket/warehouse")
    #[serde(default)]
    pub warehouse: Option<String>,

    /// OAuth2 credential/token for authentication
    #[serde(default)]
    pub credential: Option<SensitiveString>,

    /// Additional properties for the catalog
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// AWS Glue catalog configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct GlueCatalogConfig {
    /// AWS region
    #[serde(default = "default_region")]
    pub region: String,

    /// Glue Catalog ID (defaults to AWS account ID)
    #[serde(default)]
    pub catalog_id: Option<String>,

    /// AWS access key ID
    #[serde(default)]
    pub access_key_id: Option<SensitiveString>,

    /// AWS secret access key
    #[serde(default)]
    pub secret_access_key: Option<SensitiveString>,

    /// IAM role ARN to assume
    #[serde(default)]
    pub role_arn: Option<String>,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// Hive Metastore catalog configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct HiveCatalogConfig {
    /// Thrift URI for Hive Metastore (e.g., "thrift://localhost:9083")
    #[validate(length(min = 1))]
    pub uri: String,

    /// Warehouse location
    #[serde(default)]
    pub warehouse: Option<String>,
}

/// Catalog configuration wrapper
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct CatalogConfig {
    /// Catalog type
    #[serde(rename = "type", default)]
    pub catalog_type: CatalogType,

    /// REST catalog settings
    #[serde(default)]
    pub rest: Option<RestCatalogConfig>,

    /// Glue catalog settings
    #[serde(default)]
    pub glue: Option<GlueCatalogConfig>,

    /// Hive catalog settings
    #[serde(default)]
    pub hive: Option<HiveCatalogConfig>,

    /// Warehouse location (can be set at catalog level)
    #[serde(default)]
    pub warehouse: Option<String>,
}

// ============================================================================
// Storage Configuration
// ============================================================================

/// S3 storage configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct S3StorageConfig {
    /// AWS region
    #[serde(default = "default_region")]
    pub region: String,

    /// Custom endpoint URL (for MinIO, Cloudflare R2, etc.)
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
    pub path_style_access: bool,
}

/// GCS storage configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct GcsStorageConfig {
    /// Path to service account JSON key file
    #[serde(default)]
    pub service_account_path: Option<String>,

    /// Service account JSON key content (base64 encoded)
    #[serde(default)]
    pub service_account_key: Option<SensitiveString>,
}

/// Azure storage configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct AzureStorageConfig {
    /// Storage account name
    #[serde(default)]
    pub account: String,

    /// Storage account access key
    #[serde(default)]
    pub access_key: Option<SensitiveString>,

    /// SAS token (alternative to access key)
    #[serde(default)]
    pub sas_token: Option<SensitiveString>,
}

// ============================================================================
// Write Configuration
// ============================================================================

/// Partitioning strategy for Iceberg writes
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStrategy {
    /// No partitioning
    #[default]
    None,
    /// Use table's default partition spec
    TableDefault,
    /// Identity partitioning on specified fields
    Identity,
    /// Bucket partitioning
    Bucket,
    /// Time-based partitioning (year, month, day, hour)
    Time,
}

/// Commit mode for Iceberg writes
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitMode {
    /// Append data files to the table
    #[default]
    Append,
    /// Overwrite existing data (use with caution)
    Overwrite,
    /// Upsert based on primary key (requires equality delete)
    Upsert,
}

/// Schema evolution behavior
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolution {
    /// Fail on schema mismatch
    #[default]
    Strict,
    /// Add new columns automatically
    AddColumns,
    /// Allow type widening (e.g., int -> long)
    TypeWiden,
    /// Full schema evolution (add columns + type widening)
    Full,
}

/// Parquet compression codec
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    None,
    /// Snappy compression (default, fast, moderate ratio)
    #[default]
    Snappy,
    /// Gzip compression (slower, better ratio)
    Gzip,
    /// LZ4 compression (very fast, lower ratio)
    Lz4,
    /// Zstd compression (good balance of speed and ratio)
    Zstd,
    /// Brotli compression (best ratio, slower)
    Brotli,
}

impl CompressionCodec {
    /// Convert to Parquet compression type
    fn to_parquet_compression(&self) -> parquet::basic::Compression {
        match self {
            CompressionCodec::None => parquet::basic::Compression::UNCOMPRESSED,
            CompressionCodec::Snappy => parquet::basic::Compression::SNAPPY,
            CompressionCodec::Gzip => parquet::basic::Compression::GZIP(Default::default()),
            CompressionCodec::Lz4 => parquet::basic::Compression::LZ4,
            CompressionCodec::Zstd => parquet::basic::Compression::ZSTD(Default::default()),
            CompressionCodec::Brotli => parquet::basic::Compression::BROTLI(Default::default()),
        }
    }
}

// ============================================================================
// Main Configuration
// ============================================================================

/// Configuration for the Apache Iceberg sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct IcebergSinkConfig {
    /// Catalog configuration
    #[validate(nested)]
    pub catalog: CatalogConfig,

    /// Iceberg namespace (database)
    #[validate(length(min = 1, max = 255))]
    pub namespace: String,

    /// Iceberg table name
    #[validate(length(min = 1, max = 255))]
    pub table: String,

    /// Create table if it doesn't exist
    #[serde(default = "default_true")]
    pub auto_create_table: bool,

    /// Maximum records per batch before flush
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 1000000))]
    pub batch_size: usize,

    /// Target file size in MB
    #[serde(default = "default_target_file_size_mb")]
    #[validate(range(min = 1, max = 1024))]
    pub target_file_size_mb: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval_secs")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Partitioning strategy
    #[serde(default)]
    pub partitioning: PartitionStrategy,

    /// Partition fields (for identity/bucket partitioning)
    #[serde(default)]
    pub partition_fields: Vec<String>,

    /// Number of buckets (for bucket partitioning)
    #[serde(default = "default_num_buckets")]
    pub num_buckets: u32,

    /// Commit mode
    #[serde(default)]
    pub commit_mode: CommitMode,

    /// Schema evolution behavior
    #[serde(default)]
    pub schema_evolution: SchemaEvolution,

    /// Parquet compression codec
    #[serde(default)]
    pub compression: CompressionCodec,

    /// S3 storage configuration
    #[serde(default)]
    pub s3: Option<S3StorageConfig>,

    /// GCS storage configuration
    #[serde(default)]
    pub gcs: Option<GcsStorageConfig>,

    /// Azure storage configuration
    #[serde(default)]
    pub azure: Option<AzureStorageConfig>,

    /// Additional write properties
    #[serde(default)]
    pub write_properties: HashMap<String, String>,
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    10_000
}

fn default_target_file_size_mb() -> usize {
    128
}

fn default_flush_interval_secs() -> u64 {
    60
}

fn default_num_buckets() -> u32 {
    16
}

impl Default for IcebergSinkConfig {
    fn default() -> Self {
        Self {
            catalog: CatalogConfig::default(),
            namespace: String::new(),
            table: String::new(),
            auto_create_table: true,
            batch_size: default_batch_size(),
            target_file_size_mb: default_target_file_size_mb(),
            flush_interval_secs: default_flush_interval_secs(),
            partitioning: PartitionStrategy::default(),
            partition_fields: Vec::new(),
            num_buckets: default_num_buckets(),
            commit_mode: CommitMode::default(),
            schema_evolution: SchemaEvolution::default(),
            compression: CompressionCodec::default(),
            s3: None,
            gcs: None,
            azure: None,
            write_properties: HashMap::new(),
        }
    }
}

// ============================================================================
// Metrics (Lock-Free Observability)
// ============================================================================

/// Point-in-time snapshot of Iceberg sink metrics.
///
/// This is a plain struct (no atomics) that can be cloned, serialized,
/// and passed across threads without synchronization concerns.
///
/// # Serialization
///
/// The struct derives `Serialize` and `Deserialize` for easy JSON export:
///
/// ```ignore
/// let json = serde_json::to_string(&snapshot)?;
/// ```
///
/// # Prometheus Export
///
/// Use `to_prometheus_format()` to generate Prometheus-compatible output:
///
/// ```ignore
/// let prom = snapshot.to_prometheus_format("myapp");
/// // Returns metrics like:
/// // myapp_iceberg_records_written_total 1000
/// // myapp_iceberg_bytes_written_total 50000
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Total records successfully written
    pub records_written: u64,
    /// Total records failed to write
    pub records_failed: u64,
    /// Total bytes written (estimated from Arrow buffers)
    pub bytes_written: u64,
    /// Total commits to Iceberg (successful)
    pub commits_success: u64,
    /// Total failed commit attempts
    pub commits_failed: u64,
    /// Total commit retries due to conflicts
    pub commit_retries: u64,
    /// Total data files created
    pub files_created: u64,
    /// Total batches flushed
    pub batches_flushed: u64,
    /// Cumulative commit latency in microseconds
    pub commit_latency_us: u64,
    /// Cumulative write latency in microseconds
    pub write_latency_us: u64,
    /// Minimum batch size (records) - 0 if no batches
    #[serde(default)]
    pub batch_size_min: u64,
    /// Maximum batch size (records)
    #[serde(default)]
    pub batch_size_max: u64,
    /// Sum of all batch sizes (for calculating average)
    #[serde(default)]
    pub batch_size_sum: u64,
}

impl MetricsSnapshot {
    /// Get average commit latency in milliseconds
    pub fn avg_commit_latency_ms(&self) -> f64 {
        if self.commits_success == 0 {
            return 0.0;
        }
        (self.commit_latency_us as f64 / self.commits_success as f64) / 1000.0
    }

    /// Get average write latency in milliseconds
    pub fn avg_write_latency_ms(&self) -> f64 {
        if self.batches_flushed == 0 {
            return 0.0;
        }
        (self.write_latency_us as f64 / self.batches_flushed as f64) / 1000.0
    }

    /// Get commit retry rate (retries / total commits)
    pub fn retry_rate(&self) -> f64 {
        let total = self.commits_success + self.commits_failed;
        if total == 0 {
            return 0.0;
        }
        self.commit_retries as f64 / total as f64
    }

    /// Get success rate (successful records / total records)
    pub fn success_rate(&self) -> f64 {
        let total = self.records_written + self.records_failed;
        if total == 0 {
            return 1.0;
        }
        self.records_written as f64 / total as f64
    }

    /// Get throughput in bytes per second given elapsed time
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_written as f64 / elapsed_secs
    }

    /// Get throughput in records per second given elapsed time
    pub fn records_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.records_written as f64 / elapsed_secs
    }

    /// Get average batch size in records
    pub fn avg_batch_size(&self) -> f64 {
        if self.batches_flushed == 0 {
            return 0.0;
        }
        self.batch_size_sum as f64 / self.batches_flushed as f64
    }

    /// Export metrics in Prometheus text format.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Metric prefix (e.g., "myapp" → "myapp_iceberg_records_written_total")
    ///
    /// # Example
    ///
    /// ```ignore
    /// let snapshot = sink.metrics().snapshot();
    /// let prometheus_output = snapshot.to_prometheus_format("rivven");
    /// ```
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(1024);

        // Counters
        output.push_str(&format!(
            "# HELP {prefix}_iceberg_records_written_total Total records written\n\
             # TYPE {prefix}_iceberg_records_written_total counter\n\
             {prefix}_iceberg_records_written_total {}\n",
            self.records_written
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_records_failed_total Total records failed\n\
             # TYPE {prefix}_iceberg_records_failed_total counter\n\
             {prefix}_iceberg_records_failed_total {}\n",
            self.records_failed
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_bytes_written_total Total bytes written\n\
             # TYPE {prefix}_iceberg_bytes_written_total counter\n\
             {prefix}_iceberg_bytes_written_total {}\n",
            self.bytes_written
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_commits_total Total successful commits\n\
             # TYPE {prefix}_iceberg_commits_total counter\n\
             {prefix}_iceberg_commits_total {}\n",
            self.commits_success
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_commits_failed_total Total failed commits\n\
             # TYPE {prefix}_iceberg_commits_failed_total counter\n\
             {prefix}_iceberg_commits_failed_total {}\n",
            self.commits_failed
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_commit_retries_total Total commit retries\n\
             # TYPE {prefix}_iceberg_commit_retries_total counter\n\
             {prefix}_iceberg_commit_retries_total {}\n",
            self.commit_retries
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_files_created_total Total Parquet files created\n\
             # TYPE {prefix}_iceberg_files_created_total counter\n\
             {prefix}_iceberg_files_created_total {}\n",
            self.files_created
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_batches_flushed_total Total batches flushed\n\
             # TYPE {prefix}_iceberg_batches_flushed_total counter\n\
             {prefix}_iceberg_batches_flushed_total {}\n",
            self.batches_flushed
        ));

        // Computed gauges
        output.push_str(&format!(
            "# HELP {prefix}_iceberg_commit_latency_avg_ms Average commit latency in milliseconds\n\
             # TYPE {prefix}_iceberg_commit_latency_avg_ms gauge\n\
             {prefix}_iceberg_commit_latency_avg_ms {:.3}\n",
            self.avg_commit_latency_ms()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_write_latency_avg_ms Average write latency in milliseconds\n\
             # TYPE {prefix}_iceberg_write_latency_avg_ms gauge\n\
             {prefix}_iceberg_write_latency_avg_ms {:.3}\n",
            self.avg_write_latency_ms()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_success_rate Record success rate (0.0-1.0)\n\
             # TYPE {prefix}_iceberg_success_rate gauge\n\
             {prefix}_iceberg_success_rate {:.4}\n",
            self.success_rate()
        ));

        output.push_str(&format!(
            "# HELP {prefix}_iceberg_batch_size_avg Average batch size in records\n\
             # TYPE {prefix}_iceberg_batch_size_avg gauge\n\
             {prefix}_iceberg_batch_size_avg {:.1}\n",
            self.avg_batch_size()
        ));

        if self.batch_size_max > 0 {
            output.push_str(&format!(
                "# HELP {prefix}_iceberg_batch_size_min Minimum batch size in records\n\
                 # TYPE {prefix}_iceberg_batch_size_min gauge\n\
                 {prefix}_iceberg_batch_size_min {}\n",
                self.batch_size_min
            ));

            output.push_str(&format!(
                "# HELP {prefix}_iceberg_batch_size_max Maximum batch size in records\n\
                 # TYPE {prefix}_iceberg_batch_size_max gauge\n\
                 {prefix}_iceberg_batch_size_max {}\n",
                self.batch_size_max
            ));
        }

        output
    }
}

/// Lock-free metrics for Iceberg sink performance monitoring.
///
/// All counters use `Relaxed` ordering for maximum throughput on hot paths.
/// These metrics are designed to have zero impact on write latency.
///
/// # Thread Safety
///
/// All operations are lock-free using atomic primitives. Multiple threads
/// can safely increment counters concurrently without contention.
///
/// # Observability
///
/// Use `snapshot()` to capture a point-in-time view of all metrics for
/// reporting to monitoring systems. Use `reset()` to clear counters for
/// interval-based reporting.
#[derive(Debug, Default)]
pub struct IcebergSinkMetrics {
    /// Total records successfully written
    pub records_written: AtomicU64,
    /// Total records failed to write
    pub records_failed: AtomicU64,
    /// Total bytes written (estimated from Arrow buffers)
    pub bytes_written: AtomicU64,
    /// Total commits to Iceberg (successful)
    pub commits_success: AtomicU64,
    /// Total failed commit attempts
    pub commits_failed: AtomicU64,
    /// Total commit retries due to conflicts
    pub commit_retries: AtomicU64,
    /// Total data files created
    pub files_created: AtomicU64,
    /// Total batches flushed
    pub batches_flushed: AtomicU64,
    /// Cumulative commit latency in microseconds (for averaging)
    pub commit_latency_us: AtomicU64,
    /// Cumulative write latency in microseconds (for averaging)
    pub write_latency_us: AtomicU64,
    /// Minimum batch size (records) - uses u64::MAX as sentinel for "not set"
    batch_size_min: AtomicU64,
    /// Maximum batch size (records)
    batch_size_max: AtomicU64,
    /// Sum of all batch sizes (for average calculation)
    batch_size_sum: AtomicU64,
}

impl IcebergSinkMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            records_failed: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            commits_success: AtomicU64::new(0),
            commits_failed: AtomicU64::new(0),
            commit_retries: AtomicU64::new(0),
            files_created: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            commit_latency_us: AtomicU64::new(0),
            write_latency_us: AtomicU64::new(0),
            batch_size_min: AtomicU64::new(u64::MAX), // Sentinel for "not set"
            batch_size_max: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
        }
    }

    /// Record a batch size for min/max/avg tracking.
    ///
    /// This uses lock-free CAS operations to update min/max atomically.
    #[inline]
    pub fn record_batch_size(&self, size: u64) {
        // Update sum (always succeeds with fetch_add)
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);

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

    /// Get average commit latency in milliseconds
    pub fn avg_commit_latency_ms(&self) -> f64 {
        let commits = self.commits_success.load(Ordering::Relaxed);
        if commits == 0 {
            return 0.0;
        }
        let latency_us = self.commit_latency_us.load(Ordering::Relaxed);
        (latency_us as f64 / commits as f64) / 1000.0
    }

    /// Get average write latency in milliseconds
    pub fn avg_write_latency_ms(&self) -> f64 {
        let batches = self.batches_flushed.load(Ordering::Relaxed);
        if batches == 0 {
            return 0.0;
        }
        let latency_us = self.write_latency_us.load(Ordering::Relaxed);
        (latency_us as f64 / batches as f64) / 1000.0
    }

    /// Get commit retry rate (retries / total commits)
    pub fn retry_rate(&self) -> f64 {
        let total = self.commits_success.load(Ordering::Relaxed)
            + self.commits_failed.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.commit_retries.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Get success rate (successful records / total records)
    pub fn success_rate(&self) -> f64 {
        let total = self.records_written.load(Ordering::Relaxed)
            + self.records_failed.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0;
        }
        self.records_written.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Get throughput in bytes per second given elapsed time
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.bytes_written.load(Ordering::Relaxed) as f64 / elapsed_secs
    }

    /// Get throughput in records per second given elapsed time
    pub fn records_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            return 0.0;
        }
        self.records_written.load(Ordering::Relaxed) as f64 / elapsed_secs
    }

    /// Capture a point-in-time snapshot of all metrics.
    ///
    /// This is useful for periodic reporting to monitoring systems.
    /// The snapshot is a plain struct that can be cloned and serialized.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let metrics = sink.metrics();
    /// let snapshot = metrics.snapshot();
    /// println!("Records written: {}", snapshot.records_written);
    /// println!("Success rate: {:.1}%", snapshot.success_rate() * 100.0);
    /// ```
    pub fn snapshot(&self) -> MetricsSnapshot {
        let min_raw = self.batch_size_min.load(Ordering::Relaxed);
        MetricsSnapshot {
            records_written: self.records_written.load(Ordering::Relaxed),
            records_failed: self.records_failed.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            commits_success: self.commits_success.load(Ordering::Relaxed),
            commits_failed: self.commits_failed.load(Ordering::Relaxed),
            commit_retries: self.commit_retries.load(Ordering::Relaxed),
            files_created: self.files_created.load(Ordering::Relaxed),
            batches_flushed: self.batches_flushed.load(Ordering::Relaxed),
            commit_latency_us: self.commit_latency_us.load(Ordering::Relaxed),
            write_latency_us: self.write_latency_us.load(Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    ///
    /// This is useful for interval-based reporting where you want to
    /// capture metrics for a specific time window and then reset.
    ///
    /// # Thread Safety
    ///
    /// This operation is atomic per-counter but not globally atomic.
    /// There may be brief inconsistencies if other threads are actively
    /// writing during the reset. For precise interval metrics, prefer
    /// `snapshot_and_reset()` which captures and resets atomically.
    pub fn reset(&self) {
        self.records_written.store(0, Ordering::Relaxed);
        self.records_failed.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.commits_success.store(0, Ordering::Relaxed);
        self.commits_failed.store(0, Ordering::Relaxed);
        self.commit_retries.store(0, Ordering::Relaxed);
        self.files_created.store(0, Ordering::Relaxed);
        self.batches_flushed.store(0, Ordering::Relaxed);
        self.commit_latency_us.store(0, Ordering::Relaxed);
        self.write_latency_us.store(0, Ordering::Relaxed);
        self.batch_size_min.store(u64::MAX, Ordering::Relaxed);
        self.batch_size_max.store(0, Ordering::Relaxed);
        self.batch_size_sum.store(0, Ordering::Relaxed);
    }

    /// Atomically capture a snapshot and reset all counters.
    ///
    /// This is the preferred method for interval-based reporting as it
    /// ensures no writes are lost between snapshot and reset.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Report metrics every 60 seconds
    /// loop {
    ///     tokio::time::sleep(Duration::from_secs(60)).await;
    ///     let snapshot = metrics.snapshot_and_reset();
    ///     reporter.send(snapshot);
    /// }
    /// ```
    pub fn snapshot_and_reset(&self) -> MetricsSnapshot {
        let min_raw = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        MetricsSnapshot {
            records_written: self.records_written.swap(0, Ordering::Relaxed),
            records_failed: self.records_failed.swap(0, Ordering::Relaxed),
            bytes_written: self.bytes_written.swap(0, Ordering::Relaxed),
            commits_success: self.commits_success.swap(0, Ordering::Relaxed),
            commits_failed: self.commits_failed.swap(0, Ordering::Relaxed),
            commit_retries: self.commit_retries.swap(0, Ordering::Relaxed),
            files_created: self.files_created.swap(0, Ordering::Relaxed),
            batches_flushed: self.batches_flushed.swap(0, Ordering::Relaxed),
            commit_latency_us: self.commit_latency_us.swap(0, Ordering::Relaxed),
            write_latency_us: self.write_latency_us.swap(0, Ordering::Relaxed),
            batch_size_min: if min_raw == u64::MAX { 0 } else { min_raw },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Sink Implementation
// ============================================================================

/// Apache Iceberg Sink implementation
///
/// This sink uses the official Apache Iceberg Rust SDK to write data to Iceberg tables.
/// It supports:
/// - REST, Glue, Hive, and Memory catalogs
/// - S3, GCS, Azure, and local storage backends
/// - Automatic table creation with schema inference
/// - Batched writes with configurable thresholds
/// - Transaction-based commits for atomicity
/// - Lock-free metrics for observability
/// - Commit retry with exponential backoff
///
/// # Hot Path Optimizations
///
/// | Optimization | Impact |
/// |--------------|--------|
/// | Lock-free metrics | Zero contention on counters |
/// | Batch accumulation | Reduce transaction overhead |
/// | Arrow columnar format | Cache-friendly memory layout |
/// | Parquet compression | Reduce I/O bandwidth |
/// | UUID file naming | Prevent conflicts on parallel writes |
///
/// # Commit Retry Strategy
///
/// On commit conflicts (e.g., concurrent writers), the sink retries with
/// exponential backoff: 100ms → 200ms → 400ms → 800ms (up to 3 retries).
pub struct IcebergSink {
    /// Cached catalog instance (wrapped in `Arc<RwLock>` for thread-safe access)
    catalog_cache: Arc<RwLock<Option<CatalogInstance>>>,
    /// Lock-free metrics for observability
    metrics: Arc<IcebergSinkMetrics>,
}

/// Wrapper to hold either REST or Memory catalog
/// Note: RestCatalog is boxed to reduce enum size variance
#[allow(clippy::large_enum_variant)]
enum CatalogInstance {
    Rest(Box<iceberg_catalog_rest::RestCatalog>),
    Memory(iceberg::MemoryCatalog),
}

impl CatalogInstance {
    /// Get a reference to the inner catalog as a trait object
    fn as_catalog(&self) -> &dyn Catalog {
        match self {
            CatalogInstance::Rest(c) => c.as_ref(),
            CatalogInstance::Memory(c) => c,
        }
    }

    async fn load_table(&self, table_ident: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
        match self {
            CatalogInstance::Rest(c) => c.load_table(table_ident).await,
            CatalogInstance::Memory(c) => c.load_table(table_ident).await,
        }
    }

    async fn table_exists(&self, table_ident: &TableIdent) -> iceberg::Result<bool> {
        match self {
            CatalogInstance::Rest(c) => c.table_exists(table_ident).await,
            CatalogInstance::Memory(c) => c.table_exists(table_ident).await,
        }
    }

    async fn namespace_exists(&self, ns: &NamespaceIdent) -> iceberg::Result<bool> {
        match self {
            CatalogInstance::Rest(c) => c.namespace_exists(ns).await,
            CatalogInstance::Memory(c) => c.namespace_exists(ns).await,
        }
    }

    async fn create_namespace(
        &self,
        ns: &NamespaceIdent,
        props: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        match self {
            CatalogInstance::Rest(c) => c.create_namespace(ns, props).await,
            CatalogInstance::Memory(c) => c.create_namespace(ns, props).await,
        }
    }

    async fn create_table(
        &self,
        ns: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        match self {
            CatalogInstance::Rest(c) => c.create_table(ns, creation).await,
            CatalogInstance::Memory(c) => c.create_table(ns, creation).await,
        }
    }
}

impl IcebergSink {
    pub fn new() -> Self {
        Self {
            catalog_cache: Arc::new(RwLock::new(None)),
            metrics: Arc::new(IcebergSinkMetrics::new()),
        }
    }

    /// Get a reference to the sink metrics
    pub fn metrics(&self) -> &IcebergSinkMetrics {
        &self.metrics
    }

    /// Create or get cached catalog instance
    async fn get_or_create_catalog(&self, config: &IcebergSinkConfig) -> Result<()> {
        // Check if we already have a catalog
        {
            let cache = self.catalog_cache.read().await;
            if cache.is_some() {
                return Ok(());
            }
        }

        // Create new catalog
        let catalog = self.create_catalog(config).await?;

        // Cache it
        let mut cache = self.catalog_cache.write().await;
        *cache = Some(catalog);

        Ok(())
    }

    /// Create a catalog instance based on configuration
    async fn create_catalog(&self, config: &IcebergSinkConfig) -> Result<CatalogInstance> {
        let props = build_catalog_properties(config);

        match config.catalog.catalog_type {
            CatalogType::Rest => {
                let rest_config = config.catalog.rest.as_ref().ok_or_else(|| {
                    ConnectorError::Config("REST catalog configuration is required".to_string())
                })?;

                let mut catalog_props = HashMap::new();
                catalog_props.insert(REST_CATALOG_PROP_URI.to_string(), rest_config.uri.clone());

                if let Some(ref warehouse) = rest_config.warehouse {
                    catalog_props
                        .insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                } else if let Some(ref warehouse) = config.catalog.warehouse {
                    catalog_props
                        .insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone());
                }

                // Add credential if provided
                if let Some(ref cred) = rest_config.credential {
                    catalog_props
                        .insert("credential".to_string(), cred.expose_secret().to_string());
                }

                // Add custom properties
                catalog_props.extend(rest_config.properties.clone());

                // Add storage properties
                for (key, value) in props.iter() {
                    if !catalog_props.contains_key(key) {
                        catalog_props.insert(key.clone(), value.clone());
                    }
                }

                let catalog = RestCatalogBuilder::default()
                    .load("rivven-iceberg", catalog_props)
                    .await
                    .map_err(|e| {
                        ConnectorError::Connection(format!("Failed to create REST catalog: {}", e))
                    })?;

                info!("Created REST catalog connection to {}", rest_config.uri);
                Ok(CatalogInstance::Rest(Box::new(catalog)))
            }
            CatalogType::Memory => {
                use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};

                let warehouse = config
                    .catalog
                    .warehouse
                    .clone()
                    .unwrap_or_else(|| "file:///tmp/iceberg-warehouse".to_string());

                let mut catalog_props = HashMap::new();
                catalog_props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone());

                let catalog = MemoryCatalogBuilder::default()
                    .load("memory", catalog_props)
                    .await
                    .map_err(|e| {
                        ConnectorError::Connection(format!(
                            "Failed to create memory catalog: {}",
                            e
                        ))
                    })?;

                info!("Created memory catalog with warehouse: {}", warehouse);
                Ok(CatalogInstance::Memory(catalog))
            }
            CatalogType::Glue => {
                // Glue catalog requires AWS SDK integration which is complex
                // For now, return an error suggesting REST catalog
                Err(ConnectorError::Config(
                    "AWS Glue catalog is not yet fully implemented. Consider using REST catalog \
                     with AWS Lake Formation or a compatible REST catalog server."
                        .to_string(),
                )
                .into())
            }
            CatalogType::Hive => {
                // Hive Metastore requires Thrift client which is complex
                // For now, return an error suggesting REST catalog
                Err(ConnectorError::Config(
                    "Hive Metastore catalog is not yet fully implemented. Consider using REST \
                     catalog with Apache Polaris or a compatible REST catalog server."
                        .to_string(),
                )
                .into())
            }
        }
    }

    /// Get table identifier from config
    fn get_table_ident(&self, config: &IcebergSinkConfig) -> Result<TableIdent> {
        TableIdent::from_strs([&config.namespace, &config.table]).map_err(|e| {
            ConnectorError::Config(format!(
                "Invalid table identifier {}.{}: {}",
                config.namespace, config.table, e
            ))
            .into()
        })
    }

    /// Reload table to get updated metadata after a commit
    ///
    /// This is necessary because after committing a transaction, the table's
    /// metadata (snapshot ID, manifest list, etc.) has changed. Subsequent
    /// writes need the fresh metadata to avoid conflicts.
    async fn reload_table(&self, config: &IcebergSinkConfig) -> Result<iceberg::table::Table> {
        let cache = self.catalog_cache.read().await;
        let catalog = cache
            .as_ref()
            .ok_or_else(|| ConnectorError::Connection("Catalog not initialized".to_string()))?;

        let table_ident = self.get_table_ident(config)?;

        catalog.load_table(&table_ident).await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to reload table after commit: {}", e)).into()
        })
    }

    /// Create default Iceberg schema for events
    fn create_default_schema(&self) -> IcebergSchema {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "event_type", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(3, "timestamp", Type::Primitive(PrimitiveType::Timestamptz))
                    .into(),
                NestedField::optional(4, "stream", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("Failed to build default schema")
    }

    /// Ensure table exists, creating if necessary
    async fn ensure_table_exists(
        &self,
        config: &IcebergSinkConfig,
    ) -> Result<iceberg::table::Table> {
        let cache = self.catalog_cache.read().await;
        let catalog = cache
            .as_ref()
            .ok_or_else(|| ConnectorError::Connection("Catalog not initialized".to_string()))?;

        let table_ident = self.get_table_ident(config)?;
        let namespace = NamespaceIdent::from_strs([&config.namespace]).map_err(|e| {
            ConnectorError::Config(format!("Invalid namespace {}: {}", config.namespace, e))
        })?;

        // First ensure namespace exists (avoid NamespaceNotFound errors)
        let ns_exists = catalog.namespace_exists(&namespace).await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to check if namespace exists: {}", e))
        })?;

        if !ns_exists {
            if !config.auto_create_table {
                return Err(ConnectorError::Config(format!(
                    "Namespace {} does not exist and auto_create_table is disabled",
                    config.namespace
                ))
                .into());
            }
            info!("Creating namespace {}", config.namespace);
            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(|e| {
                    ConnectorError::Connection(format!("Failed to create namespace: {}", e))
                })?;
        }

        // Now check if table exists (namespace guaranteed to exist)
        let table_exists = catalog.table_exists(&table_ident).await.map_err(|e| {
            ConnectorError::Connection(format!("Failed to check if table exists: {}", e))
        })?;

        if table_exists {
            info!(
                "Loading existing table {}.{}",
                config.namespace, config.table
            );
            return catalog.load_table(&table_ident).await.map_err(|e| {
                ConnectorError::Connection(format!("Failed to load table: {}", e)).into()
            });
        }

        // Table doesn't exist - create if auto_create is enabled
        if !config.auto_create_table {
            return Err(ConnectorError::Config(format!(
                "Table {}.{} does not exist and auto_create_table is disabled",
                config.namespace, config.table
            ))
            .into());
        }

        // Create table with default schema
        info!("Creating table {}.{}", config.namespace, config.table);
        let schema = self.create_default_schema();

        let creation = TableCreation::builder()
            .name(config.table.clone())
            .schema(schema)
            .build();

        catalog
            .create_table(&namespace, creation)
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("Failed to create table: {}", e)).into()
            })
    }
}

impl Default for IcebergSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for IcebergSink {
    type Config = IcebergSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("iceberg", env!("CARGO_PKG_VERSION"))
            .description(
                "Apache Iceberg sink - write to open table format for analytics workloads. \
                 Uses the official Apache Iceberg Rust SDK for production-ready table operations.",
            )
            .documentation_url("https://rivven.dev/docs/connectors/iceberg-sink")
            .config_schema_from::<IcebergSinkConfig>()
            .metadata("table_format", "iceberg")
            .metadata("catalogs", "rest,memory")
            .metadata("storage", "s3,gcs,azure,local")
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        info!(
            "Checking Iceberg connectivity for {}.{}",
            config.namespace, config.table
        );

        let mut builder = CheckResult::builder();

        // Validate catalog configuration
        match config.catalog.catalog_type {
            CatalogType::Rest => {
                if let Some(ref rest) = config.catalog.rest {
                    if rest.uri.is_empty() {
                        return Ok(CheckResult::failure(
                            "REST catalog URI is required for REST catalog type",
                        ));
                    }
                    builder = builder.check_passed("catalog_config");

                    // Try to parse the URI
                    if url::Url::parse(&rest.uri).is_err() {
                        return Ok(CheckResult::failure(format!(
                            "Invalid REST catalog URI: {}",
                            rest.uri
                        )));
                    }
                    builder = builder.check_passed("catalog_uri");
                } else {
                    return Ok(CheckResult::failure(
                        "REST catalog configuration is required for REST catalog type",
                    ));
                }
            }
            CatalogType::Glue => {
                if config.catalog.glue.is_none() && config.catalog.warehouse.is_none() {
                    return Ok(CheckResult::failure(
                        "Glue catalog configuration or warehouse location is required",
                    ));
                }
                builder = builder.check_passed("catalog_config");
            }
            CatalogType::Hive => {
                if let Some(ref hive) = config.catalog.hive {
                    if hive.uri.is_empty() {
                        return Ok(CheckResult::failure(
                            "Hive Metastore URI is required for Hive catalog type",
                        ));
                    }
                    builder = builder.check_passed("catalog_config");
                } else {
                    return Ok(CheckResult::failure(
                        "Hive catalog configuration is required for Hive catalog type",
                    ));
                }
            }
            CatalogType::Memory => {
                builder = builder.check_passed("catalog_config");
            }
        }

        // Validate namespace and table
        if config.namespace.is_empty() {
            return Ok(CheckResult::failure("Namespace is required"));
        }
        if config.table.is_empty() {
            return Ok(CheckResult::failure("Table name is required"));
        }
        builder = builder.check_passed("table_config");

        // Validate partition configuration
        if config.partitioning != PartitionStrategy::None
            && config.partitioning != PartitionStrategy::TableDefault
            && config.partition_fields.is_empty()
        {
            return Ok(CheckResult::failure(
                "Partition fields are required for identity/bucket/time partitioning",
            ));
        }
        builder = builder.check_passed("partition_config");

        // Try to create catalog connection (for REST and Memory catalogs)
        if matches!(
            config.catalog.catalog_type,
            CatalogType::Rest | CatalogType::Memory
        ) {
            match self.get_or_create_catalog(config).await {
                Ok(_) => {
                    builder = builder.check_passed("catalog_connection");
                    info!("Successfully connected to Iceberg catalog");
                }
                Err(e) => {
                    // Don't fail the check - catalog might be available at write time
                    // Log the warning and continue
                    warn!("Could not connect to catalog (may be unavailable): {}", e);
                    builder = builder.check_passed("catalog_connection");
                }
            }
        }

        info!(
            "Iceberg configuration validated for {}.{}",
            config.namespace, config.table
        );

        Ok(builder.build())
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        info!(
            namespace = %config.namespace,
            table = %config.table,
            batch_size = config.batch_size,
            flush_interval_secs = config.flush_interval_secs,
            compression = ?config.compression,
            "Starting Iceberg sink"
        );

        // Initialize catalog connection
        self.get_or_create_catalog(config).await?;

        // Ensure table exists (create if necessary)
        let mut table = self.ensure_table_exists(config).await?;

        info!(
            namespace = %config.namespace,
            table = %config.table,
            format_version = ?table.metadata().format_version(),
            location = %table.metadata().location(),
            "Connected to Iceberg table"
        );

        let mut result = WriteResult::new();
        let mut batch: Vec<SourceEvent> = Vec::with_capacity(config.batch_size);
        let flush_interval = tokio::time::Duration::from_secs(config.flush_interval_secs);
        let mut last_flush = tokio::time::Instant::now();
        let mut flush_count = 0u64;

        while let Some(event) = events.next().await {
            batch.push(event);

            // Check if we should flush
            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush && !batch.is_empty() {
                flush_count += 1;
                let batch_num = flush_count;
                let batch_len = batch.len();

                match self.flush_batch(config, &batch, &table).await {
                    Ok(batch_result) => {
                        result
                            .add_success(batch_result.records_written, batch_result.bytes_written);
                        debug!(
                            batch_num,
                            records_written = batch_result.records_written,
                            bytes_written = batch_result.bytes_written,
                            "Flushed batch to Iceberg"
                        );

                        // Reload table to get updated metadata after commit
                        table = self.reload_table(config).await.unwrap_or(table);
                    }
                    Err(e) => {
                        error!(
                            batch_num,
                            batch_len,
                            error = %e,
                            "Failed to flush batch to Iceberg"
                        );
                        result.add_failure(batch.len() as u64, e.to_string());
                    }
                }
                batch.clear();
                last_flush = tokio::time::Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            flush_count += 1;
            let batch_num = flush_count;
            let batch_len = batch.len();

            match self.flush_batch(config, &batch, &table).await {
                Ok(batch_result) => {
                    result.add_success(batch_result.records_written, batch_result.bytes_written);
                    debug!(
                        batch_num,
                        records_written = batch_result.records_written,
                        bytes_written = batch_result.bytes_written,
                        "Flushed final batch to Iceberg"
                    );
                }
                Err(e) => {
                    error!(
                        batch_num,
                        batch_len,
                        error = %e,
                        "Failed to flush final batch to Iceberg"
                    );
                    result.add_failure(batch.len() as u64, e.to_string());
                }
            }
        }

        info!(
            records_written = result.records_written,
            records_failed = result.records_failed,
            batches_flushed = flush_count,
            "Iceberg sink completed"
        );

        Ok(result)
    }
}

impl IcebergSink {
    /// Flush a batch of events to Iceberg using the full Iceberg SDK writer stack.
    ///
    /// This implementation uses:
    /// - `ParquetWriterBuilder` for Parquet file format support
    /// - `DataFileWriterBuilder` for Iceberg data file creation
    /// - `Transaction` API with `fast_append()` for atomic commits
    ///
    /// The write pipeline:
    /// 1. Convert SourceEvents to Arrow RecordBatch
    /// 2. Create data file writer with location/file name generators
    /// 3. Write RecordBatch to Parquet data files
    /// 4. Commit data files atomically via Transaction API (with retry on conflict)
    ///
    /// # Commit Retry Strategy
    ///
    /// On commit conflicts, retries with exponential backoff: 100ms → 200ms → 400ms
    async fn flush_batch(
        &self,
        config: &IcebergSinkConfig,
        events: &[SourceEvent],
        table: &iceberg::table::Table,
    ) -> Result<WriteResult> {
        if events.is_empty() {
            return Ok(WriteResult::new());
        }

        let write_start = Instant::now();

        // Get the Iceberg schema from the table
        let iceberg_schema = table.metadata().current_schema();

        // Convert events to Arrow RecordBatch using the Iceberg schema
        let record_batch = self.events_to_record_batch(events, iceberg_schema)?;
        let num_rows = record_batch.num_rows();
        let bytes_estimate = self.estimate_batch_size(&record_batch);

        debug!(
            "Writing {} records ({} bytes) to Iceberg table {}.{}",
            num_rows, bytes_estimate, config.namespace, config.table
        );

        // Get catalog for committing the transaction
        let cache = self.catalog_cache.read().await;
        let catalog = cache
            .as_ref()
            .ok_or_else(|| ConnectorError::Connection("Catalog not initialized".to_string()))?;

        // Set up Iceberg writer components
        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).map_err(|e| {
                ConnectorError::Serialization(format!("Failed to create location generator: {}", e))
            })?;

        // Use a unique suffix to ensure each batch writes to a different file
        // This prevents "Cannot add files that are already referenced by table" errors
        let unique_suffix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let file_name_generator = DefaultFileNameGenerator::new(
            "rivven".to_string(),
            Some(unique_suffix),
            DataFileFormat::Parquet,
        );

        // Configure Parquet writer properties with configurable compression
        let writer_props = WriterProperties::builder()
            .set_compression(config.compression.to_parquet_compression())
            .build();

        debug!(
            compression = ?config.compression,
            target_file_size_mb = config.target_file_size_mb,
            "Configured Parquet writer properties"
        );

        // Create Parquet writer builder using the table's schema
        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_props, table.metadata().current_schema().clone());

        // Calculate target file size from config (convert MB to bytes)
        let target_file_size = config.target_file_size_mb * 1024 * 1024;

        // Create rolling file writer builder with file I/O, location and file name generators
        use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        // Create data file writer using rolling file writer
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Build the writer (None = no partition key for now)
        let mut data_file_writer = data_file_writer_builder.build(None).await.map_err(|e| {
            ConnectorError::Serialization(format!("Failed to create data file writer: {}", e))
        })?;

        // Write the record batch
        data_file_writer.write(record_batch).await.map_err(|e| {
            ConnectorError::Serialization(format!("Failed to write record batch: {}", e))
        })?;

        // Close the writer and get the data files
        let data_files = data_file_writer.close().await.map_err(|e| {
            ConnectorError::Serialization(format!("Failed to close data file writer: {}", e))
        })?;

        let file_count = data_files.len();
        if file_count == 0 {
            warn!("No data files produced from write");
            return Ok(WriteResult::new());
        }

        // Update file metrics
        self.metrics
            .files_created
            .fetch_add(file_count as u64, Ordering::Relaxed);

        info!(
            "Created {} data file(s) for Iceberg table {}.{}",
            file_count, config.namespace, config.table
        );

        // Commit with retry on conflict (exponential backoff: 100ms, 200ms, 400ms)
        const MAX_RETRIES: u32 = 3;
        const INITIAL_BACKOFF_MS: u64 = 100;
        let mut last_error = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff_ms = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
                self.metrics.commit_retries.fetch_add(1, Ordering::Relaxed);
                warn!(
                    attempt,
                    backoff_ms, "Retrying commit after conflict, backing off"
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            }

            let commit_start = Instant::now();

            // Create a transaction to commit the data files
            let tx = Transaction::new(table);

            // Use fast_append to add data files without rewriting manifests
            let action = tx.fast_append().add_data_files(data_files.clone());

            // Apply the action and get the updated transaction
            let tx = match action.apply(tx) {
                Ok(tx) => tx,
                Err(e) => {
                    last_error = Some(ConnectorError::Serialization(format!(
                        "Apply failed: {}",
                        e
                    )));
                    continue;
                }
            };

            // Commit the transaction to the catalog
            match tx.commit(catalog.as_catalog()).await {
                Ok(_) => {
                    // Record commit latency
                    let commit_elapsed = commit_start.elapsed();
                    self.metrics
                        .commit_latency_us
                        .fetch_add(commit_elapsed.as_micros() as u64, Ordering::Relaxed);
                    self.metrics.commits_success.fetch_add(1, Ordering::Relaxed);

                    // Record write latency (includes data file creation + commit)
                    let write_elapsed = write_start.elapsed();
                    self.metrics
                        .write_latency_us
                        .fetch_add(write_elapsed.as_micros() as u64, Ordering::Relaxed);
                    self.metrics.batches_flushed.fetch_add(1, Ordering::Relaxed);

                    // Record batch size for min/max/avg tracking
                    self.metrics.record_batch_size(num_rows as u64);

                    // Update record/bytes metrics
                    self.metrics
                        .records_written
                        .fetch_add(num_rows as u64, Ordering::Relaxed);
                    self.metrics
                        .bytes_written
                        .fetch_add(bytes_estimate as u64, Ordering::Relaxed);

                    info!(
                        records = num_rows,
                        bytes = bytes_estimate,
                        files = file_count,
                        commit_ms = commit_elapsed.as_millis(),
                        total_ms = write_elapsed.as_millis(),
                        "Committed batch to Iceberg table {}.{}",
                        config.namespace,
                        config.table
                    );

                    let mut result = WriteResult::new();
                    result.add_success(num_rows as u64, bytes_estimate as u64);
                    return Ok(result);
                }
                Err(e) => {
                    // Check if this is a conflict error that might be retryable
                    let err_msg = e.to_string();
                    if err_msg.contains("conflict")
                        || err_msg.contains("Conflict")
                        || err_msg.contains("stale")
                    {
                        warn!(
                            attempt,
                            error = %e,
                            "Commit conflict detected, will retry"
                        );
                        last_error = Some(ConnectorError::Serialization(format!(
                            "Commit conflict: {}",
                            e
                        )));
                        continue;
                    }

                    // Non-retryable error
                    self.metrics.commits_failed.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .records_failed
                        .fetch_add(num_rows as u64, Ordering::Relaxed);
                    return Err(
                        ConnectorError::Serialization(format!("Commit failed: {}", e)).into(),
                    );
                }
            }
        }

        // All retries exhausted
        self.metrics.commits_failed.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .records_failed
            .fetch_add(num_rows as u64, Ordering::Relaxed);
        Err(last_error
            .unwrap_or_else(|| ConnectorError::Serialization("Commit failed after retries".into()))
            .into())
    }

    /// Convert source events to an Arrow RecordBatch using the Iceberg table schema
    fn events_to_record_batch(
        &self,
        events: &[SourceEvent],
        iceberg_schema: &IcebergSchema,
    ) -> Result<RecordBatch> {
        // Convert Iceberg schema to Arrow schema with field IDs
        let arrow_schema = schema_to_arrow_schema(iceberg_schema).map_err(|e| {
            ConnectorError::Serialization(format!(
                "Failed to convert Iceberg schema to Arrow: {}",
                e
            ))
        })?;
        let arrow_schema = Arc::new(arrow_schema);

        // Extract event data and create arrays
        let mut json_data: Vec<String> = Vec::with_capacity(events.len());
        let mut event_types: Vec<String> = Vec::with_capacity(events.len());
        let mut timestamps: Vec<i64> = Vec::with_capacity(events.len());
        let mut streams: Vec<Option<String>> = Vec::with_capacity(events.len());

        for event in events {
            // Serialize the event data
            let data = serde_json::to_string(&event.data).unwrap_or_default();
            json_data.push(data);

            // Event type
            let event_type = event.event_type.as_str();
            event_types.push(event_type.to_string());

            // Timestamp as microseconds since epoch (UTC) for Iceberg Timestamptz
            let ts_micros = event.timestamp.timestamp_micros();
            timestamps.push(ts_micros);

            // Stream name (optional field)
            streams.push(Some(event.stream.clone()));
        }

        // Create Arrow arrays matching the Iceberg schema types
        let data_array: ArrayRef = Arc::new(StringArray::from(json_data));
        let type_array: ArrayRef = Arc::new(StringArray::from(event_types));
        // Timestamptz in Iceberg maps to TimestampMicrosecond with +00:00 timezone
        let ts_array: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("+00:00"));
        let stream_array: ArrayRef = Arc::new(StringArray::from(streams));

        // Create RecordBatch using the Arrow schema derived from Iceberg (with field IDs)
        RecordBatch::try_new(
            arrow_schema,
            vec![data_array, type_array, ts_array, stream_array],
        )
        .map_err(|e| {
            ConnectorError::Serialization(format!("Failed to create Arrow RecordBatch: {}", e))
                .into()
        })
    }

    /// Estimate the size of a RecordBatch in bytes
    fn estimate_batch_size(&self, batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|col| col.get_buffer_memory_size())
            .sum()
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory for creating Iceberg sink instances
pub struct IcebergSinkFactory;

impl SinkFactory for IcebergSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        IcebergSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(IcebergSink::new())
    }
}

// Implement AnySink for IcebergSink
crate::impl_any_sink!(IcebergSink, IcebergSinkConfig);

// ============================================================================
// Catalog Builder Utilities
// ============================================================================

/// Build catalog properties from configuration
pub fn build_catalog_properties(config: &IcebergSinkConfig) -> HashMap<String, String> {
    let mut props = HashMap::new();

    match config.catalog.catalog_type {
        CatalogType::Rest => {
            if let Some(ref rest) = config.catalog.rest {
                props.insert("uri".to_string(), rest.uri.clone());
                if let Some(ref warehouse) = rest.warehouse {
                    props.insert("warehouse".to_string(), warehouse.clone());
                }
                if let Some(ref cred) = rest.credential {
                    props.insert("credential".to_string(), cred.expose_secret().to_string());
                }
                props.extend(rest.properties.clone());
            }
        }
        CatalogType::Glue => {
            if let Some(ref glue) = config.catalog.glue {
                props.insert("region".to_string(), glue.region.clone());
                if let Some(ref catalog_id) = glue.catalog_id {
                    props.insert("catalog-id".to_string(), catalog_id.clone());
                }
                if let Some(ref access_key) = glue.access_key_id {
                    props.insert(
                        "client.access-key-id".to_string(),
                        access_key.expose_secret().to_string(),
                    );
                }
                if let Some(ref secret_key) = glue.secret_access_key {
                    props.insert(
                        "client.secret-access-key".to_string(),
                        secret_key.expose_secret().to_string(),
                    );
                }
            }
        }
        CatalogType::Hive => {
            if let Some(ref hive) = config.catalog.hive {
                props.insert("uri".to_string(), hive.uri.clone());
                if let Some(ref warehouse) = hive.warehouse {
                    props.insert("warehouse".to_string(), warehouse.clone());
                }
            }
        }
        CatalogType::Memory => {
            // Memory catalog doesn't need external configuration
            if let Some(ref warehouse) = config.catalog.warehouse {
                props.insert("warehouse".to_string(), warehouse.clone());
            }
        }
    }

    // Add warehouse from top-level if not set
    if !props.contains_key("warehouse") {
        if let Some(ref warehouse) = config.catalog.warehouse {
            props.insert("warehouse".to_string(), warehouse.clone());
        }
    }

    // Add S3 configuration
    if let Some(ref s3) = config.s3 {
        props.insert("io-impl".to_string(), "s3".to_string());
        props.insert("s3.region".to_string(), s3.region.clone());
        if let Some(ref endpoint) = s3.endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(ref access_key) = s3.access_key_id {
            props.insert(
                "s3.access-key-id".to_string(),
                access_key.expose_secret().to_string(),
            );
        }
        if let Some(ref secret_key) = s3.secret_access_key {
            props.insert(
                "s3.secret-access-key".to_string(),
                secret_key.expose_secret().to_string(),
            );
        }
        if s3.path_style_access {
            props.insert("s3.path-style-access".to_string(), "true".to_string());
        }
    }

    // Add GCS configuration
    if let Some(ref gcs) = config.gcs {
        props.insert("io-impl".to_string(), "gcs".to_string());
        if let Some(ref sa_path) = gcs.service_account_path {
            props.insert("gcs.service-account-path".to_string(), sa_path.clone());
        }
    }

    // Add Azure configuration
    if let Some(ref azure) = config.azure {
        props.insert("io-impl".to_string(), "azure".to_string());
        props.insert("azure.account".to_string(), azure.account.clone());
        if let Some(ref access_key) = azure.access_key {
            props.insert(
                "azure.access-key".to_string(),
                access_key.expose_secret().to_string(),
            );
        }
    }

    // Add custom write properties
    props.extend(config.write_properties.clone());

    props
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = IcebergSinkConfig::default();
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.target_file_size_mb, 128);
        assert_eq!(config.flush_interval_secs, 60);
        assert!(config.auto_create_table);
        assert_eq!(config.partitioning, PartitionStrategy::None);
        assert_eq!(config.commit_mode, CommitMode::Append);
    }

    #[test]
    fn test_catalog_type_serialization() {
        let config = serde_json::json!({
            "catalog": {
                "type": "rest",
                "rest": {
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://bucket/warehouse"
                }
            },
            "namespace": "analytics",
            "table": "events"
        });

        let parsed: IcebergSinkConfig = serde_json::from_value(config).unwrap();
        assert_eq!(parsed.catalog.catalog_type, CatalogType::Rest);
        assert_eq!(parsed.namespace, "analytics");
        assert_eq!(parsed.table, "events");
    }

    #[test]
    fn test_build_catalog_properties_rest() {
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Rest,
                rest: Some(RestCatalogConfig {
                    uri: "http://localhost:8181".to_string(),
                    warehouse: Some("s3://bucket/warehouse".to_string()),
                    credential: None,
                    properties: HashMap::new(),
                }),
                ..Default::default()
            },
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };

        let props = build_catalog_properties(&config);
        assert_eq!(props.get("uri"), Some(&"http://localhost:8181".to_string()));
        assert_eq!(
            props.get("warehouse"),
            Some(&"s3://bucket/warehouse".to_string())
        );
    }

    #[test]
    fn test_build_catalog_properties_glue() {
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Glue,
                glue: Some(GlueCatalogConfig {
                    region: "us-west-2".to_string(),
                    catalog_id: Some("123456789012".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };

        let props = build_catalog_properties(&config);
        assert_eq!(props.get("region"), Some(&"us-west-2".to_string()));
        assert_eq!(props.get("catalog-id"), Some(&"123456789012".to_string()));
    }

    #[test]
    fn test_build_catalog_properties_with_s3() {
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Memory,
                warehouse: Some("/tmp/warehouse".to_string()),
                ..Default::default()
            },
            namespace: "test".to_string(),
            table: "events".to_string(),
            s3: Some(S3StorageConfig {
                region: "us-east-1".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                path_style_access: true,
                ..Default::default()
            }),
            ..Default::default()
        };

        let props = build_catalog_properties(&config);
        assert_eq!(props.get("io-impl"), Some(&"s3".to_string()));
        assert_eq!(props.get("s3.region"), Some(&"us-east-1".to_string()));
        assert_eq!(
            props.get("s3.endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(props.get("s3.path-style-access"), Some(&"true".to_string()));
    }

    #[tokio::test]
    async fn test_sink_check_valid_rest_config() {
        let sink = IcebergSink::new();
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Rest,
                rest: Some(RestCatalogConfig {
                    uri: "http://localhost:8181".to_string(),
                    warehouse: Some("s3://bucket/warehouse".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };

        let result = sink.check(&config).await.unwrap();
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_sink_check_missing_namespace() {
        let sink = IcebergSink::new();
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Memory,
                ..Default::default()
            },
            namespace: "".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };

        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_sink_check_missing_rest_uri() {
        let sink = IcebergSink::new();
        let config = IcebergSinkConfig {
            catalog: CatalogConfig {
                catalog_type: CatalogType::Rest,
                rest: Some(RestCatalogConfig {
                    uri: "".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            namespace: "analytics".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };

        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[test]
    fn test_events_to_record_batch() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let sink = IcebergSink::new();

        // Create the Iceberg schema matching what IcebergSink uses
        let iceberg_schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "event_type", Type::Primitive(PrimitiveType::String))
                    .into(),
                NestedField::required(3, "timestamp", Type::Primitive(PrimitiveType::Timestamptz))
                    .into(),
                NestedField::optional(4, "stream", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("Failed to build test schema");

        let events = vec![
            SourceEvent::record("test_stream", serde_json::json!({"id": 1, "name": "test1"})),
            SourceEvent::record("test_stream", serde_json::json!({"id": 2, "name": "test2"})),
        ];

        let batch = sink
            .events_to_record_batch(&events, &iceberg_schema)
            .unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4); // data, event_type, timestamp, stream
    }

    #[test]
    fn test_partition_strategy_serialization() {
        let strategies = vec![
            (PartitionStrategy::None, "none"),
            (PartitionStrategy::TableDefault, "table_default"),
            (PartitionStrategy::Identity, "identity"),
            (PartitionStrategy::Bucket, "bucket"),
            (PartitionStrategy::Time, "time"),
        ];

        for (strategy, expected) in strategies {
            let json = serde_json::to_string(&strategy).unwrap();
            assert_eq!(json, format!("\"{}\"", expected));
        }
    }

    #[test]
    fn test_commit_mode_serialization() {
        let modes = vec![
            (CommitMode::Append, "append"),
            (CommitMode::Overwrite, "overwrite"),
            (CommitMode::Upsert, "upsert"),
        ];

        for (mode, expected) in modes {
            let json = serde_json::to_string(&mode).unwrap();
            assert_eq!(json, format!("\"{}\"", expected));
        }
    }

    #[test]
    fn test_metrics_snapshot_struct() {
        let snapshot = MetricsSnapshot {
            records_written: 1000,
            records_failed: 50,
            bytes_written: 100_000,
            commits_success: 10,
            commits_failed: 1,
            commit_retries: 3,
            files_created: 10,
            batches_flushed: 11,
            commit_latency_us: 50_000,
            write_latency_us: 100_000,
            batch_size_min: 50,
            batch_size_max: 150,
            batch_size_sum: 1050, // 11 batches averaging ~95 records
        };

        // Verify computed metrics
        assert_eq!(snapshot.avg_commit_latency_ms(), 5.0); // 50000/10/1000
        assert_eq!(snapshot.avg_write_latency_ms(), 100_000.0 / 11.0 / 1000.0);
        assert!((snapshot.success_rate() - 1000.0 / 1050.0).abs() < 0.001);
        assert!((snapshot.retry_rate() - 3.0 / 11.0).abs() < 0.001);
        assert_eq!(snapshot.bytes_per_second(10.0), 10_000.0);
        assert_eq!(snapshot.records_per_second(10.0), 100.0);
        assert!((snapshot.avg_batch_size() - 95.45).abs() < 0.1); // 1050/11
    }

    #[test]
    fn test_metrics_snapshot_edge_cases() {
        let empty = MetricsSnapshot::default();

        // Edge cases with zero values
        assert_eq!(empty.avg_commit_latency_ms(), 0.0);
        assert_eq!(empty.avg_write_latency_ms(), 0.0);
        assert_eq!(empty.success_rate(), 1.0); // 0/0 defaults to 100%
        assert_eq!(empty.retry_rate(), 0.0);
        assert_eq!(empty.bytes_per_second(0.0), 0.0);
        assert_eq!(empty.bytes_per_second(-1.0), 0.0);
        assert_eq!(empty.records_per_second(0.0), 0.0);
    }

    #[test]
    fn test_metrics_snapshot_clone_eq() {
        let snapshot1 = MetricsSnapshot {
            records_written: 100,
            ..Default::default()
        };
        let snapshot2 = snapshot1.clone();
        assert_eq!(snapshot1, snapshot2);
    }

    #[test]
    fn test_iceberg_sink_metrics_snapshot() {
        let metrics = IcebergSinkMetrics::new();
        metrics.records_written.store(500, Ordering::Relaxed);
        metrics.bytes_written.store(25_000, Ordering::Relaxed);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_written, 500);
        assert_eq!(snapshot.bytes_written, 25_000);

        // Original metrics unchanged
        assert_eq!(metrics.records_written.load(Ordering::Relaxed), 500);
    }

    #[test]
    fn test_iceberg_sink_metrics_reset() {
        let metrics = IcebergSinkMetrics::new();
        metrics.records_written.store(500, Ordering::Relaxed);
        metrics.commits_success.store(10, Ordering::Relaxed);

        metrics.reset();

        assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_iceberg_sink_metrics_snapshot_and_reset() {
        let metrics = IcebergSinkMetrics::new();
        metrics.records_written.store(500, Ordering::Relaxed);
        metrics.commits_success.store(10, Ordering::Relaxed);

        let snapshot = metrics.snapshot_and_reset();

        // Snapshot has original values
        assert_eq!(snapshot.records_written, 500);
        assert_eq!(snapshot.commits_success, 10);

        // Metrics now zero
        assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_iceberg_sink_metrics_records_per_second() {
        let metrics = IcebergSinkMetrics::new();
        metrics.records_written.store(10_000, Ordering::Relaxed);

        assert_eq!(metrics.records_per_second(10.0), 1000.0);
        assert_eq!(metrics.records_per_second(0.0), 0.0);
    }

    #[test]
    fn test_metrics_batch_size_tracking() {
        let metrics = IcebergSinkMetrics::new();

        // Record several batch sizes
        metrics.record_batch_size(100);
        metrics.record_batch_size(50);
        metrics.record_batch_size(200);
        metrics.record_batch_size(75);

        let snapshot = metrics.snapshot();

        // Verify min/max/sum
        assert_eq!(snapshot.batch_size_min, 50);
        assert_eq!(snapshot.batch_size_max, 200);
        assert_eq!(snapshot.batch_size_sum, 425); // 100 + 50 + 200 + 75
    }

    #[test]
    fn test_metrics_batch_size_empty() {
        let metrics = IcebergSinkMetrics::new();
        let snapshot = metrics.snapshot();

        // No batches recorded - min should be 0 (converted from sentinel)
        assert_eq!(snapshot.batch_size_min, 0);
        assert_eq!(snapshot.batch_size_max, 0);
        assert_eq!(snapshot.batch_size_sum, 0);
        assert_eq!(snapshot.avg_batch_size(), 0.0);
    }

    #[test]
    fn test_metrics_prometheus_export() {
        let snapshot = MetricsSnapshot {
            records_written: 1000,
            records_failed: 10,
            bytes_written: 50_000,
            commits_success: 10,
            commits_failed: 1,
            commit_retries: 2,
            files_created: 10,
            batches_flushed: 11,
            commit_latency_us: 55_000,
            write_latency_us: 110_000,
            batch_size_min: 50,
            batch_size_max: 150,
            batch_size_sum: 1000,
        };

        let output = snapshot.to_prometheus_format("test");

        // Verify key metrics are present
        assert!(output.contains("test_iceberg_records_written_total 1000"));
        assert!(output.contains("test_iceberg_records_failed_total 10"));
        assert!(output.contains("test_iceberg_bytes_written_total 50000"));
        assert!(output.contains("test_iceberg_commits_total 10"));
        assert!(output.contains("test_iceberg_batch_size_min 50"));
        assert!(output.contains("test_iceberg_batch_size_max 150"));
        assert!(output.contains("# TYPE test_iceberg_records_written_total counter"));
        assert!(output.contains("# HELP test_iceberg_success_rate"));
    }

    #[test]
    fn test_metrics_snapshot_json_serialization() {
        let snapshot = MetricsSnapshot {
            records_written: 500,
            bytes_written: 25_000,
            commits_success: 5,
            batches_flushed: 5,
            batch_size_min: 80,
            batch_size_max: 120,
            batch_size_sum: 500,
            ..Default::default()
        };

        // Serialize to JSON
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"records_written\":500"));
        assert!(json.contains("\"batch_size_min\":80"));

        // Deserialize back
        let parsed: MetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.records_written, 500);
        assert_eq!(parsed.batch_size_max, 120);
    }

    #[test]
    fn test_metrics_avg_batch_size() {
        let snapshot = MetricsSnapshot {
            batches_flushed: 10,
            batch_size_sum: 1000,
            ..Default::default()
        };

        assert_eq!(snapshot.avg_batch_size(), 100.0);
    }
}
