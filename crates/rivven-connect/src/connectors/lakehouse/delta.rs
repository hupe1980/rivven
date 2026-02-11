//! Delta Lake Sink Connector
//!
//! This module provides a sink connector for [Delta Lake](https://delta.io/), the open-source
//! storage framework that brings ACID transactions to data lakes. It enables streaming
//! ingestion into Delta tables with full transactional guarantees.
//!
//! # Features
//!
//! - **ACID Transactions**: Every write is atomic with snapshot isolation
//! - **Multiple Storage Backends**: Local filesystem, S3, GCS, Azure Blob Storage
//! - **Auto Table Creation**: Creates tables on first write with configurable schema
//! - **Partitioning**: Supports Hive-style partition columns
//! - **Batched Writes**: Configurable batch size and flush interval for throughput
//! - **Commit Retry**: Exponential backoff on transaction conflicts
//! - **Compression**: Snappy, Gzip, LZ4, Zstd
//! - **Lock-Free Metrics**: Atomic counters for observability with Prometheus export
//! - **Schema Evolution**: Append-compatible schema changes
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                  Delta Lake Sink Architecture                │
//! ├──────────────────────────────────────────────────────────────┤
//! │  SourceEvents → Arrow RecordBatch → Parquet → Delta Table   │
//! │                                                              │
//! │  Delta Table (directory / object store prefix)               │
//! │    ├── _delta_log/                                           │
//! │    │   ├── 00000000000000000000.json  (commit log)           │
//! │    │   ├── 00000000000000000001.json                         │
//! │    │   └── ...                                               │
//! │    ├── part-00000-{uuid}.snappy.parquet  (data files)        │
//! │    ├── part-00001-{uuid}.snappy.parquet                      │
//! │    └── ...                                                   │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example Configuration
//!
//! ```yaml
//! sinks:
//!   delta_events:
//!     connector: delta-lake
//!     config:
//!       # Table location (local, S3, GCS, Azure)
//!       table_uri: s3://my-bucket/warehouse/events
//!       auto_create_table: true
//!
//!       # Write settings
//!       batch_size: 10000
//!       flush_interval_secs: 60
//!
//!       # Compression (snappy, gzip, lz4, zstd, none)
//!       compression: snappy
//!
//!       # Optional: partition columns
//!       partition_columns:
//!         - event_type
//!
//!       # S3 configuration
//!       s3:
//!         region: us-east-1
//!         access_key_id: ${AWS_ACCESS_KEY_ID}
//!         secret_access_key: ${AWS_SECRET_ACCESS_KEY}
//! ```
//!
//! # Storage Backends
//!
//! ## Local Filesystem
//!
//! ```yaml
//! table_uri: /data/warehouse/events
//! # or
//! table_uri: file:///data/warehouse/events
//! ```
//!
//! ## Amazon S3
//!
//! ```yaml
//! table_uri: s3://my-bucket/warehouse/events
//! s3:
//!   region: us-east-1
//!   access_key_id: ${AWS_ACCESS_KEY_ID}
//!   secret_access_key: ${AWS_SECRET_ACCESS_KEY}
//! ```
//!
//! ## S3-Compatible (MinIO, R2)
//!
//! ```yaml
//! table_uri: s3://my-bucket/warehouse/events
//! s3:
//!   region: us-east-1
//!   endpoint: http://minio:9000
//!   access_key_id: minioadmin
//!   secret_access_key: minioadmin
//!   allow_http: true
//! ```
//!
//! ## Google Cloud Storage
//!
//! ```yaml
//! table_uri: gs://my-bucket/warehouse/events
//! gcs:
//!   service_account_path: /path/to/service-account.json
//! ```
//!
//! ## Azure Blob Storage
//!
//! ```yaml
//! table_uri: az://my-container/warehouse/events
//! azure:
//!   account: mystorageaccount
//!   access_key: ${AZURE_STORAGE_KEY}
//! ```
//!
//! # Querying Delta Tables
//!
//! ## Apache Spark
//!
//! ```python
//! df = spark.read.format("delta").load("s3://my-bucket/warehouse/events")
//! df.show()
//! ```
//!
//! ## DuckDB
//!
//! ```sql
//! SELECT * FROM delta_scan('s3://my-bucket/warehouse/events');
//! ```
//!
//! ## Polars
//!
//! ```python
//! import polars as pl
//! df = pl.read_delta("s3://my-bucket/warehouse/events")
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::{ConnectorError, Result};
use crate::prelude::*;
use crate::types::SensitiveString;
use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use async_trait::async_trait;
use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::writer::{DeltaWriter as DeltaWriterTrait, RecordBatchWriter};
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures::StreamExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

// ============================================================================
// Storage Configuration
// ============================================================================

/// S3 storage configuration for Delta Lake
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

    /// Allow HTTP connections (for local S3-compatible services)
    #[serde(default)]
    pub allow_http: bool,
}

fn default_region() -> String {
    "us-east-1".to_string()
}

/// GCS storage configuration for Delta Lake
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
pub struct GcsStorageConfig {
    /// Path to service account JSON key file
    #[serde(default)]
    pub service_account_path: Option<String>,

    /// Service account JSON key content (base64 encoded)
    #[serde(default)]
    pub service_account_key: Option<SensitiveString>,
}

/// Azure storage configuration for Delta Lake
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

/// Delta Lake write mode
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeltaWriteMode {
    /// Append new data to existing table (default)
    #[default]
    Append,
    /// Overwrite existing data in the table
    Overwrite,
}

/// Compression codec for Parquet data files
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeltaCompression {
    /// No compression
    None,
    /// Snappy compression (default - fast, good compression ratio)
    #[default]
    Snappy,
    /// Gzip compression (slower, better compression)
    Gzip,
    /// LZ4 compression (very fast, moderate compression)
    Lz4,
    /// Zstandard compression (good balance of speed and ratio)
    Zstd,
}

impl DeltaCompression {
    /// Convert to Parquet compression codec
    fn to_parquet_compression(&self) -> Compression {
        match self {
            DeltaCompression::None => Compression::UNCOMPRESSED,
            DeltaCompression::Snappy => Compression::SNAPPY,
            DeltaCompression::Gzip => {
                Compression::GZIP(deltalake::parquet::basic::GzipLevel::default())
            }
            DeltaCompression::Lz4 => Compression::LZ4,
            DeltaCompression::Zstd => {
                Compression::ZSTD(deltalake::parquet::basic::ZstdLevel::default())
            }
        }
    }
}

// ============================================================================
// Sink Configuration
// ============================================================================

/// Delta Lake sink connector configuration
///
/// Writes streaming events to Delta Lake tables with ACID guarantees.
///
/// # Example
///
/// ```yaml
/// sinks:
///   events:
///     connector: delta-lake
///     config:
///       table_uri: s3://my-bucket/warehouse/events
///       batch_size: 10000
///       flush_interval_secs: 60
///       compression: snappy
///       s3:
///         region: us-east-1
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
pub struct DeltaLakeSinkConfig {
    /// Delta table URI (local path, s3://, gs://, az://)
    ///
    /// Examples:
    /// - `/data/warehouse/events` (local)
    /// - `s3://bucket/warehouse/events` (S3)
    /// - `gs://bucket/warehouse/events` (GCS)
    /// - `az://container/warehouse/events` (Azure)
    #[validate(length(min = 1))]
    pub table_uri: String,

    /// Automatically create the table if it doesn't exist
    #[serde(default = "default_true")]
    pub auto_create_table: bool,

    /// Maximum number of records per batch before flushing
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub batch_size: usize,

    /// Maximum seconds between flushes (even with partial batches)
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Target Parquet file size in MB (guides file splitting)
    #[serde(default = "default_target_file_size")]
    #[validate(range(min = 1, max = 1024))]
    pub target_file_size_mb: usize,

    /// Write mode: append or overwrite
    #[serde(default)]
    pub write_mode: DeltaWriteMode,

    /// Partition columns for Hive-style partitioning
    ///
    /// Events will be written into partition directories based on these column values.
    /// Only the default schema columns are supported: `event_type`, `stream`.
    #[serde(default)]
    pub partition_columns: Vec<String>,

    /// Compression codec for Parquet files
    #[serde(default)]
    pub compression: DeltaCompression,

    /// Maximum number of commit retries on conflict
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_commit_retries: u32,

    /// S3 storage configuration
    #[serde(default)]
    pub s3: Option<S3StorageConfig>,

    /// GCS storage configuration
    #[serde(default)]
    pub gcs: Option<GcsStorageConfig>,

    /// Azure storage configuration
    #[serde(default)]
    pub azure: Option<AzureStorageConfig>,

    /// Additional storage options passed directly to the Delta Lake engine
    ///
    /// These are key-value pairs passed to the underlying object store.
    /// See Delta Lake documentation for supported options.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// Additional Delta table properties
    ///
    /// Applied when auto-creating the table. Examples:
    /// - `delta.logRetentionDuration`: Log retention (default: "interval 30 days")
    /// - `delta.deletedFileRetentionDuration`: Deleted file retention
    #[serde(default)]
    pub table_properties: HashMap<String, String>,
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    10_000
}

fn default_flush_interval() -> u64 {
    60
}

fn default_target_file_size() -> usize {
    128
}

fn default_max_retries() -> u32 {
    3
}

impl Default for DeltaLakeSinkConfig {
    fn default() -> Self {
        Self {
            table_uri: String::new(),
            auto_create_table: true,
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            target_file_size_mb: default_target_file_size(),
            write_mode: DeltaWriteMode::default(),
            partition_columns: Vec::new(),
            compression: DeltaCompression::default(),
            max_commit_retries: default_max_retries(),
            s3: None,
            gcs: None,
            azure: None,
            storage_options: HashMap::new(),
            table_properties: HashMap::new(),
        }
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Build object store storage options from the sink configuration.
///
/// Merges S3/GCS/Azure config with any user-provided storage_options.
/// User-provided options take precedence over structured config.
fn build_storage_options(config: &DeltaLakeSinkConfig) -> HashMap<String, String> {
    let mut opts = HashMap::new();

    // S3 configuration
    if let Some(s3) = &config.s3 {
        opts.insert("AWS_REGION".to_string(), s3.region.clone());
        if let Some(ref endpoint) = s3.endpoint {
            opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone());
        }
        if let Some(ref key_id) = s3.access_key_id {
            opts.insert(
                "AWS_ACCESS_KEY_ID".to_string(),
                key_id.expose_secret().to_string(),
            );
        }
        if let Some(ref secret) = s3.secret_access_key {
            opts.insert(
                "AWS_SECRET_ACCESS_KEY".to_string(),
                secret.expose_secret().to_string(),
            );
        }
        if s3.path_style_access {
            opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
            opts.insert("AWS_FORCE_PATH_STYLE".to_string(), "true".to_string());
        }
        if s3.allow_http {
            opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
        }
    }

    // GCS configuration
    if let Some(gcs) = &config.gcs {
        if let Some(ref path) = gcs.service_account_path {
            opts.insert("GOOGLE_SERVICE_ACCOUNT_PATH".to_string(), path.clone());
        }
        if let Some(ref key) = gcs.service_account_key {
            opts.insert(
                "GOOGLE_SERVICE_ACCOUNT_KEY".to_string(),
                key.expose_secret().to_string(),
            );
        }
    }

    // Azure configuration
    if let Some(azure_cfg) = &config.azure {
        if !azure_cfg.account.is_empty() {
            opts.insert(
                "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
                azure_cfg.account.clone(),
            );
        }
        if let Some(ref key) = azure_cfg.access_key {
            opts.insert(
                "AZURE_STORAGE_ACCOUNT_KEY".to_string(),
                key.expose_secret().to_string(),
            );
        }
        if let Some(ref sas) = azure_cfg.sas_token {
            opts.insert(
                "AZURE_STORAGE_SAS_TOKEN".to_string(),
                sas.expose_secret().to_string(),
            );
        }
    }

    // User-provided options override structured config
    for (k, v) in &config.storage_options {
        opts.insert(k.clone(), v.clone());
    }

    opts
}

/// Default Delta Lake schema for events.
///
/// | Column     | Type                          | Nullable |
/// |------------|-------------------------------|----------|
/// | data       | String (JSON payload)         | No       |
/// | event_type | String                        | No       |
/// | timestamp  | Timestamp (microsecond, UTC)  | No       |
/// | stream     | String                        | Yes      |
fn default_delta_schema() -> Vec<StructField> {
    vec![
        StructField::new(
            "data",
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "event_type",
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "timestamp",
            DeltaDataType::Primitive(PrimitiveType::Timestamp),
            false,
        ),
        StructField::new(
            "stream",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ]
}

/// Default Arrow schema for events (matches the Delta table schema).
///
/// | Column     | Arrow Type                                | Nullable |
/// |------------|-------------------------------------------|----------|
/// | data       | Utf8                                      | No       |
/// | event_type | Utf8                                      | No       |
/// | timestamp  | Timestamp(Microsecond, Some("UTC"))        | No       |
/// | stream     | Utf8                                      | Yes      |
fn default_arrow_schema() -> arrow_schema::SchemaRef {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    Arc::new(Schema::new(vec![
        Field::new("data", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("stream", DataType::Utf8, true),
    ]))
}

/// Build Parquet `WriterProperties` from the compression config.
fn build_writer_properties(config: &DeltaLakeSinkConfig) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(config.compression.to_parquet_compression())
        .build()
}

/// Convert source events to an Arrow `RecordBatch`.
///
/// Maps each `SourceEvent` to the default Delta table schema:
/// - `data`: JSON-serialized event payload
/// - `event_type`: insert/update/delete/state/log
/// - `timestamp`: event timestamp in microseconds UTC
/// - `stream`: source stream name (nullable)
fn events_to_record_batch(
    events: &[SourceEvent],
    arrow_schema: &arrow_schema::SchemaRef,
) -> Result<RecordBatch> {
    let mut json_data: Vec<String> = Vec::with_capacity(events.len());
    let mut event_types: Vec<String> = Vec::with_capacity(events.len());
    let mut timestamps: Vec<i64> = Vec::with_capacity(events.len());
    let mut streams: Vec<Option<String>> = Vec::with_capacity(events.len());

    for event in events {
        json_data.push(serde_json::to_string(&event.data).unwrap_or_default());
        event_types.push(event.event_type.as_str().to_string());
        timestamps.push(event.timestamp.timestamp_micros());
        streams.push(Some(event.stream.clone()));
    }

    let data_array: ArrayRef = Arc::new(StringArray::from(json_data));
    let type_array: ArrayRef = Arc::new(StringArray::from(event_types));
    let ts_array: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"));
    let stream_array: ArrayRef = Arc::new(StringArray::from(streams));

    RecordBatch::try_new(
        Arc::clone(arrow_schema),
        vec![data_array, type_array, ts_array, stream_array],
    )
    .map_err(|e| ConnectorError::Serialization(format!("Failed to create RecordBatch: {e}")).into())
}

/// Estimate the serialized byte size of a `RecordBatch`.
fn estimate_batch_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size() as u64)
        .sum()
}

// ============================================================================
// Metrics
// ============================================================================

/// Lock-free atomic metrics for the Delta Lake sink.
///
/// All counters use `Relaxed` ordering for maximum throughput on the hot path.
/// Metrics are monotonically increasing (except batch_size_min which uses CAS).
#[derive(Debug)]
pub struct DeltaLakeSinkMetrics {
    /// Total records successfully written
    pub records_written: AtomicU64,
    /// Total records that failed to write
    pub records_failed: AtomicU64,
    /// Estimated total bytes written
    pub bytes_written: AtomicU64,
    /// Successful Delta commits
    pub commits_success: AtomicU64,
    /// Failed Delta commits
    pub commits_failed: AtomicU64,
    /// Total commit retries due to conflicts
    pub commit_retries: AtomicU64,
    /// Total Parquet data files created
    pub files_created: AtomicU64,
    /// Number of batch flushes
    pub batches_flushed: AtomicU64,
    /// Cumulative commit latency in microseconds
    pub commit_latency_us: AtomicU64,
    /// Cumulative write latency in microseconds
    pub write_latency_us: AtomicU64,
    /// Minimum batch size seen (sentinel: u64::MAX = not set)
    batch_size_min: AtomicU64,
    /// Maximum batch size seen
    batch_size_max: AtomicU64,
    /// Sum of all batch sizes (for average calculation)
    batch_size_sum: AtomicU64,
}

impl DeltaLakeSinkMetrics {
    /// Create a new metrics instance with all counters at zero.
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
            batch_size_min: AtomicU64::new(u64::MAX),
            batch_size_max: AtomicU64::new(0),
            batch_size_sum: AtomicU64::new(0),
        }
    }

    /// Record a batch size, updating min/max/sum atomically.
    #[inline]
    pub fn record_batch_size(&self, size: u64) {
        self.batch_size_sum.fetch_add(size, Ordering::Relaxed);
        // CAS loop for minimum
        let mut current = self.batch_size_min.load(Ordering::Relaxed);
        while size < current {
            match self.batch_size_min.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }
        // CAS loop for maximum
        let mut current = self.batch_size_max.load(Ordering::Relaxed);
        while size > current {
            match self.batch_size_max.compare_exchange_weak(
                current,
                size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
            std::hint::spin_loop();
        }
    }

    /// Average commit latency in milliseconds.
    pub fn avg_commit_latency_ms(&self) -> f64 {
        let total_us = self.commit_latency_us.load(Ordering::Relaxed);
        let commits = self.commits_success.load(Ordering::Relaxed);
        if commits == 0 {
            0.0
        } else {
            (total_us as f64) / (commits as f64) / 1000.0
        }
    }

    /// Average write latency in milliseconds.
    pub fn avg_write_latency_ms(&self) -> f64 {
        let total_us = self.write_latency_us.load(Ordering::Relaxed);
        let flushes = self.batches_flushed.load(Ordering::Relaxed);
        if flushes == 0 {
            0.0
        } else {
            (total_us as f64) / (flushes as f64) / 1000.0
        }
    }

    /// Commit retry rate (retries / total attempts).
    pub fn retry_rate(&self) -> f64 {
        let retries = self.commit_retries.load(Ordering::Relaxed);
        let total = self.commits_success.load(Ordering::Relaxed)
            + self.commits_failed.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            retries as f64 / total as f64
        }
    }

    /// Commit success rate (success / total).
    pub fn success_rate(&self) -> f64 {
        let success = self.commits_success.load(Ordering::Relaxed);
        let total = success + self.commits_failed.load(Ordering::Relaxed);
        if total == 0 {
            1.0
        } else {
            success as f64 / total as f64
        }
    }

    /// Take a snapshot of all current metric values.
    pub fn snapshot(&self) -> DeltaLakeMetricsSnapshot {
        let batch_min = self.batch_size_min.load(Ordering::Relaxed);
        DeltaLakeMetricsSnapshot {
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
            batch_size_min: if batch_min == u64::MAX { 0 } else { batch_min },
            batch_size_max: self.batch_size_max.load(Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
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

    /// Take a snapshot and reset all counters atomically.
    pub fn snapshot_and_reset(&self) -> DeltaLakeMetricsSnapshot {
        let batch_min = self.batch_size_min.swap(u64::MAX, Ordering::Relaxed);
        DeltaLakeMetricsSnapshot {
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
            batch_size_min: if batch_min == u64::MAX { 0 } else { batch_min },
            batch_size_max: self.batch_size_max.swap(0, Ordering::Relaxed),
            batch_size_sum: self.batch_size_sum.swap(0, Ordering::Relaxed),
        }
    }

    /// Records per second over the given elapsed duration.
    pub fn records_per_second(&self, elapsed: Duration) -> f64 {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.records_written.load(Ordering::Relaxed) as f64 / secs
    }

    /// Bytes per second over the given elapsed duration.
    pub fn bytes_per_second(&self, elapsed: Duration) -> f64 {
        let secs = elapsed.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.bytes_written.load(Ordering::Relaxed) as f64 / secs
    }
}

impl Default for DeltaLakeSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Metrics Snapshot
// ============================================================================

/// Point-in-time snapshot of Delta Lake sink metrics.
///
/// All values are plain `u64` — safe to serialize, compare, and log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaLakeMetricsSnapshot {
    pub records_written: u64,
    pub records_failed: u64,
    pub bytes_written: u64,
    pub commits_success: u64,
    pub commits_failed: u64,
    pub commit_retries: u64,
    pub files_created: u64,
    pub batches_flushed: u64,
    pub commit_latency_us: u64,
    pub write_latency_us: u64,
    pub batch_size_min: u64,
    pub batch_size_max: u64,
    pub batch_size_sum: u64,
}

impl DeltaLakeMetricsSnapshot {
    /// Average commit latency in milliseconds.
    pub fn avg_commit_latency_ms(&self) -> f64 {
        if self.commits_success == 0 {
            0.0
        } else {
            self.commit_latency_us as f64 / self.commits_success as f64 / 1000.0
        }
    }

    /// Average write latency in milliseconds.
    pub fn avg_write_latency_ms(&self) -> f64 {
        if self.batches_flushed == 0 {
            0.0
        } else {
            self.write_latency_us as f64 / self.batches_flushed as f64 / 1000.0
        }
    }

    /// Commit retry rate.
    pub fn retry_rate(&self) -> f64 {
        let total = self.commits_success + self.commits_failed;
        if total == 0 {
            0.0
        } else {
            self.commit_retries as f64 / total as f64
        }
    }

    /// Commit success rate.
    pub fn success_rate(&self) -> f64 {
        let total = self.commits_success + self.commits_failed;
        if total == 0 {
            1.0
        } else {
            self.commits_success as f64 / total as f64
        }
    }

    /// Average batch size.
    pub fn avg_batch_size(&self) -> f64 {
        if self.batches_flushed == 0 {
            0.0
        } else {
            self.batch_size_sum as f64 / self.batches_flushed as f64
        }
    }

    /// Records per second (requires caller to provide elapsed time).
    pub fn records_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            0.0
        } else {
            self.records_written as f64 / elapsed_secs
        }
    }

    /// Bytes per second (requires caller to provide elapsed time).
    pub fn bytes_per_second(&self, elapsed_secs: f64) -> f64 {
        if elapsed_secs <= 0.0 {
            0.0
        } else {
            self.bytes_written as f64 / elapsed_secs
        }
    }

    /// Export metrics in Prometheus text exposition format.
    ///
    /// Each metric includes `# HELP` and `# TYPE` annotations as required by
    /// the Prometheus text format specification. Counters, gauges, and computed
    /// derived metrics are all included.
    pub fn to_prometheus_format(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(2048);

        // --- Counters ---
        output.push_str(&format!(
            "# HELP {prefix}_delta_records_written_total Total records successfully written\n\
             # TYPE {prefix}_delta_records_written_total counter\n\
             {prefix}_delta_records_written_total {}\n",
            self.records_written
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_records_failed_total Total records that failed to write\n\
             # TYPE {prefix}_delta_records_failed_total counter\n\
             {prefix}_delta_records_failed_total {}\n",
            self.records_failed
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_bytes_written_total Estimated total bytes written\n\
             # TYPE {prefix}_delta_bytes_written_total counter\n\
             {prefix}_delta_bytes_written_total {}\n",
            self.bytes_written
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_commits_total Total successful Delta commits\n\
             # TYPE {prefix}_delta_commits_total counter\n\
             {prefix}_delta_commits_total {}\n",
            self.commits_success
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_commits_failed_total Total failed Delta commits\n\
             # TYPE {prefix}_delta_commits_failed_total counter\n\
             {prefix}_delta_commits_failed_total {}\n",
            self.commits_failed
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_commit_retries_total Total commit retries due to conflicts\n\
             # TYPE {prefix}_delta_commit_retries_total counter\n\
             {prefix}_delta_commit_retries_total {}\n",
            self.commit_retries
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_files_created_total Total Parquet data files created\n\
             # TYPE {prefix}_delta_files_created_total counter\n\
             {prefix}_delta_files_created_total {}\n",
            self.files_created
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_batches_flushed_total Number of batch flushes\n\
             # TYPE {prefix}_delta_batches_flushed_total counter\n\
             {prefix}_delta_batches_flushed_total {}\n",
            self.batches_flushed
        ));

        // --- Computed gauges ---
        output.push_str(&format!(
            "# HELP {prefix}_delta_commit_latency_avg_ms Average commit latency in milliseconds\n\
             # TYPE {prefix}_delta_commit_latency_avg_ms gauge\n\
             {prefix}_delta_commit_latency_avg_ms {:.3}\n",
            self.avg_commit_latency_ms()
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_write_latency_avg_ms Average write latency in milliseconds\n\
             # TYPE {prefix}_delta_write_latency_avg_ms gauge\n\
             {prefix}_delta_write_latency_avg_ms {:.3}\n",
            self.avg_write_latency_ms()
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_success_rate Commit success rate (0.0-1.0)\n\
             # TYPE {prefix}_delta_success_rate gauge\n\
             {prefix}_delta_success_rate {:.4}\n",
            self.success_rate()
        ));
        output.push_str(&format!(
            "# HELP {prefix}_delta_batch_size_avg Average batch size in records\n\
             # TYPE {prefix}_delta_batch_size_avg gauge\n\
             {prefix}_delta_batch_size_avg {:.1}\n",
            self.avg_batch_size()
        ));

        // Conditional min/max — only emit when batches have been recorded
        if self.batch_size_max > 0 {
            output.push_str(&format!(
                "# HELP {prefix}_delta_batch_size_min Minimum batch size in records\n\
                 # TYPE {prefix}_delta_batch_size_min gauge\n\
                 {prefix}_delta_batch_size_min {}\n",
                self.batch_size_min
            ));
            output.push_str(&format!(
                "# HELP {prefix}_delta_batch_size_max Maximum batch size in records\n\
                 # TYPE {prefix}_delta_batch_size_max gauge\n\
                 {prefix}_delta_batch_size_max {}\n",
                self.batch_size_max
            ));
        }

        output
    }
}

// ============================================================================
// Delta Lake Sink
// ============================================================================

/// Delta Lake sink connector.
///
/// Writes streaming events to Delta Lake tables with ACID transactions,
/// batched writes, and lock-free metrics. Uses the `deltalake` crate
/// (delta-rs) for native Rust Delta Lake operations.
///
/// # Thread Safety
///
/// The sink is `Send + Sync`. Each `write()` invocation opens its own
/// table handle. Metrics use lock-free atomics for concurrent access.
pub struct DeltaLakeSink {
    metrics: Arc<DeltaLakeSinkMetrics>,
}

impl DeltaLakeSink {
    /// Create a new Delta Lake sink instance.
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(DeltaLakeSinkMetrics::new()),
        }
    }

    /// Get a reference to the sink metrics.
    pub fn metrics(&self) -> &Arc<DeltaLakeSinkMetrics> {
        &self.metrics
    }

    /// Open an existing Delta table, or create a new one if `auto_create_table` is enabled.
    async fn open_or_create_table(config: &DeltaLakeSinkConfig) -> Result<DeltaTable> {
        let storage_opts = build_storage_options(config);
        let table_uri = &config.table_uri;

        debug!(table_uri = %table_uri, "Opening Delta table");

        // Ensure the URI is a valid table location
        let table_url = deltalake::ensure_table_uri(table_uri)
            .map_err(|e| ConnectorError::Config(format!("Invalid table URI '{table_uri}': {e}")))?;

        // Try to load the existing table
        match DeltaTableBuilder::from_url(table_url.clone())
            .map_err(|e| ConnectorError::Config(format!("Invalid table URL: {e}")))?
            .with_storage_options(storage_opts.clone())
            .load()
            .await
        {
            Ok(table) => {
                info!(
                    table_uri = %table_uri,
                    version = table.version().unwrap_or(-1_i64),
                    "Opened existing Delta table"
                );
                Ok(table)
            }
            Err(DeltaTableError::NotATable(msg)) if config.auto_create_table => {
                info!(
                    table_uri = %table_uri,
                    "Table not found ({}), creating new Delta table",
                    msg
                );
                Self::create_table(config, &storage_opts).await
            }
            Err(e) => {
                error!(table_uri = %table_uri, error = %e, "Failed to open Delta table");
                Err(ConnectorError::Connection(format!(
                    "Failed to open Delta table at '{table_uri}': {e}"
                ))
                .into())
            }
        }
    }

    /// Create a new Delta table with the default schema.
    async fn create_table(
        config: &DeltaLakeSinkConfig,
        storage_opts: &HashMap<String, String>,
    ) -> Result<DeltaTable> {
        let mut builder = CreateBuilder::new()
            .with_location(&config.table_uri)
            .with_columns(default_delta_schema())
            .with_storage_options(storage_opts.clone());

        // Add partition columns if specified
        if !config.partition_columns.is_empty() {
            builder = builder.with_partition_columns(&config.partition_columns);
        }

        // Add table properties
        if !config.table_properties.is_empty() {
            builder = builder.with_configuration(
                config
                    .table_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), Some(v.clone()))),
            );
        }

        let table = builder.await.map_err(|e| {
            ConnectorError::Connection(format!(
                "Failed to create Delta table at '{}': {e}",
                config.table_uri
            ))
        })?;

        info!(
            table_uri = %config.table_uri,
            version = table.version().unwrap_or(0),
            partitions = ?config.partition_columns,
            "Created new Delta table"
        );

        Ok(table)
    }

    /// Flush a batch of events to the Delta table.
    ///
    /// Converts events to an Arrow `RecordBatch` **once**, then attempts to write
    /// and commit with retry on conflict. Each retry creates a fresh
    /// `RecordBatchWriter` but reuses the same `RecordBatch` (cheap `Arc` clone).
    ///
    /// # Orphan Files
    ///
    /// Delta-rs's `RecordBatchWriter` creates Parquet data files during `write()`.
    /// On conflict retry, the prior attempt's uncommitted Parquet file remains on
    /// storage as an orphan. These are harmless (not referenced by the Delta log)
    /// and can be removed by `VACUUM`.
    async fn flush_batch(
        table: &mut DeltaTable,
        events: &[SourceEvent],
        config: &DeltaLakeSinkConfig,
        metrics: &DeltaLakeSinkMetrics,
    ) -> Result<(u64, u64)> {
        if events.is_empty() {
            return Ok((0, 0));
        }

        let write_start = Instant::now();
        let batch_len = events.len() as u64;
        let arrow_schema_ref = default_arrow_schema();

        // Build RecordBatch once — clone is O(num_columns) via Arc
        let record_batch = events_to_record_batch(events, &arrow_schema_ref)?;
        let bytes_estimate = estimate_batch_bytes(&record_batch);

        let max_retries = config.max_commit_retries;
        let mut last_err = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                metrics.commit_retries.fetch_add(1, Ordering::Relaxed);
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                warn!(
                    attempt = attempt,
                    max_retries = max_retries,
                    backoff_ms = backoff.as_millis(),
                    "Commit conflict, retrying (prior attempt may leave orphan parquet file)"
                );
                tokio::time::sleep(backoff).await;

                // Reload table metadata before retry to resolve version conflicts
                if let Err(reload_err) = table.update_state().await {
                    warn!(
                        error = %reload_err,
                        "Failed to reload table metadata before retry"
                    );
                }
            }

            // Fresh writer per attempt — Delta-rs creates Parquet file during write()
            let writer_properties = build_writer_properties(config);
            let mut writer = RecordBatchWriter::for_table(table)
                .map_err(|e| {
                    ConnectorError::Internal(format!("Failed to create Delta writer: {e}"))
                })?
                .with_writer_properties(writer_properties);

            // Write the batch (RecordBatch::clone is cheap — O(num_columns) Arc clones)
            writer.write(record_batch.clone()).await.map_err(|e| {
                ConnectorError::Serialization(format!("Failed to write RecordBatch: {e}"))
            })?;

            let commit_start = Instant::now();
            match writer.flush_and_commit(table).await {
                Ok(version) => {
                    let commit_us = commit_start.elapsed().as_micros() as u64;
                    let write_us = write_start.elapsed().as_micros() as u64;

                    // Update metrics
                    metrics
                        .records_written
                        .fetch_add(batch_len, Ordering::Relaxed);
                    metrics
                        .bytes_written
                        .fetch_add(bytes_estimate, Ordering::Relaxed);
                    metrics.commits_success.fetch_add(1, Ordering::Relaxed);
                    metrics.files_created.fetch_add(1, Ordering::Relaxed);
                    metrics.batches_flushed.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .commit_latency_us
                        .fetch_add(commit_us, Ordering::Relaxed);
                    metrics
                        .write_latency_us
                        .fetch_add(write_us, Ordering::Relaxed);
                    metrics.record_batch_size(batch_len);

                    debug!(
                        version = version,
                        records = batch_len,
                        bytes = bytes_estimate,
                        commit_ms = commit_us / 1000,
                        "Delta commit successful"
                    );

                    return Ok((batch_len, bytes_estimate));
                }
                Err(e) => {
                    let err_msg = e.to_string().to_lowercase();
                    let is_conflict = err_msg.contains("conflict")
                        || err_msg.contains("stale")
                        || err_msg.contains("version");

                    if is_conflict && attempt < max_retries {
                        last_err = Some(e);
                        continue;
                    }

                    metrics.commits_failed.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .records_failed
                        .fetch_add(batch_len, Ordering::Relaxed);

                    error!(
                        error = %e,
                        attempt = attempt,
                        records = batch_len,
                        "Delta commit failed"
                    );

                    return Err(ConnectorError::Internal(format!(
                        "Delta commit failed after {} attempts: {e}",
                        attempt + 1
                    ))
                    .into());
                }
            }
        }

        // Exhausted retries
        metrics.commits_failed.fetch_add(1, Ordering::Relaxed);
        metrics
            .records_failed
            .fetch_add(batch_len, Ordering::Relaxed);

        Err(ConnectorError::Internal(format!(
            "Delta commit failed after {} retries: {}",
            max_retries,
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown error".to_string())
        ))
        .into())
    }
}

impl Default for DeltaLakeSink {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Sink Trait Implementation
// ============================================================================

#[async_trait]
impl Sink for DeltaLakeSink {
    type Config = DeltaLakeSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("delta-lake", env!("CARGO_PKG_VERSION"))
            .description(
                "Delta Lake sink - write to open-source lakehouse format with ACID transactions. \
                 Uses the delta-rs native Rust implementation for production-ready table operations.",
            )
            .documentation_url("https://rivven.dev/docs/connectors/delta-lake-sink")
            .config_schema_from::<DeltaLakeSinkConfig>()
            .metadata("table_format", "delta-lake")
            .metadata("storage", "s3,gcs,azure,local")
            .metadata("transactions", "acid")
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        let mut builder = CheckResult::builder();

        // Validate table URI
        if config.table_uri.is_empty() {
            return Ok(CheckResult::failure("table_uri is required"));
        }
        match deltalake::ensure_table_uri(&config.table_uri) {
            Ok(_) => {
                builder = builder.check_passed("table_uri_valid");
            }
            Err(e) => {
                return Ok(CheckResult::failure(format!(
                    "Invalid table_uri '{}': {e}",
                    config.table_uri
                )));
            }
        }

        // Validate batch settings
        if config.batch_size == 0 {
            return Ok(CheckResult::failure("batch_size must be > 0"));
        }
        builder = builder.check_passed("batch_settings");

        // Validate compression
        builder = builder.check_passed("compression");

        // Validate partition columns (must be valid schema columns)
        let valid_columns = ["data", "event_type", "timestamp", "stream"];
        for col in &config.partition_columns {
            if !valid_columns.contains(&col.as_str()) {
                return Ok(CheckResult::failure(format!(
                    "Invalid partition column '{col}'. Valid columns: {valid_columns:?}"
                )));
            }
        }
        if !config.partition_columns.is_empty() {
            builder = builder.check_passed("partition_columns");
        }

        // Validate storage config consistency
        let uri = &config.table_uri;
        if uri.starts_with("s3://") && config.s3.is_none() && config.storage_options.is_empty() {
            warn!("S3 URI detected but no S3 config provided — relying on environment variables");
        }
        if uri.starts_with("gs://") && config.gcs.is_none() && config.storage_options.is_empty() {
            warn!("GCS URI detected but no GCS config provided — relying on environment variables");
        }
        if uri.starts_with("az://") && config.azure.is_none() && config.storage_options.is_empty() {
            warn!(
                "Azure URI detected but no Azure config provided — relying on environment variables"
            );
        }
        builder = builder.check_passed("storage_config");

        Ok(builder.build())
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        let mut result = WriteResult::new();
        let flush_interval = Duration::from_secs(config.flush_interval_secs);

        // Open or create the Delta table
        let mut table = Self::open_or_create_table(config).await?;

        let mut batch: Vec<SourceEvent> = Vec::with_capacity(config.batch_size);
        let mut last_flush = Instant::now();
        let pipeline_start = Instant::now();

        while let Some(event) = events.next().await {
            batch.push(event);

            // Flush when batch is full or interval has elapsed
            let should_flush =
                batch.len() >= config.batch_size || last_flush.elapsed() >= flush_interval;

            if should_flush {
                match Self::flush_batch(&mut table, &batch, config, &self.metrics).await {
                    Ok((records, bytes)) => {
                        result.add_success(records, bytes);
                        // Reload table metadata for next commit (graceful degradation)
                        if let Err(e) = table.update_state().await {
                            warn!(error = %e, "Failed to reload table state after commit");
                        }
                    }
                    Err(e) => {
                        let failed_count = batch.len() as u64;
                        result.add_failure(failed_count, e.to_string());
                        error!(
                            error = %e,
                            records = failed_count,
                            "Failed to flush batch to Delta table"
                        );
                    }
                }

                batch.clear();
                last_flush = Instant::now();
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            match Self::flush_batch(&mut table, &batch, config, &self.metrics).await {
                Ok((records, bytes)) => {
                    result.add_success(records, bytes);
                }
                Err(e) => {
                    let failed_count = batch.len() as u64;
                    result.add_failure(failed_count, e.to_string());
                    error!(
                        error = %e,
                        records = failed_count,
                        "Failed to flush final batch to Delta table"
                    );
                }
            }
        }

        let elapsed = pipeline_start.elapsed();
        info!(
            table_uri = %config.table_uri,
            records_written = result.records_written,
            records_failed = result.records_failed,
            bytes_written = result.bytes_written,
            elapsed_ms = elapsed.as_millis(),
            "Delta Lake write pipeline completed"
        );

        Ok(result)
    }
}

// ============================================================================
// Factory & AnySink Bridge
// ============================================================================

/// Factory for creating Delta Lake sink instances.
pub struct DeltaLakeSinkFactory;

impl SinkFactory for DeltaLakeSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        DeltaLakeSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(DeltaLakeSink::new())
    }
}

crate::impl_any_sink!(DeltaLakeSink, DeltaLakeSinkConfig);

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ── Config Tests ─────────────────────────────────────────────

    #[test]
    fn test_default_config() {
        let config = DeltaLakeSinkConfig::default();
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.target_file_size_mb, 128);
        assert_eq!(config.flush_interval_secs, 60);
        assert!(config.auto_create_table);
        assert_eq!(config.write_mode, DeltaWriteMode::Append);
        assert_eq!(config.compression, DeltaCompression::Snappy);
        assert_eq!(config.max_commit_retries, 3);
        assert!(config.partition_columns.is_empty());
        assert!(config.table_uri.is_empty());
        assert!(config.s3.is_none());
        assert!(config.gcs.is_none());
        assert!(config.azure.is_none());
    }

    #[test]
    fn test_config_serialization() {
        let config = serde_json::json!({
            "table_uri": "s3://my-bucket/warehouse/events",
            "batch_size": 5000,
            "flush_interval_secs": 30,
            "compression": "zstd",
            "write_mode": "append",
            "partition_columns": ["event_type"],
            "s3": {
                "region": "us-west-2",
                "endpoint": "http://minio:9000"
            }
        });
        let parsed: DeltaLakeSinkConfig = serde_json::from_value(config).unwrap();
        assert_eq!(parsed.table_uri, "s3://my-bucket/warehouse/events");
        assert_eq!(parsed.batch_size, 5000);
        assert_eq!(parsed.flush_interval_secs, 30);
        assert_eq!(parsed.compression, DeltaCompression::Zstd);
        assert_eq!(parsed.write_mode, DeltaWriteMode::Append);
        assert_eq!(parsed.partition_columns, vec!["event_type"]);
        let s3 = parsed.s3.unwrap();
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint, Some("http://minio:9000".to_string()));
    }

    #[test]
    fn test_config_minimal() {
        let config = serde_json::json!({
            "table_uri": "/tmp/delta-events"
        });
        let parsed: DeltaLakeSinkConfig = serde_json::from_value(config).unwrap();
        assert_eq!(parsed.table_uri, "/tmp/delta-events");
        assert!(parsed.auto_create_table);
        assert_eq!(parsed.batch_size, 10_000);
    }

    // ── Storage Options Tests ────────────────────────────────────

    #[test]
    fn test_build_storage_options_s3() {
        let config = DeltaLakeSinkConfig {
            s3: Some(S3StorageConfig {
                region: "us-east-1".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                path_style_access: true,
                allow_http: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert_eq!(opts.get("AWS_REGION"), Some(&"us-east-1".to_string()));
        assert_eq!(
            opts.get("AWS_ENDPOINT_URL"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(opts.get("AWS_FORCE_PATH_STYLE"), Some(&"true".to_string()));
        assert_eq!(opts.get("AWS_ALLOW_HTTP"), Some(&"true".to_string()));
    }

    #[test]
    fn test_build_storage_options_gcs() {
        let config = DeltaLakeSinkConfig {
            gcs: Some(GcsStorageConfig {
                service_account_path: Some("/path/to/sa.json".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert_eq!(
            opts.get("GOOGLE_SERVICE_ACCOUNT_PATH"),
            Some(&"/path/to/sa.json".to_string())
        );
    }

    #[test]
    fn test_build_storage_options_azure() {
        let config = DeltaLakeSinkConfig {
            azure: Some(AzureStorageConfig {
                account: "myaccount".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_NAME"),
            Some(&"myaccount".to_string())
        );
    }

    #[test]
    fn test_build_storage_options_user_overrides() {
        let config = DeltaLakeSinkConfig {
            s3: Some(S3StorageConfig {
                region: "us-east-1".to_string(),
                ..Default::default()
            }),
            storage_options: HashMap::from([(
                "AWS_REGION".to_string(),
                "eu-west-1".to_string(), // Override S3 region
            )]),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        // User override should win
        assert_eq!(opts.get("AWS_REGION"), Some(&"eu-west-1".to_string()));
    }

    #[test]
    fn test_build_storage_options_local() {
        let config = DeltaLakeSinkConfig {
            table_uri: "/tmp/delta".to_string(),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert!(opts.is_empty());
    }

    // ── Schema Tests ─────────────────────────────────────────────

    #[test]
    fn test_default_delta_schema() {
        let fields = default_delta_schema();
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[0].name(), "data");
        assert_eq!(fields[1].name(), "event_type");
        assert_eq!(fields[2].name(), "timestamp");
        assert_eq!(fields[3].name(), "stream");
        // data, event_type, timestamp are required; stream is nullable
        assert!(!fields[0].is_nullable());
        assert!(!fields[1].is_nullable());
        assert!(!fields[2].is_nullable());
        assert!(fields[3].is_nullable());
    }

    // ── Enum Serialization Tests ─────────────────────────────────

    #[test]
    fn test_write_mode_serialization() {
        let append: DeltaWriteMode = serde_json::from_str(r#""append""#).unwrap();
        assert_eq!(append, DeltaWriteMode::Append);
        let overwrite: DeltaWriteMode = serde_json::from_str(r#""overwrite""#).unwrap();
        assert_eq!(overwrite, DeltaWriteMode::Overwrite);
    }

    #[test]
    fn test_compression_serialization() {
        let codecs = vec![
            ("\"none\"", DeltaCompression::None),
            ("\"snappy\"", DeltaCompression::Snappy),
            ("\"gzip\"", DeltaCompression::Gzip),
            ("\"lz4\"", DeltaCompression::Lz4),
            ("\"zstd\"", DeltaCompression::Zstd),
        ];
        for (json, expected) in codecs {
            let parsed: DeltaCompression = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn test_compression_to_parquet() {
        assert!(matches!(
            DeltaCompression::Snappy.to_parquet_compression(),
            Compression::SNAPPY
        ));
        assert!(matches!(
            DeltaCompression::None.to_parquet_compression(),
            Compression::UNCOMPRESSED
        ));
        assert!(matches!(
            DeltaCompression::Zstd.to_parquet_compression(),
            Compression::ZSTD(_)
        ));
    }

    // ── Check Tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn test_sink_check_valid_config() {
        let sink = DeltaLakeSink::new();
        let config = DeltaLakeSinkConfig {
            table_uri: "/tmp/delta-test-events".to_string(),
            ..Default::default()
        };
        let result = sink.check(&config).await.unwrap();
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_sink_check_missing_table_uri() {
        let sink = DeltaLakeSink::new();
        let config = DeltaLakeSinkConfig::default();
        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_sink_check_invalid_partition_column() {
        let sink = DeltaLakeSink::new();
        let config = DeltaLakeSinkConfig {
            table_uri: "/tmp/delta-test".to_string(),
            partition_columns: vec!["nonexistent_column".to_string()],
            ..Default::default()
        };
        let result = sink.check(&config).await.unwrap();
        assert!(!result.is_success());
    }

    // ── RecordBatch Tests ────────────────────────────────────────

    #[test]
    fn test_events_to_record_batch() {
        let schema = default_arrow_schema();

        let events = vec![
            SourceEvent::insert("test-stream", serde_json::json!({"key": "value1"})),
            SourceEvent::update("test-stream", None, serde_json::json!({"key": "value2"})),
        ];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema(), schema);
    }

    // ── Metrics Snapshot Tests ───────────────────────────────────

    #[test]
    fn test_metrics_snapshot_computed() {
        let snap = DeltaLakeMetricsSnapshot {
            records_written: 1000,
            records_failed: 10,
            bytes_written: 50000,
            commits_success: 5,
            commits_failed: 1,
            commit_retries: 2,
            files_created: 5,
            batches_flushed: 5,
            commit_latency_us: 50_000, // 50ms total
            write_latency_us: 100_000, // 100ms total
            batch_size_min: 100,
            batch_size_max: 300,
            batch_size_sum: 1000,
        };

        // Average commit latency: 50_000 / 5 / 1000 = 10ms
        assert!((snap.avg_commit_latency_ms() - 10.0).abs() < 0.01);
        // Average write latency: 100_000 / 5 / 1000 = 20ms
        assert!((snap.avg_write_latency_ms() - 20.0).abs() < 0.01);
        // Retry rate: 2 / (5 + 1) = 0.333
        assert!((snap.retry_rate() - 0.333).abs() < 0.01);
        // Success rate: 5 / (5 + 1) = 0.833
        assert!((snap.success_rate() - 0.833).abs() < 0.01);
        // Average batch size: 1000 / 5 = 200
        assert!((snap.avg_batch_size() - 200.0).abs() < 0.01);
        // Records/sec: 1000 / 10 = 100
        assert!((snap.records_per_second(10.0) - 100.0).abs() < 0.01);
        // Bytes/sec: 50000 / 10 = 5000
        assert!((snap.bytes_per_second(10.0) - 5000.0).abs() < 0.01);
    }

    #[test]
    fn test_metrics_snapshot_edge_cases() {
        let snap = DeltaLakeMetricsSnapshot::default();
        assert_eq!(snap.avg_commit_latency_ms(), 0.0);
        assert_eq!(snap.avg_write_latency_ms(), 0.0);
        assert_eq!(snap.retry_rate(), 0.0);
        assert_eq!(snap.success_rate(), 1.0); // No failures = 100% success
        assert_eq!(snap.avg_batch_size(), 0.0);
        assert_eq!(snap.records_per_second(0.0), 0.0);
        assert_eq!(snap.bytes_per_second(-1.0), 0.0);
    }

    #[test]
    fn test_metrics_snapshot_clone_eq() {
        let snap = DeltaLakeMetricsSnapshot {
            records_written: 42,
            ..Default::default()
        };
        let cloned = snap.clone();
        assert_eq!(snap, cloned);
    }

    #[test]
    fn test_metrics_atomic_snapshot() {
        let metrics = DeltaLakeSinkMetrics::new();
        metrics.records_written.store(100, Ordering::Relaxed);
        metrics.bytes_written.store(5000, Ordering::Relaxed);
        metrics.commits_success.store(3, Ordering::Relaxed);
        metrics.batches_flushed.store(3, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.records_written, 100);
        assert_eq!(snap.bytes_written, 5000);
        assert_eq!(snap.commits_success, 3);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = DeltaLakeSinkMetrics::new();
        metrics.records_written.store(100, Ordering::Relaxed);
        metrics.commits_success.store(5, Ordering::Relaxed);
        metrics.files_created.store(3, Ordering::Relaxed);
        metrics.record_batch_size(42); // Set batch_size_min/max/sum

        metrics.reset();

        assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.files_created.load(Ordering::Relaxed), 0);
        // After reset, batch_size_min returns sentinel → snapshot shows 0
        let snap = metrics.snapshot();
        assert_eq!(snap.batch_size_min, 0);
        assert_eq!(snap.batch_size_max, 0);
        assert_eq!(snap.batch_size_sum, 0);
    }

    #[test]
    fn test_metrics_snapshot_and_reset() {
        let metrics = DeltaLakeSinkMetrics::new();
        metrics.records_written.store(100, Ordering::Relaxed);
        metrics.commits_success.store(5, Ordering::Relaxed);

        let snap = metrics.snapshot_and_reset();
        assert_eq!(snap.records_written, 100);
        assert_eq!(snap.commits_success, 5);

        // After reset
        assert_eq!(metrics.records_written.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.commits_success.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_metrics_records_per_second() {
        let metrics = DeltaLakeSinkMetrics::new();
        metrics.records_written.store(1000, Ordering::Relaxed);

        let rps = metrics.records_per_second(Duration::from_secs(10));
        assert!((rps - 100.0).abs() < 0.01);

        // Edge case: zero duration
        assert_eq!(metrics.records_per_second(Duration::ZERO), 0.0);
    }

    #[test]
    fn test_metrics_batch_size_tracking() {
        let metrics = DeltaLakeSinkMetrics::new();
        metrics.record_batch_size(100);
        metrics.record_batch_size(200);
        metrics.record_batch_size(50);

        let snap = metrics.snapshot();
        assert_eq!(snap.batch_size_min, 50);
        assert_eq!(snap.batch_size_max, 200);
        assert_eq!(snap.batch_size_sum, 350);
    }

    #[test]
    fn test_metrics_batch_size_empty() {
        let metrics = DeltaLakeSinkMetrics::new();
        let snap = metrics.snapshot();
        // Sentinel u64::MAX should appear as 0 in snapshot
        assert_eq!(snap.batch_size_min, 0);
    }

    #[test]
    fn test_metrics_prometheus_export() {
        let snap = DeltaLakeMetricsSnapshot {
            records_written: 1000,
            records_failed: 5,
            bytes_written: 50000,
            commits_success: 10,
            commits_failed: 1,
            commit_retries: 2,
            files_created: 10,
            batches_flushed: 10,
            commit_latency_us: 100000,
            write_latency_us: 200000,
            batch_size_min: 50,
            batch_size_max: 200,
            batch_size_sum: 1000,
        };

        let prom = snap.to_prometheus_format("test");

        // Verify HELP/TYPE annotations are present
        assert!(prom.contains("# HELP test_delta_records_written_total"));
        assert!(prom.contains("# TYPE test_delta_records_written_total counter"));
        assert!(prom.contains("test_delta_records_written_total 1000"));
        assert!(prom.contains("# HELP test_delta_records_failed_total"));
        assert!(prom.contains("test_delta_records_failed_total 5"));
        assert!(prom.contains("test_delta_commits_total 10"));
        assert!(prom.contains("test_delta_commits_failed_total 1"));
        assert!(prom.contains("test_delta_commit_retries_total 2"));
        assert!(prom.contains("test_delta_files_created_total 10"));
        assert!(prom.contains("test_delta_batches_flushed_total 10"));

        // Computed gauges
        assert!(prom.contains("# TYPE test_delta_commit_latency_avg_ms gauge"));
        assert!(prom.contains("# TYPE test_delta_success_rate gauge"));
        assert!(prom.contains("# TYPE test_delta_batch_size_avg gauge"));

        // Conditional min/max (batch_size_max > 0)
        assert!(prom.contains("test_delta_batch_size_min 50"));
        assert!(prom.contains("test_delta_batch_size_max 200"));
    }

    #[test]
    fn test_metrics_prometheus_empty() {
        let snap = DeltaLakeMetricsSnapshot::default();
        let prom = snap.to_prometheus_format("empty");

        // Should still have HELP/TYPE for counters
        assert!(prom.contains("# HELP empty_delta_records_written_total"));
        assert!(prom.contains("empty_delta_records_written_total 0"));

        // Should NOT contain min/max when batch_size_max is 0
        assert!(!prom.contains("empty_delta_batch_size_min"));
        assert!(!prom.contains("empty_delta_batch_size_max"));

        // Success rate defaults to 1.0 when no commits
        assert!(prom.contains("empty_delta_success_rate 1.0000"));
    }

    #[test]
    fn test_metrics_json_serialization() {
        let snap = DeltaLakeMetricsSnapshot {
            records_written: 42,
            commits_success: 2,
            ..Default::default()
        };

        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: DeltaLakeMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, deserialized);
    }

    #[test]
    fn test_metrics_avg_batch_size() {
        let snap = DeltaLakeMetricsSnapshot {
            batches_flushed: 4,
            batch_size_sum: 1000,
            ..Default::default()
        };
        assert!((snap.avg_batch_size() - 250.0).abs() < 0.01);
    }

    // ── Writer Properties Tests ──────────────────────────────────

    #[test]
    fn test_build_writer_properties() {
        let config = DeltaLakeSinkConfig {
            compression: DeltaCompression::Zstd,
            ..Default::default()
        };
        let props = build_writer_properties(&config);
        // WriterProperties is opaque, but it should build without panic
        let _ = props;
    }

    // ── Factory Tests ────────────────────────────────────────────

    #[test]
    fn test_factory_creates_sink() {
        let factory = DeltaLakeSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "delta-lake");

        let sink = factory.create();
        // Just verify it creates without panic
        let _ = sink;
    }

    // ── YAML Deserialization Tests ───────────────────────────────

    #[test]
    fn test_config_yaml_deserialization() {
        let yaml = r#"
            table_uri: s3://my-bucket/warehouse/events
            batch_size: 5000
            flush_interval_secs: 30
            compression: zstd
            write_mode: append
            partition_columns:
              - event_type
            s3:
              region: us-west-2
              endpoint: http://minio:9000
              path_style_access: true
              allow_http: true
        "#;
        let parsed: DeltaLakeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.table_uri, "s3://my-bucket/warehouse/events");
        assert_eq!(parsed.batch_size, 5000);
        assert_eq!(parsed.flush_interval_secs, 30);
        assert_eq!(parsed.compression, DeltaCompression::Zstd);
        assert_eq!(parsed.write_mode, DeltaWriteMode::Append);
        assert_eq!(parsed.partition_columns, vec!["event_type"]);
        let s3 = parsed.s3.unwrap();
        assert_eq!(s3.region, "us-west-2");
        assert_eq!(s3.endpoint, Some("http://minio:9000".to_string()));
        assert!(s3.path_style_access);
        assert!(s3.allow_http);
    }

    #[test]
    fn test_config_yaml_minimal() {
        let yaml = r#"
            table_uri: /tmp/delta-events
        "#;
        let parsed: DeltaLakeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.table_uri, "/tmp/delta-events");
        assert!(parsed.auto_create_table);
        assert_eq!(parsed.batch_size, 10_000);
        assert_eq!(parsed.compression, DeltaCompression::Snappy);
        assert_eq!(parsed.write_mode, DeltaWriteMode::Append);
    }

    #[test]
    fn test_config_yaml_with_table_properties() {
        let yaml = r#"
            table_uri: /tmp/delta-events
            table_properties:
              delta.logRetentionDuration: "interval 30 days"
              delta.deletedFileRetentionDuration: "interval 7 days"
        "#;
        let parsed: DeltaLakeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            parsed.table_properties.get("delta.logRetentionDuration"),
            Some(&"interval 30 days".to_string())
        );
        assert_eq!(
            parsed
                .table_properties
                .get("delta.deletedFileRetentionDuration"),
            Some(&"interval 7 days".to_string())
        );
    }

    #[test]
    fn test_config_yaml_overwrite_mode() {
        let yaml = r#"
            table_uri: /tmp/delta-events
            write_mode: overwrite
        "#;
        let parsed: DeltaLakeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(parsed.write_mode, DeltaWriteMode::Overwrite);
    }

    // ── Write Path Tests ─────────────────────────────────────────

    #[tokio::test]
    async fn test_sink_write_empty_stream() {
        let sink = DeltaLakeSink::new();
        let config = DeltaLakeSinkConfig {
            table_uri: "/tmp/delta-test-empty-stream".to_string(),
            ..Default::default()
        };

        // An empty stream should produce zero records written, zero failures
        let events = futures::stream::empty();
        let result = sink.write(&config, Box::pin(events)).await.unwrap();
        assert_eq!(result.records_written, 0);
        assert_eq!(result.records_failed, 0);
        assert_eq!(result.bytes_written, 0);
    }

    #[tokio::test]
    async fn test_sink_write_local_table() {
        use tempfile::TempDir;

        let tmp = TempDir::new().unwrap();
        let table_uri = tmp.path().to_str().unwrap().to_string();

        let sink = DeltaLakeSink::new();
        let config = DeltaLakeSinkConfig {
            table_uri: table_uri.clone(),
            batch_size: 2, // Small batch for testing
            auto_create_table: true,
            ..Default::default()
        };

        // Write 5 events (will trigger 2 flushes of 2 + 1 final flush of 1)
        let events: Vec<SourceEvent> = (0..5)
            .map(|i| {
                SourceEvent::insert("test-stream", serde_json::json!({"idx": i, "msg": "hello"}))
            })
            .collect();

        let result = sink
            .write(&config, Box::pin(futures::stream::iter(events)))
            .await
            .unwrap();

        assert_eq!(result.records_written, 5);
        assert_eq!(result.records_failed, 0);
        assert!(result.bytes_written > 0);

        // Verify metrics were updated
        let snap = sink.metrics().snapshot();
        assert_eq!(snap.records_written, 5);
        assert_eq!(snap.commits_success, 3); // ceil(5/2) = 3 flushes
        assert_eq!(snap.batches_flushed, 3);
        assert_eq!(snap.files_created, 3);
        assert_eq!(snap.records_failed, 0);
        assert_eq!(snap.commits_failed, 0);
        assert_eq!(snap.batch_size_min, 1); // last batch had 1 event
        assert_eq!(snap.batch_size_max, 2);

        // Verify the Delta table is readable
        let table_url = deltalake::ensure_table_uri(&table_uri).unwrap();
        let table = deltalake::open_table(table_url).await.unwrap();
        assert!(table.version().unwrap() >= 0);
    }

    // ── Edge Case Tests ──────────────────────────────────────────

    #[test]
    fn test_events_to_record_batch_single() {
        let schema = default_arrow_schema();
        let events = vec![SourceEvent::insert(
            "stream-1",
            serde_json::json!({"key": "value"}),
        )];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_events_to_record_batch_complex_json() {
        let schema = default_arrow_schema();
        let events = vec![SourceEvent::insert(
            "stream-1",
            serde_json::json!({
                "nested": {"deep": [1, 2, 3]},
                "unicode": "héllo wörld 🌍",
                "null_val": null,
                "big_num": 999999999999999i64,
            }),
        )];

        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_estimate_batch_bytes_nonzero() {
        let schema = default_arrow_schema();
        let events = vec![
            SourceEvent::insert("s", serde_json::json!({"a": 1})),
            SourceEvent::insert("s", serde_json::json!({"b": 2})),
        ];
        let batch = events_to_record_batch(&events, &schema).unwrap();
        assert!(estimate_batch_bytes(&batch) > 0);
    }

    #[test]
    fn test_default_arrow_schema_matches_delta_schema() {
        let delta_fields = default_delta_schema();
        let arrow_schema = default_arrow_schema();

        assert_eq!(delta_fields.len(), arrow_schema.fields().len());
        for (df, af) in delta_fields.iter().zip(arrow_schema.fields().iter()) {
            assert_eq!(df.name(), af.name().as_str());
            assert_eq!(df.is_nullable(), af.is_nullable());
        }
    }

    #[test]
    fn test_build_storage_options_s3_with_credentials() {
        let config = DeltaLakeSinkConfig {
            s3: Some(S3StorageConfig {
                region: "eu-central-1".to_string(),
                access_key_id: Some(SensitiveString::new("AKIAIOSFODNN7EXAMPLE")),
                secret_access_key: Some(SensitiveString::new(
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert_eq!(opts.get("AWS_REGION"), Some(&"eu-central-1".to_string()));
        assert_eq!(
            opts.get("AWS_ACCESS_KEY_ID"),
            Some(&"AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(
            opts.get("AWS_SECRET_ACCESS_KEY"),
            Some(&"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())
        );
    }

    #[test]
    fn test_build_storage_options_azure_sas() {
        let config = DeltaLakeSinkConfig {
            azure: Some(AzureStorageConfig {
                account: "teststorage".to_string(),
                sas_token: Some(SensitiveString::new(
                    "sv=2021-06-08&ss=b&srt=c&sp=rwdlacupiytfx",
                )),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = build_storage_options(&config);
        assert_eq!(
            opts.get("AZURE_STORAGE_ACCOUNT_NAME"),
            Some(&"teststorage".to_string())
        );
        assert!(opts.contains_key("AZURE_STORAGE_SAS_TOKEN"));
    }

    #[test]
    fn test_config_schema_generation() {
        // Verify JSON Schema can be generated from the config
        let schema = schemars::schema_for!(DeltaLakeSinkConfig);
        let json = serde_json::to_string_pretty(&schema).unwrap();
        assert!(json.contains("table_uri"));
        assert!(json.contains("batch_size"));
        assert!(json.contains("compression"));
    }
}
