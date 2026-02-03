//! Object storage connectors for Rivven Connect
//!
//! This module provides sink connectors for object/blob storage systems using
//! the unified `object_store` crate:
//! - Amazon S3 and S3-compatible (MinIO, Cloudflare R2, DigitalOcean Spaces)
//! - Google Cloud Storage (GCS)
//! - Azure Blob Storage
//! - Local filesystem (for testing)
//!
//! # Example
//!
//! ```yaml
//! sinks:
//!   events:
//!     connector: object-storage
//!     config:
//!       provider: s3
//!       bucket: my-bucket
//!       prefix: events/
//!       format: jsonl
//!       compression: gzip
//!       s3:
//!         region: us-east-1
//! ```

// Common types and utilities shared by all storage connectors
pub mod common;

// Unified object storage sink
#[cfg(feature = "cloud-storage")]
pub mod unified;

// Re-export common types
pub use common::{
    default_batch_size, default_flush_interval_secs, event_to_json, generate_object_key,
    get_content_type, serialize_batch, StorageCompression, StorageFormat, StoragePartitioning,
};

// Re-export unified sink
#[cfg(feature = "cloud-storage")]
pub use unified::{
    AzureConfig, GcsConfig, LocalConfig, ObjectStorageSink, ObjectStorageSinkConfig,
    ObjectStorageSinkFactory, S3Config, StorageProvider,
};
