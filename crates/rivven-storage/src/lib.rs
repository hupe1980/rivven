//! Object storage connectors for Rivven Connect
//!
//! This crate provides sink connectors for object/blob storage systems:
//!
//! - **S3** - Amazon S3 and S3-compatible storage (MinIO, LocalStack, Cloudflare R2)
//! - **GCS** - Google Cloud Storage (ADC, service account key)
//! - **Azure Blob** - Azure Blob Storage (connection string, SAS, account key)
//!
//! # Feature Flags
//!
//! Enable only the storage providers you need:
//!
//! ```toml
//! # S3 only
//! rivven-storage = { version = "0.0.1", features = ["s3"] }
//!
//! # GCS only
//! rivven-storage = { version = "0.0.1", features = ["gcs"] }
//!
//! # Azure only
//! rivven-storage = { version = "0.0.1", features = ["azure"] }
//!
//! # All providers
//! rivven-storage = { version = "0.0.1", features = ["full"] }
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::SinkRegistry;
//! use rivven_storage::S3SinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("s3", Arc::new(S3SinkFactory));
//! ```

#[cfg(feature = "s3")]
pub mod s3;

#[cfg(feature = "gcs")]
pub mod gcs;

#[cfg(feature = "azure")]
pub mod azure;

#[cfg(feature = "parquet")]
pub mod parquet;

// Re-exports for convenience
#[cfg(feature = "s3")]
pub use s3::{S3Compression, S3Format, S3Partitioning, S3Sink, S3SinkConfig, S3SinkFactory};

#[cfg(feature = "gcs")]
pub use gcs::{GcsCompression, GcsFormat, GcsPartitioning, GcsSink, GcsSinkConfig, GcsSinkFactory};

#[cfg(feature = "azure")]
pub use azure::{
    AzureBlobCompression, AzureBlobFormat, AzureBlobPartitioning, AzureBlobSink,
    AzureBlobSinkConfig, AzureBlobSinkFactory,
};

#[cfg(feature = "parquet")]
pub use parquet::{
    write_events_to_parquet, ParquetCompression, ParquetError, ParquetResult, ParquetWriter,
    ParquetWriterConfig, SchemaInference,
};

/// Register all enabled storage connectors with the sink registry
pub fn register_all(registry: &mut rivven_connect::SinkRegistry) {
    #[cfg(feature = "s3")]
    s3::register(registry);

    #[cfg(feature = "gcs")]
    gcs::register(registry);

    #[cfg(feature = "azure")]
    azure::register(registry);

    // Suppress unused warning when no features are enabled
    let _ = registry;
}
