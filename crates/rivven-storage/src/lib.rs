//! Object storage connectors for Rivven Connect
//!
//! This crate provides sink connectors for object/blob storage systems:
//!
//! - **S3** - Amazon S3 and S3-compatible storage (MinIO, LocalStack, Cloudflare R2)
//! - **GCS** - Google Cloud Storage (future)
//! - **Azure Blob** - Azure Blob Storage (future)
//! - **HDFS** - Hadoop Distributed File System (future)
//!
//! # Feature Flags
//!
//! Enable only the storage providers you need:
//!
//! ```toml
//! # S3 only
//! rivven-storage = { version = "0.2", features = ["s3"] }
//!
//! # All providers
//! rivven-storage = { version = "0.2", features = ["full"] }
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

// Re-exports for convenience
#[cfg(feature = "s3")]
pub use s3::{S3Compression, S3Format, S3Partitioning, S3Sink, S3SinkConfig, S3SinkFactory};

/// Register all enabled storage connectors with the sink registry
pub fn register_all(registry: &mut rivven_connect::SinkRegistry) {
    #[cfg(feature = "s3")]
    s3::register(registry);

    #[cfg(feature = "gcs")]
    {
        // Future: GCS registration
    }

    #[cfg(feature = "azure")]
    {
        // Future: Azure Blob registration
    }

    // Suppress unused warning when no features are enabled
    let _ = registry;
}
