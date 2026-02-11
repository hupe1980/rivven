//! Lakehouse connectors for Rivven Connect
//!
//! This module provides sink connectors for modern lakehouse table formats:
//! - **Apache Iceberg** — Open table format for huge analytic datasets
//! - **Delta Lake** — Open-source storage framework with ACID transactions
//!
//! # Features
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Batched writes | Configurable batch size and flush interval |
//! | Commit retry | Exponential backoff on transaction conflicts |
//! | Lock-free metrics | Atomic counters for observability |
//! | Batch size tracking | Min/max/avg with lock-free CAS |
//! | Prometheus export | `to_prometheus_format()` for scraping |
//! | JSON serialization | Serde derives for export |
//! | Multiple catalogs | REST, Glue, Hive, Memory (Iceberg) |
//! | Storage backends | S3, GCS, Azure, local filesystem |
//! | Compression | Snappy, Gzip, LZ4, Zstd, Brotli |

#[cfg(feature = "iceberg")]
pub mod iceberg;

#[cfg(feature = "delta-lake")]
pub mod delta;

// Re-exports
#[cfg(feature = "iceberg")]
pub use iceberg::{
    IcebergSink, IcebergSinkConfig, IcebergSinkFactory, IcebergSinkMetrics, MetricsSnapshot,
};

#[cfg(feature = "delta-lake")]
pub use delta::{
    DeltaLakeMetricsSnapshot, DeltaLakeSink, DeltaLakeSinkConfig, DeltaLakeSinkFactory,
    DeltaLakeSinkMetrics,
};
