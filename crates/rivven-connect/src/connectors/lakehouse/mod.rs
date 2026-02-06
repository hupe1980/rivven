//! Lakehouse connectors for Rivven Connect
//!
//! This module provides sink connectors for modern lakehouse table formats:
//! - **Apache Iceberg** - Open table format for huge analytic datasets
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
//! | Multiple catalogs | REST, Glue, Hive, Memory |
//! | Compression | Snappy, Gzip, LZ4, Zstd, Brotli |
//!
//! Future additions:
//! - Delta Lake
//! - Apache Hudi

#[cfg(feature = "iceberg")]
pub mod iceberg;

// Re-exports
#[cfg(feature = "iceberg")]
pub use iceberg::{
    IcebergSink, IcebergSinkConfig, IcebergSinkFactory, IcebergSinkMetrics, MetricsSnapshot,
};
