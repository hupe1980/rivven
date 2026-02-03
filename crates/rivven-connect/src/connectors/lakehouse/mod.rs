//! Lakehouse connectors for Rivven Connect
//!
//! This module provides sink connectors for modern lakehouse table formats:
//! - **Apache Iceberg** - Open table format for huge analytic datasets
//!
//! Future additions:
//! - Delta Lake
//! - Apache Hudi

#[cfg(feature = "iceberg")]
pub mod iceberg;

// Re-exports
#[cfg(feature = "iceberg")]
pub use iceberg::{IcebergSink, IcebergSinkConfig, IcebergSinkFactory};
