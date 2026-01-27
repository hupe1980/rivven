//! Data warehouse connectors for Rivven Connect
//!
//! This crate provides sink connectors for data warehouses:
//!
//! - **Snowflake** - Snowpipe Streaming REST API
//! - **BigQuery** - Google BigQuery insertAll API
//! - **Redshift** - Amazon Redshift via PostgreSQL protocol
//!
//! # Feature Flags
//!
//! Enable only the warehouses you need:
//!
//! ```toml
//! # Snowflake only
//! rivven-warehouse = { version = "0.2", features = ["snowflake"] }
//!
//! # BigQuery only
//! rivven-warehouse = { version = "0.2", features = ["bigquery"] }
//!
//! # Redshift only
//! rivven-warehouse = { version = "0.2", features = ["redshift"] }
//!
//! # All providers
//! rivven-warehouse = { version = "0.2", features = ["full"] }
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::SinkRegistry;
//! use rivven_warehouse::SnowflakeSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("snowflake", Arc::new(SnowflakeSinkFactory));
//! ```

#[cfg(feature = "snowflake")]
pub mod snowflake;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "redshift")]
pub mod redshift;

// Re-exports for convenience
#[cfg(feature = "snowflake")]
pub use snowflake::{SnowflakeSink, SnowflakeSinkConfig, SnowflakeSinkFactory};

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQuerySink, BigQuerySinkConfig, BigQuerySinkFactory};

#[cfg(feature = "redshift")]
pub use redshift::{RedshiftSink, RedshiftSinkConfig, RedshiftSinkFactory};

/// Register all enabled warehouse connectors with the sink registry
pub fn register_all(registry: &mut rivven_connect::SinkRegistry) {
    #[cfg(feature = "snowflake")]
    snowflake::register(registry);

    #[cfg(feature = "bigquery")]
    bigquery::register(registry);

    #[cfg(feature = "redshift")]
    redshift::register(registry);

    // Suppress unused warning when no features are enabled
    let _ = registry;
}
