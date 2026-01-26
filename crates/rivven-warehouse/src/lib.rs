//! Data warehouse connectors for Rivven Connect
//!
//! This crate provides sink connectors for data warehouses:
//!
//! - **Snowflake** - Snowpipe Streaming REST API
//! - **BigQuery** - Google BigQuery (future)
//! - **Redshift** - AWS Redshift (future)
//!
//! # Feature Flags
//!
//! Enable only the warehouses you need:
//!
//! ```toml
//! # Snowflake only
//! rivven-warehouse = { version = "0.2", features = ["snowflake"] }
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

/// Register all enabled warehouse connectors with the sink registry
pub fn register_all(registry: &mut rivven_connect::SinkRegistry) {
    #[cfg(feature = "snowflake")]
    snowflake::register(registry);

    #[cfg(feature = "bigquery")]
    {
        // Future: BigQuery registration
    }

    #[cfg(feature = "redshift")]
    {
        // Future: Redshift registration
    }

    // Suppress unused warning when no features are enabled
    let _ = registry;
}
