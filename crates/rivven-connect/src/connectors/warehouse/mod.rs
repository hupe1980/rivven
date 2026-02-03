//! Data warehouse connectors for Rivven Connect
//!
//! This module provides sink connectors for data warehouses:
//! - **Snowflake** - Snowflake Data Cloud
//! - **BigQuery** - Google BigQuery
//! - **Redshift** - Amazon Redshift

#[cfg(feature = "snowflake")]
pub mod snowflake;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "redshift")]
pub mod redshift;

// Re-exports
#[cfg(feature = "snowflake")]
pub use snowflake::{SnowflakeSink, SnowflakeSinkConfig, SnowflakeSinkFactory};

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQuerySink, BigQuerySinkConfig, BigQuerySinkFactory};

#[cfg(feature = "redshift")]
pub use redshift::{RedshiftSink, RedshiftSinkConfig, RedshiftSinkFactory};
