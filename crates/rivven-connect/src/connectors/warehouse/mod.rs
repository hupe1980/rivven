//! Data warehouse connectors for Rivven Connect
//!
//! This module provides sink connectors for data warehouses:
//! - **Snowflake** - Snowflake Data Cloud
//! - **BigQuery** - Google BigQuery
//! - **Redshift** - Amazon Redshift
//! - **Databricks** - Databricks Delta tables via Zerobus
//! - **ClickHouse** - ClickHouse OLAP database via native RowBinary

#[cfg(feature = "snowflake")]
pub mod snowflake;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "redshift")]
pub mod redshift;

#[cfg(feature = "databricks")]
pub mod databricks;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

// Re-exports
#[cfg(feature = "snowflake")]
pub use snowflake::{SnowflakeSink, SnowflakeSinkConfig, SnowflakeSinkFactory};

#[cfg(feature = "bigquery")]
pub use bigquery::{BigQuerySink, BigQuerySinkConfig, BigQuerySinkFactory};

#[cfg(feature = "redshift")]
pub use redshift::{RedshiftSink, RedshiftSinkConfig, RedshiftSinkFactory};

#[cfg(feature = "databricks")]
pub use databricks::{DatabricksSink, DatabricksSinkConfig, DatabricksSinkFactory};

#[cfg(feature = "clickhouse")]
pub use clickhouse::{ClickHouseSink, ClickHouseSinkConfig, ClickHouseSinkFactory};
