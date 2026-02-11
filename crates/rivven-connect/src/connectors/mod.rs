//! Connector implementations using rivven-connect SDK traits
//!
//! This module provides the registry pattern for runtime connector lookup.
//! All connector implementations are included based on enabled feature flags.
//!
//! # Scalable Architecture (300+ Connectors)
//!
//! Rivven Connect is designed to scale to 300+ connectors.
//! The architecture uses:
//!
//! 1. **Hierarchical Categories**: Connectors organized by type (database, messaging, etc.)
//! 2. **Rich Metadata**: Tags and search capabilities
//! 3. **Lazy Loading**: Connectors instantiated only when needed
//! 4. **Inventory System**: Auto-registration with metadata indexing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                 Connector Inventory (Scalable)                   │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Categories                                                      │
//! │  ├── Database (postgres_cdc, mysql_cdc, mongodb, redis, ...)    │
//! │  ├── Messaging (kafka, mqtt, sqs, pubsub, nats, ...)            │
//! │  ├── Storage (s3, gcs, azure, minio, ...)                       │
//! │  ├── Warehouse (snowflake, bigquery, redshift, ...)             │
//! │  ├── AI/ML (openai, anthropic, pinecone, ...)                   │
//! │  └── Utility (datagen, stdout, http, ...)                       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

// CDC connectors (PostgreSQL, MySQL, SQL Server)
pub mod cdc;

// Built-in sources (bundled, minimal dependencies)
pub mod datagen;

// Built-in sinks (bundled, minimal dependencies)
#[cfg(feature = "http")]
pub mod http;
pub mod stdout;

// RDBC connectors (query-based source/sink using rivven-rdbc)
#[cfg(feature = "rdbc")]
pub mod rdbc;

// Queue connectors (Kafka, MQTT, SQS, Pub/Sub)
pub mod queue;

// Storage connectors (S3, GCS, Azure Blob, Parquet)
pub mod storage;

// Warehouse connectors (Snowflake, BigQuery, Redshift)
pub mod warehouse;

// Lakehouse connectors (Apache Iceberg, Delta Lake)
pub mod lakehouse;
// Re-export registry types from SDK (now in traits module)
pub use super::traits::registry::{
    AnySink, AnySource, SinkFactory, SinkRegistry, SourceFactory, SourceRegistry,
};

// Re-export inventory types
pub use super::traits::inventory::ConnectorInventory;
pub use super::traits::metadata::{ConnectorCategory, ConnectorMetadata, ConnectorType};

// Re-export SensitiveString from centralized types module
pub use crate::types::SensitiveString;

use std::sync::Arc;

/// Create a source registry with all enabled sources
pub fn create_source_registry() -> SourceRegistry {
    let mut registry = SourceRegistry::new();

    // Always register datagen (bundled, no dependencies)
    registry.register("datagen", Arc::new(datagen::DatagenSourceFactory));

    // Always register postgres-cdc (bundled)
    #[cfg(feature = "postgres")]
    registry.register("postgres-cdc", Arc::new(cdc::PostgresCdcSourceFactory));

    // MySQL CDC (bundled, feature-gated)
    #[cfg(feature = "mysql")]
    registry.register("mysql-cdc", Arc::new(cdc::MySqlCdcSourceFactory));

    // SQL Server CDC (bundled, feature-gated)
    #[cfg(feature = "sqlserver")]
    registry.register("sqlserver-cdc", Arc::new(cdc::SqlServerCdcSourceFactory));

    // Queue sources (feature-gated)
    #[cfg(feature = "kafka")]
    registry.register("kafka", Arc::new(queue::KafkaSourceFactory));

    #[cfg(feature = "mqtt")]
    registry.register("mqtt", Arc::new(queue::MqttSourceFactory));

    #[cfg(feature = "sqs")]
    registry.register("sqs", Arc::new(queue::SqsSourceFactory));

    // Pub/Sub is always available (simulation mode)
    registry.register("pubsub", Arc::new(queue::PubSubSourceFactory));

    // RDBC source (query-based, feature-gated)
    #[cfg(feature = "rdbc")]
    registry.register("rdbc-source", Arc::new(rdbc::RdbcSourceFactory));

    registry
}

/// Create a sink registry with all enabled sinks
pub fn create_sink_registry() -> SinkRegistry {
    let mut registry = SinkRegistry::new();

    // Always register stdout (bundled, no dependencies)
    registry.register("stdout", Arc::new(stdout::StdoutSinkFactory));

    // HTTP webhook (built-in, feature-gated)
    #[cfg(feature = "http")]
    registry.register("http-webhook", Arc::new(http::HttpWebhookSinkFactory));

    // Queue sinks (feature-gated)
    #[cfg(feature = "kafka")]
    registry.register("kafka", Arc::new(queue::KafkaSinkFactory));

    #[cfg(feature = "sqs")]
    registry.register("sqs", Arc::new(queue::SqsSinkFactory));

    // Unified object storage sink (replaces separate s3/gcs/azure connectors)
    // Use "object-storage" as the connector type, with "provider" field in config
    #[cfg(feature = "cloud-storage")]
    registry.register(
        "object-storage",
        Arc::new(storage::ObjectStorageSinkFactory),
    );

    // Legacy aliases for backward compatibility during migration
    #[cfg(feature = "s3")]
    registry.register("s3", Arc::new(storage::ObjectStorageSinkFactory));

    #[cfg(feature = "gcs")]
    registry.register("gcs", Arc::new(storage::ObjectStorageSinkFactory));

    #[cfg(feature = "azure")]
    registry.register("azure-blob", Arc::new(storage::ObjectStorageSinkFactory));

    // Warehouse sinks (feature-gated)
    #[cfg(feature = "snowflake")]
    registry.register("snowflake", Arc::new(warehouse::SnowflakeSinkFactory));

    #[cfg(feature = "bigquery")]
    registry.register("bigquery", Arc::new(warehouse::BigQuerySinkFactory));

    #[cfg(feature = "redshift")]
    registry.register("redshift", Arc::new(warehouse::RedshiftSinkFactory));

    #[cfg(feature = "databricks")]
    registry.register("databricks", Arc::new(warehouse::DatabricksSinkFactory));

    // Lakehouse sinks (feature-gated)
    #[cfg(feature = "iceberg")]
    registry.register("iceberg", Arc::new(lakehouse::IcebergSinkFactory));

    #[cfg(feature = "delta-lake")]
    registry.register("delta-lake", Arc::new(lakehouse::DeltaLakeSinkFactory));

    // RDBC sink (query-based, feature-gated)
    #[cfg(feature = "rdbc")]
    registry.register("rdbc-sink", Arc::new(rdbc::RdbcSinkFactory));

    registry
}

/// Create a connector inventory with all enabled connectors and rich metadata
///
/// The inventory provides:
/// - Search and filtering by category, tags, support level
/// - Rich metadata for documentation generation
/// - Fast lookup by name or category
///
/// # Example
///
/// ```rust
/// use rivven_connect::connectors::create_connector_inventory;
/// use rivven_connect::connectors::ConnectorCategory;
///
/// let inventory = create_connector_inventory();
///
/// // Search for database connectors
/// let results = inventory.search("postgres");
///
/// // Filter by category
/// let cdc_connectors = inventory.by_category(ConnectorCategory::DatabaseCdc);
///
/// // Generate documentation
/// let docs = inventory.generate_docs();
/// ```
pub fn create_connector_inventory() -> ConnectorInventory {
    let mut inventory = ConnectorInventory::new();

    // ─────────────────────────────────────────────────────────────────
    // SOURCES
    // ─────────────────────────────────────────────────────────────────

    // Datagen (always available)
    inventory.register_source(
        ConnectorMetadata::builder("datagen")
            .title("Data Generator")
            .description("Generate synthetic test data without external dependencies")
            .source()
            .category(ConnectorCategory::UtilityGenerate)
            .tags(["test", "synthetic", "mock", "development", "demo"])
            .aliases(["generate", "faker", "mock-data"])
            .build(),
        Arc::new(datagen::DatagenSourceFactory),
    );

    // PostgreSQL CDC
    #[cfg(feature = "postgres")]
    inventory.register_source(
        ConnectorMetadata::builder("postgres-cdc")
            .title("PostgreSQL CDC")
            .description("Change Data Capture from PostgreSQL using logical replication")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .feature("postgres")
            .tags([
                "postgresql",
                "postgres",
                "cdc",
                "replication",
                "logical",
                "database",
            ])
            .aliases(["pg-cdc", "postgres_cdc", "postgresql-cdc"])
            .related("mysql-cdc")
            .build(),
        Arc::new(cdc::PostgresCdcSourceFactory),
    );

    // MySQL CDC
    #[cfg(feature = "mysql")]
    inventory.register_source(
        ConnectorMetadata::builder("mysql-cdc")
            .title("MySQL CDC")
            .description("Change Data Capture from MySQL/MariaDB using binlog replication")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .feature("mysql")
            .tags([
                "mysql",
                "mariadb",
                "cdc",
                "binlog",
                "replication",
                "database",
            ])
            .aliases(["mysql_cdc", "mariadb-cdc"])
            .related("postgres-cdc")
            .build(),
        Arc::new(cdc::MySqlCdcSourceFactory),
    );

    // Kafka source
    #[cfg(feature = "kafka")]
    inventory.register_source(
        ConnectorMetadata::builder("kafka")
            .title("Apache Kafka")
            .description("Consume messages from Apache Kafka topics")
            .source()
            .category(ConnectorCategory::MessagingKafka)
            .feature("kafka")
            .tags(["kafka", "streaming", "messaging", "queue", "consumer"])
            .aliases(["apache-kafka", "kafka-source"])
            .related("redpanda")
            .build(),
        Arc::new(queue::KafkaSourceFactory),
    );

    // MQTT source
    #[cfg(feature = "mqtt")]
    inventory.register_source(
        ConnectorMetadata::builder("mqtt")
            .title("MQTT")
            .description("Subscribe to MQTT broker topics")
            .source()
            .category(ConnectorCategory::MessagingMqtt)
            .feature("mqtt")
            .tags(["mqtt", "iot", "messaging", "broker", "subscribe"])
            .aliases(["mqtt-source", "mosquitto"])
            .build(),
        Arc::new(queue::MqttSourceFactory),
    );

    // SQS source
    #[cfg(feature = "sqs")]
    inventory.register_source(
        ConnectorMetadata::builder("sqs")
            .title("AWS SQS")
            .description("Receive messages from Amazon SQS queues")
            .source()
            .category(ConnectorCategory::MessagingCloud)
            .feature("sqs")
            .tags(["aws", "sqs", "amazon", "queue", "cloud", "messaging"])
            .aliases(["aws-sqs", "amazon-sqs", "simple-queue-service"])
            .related("pubsub")
            .build(),
        Arc::new(queue::SqsSourceFactory),
    );

    // Pub/Sub source (always available, runs in simulation mode)
    inventory.register_source(
        ConnectorMetadata::builder("pubsub")
            .title("Google Pub/Sub")
            .description("Receive messages from Google Cloud Pub/Sub (simulation mode)")
            .source()
            .category(ConnectorCategory::MessagingCloud)
            .tags(["gcp", "google", "pubsub", "cloud", "messaging"])
            .aliases(["gcp-pubsub", "google-pubsub", "cloud-pubsub"])
            .related("sqs")
            .build(),
        Arc::new(queue::PubSubSourceFactory),
    );

    // RDBC Source (query-based polling)
    #[cfg(feature = "rdbc")]
    inventory.register_source(
        ConnectorMetadata::builder("rdbc-source")
            .title("RDBC Source")
            .description("Query-based source for PostgreSQL, MySQL, SQL Server with bulk/incremental/timestamp modes")
            .source()
            .category(ConnectorCategory::DatabaseQuery)
            .feature("rdbc")
            .tags([
                "rdbc",
                "sql",
                "postgres",
                "mysql",
                "sqlserver",
                "query",
                "polling",
                "database",
            ])
            .aliases(["rdbc", "sql-source", "database-source", "query-source"])
            .related("postgres-cdc")
            .build(),
        Arc::new(rdbc::RdbcSourceFactory),
    );

    // ─────────────────────────────────────────────────────────────────
    // SINKS
    // ─────────────────────────────────────────────────────────────────

    // Stdout (always available)
    inventory.register_sink(
        ConnectorMetadata::builder("stdout")
            .title("Standard Output")
            .description("Write events to stdout for debugging and testing")
            .sink()
            .category(ConnectorCategory::UtilityDebug)
            .tags(["stdout", "debug", "console", "log", "test"])
            .aliases(["console", "log", "print"])
            .build(),
        Arc::new(stdout::StdoutSinkFactory),
    );

    // HTTP webhook
    #[cfg(feature = "http")]
    inventory.register_sink(
        ConnectorMetadata::builder("http-webhook")
            .title("HTTP Webhook")
            .description("Send events via HTTP POST requests")
            .sink()
            .category(ConnectorCategory::HttpApi)
            .feature("http")
            .tags(["http", "webhook", "rest", "api", "post"])
            .aliases(["webhook", "http-sink", "rest-api"])
            .build(),
        Arc::new(http::HttpWebhookSinkFactory),
    );

    // Kafka sink
    #[cfg(feature = "kafka")]
    inventory.register_sink(
        ConnectorMetadata::builder("kafka")
            .title("Apache Kafka")
            .description("Produce messages to Apache Kafka topics")
            .sink()
            .category(ConnectorCategory::MessagingKafka)
            .feature("kafka")
            .tags(["kafka", "streaming", "messaging", "queue", "producer"])
            .aliases(["apache-kafka", "kafka-sink"])
            .build(),
        Arc::new(queue::KafkaSinkFactory),
    );

    // S3 sink
    #[cfg(feature = "s3")]
    inventory.register_sink(
        ConnectorMetadata::builder("s3")
            .title("Amazon S3")
            .description("Write events to Amazon S3 object storage")
            .sink()
            .category(ConnectorCategory::StorageObject)
            .feature("s3")
            .tags(["aws", "s3", "amazon", "storage", "object", "cloud"])
            .aliases([
                "aws-s3",
                "amazon-s3",
                "simple-storage-service",
                "minio",
                "r2",
            ])
            .related("gcs")
            .build(),
        Arc::new(storage::ObjectStorageSinkFactory),
    );

    // GCS sink
    #[cfg(feature = "gcs")]
    inventory.register_sink(
        ConnectorMetadata::builder("gcs")
            .title("Google Cloud Storage")
            .description("Write events to Google Cloud Storage")
            .sink()
            .category(ConnectorCategory::StorageObject)
            .feature("gcs")
            .tags(["gcp", "google", "gcs", "storage", "object", "cloud"])
            .aliases(["google-cloud-storage", "gcp-storage"])
            .related("s3")
            .build(),
        Arc::new(storage::ObjectStorageSinkFactory),
    );

    // Azure Blob sink
    #[cfg(feature = "azure")]
    inventory.register_sink(
        ConnectorMetadata::builder("azure-blob")
            .title("Azure Blob Storage")
            .description("Write events to Azure Blob Storage")
            .sink()
            .category(ConnectorCategory::StorageObject)
            .feature("azure")
            .tags(["azure", "microsoft", "blob", "storage", "object", "cloud"])
            .aliases(["azure-storage", "azure-blob-storage"])
            .related("s3")
            .build(),
        Arc::new(storage::ObjectStorageSinkFactory),
    );

    // Snowflake sink
    #[cfg(feature = "snowflake")]
    inventory.register_sink(
        ConnectorMetadata::builder("snowflake")
            .title("Snowflake")
            .description("Load events into Snowflake Data Cloud")
            .sink()
            .category(ConnectorCategory::Warehouse)
            .feature("snowflake")
            .tags(["snowflake", "warehouse", "analytics", "cloud", "data-lake"])
            .aliases(["snowflake-streaming", "snowflake-put"])
            .related("bigquery")
            .build(),
        Arc::new(warehouse::SnowflakeSinkFactory),
    );

    // BigQuery sink
    #[cfg(feature = "bigquery")]
    inventory.register_sink(
        ConnectorMetadata::builder("bigquery")
            .title("Google BigQuery")
            .description("Load events into Google BigQuery")
            .sink()
            .category(ConnectorCategory::Warehouse)
            .feature("bigquery")
            .tags(["gcp", "google", "bigquery", "warehouse", "analytics"])
            .aliases(["gcp-bigquery", "google-bigquery", "bq"])
            .related("snowflake")
            .build(),
        Arc::new(warehouse::BigQuerySinkFactory),
    );

    // Redshift sink
    #[cfg(feature = "redshift")]
    inventory.register_sink(
        ConnectorMetadata::builder("redshift")
            .title("Amazon Redshift")
            .description("Load events into Amazon Redshift")
            .sink()
            .category(ConnectorCategory::Warehouse)
            .feature("redshift")
            .tags(["aws", "amazon", "redshift", "warehouse", "analytics"])
            .aliases(["aws-redshift", "amazon-redshift"])
            .related("snowflake")
            .build(),
        Arc::new(warehouse::RedshiftSinkFactory),
    );

    // Databricks sink
    #[cfg(feature = "databricks")]
    inventory.register_sink(
        ConnectorMetadata::builder("databricks")
            .title("Databricks")
            .description("Stream events into Databricks Delta tables via Zerobus")
            .sink()
            .category(ConnectorCategory::Warehouse)
            .feature("databricks")
            .tags([
                "databricks",
                "delta",
                "warehouse",
                "lakehouse",
                "zerobus",
                "streaming",
            ])
            .aliases(["databricks-delta", "databricks-zerobus"])
            .related("snowflake")
            .build(),
        Arc::new(warehouse::DatabricksSinkFactory),
    );

    // ─────────────────────────────────────────────────────────────────
    // LAKEHOUSE SINKS
    // ─────────────────────────────────────────────────────────────────

    // Apache Iceberg sink
    #[cfg(feature = "iceberg")]
    inventory.register_sink(
        ConnectorMetadata::builder("iceberg")
            .title("Apache Iceberg")
            .description("Write to Apache Iceberg tables - open table format for analytics")
            .sink()
            .category(ConnectorCategory::Lakehouse)
            .feature("iceberg")
            .tags([
                "iceberg",
                "lakehouse",
                "table-format",
                "analytics",
                "parquet",
                "s3",
                "gcs",
                "azure",
            ])
            .aliases(["apache-iceberg", "iceberg-sink"])
            .related("delta-lake")
            .build(),
        Arc::new(lakehouse::IcebergSinkFactory),
    );

    // Delta Lake sink
    #[cfg(feature = "delta-lake")]
    inventory.register_sink(
        ConnectorMetadata::builder("delta-lake")
            .title("Delta Lake")
            .description(
                "Write to Delta Lake tables - open-source lakehouse with ACID transactions",
            )
            .sink()
            .category(ConnectorCategory::Lakehouse)
            .feature("delta-lake")
            .tags([
                "delta",
                "delta-lake",
                "lakehouse",
                "table-format",
                "analytics",
                "parquet",
                "s3",
                "gcs",
                "azure",
                "acid",
            ])
            .aliases(["delta", "delta-lake-sink", "deltalake"])
            .related("iceberg")
            .build(),
        Arc::new(lakehouse::DeltaLakeSinkFactory),
    );

    // ─────────────────────────────────────────────────────────────────
    // DATABASE SINKS
    // ─────────────────────────────────────────────────────────────────

    // RDBC Sink (batch writes)
    #[cfg(feature = "rdbc")]
    inventory.register_sink(
        ConnectorMetadata::builder("rdbc-sink")
            .title("RDBC Sink")
            .description("High-performance batch sink for PostgreSQL, MySQL, SQL Server with upsert/insert/delete modes")
            .sink()
            .category(ConnectorCategory::DatabaseQuery)
            .feature("rdbc")
            .tags([
                "rdbc",
                "sql",
                "postgres",
                "mysql",
                "sqlserver",
                "sink",
                "batch",
                "database",
            ])
            .aliases(["rdbc", "sql-sink", "database-sink", "query-sink"])
            .related("postgres-cdc")
            .build(),
        Arc::new(rdbc::RdbcSinkFactory),
    );

    inventory
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_registry() {
        let registry = create_source_registry();
        let sources = registry.list();

        // datagen is always available
        assert!(sources.iter().any(|(name, _)| *name == "datagen"));

        #[cfg(feature = "postgres")]
        assert!(sources.iter().any(|(name, _)| *name == "postgres-cdc"));
    }

    #[test]
    fn test_sink_registry() {
        let registry = create_sink_registry();
        let sinks = registry.list();

        // stdout is always available
        assert!(sinks.iter().any(|(name, _)| *name == "stdout"));
    }

    #[test]
    fn test_registry_empty_initially() {
        // Using the SDK registries directly starts empty
        let sources = SourceRegistry::new();
        assert!(sources.is_empty());

        let sinks = SinkRegistry::new();
        assert!(sinks.is_empty());
    }

    #[test]
    fn test_connector_inventory() {
        let inventory = create_connector_inventory();

        // datagen should always be available
        assert!(inventory.get_source("datagen").is_some());
        assert!(inventory.get_source_metadata("datagen").is_some());

        let meta = inventory.get_source_metadata("datagen").unwrap();
        assert_eq!(meta.title, "Data Generator");
        assert_eq!(meta.category, ConnectorCategory::UtilityGenerate);
    }

    #[test]
    fn test_inventory_search() {
        let inventory = create_connector_inventory();

        // Search for "test"
        let results = inventory.search("test");
        assert!(!results.is_empty());
        assert!(results.iter().any(|m| m.name == "datagen"));
    }

    #[test]
    fn test_inventory_category_filter() {
        let inventory = create_connector_inventory();

        let debug_connectors = inventory.by_category(ConnectorCategory::UtilityDebug);
        assert!(debug_connectors.iter().any(|m| m.name == "stdout"));
    }

    #[test]
    fn test_inventory_docs_generation() {
        let inventory = create_connector_inventory();
        let docs = inventory.generate_docs();

        assert!(docs.contains("Connector Catalog"));
        assert!(docs.contains("datagen"));
        assert!(docs.contains("stdout"));
    }
}
