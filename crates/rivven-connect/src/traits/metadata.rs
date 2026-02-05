//! Connector metadata and categorization for scalable connector discovery
//!
//! This module provides a comprehensive metadata system that enables:
//! - Categorization of connectors for scalable connector management
//! - Rich tagging and search capabilities
//! - Auto-generated documentation
//!
//! # Architecture for 300+ Connectors
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                     Connector Inventory System                           │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │  Categories (hierarchical)                                               │
//! │  ├── Database                                                            │
//! │  │   ├── CDC (postgres_cdc, mysql_cdc, mongodb_cdc, ...)                │
//! │  │   ├── Batch (sql_select, sql_insert, ...)                            │
//! │  │   └── NoSQL (mongodb, cassandra, redis, ...)                         │
//! │  ├── Messaging                                                           │
//! │  │   ├── Kafka (kafka, kafka_franz, redpanda, ...)                      │
//! │  │   ├── MQTT (mqtt, rabbitmq, nats, ...)                               │
//! │  │   └── Cloud (sqs, pubsub, azure_queue, ...)                          │
//! │  ├── Storage                                                             │
//! │  │   ├── Object (s3, gcs, azure_blob, minio, ...)                       │
//! │  │   └── File (file, sftp, hdfs, ...)                                   │
//! │  ├── Warehouse                                                           │
//! │  │   └── (snowflake, bigquery, redshift, clickhouse, ...)               │
//! │  ├── AI/ML                                                               │
//! │  │   ├── LLM (openai, anthropic, ollama, bedrock, ...)                  │
//! │  │   └── Vector (pinecone, qdrant, weaviate, ...)                       │
//! │  └── Utility                                                             │
//! │      ├── Generate (datagen, generate, ...)                              │
//! │      └── Debug (stdout, log, drop, ...)                                 │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Connector category for organization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorCategory {
    // Database connectors
    /// Change Data Capture (CDC) sources
    DatabaseCdc,
    /// Batch database operations
    DatabaseBatch,
    /// Query-based database polling (RDBC-style)
    DatabaseQuery,
    /// NoSQL databases
    DatabaseNosql,

    // Messaging connectors
    /// Apache Kafka and compatible systems
    MessagingKafka,
    /// MQTT and lightweight messaging
    MessagingMqtt,
    /// Cloud message queues (SQS, Pub/Sub, etc.)
    MessagingCloud,

    // Storage connectors
    /// Object storage (S3, GCS, Azure Blob)
    StorageObject,
    /// File systems and protocols
    StorageFile,

    // Data warehouse connectors
    /// Cloud data warehouses
    Warehouse,

    // Lakehouse connectors
    /// Open table formats (Iceberg, Delta Lake, Hudi)
    Lakehouse,

    // AI/ML connectors
    /// Large Language Models
    AiLlm,
    /// Vector databases
    AiVector,

    // HTTP/API connectors
    /// HTTP clients and webhooks
    HttpApi,

    // Utility connectors
    /// Data generation for testing
    UtilityGenerate,
    /// Debugging and development
    UtilityDebug,
    /// Data transformation
    Transform,
}

impl ConnectorCategory {
    /// Get the parent category (for hierarchical display)
    pub fn parent(&self) -> Option<&'static str> {
        match self {
            Self::DatabaseCdc | Self::DatabaseBatch | Self::DatabaseQuery | Self::DatabaseNosql => {
                Some("database")
            }
            Self::MessagingKafka | Self::MessagingMqtt | Self::MessagingCloud => Some("messaging"),
            Self::StorageObject | Self::StorageFile => Some("storage"),
            Self::Warehouse => Some("warehouse"),
            Self::Lakehouse => Some("lakehouse"),
            Self::AiLlm | Self::AiVector => Some("ai"),
            Self::HttpApi => Some("http"),
            Self::UtilityGenerate | Self::UtilityDebug => Some("utility"),
            Self::Transform => Some("transform"),
        }
    }

    /// Human-readable display name
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::DatabaseCdc => "Database CDC",
            Self::DatabaseBatch => "Database Batch",
            Self::DatabaseQuery => "Database Query (RDBC)",
            Self::DatabaseNosql => "NoSQL Databases",
            Self::MessagingKafka => "Kafka & Compatible",
            Self::MessagingMqtt => "MQTT & Lightweight",
            Self::MessagingCloud => "Cloud Queues",
            Self::StorageObject => "Object Storage",
            Self::StorageFile => "File Systems",
            Self::Warehouse => "Data Warehouses",
            Self::Lakehouse => "Lakehouse Formats",
            Self::AiLlm => "LLM Providers",
            Self::AiVector => "Vector Databases",
            Self::HttpApi => "HTTP & APIs",
            Self::UtilityGenerate => "Data Generation",
            Self::UtilityDebug => "Debug & Testing",
            Self::Transform => "Transforms",
        }
    }

    /// Get all categories
    pub fn all() -> &'static [Self] {
        &[
            Self::DatabaseCdc,
            Self::DatabaseBatch,
            Self::DatabaseNosql,
            Self::MessagingKafka,
            Self::MessagingMqtt,
            Self::MessagingCloud,
            Self::StorageObject,
            Self::StorageFile,
            Self::Warehouse,
            Self::Lakehouse,
            Self::AiLlm,
            Self::AiVector,
            Self::HttpApi,
            Self::UtilityGenerate,
            Self::UtilityDebug,
            Self::Transform,
        ]
    }
}

impl std::fmt::Display for ConnectorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// Connector type flags
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    /// Produces events
    Source,
    /// Consumes events
    Sink,
    /// Transforms events
    Processor,
    /// Caches data
    Cache,
    /// Scans/parses data formats
    Scanner,
}

/// Rich metadata for connector discovery and documentation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConnectorMetadata {
    /// Unique connector identifier (e.g., "postgres_cdc", "s3")
    pub name: String,

    /// Human-readable title
    pub title: String,

    /// Short description (one line)
    pub description: String,

    /// Long description with examples (markdown)
    #[serde(default)]
    pub long_description: Option<String>,

    /// Connector types (source, sink, processor, etc.)
    pub types: HashSet<ConnectorType>,

    /// Primary category
    pub category: ConnectorCategory,

    /// Feature flag required (if any)
    #[serde(default)]
    pub feature: Option<String>,

    /// Required dependencies description
    #[serde(default)]
    pub dependencies: Vec<String>,

    /// Search tags for discovery
    #[serde(default)]
    pub tags: HashSet<String>,

    /// Alternative names for search (e.g., "AWS S3", "Amazon S3", "Simple Storage Service")
    #[serde(default)]
    pub aliases: HashSet<String>,

    /// Related connectors
    #[serde(default)]
    pub related: Vec<String>,

    /// Available in cloud/SaaS version
    #[serde(default)]
    pub available_in_cloud: bool,

    /// Requires enterprise license
    #[serde(default)]
    pub requires_enterprise: bool,

    /// Minimum version required
    #[serde(default)]
    pub min_version: Option<String>,

    /// Deprecated since version
    #[serde(default)]
    pub deprecated_since: Option<String>,

    /// Replacement connector (if deprecated)
    #[serde(default)]
    pub replaced_by: Option<String>,
}

impl ConnectorMetadata {
    /// Create new metadata builder
    pub fn builder(name: impl Into<String>) -> ConnectorMetadataBuilder {
        ConnectorMetadataBuilder::new(name)
    }

    /// Check if metadata matches a search query
    pub fn matches_search(&self, query: &str) -> bool {
        let query = query.to_lowercase();

        // Check name
        if self.name.to_lowercase().contains(&query) {
            return true;
        }

        // Check title
        if self.title.to_lowercase().contains(&query) {
            return true;
        }

        // Check description
        if self.description.to_lowercase().contains(&query) {
            return true;
        }

        // Check tags
        if self.tags.iter().any(|t| t.to_lowercase().contains(&query)) {
            return true;
        }

        // Check aliases
        if self
            .aliases
            .iter()
            .any(|a| a.to_lowercase().contains(&query))
        {
            return true;
        }

        false
    }

    /// Check if metadata matches filters
    pub fn matches_filters(
        &self,
        category: Option<ConnectorCategory>,
        connector_type: Option<ConnectorType>,
    ) -> bool {
        if let Some(cat) = category {
            if self.category != cat {
                return false;
            }
        }

        if let Some(typ) = connector_type {
            if !self.types.contains(&typ) {
                return false;
            }
        }

        true
    }
}

/// Builder for ConnectorMetadata
#[derive(Debug)]
pub struct ConnectorMetadataBuilder {
    name: String,
    title: Option<String>,
    description: Option<String>,
    long_description: Option<String>,
    types: HashSet<ConnectorType>,
    category: Option<ConnectorCategory>,
    feature: Option<String>,
    dependencies: Vec<String>,
    tags: HashSet<String>,
    aliases: HashSet<String>,
    related: Vec<String>,
    available_in_cloud: bool,
    requires_enterprise: bool,
    min_version: Option<String>,
    deprecated_since: Option<String>,
    replaced_by: Option<String>,
}

impl ConnectorMetadataBuilder {
    /// Create a new builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            title: None,
            description: None,
            long_description: None,
            types: HashSet::new(),
            category: None,
            feature: None,
            dependencies: Vec::new(),
            tags: HashSet::new(),
            aliases: HashSet::new(),
            related: Vec::new(),
            available_in_cloud: false,
            requires_enterprise: false,
            min_version: None,
            deprecated_since: None,
            replaced_by: None,
        }
    }

    /// Set title
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Set description
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set long description (markdown)
    pub fn long_description(mut self, desc: impl Into<String>) -> Self {
        self.long_description = Some(desc.into());
        self
    }

    /// Add connector type
    pub fn connector_type(mut self, typ: ConnectorType) -> Self {
        self.types.insert(typ);
        self
    }

    /// Mark as source
    pub fn source(self) -> Self {
        self.connector_type(ConnectorType::Source)
    }

    /// Mark as sink
    pub fn sink(self) -> Self {
        self.connector_type(ConnectorType::Sink)
    }

    /// Mark as processor
    pub fn processor(self) -> Self {
        self.connector_type(ConnectorType::Processor)
    }

    /// Set category
    pub fn category(mut self, category: ConnectorCategory) -> Self {
        self.category = Some(category);
        self
    }

    /// Set feature flag
    pub fn feature(mut self, feature: impl Into<String>) -> Self {
        self.feature = Some(feature.into());
        self
    }

    /// Add dependency
    pub fn dependency(mut self, dep: impl Into<String>) -> Self {
        self.dependencies.push(dep.into());
        self
    }

    /// Add tag
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    /// Add multiple tags
    pub fn tags<I, S>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for tag in tags {
            self.tags.insert(tag.into());
        }
        self
    }

    /// Add alias
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.aliases.insert(alias.into());
        self
    }

    /// Add multiple aliases
    pub fn aliases<I, S>(mut self, aliases: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for alias in aliases {
            self.aliases.insert(alias.into());
        }
        self
    }

    /// Add related connector
    pub fn related(mut self, connector: impl Into<String>) -> Self {
        self.related.push(connector.into());
        self
    }

    /// Mark as available in cloud
    pub fn available_in_cloud(mut self) -> Self {
        self.available_in_cloud = true;
        self
    }

    /// Mark as requiring enterprise
    pub fn requires_enterprise(mut self) -> Self {
        self.requires_enterprise = true;
        self
    }

    /// Set minimum version
    pub fn min_version(mut self, version: impl Into<String>) -> Self {
        self.min_version = Some(version.into());
        self
    }

    /// Mark as deprecated
    pub fn deprecated(mut self, since: impl Into<String>, replaced_by: Option<String>) -> Self {
        self.deprecated_since = Some(since.into());
        self.replaced_by = replaced_by;
        self
    }

    /// Build the metadata
    pub fn build(self) -> ConnectorMetadata {
        ConnectorMetadata {
            name: self.name.clone(),
            title: self
                .title
                .unwrap_or_else(|| self.name.replace('_', " ").to_uppercase()),
            description: self
                .description
                .unwrap_or_else(|| format!("{} connector", self.name)),
            long_description: self.long_description,
            types: self.types,
            category: self.category.unwrap_or(ConnectorCategory::UtilityDebug),
            feature: self.feature,
            dependencies: self.dependencies,
            tags: self.tags,
            aliases: self.aliases,
            related: self.related,
            available_in_cloud: self.available_in_cloud,
            requires_enterprise: self.requires_enterprise,
            min_version: self.min_version,
            deprecated_since: self.deprecated_since,
            replaced_by: self.replaced_by,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_builder() {
        let meta = ConnectorMetadata::builder("postgres_cdc")
            .title("PostgreSQL CDC")
            .description("Change Data Capture from PostgreSQL using logical replication")
            .source()
            .category(ConnectorCategory::DatabaseCdc)
            .feature("postgres")
            .tags(["postgresql", "cdc", "replication", "logical"])
            .aliases(["pg_cdc", "postgres-cdc"])
            .related("mysql_cdc")
            .build();

        assert_eq!(meta.name, "postgres_cdc");
        assert!(meta.types.contains(&ConnectorType::Source));
        assert_eq!(meta.category, ConnectorCategory::DatabaseCdc);
    }

    #[test]
    fn test_metadata_search() {
        let meta = ConnectorMetadata::builder("s3")
            .title("Amazon S3")
            .description("AWS S3 object storage sink")
            .tags(["aws", "s3", "storage", "object"])
            .aliases(["AWS S3", "Amazon S3", "Simple Storage Service"])
            .build();

        assert!(meta.matches_search("s3"));
        assert!(meta.matches_search("amazon"));
        assert!(meta.matches_search("AWS"));
        assert!(meta.matches_search("storage"));
        assert!(!meta.matches_search("azure"));
    }

    #[test]
    fn test_metadata_filters() {
        let meta = ConnectorMetadata::builder("kafka")
            .source()
            .sink()
            .category(ConnectorCategory::MessagingKafka)
            .build();

        assert!(meta.matches_filters(None, None));
        assert!(meta.matches_filters(Some(ConnectorCategory::MessagingKafka), None));
        assert!(meta.matches_filters(None, Some(ConnectorType::Source)));
        assert!(!meta.matches_filters(Some(ConnectorCategory::DatabaseCdc), None));
    }

    #[test]
    fn test_category_hierarchy() {
        assert_eq!(ConnectorCategory::DatabaseCdc.parent(), Some("database"));
        assert_eq!(
            ConnectorCategory::MessagingKafka.parent(),
            Some("messaging")
        );
        assert_eq!(ConnectorCategory::Warehouse.parent(), Some("warehouse"));
    }
}
