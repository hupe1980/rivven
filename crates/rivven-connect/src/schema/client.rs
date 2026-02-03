//! Unified Schema Registry client
//!
//! Provides a unified interface that works with external (Confluent-compatible) and AWS Glue registries.
//!
//! # Architecture
//!
//! The broker (rivvend) is **schema-agnostic** — it only handles raw bytes.
//! Schema management is handled externally via:
//!
//! - **rivven-schema**: Standalone Confluent-compatible Schema Registry
//! - **Confluent Schema Registry**: External Kafka-compatible registry  
//! - **AWS Glue Schema Registry**: AWS-native schema management

use crate::schema::external::ExternalRegistry;
use crate::schema::glue::GlueRegistry;
use crate::schema::types::*;
use rivven_schema::CompatibilityResult;

/// Unified Schema Registry client
pub enum SchemaRegistryClient {
    /// External registry (Confluent-compatible, including rivven-schema)
    External(ExternalRegistry),
    /// AWS Glue Schema Registry
    Glue(GlueRegistry),
    /// Disabled (no-op)
    Disabled,
}

impl SchemaRegistryClient {
    /// Create from configuration
    pub fn from_config(config: &SchemaRegistryConfig) -> SchemaRegistryResult<Self> {
        match config {
            SchemaRegistryConfig::External(external_config) => {
                let registry = ExternalRegistry::new(external_config)?;
                Ok(SchemaRegistryClient::External(registry))
            }
            SchemaRegistryConfig::Glue(_glue_config) => {
                // Note: GlueRegistry::new is async, so we need a different approach
                // For now, return an error suggesting async initialization
                Err(SchemaRegistryError::ConfigError(
                    "AWS Glue registry requires async initialization. Use SchemaRegistryClient::from_config_async instead.".into()
                ))
            }
            SchemaRegistryConfig::Disabled => Ok(SchemaRegistryClient::Disabled),
        }
    }

    /// Create from configuration (async version for Glue support)
    pub async fn from_config_async(config: &SchemaRegistryConfig) -> SchemaRegistryResult<Self> {
        match config {
            SchemaRegistryConfig::External(external_config) => {
                let registry = ExternalRegistry::new(external_config)?;
                Ok(SchemaRegistryClient::External(registry))
            }
            SchemaRegistryConfig::Glue(glue_config) => {
                let registry = GlueRegistry::new(glue_config).await?;
                Ok(SchemaRegistryClient::Glue(registry))
            }
            SchemaRegistryConfig::Disabled => Ok(SchemaRegistryClient::Disabled),
        }
    }

    /// Check if registry is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, SchemaRegistryClient::Disabled)
    }

    /// Register a new schema
    pub async fn register(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        match self {
            SchemaRegistryClient::External(registry) => {
                registry.register(subject, schema_type, schema).await
            }
            SchemaRegistryClient::Glue(registry) => {
                registry.register(subject, schema_type, schema).await
            }
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Get schema by ID
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        match self {
            SchemaRegistryClient::External(registry) => registry.get_by_id(id).await,
            SchemaRegistryClient::Glue(registry) => registry.get_by_id(id).await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Get schema by subject and version
    pub async fn get_by_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaRegistryResult<SubjectVersion> {
        match self {
            SchemaRegistryClient::External(registry) => {
                registry.get_by_version(subject, version).await
            }
            SchemaRegistryClient::Glue(registry) => registry.get_by_version(subject, version).await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// List all subjects
    pub async fn list_subjects(&self) -> SchemaRegistryResult<Vec<Subject>> {
        match self {
            SchemaRegistryClient::External(registry) => registry.list_subjects().await,
            SchemaRegistryClient::Glue(registry) => registry.list_subjects().await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// List versions for a subject
    pub async fn list_versions(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        match self {
            SchemaRegistryClient::External(registry) => registry.list_versions(subject).await,
            SchemaRegistryClient::Glue(registry) => registry.list_versions(subject).await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Delete a subject
    pub async fn delete_subject(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        match self {
            SchemaRegistryClient::External(registry) => registry.delete_subject(subject).await,
            SchemaRegistryClient::Glue(registry) => registry.delete_subject(subject).await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Get compatibility level
    pub async fn get_compatibility(
        &self,
        subject: &Subject,
    ) -> SchemaRegistryResult<CompatibilityLevel> {
        match self {
            SchemaRegistryClient::External(registry) => registry.get_compatibility(subject).await,
            SchemaRegistryClient::Glue(registry) => registry.get_compatibility(subject).await,
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Set compatibility level
    pub async fn set_compatibility(
        &self,
        subject: &Subject,
        level: CompatibilityLevel,
    ) -> SchemaRegistryResult<()> {
        match self {
            SchemaRegistryClient::External(registry) => {
                registry.set_compatibility(subject, level).await
            }
            SchemaRegistryClient::Glue(registry) => {
                registry.set_compatibility(subject, level).await
            }
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }

    /// Check compatibility of a new schema
    pub async fn check_compatibility(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        match self {
            SchemaRegistryClient::External(registry) => {
                registry
                    .check_compatibility(subject, schema_type, schema, None)
                    .await
            }
            SchemaRegistryClient::Glue(registry) => {
                registry
                    .check_compatibility(subject, schema_type, schema)
                    .await
            }
            SchemaRegistryClient::Disabled => Err(SchemaRegistryError::Disabled),
        }
    }
}

/// High-level Schema Registry facade
pub struct SchemaRegistry {
    client: SchemaRegistryClient,
}

impl SchemaRegistry {
    /// Create a new schema registry
    pub fn new(config: SchemaRegistryConfig) -> SchemaRegistryResult<Self> {
        let client = SchemaRegistryClient::from_config(&config)?;
        Ok(Self { client })
    }

    /// Check if enabled
    pub fn is_enabled(&self) -> bool {
        self.client.is_enabled()
    }

    /// Register a JSON schema for a topic's value
    pub async fn register_value_schema(
        &self,
        topic: &str,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        let subject = Subject::value(topic);
        self.client
            .register(&subject, SchemaType::Json, schema)
            .await
    }

    /// Register a JSON schema for a topic's key
    pub async fn register_key_schema(
        &self,
        topic: &str,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        let subject = Subject::key(topic);
        self.client
            .register(&subject, SchemaType::Json, schema)
            .await
    }

    /// Get the latest value schema for a topic
    pub async fn get_value_schema(&self, topic: &str) -> SchemaRegistryResult<SubjectVersion> {
        let subject = Subject::value(topic);
        self.client
            .get_by_version(&subject, SchemaVersion::latest())
            .await
    }

    /// Get the latest key schema for a topic
    pub async fn get_key_schema(&self, topic: &str) -> SchemaRegistryResult<SubjectVersion> {
        let subject = Subject::key(topic);
        self.client
            .get_by_version(&subject, SchemaVersion::latest())
            .await
    }

    /// Register a schema with auto-detected type
    pub async fn register(
        &self,
        subject: impl Into<Subject>,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        self.client
            .register(&subject.into(), schema_type, schema)
            .await
    }

    /// Get schema by ID
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        self.client.get_by_id(id).await
    }

    /// List all subjects
    pub async fn list_subjects(&self) -> SchemaRegistryResult<Vec<Subject>> {
        self.client.list_subjects().await
    }

    /// Get underlying client for advanced operations
    pub fn client(&self) -> &SchemaRegistryClient {
        &self.client
    }
}
