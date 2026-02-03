//! Storage backends for Schema Registry
//!
//! This module provides pluggable storage backends for the schema registry:
//!
//! - **Memory**: In-memory storage for development and testing
//! - **Broker**: Durable storage in rivven topics (recommended for production)
//!
//! For external schema registries (Confluent, AWS Glue), use the `rivven-connect`
//! crate's external registry support instead of these storage backends.

mod memory;

#[cfg(feature = "broker")]
mod broker;

pub use memory::MemoryStorage;

#[cfg(feature = "broker")]
pub use broker::{BrokerStorage, BrokerStorageStats};

use crate::error::SchemaResult;
use crate::types::{Schema, SchemaId, SchemaVersion, Subject, SubjectVersion, VersionState};
use async_trait::async_trait;
use std::sync::Arc;

/// Storage backend trait for schema persistence
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Store a schema and return its ID
    async fn store_schema(&self, schema: Schema) -> SchemaResult<SchemaId>;

    /// Get schema by ID
    async fn get_schema(&self, id: SchemaId) -> SchemaResult<Option<Schema>>;

    /// Get schema by fingerprint (for deduplication)
    async fn get_schema_by_fingerprint(&self, fingerprint: &str) -> SchemaResult<Option<Schema>>;

    /// Register a schema under a subject
    async fn register_subject_version(
        &self,
        subject: &Subject,
        schema_id: SchemaId,
    ) -> SchemaResult<SchemaVersion>;

    /// Get all versions for a subject
    async fn get_versions(&self, subject: &Subject) -> SchemaResult<Vec<u32>>;

    /// Get a specific version of a subject
    async fn get_subject_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaResult<Option<SubjectVersion>>;

    /// Get the latest version of a subject
    async fn get_latest_version(&self, subject: &Subject) -> SchemaResult<Option<SubjectVersion>>;

    /// List all subjects
    async fn list_subjects(&self) -> SchemaResult<Vec<Subject>>;

    /// Delete a subject (soft delete by default)
    async fn delete_subject(&self, subject: &Subject, permanent: bool) -> SchemaResult<Vec<u32>>;

    /// Delete a specific version
    async fn delete_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        permanent: bool,
    ) -> SchemaResult<()>;

    /// Get the next schema ID
    async fn next_schema_id(&self) -> SchemaResult<SchemaId>;

    /// Check if a subject exists
    async fn subject_exists(&self, subject: &Subject) -> SchemaResult<bool>;

    /// Set the state of a version (enabled/deprecated/disabled)
    async fn set_version_state(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        state: VersionState,
    ) -> SchemaResult<()>;

    /// Get the state of a version
    async fn get_version_state(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaResult<VersionState>;

    /// List deleted subjects (for recovery)
    async fn list_deleted_subjects(&self) -> SchemaResult<Vec<Subject>>;

    /// Undelete a soft-deleted subject
    async fn undelete_subject(&self, subject: &Subject) -> SchemaResult<Vec<u32>>;
}

/// Type alias for storage backend
pub type Storage = Arc<dyn StorageBackend>;

/// Create a storage backend from configuration
pub async fn create_storage(config: &crate::config::StorageConfig) -> SchemaResult<Storage> {
    match config {
        crate::config::StorageConfig::Memory => Ok(Arc::new(MemoryStorage::new())),
        #[cfg(feature = "broker")]
        crate::config::StorageConfig::Broker(broker_config) => {
            let storage = BrokerStorage::new(broker_config.clone()).await?;
            Ok(Arc::new(storage))
        }
        #[cfg(not(feature = "broker"))]
        crate::config::StorageConfig::Broker(_) => Err(crate::error::SchemaError::Config(
            "Broker storage requires the 'broker' feature. Enable it in Cargo.toml.".to_string(),
        )),
        crate::config::StorageConfig::Glue { .. } => {
            // AWS Glue is an external registry, not a storage backend
            // Use the glue feature with external registry client instead
            Err(crate::error::SchemaError::Config(
                "AWS Glue is an external registry. Use the `glue` feature with external registry configuration.".to_string()
            ))
        }
    }
}
