//! Embedded Schema Registry backed by Rivven topics
//!
//! Stores schemas in a special `_schemas` topic with the following format:
//! - Key: `{subject}/{version}` or `id/{schema_id}`
//! - Value: Schema definition with metadata

use crate::broker_client::BrokerClient;
use crate::schema::compatibility::{CompatibilityChecker, CompatibilityResult};
use crate::schema::types::*;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Configuration for embedded schema registry
pub type EmbeddedRegistryConfig = EmbeddedConfig;

/// Schema entry stored in the topic
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaEntry {
    /// Subject name
    subject: String,
    /// Version within subject
    version: u32,
    /// Global schema ID
    id: u32,
    /// Schema type
    schema_type: SchemaType,
    /// Schema definition
    schema: String,
    /// References to other schemas
    #[serde(default)]
    references: Vec<SchemaReference>,
    /// Timestamp when registered
    registered_at: i64,
}

/// Subject metadata entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)] // Used for schema storage serialization
struct SubjectEntry {
    /// Subject name
    subject: String,
    /// Compatibility level
    compatibility: CompatibilityLevel,
    /// Versions (list of version numbers)
    versions: Vec<u32>,
}

/// Embedded Schema Registry implementation
pub struct EmbeddedRegistry {
    /// Broker client for topic operations
    broker: Option<Arc<BrokerClient>>,
    /// Topic name for schema storage
    topic: String,
    /// Next schema ID
    next_id: AtomicU32,
    /// Cache: schema_id -> Schema
    schema_cache: RwLock<HashMap<SchemaId, Schema>>,
    /// Cache: subject -> SubjectMetadata
    subject_cache: RwLock<HashMap<Subject, SubjectMetadata>>,
    /// Cache: (subject, version) -> SchemaId
    version_cache: RwLock<HashMap<(Subject, u32), SchemaId>>,
    /// Default compatibility level
    default_compatibility: CompatibilityLevel,
    /// Compatibility checker
    checker: CompatibilityChecker,
}

impl EmbeddedRegistry {
    /// Create a new embedded registry
    pub fn new(config: &EmbeddedRegistryConfig) -> Self {
        Self {
            broker: None,
            topic: config.topic.clone(),
            next_id: AtomicU32::new(1),
            schema_cache: RwLock::new(HashMap::new()),
            subject_cache: RwLock::new(HashMap::new()),
            version_cache: RwLock::new(HashMap::new()),
            default_compatibility: config.compatibility,
            checker: CompatibilityChecker::new(config.compatibility),
        }
    }

    /// Create with a broker client for persistence
    pub fn with_broker(mut self, broker: Arc<BrokerClient>) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Initialize from topic (load existing schemas)
    pub async fn initialize(&mut self) -> SchemaRegistryResult<()> {
        if let Some(ref _broker) = self.broker {
            // TODO: Load existing schemas from topic
            // For now, start fresh
            tracing::info!(topic = %self.topic, "Embedded schema registry initialized");
        }
        Ok(())
    }

    /// Register a new schema
    pub async fn register(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<SchemaId> {
        // Check if schema already exists (same content)
        if let Some(existing_id) = self.find_existing_schema(subject, schema)? {
            return Ok(existing_id);
        }

        // Check compatibility with existing versions
        let existing_schemas = self.get_all_schemas(subject)?;
        let new_schema = Schema::new(SchemaId::new(0), schema_type, schema.to_string());
        
        let compat_result = self.checker.check(&new_schema, &existing_schemas)?;
        if !compat_result.is_compatible {
            return Err(SchemaRegistryError::IncompatibleSchema(
                compat_result.messages.join("; "),
            ));
        }

        // Assign new ID and version
        let id = SchemaId::new(self.next_id.fetch_add(1, Ordering::SeqCst));
        let version = self.get_next_version(subject);

        let schema = Schema::new(id, schema_type, schema.to_string());

        // Update caches
        {
            let mut cache = self.schema_cache.write();
            cache.insert(id, schema.clone());
        }

        {
            let mut cache = self.version_cache.write();
            cache.insert((subject.clone(), version), id);
        }

        {
            let mut cache = self.subject_cache.write();
            let metadata = cache.entry(subject.clone()).or_insert_with(|| SubjectMetadata {
                subject: subject.clone(),
                versions: Vec::new(),
                compatibility: self.default_compatibility,
                id_map: HashMap::new(),
            });
            metadata.versions.push(version);
            metadata.id_map.insert(version, id);
        }

        // Persist to topic if broker is available
        if let Some(ref _broker) = self.broker {
            let entry = SchemaEntry {
                subject: subject.0.clone(),
                version,
                id: id.0,
                schema_type,
                schema: schema.schema.clone(),
                references: schema.references.clone(),
                registered_at: chrono::Utc::now().timestamp(),
            };
            
            let _key = format!("{}/{}", subject.0, version);
            let _value = serde_json::to_vec(&entry)
                .map_err(|e| SchemaRegistryError::SerializationError(e.to_string()))?;

            // TODO: Produce to _schemas topic
            // broker.produce(&self.topic, key.as_bytes(), &value).await?;
        }

        tracing::info!(
            subject = %subject,
            version = version,
            schema_id = %id,
            "Registered new schema"
        );

        Ok(id)
    }

    /// Get schema by ID
    pub fn get_by_id(&self, id: SchemaId) -> SchemaRegistryResult<Schema> {
        let cache = self.schema_cache.read();
        cache
            .get(&id)
            .cloned()
            .ok_or_else(|| SchemaRegistryError::SchemaNotFound(id.to_string()))
    }

    /// Get schema by subject and version
    pub fn get_by_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaRegistryResult<SubjectVersion> {
        let actual_version = if version.is_latest() {
            self.get_latest_version(subject)?
        } else {
            version.0
        };

        let version_cache = self.version_cache.read();
        let schema_id = version_cache
            .get(&(subject.clone(), actual_version))
            .ok_or_else(|| {
                SchemaRegistryError::VersionNotFound(format!(
                    "Subject: {}, Version: {}",
                    subject, version
                ))
            })?;

        let schema_cache = self.schema_cache.read();
        let schema = schema_cache
            .get(schema_id)
            .ok_or_else(|| SchemaRegistryError::SchemaNotFound(schema_id.to_string()))?;

        Ok(SubjectVersion {
            subject: subject.clone(),
            version: SchemaVersion::new(actual_version),
            id: *schema_id,
            schema: schema.schema.clone(),
            schema_type: schema.schema_type,
        })
    }

    /// Get latest version number for a subject
    pub fn get_latest_version(&self, subject: &Subject) -> SchemaRegistryResult<u32> {
        let cache = self.subject_cache.read();
        let metadata = cache
            .get(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        metadata
            .versions
            .last()
            .copied()
            .ok_or_else(|| SchemaRegistryError::VersionNotFound("No versions".to_string()))
    }

    /// List all subjects
    pub fn list_subjects(&self) -> Vec<Subject> {
        let cache = self.subject_cache.read();
        cache.keys().cloned().collect()
    }

    /// List all versions for a subject
    pub fn list_versions(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let cache = self.subject_cache.read();
        let metadata = cache
            .get(subject)
            .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;

        Ok(metadata.versions.clone())
    }

    /// Delete a subject (all versions)
    pub async fn delete_subject(&self, subject: &Subject) -> SchemaRegistryResult<Vec<u32>> {
        let versions = {
            let cache = self.subject_cache.read();
            let metadata = cache
                .get(subject)
                .ok_or_else(|| SchemaRegistryError::SubjectNotFound(subject.to_string()))?;
            metadata.versions.clone()
        };

        // Remove from caches
        {
            let mut cache = self.subject_cache.write();
            cache.remove(subject);
        }

        {
            let mut cache = self.version_cache.write();
            for version in &versions {
                cache.remove(&(subject.clone(), *version));
            }
        }

        // Note: We don't remove from schema_cache because the same schema
        // might be referenced by other subjects

        tracing::info!(subject = %subject, versions = ?versions, "Deleted subject");

        Ok(versions)
    }

    /// Get compatibility level for a subject
    pub fn get_compatibility(&self, subject: &Subject) -> SchemaRegistryResult<CompatibilityLevel> {
        let cache = self.subject_cache.read();
        Ok(cache
            .get(subject)
            .map(|m| m.compatibility)
            .unwrap_or(self.default_compatibility))
    }

    /// Set compatibility level for a subject
    pub fn set_compatibility(
        &self,
        subject: &Subject,
        level: CompatibilityLevel,
    ) -> SchemaRegistryResult<()> {
        let mut cache = self.subject_cache.write();
        if let Some(metadata) = cache.get_mut(subject) {
            metadata.compatibility = level;
        } else {
            cache.insert(
                subject.clone(),
                SubjectMetadata {
                    subject: subject.clone(),
                    versions: Vec::new(),
                    compatibility: level,
                    id_map: HashMap::new(),
                },
            );
        }
        Ok(())
    }

    /// Check compatibility of a new schema
    pub fn check_compatibility(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        let existing = self.get_all_schemas(subject)?;
        let new_schema = Schema::new(SchemaId::new(0), schema_type, schema.to_string());
        self.checker.check(&new_schema, &existing)
    }

    // Private helpers

    fn find_existing_schema(
        &self,
        subject: &Subject,
        schema: &str,
    ) -> SchemaRegistryResult<Option<SchemaId>> {
        let subject_cache = self.subject_cache.read();
        let metadata = match subject_cache.get(subject) {
            Some(m) => m,
            None => return Ok(None),
        };

        let schema_cache = self.schema_cache.read();
        for id in metadata.id_map.values() {
            if let Some(existing) = schema_cache.get(id) {
                // Normalize and compare schemas
                if self.schemas_equal(&existing.schema, schema) {
                    return Ok(Some(*id));
                }
            }
        }

        Ok(None)
    }

    fn schemas_equal(&self, a: &str, b: &str) -> bool {
        // Try to parse as JSON and compare normalized
        match (serde_json::from_str::<serde_json::Value>(a), serde_json::from_str::<serde_json::Value>(b)) {
            (Ok(ja), Ok(jb)) => ja == jb,
            _ => a == b,
        }
    }

    fn get_all_schemas(&self, subject: &Subject) -> SchemaRegistryResult<Vec<Schema>> {
        let subject_cache = self.subject_cache.read();
        let metadata = match subject_cache.get(subject) {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        let schema_cache = self.schema_cache.read();
        let mut schemas = Vec::new();

        for version in &metadata.versions {
            if let Some(id) = metadata.id_map.get(version) {
                if let Some(schema) = schema_cache.get(id) {
                    schemas.push(schema.clone());
                }
            }
        }

        Ok(schemas)
    }

    fn get_next_version(&self, subject: &Subject) -> u32 {
        let cache = self.subject_cache.read();
        cache
            .get(subject)
            .and_then(|m| m.versions.last())
            .map(|v| v + 1)
            .unwrap_or(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> EmbeddedRegistryConfig {
        EmbeddedRegistryConfig {
            topic: "_schemas_test".to_string(),
            cache_ttl_secs: 300,
            compatibility: CompatibilityLevel::Backward,
        }
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let config = test_config();
        let registry = EmbeddedRegistry::new(&config);

        let subject = Subject::new("test-value");
        let schema = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;

        let id = registry.register(&subject, SchemaType::Json, schema).await.unwrap();

        // Get by ID
        let retrieved = registry.get_by_id(id).unwrap();
        assert_eq!(retrieved.schema_type, SchemaType::Json);

        // Get by version
        let sv = registry.get_by_version(&subject, SchemaVersion::latest()).unwrap();
        assert_eq!(sv.id, id);
        assert_eq!(sv.version.0, 1);
    }

    #[tokio::test]
    async fn test_duplicate_schema() {
        let config = test_config();
        let registry = EmbeddedRegistry::new(&config);

        let subject = Subject::new("test-value");
        let schema = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;

        let id1 = registry.register(&subject, SchemaType::Json, schema).await.unwrap();
        let id2 = registry.register(&subject, SchemaType::Json, schema).await.unwrap();

        // Same schema should return same ID
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_compatibility_check() {
        let config = test_config();
        let registry = EmbeddedRegistry::new(&config);

        let subject = Subject::new("test-value");
        
        // Register initial schema
        let schema1 = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
        registry.register(&subject, SchemaType::Json, schema1).await.unwrap();

        // Try to add a new required field (should fail backward compatibility)
        let schema2 = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#;
        let result = registry.register(&subject, SchemaType::Json, schema2).await;
        assert!(result.is_err());

        // Adding optional field should work
        let schema3 = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}"#;
        let result = registry.register(&subject, SchemaType::Json, schema3).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_subjects() {
        let config = test_config();
        let registry = EmbeddedRegistry::new(&config);

        let schema = r#"{"type":"object"}"#;
        registry.register(&Subject::new("a"), SchemaType::Json, schema).await.unwrap();
        registry.register(&Subject::new("b"), SchemaType::Json, schema).await.unwrap();

        let subjects = registry.list_subjects();
        assert_eq!(subjects.len(), 2);
    }
}
