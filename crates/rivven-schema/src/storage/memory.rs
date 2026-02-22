//! In-memory storage backend for testing and development

use super::StorageBackend;
use crate::error::{SchemaError, SchemaResult};
use crate::types::{
    Schema, SchemaId, SchemaType, SchemaVersion, Subject, SubjectVersion, VersionState,
};
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// In-memory storage backend
pub struct MemoryStorage {
    /// Schema ID counter
    next_id: AtomicU32,
    /// Schemas by ID
    schemas: DashMap<u32, Schema>,
    /// Schema fingerprint -> ID mapping (for deduplication)
    fingerprints: DashMap<String, u32>,
    /// Subject -> versions mapping
    subjects: DashMap<String, Vec<SubjectVersionEntry>>,
    /// Deleted subjects (soft delete) - used for undelete feature
    deleted_subjects: DashMap<String, Vec<SubjectVersionEntry>>,
}

#[derive(Clone)]
struct SubjectVersionEntry {
    version: u32,
    schema_id: u32,
    schema_type: SchemaType,
    deleted: bool,
    state: VersionState,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU32::new(1),
            schemas: DashMap::new(),
            fingerprints: DashMap::new(),
            subjects: DashMap::new(),
            deleted_subjects: DashMap::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn store_schema(&self, schema: Schema) -> SchemaResult<SchemaId> {
        let id = schema.id.0;
        self.schemas.insert(id, schema.clone());

        if let Some(ref fp) = schema.fingerprint {
            self.fingerprints.insert(fp.clone(), id);
        }

        Ok(schema.id)
    }

    async fn get_schema(&self, id: SchemaId) -> SchemaResult<Option<Schema>> {
        Ok(self.schemas.get(&id.0).map(|s| s.clone()))
    }

    async fn get_schema_by_fingerprint(&self, fingerprint: &str) -> SchemaResult<Option<Schema>> {
        if let Some(id) = self.fingerprints.get(fingerprint) {
            self.get_schema(SchemaId::new(*id)).await
        } else {
            Ok(None)
        }
    }

    async fn register_subject_version(
        &self,
        subject: &Subject,
        schema_id: SchemaId,
    ) -> SchemaResult<SchemaVersion> {
        let schema = self
            .get_schema(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("Schema ID {}", schema_id)))?;

        let mut entry = self.subjects.entry(subject.0.clone()).or_default();
        let version = entry.len() as u32 + 1;

        entry.push(SubjectVersionEntry {
            version,
            schema_id: schema_id.0,
            schema_type: schema.schema_type,
            deleted: false,
            state: VersionState::Enabled,
        });

        Ok(SchemaVersion::new(version))
    }

    async fn get_versions(&self, subject: &Subject) -> SchemaResult<Vec<u32>> {
        let entry = self.subjects.get(&subject.0);
        match entry {
            Some(versions) => Ok(versions
                .iter()
                .filter(|v| !v.deleted)
                .map(|v| v.version)
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    async fn get_subject_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaResult<Option<SubjectVersion>> {
        let entry = self.subjects.get(&subject.0);
        match entry {
            Some(versions) => {
                let v = versions
                    .iter()
                    .find(|v| v.version == version.0 && !v.deleted);

                match v {
                    Some(entry) => {
                        let schema = self
                            .get_schema(SchemaId::new(entry.schema_id))
                            .await?
                            .ok_or_else(|| {
                                SchemaError::NotFound(format!("Schema ID {}", entry.schema_id))
                            })?;

                        Ok(Some(SubjectVersion {
                            subject: subject.clone(),
                            version: SchemaVersion::new(entry.version),
                            id: SchemaId::new(entry.schema_id),
                            schema_type: entry.schema_type,
                            schema: schema.schema,
                            state: entry.state,
                        }))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    async fn get_latest_version(&self, subject: &Subject) -> SchemaResult<Option<SubjectVersion>> {
        let entry = self.subjects.get(&subject.0);
        match entry {
            Some(versions) => {
                let latest = versions.iter().rev().find(|v| !v.deleted);

                match latest {
                    Some(entry) => {
                        let schema = self
                            .get_schema(SchemaId::new(entry.schema_id))
                            .await?
                            .ok_or_else(|| {
                                SchemaError::NotFound(format!("Schema ID {}", entry.schema_id))
                            })?;

                        Ok(Some(SubjectVersion {
                            subject: subject.clone(),
                            version: SchemaVersion::new(entry.version),
                            id: SchemaId::new(entry.schema_id),
                            schema_type: entry.schema_type,
                            schema: schema.schema,
                            state: entry.state,
                        }))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    async fn list_subjects(&self) -> SchemaResult<Vec<Subject>> {
        Ok(self
            .subjects
            .iter()
            .filter(|e| e.iter().any(|v| !v.deleted))
            .map(|e| Subject::new(e.key().clone()))
            .collect())
    }

    async fn delete_subject(&self, subject: &Subject, permanent: bool) -> SchemaResult<Vec<u32>> {
        if permanent {
            // Remove from deleted_subjects too if it was soft-deleted before
            self.deleted_subjects.remove(&subject.0);
            if let Some((_, versions)) = self.subjects.remove(&subject.0) {
                Ok(versions.iter().map(|v| v.version).collect())
            } else {
                Ok(Vec::new())
            }
        } else {
            // Soft delete - move to deleted_subjects for recovery
            if let Some((key, versions)) = self.subjects.remove(&subject.0) {
                let version_nums: Vec<u32> = versions.iter().map(|v| v.version).collect();
                // Store in deleted_subjects for potential recovery
                self.deleted_subjects.insert(key, versions);
                Ok(version_nums)
            } else {
                Ok(Vec::new())
            }
        }
    }

    async fn delete_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        permanent: bool,
    ) -> SchemaResult<()> {
        if let Some(mut entry) = self.subjects.get_mut(&subject.0) {
            if permanent {
                entry.retain(|v| v.version != version.0);
            } else if let Some(v) = entry.iter_mut().find(|v| v.version == version.0) {
                v.deleted = true;
            }
        }
        Ok(())
    }

    async fn next_schema_id(&self) -> SchemaResult<SchemaId> {
        // Use fetch_update with checked_add to detect overflow at u32::MAX
        // instead of silently wrapping to 0 (which would produce duplicate IDs).
        let id = self
            .next_id
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_add(1))
            .map_err(|_| {
                SchemaError::Internal("Schema ID space exhausted (u32::MAX reached)".into())
            })?;
        Ok(SchemaId::new(id))
    }

    async fn subject_exists(&self, subject: &Subject) -> SchemaResult<bool> {
        Ok(self.subjects.contains_key(&subject.0))
    }

    async fn set_version_state(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        state: VersionState,
    ) -> SchemaResult<()> {
        if let Some(mut entry) = self.subjects.get_mut(&subject.0) {
            if let Some(v) = entry
                .iter_mut()
                .find(|v| v.version == version.0 && !v.deleted)
            {
                v.state = state;
                return Ok(());
            }
        }
        Err(SchemaError::NotFound(format!(
            "Version {} not found for subject {}",
            version, subject
        )))
    }

    async fn get_version_state(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaResult<VersionState> {
        if let Some(entry) = self.subjects.get(&subject.0) {
            if let Some(v) = entry.iter().find(|v| v.version == version.0 && !v.deleted) {
                return Ok(v.state);
            }
        }
        Err(SchemaError::NotFound(format!(
            "Version {} not found for subject {}",
            version, subject
        )))
    }

    async fn list_deleted_subjects(&self) -> SchemaResult<Vec<Subject>> {
        Ok(self
            .deleted_subjects
            .iter()
            .map(|e| Subject::new(e.key().clone()))
            .collect())
    }

    async fn undelete_subject(&self, subject: &Subject) -> SchemaResult<Vec<u32>> {
        if let Some((key, versions)) = self.deleted_subjects.remove(&subject.0) {
            // Prevent collision: if a new subject was registered with the same
            // name after the soft-delete, restoring would overwrite it.
            if self.subjects.contains_key(&key) {
                // Put back into deleted_subjects so the entry isn't lost
                self.deleted_subjects.insert(key, versions);
                return Err(SchemaError::AlreadyExists(format!(
                    "Cannot undelete subject '{}': a subject with the same name already exists",
                    subject
                )));
            }
            let version_nums: Vec<u32> = versions.iter().map(|v| v.version).collect();
            // Restore to active subjects
            self.subjects.insert(key, versions);
            Ok(version_nums)
        } else {
            Err(SchemaError::NotFound(format!(
                "Subject '{}' not found in deleted subjects",
                subject
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_storage_basic() {
        let storage = MemoryStorage::new();

        // Store a schema
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#.to_string());
        storage.store_schema(schema.clone()).await.unwrap();

        // Retrieve it
        let retrieved = storage.get_schema(id).await.unwrap().unwrap();
        assert_eq!(retrieved.schema, schema.schema);
    }

    #[tokio::test]
    async fn test_memory_storage_subject_versions() {
        let storage = MemoryStorage::new();
        let subject = Subject::new("test-subject");

        // Store schema v1
        let id1 = storage.next_schema_id().await.unwrap();
        let schema1 = Schema::new(id1, SchemaType::Avro, r#"{"type":"string"}"#.to_string());
        storage.store_schema(schema1).await.unwrap();
        let v1 = storage
            .register_subject_version(&subject, id1)
            .await
            .unwrap();
        assert_eq!(v1.0, 1);

        // Store schema v2
        let id2 = storage.next_schema_id().await.unwrap();
        let schema2 = Schema::new(id2, SchemaType::Avro, r#"{"type":"int"}"#.to_string());
        storage.store_schema(schema2).await.unwrap();
        let v2 = storage
            .register_subject_version(&subject, id2)
            .await
            .unwrap();
        assert_eq!(v2.0, 2);

        // Check versions
        let versions = storage.get_versions(&subject).await.unwrap();
        assert_eq!(versions, vec![1, 2]);

        // Get latest
        let latest = storage.get_latest_version(&subject).await.unwrap().unwrap();
        assert_eq!(latest.version.0, 2);
    }

    #[tokio::test]
    async fn test_memory_storage_soft_delete() {
        let storage = MemoryStorage::new();
        let subject = Subject::new("test-subject");

        // Store and register
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#.to_string());
        storage.store_schema(schema).await.unwrap();
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        // Soft delete
        storage.delete_subject(&subject, false).await.unwrap();

        // Subject should appear deleted
        let subjects = storage.list_subjects().await.unwrap();
        assert!(subjects.is_empty());

        // Should be in deleted subjects list
        let deleted = storage.list_deleted_subjects().await.unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].0, "test-subject");
    }

    #[tokio::test]
    async fn test_memory_storage_undelete() {
        let storage = MemoryStorage::new();
        let subject = Subject::new("test-subject");

        // Store and register
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#.to_string());
        storage.store_schema(schema).await.unwrap();
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        // Soft delete
        let deleted_versions = storage.delete_subject(&subject, false).await.unwrap();
        assert_eq!(deleted_versions, vec![1]);

        // Verify deleted
        let subjects = storage.list_subjects().await.unwrap();
        assert!(subjects.is_empty());

        // Undelete
        let restored_versions = storage.undelete_subject(&subject).await.unwrap();
        assert_eq!(restored_versions, vec![1]);

        // Subject should be back
        let subjects = storage.list_subjects().await.unwrap();
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].0, "test-subject");

        // Deleted list should be empty
        let deleted = storage.list_deleted_subjects().await.unwrap();
        assert!(deleted.is_empty());

        // Version should be accessible
        let version = storage
            .get_subject_version(&subject, SchemaVersion::new(1))
            .await
            .unwrap();
        assert!(version.is_some());
    }

    #[tokio::test]
    async fn test_memory_storage_undelete_not_found() {
        let storage = MemoryStorage::new();
        let subject = Subject::new("nonexistent");

        // Try to undelete non-existent subject
        let result = storage.undelete_subject(&subject).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_storage_permanent_delete_prevents_undelete() {
        let storage = MemoryStorage::new();
        let subject = Subject::new("test-subject");

        // Store and register
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#.to_string());
        storage.store_schema(schema).await.unwrap();
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        // Permanent delete
        storage.delete_subject(&subject, true).await.unwrap();

        // Should not be in deleted subjects
        let deleted = storage.list_deleted_subjects().await.unwrap();
        assert!(deleted.is_empty());

        // Undelete should fail
        let result = storage.undelete_subject(&subject).await;
        assert!(result.is_err());
    }
}
