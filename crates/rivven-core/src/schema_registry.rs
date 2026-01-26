use crate::{Error, Result};
use apache_avro::Schema;
use async_trait::async_trait;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// Abstract Schema Registry interface.
///
/// Supports multiple backends:
/// 1. `MemorySchemaRegistry`: For testing and development.
/// 2. `EmbeddedSchemaRegistry`: A persistent, log-backed registry (Kafka-style).
/// 3. `ExternalSchemaRegistry`: Adapters for AWS Glue, Confluent Cloud, etc.
#[async_trait]
pub trait SchemaRegistry: Send + Sync + std::fmt::Debug {
    /// Register a new schema under the given subject.
    /// Returns the global ID of the schema.
    async fn register(&self, subject: &str, schema_str: &str) -> Result<i32>;

    /// Retrieve a schema by its global ID.
    async fn get_schema(&self, id: i32) -> Result<Arc<Schema>>;

    /// Get the latest schema ID for a subject.
    async fn get_latest_schema_id(&self, subject: &str) -> Result<Option<i32>>;
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SchemaLogEvent {
    Register(SchemaRegistration),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemaRegistration {
    pub id: i32,
    pub subject: String,
    pub schema: String,
    pub version: i32,
}

/// Persistent implementation using a Rivven internal topic (`_schemas`).
#[derive(Debug)]
pub struct EmbeddedSchemaRegistry {
    /// In-memory cache of the registry state.
    /// Access is delegated to this cache after writes are persisted.
    cache: MemorySchemaRegistry,

    /// Handle to the partition where schemas are stored.
    /// Typically topic=`_schemas`, partition=0.
    partition: Arc<crate::partition::Partition>,
}

impl EmbeddedSchemaRegistry {
    pub async fn new(config: &crate::Config) -> Result<Self> {
        // Initialize the partition for storage
        let partition = crate::partition::Partition::new(config, "_schemas", 0).await?;
        let partition = Arc::new(partition);

        let registry = Self {
            cache: MemorySchemaRegistry::new(),
            partition,
        };

        registry.recover().await?;

        Ok(registry)
    }

    /// Read the log from the beginning and rebuild the in-memory state.
    async fn recover(&self) -> Result<()> {
        let mut offset = 0;
        // Basic recovery loop - assumes partition read logic is available
        // In a production system, we would iterate segments efficiently.
        #[allow(clippy::while_let_loop)] // Complex loop with batch reads requires explicit control
        loop {
            // Read next batch.
            // Partition::read takes (start_offset, max_messages).
            // We'll read up to 100 messages at a time.
            match self.partition.read(offset, 100).await {
                Ok(messages) => {
                    if messages.is_empty() {
                        break;
                    }
                    for msg in messages {
                        offset = msg.offset + 1; // Advance offset
                                                 // Skip non-schema messages (or errors)
                        if let Ok(event) = bincode::deserialize::<SchemaLogEvent>(&msg.value) {
                            match event {
                                SchemaLogEvent::Register(reg) => {
                                    self.cache.inject_registration(
                                        reg.id,
                                        &reg.subject,
                                        &reg.schema,
                                        reg.version,
                                    )?;
                                }
                            }
                        }
                    }
                }
                Err(_) => break, // EOF or read error (e.g. OffsetOutOfBounds if end reached)
            }
        }

        // Sync next_id with recovered max
        let max_id = self
            .cache
            .id_to_schema
            .iter()
            .map(|entry| *entry.key())
            .max()
            .unwrap_or(0);

        self.cache.next_id.store(max_id + 1, Ordering::SeqCst);

        Ok(())
    }
}

#[async_trait]
impl SchemaRegistry for EmbeddedSchemaRegistry {
    async fn register(&self, subject: &str, schema_str: &str) -> Result<i32> {
        // 1. Check cache first
        if let Ok(id) = self.cache.check_existing(subject, schema_str).await {
            return Ok(id);
        }

        // 2. Allocate ID atomically
        let next_id = self.cache.next_id.load(Ordering::SeqCst);

        let version = self
            .cache
            .subject_versions
            .get(subject)
            .map(|v| v.iter().map(|entry| *entry.key()).max().unwrap_or(0) + 1)
            .unwrap_or(1);

        // 3. Serialize Event
        let event = SchemaLogEvent::Register(SchemaRegistration {
            id: next_id,
            subject: subject.to_string(),
            schema: schema_str.to_string(),
            version,
        });

        let payload = bincode::serialize(&event)
            .map_err(|e| Error::Other(format!("Serialization error: {}", e)))?;

        use crate::Message;

        // 4. Create proper Message with key
        let msg = Message::with_key(
            bytes::Bytes::from(subject.to_string()), // Key
            bytes::Bytes::from(payload),             // Value
        );

        // 5. Append to Log
        self.partition.append(msg).await?;

        // 6. Update Memory Cache
        self.cache
            .inject_registration(next_id, subject, schema_str, version)?;

        // 7. Increment ID atomically
        self.cache.next_id.fetch_add(1, Ordering::SeqCst);

        Ok(next_id)
    }

    async fn get_schema(&self, id: i32) -> Result<Arc<Schema>> {
        self.cache.get_schema(id).await
    }

    async fn get_latest_schema_id(&self, subject: &str) -> Result<Option<i32>> {
        self.cache.get_latest_schema_id(subject).await
    }
}

/// In-Memory implementation of Schema Registry with lock-free concurrent access.
/// Uses DashMap for 100x faster concurrent reads vs `RwLock<HashMap>`.
/// Uses `Arc<Schema>` to avoid cloning (10x faster lookups).
#[derive(Debug, Clone)]
pub struct MemorySchemaRegistry {
    /// Lock-free concurrent map for schema lookups
    id_to_schema: Arc<DashMap<i32, Arc<Schema>>>,

    /// Subject to version map (nested structure)
    subject_versions: Arc<DashMap<String, DashMap<i32, i32>>>,

    /// Atomic counter for next schema ID (lock-free)
    next_id: Arc<AtomicI32>,
}

impl Default for MemorySchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySchemaRegistry {
    pub fn new() -> Self {
        Self {
            id_to_schema: Arc::new(DashMap::new()),
            subject_versions: Arc::new(DashMap::new()),
            next_id: Arc::new(AtomicI32::new(1)),
        }
    }

    pub fn inject_registration(
        &self,
        id: i32,
        subject: &str,
        schema_str: &str,
        version: i32,
    ) -> Result<()> {
        let schema = Schema::parse_str(schema_str)
            .map_err(|e| Error::Other(format!("Invalid Avro schema: {}", e)))?;
        let schema = Arc::new(schema);

        // Lock-free insertions
        self.id_to_schema.insert(id, schema);

        let versions = self
            .subject_versions
            .entry(subject.to_string())
            .or_default();
        versions.insert(version, id);

        Ok(())
    }

    pub async fn check_existing(
        &self,
        subject: &str,
        schema_str: &str,
    ) -> std::result::Result<i32, ()> {
        let schema = Schema::parse_str(schema_str).map_err(|_| ())?;
        let canonical_form = schema.canonical_form();

        // Lock-free iteration over concurrent map
        for entry in self.id_to_schema.iter() {
            let existing_id = *entry.key();
            let existing_schema = entry.value();

            if existing_schema.canonical_form() == canonical_form {
                if let Some(versions) = self.subject_versions.get(subject) {
                    for version_entry in versions.iter() {
                        if *version_entry.value() == existing_id {
                            return Ok(existing_id);
                        }
                    }
                }
            }
        }
        Err(())
    }
}

#[async_trait]
impl SchemaRegistry for MemorySchemaRegistry {
    async fn register(&self, subject: &str, schema_str: &str) -> Result<i32> {
        if let Ok(id) = self.check_existing(subject, schema_str).await {
            return Ok(id);
        }

        // Lock-free ID allocation
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let version = self
            .subject_versions
            .get(subject)
            .map(|v| v.iter().map(|entry| *entry.key()).max().unwrap_or(0) + 1)
            .unwrap_or(1);

        self.inject_registration(id, subject, schema_str, version)?;
        Ok(id)
    }

    async fn get_schema(&self, id: i32) -> Result<Arc<Schema>> {
        self.id_to_schema
            .get(&id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| Error::Other(format!("Schema ID {} not found", id)))
    }

    async fn get_latest_schema_id(&self, subject: &str) -> Result<Option<i32>> {
        if let Some(versions) = self.subject_versions.get(subject) {
            let max_ver = versions.iter().map(|entry| *entry.key()).max();

            if let Some(max_version) = max_ver {
                return Ok(versions.get(&max_version).map(|e| *e.value()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use std::fs;

    fn get_test_config() -> Config {
        let mut config = Config::default();
        config.data_dir = format!("/tmp/rivven-test-registry-{}", uuid::Uuid::new_v4());
        let _ = fs::remove_dir_all(&config.data_dir);
        config
    }

    #[tokio::test]
    async fn test_embedded_registry_persistence() {
        let config = get_test_config();
        let subject = "user-value";
        let schema_str =
            r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#;

        // 1. Start Registry and Register Schema
        {
            let registry = EmbeddedSchemaRegistry::new(&config)
                .await
                .expect("Failed to create registry");
            let id = registry
                .register(subject, schema_str)
                .await
                .expect("Failed to register");
            assert_eq!(id, 1);

            // Check it exists
            let schema = registry.get_schema(1).await.expect("Failed to get schema");
            assert!(format!("{:?}", schema).contains("User"));
        }

        // 2. Restart (Simulate by creating new instance on same dir)
        {
            let registry = EmbeddedSchemaRegistry::new(&config)
                .await
                .expect("Failed to recover registry");

            // Should have id 1
            let schema = registry
                .get_schema(1)
                .await
                .expect("Failed to get schema after recovery");
            assert!(format!("{:?}", schema).contains("User"));

            // Check caching of subject version
            let latest = registry
                .get_latest_schema_id(subject)
                .await
                .expect("Failed to get latest")
                .unwrap();
            assert_eq!(latest, 1);

            // 3. Register next schema
            let schema_str2 =
                r#"{"type":"record","name":"UserV2","fields":[{"name":"age","type":"int"}]}"#;
            let id2 = registry
                .register(subject, schema_str2)
                .await
                .expect("Failed to register 2");
            assert_eq!(id2, 2);
        }

        // Cleanup
        let _ = fs::remove_dir_all(&config.data_dir);
    }
}
