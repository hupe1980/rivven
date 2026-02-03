//! Broker-backed storage for Schema Registry
//!
//! This storage backend persists schemas in a compacted rivven topic, similar to
//! Confluent Schema Registry's `_schemas` topic in Kafka. This provides:
//!
//! - **Durability**: Schemas survive registry restarts
//! - **Replication**: Schemas are replicated across broker nodes
//! - **Consistency**: Single partition ensures global ordering for schema IDs
//! - **No external dependencies**: Uses the rivven broker itself for storage
//!
//! # Architecture
//!
//! The broker storage uses a single compacted topic (default: `_schemas`) with:
//! - Key: Schema operation key (e.g., "schema-{id}", "subject-{name}-{version}")
//! - Value: JSON-encoded schema data
//!
//! On startup, the storage backend reads all messages from the topic to rebuild
//! the in-memory index. Write operations produce messages to the topic and
//! update the local index.
//!
//! # Key Format
//!
//! - `schema/{id}` - Schema content by ID
//! - `subject/{name}/version/{version}` - Subject version mapping
//! - `subject/{name}/config` - Subject-level configuration
//! - `config/global` - Global configuration
//! - `id/counter` - Schema ID counter (for monotonic ID generation)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_schema::storage::BrokerStorage;
//! use rivven_schema::config::BrokerStorageConfig;
//!
//! let config = BrokerStorageConfig {
//!     brokers: "localhost:9092".to_string(),
//!     topic: "_schemas".to_string(),
//!     ..Default::default()
//! };
//!
//! let storage = BrokerStorage::new(config).await?;
//! ```

use crate::config::BrokerStorageConfig;
use crate::error::{SchemaError, SchemaResult};
use crate::types::{
    Schema, SchemaId, SchemaType, SchemaVersion, Subject, SubjectVersion, VersionState,
};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use rivven_client::{ResilientClient, ResilientClientConfig};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::StorageBackend;

/// Broker-backed schema storage
///
/// Stores schemas in a compacted rivven topic for durability and replication.
/// Uses the resilient client for automatic retries and failover.
pub struct BrokerStorage {
    /// Configuration
    config: BrokerStorageConfig,

    /// Resilient client for broker communication
    client: Arc<ResilientClient>,

    /// Schema ID counter
    next_id: AtomicU32,

    /// Current offset in the schema topic (for consuming new records)
    current_offset: AtomicU64,

    /// Schemas by ID
    schemas: DashMap<u32, Schema>,

    /// Schema fingerprint -> ID mapping (for deduplication)
    fingerprints: DashMap<String, u32>,

    /// Subject -> versions mapping
    subjects: DashMap<String, Vec<SubjectVersionEntry>>,

    /// Client connection state
    state: RwLock<ConnectionState>,
}

/// Internal entry for tracking subject versions
#[derive(Clone, Serialize, Deserialize)]
struct SubjectVersionEntry {
    version: u32,
    schema_id: u32,
    schema_type: SchemaType,
    deleted: bool,
    #[serde(default)]
    state: VersionState,
}

/// Connection state for broker storage
enum ConnectionState {
    /// Not connected
    Disconnected,

    /// Connecting to broker
    Connecting,

    /// Connected and ready
    Connected,
}

/// Schema record stored in broker topic
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SchemaRecord {
    /// Schema ID
    id: u32,
    /// Schema type
    schema_type: SchemaType,
    /// Schema definition
    schema: String,
    /// Schema fingerprint
    fingerprint: Option<String>,
    /// References to other schemas
    #[serde(default)]
    references: Vec<crate::types::SchemaReference>,
    /// Schema metadata
    #[serde(default)]
    metadata: Option<crate::types::SchemaMetadata>,
}

/// Subject version record stored in broker topic
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubjectVersionRecord {
    /// Subject name
    subject: String,
    /// Version number
    version: u32,
    /// Associated schema ID
    schema_id: u32,
    /// Schema type
    schema_type: SchemaType,
    /// Whether this version is deleted
    #[serde(default)]
    deleted: bool,
}

/// ID counter record stored in broker topic
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdCounterRecord {
    /// Next schema ID to assign
    next_id: u32,
}

impl BrokerStorage {
    /// Create a new broker storage instance
    ///
    /// This will connect to the broker and bootstrap from the schema topic.
    pub async fn new(config: BrokerStorageConfig) -> SchemaResult<Self> {
        // Parse broker addresses
        let bootstrap_servers: Vec<String> = config
            .brokers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        if bootstrap_servers.is_empty() {
            return Err(SchemaError::Config(
                "No broker addresses provided".to_string(),
            ));
        }

        // Build resilient client config
        let client_config = ResilientClientConfig::builder()
            .bootstrap_servers(bootstrap_servers)
            .pool_size(5)
            .retry_max_attempts(3)
            .connection_timeout(Duration::from_millis(config.connect_timeout_ms))
            .request_timeout(Duration::from_secs(30))
            .circuit_breaker_threshold(5)
            .health_check_enabled(true)
            .build();

        // Create resilient client
        let client = ResilientClient::new(client_config)
            .await
            .map_err(|e| SchemaError::Storage(format!("Failed to connect to broker: {}", e)))?;

        let storage = Self {
            config,
            client: Arc::new(client),
            next_id: AtomicU32::new(1),
            current_offset: AtomicU64::new(0),
            schemas: DashMap::new(),
            fingerprints: DashMap::new(),
            subjects: DashMap::new(),
            state: RwLock::new(ConnectionState::Disconnected),
        };

        // Bootstrap: read existing schemas from broker
        storage.bootstrap().await?;

        Ok(storage)
    }

    /// Create broker storage for testing (without actual broker connection)
    #[cfg(test)]
    pub async fn new_for_testing(config: BrokerStorageConfig) -> SchemaResult<Self> {
        // For testing, we create a mock that doesn't actually connect
        let bootstrap_servers = vec!["localhost:9092".to_string()];

        let client_config = ResilientClientConfig::builder()
            .bootstrap_servers(bootstrap_servers)
            .pool_size(1)
            .retry_max_attempts(0) // No retries for tests
            .connection_timeout(Duration::from_millis(100))
            .health_check_enabled(false)
            .build();

        // For tests, we'll proceed without a real connection
        // The ResilientClient::new will fail, but we catch it
        let client = match ResilientClient::new(client_config).await {
            Ok(c) => Arc::new(c),
            Err(_) => {
                // Create a dummy client for testing - tests will use in-memory state
                return Self::new_in_memory_for_testing(config).await;
            }
        };

        let storage = Self {
            config,
            client,
            next_id: AtomicU32::new(1),
            current_offset: AtomicU64::new(0),
            schemas: DashMap::new(),
            fingerprints: DashMap::new(),
            subjects: DashMap::new(),
            state: RwLock::new(ConnectionState::Connected),
        };

        Ok(storage)
    }

    /// Create in-memory storage for testing when broker is unavailable
    #[cfg(test)]
    async fn new_in_memory_for_testing(_config: BrokerStorageConfig) -> SchemaResult<Self> {
        // Create a minimal client config - won't actually be used
        let _client_config = ResilientClientConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".to_string()])
            .pool_size(1)
            .health_check_enabled(false)
            .build();

        // We need a placeholder client - in tests, we'll skip broker writes
        // This is a workaround since we can't create ResilientClient without a broker
        // For proper testing, use integration tests with a real broker

        // Return error indicating tests need special handling
        Err(SchemaError::Storage(
            "Broker not available - use MemoryStorage for unit tests".to_string(),
        ))
    }

    /// Bootstrap storage from broker topic
    async fn bootstrap(&self) -> SchemaResult<()> {
        info!(
            topic = %self.config.topic,
            brokers = %self.config.brokers,
            "Bootstrapping schema storage from broker"
        );

        // Update state to connecting
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connecting;
        }

        // Ensure topic exists
        match self.ensure_topic_exists().await {
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "Failed to ensure topic exists, continuing anyway");
            }
        }

        // Read all existing records from the schema topic
        let mut offset: u64 = 0;
        let mut max_schema_id: u32 = 0;
        let mut records_loaded: u64 = 0;

        let timeout = Duration::from_millis(self.config.bootstrap_timeout_ms);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                warn!(
                    timeout_ms = self.config.bootstrap_timeout_ms,
                    records_loaded = records_loaded,
                    "Bootstrap timeout reached"
                );
                break;
            }

            // Consume messages from the schema topic
            match self
                .client
                .consume(&self.config.topic, 0, offset, 1000)
                .await
            {
                Ok(messages) => {
                    if messages.is_empty() {
                        // No more messages
                        debug!(offset = offset, "Reached end of schema topic");
                        break;
                    }

                    for msg in messages {
                        if let Err(e) = self.process_bootstrap_message(&msg.key, &msg.value).await {
                            warn!(
                                offset = msg.offset,
                                error = %e,
                                "Failed to process bootstrap message"
                            );
                        }

                        // Track max schema ID for counter initialization
                        if let Some(key) = &msg.key {
                            if let Some(id) = Self::extract_schema_id_from_key(key) {
                                max_schema_id = max_schema_id.max(id);
                            }
                        }

                        offset = msg.offset + 1;
                        records_loaded += 1;
                    }
                }
                Err(e) => {
                    // If topic doesn't exist or is empty, that's OK for initial setup
                    if e.to_string().contains("not found")
                        || e.to_string().contains("empty")
                        || e.to_string().contains("no messages")
                    {
                        debug!("Schema topic is empty or doesn't exist yet");
                        break;
                    }
                    error!(error = %e, "Failed to consume from schema topic during bootstrap");
                    break;
                }
            }
        }

        // Initialize schema ID counter to max + 1
        self.next_id.store(max_schema_id + 1, Ordering::SeqCst);
        self.current_offset.store(offset, Ordering::SeqCst);

        // Mark as connected
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connected;
        }

        info!(
            schemas = self.schemas.len(),
            subjects = self.subjects.len(),
            next_id = max_schema_id + 1,
            records_loaded = records_loaded,
            "Schema storage bootstrap complete"
        );

        Ok(())
    }

    /// Ensure the schema topic exists, create if necessary
    async fn ensure_topic_exists(&self) -> SchemaResult<()> {
        match self
            .client
            .create_topic(&self.config.topic, Some(self.config.partitions))
            .await
        {
            Ok(partitions) => {
                info!(
                    topic = %self.config.topic,
                    partitions = partitions,
                    "Schema topic ready"
                );
                Ok(())
            }
            Err(e) => {
                // Topic may already exist, which is fine
                if e.to_string().contains("already exists") {
                    debug!(topic = %self.config.topic, "Schema topic already exists");
                    Ok(())
                } else {
                    Err(SchemaError::Storage(format!(
                        "Failed to create schema topic: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Extract schema ID from a key like "schema/123"
    fn extract_schema_id_from_key(key: &Bytes) -> Option<u32> {
        let key_str = std::str::from_utf8(key).ok()?;
        key_str.strip_prefix("schema/")?.parse().ok()
    }

    /// Process a message during bootstrap
    async fn process_bootstrap_message(
        &self,
        key: &Option<Bytes>,
        value: &Bytes,
    ) -> SchemaResult<()> {
        let key = key
            .as_ref()
            .ok_or_else(|| SchemaError::Storage("Message has no key".to_string()))?;

        let key_str = std::str::from_utf8(key)
            .map_err(|e| SchemaError::Storage(format!("Invalid key encoding: {}", e)))?;

        // Handle tombstone (empty value = deletion)
        if value.is_empty() {
            debug!(key = %key_str, "Processing tombstone record");
            return Ok(());
        }

        if key_str.starts_with("schema/") {
            // Schema record
            let record: SchemaRecord = serde_json::from_slice(value)
                .map_err(|e| SchemaError::Storage(format!("Invalid schema record: {}", e)))?;

            let schema = Schema {
                id: SchemaId::new(record.id),
                schema_type: record.schema_type,
                schema: record.schema,
                fingerprint: record.fingerprint.clone(),
                references: record.references,
                metadata: record.metadata,
            };

            if let Some(ref fp) = record.fingerprint {
                self.fingerprints.insert(fp.clone(), record.id);
            }
            self.schemas.insert(record.id, schema);

            debug!(schema_id = record.id, "Loaded schema from broker");
        } else if key_str.starts_with("subject/") && key_str.contains("/version/") {
            // Subject version record
            let record: SubjectVersionRecord = serde_json::from_slice(value).map_err(|e| {
                SchemaError::Storage(format!("Invalid subject version record: {}", e))
            })?;

            let entry = SubjectVersionEntry {
                version: record.version,
                schema_id: record.schema_id,
                schema_type: record.schema_type,
                deleted: record.deleted,
                state: VersionState::Enabled, // Default state when loading
            };

            let mut versions = self.subjects.entry(record.subject.clone()).or_default();

            // Update existing version or add new one
            if let Some(existing) = versions.iter_mut().find(|v| v.version == record.version) {
                *existing = entry;
            } else {
                versions.push(entry);
            }

            debug!(
                subject = %record.subject,
                version = record.version,
                deleted = record.deleted,
                "Loaded subject version from broker"
            );
        } else if key_str == "id/counter" {
            // ID counter record
            let record: IdCounterRecord = serde_json::from_slice(value)
                .map_err(|e| SchemaError::Storage(format!("Invalid ID counter record: {}", e)))?;

            // Only update if greater than current
            let current = self.next_id.load(Ordering::SeqCst);
            if record.next_id > current {
                self.next_id.store(record.next_id, Ordering::SeqCst);
            }

            debug!(next_id = record.next_id, "Loaded ID counter from broker");
        }

        Ok(())
    }

    /// Write a schema record to the broker topic
    async fn write_schema_record(&self, record: &SchemaRecord) -> SchemaResult<()> {
        let key = format!("schema/{}", record.id);
        let value = serde_json::to_vec(record).map_err(|e| {
            SchemaError::Storage(format!("Failed to serialize schema record: {}", e))
        })?;

        debug!(key = %key, schema_id = record.id, "Writing schema record to broker");

        self.client
            .publish_with_key(
                &self.config.topic,
                Some(Bytes::from(key)),
                Bytes::from(value),
            )
            .await
            .map_err(|e| {
                SchemaError::Storage(format!("Failed to write schema to broker: {}", e))
            })?;

        Ok(())
    }

    /// Write a subject version record to the broker topic
    async fn write_subject_version_record(
        &self,
        record: &SubjectVersionRecord,
    ) -> SchemaResult<()> {
        let key = format!("subject/{}/version/{}", record.subject, record.version);
        let value = serde_json::to_vec(record).map_err(|e| {
            SchemaError::Storage(format!("Failed to serialize subject version record: {}", e))
        })?;

        debug!(
            key = %key,
            subject = %record.subject,
            version = record.version,
            "Writing subject version record to broker"
        );

        self.client
            .publish_with_key(
                &self.config.topic,
                Some(Bytes::from(key)),
                Bytes::from(value),
            )
            .await
            .map_err(|e| {
                SchemaError::Storage(format!("Failed to write subject version to broker: {}", e))
            })?;

        Ok(())
    }

    /// Write the ID counter to broker topic (for persistence across restarts)
    async fn write_id_counter(&self, next_id: u32) -> SchemaResult<()> {
        let key = "id/counter";
        let record = IdCounterRecord { next_id };
        let value = serde_json::to_vec(&record)
            .map_err(|e| SchemaError::Storage(format!("Failed to serialize ID counter: {}", e)))?;

        debug!(key = %key, next_id = next_id, "Writing ID counter to broker");

        self.client
            .publish_with_key(
                &self.config.topic,
                Some(Bytes::from(key.to_string())),
                Bytes::from(value),
            )
            .await
            .map_err(|e| {
                SchemaError::Storage(format!("Failed to write ID counter to broker: {}", e))
            })?;

        Ok(())
    }

    /// Ensure we're connected to the broker
    async fn ensure_connected(&self) -> SchemaResult<()> {
        let state = self.state.read().await;
        match &*state {
            ConnectionState::Connected => Ok(()),
            ConnectionState::Connecting => Err(SchemaError::Storage(
                "Broker storage is still bootstrapping".to_string(),
            )),
            ConnectionState::Disconnected => Err(SchemaError::Storage(
                "Broker storage is disconnected".to_string(),
            )),
        }
    }

    /// Get storage statistics
    pub fn stats(&self) -> BrokerStorageStats {
        self.detailed_stats()
    }

    /// Check if storage is healthy
    pub async fn is_healthy(&self) -> bool {
        matches!(*self.state.read().await, ConnectionState::Connected)
    }
}

/// Statistics for broker storage
#[derive(Debug, Clone)]
pub struct BrokerStorageStats {
    /// Number of schemas stored
    pub schema_count: usize,
    /// Number of subjects
    pub subject_count: usize,
    /// Number of fingerprints indexed
    pub fingerprint_count: usize,
    /// Next schema ID to be assigned
    pub next_schema_id: u32,
    /// Current offset in schema topic
    pub current_offset: u64,
    /// Whether storage is connected
    pub is_connected: bool,
    /// Total versions across all subjects
    pub version_count: usize,
    /// Number of soft-deleted versions
    pub deleted_version_count: usize,
}

impl BrokerStorage {
    /// Attempt to reconnect to the broker
    ///
    /// This will re-bootstrap the storage from the schema topic.
    /// Useful after network failures or broker restarts.
    pub async fn reconnect(&self) -> SchemaResult<()> {
        info!("Attempting to reconnect to broker");

        // Mark as disconnected
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Disconnected;
        }

        // Clear in-memory state
        self.schemas.clear();
        self.fingerprints.clear();
        self.subjects.clear();
        self.next_id.store(1, Ordering::SeqCst);
        self.current_offset.store(0, Ordering::SeqCst);

        // Re-bootstrap
        self.bootstrap().await
    }

    /// Sync new records from the broker
    ///
    /// Reads any new messages since the last sync. Useful for multi-instance
    /// deployments where another instance may have written new schemas.
    pub async fn sync(&self) -> SchemaResult<u64> {
        self.ensure_connected().await?;

        let start_offset = self.current_offset.load(Ordering::SeqCst);
        let mut offset = start_offset;
        let mut records_synced: u64 = 0;

        loop {
            match self
                .client
                .consume(&self.config.topic, 0, offset, 100)
                .await
            {
                Ok(messages) => {
                    if messages.is_empty() {
                        break;
                    }

                    for msg in messages {
                        if let Err(e) = self.process_bootstrap_message(&msg.key, &msg.value).await {
                            warn!(
                                offset = msg.offset,
                                error = %e,
                                "Failed to process sync message"
                            );
                        }

                        // Track max schema ID
                        if let Some(key) = &msg.key {
                            if let Some(id) = Self::extract_schema_id_from_key(key) {
                                let current = self.next_id.load(Ordering::SeqCst);
                                if id >= current {
                                    self.next_id.store(id + 1, Ordering::SeqCst);
                                }
                            }
                        }

                        offset = msg.offset + 1;
                        records_synced += 1;
                    }
                }
                Err(e) => {
                    if e.to_string().contains("no messages") {
                        break;
                    }
                    return Err(SchemaError::Storage(format!(
                        "Failed to sync from broker: {}",
                        e
                    )));
                }
            }
        }

        self.current_offset.store(offset, Ordering::SeqCst);

        if records_synced > 0 {
            debug!(
                records_synced = records_synced,
                new_offset = offset,
                "Synced new records from broker"
            );
        }

        Ok(records_synced)
    }

    /// Graceful shutdown - flush any pending writes
    pub async fn shutdown(&self) -> SchemaResult<()> {
        info!("Shutting down broker storage");

        // Mark as disconnected to prevent new writes
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Disconnected;
        }

        // The resilient client handles connection cleanup
        info!(
            schemas = self.schemas.len(),
            subjects = self.subjects.len(),
            "Broker storage shutdown complete"
        );

        Ok(())
    }

    /// Get a schema by subject and version (optimized lookup)
    pub async fn get_schema_by_subject_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
    ) -> SchemaResult<Option<Schema>> {
        self.ensure_connected().await?;

        let entry = self.subjects.get(&subject.0);
        match entry {
            Some(versions) => {
                let v = versions
                    .iter()
                    .find(|v| v.version == version.0 && !v.deleted);

                match v {
                    Some(entry) => Ok(self.get_schema_internal(SchemaId::new(entry.schema_id))),
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }

    /// Check if a specific schema ID exists
    pub fn schema_exists(&self, id: SchemaId) -> bool {
        self.schemas.contains_key(&id.0)
    }

    /// Get the total number of schema versions (including deleted)
    pub fn total_version_count(&self) -> usize {
        self.subjects.iter().map(|entry| entry.value().len()).sum()
    }

    /// Get detailed statistics
    pub fn detailed_stats(&self) -> BrokerStorageStats {
        let mut version_count = 0;
        let mut deleted_count = 0;

        for entry in self.subjects.iter() {
            for v in entry.value().iter() {
                version_count += 1;
                if v.deleted {
                    deleted_count += 1;
                }
            }
        }

        let is_connected = self
            .state
            .try_read()
            .map(|s| matches!(*s, ConnectionState::Connected))
            .unwrap_or(false);

        BrokerStorageStats {
            schema_count: self.schemas.len(),
            subject_count: self.subjects.len(),
            fingerprint_count: self.fingerprints.len(),
            next_schema_id: self.next_id.load(Ordering::SeqCst),
            current_offset: self.current_offset.load(Ordering::SeqCst),
            is_connected,
            version_count,
            deleted_version_count: deleted_count,
        }
    }

    /// Get the broker client (for advanced operations)
    pub fn client(&self) -> &Arc<ResilientClient> {
        &self.client
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.config.topic
    }

    /// Get schema by ID (internal helper, without connection check)
    fn get_schema_internal(&self, id: SchemaId) -> Option<Schema> {
        self.schemas.get(&id.0).map(|s| s.clone())
    }
}

#[async_trait]
impl StorageBackend for BrokerStorage {
    async fn store_schema(&self, schema: Schema) -> SchemaResult<SchemaId> {
        self.ensure_connected().await?;

        let id = schema.id.0;
        self.schemas.insert(id, schema.clone());

        if let Some(ref fp) = schema.fingerprint {
            self.fingerprints.insert(fp.clone(), id);
        }

        // Write to broker for durability
        let record = SchemaRecord {
            id,
            schema_type: schema.schema_type,
            schema: schema.schema.clone(),
            fingerprint: schema.fingerprint.clone(),
            references: schema.references.clone(),
            metadata: schema.metadata.clone(),
        };
        self.write_schema_record(&record).await?;

        Ok(schema.id)
    }

    async fn get_schema(&self, id: SchemaId) -> SchemaResult<Option<Schema>> {
        self.ensure_connected().await?;

        Ok(self.schemas.get(&id.0).map(|s| s.clone()))
    }

    async fn get_schema_by_fingerprint(&self, fingerprint: &str) -> SchemaResult<Option<Schema>> {
        self.ensure_connected().await?;

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
        self.ensure_connected().await?;

        let schema = self
            .get_schema(schema_id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("Schema ID {}", schema_id)))?;

        let mut entry = self.subjects.entry(subject.0.clone()).or_default();
        let version = entry.len() as u32 + 1;

        let version_entry = SubjectVersionEntry {
            version,
            schema_id: schema_id.0,
            schema_type: schema.schema_type,
            deleted: false,
            state: VersionState::Enabled,
        };

        entry.push(version_entry.clone());

        // Write to broker for durability
        let record = SubjectVersionRecord {
            subject: subject.0.clone(),
            version,
            schema_id: schema_id.0,
            schema_type: schema.schema_type,
            deleted: false,
        };
        self.write_subject_version_record(&record).await?;

        debug!(
            subject = %subject,
            version = version,
            schema_id = %schema_id,
            "Registered subject version"
        );

        Ok(SchemaVersion::new(version))
    }

    async fn get_versions(&self, subject: &Subject) -> SchemaResult<Vec<u32>> {
        self.ensure_connected().await?;

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
        self.ensure_connected().await?;

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
        self.ensure_connected().await?;

        let entry = self.subjects.get(&subject.0);
        match entry {
            Some(versions) => {
                let latest = versions
                    .iter()
                    .filter(|v| !v.deleted)
                    .max_by_key(|v| v.version);

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
        self.ensure_connected().await?;

        Ok(self
            .subjects
            .iter()
            .filter(|entry| entry.value().iter().any(|v| !v.deleted))
            .map(|entry| Subject::new(entry.key().clone()))
            .collect())
    }

    async fn delete_subject(&self, subject: &Subject, permanent: bool) -> SchemaResult<Vec<u32>> {
        self.ensure_connected().await?;

        if permanent {
            // Hard delete
            if let Some((_, versions)) = self.subjects.remove(&subject.0) {
                let deleted_versions: Vec<u32> = versions.iter().map(|v| v.version).collect();

                // Write tombstone records to broker
                for v in &versions {
                    let record = SubjectVersionRecord {
                        subject: subject.0.clone(),
                        version: v.version,
                        schema_id: v.schema_id,
                        schema_type: v.schema_type,
                        deleted: true,
                    };
                    self.write_subject_version_record(&record).await?;
                }

                info!(
                    subject = %subject,
                    versions = ?deleted_versions,
                    "Permanently deleted subject"
                );

                return Ok(deleted_versions);
            }
        } else {
            // Soft delete
            if let Some(mut versions) = self.subjects.get_mut(&subject.0) {
                let deleted_versions: Vec<u32> = versions
                    .iter()
                    .filter(|v| !v.deleted)
                    .map(|v| v.version)
                    .collect();

                for v in versions.iter_mut() {
                    if !v.deleted {
                        v.deleted = true;

                        // Write updated record to broker
                        let record = SubjectVersionRecord {
                            subject: subject.0.clone(),
                            version: v.version,
                            schema_id: v.schema_id,
                            schema_type: v.schema_type,
                            deleted: true,
                        };
                        self.write_subject_version_record(&record).await?;
                    }
                }

                info!(
                    subject = %subject,
                    versions = ?deleted_versions,
                    "Soft deleted subject"
                );

                return Ok(deleted_versions);
            }
        }

        Ok(vec![])
    }

    async fn delete_version(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        permanent: bool,
    ) -> SchemaResult<()> {
        self.ensure_connected().await?;

        if let Some(mut versions) = self.subjects.get_mut(&subject.0) {
            if permanent {
                // Hard delete
                if let Some(pos) = versions.iter().position(|v| v.version == version.0) {
                    let v = versions.remove(pos);

                    // Write tombstone to broker
                    let record = SubjectVersionRecord {
                        subject: subject.0.clone(),
                        version: version.0,
                        schema_id: v.schema_id,
                        schema_type: v.schema_type,
                        deleted: true,
                    };
                    self.write_subject_version_record(&record).await?;

                    info!(
                        subject = %subject,
                        version = %version,
                        "Permanently deleted version"
                    );
                }
            } else {
                // Soft delete
                if let Some(v) = versions.iter_mut().find(|v| v.version == version.0) {
                    v.deleted = true;

                    // Write updated record to broker
                    let record = SubjectVersionRecord {
                        subject: subject.0.clone(),
                        version: version.0,
                        schema_id: v.schema_id,
                        schema_type: v.schema_type,
                        deleted: true,
                    };
                    self.write_subject_version_record(&record).await?;

                    info!(
                        subject = %subject,
                        version = %version,
                        "Soft deleted version"
                    );
                }
            }
        }

        Ok(())
    }

    async fn next_schema_id(&self) -> SchemaResult<SchemaId> {
        self.ensure_connected().await?;

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        // Persist the ID counter to broker for durability
        // We persist the next value (id + 1) so on restart we continue from there
        self.write_id_counter(id + 1).await?;

        Ok(SchemaId::new(id))
    }

    async fn subject_exists(&self, subject: &Subject) -> SchemaResult<bool> {
        self.ensure_connected().await?;

        let exists = self
            .subjects
            .get(&subject.0)
            .map(|versions| versions.iter().any(|v| !v.deleted))
            .unwrap_or(false);

        Ok(exists)
    }

    async fn set_version_state(
        &self,
        subject: &Subject,
        version: SchemaVersion,
        state: VersionState,
    ) -> SchemaResult<()> {
        self.ensure_connected().await?;

        if let Some(mut entry) = self.subjects.get_mut(&subject.0) {
            if let Some(v) = entry
                .iter_mut()
                .find(|v| v.version == version.0 && !v.deleted)
            {
                v.state = state;
                // TODO: Persist state change to broker topic
                debug!(
                    subject = %subject,
                    version = version.0,
                    state = ?state,
                    "Updated version state"
                );
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
        self.ensure_connected().await?;

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
        self.ensure_connected().await?;

        // Return subjects where all versions are marked as deleted
        Ok(self
            .subjects
            .iter()
            .filter(|e| e.iter().all(|v| v.deleted))
            .map(|e| Subject::new(e.key().clone()))
            .collect())
    }

    async fn undelete_subject(&self, subject: &Subject) -> SchemaResult<Vec<u32>> {
        self.ensure_connected().await?;

        if let Some(mut entry) = self.subjects.get_mut(&subject.0) {
            // Check if subject was actually deleted (all versions deleted)
            if entry.iter().all(|v| v.deleted) {
                let version_nums: Vec<u32> = entry.iter().map(|v| v.version).collect();
                // Restore all versions
                for v in entry.iter_mut() {
                    v.deleted = false;
                }
                // Write updates to broker
                for v in entry.iter() {
                    let record = SubjectVersionRecord {
                        subject: subject.0.clone(),
                        version: v.version,
                        schema_id: v.schema_id,
                        schema_type: v.schema_type,
                        deleted: false,
                    };
                    self.write_subject_version_record(&record).await?;
                }
                Ok(version_nums)
            } else {
                Err(SchemaError::NotFound(format!(
                    "Subject '{}' is not deleted",
                    subject
                )))
            }
        } else {
            Err(SchemaError::NotFound(format!(
                "Subject '{}' not found",
                subject
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running rivven broker.
    // For unit tests without a broker, use MemoryStorage instead.
    // Run with: cargo test --features broker -- --ignored

    #[test]
    fn test_extract_schema_id_from_key() {
        // Valid schema key
        let key = Bytes::from("schema/123");
        assert_eq!(BrokerStorage::extract_schema_id_from_key(&key), Some(123));

        // Invalid prefix
        let key = Bytes::from("subject/test");
        assert_eq!(BrokerStorage::extract_schema_id_from_key(&key), None);

        // Invalid number
        let key = Bytes::from("schema/abc");
        assert_eq!(BrokerStorage::extract_schema_id_from_key(&key), None);
    }

    #[test]
    fn test_schema_record_serialization() {
        let record = SchemaRecord {
            id: 1,
            schema_type: SchemaType::Avro,
            schema: r#"{"type": "string"}"#.to_string(),
            fingerprint: Some("abc123".to_string()),
            references: vec![],
            metadata: None,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: SchemaRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, record.id);
        assert_eq!(parsed.schema, record.schema);
        assert_eq!(parsed.fingerprint, record.fingerprint);
    }

    #[test]
    fn test_subject_version_record_serialization() {
        let record = SubjectVersionRecord {
            subject: "test-topic".to_string(),
            version: 1,
            schema_id: 100,
            schema_type: SchemaType::Avro,
            deleted: false,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: SubjectVersionRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.subject, record.subject);
        assert_eq!(parsed.version, record.version);
        assert_eq!(parsed.schema_id, record.schema_id);
        assert!(!parsed.deleted);
    }

    #[test]
    fn test_id_counter_record_serialization() {
        let record = IdCounterRecord { next_id: 42 };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: IdCounterRecord = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.next_id, 42);
    }

    #[test]
    fn test_broker_storage_config_default() {
        let config = BrokerStorageConfig::default();

        assert_eq!(config.brokers, "localhost:9092");
        assert_eq!(config.topic, "_schemas");
        assert_eq!(config.partitions, 1);
        assert_eq!(config.replication_factor, 3);
    }

    // Integration tests that require a broker - run with --ignored
    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_basic_operations() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Get a schema ID first
        let id = storage.next_schema_id().await.unwrap();

        // Store a schema
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type": "string"}"#.to_string())
            .with_fingerprint("test-fingerprint-1".to_string());

        let stored_id = storage.store_schema(schema.clone()).await.unwrap();
        assert_eq!(stored_id, id);

        // Retrieve the schema
        let retrieved = storage.get_schema(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().schema, schema.schema);

        // Retrieve by fingerprint
        let by_fp = storage
            .get_schema_by_fingerprint("test-fingerprint-1")
            .await
            .unwrap();
        assert!(by_fp.is_some());
        assert_eq!(by_fp.unwrap().id, id);
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_subject_versions() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Create subject
        let subject = Subject::new("test-subject");

        // Store schemas
        let id1 = storage.next_schema_id().await.unwrap();
        let schema1 = Schema::new(id1, SchemaType::Avro, r#"{"type": "string"}"#.to_string())
            .with_fingerprint("fp1".to_string());
        storage.store_schema(schema1).await.unwrap();

        let id2 = storage.next_schema_id().await.unwrap();
        let schema2 = Schema::new(id2, SchemaType::Avro, r#"{"type": "int"}"#.to_string())
            .with_fingerprint("fp2".to_string());
        storage.store_schema(schema2).await.unwrap();

        // Register versions
        let v1 = storage
            .register_subject_version(&subject, id1)
            .await
            .unwrap();
        assert_eq!(v1.0, 1);

        let v2 = storage
            .register_subject_version(&subject, id2)
            .await
            .unwrap();
        assert_eq!(v2.0, 2);

        // Get versions
        let versions = storage.get_versions(&subject).await.unwrap();
        assert_eq!(versions, vec![1, 2]);

        // Get latest version
        let latest = storage.get_latest_version(&subject).await.unwrap();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version.0, 2);

        // List subjects
        let subjects = storage.list_subjects().await.unwrap();
        assert_eq!(subjects.len(), 1);
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_delete_operations() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        let subject = Subject::new("delete-test");

        // Store and register schema
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type": "string"}"#.to_string())
            .with_fingerprint("delete-fp".to_string());
        storage.store_schema(schema).await.unwrap();
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        // Soft delete
        let deleted = storage.delete_subject(&subject, false).await.unwrap();
        assert_eq!(deleted, vec![1]);

        // Subject should no longer appear in list
        let subjects = storage.list_subjects().await.unwrap();
        assert!(subjects.is_empty());
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_stats() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        let stats = storage.stats();
        assert!(stats.next_schema_id >= 1);
        assert!(storage.is_healthy().await);
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_bootstrap_recovery() {
        // Test that a second instance can bootstrap from the same topic
        let config = BrokerStorageConfig::default();

        // First instance - write some data
        {
            let storage = BrokerStorage::new(config.clone()).await.unwrap();

            let id = storage.next_schema_id().await.unwrap();
            let schema = Schema::new(
                id,
                SchemaType::Avro,
                r#"{"type": "record", "name": "Test", "fields": []}"#.to_string(),
            )
            .with_fingerprint("recovery-test-fp".to_string());
            storage.store_schema(schema).await.unwrap();

            let subject = Subject::new("recovery-test-subject");
            storage
                .register_subject_version(&subject, id)
                .await
                .unwrap();
        }

        // Wait a bit for messages to be committed
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second instance - should recover the data
        {
            let storage = BrokerStorage::new(config).await.unwrap();

            // Should be able to find the schema by fingerprint
            let recovered = storage
                .get_schema_by_fingerprint("recovery-test-fp")
                .await
                .unwrap();
            assert!(
                recovered.is_some(),
                "Schema should be recovered from broker"
            );

            // Should be able to list the subject
            let subjects = storage.list_subjects().await.unwrap();
            assert!(
                subjects.iter().any(|s| s.0 == "recovery-test-subject"),
                "Subject should be recovered from broker"
            );
        }
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_sync() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Sync should work even when no new records
        let synced = storage.sync().await.unwrap();
        assert_eq!(synced, 0, "No new records to sync initially");
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_reconnect() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Store a schema
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type": "string"}"#.to_string())
            .with_fingerprint("reconnect-test-fp".to_string());
        storage.store_schema(schema).await.unwrap();

        // Reconnect should recover the schema
        storage.reconnect().await.unwrap();

        // Verify schema is still accessible
        let recovered = storage
            .get_schema_by_fingerprint("reconnect-test-fp")
            .await
            .unwrap();
        assert!(
            recovered.is_some(),
            "Schema should be recovered after reconnect"
        );
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_shutdown() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        assert!(storage.is_healthy().await);

        storage.shutdown().await.unwrap();

        assert!(!storage.is_healthy().await);
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_detailed_stats() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Create some data
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type": "string"}"#.to_string())
            .with_fingerprint("stats-test-fp".to_string());
        storage.store_schema(schema).await.unwrap();

        let subject = Subject::new("stats-test-subject");
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        let stats = storage.detailed_stats();
        assert!(stats.is_connected);
        assert!(stats.schema_count >= 1);
        assert!(stats.subject_count >= 1);
        assert!(stats.version_count >= 1);
        assert_eq!(stats.deleted_version_count, 0);
    }

    #[tokio::test]
    #[ignore = "Requires running rivven broker"]
    async fn test_broker_storage_get_schema_by_subject_version() {
        let config = BrokerStorageConfig::default();
        let storage = BrokerStorage::new(config).await.unwrap();

        // Create schema and subject
        let id = storage.next_schema_id().await.unwrap();
        let schema = Schema::new(id, SchemaType::Avro, r#"{"type": "int"}"#.to_string())
            .with_fingerprint("subject-version-test-fp".to_string());
        storage.store_schema(schema.clone()).await.unwrap();

        let subject = Subject::new("subject-version-test");
        storage
            .register_subject_version(&subject, id)
            .await
            .unwrap();

        // Retrieve by subject and version
        let retrieved = storage
            .get_schema_by_subject_version(&subject, SchemaVersion::new(1))
            .await
            .unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().schema, schema.schema);

        // Non-existent version should return None
        let not_found = storage
            .get_schema_by_subject_version(&subject, SchemaVersion::new(99))
            .await
            .unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_broker_storage_stats_struct() {
        let stats = BrokerStorageStats {
            schema_count: 10,
            subject_count: 5,
            fingerprint_count: 10,
            next_schema_id: 11,
            current_offset: 100,
            is_connected: true,
            version_count: 15,
            deleted_version_count: 2,
        };

        assert_eq!(stats.schema_count, 10);
        assert_eq!(stats.subject_count, 5);
        assert_eq!(stats.version_count, 15);
        assert_eq!(stats.deleted_version_count, 2);
        assert!(stats.is_connected);
    }
}
