use crate::storage::TieredStorage;
use crate::{Config, Error, Message, Partition, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Topic metadata for persistence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub num_partitions: u32,
    pub created_at: i64,
}

/// Represents a topic with multiple partitions
#[derive(Debug)]
pub struct Topic {
    /// Topic name
    name: String,

    /// Partitions in this topic (growable via add_partitions)
    ///
    /// F-097: `parking_lot::RwLock` is intentional here — critical sections are
    /// O(1) Vec index lookups and never held across `.await` points.
    /// `tokio::sync::RwLock` would add unnecessary overhead for pure-sync access.
    partitions: parking_lot::RwLock<Vec<Arc<Partition>>>,
}

impl Topic {
    /// Create a new topic with the specified number of partitions
    pub async fn new(config: &Config, name: String, num_partitions: u32) -> Result<Self> {
        Self::new_with_tiered_storage(config, name, num_partitions, None).await
    }

    /// Create a new topic with the specified number of partitions and optional tiered storage
    pub async fn new_with_tiered_storage(
        config: &Config,
        name: String,
        num_partitions: u32,
        tiered_storage: Option<Arc<TieredStorage>>,
    ) -> Result<Self> {
        info!(
            "Creating topic '{}' with {} partitions (tiered_storage: {})",
            name,
            num_partitions,
            tiered_storage.is_some()
        );

        let mut partitions = Vec::new();
        for id in 0..num_partitions {
            partitions.push(Arc::new(
                Partition::new_with_tiered_storage(config, &name, id, tiered_storage.clone())
                    .await?,
            ));
        }

        Ok(Self {
            name,
            partitions: parking_lot::RwLock::new(partitions),
        })
    }

    /// Get the topic name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.partitions.read().len()
    }

    /// Get a specific partition
    pub fn partition(&self, partition_id: u32) -> Result<Arc<Partition>> {
        self.partitions
            .read()
            .get(partition_id as usize)
            .cloned()
            .ok_or(Error::PartitionNotFound(partition_id))
    }

    /// Append a message to a specific partition
    pub async fn append(&self, partition_id: u32, message: Message) -> Result<u64> {
        let partition = self.partition(partition_id)?;
        partition.append(message).await
    }

    /// Read messages from a specific partition
    pub async fn read(
        &self,
        partition_id: u32,
        start_offset: u64,
        max_messages: usize,
    ) -> Result<Vec<Message>> {
        let partition = self.partition(partition_id)?;
        partition.read(start_offset, max_messages).await
    }

    /// Get all partitions
    pub fn all_partitions(&self) -> Vec<Arc<Partition>> {
        self.partitions.read().clone()
    }

    /// Flush all partitions to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        let partitions = self.partitions.read().clone();
        for partition in &partitions {
            partition.flush().await?;
        }
        Ok(())
    }

    /// Find the first offset with timestamp >= target_timestamp (milliseconds since epoch)
    /// Returns None if no matching offset is found.
    pub async fn find_offset_for_timestamp(
        &self,
        partition_id: u32,
        target_timestamp: i64,
    ) -> Result<Option<u64>> {
        let partition = self.partition(partition_id)?;
        partition.find_offset_for_timestamp(target_timestamp).await
    }

    /// Dynamically add partitions to this topic.
    ///
    /// Creates new partitions with IDs from `current_count` to `new_total - 1`.
    /// Existing partitions and their data are unaffected.
    pub async fn add_partitions(
        &self,
        config: &Config,
        new_total: u32,
        tiered_storage: Option<Arc<TieredStorage>>,
    ) -> Result<u32> {
        let current_count = self.num_partitions() as u32;
        if new_total <= current_count {
            return Err(Error::Other(format!(
                "New partition count {} must exceed current count {}",
                new_total, current_count
            )));
        }

        let mut new_partitions = Vec::new();
        for id in current_count..new_total {
            new_partitions.push(Arc::new(
                Partition::new_with_tiered_storage(config, &self.name, id, tiered_storage.clone())
                    .await?,
            ));
        }

        let added = new_partitions.len() as u32;
        self.partitions.write().extend(new_partitions);

        info!(
            "Added {} partitions to topic '{}' (total: {})",
            added, self.name, new_total
        );

        Ok(added)
    }
}

/// Manages all topics in the system
#[derive(Debug, Clone)]
pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, Arc<Topic>>>>,
    config: Config,
    tiered_storage: Option<Arc<TieredStorage>>,
}

/// Metadata file name for topic persistence
const TOPIC_METADATA_FILE: &str = "topic_metadata.json";

impl TopicManager {
    /// Create a new topic manager and recover any existing topics from disk
    pub fn new(config: Config) -> Self {
        info!(
            "Creating TopicManager with {} default partitions (tiered_storage: disabled)",
            config.default_partitions
        );

        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            config,
            tiered_storage: None,
        }
    }

    /// Create a new topic manager with tiered storage support
    pub fn new_with_tiered_storage(config: Config, tiered_storage: Arc<TieredStorage>) -> Self {
        info!(
            "Creating TopicManager with {} default partitions (tiered_storage: enabled)",
            config.default_partitions
        );

        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            config,
            tiered_storage: Some(tiered_storage),
        }
    }

    /// Check if tiered storage is enabled
    pub fn has_tiered_storage(&self) -> bool {
        self.tiered_storage.is_some()
    }

    /// Get tiered storage statistics
    pub fn tiered_storage_stats(&self) -> Option<crate::storage::TieredStorageStatsSnapshot> {
        self.tiered_storage.as_ref().map(|ts| ts.stats())
    }

    /// Initialize and recover topics from disk
    /// This should be called after construction to restore persisted topics
    pub async fn recover(&self) -> Result<usize> {
        if !self.config.enable_persistence {
            info!("Persistence disabled, skipping topic recovery");
            return Ok(0);
        }

        let data_dir = PathBuf::from(&self.config.data_dir);
        let metadata_path = data_dir.join(TOPIC_METADATA_FILE);

        // Try to load metadata file
        if metadata_path.exists() {
            match fs::read_to_string(&metadata_path).await {
                Ok(content) => match serde_json::from_str::<Vec<TopicMetadata>>(&content) {
                    Ok(topics_metadata) => {
                        let count = topics_metadata.len();
                        info!("Recovering {} topics from metadata file", count);

                        for meta in topics_metadata {
                            if let Err(e) = self.recover_topic(&meta).await {
                                warn!("Failed to recover topic '{}': {}", meta.name, e);
                            }
                        }

                        return Ok(count);
                    }
                    Err(e) => {
                        warn!("Failed to parse topic metadata: {}", e);
                    }
                },
                Err(e) => {
                    warn!("Failed to read topic metadata file: {}", e);
                }
            }
        }

        // Fallback: scan data directory for topic directories
        self.recover_from_directory_scan().await
    }

    /// Recover a single topic from metadata
    async fn recover_topic(&self, meta: &TopicMetadata) -> Result<()> {
        let mut topics = self.topics.write().await;

        if topics.contains_key(&meta.name) {
            return Ok(()); // Already recovered
        }

        info!(
            "Recovering topic '{}' with {} partitions",
            meta.name, meta.num_partitions
        );

        let topic = Arc::new(
            Topic::new_with_tiered_storage(
                &self.config,
                meta.name.clone(),
                meta.num_partitions,
                self.tiered_storage.clone(),
            )
            .await?,
        );
        topics.insert(meta.name.clone(), topic);

        Ok(())
    }

    /// Scan data directory for existing topic directories (fallback recovery)
    async fn recover_from_directory_scan(&self) -> Result<usize> {
        let data_dir = PathBuf::from(&self.config.data_dir);

        if !data_dir.exists() {
            return Ok(0);
        }

        let mut recovered = 0;
        let mut entries = match fs::read_dir(&data_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!("Failed to read data directory: {}", e);
                return Ok(0);
            }
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Skip internal directories
            if dir_name.starts_with('_') || dir_name.starts_with('.') {
                continue;
            }

            // Check if this looks like a topic directory by looking for partition subdirs
            let mut partition_count = 0u32;
            if let Ok(mut topic_entries) = fs::read_dir(&path).await {
                while let Ok(Some(partition_entry)) = topic_entries.next_entry().await {
                    let partition_path = partition_entry.path();
                    if partition_path.is_dir() {
                        if let Some(name) = partition_path.file_name().and_then(|n| n.to_str()) {
                            if name.starts_with("partition-") {
                                partition_count += 1;
                            }
                        }
                    }
                }
            }

            if partition_count > 0 {
                info!(
                    "Discovered topic '{}' with {} partitions from directory scan",
                    dir_name, partition_count
                );

                let meta = TopicMetadata {
                    name: dir_name,
                    num_partitions: partition_count,
                    created_at: 0, // Unknown
                };

                if let Err(e) = self.recover_topic(&meta).await {
                    warn!("Failed to recover topic '{}': {}", meta.name, e);
                } else {
                    recovered += 1;
                }
            }
        }

        // Save discovered topics to metadata file for faster recovery next time
        if recovered > 0 {
            let _ = self.persist_metadata().await;
        }

        Ok(recovered)
    }

    /// Persist topic metadata to disk
    async fn persist_metadata(&self) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let data_dir = PathBuf::from(&self.config.data_dir);
        fs::create_dir_all(&data_dir)
            .await
            .map_err(|e| Error::Other(format!("Failed to create data directory: {}", e)))?;

        let topics = self.topics.read().await;
        let metadata: Vec<TopicMetadata> = topics
            .iter()
            .map(|(name, topic)| TopicMetadata {
                name: name.clone(),
                num_partitions: topic.num_partitions() as u32,
                created_at: chrono::Utc::now().timestamp_millis(),
            })
            .collect();

        let metadata_path = data_dir.join(TOPIC_METADATA_FILE);
        let tmp_path = data_dir.join(format!("{}.tmp", TOPIC_METADATA_FILE));
        let content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| Error::Other(format!("Failed to serialize topic metadata: {}", e)))?;

        // Atomic write via temp file + rename.
        // A crash during write only corrupts the temp file; the original
        // metadata file remains intact for recovery.
        fs::write(&tmp_path, content)
            .await
            .map_err(|e| Error::Other(format!("Failed to write topic metadata temp: {}", e)))?;
        fs::rename(&tmp_path, &metadata_path)
            .await
            .map_err(|e| Error::Other(format!("Failed to rename topic metadata: {}", e)))?;

        info!("Persisted metadata for {} topics", topics.len());
        Ok(())
    }

    /// Create a new topic
    pub async fn create_topic(
        &self,
        name: String,
        num_partitions: Option<u32>,
    ) -> Result<Arc<Topic>> {
        let mut topics = self.topics.write().await;

        if topics.contains_key(&name) {
            return Err(Error::Other(format!("Topic '{}' already exists", name)));
        }

        let num_partitions = num_partitions.unwrap_or(self.config.default_partitions);
        let topic = Arc::new(
            Topic::new_with_tiered_storage(
                &self.config,
                name.clone(),
                num_partitions,
                self.tiered_storage.clone(),
            )
            .await?,
        );

        topics.insert(name.clone(), topic.clone());
        drop(topics); // Release lock before persistence

        // Persist metadata asynchronously
        let _ = self.persist_metadata().await;

        Ok(topic)
    }

    /// Get a topic by name
    pub async fn get_topic(&self, name: &str) -> Result<Arc<Topic>> {
        let topics = self.topics.read().await;
        topics
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TopicNotFound(name.to_string()))
    }

    /// Get or create a topic (race-safe: uses write lock directly)
    pub async fn get_or_create_topic(&self, name: String) -> Result<Arc<Topic>> {
        // Use write lock to atomically check-and-create, avoiding TOCTOU race
        let mut topics = self.topics.write().await;
        if let Some(topic) = topics.get(&name) {
            return Ok(topic.clone());
        }

        let num_partitions = self.config.default_partitions;
        let topic = Arc::new(
            Topic::new_with_tiered_storage(
                &self.config,
                name.clone(),
                num_partitions,
                self.tiered_storage.clone(),
            )
            .await?,
        );

        topics.insert(name.clone(), topic.clone());
        drop(topics); // Release lock before persistence

        // Persist metadata asynchronously
        let _ = self.persist_metadata().await;

        Ok(topic)
    }

    /// List all topics
    pub async fn list_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics.keys().cloned().collect()
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let mut topics = self.topics.write().await;
        topics
            .remove(name)
            .ok_or_else(|| Error::TopicNotFound(name.to_string()))?;
        drop(topics); // Release lock before persistence

        info!("Deleted topic '{}'", name);

        // Update persisted metadata
        let _ = self.persist_metadata().await;

        Ok(())
    }

    /// Flush all topics to disk ensuring durability during shutdown
    pub async fn flush_all(&self) -> Result<()> {
        let topics = self.topics.read().await;
        for (name, topic) in topics.iter() {
            info!("Flushing topic '{}'...", name);
            topic.flush().await?;
        }
        Ok(())
    }

    /// Add partitions to an existing topic.
    ///
    /// Increases the partition count of the topic to `new_partition_count`.
    /// Returns the number of partitions actually added.
    pub async fn add_partitions(&self, name: &str, new_partition_count: u32) -> Result<u32> {
        let topics = self.topics.read().await;
        let topic = topics
            .get(name)
            .ok_or_else(|| Error::TopicNotFound(name.to_string()))?
            .clone();
        drop(topics);

        let added = topic
            .add_partitions(
                &self.config,
                new_partition_count,
                self.tiered_storage.clone(),
            )
            .await?;

        // Update persisted metadata
        let _ = self.persist_metadata().await;

        Ok(added)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn get_test_config() -> Config {
        Config {
            data_dir: format!("/tmp/rivven-test-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_topic_creation() {
        let config = get_test_config();
        let topic = Topic::new(&config, "test-topic".to_string(), 3)
            .await
            .unwrap();
        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
    }

    #[tokio::test]
    async fn test_topic_append_and_read() {
        let config = get_test_config();
        let topic = Topic::new(&config, "test-topic".to_string(), 2)
            .await
            .unwrap();

        let msg = Message::new(Bytes::from("test"));
        let offset = topic.append(0, msg).await.unwrap();
        assert_eq!(offset, 0);

        let messages = topic.read(0, 0, 10).await.unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_topic_manager() {
        let config = get_test_config();
        let manager = TopicManager::new(config);

        let topic = manager
            .create_topic("test".to_string(), None)
            .await
            .unwrap();
        assert_eq!(topic.num_partitions(), 3);

        let retrieved = manager.get_topic("test").await.unwrap();
        assert_eq!(retrieved.name(), "test");

        let topics = manager.list_topics().await;
        assert_eq!(topics.len(), 1);
    }
}
