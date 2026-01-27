use crate::{Config, Error, Message, Partition, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Represents a topic with multiple partitions
#[derive(Debug)]
pub struct Topic {
    /// Topic name
    name: String,

    /// Partitions in this topic
    partitions: Vec<Arc<Partition>>,
}

impl Topic {
    /// Create a new topic with the specified number of partitions
    pub async fn new(config: &Config, name: String, num_partitions: u32) -> Result<Self> {
        info!(
            "Creating topic '{}' with {} partitions",
            name, num_partitions
        );

        let mut partitions = Vec::new();
        for id in 0..num_partitions {
            partitions.push(Arc::new(Partition::new(config, &name, id).await?));
        }

        Ok(Self { name, partitions })
    }

    /// Get the topic name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    /// Get a specific partition
    pub fn partition(&self, partition_id: u32) -> Result<Arc<Partition>> {
        self.partitions
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
        self.partitions.clone()
    }

    /// Flush all partitions to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        for partition in &self.partitions {
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
}

/// Manages all topics in the system
#[derive(Debug, Clone)]
pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, Arc<Topic>>>>,
    config: Config,
}

impl TopicManager {
    /// Create a new topic manager
    pub fn new(config: Config) -> Self {
        info!(
            "Creating TopicManager with {} default partitions",
            config.default_partitions
        );

        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
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
        let topic = Arc::new(Topic::new(&self.config, name.clone(), num_partitions).await?);

        topics.insert(name, topic.clone());
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

    /// Get or create a topic
    pub async fn get_or_create_topic(&self, name: String) -> Result<Arc<Topic>> {
        {
            let topics = self.topics.read().await;
            if let Some(topic) = topics.get(&name) {
                return Ok(topic.clone());
            }
        }

        self.create_topic(name, None).await
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

        info!("Deleted topic '{}'", name);
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
