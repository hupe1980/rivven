use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Partition to offset mapping
type PartitionOffsets = HashMap<u32, u64>;
/// Topic to partition offsets mapping
type TopicOffsets = HashMap<String, PartitionOffsets>;
/// Consumer group to topic offsets mapping
type GroupOffsets = HashMap<String, TopicOffsets>;

/// Manages consumer offsets for topics and partitions
#[derive(Debug, Clone)]
pub struct OffsetManager {
    /// Map of consumer_group -> topic -> partition -> offset
    offsets: Arc<RwLock<GroupOffsets>>,
}

impl OffsetManager {
    /// Create a new offset manager
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Commit an offset for a consumer group
    pub async fn commit_offset(
        &self,
        consumer_group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) {
        let mut offsets = self.offsets.write().await;
        
        offsets
            .entry(consumer_group.to_string())
            .or_insert_with(HashMap::new)
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }

    /// Get the committed offset for a consumer group
    pub async fn get_offset(
        &self,
        consumer_group: &str,
        topic: &str,
        partition: u32,
    ) -> Option<u64> {
        let offsets = self.offsets.read().await;
        
        offsets
            .get(consumer_group)
            .and_then(|topics| topics.get(topic))
            .and_then(|partitions| partitions.get(&partition))
            .copied()
    }

    /// Reset offsets for a consumer group
    pub async fn reset_offsets(&self, consumer_group: &str) {
        let mut offsets = self.offsets.write().await;
        offsets.remove(consumer_group);
    }

    /// List all consumer groups with committed offsets
    pub async fn list_groups(&self) -> Vec<String> {
        let offsets = self.offsets.read().await;
        offsets.keys().cloned().collect()
    }

    /// Get all offsets for a consumer group
    /// Returns: topic → partition → offset
    pub async fn get_group_offsets(
        &self,
        consumer_group: &str,
    ) -> Option<HashMap<String, HashMap<u32, u64>>> {
        let offsets = self.offsets.read().await;
        offsets.get(consumer_group).cloned()
    }

    /// Delete a consumer group and all its offsets
    pub async fn delete_group(&self, consumer_group: &str) -> bool {
        let mut offsets = self.offsets.write().await;
        offsets.remove(consumer_group).is_some()
    }
}

impl Default for OffsetManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_offset_management() {
        let manager = OffsetManager::new();
        
        manager.commit_offset("group1", "topic1", 0, 100).await;
        
        let offset = manager.get_offset("group1", "topic1", 0).await;
        assert_eq!(offset, Some(100));

        let missing = manager.get_offset("group1", "topic1", 1).await;
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn test_reset_offsets() {
        let manager = OffsetManager::new();
        
        manager.commit_offset("group1", "topic1", 0, 100).await;
        manager.reset_offsets("group1").await;
        
        let offset = manager.get_offset("group1", "topic1", 0).await;
        assert_eq!(offset, None);
    }
}
