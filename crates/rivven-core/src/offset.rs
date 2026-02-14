use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Partition to offset mapping
type PartitionOffsets = HashMap<u32, u64>;
/// Topic to partition offsets mapping
type TopicOffsets = HashMap<String, PartitionOffsets>;
/// Consumer group to topic offsets mapping
type GroupOffsets = HashMap<String, TopicOffsets>;

/// Manages consumer offsets for topics and partitions.
///
/// Supports optional file-based persistence — when a `data_dir` is provided,
/// offsets are atomically checkpointed to `<data_dir>/offsets.json` on every
/// commit and loaded on startup.
#[derive(Debug, Clone)]
pub struct OffsetManager {
    /// Map of consumer_group -> topic -> partition -> offset
    offsets: Arc<RwLock<GroupOffsets>>,
    /// Optional persistence path
    data_dir: Option<PathBuf>,
}

impl OffsetManager {
    /// Create a new in-memory offset manager (no persistence)
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
            data_dir: None,
        }
    }

    /// Create an offset manager that persists offsets to disk.
    ///
    /// Loads existing offsets from `<data_dir>/offsets.json` if the file exists.
    pub fn with_persistence(data_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&data_dir).ok();
        let path = data_dir.join("offsets.json");

        let offsets = if path.exists() {
            match std::fs::read_to_string(&path) {
                Ok(content) => match serde_json::from_str::<GroupOffsets>(&content) {
                    Ok(loaded) => {
                        debug!(
                            "Loaded {} consumer group offsets from {}",
                            loaded.len(),
                            path.display()
                        );
                        loaded
                    }
                    Err(e) => {
                        warn!("Failed to parse offsets file {}: {}", path.display(), e);
                        HashMap::new()
                    }
                },
                Err(e) => {
                    warn!("Failed to read offsets file {}: {}", path.display(), e);
                    HashMap::new()
                }
            }
        } else {
            HashMap::new()
        };

        Self {
            offsets: Arc::new(RwLock::new(offsets)),
            data_dir: Some(data_dir),
        }
    }

    /// Atomically checkpoint offsets to disk (if persistence is enabled).
    ///
    /// Filesystem I/O is performed via `spawn_blocking` so that
    /// the tokio runtime thread is never blocked on synchronous `write`/`rename`.
    async fn checkpoint(&self) {
        if let Some(ref dir) = self.data_dir {
            let offsets = self.offsets.read().await;
            let path = dir.join("offsets.json");
            let tmp_path = dir.join("offsets.json.tmp");

            match serde_json::to_string(&*offsets) {
                Ok(json) => {
                    let tmp = tmp_path.clone();
                    let dst = path.clone();
                    let json_clone = json;
                    if let Err(e) = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                        // Write temp file
                        std::fs::write(&tmp, json_clone.as_bytes())?;
                        // F-029: fsync temp file before rename to ensure data is durable
                        {
                            let f = std::fs::File::open(&tmp)?;
                            f.sync_all()?;
                        }
                        // Atomic rename
                        std::fs::rename(&tmp, &dst)?;
                        // F-029: fsync parent directory to ensure rename is durable
                        if let Some(parent) = dst.parent() {
                            if let Ok(dir) = std::fs::File::open(parent) {
                                let _ = dir.sync_all();
                            }
                        }
                        Ok(())
                    })
                    .await
                    {
                        warn!("Offset checkpoint task failed: {}", e);
                    }
                }
                Err(e) => {
                    warn!("Failed to serialize offsets: {}", e);
                }
            }
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
        {
            let mut offsets = self.offsets.write().await;

            offsets
                .entry(consumer_group.to_string())
                .or_insert_with(HashMap::new)
                .entry(topic.to_string())
                .or_insert_with(HashMap::new)
                .insert(partition, offset);
        }

        self.checkpoint().await;
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
        {
            let mut offsets = self.offsets.write().await;
            offsets.remove(consumer_group);
        }
        self.checkpoint().await;
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
        let removed = {
            let mut offsets = self.offsets.write().await;
            offsets.remove(consumer_group).is_some()
        };
        if removed {
            self.checkpoint().await;
        }
        removed
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

    #[tokio::test]
    async fn test_persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        // Commit offsets
        {
            let manager = OffsetManager::with_persistence(data_dir.clone());
            manager.commit_offset("grp1", "orders", 0, 42).await;
            manager.commit_offset("grp1", "orders", 1, 99).await;
            manager.commit_offset("grp2", "events", 0, 7).await;
        }

        // Reload from disk
        let manager = OffsetManager::with_persistence(data_dir);
        assert_eq!(manager.get_offset("grp1", "orders", 0).await, Some(42));
        assert_eq!(manager.get_offset("grp1", "orders", 1).await, Some(99));
        assert_eq!(manager.get_offset("grp2", "events", 0).await, Some(7));
    }

    #[tokio::test]
    async fn test_persistence_delete_group() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let manager = OffsetManager::with_persistence(data_dir.clone());
        manager.commit_offset("grp1", "t", 0, 10).await;
        assert!(manager.delete_group("grp1").await);

        // Reload — should be gone
        let manager2 = OffsetManager::with_persistence(data_dir);
        assert_eq!(manager2.get_offset("grp1", "t", 0).await, None);
    }
}
