use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Number of offset commits between automatic checkpoints.
///
/// Batching avoids an fsync on every single `commit_offset` call while still
/// bounding the amount of data that could be lost on a crash to at most this
/// many commits.
const CHECKPOINT_INTERVAL: u32 = 50;

/// Partition to offset mapping
type PartitionOffsets = HashMap<u32, u64>;
/// Topic to partition offsets mapping
type TopicOffsets = HashMap<String, PartitionOffsets>;
/// Consumer group to topic offsets mapping
type GroupOffsets = HashMap<String, TopicOffsets>;

/// Manages consumer offsets for topics and partitions.
///
/// Supports optional file-based persistence — when a `data_dir` is provided,
/// offsets are atomically checkpointed to `<data_dir>/offsets.json` and loaded
/// on startup.
///
/// To avoid an expensive fsync on every single commit, checkpoints are batched:
/// offsets are written to disk every [`CHECKPOINT_INTERVAL`] commits. Callers
/// should invoke [`flush()`](Self::flush) during graceful shutdown to persist
/// any remaining uncommitted changes.
#[derive(Debug, Clone)]
pub struct OffsetManager {
    /// Map of consumer_group -> topic -> partition -> offset
    offsets: Arc<RwLock<GroupOffsets>>,
    /// Optional persistence path
    data_dir: Option<PathBuf>,
    /// Number of commits since the last checkpoint (shared across clones).
    pending_commits: Arc<AtomicU32>,
}

impl OffsetManager {
    /// Create a new in-memory offset manager (no persistence)
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
            data_dir: None,
            pending_commits: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Create an offset manager that persists offsets to disk.
    ///
    /// Loads existing offsets from `<data_dir>/offsets.json` if the file exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the data directory cannot be created.
    pub fn with_persistence(data_dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
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

        Ok(Self {
            offsets: Arc::new(RwLock::new(offsets)),
            data_dir: Some(data_dir),
            pending_commits: Arc::new(AtomicU32::new(0)),
        })
    }

    /// Atomically checkpoint offsets to disk (if persistence is enabled).
    ///
    /// Clones the data and drops the read lock before disk I/O to avoid
    /// serializing concurrent `commit_offset` writes.
    async fn checkpoint(&self) {
        if let Some(ref dir) = self.data_dir {
            // Clone snapshot and release lock before any I/O
            let snapshot = { self.offsets.read().await.clone() };
            let path = dir.join("offsets.json");
            let tmp_path = dir.join("offsets.json.tmp");

            match serde_json::to_string(&snapshot) {
                Ok(json) => {
                    let tmp = tmp_path.clone();
                    let dst = path.clone();
                    let json_clone = json;
                    if let Err(e) = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                        // Write temp file
                        std::fs::write(&tmp, json_clone.as_bytes())?;
                        // fsync temp file before rename to ensure data is durable
                        {
                            let f = std::fs::File::open(&tmp)?;
                            f.sync_all()?;
                        }
                        // Atomic rename
                        std::fs::rename(&tmp, &dst)?;
                        // fsync parent directory to ensure rename is durable
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

    /// Commit an offset for a consumer group.
    ///
    /// The in-memory state is updated immediately. The on-disk checkpoint is
    /// written only every [`CHECKPOINT_INTERVAL`] commits to amortize fsync
    /// cost. Call [`flush()`](Self::flush) during shutdown to persist any
    /// remaining changes.
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

        let pending = self.pending_commits.fetch_add(1, Ordering::Relaxed) + 1;
        if pending >= CHECKPOINT_INTERVAL {
            self.pending_commits.store(0, Ordering::Relaxed);
            self.checkpoint().await;
        }
    }

    /// Force an immediate checkpoint of all pending offset changes.
    ///
    /// Should be called during graceful shutdown to ensure no committed
    /// offsets are lost.
    pub async fn flush(&self) {
        if self.pending_commits.swap(0, Ordering::Relaxed) > 0 {
            self.checkpoint().await;
        }
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
            let manager = OffsetManager::with_persistence(data_dir.clone()).unwrap();
            manager.commit_offset("grp1", "orders", 0, 42).await;
            manager.commit_offset("grp1", "orders", 1, 99).await;
            manager.commit_offset("grp2", "events", 0, 7).await;
            manager.flush().await;
        }

        // Reload from disk
        let manager = OffsetManager::with_persistence(data_dir).unwrap();
        assert_eq!(manager.get_offset("grp1", "orders", 0).await, Some(42));
        assert_eq!(manager.get_offset("grp1", "orders", 1).await, Some(99));
        assert_eq!(manager.get_offset("grp2", "events", 0).await, Some(7));
    }

    #[tokio::test]
    async fn test_persistence_delete_group() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let manager = OffsetManager::with_persistence(data_dir.clone()).unwrap();
        manager.commit_offset("grp1", "t", 0, 10).await;
        manager.flush().await;
        assert!(manager.delete_group("grp1").await);

        // Reload — should be gone
        let manager2 = OffsetManager::with_persistence(data_dir).unwrap();
        assert_eq!(manager2.get_offset("grp1", "t", 0).await, None);
    }
}
