use crate::metrics::{CoreMetrics, Timer};
use crate::storage::{LogManager, TieredStorage};
use crate::{Config, Message, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// A single partition within a topic
#[derive(Debug)]
pub struct Partition {
    /// Topic name (for tiered storage)
    topic: String,

    /// Partition ID
    id: u32,

    /// Storage Manager
    log_manager: Arc<RwLock<LogManager>>,

    /// Tiered storage (optional, for hot/warm/cold data tiering)
    tiered_storage: Option<Arc<TieredStorage>>,

    /// Current offset (next offset to be assigned)
    /// Lock-free atomic for 5-10x throughput improvement
    next_offset: AtomicU64,
}

impl Partition {
    /// Create a new partition
    pub async fn new(config: &Config, topic: &str, id: u32) -> Result<Self> {
        Self::new_with_tiered_storage(config, topic, id, None).await
    }

    /// Create a new partition with optional tiered storage
    pub async fn new_with_tiered_storage(
        config: &Config,
        topic: &str,
        id: u32,
        tiered_storage: Option<Arc<TieredStorage>>,
    ) -> Result<Self> {
        info!(
            "Creating partition {} for topic {} (tiered_storage: {})",
            id,
            topic,
            tiered_storage.is_some()
        );
        let base_dir = std::path::PathBuf::from(&config.data_dir);
        let log_manager = LogManager::new(base_dir, topic, id, config.max_segment_size).await?;

        // Recover offset from storage
        let recovered_offset = log_manager.recover_next_offset().await?;
        let next_offset = AtomicU64::new(recovered_offset);

        Ok(Self {
            topic: topic.to_string(),
            id,
            log_manager: Arc::new(RwLock::new(log_manager)),
            tiered_storage,
            next_offset,
        })
    }

    /// Get the partition ID
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Check if tiered storage is enabled
    pub fn has_tiered_storage(&self) -> bool {
        self.tiered_storage.is_some()
    }

    /// Append a message to the partition
    /// Lock-free implementation using AtomicU64 for 5-10x throughput
    pub async fn append(&self, mut message: Message) -> Result<u64> {
        let timer = Timer::new();

        // Lock-free offset allocation - single atomic operation
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);

        message.offset = offset;

        // Write to log manager (primary storage)
        {
            let mut log = self.log_manager.write().await;
            log.append(offset, message.clone()).await?;
        }

        // Also write to tiered storage if enabled
        if let Some(tiered) = &self.tiered_storage {
            let data = message.to_bytes()?;
            if let Err(e) = tiered
                .write(&self.topic, self.id, offset, offset + 1, Bytes::from(data))
                .await
            {
                // Log warning but don't fail - log manager has the authoritative copy
                warn!(
                    "Failed to write to tiered storage: {} (data safe in log)",
                    e
                );
            }
        }

        // Record metrics
        CoreMetrics::increment_messages_appended();
        CoreMetrics::record_append_latency_us(timer.elapsed_us());

        debug!(
            "Appended message at offset {} to partition {}",
            offset, self.id
        );

        Ok(offset)
    }

    /// Read messages from a given offset
    pub async fn read(&self, start_offset: u64, max_messages: usize) -> Result<Vec<Message>> {
        let timer = Timer::new();

        let log = self.log_manager.read().await;
        // Estimate size: 4KB per message to be safe/generous for the 'max_bytes' parameter of log.read
        let messages = log.read(start_offset, max_messages * 4096).await?;

        let result: Vec<Message> = messages.into_iter().take(max_messages).collect();

        // Record metrics
        CoreMetrics::add_messages_read(result.len() as u64);
        CoreMetrics::record_read_latency_us(timer.elapsed_us());

        debug!(
            "Read {} messages from partition {} starting at offset {}",
            result.len(),
            self.id,
            start_offset
        );

        Ok(result)
    }

    /// Get the latest offset
    pub async fn latest_offset(&self) -> u64 {
        self.next_offset.load(Ordering::SeqCst)
    }

    pub async fn earliest_offset(&self) -> Option<u64> {
        let log = self.log_manager.read().await;
        Some(log.earliest_offset())
    }

    pub async fn message_count(&self) -> usize {
        let earliest = self.earliest_offset().await.unwrap_or(0);
        let next = self.next_offset.load(Ordering::SeqCst);
        (next.saturating_sub(earliest)) as usize
    }

    /// Batch append multiple messages for 20-50x throughput improvement
    /// Single fsync per batch instead of per-message
    pub async fn append_batch(&self, messages: Vec<Message>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let timer = Timer::new();
        let batch_size = messages.len();

        // Allocate offsets atomically for entire batch
        let start_offset = self
            .next_offset
            .fetch_add(batch_size as u64, Ordering::SeqCst);

        let mut offsets = Vec::with_capacity(batch_size);
        let mut batch_messages = Vec::with_capacity(batch_size);
        let mut batch_data = Vec::new();

        // Prepare messages with offsets
        for (i, mut message) in messages.into_iter().enumerate() {
            let offset = start_offset + i as u64;
            message.offset = offset;

            // Collect data for tiered storage
            if self.tiered_storage.is_some() {
                if let Ok(data) = message.to_bytes() {
                    batch_data.extend_from_slice(&data);
                }
            }

            batch_messages.push((offset, message));
            offsets.push(offset);
        }

        // Write to log manager using optimized batch append
        {
            let mut log = self.log_manager.write().await;
            log.append_batch(batch_messages).await?;
        }

        // Also write to tiered storage if enabled
        if let Some(tiered) = &self.tiered_storage {
            if !batch_data.is_empty() {
                let end_offset = start_offset + batch_size as u64;
                if let Err(e) = tiered
                    .write(
                        &self.topic,
                        self.id,
                        start_offset,
                        end_offset,
                        Bytes::from(batch_data),
                    )
                    .await
                {
                    // Log warning but don't fail - log manager has the authoritative copy
                    warn!(
                        "Failed to write batch to tiered storage: {} (data safe in log)",
                        e
                    );
                }
            }
        }

        // Record metrics
        CoreMetrics::increment_batch_appends();
        CoreMetrics::add_messages_appended(batch_size as u64);
        CoreMetrics::record_batch_append_latency_us(timer.elapsed_us());

        debug!(
            "Batch appended {} messages to partition {} (offsets {}-{})",
            batch_size,
            self.id,
            start_offset,
            start_offset + batch_size as u64 - 1
        );

        Ok(offsets)
    }

    /// Flush partition data to disk ensuring durability
    pub async fn flush(&self) -> Result<()> {
        let log = self.log_manager.read().await;
        log.flush().await?;

        // Also flush tiered storage hot tier if enabled
        if let Some(tiered) = &self.tiered_storage {
            tiered.flush_hot_tier(&self.topic, self.id).await?;
        }

        Ok(())
    }

    /// Find the first offset with timestamp >= target_timestamp (milliseconds since epoch)
    /// Returns None if no matching offset is found.
    pub async fn find_offset_for_timestamp(&self, target_timestamp: i64) -> Result<Option<u64>> {
        let log = self.log_manager.read().await;
        log.find_offset_for_timestamp(target_timestamp).await
    }

    /// Get tiered storage statistics for this partition
    pub fn tiered_storage_stats(&self) -> Option<crate::storage::TieredStorageStatsSnapshot> {
        self.tiered_storage.as_ref().map(|ts| ts.stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use bytes::Bytes;
    use std::fs;

    fn get_test_config() -> Config {
        let config = Config {
            data_dir: format!("/tmp/rivven-test-partition-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let _ = fs::remove_dir_all(&config.data_dir);
        config
    }

    #[tokio::test]
    async fn test_partition_persistence() {
        let config = get_test_config();
        let topic = "test-topic";
        let part_id = 0;

        // 1. Create partition and write messages
        {
            let partition = Partition::new(&config, topic, part_id).await.unwrap();

            partition
                .append(Message::new(Bytes::from("msg1")))
                .await
                .unwrap();
            partition
                .append(Message::new(Bytes::from("msg2")))
                .await
                .unwrap();

            let stored = partition.read(0, 10).await.unwrap();
            assert_eq!(stored.len(), 2);
            assert_eq!(stored[0].value, Bytes::from("msg1"));
            assert_eq!(stored[1].value, Bytes::from("msg2"));
        }

        // 2. Re-open partition to test persistence and recovery
        {
            let partition = Partition::new(&config, topic, part_id).await.unwrap();

            // Check next offset
            assert_eq!(partition.latest_offset().await, 2);

            // Read old messages
            let stored = partition.read(0, 10).await.unwrap();
            assert_eq!(stored.len(), 2);
            assert_eq!(stored[0].value, Bytes::from("msg1"));

            // Append new message
            partition
                .append(Message::new(Bytes::from("msg3")))
                .await
                .unwrap();
            let stored = partition.read(0, 10).await.unwrap();
            assert_eq!(stored.len(), 3);
            assert_eq!(stored[2].value, Bytes::from("msg3"));
        }

        fs::remove_dir_all(&config.data_dir).unwrap();
    }

    #[tokio::test]
    async fn test_find_offset_for_timestamp() {
        let config = get_test_config();
        let topic = "test-topic-ts";
        let part_id = 0;

        let partition = Partition::new(&config, topic, part_id).await.unwrap();

        // Append some messages
        for i in 0..5 {
            let msg = Message::new(Bytes::from(format!("msg{}", i)));
            partition.append(msg).await.unwrap();
            // Small delay to ensure distinct timestamps
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Read messages to get their timestamps
        let messages = partition.read(0, 10).await.unwrap();
        assert_eq!(messages.len(), 5);

        // Get timestamp of the third message (offset 2)
        let ts_msg2 = messages[2].timestamp.timestamp_millis();

        // Find offset for that timestamp - should return offset 2
        let found_offset = partition.find_offset_for_timestamp(ts_msg2).await.unwrap();
        assert_eq!(
            found_offset,
            Some(2),
            "Should find offset 2 for timestamp {}",
            ts_msg2
        );

        // Find offset for timestamp before all messages - should return offset 0
        let very_old_ts = ts_msg2 - 10000; // 10 seconds before
        let found_offset = partition
            .find_offset_for_timestamp(very_old_ts)
            .await
            .unwrap();
        assert_eq!(
            found_offset,
            Some(0),
            "Should find offset 0 for very old timestamp"
        );

        // Find offset for timestamp in the future - should return None
        let future_ts = chrono::Utc::now().timestamp_millis() + 60000; // 1 minute in future
        let found_offset = partition
            .find_offset_for_timestamp(future_ts)
            .await
            .unwrap();
        assert_eq!(
            found_offset, None,
            "Should return None for future timestamp"
        );

        fs::remove_dir_all(&config.data_dir).unwrap();
    }
}
