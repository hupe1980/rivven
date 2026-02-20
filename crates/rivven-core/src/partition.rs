use crate::metrics::{CoreMetrics, Timer};
use crate::storage::{LogManager, TieredStorage};
use crate::{Config, Error, Message, Result};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Execute deferred fsync via `spawn_blocking`.
///
/// Takes the dup'd file descriptor from `LogManager::take_pending_sync()`
/// and runs `sync_data()` (fdatasync) on a blocking thread pool thread.
/// This must be called AFTER releasing the `LogManager` write lock so that
/// readers and other writers are not blocked during the slow syscall.
async fn deferred_fsync(sync_handle: Option<Arc<std::fs::File>>) -> Result<()> {
    if let Some(sync_fd) = sync_handle {
        tokio::task::spawn_blocking(move || -> std::io::Result<()> { sync_fd.sync_data() })
            .await
            .map_err(|e| Error::Other(format!("sync task join: {}", e)))?
            .map_err(|e| Error::Other(format!("fsync: {}", e)))?;
    }
    Ok(())
}

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

    /// Low watermark: records before this offset are logically deleted.
    /// Set via `set_low_watermark()` (e.g., from DeleteRecords API).
    low_watermark: AtomicU64,
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
        let mut log_manager = LogManager::with_sync_policy(
            base_dir,
            topic,
            id,
            config.max_segment_size,
            config.sync_policy,
        )
        .await?;

        // Recover offset from storage
        let recovered_offset = log_manager.recover_next_offset().await?;
        let next_offset = AtomicU64::new(recovered_offset);

        Ok(Self {
            topic: topic.to_string(),
            id,
            log_manager: Arc::new(RwLock::new(log_manager)),
            tiered_storage,
            next_offset,
            low_watermark: AtomicU64::new(0),
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

        // Acquire write lock first, then allocate offset to avoid gaps on failure
        let mut log = self.log_manager.write().await;

        // Lock-free offset allocation - single atomic operation
        // Using AcqRel: ensures our write is visible to other threads (Release)
        // and we see all previous writes (Acquire). SeqCst is unnecessary here.
        let offset = self.next_offset.fetch_add(1, Ordering::AcqRel);

        message.offset = offset;

        // Pre-serialize for tiered storage BEFORE consuming the message,
        // avoiding a full clone. Only serialize if tiered storage is enabled.
        let tiered_bytes = if self.tiered_storage.is_some() {
            match message.to_bytes() {
                Ok(data) => Some(Bytes::from(data)),
                Err(e) => {
                    warn!("Failed to serialize for tiered storage: {} (continuing)", e);
                    None
                }
            }
        } else {
            None
        };

        // Write to log manager (primary storage) — consumes message, no clone needed
        if let Err(e) = log.append(offset, message).await {
            // Reclaim the offset on failure. We hold the write lock so no
            // concurrent append can interleave — a simple store suffices.
            self.next_offset.store(offset, Ordering::Release);
            return Err(e);
        }

        // Take the deferred sync handle BEFORE releasing the write lock,
        // then release the lock so readers and other writers can proceed during
        // the slow fdatasync syscall.
        let deferred_sync = log.take_pending_sync();
        drop(log);

        // Deferred fsync — runs via spawn_blocking to avoid blocking the
        // tokio runtime. Readers are unblocked because the RwLock is released.
        // Data is already visible via mmap (flushed to OS page cache above).
        deferred_fsync(deferred_sync).await?;

        // Write pre-serialized bytes to tiered storage (no second serialization)
        if let (Some(tiered), Some(data)) = (&self.tiered_storage, tiered_bytes) {
            if let Err(e) = tiered
                .write(&self.topic, self.id, offset, offset + 1, data)
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

        // Respect low watermark — don't serve records before it
        let wm = self.low_watermark.load(Ordering::Acquire);
        let effective_offset = start_offset.max(wm);

        let log = self.log_manager.read().await;
        // Estimate size: 4KB per message to be safe/generous for the 'max_bytes' parameter of log.read
        let messages = log.read(effective_offset, max_messages * 4096).await?;

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
        self.next_offset.load(Ordering::Acquire)
    }

    pub async fn earliest_offset(&self) -> Option<u64> {
        let log_earliest = {
            let log = self.log_manager.read().await;
            log.earliest_offset()
        };
        let wm = self.low_watermark.load(Ordering::Acquire);
        Some(log_earliest.max(wm))
    }

    /// Set the low watermark for this partition.
    ///
    /// Records before this offset are logically deleted and will not be
    /// returned by `read()`. This implements the DeleteRecords API behavior.
    /// The watermark can only advance forward (monotonically increasing).
    ///
    /// Also triggers physical segment truncation: segments whose data is
    /// entirely below the watermark are deleted from disk to reclaim space.
    pub async fn set_low_watermark(&self, offset: u64) {
        self.low_watermark.fetch_max(offset, Ordering::Release);

        // Physically remove segments that are entirely below the watermark.
        // This reclaims disk space — the logical watermark alone only hides
        // records from consumers.
        let mut log = self.log_manager.write().await;
        match log.truncate_before(offset) {
            Ok(0) => {}
            Ok(n) => {
                info!(
                    "Partition {}/{}: truncated {} segment(s) below watermark {}",
                    self.topic, self.id, n, offset
                );
            }
            Err(e) => {
                warn!(
                    "Partition {}/{}: segment truncation failed: {}",
                    self.topic, self.id, e
                );
            }
        }
    }

    /// Get the current low watermark
    pub fn low_watermark(&self) -> u64 {
        self.low_watermark.load(Ordering::Acquire)
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

        // /Acquire write lock FIRST, then allocate offsets inside
        // the critical section. This prevents out-of-order writes from concurrent
        // callers reserving disjoint offset ranges before acquiring the lock.
        // On failure, use fetch_sub instead of a CAS loop — since we hold the
        // exclusive write lock, no concurrent reservation can interleave.
        let deferred_sync;
        let offsets;
        let batch_data;
        let start_offset;
        {
            let mut log = self.log_manager.write().await;

            // Allocate offsets inside write lock to guarantee monotonic ordering
            start_offset = self
                .next_offset
                .fetch_add(batch_size as u64, Ordering::SeqCst);

            let mut local_offsets = Vec::with_capacity(batch_size);
            let mut batch_messages = Vec::with_capacity(batch_size);
            let mut local_batch_data = Vec::new();

            // Prepare messages with offsets
            for (i, mut message) in messages.into_iter().enumerate() {
                let offset = start_offset + i as u64;
                message.offset = offset;

                // Collect data for tiered storage
                if self.tiered_storage.is_some() {
                    match message.to_bytes() {
                        Ok(data) => local_batch_data.extend_from_slice(&data),
                        Err(e) => tracing::warn!(
                            offset = offset,
                            error = %e,
                            "Failed to serialize message for tiered storage"
                        ),
                    }
                }

                batch_messages.push((offset, message));
                local_offsets.push(offset);
            }

            if let Err(e) = log.append_batch(batch_messages).await {
                // Simple fetch_sub rollback — safe because we hold the
                // exclusive write lock, so no concurrent offset reservation exists.
                self.next_offset
                    .fetch_sub(batch_size as u64, Ordering::SeqCst);
                return Err(e);
            }

            // Take sync handle before releasing write lock
            deferred_sync = log.take_pending_sync();
            offsets = local_offsets;
            batch_data = local_batch_data;
        }

        // Deferred fsync — readers and other writers are unblocked
        deferred_fsync(deferred_sync).await?;

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

    /// Append a replicated message preserving the leader-assigned offset.
    ///
    /// Used by followers (ISR replication) to persist records fetched from the
    /// leader. Unlike `append()`, this does NOT allocate a new offset — it uses
    /// the offset already set on the message by the leader.
    ///
    /// The `next_offset` counter is advanced to `max(current, msg.offset + 1)`
    /// so that if this node is later promoted to leader, offset allocation is
    /// monotonically increasing.
    pub async fn append_replicated(&self, message: Message) -> Result<u64> {
        let offset = message.offset;

        // Write to log manager preserving leader-assigned offset
        let deferred_sync;
        {
            let mut log = self.log_manager.write().await;
            log.append(offset, message).await?;
            // Take sync handle before releasing write lock
            deferred_sync = log.take_pending_sync();
        }

        // Deferred fsync — unblocks readers during fdatasync
        deferred_fsync(deferred_sync).await?;

        // Advance next_offset to max(current, offset + 1) so that a future
        // leader promotion allocates offsets beyond what was replicated.
        self.next_offset.fetch_max(offset + 1, Ordering::Release);

        Ok(offset)
    }

    /// Append a batch of replicated messages preserving leader-assigned offsets.
    ///
    /// Batch variant of `append_replicated` for efficient follower writes.
    pub async fn append_replicated_batch(&self, messages: Vec<Message>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        let batch: Vec<(u64, Message)> = messages
            .into_iter()
            .map(|m| {
                let offset = m.offset;
                (offset, m)
            })
            .collect();

        let max_offset = batch.iter().map(|(o, _)| *o).max().unwrap_or(0);

        let deferred_sync;
        let positions = {
            let mut log = self.log_manager.write().await;
            let pos = log.append_batch(batch).await?;
            // Take sync handle before releasing write lock
            deferred_sync = log.take_pending_sync();
            pos
        };

        // Deferred fsync — unblocks readers during fdatasync
        deferred_fsync(deferred_sync).await?;

        // Advance next_offset past the highest replicated offset
        self.next_offset
            .fetch_max(max_offset + 1, Ordering::Release);

        Ok(positions)
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

    /// Run key-based log compaction on sealed segments.
    ///
    /// Keeps only the latest value per key and removes tombstones (empty value).
    /// Only sealed (non-active) segments are compacted — the active segment is
    /// untouched. Returns the number of messages removed.
    pub async fn compact(&self) -> Result<usize> {
        let mut log = self.log_manager.write().await;
        let removed = log.compact().await?;
        let deferred_sync = log.take_pending_sync();
        drop(log);
        deferred_fsync(deferred_sync).await?;
        Ok(removed)
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
