//! Sticky Partitioner
//!
//! Implements Kafka-style sticky partitioning for keyless messages.
//! Messages without keys are batched to a "sticky" partition until:
//! - A configurable batch size is reached
//! - A time threshold expires
//!
//! This provides better batching efficiency while maintaining good
//! partition distribution over time.
//!
//! ## Design
//!
//! - **With key**: Hash-based partitioning (deterministic, same key → same partition)
//! - **Without key**: Sticky partitioning (batches to one partition, rotates periodically)
//!
//! ## Benefits over pure round-robin
//!
//! - Better batching efficiency (more messages per network call)
//! - Lower producer latency
//! - Still achieves good partition balance over time

use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for sticky partitioner
#[derive(Debug, Clone)]
pub struct StickyPartitionerConfig {
    /// Number of messages before switching partitions (0 = time-based only)
    pub batch_size: u32,
    /// Duration before switching partitions
    pub linger_duration: Duration,
}

impl Default for StickyPartitionerConfig {
    fn default() -> Self {
        Self {
            batch_size: 16 * 1024, // 16K messages per batch (Kafka default-ish)
            linger_duration: Duration::from_millis(100), // 100ms linger time
        }
    }
}

/// Per-topic sticky state
struct TopicStickyState {
    /// Current sticky partition for this topic
    current_partition: AtomicU32,
    /// Messages sent to current partition
    messages_in_batch: AtomicU64,
    /// When the current batch started (nanos since process start, via Instant)
    batch_start_nanos: AtomicU64,
}

/// Baseline instant for converting `Instant` to/from AtomicU64.
/// Using a thread-local or global LazyLock to avoid repeated system calls.
static EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

impl TopicStickyState {
    fn new(initial_partition: u32) -> Self {
        Self {
            current_partition: AtomicU32::new(initial_partition),
            messages_in_batch: AtomicU64::new(0),
            batch_start_nanos: AtomicU64::new(Self::now_nanos()),
        }
    }

    /// Get current time as nanos since process EPOCH
    #[inline]
    fn now_nanos() -> u64 {
        EPOCH.elapsed().as_nanos() as u64
    }

    /// Get elapsed duration since batch start
    #[inline]
    fn batch_elapsed(&self) -> Duration {
        let start = self.batch_start_nanos.load(Ordering::Relaxed);
        let now = Self::now_nanos();
        Duration::from_nanos(now.saturating_sub(start))
    }

    /// Reset batch start to now
    #[inline]
    fn reset_batch_start(&self) {
        self.batch_start_nanos
            .store(Self::now_nanos(), Ordering::Relaxed);
    }
}

/// Sticky partitioner for keyless message distribution
///
/// Implements Kafka 2.4+ style sticky partitioning:
/// - Messages with keys use hash-based partitioning
/// - Messages without keys stick to one partition until batch/time threshold
///
/// F-045 fix: Uses `DashMap` instead of `RwLock<HashMap>` for per-shard locking.
/// Multi-topic workloads no longer contend on a single global lock.
pub struct StickyPartitioner {
    config: StickyPartitionerConfig,
    /// Per-topic sticky state (DashMap provides per-shard locking)
    topic_states: DashMap<String, TopicStickyState>,
    /// Global counter for initial partition selection (ensures even distribution)
    global_counter: AtomicU32,
}

impl StickyPartitioner {
    /// Create a new sticky partitioner with default config
    pub fn new() -> Self {
        Self::with_config(StickyPartitionerConfig::default())
    }

    /// Create a sticky partitioner with custom config
    pub fn with_config(config: StickyPartitionerConfig) -> Self {
        Self {
            config,
            topic_states: DashMap::new(),
            global_counter: AtomicU32::new(0),
        }
    }

    /// Get partition for a message
    ///
    /// - If `key` is Some, uses hash-based partitioning (deterministic)
    /// - If `key` is None, uses sticky partitioning (batched rotation)
    pub fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }

        match key {
            Some(k) => self.hash_partition(k, num_partitions),
            None => self.sticky_partition(topic, num_partitions),
        }
    }

    /// Hash-based partitioning for keyed messages
    /// Uses murmur2-style hash for Kafka compatibility
    fn hash_partition(&self, key: &[u8], num_partitions: u32) -> u32 {
        // Delegate to the shared canonical implementation
        rivven_core::hash::murmur2_partition(key, num_partitions)
    }

    /// Sticky partitioning for keyless messages
    fn sticky_partition(&self, topic: &str, num_partitions: u32) -> u32 {
        // Fast path: check existing entry via DashMap shard lock (no global lock)
        if let Some(state) = self.topic_states.get(topic) {
            if !self.should_rotate(&state) {
                state.messages_in_batch.fetch_add(1, Ordering::Relaxed);
                return state.current_partition.load(Ordering::Relaxed) % num_partitions;
            }

            // Rotate in-place (we hold a read ref, but interior mutability via atomics)
            let current = state.current_partition.load(Ordering::Relaxed);
            let next = (current + 1) % num_partitions;
            state.current_partition.store(next, Ordering::Relaxed);
            state.messages_in_batch.store(1, Ordering::Relaxed);
            state.reset_batch_start();
            return next;
        }

        // Slow path: initialize new topic entry
        let initial = self.global_counter.fetch_add(1, Ordering::Relaxed) % num_partitions;
        let state = TopicStickyState::new(initial);
        state.messages_in_batch.store(1, Ordering::Relaxed);
        self.topic_states.insert(topic.to_string(), state);
        initial
    }

    /// Check if we should rotate to a new partition
    fn should_rotate(&self, state: &TopicStickyState) -> bool {
        // Check batch size threshold
        if self.config.batch_size > 0 {
            let count = state.messages_in_batch.load(Ordering::Relaxed);
            if count >= self.config.batch_size as u64 {
                return true;
            }
        }

        // Check time threshold
        state.batch_elapsed() >= self.config.linger_duration
    }

    /// Remove state for a topic (called on topic deletion to prevent L-9 memory leak)
    pub fn reset_topic(&self, topic: &str) {
        self.topic_states.remove(topic);
    }

    /// Remove state for topics no longer in the active set.
    ///
    /// Call periodically or on topic deletion to prevent
    /// the `topic_states` map from growing without bound.
    pub fn retain_topics(&self, active_topics: &std::collections::HashSet<String>) {
        self.topic_states
            .retain(|topic, _| active_topics.contains(topic));
    }
}

impl Default for StickyPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

/// Murmur2 hash — delegates to the canonical shared implementation
/// in `rivven_core::hash` to guarantee client/server partition agreement.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyed_messages_same_partition() {
        let partitioner = StickyPartitioner::new();
        let key = b"user-123";

        // Same key should always go to same partition
        let p1 = partitioner.partition("topic", Some(key), 10);
        let p2 = partitioner.partition("topic", Some(key), 10);
        let p3 = partitioner.partition("topic", Some(key), 10);

        assert_eq!(p1, p2);
        assert_eq!(p2, p3);
    }

    #[test]
    fn test_keyless_messages_sticky() {
        let config = StickyPartitionerConfig {
            batch_size: 100,
            linger_duration: Duration::from_secs(60), // Long duration to test batch size
        };
        let partitioner = StickyPartitioner::with_config(config);

        // First 100 messages should go to same partition
        let mut partitions = Vec::new();
        for _ in 0..100 {
            partitions.push(partitioner.partition("topic", None, 10));
        }

        // All should be the same (batch not exceeded)
        let first = partitions[0];
        assert!(
            partitions.iter().all(|&p| p == first),
            "Messages within batch should go to same partition"
        );
    }

    #[test]
    fn test_batch_rotation() {
        let config = StickyPartitionerConfig {
            batch_size: 10, // Small batch for testing
            linger_duration: Duration::from_secs(60),
        };
        let partitioner = StickyPartitioner::with_config(config);

        // Send 25 messages - should span 3 batches
        let mut partitions = Vec::new();
        for _ in 0..25 {
            partitions.push(partitioner.partition("topic", None, 10));
        }

        // Should have rotated at least twice
        let unique: std::collections::HashSet<_> = partitions.iter().collect();
        assert!(unique.len() >= 2, "Should have rotated partitions");
    }

    #[test]
    fn test_different_topics_different_partitions() {
        let partitioner = StickyPartitioner::new();

        // Different topics should potentially start on different partitions
        let p1 = partitioner.partition("topic-a", None, 100);
        let p2 = partitioner.partition("topic-b", None, 100);
        let p3 = partitioner.partition("topic-c", None, 100);

        // At least some should be different (global counter ensures distribution)
        assert!(
            p1 != p2 || p2 != p3 || p1 != p3,
            "Different topics should get different initial partitions"
        );
    }

    #[test]
    fn test_murmur2_deterministic() {
        // Test that same key always produces same hash (using shared impl)
        let key = b"test-key-12345";
        let h1 = rivven_core::hash::murmur2(key);
        let h2 = rivven_core::hash::murmur2(key);
        assert_eq!(h1, h2, "Same key should produce same hash");

        // Different keys should (likely) produce different hashes
        let h3 = rivven_core::hash::murmur2(b"different-key");
        assert_ne!(h1, h3, "Different keys should produce different hashes");
    }

    #[test]
    fn test_key_distribution() {
        // Test that keys distribute well across partitions
        let partitioner = StickyPartitioner::new();
        let num_partitions = 8;
        let mut counts = vec![0u32; num_partitions as usize];

        // Generate 1000 different keys
        for i in 0..1000 {
            let key = format!("user-{}", i);
            let partition = partitioner.partition("topic", Some(key.as_bytes()), num_partitions);
            counts[partition as usize] += 1;
        }

        // Check that distribution is reasonably even (no partition has < 50 or > 200)
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count >= 50,
                "Partition {} only got {} keys (expected >= 50)",
                i,
                count
            );
            assert!(
                count <= 200,
                "Partition {} got {} keys (expected <= 200)",
                i,
                count
            );
        }
    }
}
