//! # CDC Event Partitioning
//!
//! Strategies for distributing events across partitions while maintaining ordering.
//!
//! ## Features
//!
//! - Multiple partitioning strategies (hash, round-robin, key-based)
//! - Consistent hashing for stable partition assignment
//! - Per-key ordering guarantees
//! - Murmur3 hash for uniform distribution
//!
//! ## Example
//!
//! ```rust,ignore
//! use rivven_cdc::common::partitioner::{Partitioner, PartitionStrategy};
//!
//! let partitioner = Partitioner::new(16, PartitionStrategy::KeyHash(vec!["id".to_string()]));
//! let partition = partitioner.partition(&event);
//! ```

use crate::common::CdcEvent;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

/// Partitioning strategy.
#[derive(Debug, Clone)]
pub enum PartitionStrategy {
    /// Round-robin distribution (no ordering guarantees)
    RoundRobin,
    /// Hash on specific key columns (per-key ordering)
    KeyHash(Vec<String>),
    /// Hash on table name (per-table ordering)
    TableHash,
    /// Hash on schema.table (per-table ordering)
    FullTableHash,
    /// Sticky: same partition for all events (global ordering)
    Sticky(u32),
    /// Custom partitioner function
    Custom(CustomPartitioner),
}

impl Default for PartitionStrategy {
    fn default() -> Self {
        Self::KeyHash(vec!["id".to_string()])
    }
}

/// Custom partitioner function.
#[derive(Clone)]
pub struct CustomPartitioner {
    func: fn(&CdcEvent, u32) -> u32,
}

impl CustomPartitioner {
    pub fn new(func: fn(&CdcEvent, u32) -> u32) -> Self {
        Self { func }
    }
}

impl std::fmt::Debug for CustomPartitioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CustomPartitioner(<fn>)")
    }
}

/// Event partitioner.
pub struct Partitioner {
    num_partitions: u32,
    strategy: PartitionStrategy,
    round_robin_counter: AtomicU64,
}

impl Partitioner {
    /// Create a new partitioner.
    pub fn new(num_partitions: u32, strategy: PartitionStrategy) -> Self {
        Self {
            num_partitions: num_partitions.max(1),
            strategy,
            round_robin_counter: AtomicU64::new(0),
        }
    }

    /// Get partition for an event.
    pub fn partition(&self, event: &CdcEvent) -> u32 {
        match &self.strategy {
            PartitionStrategy::RoundRobin => self.round_robin(),
            PartitionStrategy::KeyHash(columns) => self.key_hash(event, columns),
            PartitionStrategy::TableHash => self.table_hash(event),
            PartitionStrategy::FullTableHash => self.full_table_hash(event),
            PartitionStrategy::Sticky(partition) => *partition % self.num_partitions,
            PartitionStrategy::Custom(custom) => (custom.func)(event, self.num_partitions),
        }
    }

    /// Round-robin partitioning.
    fn round_robin(&self) -> u32 {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
        (counter % self.num_partitions as u64) as u32
    }

    /// Hash on key columns.
    fn key_hash(&self, event: &CdcEvent, columns: &[String]) -> u32 {
        let mut hasher = DefaultHasher::new();

        // Include table in hash for cross-table uniqueness
        event.schema.hash(&mut hasher);
        event.table.hash(&mut hasher);

        // Hash key columns from after or before
        let data = event.after.as_ref().or(event.before.as_ref());
        if let Some(obj) = data {
            for col in columns {
                if let Some(value) = obj.get(col) {
                    value.to_string().hash(&mut hasher);
                }
            }
        }

        let hash = hasher.finish();
        murmur3_finalize(hash) % self.num_partitions
    }

    /// Hash on table name only.
    fn table_hash(&self, event: &CdcEvent) -> u32 {
        let mut hasher = DefaultHasher::new();
        event.table.hash(&mut hasher);
        let hash = hasher.finish();
        murmur3_finalize(hash) % self.num_partitions
    }

    /// Hash on schema.table.
    fn full_table_hash(&self, event: &CdcEvent) -> u32 {
        let mut hasher = DefaultHasher::new();
        event.schema.hash(&mut hasher);
        event.table.hash(&mut hasher);
        let hash = hasher.finish();
        murmur3_finalize(hash) % self.num_partitions
    }

    /// Get number of partitions.
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Get strategy.
    pub fn strategy(&self) -> &PartitionStrategy {
        &self.strategy
    }
}

/// Murmur3 finalization for better distribution.
fn murmur3_finalize(mut h: u64) -> u32 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h as u32
}

/// Consistent hasher for stable partition assignment.
pub struct ConsistentHasher {
    num_partitions: u32,
    virtual_nodes: u32,
    ring: Vec<(u64, u32)>, // (hash, partition)
}

impl ConsistentHasher {
    /// Create a new consistent hasher.
    pub fn new(num_partitions: u32, virtual_nodes: u32) -> Self {
        let virtual_nodes = virtual_nodes.max(1);
        let num_partitions = num_partitions.max(1);

        let mut ring = Vec::with_capacity((num_partitions * virtual_nodes) as usize);

        for partition in 0..num_partitions {
            for vnode in 0..virtual_nodes {
                let key = format!("partition-{}-vnode-{}", partition, vnode);
                let hash = hash_key(&key);
                ring.push((hash, partition));
            }
        }

        ring.sort_by_key(|(hash, _)| *hash);

        Self {
            num_partitions,
            virtual_nodes,
            ring,
        }
    }

    /// Get partition for a key.
    pub fn partition(&self, key: &str) -> u32 {
        if self.ring.is_empty() {
            return 0;
        }

        let hash = hash_key(key);

        // Binary search for first hash >= key hash
        match self.ring.binary_search_by_key(&hash, |(h, _)| *h) {
            Ok(idx) => self.ring[idx].1,
            Err(idx) => {
                if idx >= self.ring.len() {
                    self.ring[0].1 // Wrap around
                } else {
                    self.ring[idx].1
                }
            }
        }
    }

    /// Get partition for an event based on key columns.
    pub fn partition_event(&self, event: &CdcEvent, key_columns: &[String]) -> u32 {
        let key = extract_key(event, key_columns);
        self.partition(&key)
    }

    /// Number of partitions.
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    /// Number of virtual nodes per partition.
    pub fn virtual_nodes(&self) -> u32 {
        self.virtual_nodes
    }
}

/// Hash a key string.
fn hash_key(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Extract key from event.
fn extract_key(event: &CdcEvent, columns: &[String]) -> String {
    let mut parts = vec![event.schema.clone(), event.table.clone()];

    let data = event.after.as_ref().or(event.before.as_ref());
    if let Some(obj) = data {
        for col in columns {
            if let Some(value) = obj.get(col) {
                parts.push(value.to_string());
            }
        }
    }

    parts.join(":")
}

/// Partition assignment result.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    /// Partition number
    pub partition: u32,
    /// Key used for assignment
    pub key: String,
    /// Hash value
    pub hash: u64,
}

impl PartitionAssignment {
    pub fn new(partition: u32, key: String, hash: u64) -> Self {
        Self {
            partition,
            key,
            hash,
        }
    }
}

/// Batch partitioner for efficient bulk partitioning.
pub struct BatchPartitioner {
    partitioner: Partitioner,
}

impl BatchPartitioner {
    pub fn new(num_partitions: u32, strategy: PartitionStrategy) -> Self {
        Self {
            partitioner: Partitioner::new(num_partitions, strategy),
        }
    }

    /// Partition a batch of events.
    pub fn partition_batch<'a>(&self, events: &'a [CdcEvent]) -> Vec<(u32, Vec<&'a CdcEvent>)> {
        let mut partitions: Vec<Vec<&'a CdcEvent>> =
            vec![Vec::new(); self.partitioner.num_partitions as usize];

        for event in events {
            let partition = self.partitioner.partition(event) as usize;
            partitions[partition].push(event);
        }

        partitions
            .into_iter()
            .enumerate()
            .filter(|(_, events)| !events.is_empty())
            .map(|(p, events)| (p as u32, events))
            .collect()
    }

    /// Partition events into owned groups.
    pub fn partition_batch_owned(&self, events: Vec<CdcEvent>) -> Vec<(u32, Vec<CdcEvent>)> {
        let mut partitions: Vec<Vec<CdcEvent>> =
            vec![Vec::new(); self.partitioner.num_partitions as usize];

        for event in events {
            let partition = self.partitioner.partition(&event) as usize;
            partitions[partition].push(event);
        }

        partitions
            .into_iter()
            .enumerate()
            .filter(|(_, events)| !events.is_empty())
            .map(|(p, events)| (p as u32, events))
            .collect()
    }
}

/// Partition statistics.
#[derive(Debug, Default, Clone)]
pub struct PartitionStats {
    /// Events per partition
    pub event_counts: Vec<u64>,
    /// Total events
    pub total_events: u64,
}

impl PartitionStats {
    pub fn new(num_partitions: u32) -> Self {
        Self {
            event_counts: vec![0; num_partitions as usize],
            total_events: 0,
        }
    }

    /// Record an event to a partition.
    pub fn record(&mut self, partition: u32) {
        if (partition as usize) < self.event_counts.len() {
            self.event_counts[partition as usize] += 1;
            self.total_events += 1;
        }
    }

    /// Get distribution (percentage per partition).
    pub fn distribution(&self) -> Vec<f64> {
        if self.total_events == 0 {
            return vec![0.0; self.event_counts.len()];
        }

        self.event_counts
            .iter()
            .map(|&count| (count as f64 / self.total_events as f64) * 100.0)
            .collect()
    }

    /// Get standard deviation of distribution.
    pub fn std_deviation(&self) -> f64 {
        if self.event_counts.is_empty() || self.total_events == 0 {
            return 0.0;
        }

        let mean = self.total_events as f64 / self.event_counts.len() as f64;
        let variance: f64 = self
            .event_counts
            .iter()
            .map(|&count| {
                let diff = count as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / self.event_counts.len() as f64;

        variance.sqrt()
    }

    /// Check if distribution is balanced (within threshold).
    pub fn is_balanced(&self, threshold_percent: f64) -> bool {
        if self.event_counts.is_empty() || self.total_events == 0 {
            return true;
        }

        let mean = self.total_events as f64 / self.event_counts.len() as f64;
        let threshold = mean * (threshold_percent / 100.0);

        self.event_counts
            .iter()
            .all(|&count| (count as f64 - mean).abs() <= threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::CdcOp;

    fn make_event(table: &str, id: i64) -> CdcEvent {
        CdcEvent {
            source_type: "test".to_string(),
            database: "test".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op: CdcOp::Insert,
            before: None,
            after: Some(serde_json::json!({"id": id, "name": format!("user{}", id)})),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[test]
    fn test_partition_strategy_default() {
        let strategy = PartitionStrategy::default();
        match strategy {
            PartitionStrategy::KeyHash(cols) => assert_eq!(cols, vec!["id".to_string()]),
            _ => panic!("Expected KeyHash"),
        }
    }

    #[test]
    fn test_partitioner_round_robin() {
        let partitioner = Partitioner::new(4, PartitionStrategy::RoundRobin);

        let event = make_event("users", 1);

        let p0 = partitioner.partition(&event);
        let p1 = partitioner.partition(&event);
        let p2 = partitioner.partition(&event);
        let p3 = partitioner.partition(&event);
        let p4 = partitioner.partition(&event);

        // Should cycle through partitions
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(p3, 3);
        assert_eq!(p4, 0);
    }

    #[test]
    fn test_partitioner_key_hash() {
        let partitioner = Partitioner::new(16, PartitionStrategy::KeyHash(vec!["id".to_string()]));

        let event1 = make_event("users", 1);
        let event2 = make_event("users", 1); // Same key
        let event3 = make_event("users", 2); // Different key

        let p1 = partitioner.partition(&event1);
        let p2 = partitioner.partition(&event2);
        let p3 = partitioner.partition(&event3);

        // Same key should always go to same partition
        assert_eq!(p1, p2);
        // Different keys might go to different partitions (not guaranteed but likely)
        // At minimum, partitions should be valid
        assert!(p1 < 16);
        assert!(p3 < 16);
    }

    #[test]
    fn test_partitioner_table_hash() {
        let partitioner = Partitioner::new(8, PartitionStrategy::TableHash);

        let event1 = make_event("users", 1);
        let event2 = make_event("users", 2);
        let event3 = make_event("orders", 1);

        let p1 = partitioner.partition(&event1);
        let p2 = partitioner.partition(&event2);
        let p3 = partitioner.partition(&event3);

        // Same table should always go to same partition
        assert_eq!(p1, p2);
        // Different tables might go to different partitions
        assert!(p1 < 8);
        assert!(p3 < 8);
    }

    #[test]
    fn test_partitioner_full_table_hash() {
        let partitioner = Partitioner::new(8, PartitionStrategy::FullTableHash);

        let mut event1 = make_event("users", 1);
        event1.schema = "public".to_string();

        let mut event2 = make_event("users", 1);
        event2.schema = "private".to_string();

        let p1 = partitioner.partition(&event1);
        let p2 = partitioner.partition(&event2);

        // Different schemas should potentially go to different partitions
        assert!(p1 < 8);
        assert!(p2 < 8);
    }

    #[test]
    fn test_partitioner_sticky() {
        let partitioner = Partitioner::new(8, PartitionStrategy::Sticky(5));

        let event1 = make_event("users", 1);
        let event2 = make_event("orders", 2);

        let p1 = partitioner.partition(&event1);
        let p2 = partitioner.partition(&event2);

        // All events should go to partition 5
        assert_eq!(p1, 5);
        assert_eq!(p2, 5);
    }

    #[test]
    fn test_partitioner_custom() {
        fn always_zero(_event: &CdcEvent, _num_partitions: u32) -> u32 {
            0
        }

        let partitioner = Partitioner::new(
            8,
            PartitionStrategy::Custom(CustomPartitioner::new(always_zero)),
        );

        let event = make_event("users", 1);
        assert_eq!(partitioner.partition(&event), 0);
    }

    #[test]
    fn test_consistent_hasher_creation() {
        let hasher = ConsistentHasher::new(4, 100);

        assert_eq!(hasher.num_partitions(), 4);
        assert_eq!(hasher.virtual_nodes(), 100);
    }

    #[test]
    fn test_consistent_hasher_partition() {
        let hasher = ConsistentHasher::new(4, 100);

        let p1 = hasher.partition("key1");
        let p2 = hasher.partition("key1");
        let p3 = hasher.partition("key2");

        // Same key should always map to same partition
        assert_eq!(p1, p2);
        // Partitions should be valid
        assert!(p1 < 4);
        assert!(p3 < 4);
    }

    #[test]
    fn test_consistent_hasher_event() {
        let hasher = ConsistentHasher::new(8, 50);

        let event1 = make_event("users", 1);
        let event2 = make_event("users", 1);
        let event3 = make_event("users", 2);

        let key_columns = vec!["id".to_string()];

        let p1 = hasher.partition_event(&event1, &key_columns);
        let p2 = hasher.partition_event(&event2, &key_columns);
        let p3 = hasher.partition_event(&event3, &key_columns);

        // Same event should map to same partition
        assert_eq!(p1, p2);
        assert!(p1 < 8);
        assert!(p3 < 8);
    }

    #[test]
    fn test_batch_partitioner() {
        let batch_partitioner = BatchPartitioner::new(4, PartitionStrategy::TableHash);

        let events = vec![
            make_event("users", 1),
            make_event("users", 2),
            make_event("orders", 1),
            make_event("orders", 2),
        ];

        let partitions = batch_partitioner.partition_batch(&events);

        // Should have events distributed across partitions
        let total_events: usize = partitions.iter().map(|(_, e)| e.len()).sum();
        assert_eq!(total_events, 4);
    }

    #[test]
    fn test_batch_partitioner_owned() {
        let batch_partitioner =
            BatchPartitioner::new(4, PartitionStrategy::KeyHash(vec!["id".to_string()]));

        let events = vec![
            make_event("users", 1),
            make_event("users", 2),
            make_event("users", 3),
            make_event("users", 4),
        ];

        let partitions = batch_partitioner.partition_batch_owned(events);

        let total_events: usize = partitions.iter().map(|(_, e)| e.len()).sum();
        assert_eq!(total_events, 4);
    }

    #[test]
    fn test_partition_stats() {
        let mut stats = PartitionStats::new(4);

        stats.record(0);
        stats.record(0);
        stats.record(1);
        stats.record(2);
        stats.record(3);

        assert_eq!(stats.total_events, 5);
        assert_eq!(stats.event_counts[0], 2);
        assert_eq!(stats.event_counts[1], 1);
    }

    #[test]
    fn test_partition_stats_distribution() {
        let mut stats = PartitionStats::new(4);

        for _ in 0..25 {
            stats.record(0);
        }
        for _ in 0..25 {
            stats.record(1);
        }
        for _ in 0..25 {
            stats.record(2);
        }
        for _ in 0..25 {
            stats.record(3);
        }

        let dist = stats.distribution();
        assert_eq!(dist.len(), 4);
        for &d in &dist {
            assert!((d - 25.0).abs() < 0.01);
        }
    }

    #[test]
    fn test_partition_stats_std_deviation() {
        let mut stats = PartitionStats::new(4);

        // Perfectly balanced
        for _ in 0..100 {
            stats.record(0);
        }
        for _ in 0..100 {
            stats.record(1);
        }
        for _ in 0..100 {
            stats.record(2);
        }
        for _ in 0..100 {
            stats.record(3);
        }

        let std_dev = stats.std_deviation();
        assert!(std_dev < 0.01); // Should be near zero
    }

    #[test]
    fn test_partition_stats_is_balanced() {
        let mut stats = PartitionStats::new(4);

        // Roughly balanced
        for _ in 0..95 {
            stats.record(0);
        }
        for _ in 0..100 {
            stats.record(1);
        }
        for _ in 0..105 {
            stats.record(2);
        }
        for _ in 0..100 {
            stats.record(3);
        }

        // 10% threshold should pass
        assert!(stats.is_balanced(15.0));
        // 1% threshold might fail
        assert!(!stats.is_balanced(1.0));
    }

    #[test]
    fn test_partition_assignment() {
        let assignment = PartitionAssignment::new(5, "users:1".to_string(), 12345);

        assert_eq!(assignment.partition, 5);
        assert_eq!(assignment.key, "users:1");
        assert_eq!(assignment.hash, 12345);
    }

    #[test]
    fn test_murmur3_finalize() {
        // Test that finalization produces different outputs for different inputs
        let h1 = murmur3_finalize(1);
        let h2 = murmur3_finalize(2);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_partitioner_num_partitions() {
        let partitioner = Partitioner::new(16, PartitionStrategy::RoundRobin);
        assert_eq!(partitioner.num_partitions(), 16);
    }

    #[test]
    fn test_partitioner_min_partitions() {
        // Should enforce minimum of 1
        let partitioner = Partitioner::new(0, PartitionStrategy::RoundRobin);
        assert_eq!(partitioner.num_partitions(), 1);
    }

    #[test]
    fn test_consistent_hasher_distribution() {
        let hasher = ConsistentHasher::new(8, 100);
        let mut stats = PartitionStats::new(8);

        // Partition many keys
        for i in 0..10000 {
            let key = format!("key-{}", i);
            let partition = hasher.partition(&key);
            stats.record(partition);
        }

        // Distribution should be reasonably balanced
        // With 10000 keys across 8 partitions, expect ~1250 per partition
        // Allow 30% variance
        assert!(stats.is_balanced(30.0));
    }

    #[test]
    fn test_custom_partitioner_debug() {
        fn test_fn(_: &CdcEvent, _: u32) -> u32 {
            0
        }
        let custom = CustomPartitioner::new(test_fn);
        let debug_str = format!("{:?}", custom);
        assert!(debug_str.contains("CustomPartitioner"));
    }

    #[test]
    fn test_extract_key() {
        let event = make_event("users", 42);
        let key = extract_key(&event, &["id".to_string()]);

        assert!(key.contains("public"));
        assert!(key.contains("users"));
        assert!(key.contains("42"));
    }
}
