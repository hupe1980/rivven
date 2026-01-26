//! # CDC Event Deduplication
//!
//! Idempotent event processing with deduplication.
//!
//! ## Features
//!
//! - **Bloom Filter**: Fast probabilistic duplicate check
//! - **LRU Cache**: Exact duplicate detection for recent events
//! - **Composite Keys**: Dedup by table + PK or custom key
//! - **Time-Based Expiry**: Auto-cleanup of old entries
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{Deduplicator, DeduplicatorConfig, CdcEvent};
//!
//! let config = DeduplicatorConfig::default();
//! let dedup = Deduplicator::new(config);
//!
//! // Check if event is duplicate
//! if dedup.is_duplicate(&event).await {
//!     continue; // Skip duplicate
//! }
//!
//! // Mark as seen
//! dedup.mark_seen(&event).await;
//! ```

use crate::common::CdcEvent;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// Configuration for the deduplicator.
#[derive(Debug, Clone)]
pub struct DeduplicatorConfig {
    /// Maximum number of recent event keys to track (exact dedup)
    pub lru_capacity: usize,
    /// Bloom filter size in bits (probabilistic dedup)
    pub bloom_size_bits: usize,
    /// Number of hash functions for bloom filter
    pub bloom_hash_count: usize,
    /// Time-to-live for seen events
    pub ttl: Duration,
    /// Key extraction strategy
    pub key_strategy: KeyStrategy,
}

impl Default for DeduplicatorConfig {
    fn default() -> Self {
        Self {
            lru_capacity: 100_000,
            bloom_size_bits: 1_000_000, // ~1MB
            bloom_hash_count: 7,
            ttl: Duration::from_secs(3600), // 1 hour
            key_strategy: KeyStrategy::TableAndPrimaryKey,
        }
    }
}

impl DeduplicatorConfig {
    /// High-memory config for exact deduplication.
    pub fn exact() -> Self {
        Self {
            lru_capacity: 1_000_000,
            bloom_size_bits: 0, // No bloom filter
            ttl: Duration::from_secs(7200),
            ..Default::default()
        }
    }

    /// Low-memory config using bloom filter.
    pub fn compact() -> Self {
        Self {
            lru_capacity: 10_000,
            bloom_size_bits: 10_000_000, // ~10MB
            bloom_hash_count: 10,
            ttl: Duration::from_secs(1800),
            ..Default::default()
        }
    }
}

/// Strategy for extracting deduplication keys from events.
#[derive(Debug, Clone)]
pub enum KeyStrategy {
    /// Use table name + primary key columns
    TableAndPrimaryKey,
    /// Use table + all column values
    TableAndAllColumns,
    /// Use transaction ID + LSN
    TransactionPosition,
    /// Custom key extractor
    Custom(fn(&CdcEvent) -> String),
}

impl KeyStrategy {
    /// Extract key from event.
    pub fn extract_key(&self, event: &CdcEvent) -> String {
        match self {
            KeyStrategy::TableAndPrimaryKey => {
                // Use table + id field if present
                let pk = event
                    .after
                    .as_ref()
                    .and_then(|m| m.get("id"))
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| {
                        // Fall back to hash of all values
                        event
                            .after
                            .as_ref()
                            .map(|m| {
                                let mut h = DefaultHasher::new();
                                if let Some(obj) = m.as_object() {
                                    for (k, v) in obj {
                                        k.hash(&mut h);
                                        v.to_string().hash(&mut h);
                                    }
                                }
                                h.finish().to_string()
                            })
                            .unwrap_or_default()
                    });
                format!("{}:{}:{}:{:?}", event.schema, event.table, pk, event.op)
            }
            KeyStrategy::TableAndAllColumns => {
                let mut hasher = DefaultHasher::new();
                event.schema.hash(&mut hasher);
                event.table.hash(&mut hasher);
                format!("{:?}", event.op).hash(&mut hasher);
                if let Some(after) = &event.after {
                    if let Some(obj) = after.as_object() {
                        for (k, v) in obj {
                            k.hash(&mut hasher);
                            v.to_string().hash(&mut hasher);
                        }
                    }
                }
                hasher.finish().to_string()
            }
            KeyStrategy::TransactionPosition => {
                // Use database and timestamp as a fallback for transaction position
                format!("{}:{}:{}", event.database, event.table, event.timestamp)
            }
            KeyStrategy::Custom(f) => f(event),
        }
    }
}

/// Simple bloom filter for fast probabilistic deduplication.
struct BloomFilter {
    bits: Vec<u64>,
    size_bits: usize,
    hash_count: usize,
}

impl BloomFilter {
    fn new(size_bits: usize, hash_count: usize) -> Self {
        let num_words = size_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            size_bits,
            hash_count,
        }
    }

    fn insert(&mut self, key: &str) {
        for i in 0..self.hash_count {
            let bit_index = self.hash(key, i);
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            self.bits[word_index] |= 1u64 << bit_offset;
        }
    }

    fn contains(&self, key: &str) -> bool {
        for i in 0..self.hash_count {
            let bit_index = self.hash(key, i);
            let word_index = bit_index / 64;
            let bit_offset = bit_index % 64;
            if (self.bits[word_index] & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    fn hash(&self, key: &str, seed: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        (hasher.finish() as usize) % self.size_bits
    }

    fn clear(&mut self) {
        for word in &mut self.bits {
            *word = 0;
        }
    }
}

/// Entry in the LRU cache.
struct LruEntry {
    seen_at: Instant,
    count: u32,
}

/// Deduplicator with LRU cache and optional bloom filter.
pub struct Deduplicator {
    config: DeduplicatorConfig,
    lru: RwLock<LruState>,
    bloom: RwLock<Option<BloomFilter>>,
    stats: DeduplicatorStats,
}

struct LruState {
    cache: HashMap<String, LruEntry>,
    order: VecDeque<String>,
    last_cleanup: Instant,
}

/// Statistics for deduplication.
#[derive(Debug, Default)]
pub struct DeduplicatorStats {
    pub events_checked: AtomicU64,
    pub duplicates_found: AtomicU64,
    pub bloom_false_positives: AtomicU64,
    pub lru_hits: AtomicU64,
    pub lru_misses: AtomicU64,
    pub cleanups: AtomicU64,
}

impl DeduplicatorStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> DeduplicatorStatsSnapshot {
        DeduplicatorStatsSnapshot {
            events_checked: self.events_checked.load(Ordering::Relaxed),
            duplicates_found: self.duplicates_found.load(Ordering::Relaxed),
            bloom_false_positives: self.bloom_false_positives.load(Ordering::Relaxed),
            lru_hits: self.lru_hits.load(Ordering::Relaxed),
            lru_misses: self.lru_misses.load(Ordering::Relaxed),
            cleanups: self.cleanups.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of deduplicator statistics.
#[derive(Debug, Clone)]
pub struct DeduplicatorStatsSnapshot {
    pub events_checked: u64,
    pub duplicates_found: u64,
    pub bloom_false_positives: u64,
    pub lru_hits: u64,
    pub lru_misses: u64,
    pub cleanups: u64,
}

impl DeduplicatorStatsSnapshot {
    /// Calculate duplicate rate (0.0 - 1.0).
    pub fn duplicate_rate(&self) -> f64 {
        if self.events_checked == 0 {
            return 0.0;
        }
        self.duplicates_found as f64 / self.events_checked as f64
    }

    /// Calculate bloom filter false positive rate.
    pub fn bloom_fp_rate(&self) -> f64 {
        let true_positives = self
            .duplicates_found
            .saturating_sub(self.bloom_false_positives);
        let total_positives = true_positives + self.bloom_false_positives;
        if total_positives == 0 {
            return 0.0;
        }
        self.bloom_false_positives as f64 / total_positives as f64
    }
}

impl Deduplicator {
    /// Create a new deduplicator.
    pub fn new(config: DeduplicatorConfig) -> Self {
        let bloom = if config.bloom_size_bits > 0 {
            Some(BloomFilter::new(
                config.bloom_size_bits,
                config.bloom_hash_count,
            ))
        } else {
            None
        };

        Self {
            lru: RwLock::new(LruState {
                cache: HashMap::with_capacity(config.lru_capacity),
                order: VecDeque::with_capacity(config.lru_capacity),
                last_cleanup: Instant::now(),
            }),
            bloom: RwLock::new(bloom),
            stats: DeduplicatorStats::new(),
            config,
        }
    }

    /// Check if an event is a duplicate.
    pub async fn is_duplicate(&self, event: &CdcEvent) -> bool {
        self.stats.events_checked.fetch_add(1, Ordering::Relaxed);
        let key = self.config.key_strategy.extract_key(event);

        // Check bloom filter first (fast path)
        if let Some(bloom) = self.bloom.read().await.as_ref() {
            if !bloom.contains(&key) {
                self.stats.lru_misses.fetch_add(1, Ordering::Relaxed);
                return false; // Definitely not seen
            }
        }

        // Check LRU cache (exact)
        let lru = self.lru.read().await;
        if let Some(entry) = lru.cache.get(&key) {
            if entry.seen_at.elapsed() < self.config.ttl {
                self.stats.lru_hits.fetch_add(1, Ordering::Relaxed);
                self.stats.duplicates_found.fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        // Bloom said yes, but LRU says no - false positive
        if self.config.bloom_size_bits > 0 {
            self.stats
                .bloom_false_positives
                .fetch_add(1, Ordering::Relaxed);
        }
        self.stats.lru_misses.fetch_add(1, Ordering::Relaxed);
        false
    }

    /// Mark an event as seen.
    pub async fn mark_seen(&self, event: &CdcEvent) {
        let key = self.config.key_strategy.extract_key(event);

        // Add to bloom filter
        if let Some(bloom) = self.bloom.write().await.as_mut() {
            bloom.insert(&key);
        }

        // Add to LRU cache
        let mut lru = self.lru.write().await;

        // Update existing or insert new
        if let Some(entry) = lru.cache.get_mut(&key) {
            entry.seen_at = Instant::now();
            entry.count += 1;
        } else {
            // Evict if at capacity
            while lru.cache.len() >= self.config.lru_capacity {
                if let Some(old_key) = lru.order.pop_front() {
                    lru.cache.remove(&old_key);
                }
            }

            lru.cache.insert(
                key.clone(),
                LruEntry {
                    seen_at: Instant::now(),
                    count: 1,
                },
            );
            lru.order.push_back(key);
        }

        // Periodic cleanup
        if lru.last_cleanup.elapsed() > Duration::from_secs(60) {
            drop(lru);
            self.cleanup().await;
        }
    }

    /// Check and mark in one operation.
    pub async fn check_and_mark(&self, event: &CdcEvent) -> bool {
        if self.is_duplicate(event).await {
            return true;
        }
        self.mark_seen(event).await;
        false
    }

    /// Process events, filtering out duplicates.
    pub async fn filter_duplicates(&self, events: Vec<CdcEvent>) -> Vec<CdcEvent> {
        let mut unique = Vec::with_capacity(events.len());
        for event in events {
            if !self.check_and_mark(&event).await {
                unique.push(event);
            }
        }
        unique
    }

    /// Clean up expired entries.
    pub async fn cleanup(&self) {
        let mut lru = self.lru.write().await;
        let now = Instant::now();

        // Remove expired entries
        lru.cache
            .retain(|_, entry| entry.seen_at.elapsed() < self.config.ttl);

        // Rebuild order queue - collect keys first to avoid borrow conflict
        let valid_keys: Vec<_> = lru
            .order
            .iter()
            .filter(|key| lru.cache.contains_key(*key))
            .cloned()
            .collect();
        lru.order = std::collections::VecDeque::from(valid_keys);

        lru.last_cleanup = now;
        self.stats.cleanups.fetch_add(1, Ordering::Relaxed);

        debug!(
            "Deduplicator cleanup: {} entries remaining",
            lru.cache.len()
        );
    }

    /// Clear all state.
    pub async fn clear(&self) {
        let mut lru = self.lru.write().await;
        lru.cache.clear();
        lru.order.clear();

        if let Some(bloom) = self.bloom.write().await.as_mut() {
            bloom.clear();
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> DeduplicatorStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get current cache size.
    pub async fn cache_size(&self) -> usize {
        self.lru.read().await.cache.len()
    }
}

/// Deduplication key generator for common patterns.
pub mod keys {
    use super::*;

    /// Generate key from specific columns.
    pub fn from_columns(columns: Vec<String>) -> impl Fn(&CdcEvent) -> String + Send + Sync {
        move |event| {
            let values: Vec<String> = columns
                .iter()
                .filter_map(|col| {
                    event
                        .after
                        .as_ref()
                        .and_then(|m| m.get(col))
                        .map(|v| v.to_string())
                })
                .collect();
            format!("{}:{}:{}", event.schema, event.table, values.join(":"))
        }
    }

    /// Generate key including operation type.
    pub fn with_operation<F>(base: F) -> impl Fn(&CdcEvent) -> String + Send + Sync
    where
        F: Fn(&CdcEvent) -> String + Send + Sync,
    {
        move |event| format!("{}:{:?}", base(event), event.op)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CdcOp;

    fn make_event(table: &str, id: i64, op: CdcOp) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: table.to_string(),
            op,
            before: None,
            after: Some(serde_json::json!({ "id": id })),
            timestamp: chrono::Utc::now().timestamp(),
            transaction: None,
        }
    }

    #[tokio::test]
    async fn test_deduplicator_basic() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let event = make_event("users", 1, CdcOp::Insert);

        // First time - not duplicate
        assert!(!dedup.is_duplicate(&event).await);
        dedup.mark_seen(&event).await;

        // Second time - duplicate
        assert!(dedup.is_duplicate(&event).await);
    }

    #[tokio::test]
    async fn test_deduplicator_different_ids() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let event1 = make_event("users", 1, CdcOp::Insert);
        let event2 = make_event("users", 2, CdcOp::Insert);

        dedup.mark_seen(&event1).await;

        assert!(dedup.is_duplicate(&event1).await);
        assert!(!dedup.is_duplicate(&event2).await);
    }

    #[tokio::test]
    async fn test_deduplicator_different_ops() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let insert = make_event("users", 1, CdcOp::Insert);
        let update = make_event("users", 1, CdcOp::Update);

        dedup.mark_seen(&insert).await;

        assert!(dedup.is_duplicate(&insert).await);
        assert!(!dedup.is_duplicate(&update).await); // Different op = different key
    }

    #[tokio::test]
    async fn test_check_and_mark() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let event = make_event("users", 1, CdcOp::Insert);

        // First call marks as seen
        assert!(!dedup.check_and_mark(&event).await);

        // Second call detects duplicate
        assert!(dedup.check_and_mark(&event).await);
    }

    #[tokio::test]
    async fn test_filter_duplicates() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let events = vec![
            make_event("users", 1, CdcOp::Insert),
            make_event("users", 1, CdcOp::Insert), // Duplicate
            make_event("users", 2, CdcOp::Insert),
            make_event("users", 2, CdcOp::Insert), // Duplicate
            make_event("users", 3, CdcOp::Insert),
        ];

        let unique = dedup.filter_duplicates(events).await;
        assert_eq!(unique.len(), 3); // Only unique events
    }

    #[tokio::test]
    async fn test_deduplicator_stats() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let event = make_event("users", 1, CdcOp::Insert);
        dedup.check_and_mark(&event).await;
        dedup.check_and_mark(&event).await;
        dedup.check_and_mark(&event).await;

        let stats = dedup.stats();
        assert_eq!(stats.events_checked, 3);
        assert_eq!(stats.duplicates_found, 2);
    }

    #[tokio::test]
    async fn test_deduplicator_clear() {
        let dedup = Deduplicator::new(DeduplicatorConfig::default());

        let event = make_event("users", 1, CdcOp::Insert);
        dedup.mark_seen(&event).await;
        assert!(dedup.is_duplicate(&event).await);

        dedup.clear().await;
        assert!(!dedup.is_duplicate(&event).await);
    }

    #[tokio::test]
    async fn test_key_strategy_transaction() {
        let config = DeduplicatorConfig {
            key_strategy: KeyStrategy::TransactionPosition,
            ..Default::default()
        };
        let dedup = Deduplicator::new(config);

        let mut event1 = make_event("users", 1, CdcOp::Insert);
        event1.database = "db1".to_string();
        event1.timestamp = 1000;

        let mut event2 = make_event("users", 2, CdcOp::Insert);
        event2.database = "db1".to_string();
        event2.timestamp = 1000;

        // Same database + table + timestamp = duplicate
        dedup.mark_seen(&event1).await;
        assert!(dedup.is_duplicate(&event2).await);
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        let config = DeduplicatorConfig {
            lru_capacity: 100,
            bloom_size_bits: 10_000,
            bloom_hash_count: 5,
            ..Default::default()
        };
        let dedup = Deduplicator::new(config);

        // Add many events
        for i in 0..50 {
            let event = make_event("users", i, CdcOp::Insert);
            dedup.mark_seen(&event).await;
        }

        // Check duplicates
        for i in 0..50 {
            let event = make_event("users", i, CdcOp::Insert);
            assert!(dedup.is_duplicate(&event).await);
        }

        // New events should not be duplicates
        let new_event = make_event("users", 999, CdcOp::Insert);
        assert!(!dedup.is_duplicate(&new_event).await);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let config = DeduplicatorConfig {
            lru_capacity: 10,
            bloom_size_bits: 0, // No bloom filter
            ..Default::default()
        };
        let dedup = Deduplicator::new(config);

        // Add more than capacity
        for i in 0..20 {
            let event = make_event("users", i, CdcOp::Insert);
            dedup.mark_seen(&event).await;
        }

        // Only last 10 should be in cache
        assert_eq!(dedup.cache_size().await, 10);

        // Recent events should be detected
        let recent = make_event("users", 19, CdcOp::Insert);
        assert!(dedup.is_duplicate(&recent).await);

        // Old events should not (evicted)
        let old = make_event("users", 0, CdcOp::Insert);
        assert!(!dedup.is_duplicate(&old).await);
    }

    #[test]
    fn test_duplicate_rate() {
        let stats = DeduplicatorStatsSnapshot {
            events_checked: 100,
            duplicates_found: 25,
            bloom_false_positives: 5,
            lru_hits: 20,
            lru_misses: 80,
            cleanups: 1,
        };

        assert!((stats.duplicate_rate() - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_config_presets() {
        let exact = DeduplicatorConfig::exact();
        assert_eq!(exact.bloom_size_bits, 0);
        assert_eq!(exact.lru_capacity, 1_000_000);

        let compact = DeduplicatorConfig::compact();
        assert_eq!(compact.bloom_size_bits, 10_000_000);
        assert_eq!(compact.lru_capacity, 10_000);
    }
}
