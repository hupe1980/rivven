//! High-Performance Bloom Filters and Probabilistic Data Structures
//!
//! Optimized for streaming workloads with:
//! - **Bloom Filter**: Fast set membership testing for offset ranges
//! - **Counting Bloom Filter**: Supports deletions for dynamic sets
//! - **Cuckoo Filter**: Better space efficiency with deletions
//! - **HyperLogLog**: Cardinality estimation for metrics
//!
//! # Use Cases in Rivven
//!
//! 1. **Offset Range Queries**: Quickly determine if an offset exists in a segment
//! 2. **Consumer Group Tracking**: Track which offsets have been consumed
//! 3. **Deduplication**: Detect duplicate messages in idempotent producers
//! 4. **Cardinality Estimation**: Estimate unique keys/consumers without memory explosion
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    Probabilistic Data Structures                         │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌────────────────────┐     ┌────────────────────┐                      │
//! │  │    Bloom Filter     │     │  Counting Bloom    │                      │
//! │  │  ┌──┬──┬──┬──┬──┐  │     │  ┌──┬──┬──┬──┬──┐  │                      │
//! │  │  │0 │1 │0 │1 │0 │  │     │  │0 │2 │0 │1 │0 │  │                      │
//! │  │  └──┴──┴──┴──┴──┘  │     │  └──┴──┴──┴──┴──┘  │                      │
//! │  │  O(k) lookup       │     │  Supports delete   │                      │
//! │  │  False positive    │     │  Counter overflow  │                      │
//! │  └────────────────────┘     └────────────────────┘                      │
//! │                                                                          │
//! │  ┌────────────────────┐     ┌────────────────────┐                      │
//! │  │   Cuckoo Filter    │     │    HyperLogLog     │                      │
//! │  │  ┌──────┐ ┌──────┐ │     │  ┌──┬──┬──┬──┬──┐  │                      │
//! │  │  │ fp₁  │ │ fp₂  │ │     │  │3 │5 │2 │7 │4 │  │                      │
//! │  │  └──────┘ └──────┘ │     │  └──┴──┴──┴──┴──┘  │                      │
//! │  │  Better deletion   │     │  Cardinality est.  │                      │
//! │  │  Lower false pos   │     │  1.04/√m accuracy  │                      │
//! │  └────────────────────┘     └────────────────────┘                      │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

// ============================================================================
// Bloom Filter
// ============================================================================

/// High-performance Bloom filter with SIMD-friendly layout
///
/// Uses multiple hash functions derived from two base hashes (Kirsch-Mitzenmacher optimization)
/// to provide O(k) lookup where k is the number of hash functions.
pub struct BloomFilter {
    /// Bit array stored as 64-bit words for cache efficiency
    bits: Vec<AtomicU64>,
    /// Number of bits
    num_bits: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of items inserted
    count: AtomicU64,
}

impl BloomFilter {
    /// Create a new Bloom filter with optimal parameters
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to insert
    /// * `false_positive_rate` - Desired false positive rate (0.0 - 1.0)
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Optimal number of bits: m = -n*ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits =
            (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;

        // Round up to multiple of 64 for word alignment
        let num_bits = num_bits.div_ceil(64) * 64;

        // Optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes =
            ((num_bits as f64 / expected_items as f64) * std::f64::consts::LN_2).ceil() as usize;
        let num_hashes = num_hashes.clamp(1, 16); // Clamp to reasonable range

        let num_words = num_bits / 64;
        let bits = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        Self {
            bits,
            num_bits,
            num_hashes,
            count: AtomicU64::new(0),
        }
    }

    /// Create a Bloom filter with explicit parameters
    pub fn with_params(num_bits: usize, num_hashes: usize) -> Self {
        let num_bits = num_bits.div_ceil(64) * 64;
        let num_words = num_bits / 64;
        let bits = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        Self {
            bits,
            num_bits,
            num_hashes: num_hashes.max(1),
            count: AtomicU64::new(0),
        }
    }

    /// Insert an item into the filter
    pub fn insert<T: Hash>(&self, item: &T) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let bit_index = self.combined_hash(h1, h2, i) % self.num_bits;
            self.set_bit(bit_index);
        }

        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if an item might be in the filter
    ///
    /// Returns `true` if the item might be present (possible false positive)
    /// Returns `false` if the item is definitely not present
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let bit_index = self.combined_hash(h1, h2, i) % self.num_bits;
            if !self.get_bit(bit_index) {
                return false;
            }
        }

        true
    }

    /// Insert an item and check if it was already present
    pub fn insert_and_check<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);
        let mut was_present = true;

        for i in 0..self.num_hashes {
            let bit_index = self.combined_hash(h1, h2, i) % self.num_bits;
            if !self.set_bit(bit_index) {
                was_present = false;
            }
        }

        self.count.fetch_add(1, Ordering::Relaxed);
        was_present
    }

    /// Get the number of items inserted
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Estimate current false positive rate
    pub fn estimated_fp_rate(&self) -> f64 {
        let bits_set = self.count_bits_set();
        let fill_ratio = bits_set as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Get fill ratio (0.0 - 1.0)
    pub fn fill_ratio(&self) -> f64 {
        self.count_bits_set() as f64 / self.num_bits as f64
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.bits.len() * 8
    }

    /// Clear the filter
    pub fn clear(&self) {
        for word in &self.bits {
            word.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
    }

    /// Compute two independent hashes for an item
    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut h1 = DefaultHasher::new();
        item.hash(&mut h1);
        let hash1 = h1.finish();

        // Use different seeds for second hash
        let mut h2 = DefaultHasher::new();
        hash1.hash(&mut h2);
        let hash2 = h2.finish();

        (hash1, hash2)
    }

    /// Combine two hashes using Kirsch-Mitzenmacher optimization
    fn combined_hash(&self, h1: u64, h2: u64, i: usize) -> usize {
        h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize
    }

    /// Set a bit and return whether it was already set
    fn set_bit(&self, bit_index: usize) -> bool {
        let word_index = bit_index / 64;
        let bit_offset = bit_index % 64;
        let mask = 1u64 << bit_offset;

        let old = self.bits[word_index].fetch_or(mask, Ordering::AcqRel);
        (old & mask) != 0
    }

    /// Get a bit value
    fn get_bit(&self, bit_index: usize) -> bool {
        let word_index = bit_index / 64;
        let bit_offset = bit_index % 64;
        let mask = 1u64 << bit_offset;

        (self.bits[word_index].load(Ordering::Acquire) & mask) != 0
    }

    /// Count total bits set
    fn count_bits_set(&self) -> usize {
        self.bits
            .iter()
            .map(|w| w.load(Ordering::Relaxed).count_ones() as usize)
            .sum()
    }
}

// ============================================================================
// Counting Bloom Filter
// ============================================================================

/// Counting Bloom filter that supports deletions
///
/// Uses 4-bit counters per bucket, allowing items to be removed.
/// Counter overflow is handled by saturation (max 15).
pub struct CountingBloomFilter {
    /// 4-bit counters packed into bytes (2 counters per byte)
    counters: Vec<AtomicU8>,
    /// Number of counters
    num_counters: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of items inserted (approximate)
    count: AtomicU64,
}

impl CountingBloomFilter {
    /// Create a new counting Bloom filter
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_counters =
            (-(expected_items as f64) * false_positive_rate.ln() / ln2_squared).ceil() as usize;
        let num_counters = num_counters.max(64);

        let num_hashes = ((num_counters as f64 / expected_items as f64) * std::f64::consts::LN_2)
            .ceil() as usize;
        let num_hashes = num_hashes.clamp(1, 16);

        // Each byte holds 2 counters
        let num_bytes = num_counters.div_ceil(2);
        let counters = (0..num_bytes).map(|_| AtomicU8::new(0)).collect();

        Self {
            counters,
            num_counters,
            num_hashes,
            count: AtomicU64::new(0),
        }
    }

    /// Insert an item
    pub fn insert<T: Hash>(&self, item: &T) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let counter_index = self.combined_hash(h1, h2, i) % self.num_counters;
            self.increment_counter(counter_index);
        }

        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove an item (decrement counters)
    pub fn remove<T: Hash>(&self, item: &T) {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let counter_index = self.combined_hash(h1, h2, i) % self.num_counters;
            self.decrement_counter(counter_index);
        }

        self.count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Check if an item might be present
    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let (h1, h2) = self.hash_pair(item);

        for i in 0..self.num_hashes {
            let counter_index = self.combined_hash(h1, h2, i) % self.num_counters;
            if self.get_counter(counter_index) == 0 {
                return false;
            }
        }

        true
    }

    /// Get approximate count of items
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn hash_pair<T: Hash>(&self, item: &T) -> (u64, u64) {
        let mut h1 = DefaultHasher::new();
        item.hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        hash1.hash(&mut h2);
        let hash2 = h2.finish();

        (hash1, hash2)
    }

    fn combined_hash(&self, h1: u64, h2: u64, i: usize) -> usize {
        h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize
    }

    fn increment_counter(&self, counter_index: usize) {
        let byte_index = counter_index / 2;
        let is_high_nibble = counter_index % 2 == 1;

        loop {
            let old_byte = self.counters[byte_index].load(Ordering::Acquire);
            let old_counter = if is_high_nibble {
                (old_byte >> 4) & 0x0F
            } else {
                old_byte & 0x0F
            };

            // Saturate at 15
            if old_counter >= 15 {
                return;
            }

            let new_byte = if is_high_nibble {
                (old_byte & 0x0F) | ((old_counter + 1) << 4)
            } else {
                (old_byte & 0xF0) | (old_counter + 1)
            };

            if self.counters[byte_index]
                .compare_exchange_weak(old_byte, new_byte, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn decrement_counter(&self, counter_index: usize) {
        let byte_index = counter_index / 2;
        let is_high_nibble = counter_index % 2 == 1;

        loop {
            let old_byte = self.counters[byte_index].load(Ordering::Acquire);
            let old_counter = if is_high_nibble {
                (old_byte >> 4) & 0x0F
            } else {
                old_byte & 0x0F
            };

            // Don't go below 0
            if old_counter == 0 {
                return;
            }

            let new_byte = if is_high_nibble {
                (old_byte & 0x0F) | ((old_counter - 1) << 4)
            } else {
                (old_byte & 0xF0) | (old_counter - 1)
            };

            if self.counters[byte_index]
                .compare_exchange_weak(old_byte, new_byte, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn get_counter(&self, counter_index: usize) -> u8 {
        let byte_index = counter_index / 2;
        let is_high_nibble = counter_index % 2 == 1;

        let byte = self.counters[byte_index].load(Ordering::Acquire);
        if is_high_nibble {
            (byte >> 4) & 0x0F
        } else {
            byte & 0x0F
        }
    }
}

// ============================================================================
// HyperLogLog for Cardinality Estimation
// ============================================================================

/// HyperLogLog for cardinality estimation
///
/// Estimates the number of unique elements with ~1.04/√m relative error
/// where m is the number of registers.
pub struct HyperLogLog {
    /// Registers (each stores max leading zeros seen)
    registers: Vec<AtomicU8>,
    /// Number of registers (2^p)
    num_registers: usize,
    /// Precision parameter (log2 of num_registers)
    precision: u8,
    /// Alpha correction factor
    alpha: f64,
}

impl HyperLogLog {
    /// Create a new HyperLogLog with given precision
    ///
    /// # Arguments
    /// * `precision` - Number of bits for register indexing (4-18, higher = more accurate)
    pub fn new(precision: u8) -> Self {
        let precision = precision.clamp(4, 18);
        let num_registers = 1 << precision;

        // Alpha correction factor
        let alpha = match precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / num_registers as f64),
        };

        let registers = (0..num_registers).map(|_| AtomicU8::new(0)).collect();

        Self {
            registers,
            num_registers,
            precision,
            alpha,
        }
    }

    /// Add an item to the estimator
    pub fn add<T: Hash>(&self, item: &T) {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();

        // Use first p bits for register index
        let register_idx = (hash >> (64 - self.precision)) as usize;

        // Count leading zeros in remaining bits + 1
        let remaining = (hash << self.precision) | (1 << (self.precision - 1));
        let leading_zeros = (remaining.leading_zeros() + 1) as u8;

        // Update register with max
        loop {
            let current = self.registers[register_idx].load(Ordering::Acquire);
            if leading_zeros <= current {
                break;
            }
            if self.registers[register_idx]
                .compare_exchange_weak(current, leading_zeros, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    /// Estimate the cardinality
    pub fn estimate(&self) -> u64 {
        // Harmonic mean of 2^register values
        let mut sum = 0.0;
        let mut zeros = 0;

        for reg in &self.registers {
            let val = reg.load(Ordering::Relaxed);
            sum += 1.0 / (1u64 << val) as f64;
            if val == 0 {
                zeros += 1;
            }
        }

        let m = self.num_registers as f64;
        let raw_estimate = self.alpha * m * m / sum;

        // Apply corrections for small and large cardinalities
        let estimate = if raw_estimate <= 2.5 * m && zeros > 0 {
            // Small range correction (linear counting)
            m * (m / zeros as f64).ln()
        } else if raw_estimate > (1u64 << 32) as f64 / 30.0 {
            // Large range correction
            -((1u64 << 32) as f64) * (1.0 - raw_estimate / (1u64 << 32) as f64).ln()
        } else {
            raw_estimate
        };

        estimate.round() as u64
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&self, other: &HyperLogLog) {
        assert_eq!(self.num_registers, other.num_registers);

        for i in 0..self.num_registers {
            loop {
                let current = self.registers[i].load(Ordering::Acquire);
                let other_val = other.registers[i].load(Ordering::Relaxed);
                let new_val = current.max(other_val);

                if new_val == current {
                    break;
                }

                if self.registers[i]
                    .compare_exchange_weak(current, new_val, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.num_registers
    }

    /// Get the relative error (theoretical)
    pub fn relative_error(&self) -> f64 {
        1.04 / (self.num_registers as f64).sqrt()
    }
}

// ============================================================================
// Offset Range Bloom Filter (Specialized for Rivven)
// ============================================================================

/// Specialized Bloom filter for offset range queries
///
/// Optimized for the common pattern of "does this segment contain offset X?"
/// Uses bucketed approach for range-based queries.
pub struct OffsetBloomFilter {
    /// Main filter for individual offsets
    filter: BloomFilter,
    /// Minimum offset in the filter
    min_offset: AtomicU64,
    /// Maximum offset in the filter
    max_offset: AtomicU64,
    /// Bucket size for range queries (e.g., 1000 offsets per bucket)
    bucket_size: u64,
}

impl OffsetBloomFilter {
    /// Create a new offset Bloom filter
    ///
    /// # Arguments
    /// * `expected_offsets` - Expected number of offsets
    /// * `bucket_size` - Size of offset buckets for range queries
    pub fn new(expected_offsets: usize, bucket_size: u64) -> Self {
        Self {
            filter: BloomFilter::new(expected_offsets, 0.01),
            min_offset: AtomicU64::new(u64::MAX),
            max_offset: AtomicU64::new(0),
            bucket_size,
        }
    }

    /// Insert an offset
    pub fn insert(&self, offset: u64) {
        self.filter.insert(&offset);

        // Update min/max
        self.min_offset.fetch_min(offset, Ordering::AcqRel);
        self.max_offset.fetch_max(offset, Ordering::AcqRel);
    }

    /// Check if an offset might exist
    pub fn contains(&self, offset: u64) -> bool {
        // Quick range check first
        let min = self.min_offset.load(Ordering::Acquire);
        let max = self.max_offset.load(Ordering::Acquire);

        if offset < min || offset > max {
            return false;
        }

        self.filter.contains(&offset)
    }

    /// Check if any offset in the range might exist
    pub fn contains_range(&self, start: u64, end: u64) -> bool {
        let min = self.min_offset.load(Ordering::Acquire);
        let max = self.max_offset.load(Ordering::Acquire);

        // Quick range overlap check
        if end < min || start > max {
            return false;
        }

        // For small ranges, check each offset
        if end - start <= 10 {
            for offset in start..=end {
                if self.filter.contains(&offset) {
                    return true;
                }
            }
            return false;
        }

        // For larger ranges, check bucket boundaries
        let start_bucket = start / self.bucket_size;
        let end_bucket = end / self.bucket_size;

        for bucket in start_bucket..=end_bucket {
            let bucket_start = bucket * self.bucket_size;
            let bucket_end = bucket_start + self.bucket_size - 1;

            // Check bucket boundaries and midpoint
            for &offset in &[
                bucket_start,
                bucket_start + self.bucket_size / 2,
                bucket_end,
            ] {
                if offset >= start && offset <= end && self.filter.contains(&offset) {
                    return true;
                }
            }
        }

        // Fallback: check some offsets in the range
        let step = ((end - start) / 10).max(1);
        let mut offset = start;
        while offset <= end {
            if self.filter.contains(&offset) {
                return true;
            }
            offset += step;
        }

        false
    }

    /// Get the offset range
    pub fn offset_range(&self) -> (u64, u64) {
        (
            self.min_offset.load(Ordering::Acquire),
            self.max_offset.load(Ordering::Acquire),
        )
    }

    /// Get count of inserted offsets
    pub fn count(&self) -> u64 {
        self.filter.count()
    }
}

// ============================================================================
// Adaptive Batch Accumulator
// ============================================================================

/// Configuration for adaptive batching
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Minimum batch size before flushing
    pub min_batch_size: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum time to wait for a batch (microseconds)
    pub max_linger_us: u64,
    /// Target batch latency (microseconds)
    pub target_latency_us: u64,
    /// Adaptive sizing enabled
    pub adaptive: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 16,
            max_batch_size: 1024,
            max_linger_us: 5000,     // 5ms
            target_latency_us: 1000, // 1ms
            adaptive: true,
        }
    }
}

/// Adaptive batch accumulator for high-throughput ingestion
///
/// Automatically adjusts batch size based on throughput and latency.
pub struct AdaptiveBatcher<T> {
    config: BatchConfig,
    /// Current batch
    batch: parking_lot::Mutex<Vec<T>>,
    /// Current adaptive batch size
    current_batch_size: AtomicU32,
    /// Timestamp of first item in batch
    batch_start_us: AtomicU64,
    /// Recent latencies for adaptation (microseconds)
    recent_latencies: [AtomicU64; 8],
    latency_index: AtomicU32,
    /// Total batches flushed
    batches_flushed: AtomicU64,
    /// Total items batched
    items_batched: AtomicU64,
}

impl<T> AdaptiveBatcher<T> {
    /// Create a new adaptive batcher
    pub fn new(config: BatchConfig) -> Self {
        let initial_size = (config.min_batch_size + config.max_batch_size) / 2;

        Self {
            config,
            batch: parking_lot::Mutex::new(Vec::with_capacity(initial_size)),
            current_batch_size: AtomicU32::new(initial_size as u32),
            batch_start_us: AtomicU64::new(0),
            recent_latencies: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_index: AtomicU32::new(0),
            batches_flushed: AtomicU64::new(0),
            items_batched: AtomicU64::new(0),
        }
    }

    /// Add an item to the batch
    /// Returns Some(batch) if the batch should be flushed
    pub fn add(&self, item: T) -> Option<Vec<T>> {
        let now = Self::now_us();
        let mut batch = self.batch.lock();

        // Set batch start time on first item
        if batch.is_empty() {
            self.batch_start_us.store(now, Ordering::Release);
        }

        batch.push(item);
        self.items_batched.fetch_add(1, Ordering::Relaxed);

        let batch_size = self.current_batch_size.load(Ordering::Relaxed) as usize;
        let batch_age = now.saturating_sub(self.batch_start_us.load(Ordering::Acquire));

        // Check if we should flush
        let should_flush = batch.len() >= batch_size
            || batch_age >= self.config.max_linger_us
            || batch.len() >= self.config.max_batch_size;

        if should_flush {
            let flushed = std::mem::take(&mut *batch);
            batch.reserve(batch_size);
            self.batches_flushed.fetch_add(1, Ordering::Relaxed);

            // Record latency for adaptation
            self.record_latency(batch_age);

            Some(flushed)
        } else {
            None
        }
    }

    /// Force flush the current batch
    pub fn flush(&self) -> Vec<T> {
        let now = Self::now_us();
        let mut batch = self.batch.lock();

        if !batch.is_empty() {
            let batch_age = now.saturating_sub(self.batch_start_us.load(Ordering::Acquire));
            self.record_latency(batch_age);
            self.batches_flushed.fetch_add(1, Ordering::Relaxed);
        }

        let batch_size = self.current_batch_size.load(Ordering::Relaxed) as usize;
        let flushed = std::mem::take(&mut *batch);
        batch.reserve(batch_size);

        flushed
    }

    /// Check if batch needs flushing due to time
    pub fn needs_flush(&self) -> bool {
        let batch = self.batch.lock();
        if batch.is_empty() {
            return false;
        }

        let now = Self::now_us();
        let batch_age = now.saturating_sub(self.batch_start_us.load(Ordering::Acquire));
        batch_age >= self.config.max_linger_us
    }

    /// Get current batch size setting
    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size.load(Ordering::Relaxed) as usize
    }

    /// Get number of items currently in batch
    pub fn pending_count(&self) -> usize {
        self.batch.lock().len()
    }

    /// Get statistics
    pub fn stats(&self) -> BatcherStats {
        BatcherStats {
            batches_flushed: self.batches_flushed.load(Ordering::Relaxed),
            items_batched: self.items_batched.load(Ordering::Relaxed),
            current_batch_size: self.current_batch_size.load(Ordering::Relaxed) as usize,
            avg_latency_us: self.average_latency(),
        }
    }

    fn record_latency(&self, latency_us: u64) {
        if !self.config.adaptive {
            return;
        }

        let idx = self.latency_index.fetch_add(1, Ordering::Relaxed) as usize % 8;
        self.recent_latencies[idx].store(latency_us, Ordering::Relaxed);

        // Adapt batch size based on latency
        let avg_latency = self.average_latency();
        let current_size = self.current_batch_size.load(Ordering::Relaxed);

        let new_size = if avg_latency > self.config.target_latency_us * 2 {
            // Latency too high, reduce batch size
            (current_size * 3 / 4).max(self.config.min_batch_size as u32)
        } else if avg_latency < self.config.target_latency_us / 2 {
            // Latency low, can increase batch size
            (current_size * 5 / 4).min(self.config.max_batch_size as u32)
        } else {
            current_size
        };

        self.current_batch_size.store(new_size, Ordering::Relaxed);
    }

    fn average_latency(&self) -> u64 {
        let mut sum = 0u64;
        let mut count = 0u64;

        for lat in &self.recent_latencies {
            let l = lat.load(Ordering::Relaxed);
            if l > 0 {
                sum += l;
                count += 1;
            }
        }

        if count > 0 {
            sum / count
        } else {
            0
        }
    }

    fn now_us() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

/// Batcher statistics
#[derive(Debug, Clone)]
pub struct BatcherStats {
    pub batches_flushed: u64,
    pub items_batched: u64,
    pub current_batch_size: usize,
    pub avg_latency_us: u64,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let filter = BloomFilter::new(1000, 0.01);

        // Insert items
        for i in 0..1000 {
            filter.insert(&i);
        }

        // Check presence (should be ~100% accurate for inserted items)
        for i in 0..1000 {
            assert!(filter.contains(&i), "Item {} should be present", i);
        }

        // Check false positives (should be ~1%)
        let mut false_positives = 0;
        for i in 1000..2000 {
            if filter.contains(&i) {
                false_positives += 1;
            }
        }

        // Allow up to 3% FP rate due to randomness
        assert!(
            false_positives < 30,
            "Too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn test_counting_bloom_filter() {
        let filter = CountingBloomFilter::new(1000, 0.01);

        // Insert
        filter.insert(&42);
        filter.insert(&43);
        assert!(filter.contains(&42));
        assert!(filter.contains(&43));

        // Remove
        filter.remove(&42);
        assert!(!filter.contains(&42));
        assert!(filter.contains(&43));
    }

    #[test]
    fn test_hyperloglog() {
        let hll = HyperLogLog::new(14); // 2^14 = 16384 registers

        // Add unique items
        for i in 0..10000 {
            hll.add(&i);
        }

        let estimate = hll.estimate();

        // Should be within 10% of actual
        let error = (estimate as i64 - 10000i64).abs() as f64 / 10000.0;
        assert!(
            error < 0.1,
            "Estimate {} too far from 10000 (error: {}%)",
            estimate,
            error * 100.0
        );
    }

    #[test]
    fn test_hyperloglog_merge() {
        let hll1 = HyperLogLog::new(10);
        let hll2 = HyperLogLog::new(10);

        for i in 0..5000 {
            hll1.add(&i);
        }
        for i in 5000..10000 {
            hll2.add(&i);
        }

        hll1.merge(&hll2);

        let estimate = hll1.estimate();
        let error = (estimate as i64 - 10000i64).abs() as f64 / 10000.0;
        assert!(
            error < 0.15,
            "Merged estimate {} too far from 10000",
            estimate
        );
    }

    #[test]
    fn test_offset_bloom_filter() {
        let filter = OffsetBloomFilter::new(1000, 100);

        // Insert offsets
        for offset in (0..1000).step_by(10) {
            filter.insert(offset);
        }

        // Check contains
        assert!(filter.contains(0));
        assert!(filter.contains(100));
        assert!(!filter.contains(5)); // Not inserted

        // Check range
        assert!(filter.contains_range(0, 50));
        assert!(filter.contains_range(90, 110));

        // Range outside
        let (min, max) = filter.offset_range();
        assert_eq!(min, 0);
        assert_eq!(max, 990);
    }

    #[test]
    fn test_adaptive_batcher() {
        let config = BatchConfig {
            min_batch_size: 4,
            max_batch_size: 16,
            max_linger_us: 10000,
            target_latency_us: 1000,
            adaptive: true,
        };

        let batcher = AdaptiveBatcher::new(config);

        // Add items until batch flushes
        let mut flushed = None;
        for i in 0..20 {
            if let Some(batch) = batcher.add(i) {
                flushed = Some(batch);
                break;
            }
        }

        assert!(flushed.is_some());
        let batch = flushed.unwrap();
        assert!(!batch.is_empty());
    }
}
