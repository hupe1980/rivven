//! Lock-Free Concurrent Data Structures
//!
//! High-performance concurrent data structures optimized for streaming workloads:
//!
//! - **LockFreeQueue**: MPMC queue with bounded/unbounded variants
//! - **AppendOnlyLog**: Lock-free append-only log for message batching
//! - **ConcurrentHashMap**: Read-optimized concurrent map with minimal locking
//! - **SkipList**: Lock-free ordered map for offset indexing
//!
//! # Design Principles
//!
//! 1. **Minimize Contention**: Use atomic operations over locks where possible
//! 2. **Cache-Friendly**: Align structures to cache lines to prevent false sharing
//! 3. **Memory Efficient**: Pool allocations to reduce GC pressure
//! 4. **Backpressure-Aware**: Bounded structures with overflow policies
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    Concurrent Data Structures                            │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌────────────────────┐     ┌────────────────────┐                      │
//! │  │   LockFreeQueue    │     │   AppendOnlyLog    │                      │
//! │  │  ┌──┬──┬──┬──┬──┐  │     │  ┌────────────────┐│                      │
//! │  │  │H │  │  │  │T │  │     │  │ Segment 0      ││                      │
//! │  │  └──┴──┴──┴──┴──┘  │     │  │ Segment 1      ││                      │
//! │  │  MPMC lock-free    │     │  │ Segment N      ││                      │
//! │  └────────────────────┘     │  └────────────────┘│                      │
//! │                             │  Lock-free append  │                      │
//! │  ┌────────────────────┐     └────────────────────┘                      │
//! │  │   ConcurrentMap    │                                                 │
//! │  │  ┌──┬──┬──┬──┬──┐  │     ┌────────────────────┐                      │
//! │  │  │S0│S1│S2│S3│SN│  │     │     SkipList       │                      │
//! │  │  └──┴──┴──┴──┴──┘  │     │  Level 3: ──●──────│                      │
//! │  │  Sharded for perf  │     │  Level 2: ──●──●───│                      │
//! │  └────────────────────┘     │  Level 1: ●─●─●─●──│                      │
//! │                             │  O(log n) lookup   │                      │
//! │                             └────────────────────┘                      │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use bytes::Bytes;
use crossbeam_channel::{bounded, unbounded, Receiver, Sender, TryRecvError, TrySendError};
use parking_lot::RwLock;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// ============================================================================
// Cache Line Alignment
// ============================================================================

/// Pad a value to cache line size to prevent false sharing
#[repr(C, align(64))]
pub struct CacheAligned<T>(pub T);

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

// ============================================================================
// Lock-Free MPMC Queue
// ============================================================================

/// A high-performance bounded MPMC queue using crossbeam-channel
///
/// This queue is optimized for producer-consumer patterns common in streaming:
/// - Network receive -> Message processing
/// - Message batching -> Disk write
/// - Raft log append -> Replication
pub struct LockFreeQueue<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
    capacity: usize,
    /// Number of items currently in queue (approximate)
    len: AtomicUsize,
    /// Total items ever enqueued
    total_enqueued: AtomicU64,
    /// Total items ever dequeued
    total_dequeued: AtomicU64,
    /// Number of times enqueue was blocked (backpressure)
    blocked_enqueues: AtomicU64,
}

impl<T> LockFreeQueue<T> {
    /// Create a new bounded queue
    pub fn bounded(capacity: usize) -> Self {
        let (sender, receiver) = bounded(capacity);
        Self {
            sender,
            receiver,
            capacity,
            len: AtomicUsize::new(0),
            total_enqueued: AtomicU64::new(0),
            total_dequeued: AtomicU64::new(0),
            blocked_enqueues: AtomicU64::new(0),
        }
    }

    /// Create a new unbounded queue (use with caution)
    pub fn unbounded() -> Self {
        let (sender, receiver) = unbounded();
        Self {
            sender,
            receiver,
            capacity: usize::MAX,
            len: AtomicUsize::new(0),
            total_enqueued: AtomicU64::new(0),
            total_dequeued: AtomicU64::new(0),
            blocked_enqueues: AtomicU64::new(0),
        }
    }

    /// Try to enqueue an item without blocking
    pub fn try_push(&self, item: T) -> Result<(), T> {
        match self.sender.try_send(item) {
            Ok(()) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                self.total_enqueued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(item)) => {
                self.blocked_enqueues.fetch_add(1, Ordering::Relaxed);
                Err(item)
            }
            Err(TrySendError::Disconnected(item)) => Err(item),
        }
    }

    /// Blocking enqueue
    pub fn push(&self, item: T) -> Result<(), T> {
        match self.sender.send(item) {
            Ok(()) => {
                self.len.fetch_add(1, Ordering::Relaxed);
                self.total_enqueued.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e.0),
        }
    }

    /// Try to dequeue an item without blocking
    pub fn try_pop(&self) -> Option<T> {
        match self.receiver.try_recv() {
            Ok(item) => {
                self.len.fetch_sub(1, Ordering::Relaxed);
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                Some(item)
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => None,
        }
    }

    /// Blocking dequeue
    pub fn pop(&self) -> Option<T> {
        match self.receiver.recv() {
            Ok(item) => {
                self.len.fetch_sub(1, Ordering::Relaxed);
                self.total_dequeued.fetch_add(1, Ordering::Relaxed);
                Some(item)
            }
            Err(_) => None,
        }
    }

    /// Try to dequeue up to `max` items
    pub fn pop_batch(&self, max: usize) -> Vec<T> {
        let mut batch = Vec::with_capacity(max.min(64));
        for _ in 0..max {
            match self.receiver.try_recv() {
                Ok(item) => {
                    batch.push(item);
                }
                Err(_) => break,
            }
        }
        let count = batch.len();
        if count > 0 {
            self.len.fetch_sub(count, Ordering::Relaxed);
            self.total_dequeued
                .fetch_add(count as u64, Ordering::Relaxed);
        }
        batch
    }

    /// Get approximate queue length
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get queue capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get fill percentage (0.0 - 1.0)
    pub fn fill_ratio(&self) -> f64 {
        if self.capacity == usize::MAX {
            0.0
        } else {
            self.len() as f64 / self.capacity as f64
        }
    }

    /// Get statistics
    pub fn stats(&self) -> QueueStats {
        QueueStats {
            len: self.len(),
            capacity: self.capacity,
            total_enqueued: self.total_enqueued.load(Ordering::Relaxed),
            total_dequeued: self.total_dequeued.load(Ordering::Relaxed),
            blocked_enqueues: self.blocked_enqueues.load(Ordering::Relaxed),
        }
    }
}

/// Queue statistics
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub len: usize,
    pub capacity: usize,
    pub total_enqueued: u64,
    pub total_dequeued: u64,
    pub blocked_enqueues: u64,
}

// ============================================================================
// Lock-Free Append-Only Log
// ============================================================================

/// Configuration for the append-only log
#[derive(Debug, Clone)]
pub struct AppendLogConfig {
    /// Size of each segment in bytes
    pub segment_size: usize,
    /// Maximum number of segments to keep in memory
    pub max_segments: usize,
    /// Whether to preallocate segments
    pub preallocate: bool,
}

impl Default for AppendLogConfig {
    fn default() -> Self {
        Self {
            segment_size: 64 * 1024 * 1024, // 64 MB
            max_segments: 4,
            preallocate: true,
        }
    }
}

/// A segment in the append-only log
struct LogSegment {
    /// Segment data — UnsafeCell for interior mutability (lock-free writes via CAS)
    data: UnsafeCell<Vec<u8>>,
    /// Current write position
    write_pos: AtomicUsize,
    /// Segment capacity (cached to avoid accessing data through UnsafeCell)
    capacity: usize,
    /// Segment base offset
    base_offset: u64,
    /// Is this segment sealed (no more writes)?
    sealed: AtomicBool,
}

// SAFETY: All accesses to `data` are coordinated through atomic `write_pos`.
// The CAS on write_pos guarantees exclusive write access to the reserved range.
// Reads are bounded by write_pos (Acquire ordering) to prevent torn reads.
unsafe impl Send for LogSegment {}
unsafe impl Sync for LogSegment {}

impl LogSegment {
    fn new(base_offset: u64, capacity: usize, preallocate: bool) -> Self {
        let data = if preallocate {
            vec![0u8; capacity]
        } else {
            let v = vec![0; capacity];
            v
        };

        Self {
            capacity,
            data: UnsafeCell::new(data),
            write_pos: AtomicUsize::new(0),
            base_offset,
            sealed: AtomicBool::new(false),
        }
    }

    /// Try to append data to this segment
    /// Returns (position, entry_offset) on success
    fn try_append(&self, data: &[u8]) -> Option<(usize, u64)> {
        if self.sealed.load(Ordering::Acquire) {
            return None;
        }

        let needed = 4 + data.len(); // 4 bytes for length prefix

        // CAS loop to reserve space
        loop {
            let current_pos = self.write_pos.load(Ordering::Acquire);
            let new_pos = current_pos + needed;

            if new_pos > self.capacity {
                // Segment is full, seal it
                self.sealed.store(true, Ordering::Release);
                return None;
            }

            // Try to reserve space
            match self.write_pos.compare_exchange_weak(
                current_pos,
                new_pos,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Space reserved, write data
                    // SAFETY: The CAS above guarantees exclusive access to
                    // [current_pos..new_pos]. The UnsafeCell provides interior
                    // mutability. No other writer can touch this range.
                    unsafe {
                        let buf = &mut *self.data.get();
                        let ptr = buf.as_mut_ptr();
                        // Write length prefix (big-endian)
                        let len = data.len() as u32;
                        let len_bytes = len.to_be_bytes();
                        std::ptr::copy_nonoverlapping(len_bytes.as_ptr(), ptr.add(current_pos), 4);

                        // Write data
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            ptr.add(current_pos + 4),
                            data.len(),
                        );
                    }

                    let offset = self.base_offset + current_pos as u64;
                    return Some((current_pos, offset));
                }
                Err(_) => {
                    // CAS failed, retry
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Read an entry at the given position
    fn read(&self, position: usize) -> Option<&[u8]> {
        let committed = self.write_pos.load(Ordering::Acquire);

        if position + 4 > committed {
            return None;
        }

        // SAFETY: position..position+4 is within committed range,
        // and all data up to committed has been fully written.
        let buf = unsafe { &*self.data.get() };

        // Read length prefix
        let len_bytes: [u8; 4] = buf[position..position + 4].try_into().ok()?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        if position + 4 + len > committed {
            return None;
        }

        Some(&buf[position + 4..position + 4 + len])
    }

    /// Get committed size
    fn committed_size(&self) -> usize {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Check if segment is sealed
    fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::Acquire)
    }
}

/// A lock-free append-only log for high-throughput message storage
///
/// Design goals:
/// - Lock-free appends from multiple producers
/// - Sequential reads optimized for batching
/// - Memory-efficient with segment rotation
pub struct AppendOnlyLog {
    /// Configuration
    config: AppendLogConfig,
    /// Active segments
    segments: RwLock<Vec<Arc<LogSegment>>>,
    /// Total bytes written
    total_bytes: AtomicU64,
    /// Total entries written (also serves as global offset counter)
    total_entries: AtomicU64,
}

impl AppendOnlyLog {
    /// Create a new append-only log
    pub fn new(config: AppendLogConfig) -> Self {
        let initial_segment = Arc::new(LogSegment::new(0, config.segment_size, config.preallocate));

        Self {
            config,
            segments: RwLock::new(vec![initial_segment]),
            total_bytes: AtomicU64::new(0),
            total_entries: AtomicU64::new(0),
        }
    }

    /// Append data to the log, returns the offset
    pub fn append(&self, data: &[u8]) -> u64 {
        loop {
            // Try to append to current segment
            {
                let segments = self.segments.read();
                if let Some(segment) = segments.last() {
                    if let Some((_, offset)) = segment.try_append(data) {
                        self.total_bytes
                            .fetch_add(data.len() as u64, Ordering::Relaxed);
                        self.total_entries.fetch_add(1, Ordering::Relaxed);
                        return offset;
                    }
                }
            }

            // Segment is full, need to create a new one
            self.rotate_segment();
        }
    }

    /// Append a batch of entries, returns vec of offsets
    pub fn append_batch(&self, entries: &[&[u8]]) -> Vec<u64> {
        let mut offsets = Vec::with_capacity(entries.len());

        for data in entries {
            offsets.push(self.append(data));
        }

        offsets
    }

    /// Rotate to a new segment
    fn rotate_segment(&self) {
        let mut segments = self.segments.write();

        // Double-check the last segment is actually sealed
        if let Some(last) = segments.last() {
            if !last.is_sealed() {
                // Another thread may have already rotated
                return;
            }
        }

        // Calculate next base offset
        let next_base = segments
            .last()
            .map(|s| s.base_offset + s.committed_size() as u64)
            .unwrap_or(0);

        // Create new segment
        let new_segment = Arc::new(LogSegment::new(
            next_base,
            self.config.segment_size,
            self.config.preallocate,
        ));

        segments.push(new_segment);

        // Remove old segments if we have too many
        while segments.len() > self.config.max_segments {
            segments.remove(0);
        }
    }

    /// Read entries starting from an offset
    pub fn read(&self, start_offset: u64, max_entries: usize) -> Vec<Bytes> {
        let segments = self.segments.read();
        let mut entries = Vec::with_capacity(max_entries);

        // Find the segment containing start_offset
        for segment in segments.iter() {
            if segment.base_offset > start_offset {
                continue;
            }

            let relative_pos = (start_offset - segment.base_offset) as usize;
            let mut pos = relative_pos;

            while entries.len() < max_entries {
                match segment.read(pos) {
                    Some(data) => {
                        entries.push(Bytes::copy_from_slice(data));
                        pos += 4 + data.len(); // Move to next entry
                    }
                    None => break,
                }
            }
        }

        entries
    }

    /// Get total bytes written
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get total entries written
    pub fn total_entries(&self) -> u64 {
        self.total_entries.load(Ordering::Relaxed)
    }

    /// Get current end offset
    pub fn end_offset(&self) -> u64 {
        let segments = self.segments.read();
        segments
            .last()
            .map(|s| s.base_offset + s.committed_size() as u64)
            .unwrap_or(0)
    }

    /// Get number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }
}

// ============================================================================
// Sharded Concurrent HashMap
// ============================================================================

/// Number of shards (should be power of 2)
const SHARD_COUNT: usize = 64;

/// A cache-friendly sharded concurrent hash map
///
/// Uses multiple shards to reduce contention. Each shard has its own lock,
/// so operations on different keys in different shards can proceed in parallel.
pub struct ConcurrentHashMap<K, V> {
    shards: [CacheAligned<RwLock<HashMap<K, V>>>; SHARD_COUNT],
    len: AtomicUsize,
}

impl<K: Hash + Eq + Clone, V: Clone> ConcurrentHashMap<K, V> {
    /// Create a new concurrent hash map
    pub fn new() -> Self {
        // Initialize all shards
        let shards = std::array::from_fn(|_| CacheAligned(RwLock::new(HashMap::new())));

        Self {
            shards,
            len: AtomicUsize::new(0),
        }
    }

    /// Get the shard index for a key
    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % SHARD_COUNT
    }

    /// Insert a key-value pair
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].write();

        let old = shard.insert(key, value);
        if old.is_none() {
            self.len.fetch_add(1, Ordering::Relaxed);
        }
        old
    }

    /// Get a value by key
    pub fn get(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].read();
        shard.get(key).cloned()
    }

    /// Check if key exists
    pub fn contains_key(&self, key: &K) -> bool {
        let shard_idx = self.shard_index(key);
        let shard = self.shards[shard_idx].read();
        shard.contains_key(key)
    }

    /// Remove a key
    pub fn remove(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write();

        let removed = shard.remove(key);
        if removed.is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    /// Get approximate length
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Apply a function to a value
    pub fn update<F>(&self, key: &K, f: F) -> Option<V>
    where
        F: FnOnce(&mut V),
    {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].write();

        if let Some(value) = shard.get_mut(key) {
            f(value);
            Some(value.clone())
        } else {
            None
        }
    }

    /// Get or insert with a default value
    pub fn get_or_insert(&self, key: K, default: V) -> V {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].write();

        if let Some(value) = shard.get(&key) {
            value.clone()
        } else {
            self.len.fetch_add(1, Ordering::Relaxed);
            shard.insert(key, default.clone());
            default
        }
    }

    /// Get or insert with a closure
    pub fn get_or_insert_with<F>(&self, key: K, f: F) -> V
    where
        F: FnOnce() -> V,
    {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].write();

        if let Some(value) = shard.get(&key) {
            value.clone()
        } else {
            let value = f();
            self.len.fetch_add(1, Ordering::Relaxed);
            shard.insert(key, value.clone());
            value
        }
    }

    /// Iterate over all entries (snapshot)
    pub fn snapshot(&self) -> Vec<(K, V)> {
        let mut entries = Vec::new();

        for shard in &self.shards {
            let shard = shard.read();
            for (k, v) in shard.iter() {
                entries.push((k.clone(), v.clone()));
            }
        }

        entries
    }

    /// Clear all entries
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
        self.len.store(0, Ordering::Relaxed);
    }
}

impl<K: Hash + Eq + Clone, V: Clone> Default for ConcurrentHashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Lock-Free Skip List for Offset Indexing
// ============================================================================

/// Maximum height for skip list nodes
const MAX_HEIGHT: usize = 32;

/// A skip list node
struct SkipNode<K, V> {
    key: K,
    value: UnsafeCell<V>,
    /// Forward pointers for each level
    forward: [AtomicPtr<SkipNode<K, V>>; MAX_HEIGHT],
    /// Number of levels this node participates in (used for probabilistic balancing)
    #[allow(dead_code)]
    height: usize,
}

impl<K, V> SkipNode<K, V> {
    fn new(key: K, value: V, height: usize) -> *mut Self {
        let forward = std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut()));

        let node = Box::new(Self {
            key,
            value: UnsafeCell::new(value),
            forward,
            height,
        });

        Box::into_raw(node)
    }
}

/// A concurrent skip list optimized for offset lookups
///
/// Provides O(log n) lookups for finding messages by offset, which is
/// critical for efficient consumer fetching.
///
/// # Design: Append-Only
///
/// This skip list intentionally does **not** implement `remove()`. It is designed for
/// append-only offset indexing where:
///
/// 1. **Entries are never deleted individually** — the entire skip list is dropped
///    when the corresponding segment is compacted or deleted
/// 2. **Memory lifecycle is tied to segment lifetime** — when a segment is removed,
///    the skip list is dropped via the `Drop` implementation, which properly
///    deallocates all nodes
/// 3. **Offset indices are monotonically increasing** — no reuse of keys
///
/// This design provides better performance than a skip list with remove support:
/// - No ABA problem handling required
/// - Simpler atomic operations (no backlink management)
/// - Predictable memory layout
pub struct ConcurrentSkipList<K: Ord + Clone, V: Clone> {
    /// Sentinel head node
    head: *mut SkipNode<K, V>,
    /// Current maximum height
    max_level: AtomicUsize,
    /// Number of elements
    len: AtomicUsize,
    /// Random state for level generation
    rand_state: AtomicU64,
}

// SAFETY: The skip list is thread-safe through atomic operations
unsafe impl<K: Ord + Clone + Send, V: Clone + Send> Send for ConcurrentSkipList<K, V> {}
unsafe impl<K: Ord + Clone + Sync, V: Clone + Sync> Sync for ConcurrentSkipList<K, V> {}

impl<K: Ord + Clone + Default, V: Clone + Default> ConcurrentSkipList<K, V> {
    /// Create a new concurrent skip list
    pub fn new() -> Self {
        // Create head sentinel
        let head = SkipNode::new(K::default(), V::default(), MAX_HEIGHT);

        // Use runtime entropy for PRNG seed (process ID, time, address)
        let seed = Self::generate_seed();

        Self {
            head,
            max_level: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            rand_state: AtomicU64::new(seed),
        }
    }

    /// Generate a random seed using runtime entropy sources
    fn generate_seed() -> u64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        // RandomState uses OS entropy (getrandom on Linux, arc4random on macOS)
        let state = RandomState::new();
        let mut hasher = state.build_hasher();

        // Mix in additional entropy sources
        hasher.write_u64(std::process::id().into());

        if let Ok(time) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            hasher.write_u64(time.as_nanos() as u64);
        }

        // Mix in address of the hasher itself for extra entropy
        hasher.write_usize(&hasher as *const _ as usize);

        // Ensure non-zero (XORShift requires non-zero seed)
        hasher.finish().max(1)
    }

    /// Generate a random level for a new node
    fn random_level(&self) -> usize {
        // XORShift random number generation with atomic CAS
        let mut level = 1;

        let x = self
            .rand_state
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |mut x| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                Some(x)
            })
            .unwrap_or(1);

        let mut bits = x;
        while bits & 1 == 0 && level < MAX_HEIGHT {
            level += 1;
            bits >>= 1;
        }

        level
    }

    /// Insert a key-value pair
    pub fn insert(&self, key: K, value: V) {
        let height = self.random_level();
        let new_node = SkipNode::new(key.clone(), value, height);

        // Update max level if needed
        let mut current_max = self.max_level.load(Ordering::Relaxed);
        while height > current_max {
            match self.max_level.compare_exchange_weak(
                current_max,
                height,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(m) => current_max = m,
            }
        }

        // Find position and insert
        let mut update = [std::ptr::null_mut::<SkipNode<K, V>>(); MAX_HEIGHT];
        let mut current = self.head;

        #[allow(clippy::needless_range_loop)]
        // Intentional: unsafe pointer array requires explicit indexing
        for i in (0..self.max_level.load(Ordering::Acquire)).rev() {
            // SAFETY: We traverse from head which is always valid. Each `forward` pointer
            // is either null or points to a valid SkipNode allocated by `insert`.
            // Acquire ordering ensures we see the complete node data.
            unsafe {
                loop {
                    let next = (*current).forward[i].load(Ordering::Acquire);
                    if next.is_null() || (*next).key >= key {
                        break;
                    }
                    current = next;
                }
                update[i] = current;
            }
        }

        // Insert node at each level using compare_exchange (C-4 fix).
        //
        // The standard lock-free skip list algorithm (Herlihy & Shavit) requires
        // a CAS loop for each level's forward pointer update. Without CAS, two
        // concurrent inserts at the same position both read the same `next`,
        // link to it, and one's `store` silently overwrites the other — permanently
        // losing a node. The CAS detects this conflict and re-traverses to find
        // the correct predecessor.
        //
        // Additionally, the predecessor from the initial traversal (`update[i]`)
        // may become stale if another thread inserts a node between `pred` and
        // our key. We validate that `next.key >= key` before each CAS attempt
        // to ensure correct ordering; if not, we re-traverse first.
        #[allow(clippy::needless_range_loop)]
        for i in 0..height {
            // SAFETY: `pred` is either `self.head` (always valid) or a node from `update`
            // which was found during traversal. `new_node` was just allocated above.
            // AcqRel ordering on the CAS ensures:
            //   - Acquire: we see the latest forward pointer written by other threads
            //   - Release: the new node's data is fully visible before it's linked in
            unsafe {
                let mut pred = if update[i].is_null() {
                    self.head
                } else {
                    update[i]
                };

                loop {
                    let next = (*pred).forward[i].load(Ordering::Acquire);

                    // Validate that the predecessor is still correct: the next
                    // node must be null or have a key >= ours. If another thread
                    // inserted a smaller key after `pred`, `next` would have
                    // key < ours and we'd break sort order by linking here.
                    if !next.is_null() && (*next).key < key {
                        // Predecessor is stale — re-traverse this level.
                        let mut cur = self.head;
                        loop {
                            let n = (*cur).forward[i].load(Ordering::Acquire);
                            if n.is_null() || (*n).key >= key {
                                break;
                            }
                            cur = n;
                        }
                        pred = cur;
                        continue;
                    }

                    (*new_node).forward[i].store(next, Ordering::Release);

                    // Attempt to atomically link: pred.forward[i] = new_node
                    // This only succeeds if pred.forward[i] still points to `next`
                    match (*pred).forward[i].compare_exchange(
                        next,
                        new_node,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break, // Successfully linked at this level
                        Err(_) => {
                            // Another thread modified pred.forward[i] concurrently.
                            // Re-traverse this level to find the correct predecessor.
                            let mut cur = self.head;
                            loop {
                                let n = (*cur).forward[i].load(Ordering::Acquire);
                                if n.is_null() || (*n).key >= key {
                                    break;
                                }
                                cur = n;
                            }
                            pred = cur;
                            // Retry the CAS with the updated predecessor
                        }
                    }
                }
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Find a value by key
    pub fn get(&self, key: &K) -> Option<V> {
        let mut current = self.head;

        for i in (0..self.max_level.load(Ordering::Acquire)).rev() {
            // SAFETY: Traversal starts from `self.head` which is always valid.
            // All forward pointers are either null or point to valid SkipNodes.
            // Acquire ordering ensures we see the node's complete data.
            unsafe {
                loop {
                    let next = (*current).forward[i].load(Ordering::Acquire);
                    if next.is_null() {
                        break;
                    }
                    if (*next).key == *key {
                        return Some((*(*next).value.get()).clone());
                    }
                    if (*next).key > *key {
                        break;
                    }
                    current = next;
                }
            }
        }

        None
    }

    /// Find the greatest key less than or equal to the given key
    /// This is useful for finding the closest offset <= target
    pub fn floor(&self, key: &K) -> Option<(K, V)> {
        let mut current = self.head;
        let mut result: Option<*mut SkipNode<K, V>> = None;

        for i in (0..self.max_level.load(Ordering::Acquire)).rev() {
            // SAFETY: Traversal starts from `self.head` which is always valid.
            // All forward pointers are either null or point to valid SkipNodes.
            // Acquire ordering ensures visibility of node data.
            unsafe {
                loop {
                    let next = (*current).forward[i].load(Ordering::Acquire);
                    if next.is_null() {
                        break;
                    }
                    if (*next).key <= *key {
                        result = Some(next);
                        current = next;
                    } else {
                        break;
                    }
                }
            }
        }

        // SAFETY: `node` was obtained from traversal and is a valid SkipNode pointer.
        result.map(|node| unsafe { ((*node).key.clone(), (*(*node).value.get()).clone()) })
    }

    /// Find the smallest key greater than or equal to the given key
    pub fn ceiling(&self, key: &K) -> Option<(K, V)> {
        let mut current = self.head;

        for i in (0..self.max_level.load(Ordering::Acquire)).rev() {
            // SAFETY: Traversal starts from `self.head` which is always valid.
            // All forward pointers are either null or point to valid SkipNodes.
            unsafe {
                loop {
                    let next = (*current).forward[i].load(Ordering::Acquire);
                    if next.is_null() || (*next).key >= *key {
                        break;
                    }
                    current = next;
                }
            }
        }

        // SAFETY: `current` is a valid node from traversal (or head).
        // The next pointer is either null or points to a valid SkipNode.
        unsafe {
            let next = (*current).forward[0].load(Ordering::Acquire);
            if !next.is_null() {
                Some(((*next).key.clone(), (*(*next).value.get()).clone()))
            } else {
                None
            }
        }
    }

    /// Get number of elements
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a range of entries
    pub fn range(&self, start: &K, end: &K, limit: usize) -> Vec<(K, V)> {
        let mut entries = Vec::with_capacity(limit.min(1000));
        let mut current = self.head;

        // Find start position
        for i in (0..self.max_level.load(Ordering::Acquire)).rev() {
            // SAFETY: Traversal starts from `self.head` which is always valid.
            // All forward pointers are either null or point to valid SkipNodes.
            unsafe {
                loop {
                    let next = (*current).forward[i].load(Ordering::Acquire);
                    if next.is_null() || (*next).key >= *start {
                        break;
                    }
                    current = next;
                }
            }
        }

        // Collect entries in range
        // SAFETY: `current` is valid from traversal. We iterate level-0 forward pointers
        // which are either null or point to valid SkipNodes allocated by `insert`.
        unsafe {
            let mut node = (*current).forward[0].load(Ordering::Acquire);
            while !node.is_null() && entries.len() < limit {
                if (*node).key > *end {
                    break;
                }
                entries.push(((*node).key.clone(), (*(*node).value.get()).clone()));
                node = (*node).forward[0].load(Ordering::Acquire);
            }
        }

        entries
    }
}

impl<K: Ord + Clone + Default, V: Clone + Default> Default for ConcurrentSkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V: Clone> Drop for ConcurrentSkipList<K, V> {
    fn drop(&mut self) {
        // Free all nodes
        let mut current = self.head;
        // SAFETY: We have exclusive access via `&mut self`. All nodes were allocated
        // by `insert` using `Box::into_raw`. We traverse level-0 to visit every node
        // exactly once, converting them back to Box for proper deallocation.
        unsafe {
            while !current.is_null() {
                let next = (*current).forward[0].load(Ordering::Relaxed);
                drop(Box::from_raw(current));
                current = next;
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lock_free_queue() {
        let queue = LockFreeQueue::<i32>::bounded(100);

        assert!(queue.is_empty());

        queue.push(1).unwrap();
        queue.push(2).unwrap();
        queue.push(3).unwrap();

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop(), Some(1));
        assert_eq!(queue.pop(), Some(2));
        assert_eq!(queue.pop(), Some(3));
        assert!(queue.is_empty());
    }

    #[test]
    fn test_lock_free_queue_concurrent() {
        let queue = Arc::new(LockFreeQueue::<i32>::bounded(1000));
        let mut handles = vec![];

        // Spawn producers
        for i in 0..4 {
            let q = queue.clone();
            handles.push(thread::spawn(move || {
                for j in 0..250 {
                    q.push(i * 250 + j).unwrap();
                }
            }));
        }

        // Wait for producers
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(queue.len(), 1000);

        // Consume all
        let batch = queue.pop_batch(1000);
        assert_eq!(batch.len(), 1000);
    }

    #[test]
    fn test_append_only_log() {
        let config = AppendLogConfig {
            segment_size: 1024,
            max_segments: 4,
            preallocate: true,
        };
        let log = AppendOnlyLog::new(config);

        let offset1 = log.append(b"hello");
        let offset2 = log.append(b"world");

        assert!(offset2 > offset1);

        let entries = log.read(offset1, 10);
        assert_eq!(entries.len(), 2);
        assert_eq!(&entries[0][..], b"hello");
        assert_eq!(&entries[1][..], b"world");
    }

    #[test]
    fn test_concurrent_hash_map() {
        let map = Arc::new(ConcurrentHashMap::<String, i32>::new());
        let mut handles = vec![];

        // Concurrent inserts
        for i in 0..4 {
            let m = map.clone();
            handles.push(thread::spawn(move || {
                for j in 0..250 {
                    m.insert(format!("key-{}-{}", i, j), i * 250 + j);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(map.len(), 1000);

        // Verify reads
        assert_eq!(map.get(&"key-0-0".to_string()), Some(0));
        assert_eq!(map.get(&"key-3-249".to_string()), Some(999));
    }

    #[test]
    fn test_skip_list() {
        let list = ConcurrentSkipList::<u64, String>::new();

        list.insert(10, "ten".to_string());
        list.insert(20, "twenty".to_string());
        list.insert(5, "five".to_string());
        list.insert(15, "fifteen".to_string());

        assert_eq!(list.len(), 4);
        assert_eq!(list.get(&10), Some("ten".to_string()));
        assert_eq!(list.get(&99), None);

        // Floor test
        assert_eq!(list.floor(&12), Some((10, "ten".to_string())));
        assert_eq!(list.floor(&15), Some((15, "fifteen".to_string())));

        // Ceiling test
        assert_eq!(list.ceiling(&12), Some((15, "fifteen".to_string())));
        assert_eq!(list.ceiling(&1), Some((5, "five".to_string())));
    }

    #[test]
    fn test_skip_list_range() {
        let list = ConcurrentSkipList::<u64, String>::new();

        for i in 0..100 {
            list.insert(i * 10, format!("value-{}", i));
        }

        let range = list.range(&150, &350, 100);
        assert!(!range.is_empty());

        // Should include 150, 160, ..., 350
        for (k, _) in &range {
            assert!(*k >= 150 && *k <= 350);
        }
    }

    /// Regression test for C-4: concurrent inserts must not lose nodes.
    ///
    /// Before the CAS fix, two threads inserting at the same position would
    /// race on `pred.forward[i].store()`, causing one node to be silently
    /// lost. This test verifies that all inserted keys are retrievable.
    #[test]
    fn test_skip_list_concurrent_insert_no_lost_nodes() {
        let list = Arc::new(ConcurrentSkipList::<u64, u64>::new());
        let per_thread = 500;
        let num_threads = 8;
        let mut handles = vec![];

        for t in 0..num_threads {
            let l = list.clone();
            handles.push(thread::spawn(move || {
                for i in 0..per_thread {
                    // Interleave keys across threads to maximize contention
                    let key = (i * num_threads + t) as u64;
                    l.insert(key, key);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let expected = (num_threads * per_thread) as usize;
        assert_eq!(
            list.len(),
            expected,
            "Expected {} entries but got {} — nodes were lost under concurrency",
            expected,
            list.len()
        );

        // Verify every single key is retrievable
        for i in 0..(num_threads * per_thread) {
            let key = i as u64;
            assert!(
                list.get(&key).is_some(),
                "Key {} was lost under concurrent insertion",
                key
            );
        }
    }
}
