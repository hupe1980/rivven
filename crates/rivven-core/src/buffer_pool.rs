//! Zero-Copy Buffer Pool
//!
//! High-performance buffer management for Rivven with:
//! - **Slab Allocation**: Pre-allocated buffers to avoid runtime allocation
//! - **Size Classes**: Different buffer sizes for optimal memory usage
//! - **Thread-Local Caching**: Reduce contention in hot paths
//! - **Reference Counting**: Safe buffer sharing without copies
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Buffer Pool                                │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐ │
//! │  │  Small   │  │  Medium  │  │  Large   │  │   Huge (alloc)   │ │
//! │  │  <= 4KB  │  │  <= 64KB │  │  <= 1MB  │  │   > 1MB          │ │
//! │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────────┬─────────┘ │
//! │       │              │              │                │          │
//! │       └──────────────┴──────────────┴────────────────┘          │
//! │                           │                                      │
//! │              ┌────────────▼────────────┐                        │
//! │              │    Thread-Local Cache    │                        │
//! │              │   (lock-free fast path)  │                        │
//! │              └────────────┬────────────┘                        │
//! │                           │                                      │
//! │              ┌────────────▼────────────┐                        │
//! │              │    Global Pool (CAS)     │                        │
//! │              └─────────────────────────┘                        │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Performance Characteristics
//!
//! - Allocation: O(1) for cached sizes
//! - Deallocation: O(1) (return to pool)
//! - Memory overhead: ~4 bytes per buffer (ref count)
//! - Contention: Near-zero with thread-local caching

use bytes::{Bytes, BytesMut};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// ============================================================================
// Configuration
// ============================================================================

/// Buffer size classes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeClass {
    /// Small buffers (<= 4KB) - for headers, small messages
    Small = 0,
    /// Medium buffers (<= 64KB) - for typical messages
    Medium = 1,
    /// Large buffers (<= 1MB) - for batch operations
    Large = 2,
    /// Huge buffers (> 1MB) - direct allocation
    Huge = 3,
}

impl SizeClass {
    /// Get the buffer size for this class
    pub const fn size(&self) -> usize {
        match self {
            Self::Small => 4 * 1024,    // 4 KB
            Self::Medium => 64 * 1024,  // 64 KB
            Self::Large => 1024 * 1024, // 1 MB
            Self::Huge => 0,            // Dynamic
        }
    }

    /// Determine size class for a given size
    pub fn for_size(size: usize) -> Self {
        if size <= Self::Small.size() {
            Self::Small
        } else if size <= Self::Medium.size() {
            Self::Medium
        } else if size <= Self::Large.size() {
            Self::Large
        } else {
            Self::Huge
        }
    }
}

/// Buffer pool configuration
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Number of small buffers to pre-allocate
    pub small_pool_size: usize,
    /// Number of medium buffers to pre-allocate  
    pub medium_pool_size: usize,
    /// Number of large buffers to pre-allocate
    pub large_pool_size: usize,
    /// Thread-local cache size per size class
    pub thread_cache_size: usize,
    /// Enable memory usage tracking
    pub enable_tracking: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            small_pool_size: 1024,
            medium_pool_size: 256,
            large_pool_size: 32,
            thread_cache_size: 16,
            enable_tracking: true,
        }
    }
}

impl BufferPoolConfig {
    /// Configuration for high-throughput workloads
    pub fn high_throughput() -> Self {
        Self {
            small_pool_size: 4096,
            medium_pool_size: 1024,
            large_pool_size: 128,
            thread_cache_size: 64,
            enable_tracking: false,
        }
    }

    /// Configuration for low-memory environments
    pub fn low_memory() -> Self {
        Self {
            small_pool_size: 256,
            medium_pool_size: 64,
            large_pool_size: 8,
            thread_cache_size: 4,
            enable_tracking: true,
        }
    }
}

// ============================================================================
// Pool Statistics
// ============================================================================

/// Buffer pool statistics
#[derive(Debug, Default)]
pub struct PoolStats {
    /// Total allocations
    pub allocations: AtomicU64,
    /// Total deallocations (returns to pool)
    pub deallocations: AtomicU64,
    /// Cache hits (from thread-local)
    pub cache_hits: AtomicU64,
    /// Cache misses (went to global pool)
    pub cache_misses: AtomicU64,
    /// Pool misses (new allocation)
    pub pool_misses: AtomicU64,
    /// Current bytes allocated
    pub bytes_allocated: AtomicUsize,
    /// Peak bytes allocated
    pub peak_bytes: AtomicUsize,
}

impl PoolStats {
    /// Get cache hit rate (0.0 - 1.0)
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get pool hit rate (0.0 - 1.0)
    pub fn pool_hit_rate(&self) -> f64 {
        let allocs = self.allocations.load(Ordering::Relaxed);
        let misses = self.pool_misses.load(Ordering::Relaxed);
        if allocs == 0 {
            1.0
        } else {
            1.0 - (misses as f64 / allocs as f64)
        }
    }
}

// ============================================================================
// Global Buffer Pool
// ============================================================================

/// Global buffer pool with size-class segregation
pub struct BufferPool {
    /// Small buffer free list
    small_pool: (Sender<BytesMut>, Receiver<BytesMut>),
    /// Medium buffer free list
    medium_pool: (Sender<BytesMut>, Receiver<BytesMut>),
    /// Large buffer free list
    large_pool: (Sender<BytesMut>, Receiver<BytesMut>),
    /// Configuration
    config: BufferPoolConfig,
    /// Statistics
    stats: Arc<PoolStats>,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(config: BufferPoolConfig) -> Arc<Self> {
        let small_pool = bounded(config.small_pool_size);
        let medium_pool = bounded(config.medium_pool_size);
        let large_pool = bounded(config.large_pool_size);

        let pool = Arc::new(Self {
            small_pool,
            medium_pool,
            large_pool,
            config: config.clone(),
            stats: Arc::new(PoolStats::default()),
        });

        // Pre-allocate buffers
        pool.preallocate();

        pool
    }

    /// Pre-allocate buffers to fill the pools
    fn preallocate(&self) {
        // Small buffers
        for _ in 0..self.config.small_pool_size {
            let buf = BytesMut::with_capacity(SizeClass::Small.size());
            let _ = self.small_pool.0.try_send(buf);
        }

        // Medium buffers
        for _ in 0..self.config.medium_pool_size {
            let buf = BytesMut::with_capacity(SizeClass::Medium.size());
            let _ = self.medium_pool.0.try_send(buf);
        }

        // Large buffers
        for _ in 0..self.config.large_pool_size {
            let buf = BytesMut::with_capacity(SizeClass::Large.size());
            let _ = self.large_pool.0.try_send(buf);
        }
    }

    /// Allocate a buffer of at least the given size
    pub fn allocate(&self, size: usize) -> BytesMut {
        if self.config.enable_tracking {
            self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        }

        let class = SizeClass::for_size(size);
        let (receiver, class_size) = match class {
            SizeClass::Small => (&self.small_pool.1, SizeClass::Small.size()),
            SizeClass::Medium => (&self.medium_pool.1, SizeClass::Medium.size()),
            SizeClass::Large => (&self.large_pool.1, SizeClass::Large.size()),
            SizeClass::Huge => {
                // Huge buffers are always freshly allocated
                if self.config.enable_tracking {
                    self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);
                    self.update_bytes_allocated(size as isize);
                }
                return BytesMut::with_capacity(size);
            }
        };

        // Try to get from pool
        match receiver.try_recv() {
            Ok(mut buf) => {
                buf.clear();
                if self.config.enable_tracking {
                    self.update_bytes_allocated(class_size as isize);
                }
                buf
            }
            Err(_) => {
                // Pool empty, allocate new
                if self.config.enable_tracking {
                    self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);
                    self.update_bytes_allocated(class_size as isize);
                }
                BytesMut::with_capacity(class_size)
            }
        }
    }

    /// Return a buffer to the pool
    pub fn deallocate(&self, mut buf: BytesMut) {
        if self.config.enable_tracking {
            self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
            self.update_bytes_allocated(-(buf.capacity() as isize));
        }

        buf.clear();
        let class = SizeClass::for_size(buf.capacity());

        let sender = match class {
            SizeClass::Small => &self.small_pool.0,
            SizeClass::Medium => &self.medium_pool.0,
            SizeClass::Large => &self.large_pool.0,
            SizeClass::Huge => return, // Don't pool huge buffers
        };

        // Try to return to pool, drop if full
        let _ = sender.try_send(buf);
    }

    /// Get pool statistics
    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }

    fn update_bytes_allocated(&self, delta: isize) {
        if delta > 0 {
            let new = self
                .stats
                .bytes_allocated
                .fetch_add(delta as usize, Ordering::Relaxed)
                + delta as usize;
            // Update peak if necessary
            let mut peak = self.stats.peak_bytes.load(Ordering::Relaxed);
            while new > peak {
                match self.stats.peak_bytes.compare_exchange_weak(
                    peak,
                    new,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        } else {
            self.stats
                .bytes_allocated
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Thread-Local Buffer Cache
// ============================================================================

thread_local! {
    static THREAD_CACHE: RefCell<ThreadCache> = RefCell::new(ThreadCache::new());
}

/// Thread-local buffer cache for lock-free fast path
struct ThreadCache {
    small: Vec<BytesMut>,
    medium: Vec<BytesMut>,
    large: Vec<BytesMut>,
    max_size: usize,
}

impl ThreadCache {
    fn new() -> Self {
        Self {
            small: Vec::with_capacity(16),
            medium: Vec::with_capacity(8),
            large: Vec::with_capacity(4),
            max_size: 16,
        }
    }

    fn get(&mut self, class: SizeClass) -> Option<BytesMut> {
        match class {
            SizeClass::Small => self.small.pop(),
            SizeClass::Medium => self.medium.pop(),
            SizeClass::Large => self.large.pop(),
            SizeClass::Huge => None,
        }
    }

    fn put(&mut self, buf: BytesMut) -> bool {
        let class = SizeClass::for_size(buf.capacity());
        let (cache, max) = match class {
            SizeClass::Small => (&mut self.small, self.max_size),
            SizeClass::Medium => (&mut self.medium, self.max_size / 2),
            SizeClass::Large => (&mut self.large, self.max_size / 4),
            SizeClass::Huge => return false,
        };

        if cache.len() < max {
            cache.push(buf);
            true
        } else {
            false
        }
    }
}

// ============================================================================
// Pooled Buffer Handle
// ============================================================================

/// A buffer handle that returns to the pool on drop
pub struct PooledBuffer {
    inner: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl PooledBuffer {
    /// Create from pool
    pub fn new(pool: Arc<BufferPool>, size: usize) -> Self {
        // Try thread-local cache first
        let buf = THREAD_CACHE
            .with(|cache| {
                let mut cache = cache.borrow_mut();
                let class = SizeClass::for_size(size);
                cache.get(class)
            })
            .unwrap_or_else(|| {
                if pool.config.enable_tracking {
                    pool.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
                }
                pool.allocate(size)
            });

        if pool.config.enable_tracking && buf.capacity() > 0 {
            pool.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
        }

        Self {
            inner: Some(buf),
            pool,
        }
    }

    /// Get mutable reference to buffer
    pub fn inner_mut(&mut self) -> &mut BytesMut {
        self.inner.as_mut().unwrap()
    }

    /// Get immutable reference to buffer
    pub fn inner_ref(&self) -> &BytesMut {
        self.inner.as_ref().unwrap()
    }

    /// Freeze into immutable Bytes (consumes the buffer)
    pub fn freeze(mut self) -> Bytes {
        self.inner.take().unwrap().freeze()
    }

    /// Get length of data in buffer
    pub fn len(&self) -> usize {
        self.inner.as_ref().map(|b| b.len()).unwrap_or(0)
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get capacity of buffer
    pub fn capacity(&self) -> usize {
        self.inner.as_ref().map(|b| b.capacity()).unwrap_or(0)
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(mut buf) = self.inner.take() {
            buf.clear();

            // Try thread-local cache first
            let returned = THREAD_CACHE.with(|cache| cache.borrow_mut().put(buf.clone()));

            if !returned {
                // Thread-local cache full, return to global pool
                self.pool.deallocate(buf);
            }
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

// ============================================================================
// Zero-Copy Buffer Chain
// ============================================================================

/// A chain of buffers for scatter-gather I/O
#[derive(Default)]
pub struct BufferChain {
    buffers: Vec<Bytes>,
    total_len: usize,
}

impl BufferChain {
    /// Create empty chain
    pub fn new() -> Self {
        Self::default()
    }

    /// Create chain with single buffer
    pub fn single(buf: Bytes) -> Self {
        let len = buf.len();
        Self {
            buffers: vec![buf],
            total_len: len,
        }
    }

    /// Append a buffer to the chain
    pub fn push(&mut self, buf: Bytes) {
        self.total_len += buf.len();
        self.buffers.push(buf);
    }

    /// Prepend a buffer to the chain
    pub fn prepend(&mut self, buf: Bytes) {
        self.total_len += buf.len();
        self.buffers.insert(0, buf);
    }

    /// Get total length of all buffers
    pub fn len(&self) -> usize {
        self.total_len
    }

    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Get number of buffers in chain
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Iterate over buffers
    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.buffers.iter()
    }

    /// Flatten into single buffer (copies data)
    pub fn flatten(self) -> Bytes {
        if self.buffers.len() == 1 {
            return self.buffers.into_iter().next().unwrap();
        }

        let mut result = BytesMut::with_capacity(self.total_len);
        for buf in self.buffers {
            result.extend_from_slice(&buf);
        }
        result.freeze()
    }

    /// Convert to iovec-style slices for vectored I/O
    pub fn as_slices(&self) -> Vec<&[u8]> {
        self.buffers.iter().map(|b| b.as_ref()).collect()
    }
}

// ============================================================================
// Aligned Buffer for Direct I/O
// ============================================================================

/// Buffer aligned for direct I/O (O_DIRECT)
/// Required alignment is typically 512 bytes or 4KB
#[repr(C, align(4096))]
pub struct AlignedBuffer {
    data: [u8; 4096],
}

impl Default for AlignedBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl AlignedBuffer {
    /// Create new aligned buffer
    pub const fn new() -> Self {
        Self { data: [0u8; 4096] }
    }

    /// Get slice reference
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable slice reference
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Check alignment
    pub fn is_aligned(&self) -> bool {
        (self.data.as_ptr() as usize).is_multiple_of(4096)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_class() {
        assert_eq!(SizeClass::for_size(100), SizeClass::Small);
        assert_eq!(SizeClass::for_size(4096), SizeClass::Small);
        assert_eq!(SizeClass::for_size(4097), SizeClass::Medium);
        assert_eq!(SizeClass::for_size(65536), SizeClass::Medium);
        assert_eq!(SizeClass::for_size(65537), SizeClass::Large);
        assert_eq!(SizeClass::for_size(1024 * 1024), SizeClass::Large);
        assert_eq!(SizeClass::for_size(1024 * 1024 + 1), SizeClass::Huge);
    }

    #[test]
    fn test_buffer_pool_allocate() {
        let pool = BufferPool::new(BufferPoolConfig::default());

        let buf1 = pool.allocate(100);
        assert!(buf1.capacity() >= 100);
        assert!(buf1.capacity() <= SizeClass::Small.size());

        let buf2 = pool.allocate(10000);
        assert!(buf2.capacity() >= 10000);
        assert!(buf2.capacity() <= SizeClass::Medium.size());
    }

    #[test]
    fn test_buffer_pool_roundtrip() {
        let pool = BufferPool::new(BufferPoolConfig::default());

        let buf = pool.allocate(1000);
        let cap = buf.capacity();

        pool.deallocate(buf);

        let buf2 = pool.allocate(1000);
        assert_eq!(buf2.capacity(), cap);
    }

    #[test]
    fn test_pooled_buffer() {
        let pool = BufferPool::new(BufferPoolConfig::default());

        {
            let mut buf = PooledBuffer::new(pool.clone(), 1000);
            buf.extend_from_slice(b"hello world");
            assert_eq!(buf.len(), 11);
        }
        // Buffer returned to pool on drop
    }

    #[test]
    fn test_buffer_chain() {
        let mut chain = BufferChain::new();
        chain.push(Bytes::from_static(b"hello "));
        chain.push(Bytes::from_static(b"world"));

        assert_eq!(chain.len(), 11);
        assert_eq!(chain.buffer_count(), 2);

        let flat = chain.flatten();
        assert_eq!(&flat[..], b"hello world");
    }

    #[test]
    fn test_aligned_buffer() {
        let buf = AlignedBuffer::new();
        assert!(buf.is_aligned());
    }

    #[test]
    fn test_pool_stats() {
        let config = BufferPoolConfig {
            enable_tracking: true,
            ..Default::default()
        };
        let pool = BufferPool::new(config);

        let _buf1 = pool.allocate(100);
        let _buf2 = pool.allocate(200);

        assert_eq!(pool.stats().allocations.load(Ordering::Relaxed), 2);
    }
}
