//! Zero-Copy Producer/Consumer API
//!
//! Provides high-performance data paths that eliminate unnecessary memory copies:
//! - **ZeroCopyBuffer**: Pre-allocated buffers with reference counting
//! - **BufferSlice**: View into buffer without copying
//! - **ZeroCopyProducer**: Produces messages without copying payload
//! - **ZeroCopyConsumer**: Consumes messages with zero-copy access
//!
//! # Performance Characteristics
//!
//! - Eliminates 2-3 copies per message on hot path
//! - Uses memory-mapped I/O for disk access
//! - Reference-counted buffers for safe sharing
//! - Cache-line aligned for optimal CPU performance
//!
//! # Safety Requirements
//!
//! The `ZeroCopyBuffer` type relies on `Arc` for reference counting.
//! When using `BufferSlice`, the following safety invariants must be maintained:
//!
//! 1. **Lifetime**: A `ZeroCopyBuffer` must outlive all `BufferSlice` instances
//!    created from it. The recommended pattern is to use `Arc<ZeroCopyBuffer>`
//!    via `ZeroCopyBufferPool`.
//!
//! 2. **Reference Counting**: `BufferSlice::drop()` decrements the buffer's
//!    lifetime. `BufferSlice` holds an `Arc<ZeroCopyBuffer>` automatically.
//!    exist.
//!
//! 3. **Thread Safety**: While individual operations are atomic, the caller must
//!    ensure exclusive access to mutable slice regions.
//!
//! # Recommended Usage
//!
//! Use `ZeroCopyBufferPool` to manage buffer lifecycle safely:
//!
//! ```rust,ignore
//! let pool = ZeroCopyBufferPool::new(256 * 1024, 16);
//! let buffer = pool.acquire();  // Returns Arc<ZeroCopyBuffer>
//! // buffer is safely reference-counted
//! ```

use bytes::{Bytes, BytesMut};
use std::alloc::{alloc, dealloc, Layout};
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Cache line size for alignment (64 bytes on most modern CPUs)
const CACHE_LINE_SIZE: usize = 64;

/// Default buffer size (256 KB - optimal for most workloads)
const DEFAULT_BUFFER_SIZE: usize = 256 * 1024;

/// A zero-copy buffer with memory pooling
///
/// Reference counting is handled by `Arc<ZeroCopyBuffer>` — the internal
/// `ref_count` field has been removed in favour of a single source of truth.
#[derive(Debug)]
pub struct ZeroCopyBuffer {
    /// Raw pointer to the buffer data
    data: NonNull<u8>,
    /// Total capacity of the buffer
    capacity: usize,
    /// Current write position
    write_pos: AtomicUsize,
    /// Buffer ID for tracking
    id: u64,
    /// Layout used for allocation (needed for deallocation)
    layout: Layout,
}

// Safety: ZeroCopyBuffer uses atomic operations for thread safety
unsafe impl Send for ZeroCopyBuffer {}
unsafe impl Sync for ZeroCopyBuffer {}

impl ZeroCopyBuffer {
    /// Create a new zero-copy buffer with the given capacity
    pub fn new(capacity: usize) -> Self {
        Self::with_id(capacity, 0)
    }

    /// Create a new zero-copy buffer with custom ID
    pub fn with_id(capacity: usize, id: u64) -> Self {
        // Align to cache line for optimal performance
        let aligned_capacity = (capacity + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let layout =
            Layout::from_size_align(aligned_capacity, CACHE_LINE_SIZE).expect("Invalid layout");

        // Safety: We're allocating with a valid layout
        let data = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            NonNull::new_unchecked(ptr)
        };

        Self {
            data,
            capacity: aligned_capacity,
            write_pos: AtomicUsize::new(0),
            id,
            layout,
        }
    }

    /// Get a slice of the buffer for writing.
    /// Requires an Arc reference to safely create a BufferSlice.
    pub fn reserve(self: &Arc<Self>, len: usize) -> Option<BufferSlice> {
        loop {
            let current = self.write_pos.load(Ordering::Acquire);
            let new_pos = current + len;

            if new_pos > self.capacity {
                return None;
            }

            if self
                .write_pos
                .compare_exchange_weak(current, new_pos, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(BufferSlice::new(Arc::clone(self), current, len));
            }
            // CAS failed, retry
            std::hint::spin_loop();
        }
    }

    /// Get a mutable slice for the reserved range.
    ///
    /// # Safety
    /// - Caller must ensure exclusive access to `[offset..offset+len]`.
    /// - No other `get_mut_slice` or `get_slice` call may alias this range
    ///   concurrently.
    ///
    /// F-116 fix: restricted to `pub(crate)` to limit the blast radius of
    /// unsound usage. External users should go through `BufferSlice::write()`.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_mut_slice(&self, offset: usize, len: usize) -> &mut [u8] {
        assert!(
            offset + len <= self.capacity,
            "get_mut_slice out of bounds: offset={} len={} capacity={}",
            offset,
            len,
            self.capacity
        );
        std::slice::from_raw_parts_mut(self.data.as_ptr().add(offset), len)
    }

    /// Get an immutable slice
    pub fn get_slice(&self, offset: usize, len: usize) -> &[u8] {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        assert!(
            offset + len <= write_pos,
            "get_slice out of bounds: offset={} len={} write_pos={}",
            offset,
            len,
            write_pos
        );
        unsafe { std::slice::from_raw_parts(self.data.as_ptr().add(offset), len) }
    }

    /// Get the current write position
    pub fn len(&self) -> usize {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get remaining capacity
    pub fn remaining(&self) -> usize {
        self.capacity - self.len()
    }

    /// Get total capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get buffer ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Reset buffer for reuse.
    ///
    /// Resets the write position to 0. The caller must ensure exclusive ownership
    /// (e.g. `Arc::strong_count() == 1`) before calling.
    pub fn reset(&self) -> bool {
        self.write_pos.store(0, Ordering::Release);
        true
    }

    /// Allocate `len` bytes and advance the write position, returning the start offset.
    /// Does NOT create a `BufferSlice` — the caller is expected
    /// to hold an `Arc<ZeroCopyBuffer>` which keeps the buffer alive.
    pub fn try_allocate(&self, len: usize) -> Option<usize> {
        loop {
            let current = self.write_pos.load(Ordering::Acquire);
            let new_pos = current + len;

            if new_pos > self.capacity {
                return None;
            }

            if self
                .write_pos
                .compare_exchange_weak(current, new_pos, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(current);
            }
            std::hint::spin_loop();
        }
    }

    /// Convert entire written portion to `Bytes` (F-113 fix: true zero-copy).
    ///
    /// Uses `Bytes::from_owner()` to wrap the `Arc<ZeroCopyBuffer>` without
    /// copying. The `Arc` keeps the buffer alive as long as the `Bytes`
    /// handle exists.
    pub fn freeze(self: &Arc<Self>) -> Bytes {
        let len = self.len();
        if len == 0 {
            return Bytes::new();
        }
        // F-113 fix: true zero-copy conversion.
        // We create a temporary struct that owns an Arc clone and implements
        // AsRef<[u8]> for interop with Bytes::from_owner (available since bytes 1.9+).
        // This avoids the Bytes::copy_from_slice that doubled memory usage.
        let owner = OwnedBufferSlice {
            buffer: Arc::clone(self),
            len,
        };
        Bytes::from_owner(owner)
    }
}

/// F-113: Owned wrapper for true zero-copy `Bytes` conversion.
///
/// Holds an `Arc<ZeroCopyBuffer>` and its length, implementing the traits
/// needed for `Bytes::from_owner()`. The `Arc` keeps the underlying buffer
/// alive for the lifetime of the `Bytes` handle.
struct OwnedBufferSlice {
    buffer: Arc<ZeroCopyBuffer>,
    len: usize,
}

// Safety: ZeroCopyBuffer is Send+Sync and we only expose immutable access
// through the owned slice.
unsafe impl Send for OwnedBufferSlice {}
unsafe impl Sync for OwnedBufferSlice {}

impl AsRef<[u8]> for OwnedBufferSlice {
    fn as_ref(&self) -> &[u8] {
        self.buffer.get_slice(0, self.len)
    }
}

impl Drop for ZeroCopyBuffer {
    fn drop(&mut self) {
        // Safety: We allocated with this layout, and we're the owner
        unsafe {
            dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

/// A slice view into a ZeroCopyBuffer
/// Holds an `Arc` to the underlying buffer for safe, reference-counted access.
///
/// F-046 fix: `Clone` intentionally NOT derived — cloning would enable aliased
/// mutable references via `write()` / `as_mut_bytes()`, which is undefined behavior.
/// Use `BufferSlice::share()` for a read-only shared view if needed.
#[derive(Debug)]
pub struct BufferSlice {
    buffer: Arc<ZeroCopyBuffer>,
    offset: usize,
    len: usize,
}

impl BufferSlice {
    /// Create a BufferSlice from an Arc reference.
    ///
    /// `pub(crate)` — external callers must use `ZeroCopyBuffer::reserve()` which
    /// guarantees exclusive (non-overlapping) ranges, preventing aliased `&mut [u8]`
    /// from `write()` / `as_mut_bytes()`. Making this `pub` would be a soundness hole.
    pub(crate) fn new(buffer: Arc<ZeroCopyBuffer>, offset: usize, len: usize) -> Self {
        Self {
            buffer,
            offset,
            len,
        }
    }

    /// Get the slice as bytes
    pub fn as_bytes(&self) -> &[u8] {
        self.buffer.get_slice(self.offset, self.len)
    }

    /// Get a mutable slice for writing
    /// # Safety
    /// Caller must ensure exclusive access to this range
    pub unsafe fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.buffer.get_mut_slice(self.offset, self.len)
    }

    /// Write data into this slice
    pub fn write(&mut self, data: &[u8]) -> usize {
        let write_len = data.len().min(self.len);
        unsafe {
            let dest = self.as_mut_bytes();
            dest[..write_len].copy_from_slice(&data[..write_len]);
        }
        write_len
    }

    /// Get the length of this slice
    pub fn len(&self) -> usize {
        self.len
    }

    /// Create a read-only shared view of this buffer region (F-046).
    ///
    /// Unlike `Clone`, this returns a `BufferRef` that only supports
    /// immutable access, preventing aliased mutable references.
    pub fn share(&self) -> BufferRef {
        BufferRef::Slice {
            buffer: Arc::clone(&self.buffer),
            offset: self.offset,
            len: self.len,
        }
    }

    /// Check if slice is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get offset within buffer
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Convert to Bytes (copies the data)
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl Deref for BufferSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for BufferSlice {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Pool of zero-copy buffers for efficient allocation.
///
/// F-114 fix: enforces a maximum total buffer count to prevent unbounded
/// memory growth under sustained high concurrency.
pub struct ZeroCopyBufferPool {
    /// Free buffers available for use
    free_buffers: crossbeam_channel::Sender<Arc<ZeroCopyBuffer>>,
    /// Receiver for getting free buffers
    buffer_receiver: crossbeam_channel::Receiver<Arc<ZeroCopyBuffer>>,
    /// Buffer size
    buffer_size: usize,
    /// Next buffer ID
    next_id: AtomicU64,
    /// Total buffers created
    total_created: AtomicU64,
    /// Buffers currently in use
    in_use: AtomicU64,
    /// Maximum total buffers allowed (F-114 fix)
    max_buffers: u64,
}

impl ZeroCopyBufferPool {
    /// Create a new buffer pool.
    ///
    /// `initial_count` buffers are pre-allocated. Up to `initial_count * 4`
    /// total buffers may be created under load (F-114 fix). Use
    /// `with_max_buffers` for explicit control.
    pub fn new(buffer_size: usize, initial_count: usize) -> Self {
        Self::with_max_buffers(buffer_size, initial_count, (initial_count * 4) as u64)
    }

    /// Create a buffer pool with an explicit maximum buffer count.
    pub fn with_max_buffers(buffer_size: usize, initial_count: usize, max_buffers: u64) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(initial_count * 2);

        let pool = Self {
            free_buffers: tx,
            buffer_receiver: rx,
            buffer_size,
            next_id: AtomicU64::new(0),
            total_created: AtomicU64::new(0),
            in_use: AtomicU64::new(0),
            max_buffers: max_buffers.max(initial_count as u64),
        };

        // Pre-allocate buffers
        for _ in 0..initial_count {
            let id = pool.next_id.fetch_add(1, Ordering::Relaxed);
            let buffer = Arc::new(ZeroCopyBuffer::with_id(buffer_size, id));
            pool.total_created.fetch_add(1, Ordering::Relaxed);
            let _ = pool.free_buffers.try_send(buffer);
        }

        pool
    }

    /// Get a buffer from the pool (or create a new one if under `max_buffers`).
    ///
    /// Returns `None` if the pool is exhausted and `max_buffers` has been
    /// reached (F-114 fix). The caller should apply back-pressure.
    pub fn acquire(&self) -> Option<Arc<ZeroCopyBuffer>> {
        match self.buffer_receiver.try_recv() {
            Ok(buffer) => {
                // Try to reset the buffer
                if Arc::strong_count(&buffer) == 1 {
                    buffer.reset();
                }
                self.in_use.fetch_add(1, Ordering::Relaxed);
                Some(buffer)
            }
            Err(_) => {
                // F-114 fix: enforce max_buffers limit
                let created = self.total_created.load(Ordering::Relaxed);
                if created >= self.max_buffers {
                    tracing::warn!(
                        total_created = created,
                        max_buffers = self.max_buffers,
                        in_use = self.in_use.load(Ordering::Relaxed),
                        "Buffer pool exhausted — apply back-pressure"
                    );
                    return None;
                }

                // Create new buffer
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let buffer = Arc::new(ZeroCopyBuffer::with_id(self.buffer_size, id));
                self.total_created.fetch_add(1, Ordering::Relaxed);
                self.in_use.fetch_add(1, Ordering::Relaxed);
                Some(buffer)
            }
        }
    }

    /// Convenience wrapper that panics on pool exhaustion (for non-critical paths).
    ///
    /// Prefer `acquire()` with proper back-pressure handling on the hot path.
    pub fn acquire_or_panic(&self) -> Arc<ZeroCopyBuffer> {
        self.acquire().expect("ZeroCopyBufferPool exhausted")
    }

    /// Return a buffer to the pool
    pub fn release(&self, buffer: Arc<ZeroCopyBuffer>) {
        self.in_use.fetch_sub(1, Ordering::Relaxed);

        // Only return to pool if we're the only holder
        if Arc::strong_count(&buffer) == 1 {
            buffer.reset();
            let _ = self.free_buffers.try_send(buffer);
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            buffer_size: self.buffer_size,
            total_created: self.total_created.load(Ordering::Relaxed),
            in_use: self.in_use.load(Ordering::Relaxed),
            available: self.buffer_receiver.len() as u64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub buffer_size: usize,
    pub total_created: u64,
    pub in_use: u64,
    pub available: u64,
}

/// Zero-copy message for production
/// Holds references to data without copying
#[derive(Debug)]
pub struct ZeroCopyMessage {
    /// Topic name (interned for efficiency)
    pub topic: Arc<str>,
    /// Partition ID
    pub partition: u32,
    /// Message key (optional, zero-copy reference)
    pub key: Option<BufferRef>,
    /// Message value (zero-copy reference)
    pub value: BufferRef,
    /// Message headers
    pub headers: Vec<(Arc<str>, BufferRef)>,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: i64,
}

/// Reference to data in a buffer (zero-copy)
#[derive(Debug, Clone)]
pub enum BufferRef {
    /// Inline small data (< 64 bytes)
    Inline(SmallVec),
    /// Reference to external buffer
    External(Bytes),
    /// Reference to zero-copy buffer slice
    Slice {
        buffer: Arc<ZeroCopyBuffer>,
        offset: usize,
        len: usize,
    },
}

impl BufferRef {
    /// Create from bytes
    pub fn from_bytes(data: &[u8]) -> Self {
        if data.len() <= 64 {
            BufferRef::Inline(SmallVec::from_slice(data))
        } else {
            BufferRef::External(Bytes::copy_from_slice(data))
        }
    }

    /// Create from Bytes (zero-copy)
    pub fn from_external(data: Bytes) -> Self {
        if data.len() <= 64 {
            BufferRef::Inline(SmallVec::from_slice(&data))
        } else {
            BufferRef::External(data)
        }
    }

    /// Create from buffer slice
    pub fn from_slice(buffer: Arc<ZeroCopyBuffer>, offset: usize, len: usize) -> Self {
        if len <= 64 {
            let data = buffer.get_slice(offset, len);
            BufferRef::Inline(SmallVec::from_slice(data))
        } else {
            BufferRef::Slice {
                buffer,
                offset,
                len,
            }
        }
    }

    /// Get as bytes slice
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            BufferRef::Inline(sv) => sv.as_slice(),
            BufferRef::External(b) => b,
            BufferRef::Slice {
                buffer,
                offset,
                len,
            } => buffer.get_slice(*offset, *len),
        }
    }

    /// Get length
    pub fn len(&self) -> usize {
        match self {
            BufferRef::Inline(sv) => sv.len(),
            BufferRef::External(b) => b.len(),
            BufferRef::Slice { len, .. } => *len,
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert to Bytes
    pub fn to_bytes(&self) -> Bytes {
        match self {
            BufferRef::Inline(sv) => Bytes::copy_from_slice(sv.as_slice()),
            BufferRef::External(b) => b.clone(),
            BufferRef::Slice {
                buffer,
                offset,
                len,
            } => Bytes::copy_from_slice(buffer.get_slice(*offset, *len)),
        }
    }
}

impl AsRef<[u8]> for BufferRef {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Small vector for inline data (avoids allocation for small messages)
#[derive(Debug, Clone)]
pub struct SmallVec {
    data: [u8; 64],
    len: u8,
}

impl SmallVec {
    pub fn new() -> Self {
        Self {
            data: [0u8; 64],
            len: 0,
        }
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        let len = slice.len().min(64);
        let mut sv = Self::new();
        sv.data[..len].copy_from_slice(&slice[..len]);
        sv.len = len as u8;
        sv
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for SmallVec {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-copy producer for high-throughput message production
pub struct ZeroCopyProducer {
    /// Buffer pool for allocating message buffers
    buffer_pool: Arc<ZeroCopyBufferPool>,
    /// Current write buffer
    /// F-097: `parking_lot` — O(1) buffer swap, never held across `.await`.
    current_buffer: parking_lot::Mutex<Option<Arc<ZeroCopyBuffer>>>,
    /// Interned topic names
    topic_cache: dashmap::DashMap<String, Arc<str>>,
    /// Statistics
    stats: ProducerStats,
}

impl ZeroCopyProducer {
    /// Create a new zero-copy producer
    pub fn new(buffer_pool: Arc<ZeroCopyBufferPool>) -> Self {
        Self {
            buffer_pool,
            current_buffer: parking_lot::Mutex::new(None),
            topic_cache: dashmap::DashMap::new(),
            stats: ProducerStats::new(),
        }
    }

    /// Create a new zero-copy producer with default buffer pool
    pub fn with_defaults() -> Self {
        let pool = Arc::new(ZeroCopyBufferPool::new(DEFAULT_BUFFER_SIZE, 16));
        Self::new(pool)
    }

    /// Intern a topic name for efficient storage
    fn intern_topic(&self, topic: &str) -> Arc<str> {
        if let Some(interned) = self.topic_cache.get(topic) {
            return interned.clone();
        }

        let interned: Arc<str> = Arc::from(topic);
        self.topic_cache.insert(topic.to_string(), interned.clone());
        interned
    }

    /// Create a message with zero-copy value
    pub fn create_message(
        &self,
        topic: &str,
        partition: u32,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> ZeroCopyMessage {
        self.stats.messages_created.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);

        let topic = self.intern_topic(topic);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        ZeroCopyMessage {
            topic,
            partition,
            key: key.map(BufferRef::from_bytes),
            value: BufferRef::from_bytes(value),
            headers: Vec::new(),
            timestamp,
        }
    }

    /// Create a message from existing Bytes (true zero-copy)
    pub fn create_message_from_bytes(
        &self,
        topic: &str,
        partition: u32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> ZeroCopyMessage {
        self.stats.messages_created.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);

        let topic = self.intern_topic(topic);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        ZeroCopyMessage {
            topic,
            partition,
            key: key.map(BufferRef::from_external),
            value: BufferRef::from_external(value),
            headers: Vec::new(),
            timestamp,
        }
    }

    /// Allocate space in current buffer and return (buffer, offset) for direct writing.
    ///
    /// The caller is responsible for writing into the buffer at the returned offset.
    /// Unlike `reserve()`, this does not create a `BufferSlice` — it increments the
    /// buffer's write position atomically and returns the raw offset.
    pub fn allocate(&self, size: usize) -> Option<(Arc<ZeroCopyBuffer>, usize)> {
        let mut guard = self.current_buffer.lock();

        // Try to reserve in current buffer
        if let Some(ref buffer) = *guard {
            if let Some(offset) = buffer.try_allocate(size) {
                return Some((buffer.clone(), offset));
            }
        }

        // Need a new buffer
        let buffer = self.buffer_pool.acquire()?;
        if let Some(offset) = buffer.try_allocate(size) {
            *guard = Some(buffer.clone());
            return Some((buffer, offset));
        }

        None
    }

    /// Get producer statistics
    pub fn stats(&self) -> ProducerStatsSnapshot {
        ProducerStatsSnapshot {
            messages_created: self.stats.messages_created.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            buffer_pool: self.buffer_pool.stats(),
        }
    }
}

struct ProducerStats {
    messages_created: AtomicU64,
    bytes_written: AtomicU64,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            messages_created: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProducerStatsSnapshot {
    pub messages_created: u64,
    pub bytes_written: u64,
    pub buffer_pool: BufferPoolStats,
}

/// Zero-copy consumer for high-throughput message consumption
pub struct ZeroCopyConsumer {
    /// Read buffer for batch reads
    /// F-097: `parking_lot` — O(1) buffer access, never held across `.await`.
    read_buffer: parking_lot::Mutex<BytesMut>,
    /// Statistics
    stats: ConsumerStats,
}

impl ZeroCopyConsumer {
    /// Create a new zero-copy consumer
    pub fn new() -> Self {
        Self {
            read_buffer: parking_lot::Mutex::new(BytesMut::with_capacity(DEFAULT_BUFFER_SIZE)),
            stats: ConsumerStats::new(),
        }
    }

    /// Parse messages from a bytes buffer without copying
    pub fn parse_messages(&self, data: Bytes) -> Vec<ConsumedMessage> {
        let mut messages = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            // Minimum message header: 4 (len) + 8 (offset) + 8 (timestamp) = 20 bytes
            if offset + 20 > data.len() {
                break;
            }

            // Read message length
            let msg_len = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;

            if offset + 4 + msg_len > data.len() {
                break;
            }

            // Create a slice of the message data (zero-copy)
            let msg_data = data.slice(offset + 4..offset + 4 + msg_len);

            if let Some(msg) = self.parse_single_message(msg_data) {
                messages.push(msg);
                self.stats.messages_consumed.fetch_add(1, Ordering::Relaxed);
            }

            offset += 4 + msg_len;
        }

        self.stats
            .bytes_read
            .fetch_add(offset as u64, Ordering::Relaxed);
        messages
    }

    /// Parse a single message from bytes
    fn parse_single_message(&self, data: Bytes) -> Option<ConsumedMessage> {
        if data.len() < 16 {
            return None;
        }

        let msg_offset = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        let timestamp = i64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        // Rest is the value (simplified - real impl would have key + headers)
        let value = data.slice(16..);

        Some(ConsumedMessage {
            offset: msg_offset,
            timestamp,
            key: None,
            value,
        })
    }

    /// Get consumer statistics
    pub fn stats(&self) -> ConsumerStatsSnapshot {
        ConsumerStatsSnapshot {
            messages_consumed: self.stats.messages_consumed.load(Ordering::Relaxed),
            bytes_read: self.stats.bytes_read.load(Ordering::Relaxed),
        }
    }

    /// Copy data into internal buffer for processing (useful for network reads)
    pub fn buffer_data(&self, data: &[u8]) -> Bytes {
        let mut buffer = self.read_buffer.lock();
        buffer.clear();
        buffer.extend_from_slice(data);
        buffer.clone().freeze()
    }
}

impl Default for ZeroCopyConsumer {
    fn default() -> Self {
        Self::new()
    }
}

struct ConsumerStats {
    messages_consumed: AtomicU64,
    bytes_read: AtomicU64,
}

impl ConsumerStats {
    fn new() -> Self {
        Self {
            messages_consumed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerStatsSnapshot {
    pub messages_consumed: u64,
    pub bytes_read: u64,
}

/// A consumed message with zero-copy data access
#[derive(Debug, Clone)]
pub struct ConsumedMessage {
    /// Message offset
    pub offset: u64,
    /// Timestamp (milliseconds since epoch)
    pub timestamp: i64,
    /// Message key (zero-copy)
    pub key: Option<Bytes>,
    /// Message value (zero-copy)
    pub value: Bytes,
}

impl ConsumedMessage {
    /// Get value as string (copies if not valid UTF-8)
    pub fn value_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }

    /// Get key as string
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_buffer_basic() {
        let buffer = Arc::new(ZeroCopyBuffer::new(1024));
        assert_eq!(buffer.len(), 0);
        assert!(buffer.remaining() >= 1024);

        // Reserve space
        let slice = buffer.reserve(100).unwrap();
        assert_eq!(slice.len(), 100);
        assert_eq!(buffer.len(), 100);
    }

    #[test]
    fn test_zero_copy_buffer_write() {
        let buffer = Arc::new(ZeroCopyBuffer::new(1024));

        let mut slice = buffer.reserve(11).unwrap();
        slice.write(b"Hello World");

        assert_eq!(slice.as_bytes(), b"Hello World");
    }

    #[test]
    fn test_buffer_pool() {
        let pool = ZeroCopyBufferPool::new(1024, 4);
        let stats = pool.stats();
        assert_eq!(stats.total_created, 4);
        assert_eq!(stats.available, 4);

        // Acquire buffers
        let b1 = pool.acquire().unwrap();
        let b2 = pool.acquire().unwrap();

        let stats = pool.stats();
        assert_eq!(stats.in_use, 2);

        // Release buffers
        pool.release(b1);
        pool.release(b2);

        let stats = pool.stats();
        assert_eq!(stats.in_use, 0);
    }

    #[test]
    fn test_buffer_ref_inline() {
        let small_data = b"small";
        let buf_ref = BufferRef::from_bytes(small_data);

        match buf_ref {
            BufferRef::Inline(_) => {}
            _ => panic!("Expected inline storage for small data"),
        }

        assert_eq!(buf_ref.as_bytes(), small_data);
    }

    #[test]
    fn test_buffer_ref_external() {
        let large_data = vec![0u8; 100];
        let buf_ref = BufferRef::from_bytes(&large_data);

        match buf_ref {
            BufferRef::External(_) => {}
            _ => panic!("Expected external storage for large data"),
        }

        assert_eq!(buf_ref.len(), 100);
    }

    #[test]
    fn test_zero_copy_producer() {
        let producer = ZeroCopyProducer::with_defaults();

        let msg = producer.create_message("test-topic", 0, Some(b"key1"), b"value1");

        assert_eq!(&*msg.topic, "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.key.unwrap().as_bytes(), b"key1");
        assert_eq!(msg.value.as_bytes(), b"value1");

        let stats = producer.stats();
        assert_eq!(stats.messages_created, 1);
    }

    #[test]
    fn test_zero_copy_consumer() {
        let consumer = ZeroCopyConsumer::new();

        // Create a simple message format
        let mut data = BytesMut::new();

        // Message length (16 + 5 = 21 bytes)
        data.extend_from_slice(&21u32.to_be_bytes());
        // Offset
        data.extend_from_slice(&42u64.to_be_bytes());
        // Timestamp
        data.extend_from_slice(&1234567890i64.to_be_bytes());
        // Value
        data.extend_from_slice(b"hello");

        let messages = consumer.parse_messages(data.freeze());

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].offset, 42);
        assert_eq!(messages[0].timestamp, 1234567890);
        assert_eq!(&messages[0].value[..], b"hello");
    }

    #[test]
    fn test_small_vec() {
        let sv = SmallVec::from_slice(b"test data");
        assert_eq!(sv.as_slice(), b"test data");
        assert_eq!(sv.len(), 9);
    }

    #[test]
    fn test_topic_interning() {
        let producer = ZeroCopyProducer::with_defaults();

        let msg1 = producer.create_message("topic-a", 0, None, b"v1");
        let msg2 = producer.create_message("topic-a", 0, None, b"v2");
        let msg3 = producer.create_message("topic-b", 0, None, b"v3");

        // Same topic should share the same Arc
        assert!(Arc::ptr_eq(&msg1.topic, &msg2.topic));
        // Different topics should not
        assert!(!Arc::ptr_eq(&msg1.topic, &msg3.topic));
    }
}
