# rivven-core

> High-performance storage engine for the Rivven event streaming platform.

## Overview

`rivven-core` is the foundational storage engine that powers Rivven's ultra-low-latency message persistence. It implements **hot path optimizations** including zero-copy I/O, portable async I/O (io_uring-style API), lock-free data structures, and cache-aligned memory layouts.

## Features

| Category | Features |
|:---------|:---------|
| **Hot Path** | Zero-copy buffers, cache-line alignment, lock-free queues |
| **Storage** | Log segments, tiered storage (hot/warm/cold), compaction |
| **I/O** | Portable async I/O (io_uring-style API, std::fs fallback), memory-mapped files |
| **Compression** | LZ4, Zstd, Snappy (streaming-optimized) |
| **Transactions** | Exactly-once semantics, 2PC protocol |
| **Batching** | Group commit WAL, vectorized encoding, fast checksums (delegates to crc32fast/memchr) |
| **Security** | TLS 1.3, Cedar authorization, indexed ACL lookups, AES-256-GCM / ChaCha20-Poly1305 encryption with key rotation, SCRAM-SHA-256 (600K PBKDF2) |

## Installation

```toml
[dependencies]
rivven-core = "0.2"

# Enable optional features
rivven-core = { version = "0.2", features = ["compression", "tls", "metrics"] }
```

### Feature Flags

| Feature | Description | Dependencies |
|:--------|:------------|:-------------|
| `compression` | LZ4, Zstd, Snappy codecs | lz4_flex, zstd, snap |
| `encryption` | AES-256-GCM / ChaCha20-Poly1305 at-rest encryption with key rotation | ring, rand |
| `tls` | TLS 1.3 transport security | rustls, webpki |
| `metrics` | Prometheus-compatible metrics | metrics, metrics-exporter-prometheus |
| `cedar` | Cedar policy-based authorization | cedar-policy |
| `oidc` | OpenID Connect authentication | openidconnect |
| `cloud-storage` | S3/GCS/Azure tiered storage | object_store |

## Architecture

```
rivven-core/
├── Hot Path (Ultra-Fast)
│   ├── zero_copy.rs      # Cache-aligned zero-copy buffers
│   ├── io_uring.rs       # Portable async I/O (io_uring-style API, std::fs fallback)
│   ├── concurrent.rs     # Lock-free MPMC queues, hashmaps
│   ├── buffer_pool.rs    # Slab-allocated buffer pooling
│   └── vectorized.rs     # Batch processing (delegates to crc32fast/memchr for SIMD)
│
├── Storage Engine
│   ├── storage/
│   │   ├── segment.rs    # Log segment files with indexes
│   │   ├── tiered.rs     # Hot/warm/cold tier management
│   │   ├── log_manager.rs # Segment lifecycle management
│   │   └── memory.rs     # In-memory hot tier cache
│   ├── wal.rs            # Group commit write-ahead log
│   └── compaction.rs     # Log compaction with tombstones
│
├── Transactions
│   └── transaction.rs    # Exactly-once semantics
│
├── Security
│   ├── auth.rs           # Authentication providers
│   ├── tls.rs            # TLS 1.3 configuration
│   └── encryption.rs     # At-rest encryption
│
└── Utilities
    ├── compression.rs    # Streaming codec implementations
    ├── bloom.rs          # Bloom filters for segment lookup
    └── metrics.rs        # Performance observability
```

## Tiered Storage

Rivven supports automatic data tiering across hot (memory), warm (local disk), and cold (object storage) tiers:

```rust
use rivven_core::{Config, storage::{TieredStorageConfig, ColdStorageConfig}};

// Enable tiered storage with high-performance preset
let config = Config::new()
    .with_tiered_storage(TieredStorageConfig::high_performance());

// Or use cost-optimized preset for archival workloads
let config = Config::new()
    .with_tiered_storage(TieredStorageConfig::cost_optimized());

// Custom configuration
let tiered_config = TieredStorageConfig {
    enabled: true,
    hot_tier_max_bytes: 8 * 1024 * 1024 * 1024, // 8 GB
    hot_tier_max_age_secs: 7200,                 // 2 hours
    warm_tier_path: "/var/lib/rivven/warm".to_string(),
    cold_storage: ColdStorageConfig::S3 {
        endpoint: None,
        bucket: "rivven-archive".to_string(),
        region: "us-east-1".to_string(),
        access_key: None,
        secret_key: None,
        use_path_style: false,
    },
    ..Default::default()
};

let config = Config::new().with_tiered_storage(tiered_config);
```

**Storage tiers:**
- **Hot**: In-memory for recent data and active consumers
- **Warm**: Local disk for medium-aged data
- **Cold**: S3/GCS/Azure for archival and compliance

## Hot Path Optimizations

### Zero-Copy Buffers

Cache-line aligned (64-byte) buffers eliminate unnecessary memory copies:

```rust
use rivven_core::zero_copy::{ZeroCopyBuffer, BufferSlice};

// Create a producer-side buffer
let mut buffer = ZeroCopyBuffer::new(64 * 1024); // 64 KB

// Write directly into buffer (no intermediate copies)
let slice = buffer.write_slice(1024);
slice.copy_from_slice(&data);

// Transfer ownership to consumer (zero-copy)
let consumer_view = buffer.freeze();
```

**Performance Impact:**
- **4x reduction** in memory bandwidth for large messages
- **Cache-friendly** access patterns with 64-byte alignment
- **Reference counting** for safe shared access

### Lock-Free Data Structures

High-performance concurrent primitives optimized for streaming:

```rust
use rivven_core::concurrent::{LockFreeQueue, ConcurrentHashMap, AppendOnlyLog};

// MPMC queue with backpressure
let queue = LockFreeQueue::bounded(10_000);
queue.push(message)?;  // Non-blocking
let msg = queue.pop()?;

// Lock-free hashmap with sharded locks
let map = ConcurrentHashMap::new();
map.insert("key".to_string(), value);
let val = map.get("key");

// Append-only log for sequential writes
let log = AppendOnlyLog::new(1_000_000);
log.append(entry);
```

| Data Structure | Operations | Contention Handling |
|:---------------|:-----------|:--------------------|
| `LockFreeQueue` | push/pop O(1) | Bounded backpressure |
| `ConcurrentHashMap` | get/insert O(1) | Sharded RwLocks |
| `AppendOnlyLog` | append O(1) | Single-writer optimized |
| `ConcurrentSkipList` | range O(log n) | Lock-free traversal |

### Buffer Pooling

Slab-allocated buffer pool with thread-local caching:

```rust
use rivven_core::buffer_pool::{BufferPool, BufferPoolConfig};

// High-throughput configuration
let pool = BufferPool::with_config(BufferPoolConfig::high_throughput());

// Acquire buffer from pool (fast path: thread-local cache)
let mut buffer = pool.acquire(4096);
buffer.extend_from_slice(&data);

// Return to pool automatically on drop
drop(buffer);

// Pool statistics
let stats = pool.stats();
println!("Hit rate: {:.1}%", stats.hit_rate() * 100.0);
```

**Size Classes:**

| Class | Size Range | Use Case |
|:------|:-----------|:---------|
| Small | 64-512 bytes | Headers, metadata |
| Medium | 512-4KB | Typical messages |
| Large | 4KB-64KB | Batched records |
| Huge | 64KB-1MB | Large payloads |

## Async I/O (io_uring-style API)

rivven-core provides a portable async I/O layer with an io_uring-style API. The current implementation uses `std::fs::File` behind `parking_lot::Mutex` as a portable fallback. The API is designed so a true io_uring backend can be swapped in on Linux 5.6+ without changing callers:

```rust
use rivven_core::io_uring::{IoUringConfig, WalWriter, SegmentReader, IoBatch, BatchExecutor};

// High-throughput WAL writer
let config = IoUringConfig::high_throughput();
let wal = WalWriter::new("/data/wal.log", config)?;

// Direct write (immediate)
let offset = wal.append(b"record data")?;

// Batched writes (queued until flush)
wal.append_batched(b"record1")?;
wal.append_batched(b"record2")?;
wal.flush_batch()?; // Execute all batched writes

// Append with checksum
let offset = wal.append_with_checksum(b"record data")?;
wal.sync()?;

// Read segments
let reader = SegmentReader::open("/data/segment.log", IoUringConfig::default())?;
let messages = reader.read_messages(0, 64 * 1024)?;
```

### Batch Operations

Batched I/O reduces syscall overhead by queueing multiple operations:

```rust
use rivven_core::io_uring::{IoBatch, BatchExecutor, AsyncWriter, IoUringConfig, BatchStats};

// Create a batch of operations
let mut batch = IoBatch::new();
batch.write(0, b"hello".to_vec());
batch.write(5, b"world".to_vec());
batch.read(100, 50);
batch.sync();

// Get batch statistics before execution
let stats: BatchStats = batch.stats();
println!("Batch: {} writes ({} bytes), {} reads ({} bytes), {} syncs",
    stats.write_ops, stats.write_bytes,
    stats.read_ops, stats.read_bytes,
    stats.sync_ops);

// Execute batch
let writer = AsyncWriter::new("/data/file.log", IoUringConfig::default())?;
let executor = BatchExecutor::for_writer(writer);
executor.execute(&mut batch)?;
```

#### Batch Statistics

The `BatchStats` struct provides insight into batch composition:

| Field | Type | Description |
|-------|------|-------------|
| `total_ops` | u64 | Total operations in batch |
| `write_ops` | u64 | Number of write operations |
| `read_ops` | u64 | Number of read operations |
| `sync_ops` | u64 | Number of sync operations |
| `write_bytes` | u64 | Total bytes to be written |
| `read_bytes` | u64 | Total bytes to be read |

## Transactions

Native exactly-once semantics with two-phase commit:

```rust
use rivven_core::transaction::{TransactionCoordinator, TransactionConfig};

// Create coordinator
let coordinator = TransactionCoordinator::new(TransactionConfig::default());

// Begin transaction
let txn = coordinator.begin_transaction("txn-001".to_string())?;

// Add writes to transaction
txn.add_write("topic-a", 0, message1)?;
txn.add_write("topic-b", 1, message2)?;

// Commit atomically (all-or-nothing)
coordinator.commit(&txn).await?;

// Or abort on failure
// coordinator.abort(&txn).await?;
```

**Transaction Guarantees:**

| Property | Implementation |
|:---------|:---------------|
| Atomicity | Two-phase commit with coordinator |
| Isolation | Epoch-based producer fencing |
| Durability | WAL persistence before commit |
| Exactly-Once | Idempotent sequence numbers |

## Vectorized Batch Processing

Batch processing operations for high-throughput workloads. CRC32 and memory search delegate to `crc32fast` and `memchr` crates respectively, which use SIMD internally when available:

```rust
use rivven_core::vectorized::{BatchEncoder, BatchDecoder, crc32_fast, RecordBatch};

// Batch encoding (2-4x faster than sequential)
let mut encoder = BatchEncoder::with_capacity(64 * 1024);
for msg in messages {
    encoder.add_message(msg.key.as_deref(), &msg.value, msg.timestamp);
}
let encoded = encoder.finish();

// Batch decoding
let decoder = BatchDecoder::new();
let messages = decoder.decode_all(&encoded);

// Fast CRC32 (delegates to crc32fast, which uses SSE4.2/AVX2 when available)
let checksum = crc32_fast(&data);

// Columnar record batch for analytics
let mut batch = RecordBatch::new();
batch.add(timestamp, Some(b"key"), b"value");
let filtered = batch.filter(|ts, _, _| ts > cutoff);
```

**Vectorization Benefits:**

| Operation | Speedup | Acceleration |
|:----------|:--------|:-------------|
| CRC32 | 4-8x | crc32fast (SSE4.2/AVX2/ARM CRC32 when available) |
| Batch encode | 2-4x | Cache-optimized sequential processing |
| Memory search | 3-5x | memchr crate (AVX2/SSE2/NEON when available) |

## Group Commit WAL

Write-ahead log with group commit optimization (10-100x throughput improvement):

```rust
use rivven_core::wal::{GroupCommitWal, WalConfig, SyncMode};

// Configure for maximum throughput
let config = WalConfig {
    group_commit_window: Duration::from_micros(200), // Batch window
    max_batch_size: 4 * 1024 * 1024,                 // 4 MB batches
    max_pending_writes: 1000,                        // Trigger flush
    sync_mode: SyncMode::Fsync,                      // Durability
    ..Default::default()
};

let wal = GroupCommitWal::new(config)?;

// Writes are batched and flushed together
let (offset, committed) = wal.append(record)?;
committed.await?;  // Wait for fsync
```

**Group Commit Performance:**

| Batch Size | fsync/sec | Throughput |
|:-----------|:----------|:-----------|
| 1 (no batching) | 10,000 | 10K msg/sec |
| 100 | 100 | 1M msg/sec |
| 1000 | 10 | 10M msg/sec |

## Core Types

```rust
use rivven_core::{
    Record, Topic, Partition, Offset,
    ProducerConfig, ConsumerConfig,
    CompressionCodec,
};

// Create a record
let record = Record::builder()
    .key(b"user-123")
    .value(b"event data")
    .header("source", "api")
    .build();
```

### Partition Append Optimization

When tiered storage is enabled, the partition pre-serializes the message **once** before consuming it into the segment log. This eliminates both the `message.clone()` and double-serialization that would otherwise occur:

1. Single `message.to_bytes()` for the tiered-storage copy
2. Owned `message` moved into `log.append()` (zero-copy handoff)
3. `LogManager::truncate_before()` physically deletes segments below the low-watermark (used by `DeleteRecords`)

## Storage Engine

The storage engine uses a log-structured design:

```
data/
└── topics/
    └── orders/
        ├── partition-0/
        │   ├── 00000000000000000000.log  # Segment file
        │   ├── 00000000000000000000.idx  # Offset index
        │   └── 00000000000000001000.log  # Next segment
        └── partition-1/
```

## Test Coverage

```bash
# Run all tests
cargo test -p rivven-core --lib

# Run with feature flags
cargo test -p rivven-core --lib --features "compression,tls,metrics"
```

**Current Coverage:** 282 tests (100% passing)

| Category | Tests | Description |
|:---------|:------|:------------|
| Zero-copy | 12 | Buffer allocation, slicing, freeze |
| Concurrent | 18 | Lock-free queue, hashmap, skiplist |
| Storage | 45 | Segments, indexes, tiered storage |
| WAL | 22 | Group commit, recovery, checksums |
| Transactions | 28 | 2PC, abort, idempotence |
| Vectorized | 15 | Batch encoding, CRC32, SIMD |
| TLS | 34 | Certificate validation, handshake |
| Auth | 25 | RBAC, Cedar policies, indexed ACL lookups |
| Compression | 18 | LZ4, Zstd, Snappy codecs |

## Documentation

- [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)
- [Tiered Storage](https://rivven.hupe1980.github.io/rivven/docs/tiered-storage)
- [Performance Optimization](https://rivven.hupe1980.github.io/rivven/docs/performance)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
