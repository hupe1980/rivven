---
layout: default
title: Architecture
nav_order: 3
---

# Architecture
{: .no_toc }

Understanding Rivven's design and components.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Design Principles

### Lightweight, Focused Binaries

Rivven provides **four focused binaries**, each with a clear responsibility:

| Binary | Purpose |
|:-------|:--------|
| `rivvend` | Message broker with storage, replication, and auth |
| `rivven` | Command-line interface for topic and cluster management |
| `rivven-schema` | Schema registry with Avro, Protobuf, JSON Schema |
| `rivven-connect` | Connector runtime for CDC and data pipelines |

All binaries share these characteristics:
- No JVM or runtime dependencies
- No ZooKeeper or external coordinators
- Sub-second startup time
- Native Rust performance

### Security Through Separation

This multi-binary architecture provides **security isolation**—database credentials stay in the connector process, never reaching the broker. Schema registry credentials are similarly isolated.

### Rivven Connect Features

- **Bootstrap server failover**: Connect to multiple brokers with automatic failover
- **Auto-create topics**: Topics created automatically with configurable defaults
- **Per-source configuration**: Override partitions and settings per connector
- **Connection resilience**: Exponential backoff, automatic reconnection

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      RIVVEN ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐       ┌────────────────────────────────┐ │
│  │     rivvend      │       │       rivven-connect           │ │
│  │    (broker)      │◄─────►│  (SDK + CLI)                   │ │
│  │                  │native │                                 │ │
│  │  • Storage       │proto  │  SDK:   Source, Sink, Transform │ │
│  │  • Replication   │       │  CLI:   Config-driven pipelines │ │
│  │  • Consumer Grps │       │  Sources: postgres-cdc, mysql   │ │
│  │  • Auth/RBAC     │       │  Sinks:   stdout, s3, http      │ │
│  └──────────────────┘       └────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

| Crate | Purpose | Description |
|:------|:--------|:------------|
| `rivven-core` | Storage engine | Topics, partitions, consumer groups, compression |
| `rivvend` | Broker binary | TCP server, protocol handling, auth |
| `rivven-client` | Rust client | Production client with pooling, batching, retries |
| `rivven-protocol` | Wire protocol | postcard + protobuf serialization |
| `rivven` | CLI | Command-line interface |
| `rivven-cluster` | Distributed | Raft consensus, SWIM gossip, partitioning |
| `rivven-cdc` | CDC library | PostgreSQL and MySQL replication primitives |
| `rivven-connect` | Connector SDK & Runtime | Built-in connectors + SDK traits |
| `rivven-schema` | Schema Registry | High-performance schema management |
| `rivven-operator` | Kubernetes | CRDs and controller |
| `rivven-python` | Python bindings | PyO3-based Python SDK |

---

## Connector Architecture

Rivven organizes connectors **within rivven-connect** using feature flags to isolate heavy dependencies.
This follows a modular architecture where each connector category can be enabled independently.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    rivven-connect (SDK + Runtime)               │
│  SourceFactory, SinkFactory, AnySource, AnySink, Registry       │
├─────────────────────────────────────────────────────────────────┤
│                    Built-in Connectors                          │
│  ├── Database CDC: postgres-cdc, mysql-cdc                      │
│  ├── Messaging: mqtt, sqs, pubsub                               │
│  ├── Storage: s3, gcs, azure-blob                               │
│  ├── Warehouse: snowflake, bigquery, redshift                   │
│  └── Utility: datagen, stdout, http-webhook                     │
├─────────────────────────────────────────────────────────────────┤
│                    External Crates                              │
│  └── rivven-cdc: Optional reusable CDC primitives               │
└─────────────────────────────────────────────────────────────────┘
```

### Connector Feature Flags

| Feature | Domain | Connectors |
|:--------|:-------|:-----------|
| `postgres` | Database CDC | postgres-cdc |
| `mysql` | Database CDC | mysql-cdc |
| `mqtt` | Messaging | mqtt |
| `sqs` | Messaging | AWS SQS |
| `pubsub` | Messaging | Google Pub/Sub |
| `s3` | Object Storage | Amazon S3, MinIO, R2 |
| `gcs` | Object Storage | Google Cloud Storage |
| `azure` | Object Storage | Azure Blob Storage |
| `snowflake` | Data Warehouse | Snowflake |
| `bigquery` | Data Warehouse | Google BigQuery |
| `redshift` | Data Warehouse | Amazon Redshift |
| `parquet` | File Format | Parquet output format |

### Benefits of Feature Flags

1. **Dependency Isolation** - Heavy dependencies (cloud SDKs) only included when needed
2. **Build Time Reduction** - Compile only the connectors you use
3. **Single Binary Option** - Enable `full` feature for all connectors
4. **Minimal Core** - Default build includes only datagen and stdout

### Usage Example

```toml
# Only include what you need
[dependencies]
rivven-connect = { version = "0.0.18", features = ["postgres", "s3"] }

# Or include everything
[dependencies]
rivven-connect = { version = "0.0.18", features = ["full"] }
```

### Bundle Features

| Bundle | Includes |
|:-------|:---------|
| `queue-full` | mqtt, sqs, pubsub |
| `storage-full` | s3, gcs, azure, parquet |
| `warehouse-full` | snowflake, bigquery, redshift |
| `full` | All connectors |

### Creating Custom Registries

```rust
use rivven_connect::connectors::{create_source_registry, create_sink_registry};

// Get pre-configured registries with all enabled connectors
let sources = create_source_registry();
let sinks = create_sink_registry();

// Or use the connector inventory for rich metadata
use rivven_connect::connectors::create_connector_inventory;
let inventory = create_connector_inventory();
```

---

## Storage Engine

### Log-Structured Storage

Rivven uses a **log-structured storage engine** optimized for sequential writes:

```
data/
└── topics/
    └── orders/
        ├── partition-0/
        │   ├── 00000000000000000000.log  # Segment file
        │   ├── 00000000000000000000.idx  # Offset index
        │   └── 00000000000000001000.log  # Next segment
        └── partition-1/
            └── ...
```

### Durability Guarantees

Rivven ensures **full durability** across broker restarts through multiple mechanisms:

#### Segment Sync Policy

Every segment write is governed by the configurable `sync_policy`:

| Policy | Behavior | Trade-off |
|:-------|:---------|:----------|
| `EveryWrite` | `fdatasync` after every append | Maximum durability, lowest throughput |
| `EveryNWrites(n)` | `fdatasync` every N appends | Balanced (default: n=1) |
| `None` | OS page cache only | Fastest, data loss on crash |

Configure via broker config:

```yaml
# rivven.yaml
server:
  sync_policy: EveryNWrites(1)  # default — every write fsynced
```

The sync policy propagates through the full write path:
`Config` → `LogManager` → `Segment` → index files. When `EveryWrite` or
`EveryNWrites` triggers, both the log file and pending index entries are
fsynced atomically.

#### Deferred Fsync Architecture

To maximize concurrent throughput, the write path uses a **deferred fsync** design:

1. **Under write lock** (~µs): `BufWriter::flush()` pushes data to OS page cache, index entries are synced, a `pending_sync` flag is set atomically. Data is immediately visible to readers via mmap.
2. **After lock release** (~1–10ms): `fdatasync` runs on a dup'd file descriptor (`File::try_clone()`) via `tokio::task::spawn_blocking`, ensuring durability without holding the partition-wide `RwLock`.

This means readers are never blocked by fsync latency — they see new data as soon as it reaches the page cache. Durability follows asynchronously. The dup'd fd ensures fsync covers all data written before lock release, and `fdatasync` is idempotent so concurrent syncs are safe.

#### WAL Recovery with CRC Validation

The Write-Ahead Log validates CRC32 checksums consistently across all scan paths:

- **`scan_wal_file()`** (recovery): Validates CRC for every record; stops at first corruption
- **`find_actual_end()`** (WAL open): Validates CRC to find the true end of valid data, preventing new writes from landing after corrupted records
- **`timestamp_bounds()`** (segment scan): Skips CRC-invalid records to prevent corrupted timestamps from skewing min/max bounds
- **`find_offset_for_timestamp()`** (consumer seek): Validates CRC per record during timestamp-based lookups

WAL file pre-allocation uses `tokio::task::spawn_blocking` to avoid creating unbounded OS threads during rapid rotation.

#### Topic Metadata Persistence

Topic configuration and metadata are persisted to `topic_metadata.json`:

```json
[
  {
    "name": "orders",
    "num_partitions": 8,
    "created_at": 1706745600
  },
  {
    "name": "events",
    "num_partitions": 4,
    "created_at": 1706745900
  }
]
```

This ensures:
- **Topics survive restarts** - All topic definitions are restored automatically
- **Partition counts preserved** - No need to recreate topics after restart
- **Atomic updates** - Metadata file is updated transactionally on create/delete

#### Recovery Process

On startup, the broker performs recovery in this order:

1. **Primary Recovery** - Load `topic_metadata.json` if present
2. **Fallback Recovery** - Scan data directory for `partition-*` subdirs
3. **Segment Recovery** - Validate and load existing segment files
4. **Offset Recovery** - Restore consumer group offsets from storage

```
┌───────────────┐     ┌──────────────┐     ┌─────────────┐
│ Load Metadata │────>│ Scan Partitions │──>│ Validate    │
│ JSON          │     │ (fallback)       │  │ Segments    │
└───────────────┘     └──────────────────┘  └─────────────┘
                                                   │
                                                   ▼
                              ┌─────────────────────────────┐
                              │ Broker Ready (all data      │
                              │ restored, no data loss)     │
                              └─────────────────────────────┘
```

#### Data Integrity

| Component | Durability Mechanism | Recovery Method |
|:----------|:--------------------|:----------------|
| Topic Metadata | JSON file | Load on startup |
| Messages | Segment files (.log) | Memory-map existing |
| Offsets | Index files (.idx) | Rebuild from segments |
| Consumer Groups | Offset storage | Replay from log |

### Segment Files

- **Append-only** log files
- Configurable segment size (default: 1 GB)
- Memory-mapped for efficient reads
- CRC32 checksums for integrity

### Index Files

- Sparse offset index
- Maps offset → file position
- Enables O(log n) seeks

### Compression

| Algorithm | Ratio | Speed | Use Case |
|:----------|:------|:------|:---------|
| LZ4 | ~2x | Very fast | Real-time streaming |
| Zstd | ~4x | Fast | Batch/archival |
| None | 1x | N/A | Already compressed data |

---

## Performance Optimizations

### rivven-core Hot Path Architecture

The core storage engine implements hot path optimizations:

#### Zero-Copy Buffers

Cache-line aligned (64-byte) buffers minimize memory bandwidth:

```rust
use rivven_core::zero_copy::{ZeroCopyBuffer, BufferSlice};

// Direct writes without intermediate copies
let mut buffer = ZeroCopyBuffer::new(64 * 1024);
let slice = buffer.write_slice(data.len());
slice.copy_from_slice(&data);
let frozen = buffer.freeze();  // True zero-copy via Bytes::from_owner()
```

The `freeze()` method uses `Bytes::from_owner()` to wrap the buffer in an `Arc`-backed `Bytes` — no `memcpy` occurs. The `ZeroCopyBufferPool` enforces a `max_buffers` limit (default: `initial_count * 4`) to prevent unbounded memory growth under backpressure.

#### Lock-Free Data Structures

Optimized for streaming workloads:

| Structure | Use Case | Performance |
|:----------|:---------|:------------|
| `LockFreeQueue` | MPMC message passing | O(1) push/pop |
| `ConcurrentHashMap` | Partition lookup | Sharded RwLocks |
| `AppendOnlyLog` | Sequential writes | Single-writer |
| `ConcurrentSkipList` | Range queries | Lock-free traversal |
| `TokenBucketRateLimiter` | Connector throughput control | Fully lock-free (AtomicU64 CAS for refill + acquire) |

The `LogSegment` backing `AppendOnlyLog` uses raw pointer access (no `UnsafeCell<Vec>`) to avoid Rust aliasing violations during concurrent CAS-reserved writes and committed-range reads. The buffer is pre-allocated at segment creation and never resized.

#### Partition Append Optimization

The partition append path avoids unnecessary copies:

- **Zero-alloc serialization**: `Segment::append()` uses `postcard::to_extend` with an 8-byte header placeholder, then patches CRC + length in-place — **1 allocation, 0 copies** per message. Batch append reuses a single serialization buffer across all messages.
- **No message clone**: `LogManager::append_batch()` uses `split_off()` ownership transfer to partition batches across segments without cloning `Message` structs (avoids header `String`/`Vec<u8>` allocations). When tiered storage is enabled, the message is serialized once before being consumed by the log manager.
- **Lock-free offset allocation**: Next offset is allocated via `AtomicU64::fetch_add` (AcqRel ordering).
- **Single-pass consume**: The consume handler combines isolation-level filtering and protocol conversion into a single iterator pass, avoiding intermediate `Vec` allocations.

#### Buffer Pooling

Slab allocation with thread-local caching:

- **Size Classes**: Small (64-512B), Medium (512-4KB), Large (4-64KB), Huge (64KB-1MB)
- **Thread-Local Cache**: Fast path avoids global lock
- **Pool Statistics**: Hit rate monitoring

#### Vectorized Batch Processing

Accelerated batch operations (delegates to `crc32fast`/`memchr` for SIMD when available):

- **CRC32**: Delegates to crc32fast (uses SSE4.2/AVX2 when available)
- **Batch Encoding**: 2-4x faster than sequential (cache-optimized)
- **Memory Search**: Delegates to memchr crate (uses SIMD when available)

#### Group Commit WAL

Write batching for 10-100x throughput improvement:

- **Batch Window**: Configurable commit interval (default: 200μs)
- **Batch Size**: Trigger flush at threshold (default: 4 MB)
- **Pending Writes**: Flush after N writes (default: 1000)
- **Zero-alloc serialization**: `WalRecord::write_to_buf()` serializes header + CRC + data directly into the shared batch buffer — no per-record `BytesMut` intermediate allocation
- **Buffer shrink**: After burst traffic, batch buffer re-allocates to default capacity when oversized (>2x max)
- **CRC-validated recovery**: Both `find_actual_end()` and `scan_wal_file()` validate CRC32 for every record

#### Segment Read Path

The read path minimizes lock contention:

- **Dirty flag**: An atomic `write_dirty` flag tracks whether buffered data exists. Reads check the flag first and skip the write-mutex acquisition when no data is pending — eliminates head-of-line blocking behind concurrent appends
- **Cached mmap**: Read-only memory maps are cached and only invalidated on write, avoiding per-read `mmap()` syscalls
- **Sparse index**: Binary search over 4KB-interval index entries for O(log n) position lookup
- **CRC validation**: Every read validates CRC32 before deserialization
- **Varint offset extraction**: Messages below the target offset are skipped using `postcard::take_from_bytes::<u64>()` — extracting only the 1–10 byte varint-encoded offset without allocating key, value, or headers. Full deserialization is deferred until a matching message is found

### Async I/O (Portable, io_uring-style API)

Rivven provides a portable async I/O layer with an io_uring-style API. The current implementation uses `std::fs::File` behind `parking_lot::Mutex` as a portable fallback; the API is designed for a future true io_uring backend on Linux 5.6+:

#### Synchronization Contract

`parking_lot::{Mutex, RwLock}` is used for **O(1) critical sections** (HashMap lookups, counter increments, buffer swaps) where the future-boxing overhead of `tokio::sync` is unnecessary. These locks are **never** held across `.await` boundaries. Where a lock must span async I/O (e.g., `Segment::log_file`), `tokio::sync::Mutex` is used instead.

- **Submission Queue** - Batch I/O operations
- **Completion Queue** - Poll results
- **Registered Buffers** - Pre-registered memory for zero-copy

```
┌────────────┐     ┌─────────────────┐     ┌────────────┐
│ Application│     │ Async I/O Layer │     │   Kernel   │
│   Thread   │────>│  (io_uring API) │────>│  I/O Path  │
│            │<────│ (std::fs impl)  │<────│            │
└────────────┘     └─────────────────┘     └────────────┘
```

### Request Pipelining (Client)

The Rivven client supports **HTTP/2-style request pipelining**:

- Multiple in-flight requests over single TCP connection
- Automatic batching with configurable flush intervals
- Backpressure via semaphores to prevent memory exhaustion

```rust
use rivven_client::{PipelinedClient, PipelineConfig};

let client = PipelinedClient::connect(addr, PipelineConfig::high_throughput()).await?;

// Send requests without waiting for responses
let f1 = client.send(Request::Publish { /* ... */ });
let f2 = client.send(Request::Publish { /* ... */ });
let f3 = client.send(Request::Consume { /* ... */ });

// Await responses concurrently
let (r1, r2, r3) = tokio::join!(f1, f2, f3);
```

### Zero-Copy Transfers

Rivven minimizes data copying throughout the stack:

- **Memory-mapped segments** - Direct disk-to-network transfers
- **Buffer pooling** - Reusable byte buffers reduce allocations
- **Vectored I/O** - Scatter-gather for multi-buffer operations

---

## Distributed Coordination

### Raft Consensus

For clusters, Rivven uses **Raft** (via openraft) for:

- Leader election (deterministic: lowest node ID wins ties)
- Log replication
- Membership changes
- **Authenticated API**: All Raft management endpoints require authentication middleware

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │◄───►│ Node 2  │◄───►│ Node 3  │
│ (Leader)│     │(Follower)│    │(Follower)│
└─────────┘     └─────────┘     └─────────┘
     │
     ▼
  Clients
```

### SWIM Gossip

For membership and failure detection:

- Epidemic protocol for state propagation
- Suspicion mechanism for false-positive reduction
- Efficient O(N) protocol-period dissemination
- **HMAC-authenticated protocol messages** prevent cluster poisoning

### ISR Replication

Kafka-style In-Sync Replica management:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Leader    │────►│  Follower 1 │     │  Follower 2 │
│  (Node 1)   │     │  (Node 2)   │     │  (Node 3)   │
│             │     │             │     │             │
│ LEO: 1000   │     │ LEO: 998    │     │ LEO: 995    │
│ HWM: 995    │     │ In ISR ✓   │     │ In ISR ✓   │
└─────────────┘     └─────────────┘     └─────────────┘
```

**ISR Membership Rules:**
- Follower must fetch within `replica_lag_max_time` (default: 10s)
- Follower must be within `replica_lag_max_messages` (default: 1000)
- High watermark advances when all ISR members acknowledge

**Automatic Partition Reassignment:**
When a broker fails (detected via SWIM gossip `NodeFailed` event), the cluster coordinator automatically reassigns affected partitions:
1. The Raft leader queries all partitions led by the failed node
2. For each partition, the dead node is removed from ISR candidates
3. A new leader is elected from remaining replicas (deterministic sort)
4. The new assignment is proposed via Raft with an incremented epoch

This ensures partitions regain leadership within seconds of broker failure detection.

### Ack Modes

| Mode | Guarantee | Latency |
|:-----|:----------|:--------|
| `acks=0` | None (fire & forget) | Lowest |
| `acks=1` | Leader durability | Low |
| `acks=all` | ISR durability | Higher |

### Partition Placement

Consistent hashing with rack awareness for fault tolerance:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Hash Ring (150 vnodes/node)                 │
├─────────────────────────────────────────────────────────────────┤
│  [Node1#0]──[Node2#0]──[Node3#0]──[Node1#1]──[Node2#1]──...    │
│       │           │           │                                 │
│   topic-0/0   topic-0/1   topic-0/2                             │
└─────────────────────────────────────────────────────────────────┘

Rack-Aware Placement:
├── Rack A: Node 1, Node 2
├── Rack B: Node 3, Node 4
└── Replicas spread across racks for fault tolerance
```

---

## Consumer Groups

### Offset Management

Consumer groups track offsets per topic-partition:

```
┌─────────────────────────────────────┐
│         Consumer Group: app-1       │
├─────────────────────────────────────┤
│ orders/0 → offset: 1234             │
│ orders/1 → offset: 5678             │
│ orders/2 → offset: 9012             │
└─────────────────────────────────────┘
```

### Commit Strategies

| Strategy | Guarantee | Performance |
|:---------|:----------|:------------|
| Auto-commit | At-least-once | High |
| Manual commit | Exactly-once (with idempotency) | Medium |
| Sync commit | Strong consistency | Lower |

---

## Exactly-Once Semantics

Rivven provides exactly-once semantics through two complementary features:

### Idempotent Producer

Eliminates duplicates during retries without full transactions:

```
Producer                           Broker
   │─── InitProducerId ──────────────>│
   │<── PID=123, Epoch=0 ─────────────│
   │─── Produce(PID,Seq=0) ──────────>│  First message
   │<── Success(offset=0) ────────────│
   │─── Produce(PID,Seq=0) ──────────>│  Retry (duplicate!)
   │<── DuplicateSequence(offset=0) ──│  Returns cached offset
```

Key concepts:
- **Producer ID (PID)**: Unique 64-bit identifier per producer
- **Epoch**: Fences old producer instances on restart
- **Sequence**: Per-partition counter for deduplication

### Native Transactions

Atomic writes across multiple topics with two-phase commit:

```
Producer                    Transaction Coordinator
   │─── BeginTransaction ─────────────>│
   │<── OK ────────────────────────────│
   │─── AddPartitionsToTxn ───────────>│
   │<── OK ────────────────────────────│
   │─── TransactionalPublish(topic1) ─>│
   │─── TransactionalPublish(topic2) ─>│
   │─── CommitTransaction ────────────>│
   │<── OK (all-or-nothing commit) ────│
```

For detailed usage, see the [Exactly-Once Guide](exactly-once).

---

## Message Partitioning

### Sticky Partitioner

Rivven implements **sticky partitioning** for optimal throughput and distribution:

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition |
| Without key | Sticky batching | Batches to one partition, rotates periodically |
| Explicit partition | Direct | Uses specified partition |

### Why Sticky Partitioning?

Unlike simple round-robin, sticky partitioning provides:

- **Better batching efficiency** - More messages per network call
- **Lower latency** - Fewer partition switches during bursts  
- **Good distribution over time** - Rotation ensures balance

### Partition Rotation Triggers

Messages without keys stick to one partition until:

1. **Batch size threshold** reached (default: 16K messages)
2. **Time threshold** elapsed (default: 100ms)

### Key Hashing (Murmur2)

Keyed messages use **Murmur2** hash for consistent routing:

```rust
// Same key always routes to same partition
partition = murmur2_hash(key) % num_partitions
```

This ensures:
- Ordering guarantee for messages with same key
- Deterministic routing across restarts

---

## Protocol

### Wire Format

Rivven uses a **format-prefixed** binary protocol with correlation IDs for request-response matching:

```
┌────────────┬─────────────┬──────────────────┬──────────────────────────┐
│ Length (4B)│ Format (1B) │ Correlation (4B) │ Serialized payload       │
│ Big-endian │ 0x00=post   │ Big-endian u32   │ Request or Response      │
│ u32        │ 0x01=proto  │                  │                         │
└────────────┴─────────────┴──────────────────┴──────────────────────────┘
```

- **Length**: 4-byte big-endian unsigned integer (includes format + correlation ID + payload)
- **Format**: 1-byte wire format identifier
  - `0x00`: postcard (Rust-native, ~50ns serialize)
  - `0x01`: protobuf (cross-language compatible)
- **Correlation ID**: 4-byte big-endian u32 matching responses to requests (enables pipelining)
- **Payload**: Serialized Request or Response

### Connection Handshake

Clients should send a `Handshake` request as the first message after connecting:

```
Client → Server: Handshake { protocol_version: 2, client_id: "my-app" }
Server → Client: HandshakeResult { server_version: 2, compatible: true, message: "..." }
```

The server validates the client's protocol version and returns compatibility information.
Incompatible versions receive `compatible: false` with a descriptive error message.

### Format Auto-Detection

The server **auto-detects** the wire format from the first byte and responds in the same format:

```rust
// Rust client (postcard - fastest)
let wire_bytes = request.to_wire(WireFormat::Postcard, correlation_id)?;
let (response, format, corr_id) = Response::from_wire(&response_bytes)?;

// Non-Rust clients use format byte 0x01 for protobuf
```

### Cross-Language Support

| Format | Feature | Languages | Use Case |
|:-------|:--------|:----------|:---------|
| **postcard** | default | Rust | Maximum performance |
| **protobuf** | `protobuf` | Go, Java, Python, C++ | Cross-language |

| Language | Status | Approach |
|:---------|:-------|:---------|
| **Rust** | ✅ Native | `rivven-client` crate |
| **Python** | ✅ Bindings | `rivven-python` (PyO3) |
| **Go** | 🔜 Planned | Use proto file in `rivven-protocol` |
| **Java** | 🔜 Planned | Use proto file in `rivven-protocol` |

**Protobuf Usage:**
```bash
# Generate Go client
protoc --go_out=. crates/rivven-protocol/proto/rivven.proto

# Generate Java client
protoc --java_out=. crates/rivven-protocol/proto/rivven.proto
```

### Request Types

- `Publish` - Send message to topic
- `Consume` - Read messages from partition
- `CreateTopic` / `DeleteTopic` - Topic management
- `CommitOffset` / `GetOffset` - Consumer group operations
- `Authenticate` - SASL authentication

### Response Types

- `Published` - Offset confirmation
- `Messages` - Batch of messages
- `Error` - Error with message

---

## Connector Architecture

### Airbyte-Style Protocol

Connectors follow the **Airbyte protocol**:

```rust
pub trait Source {
    fn spec() -> ConnectorSpec;        // Describe configuration
    fn check(&self) -> CheckResult;    // Test connectivity
    fn discover(&self) -> Catalog;     // List available streams
    fn read(&self) -> Stream<Event>;   // Stream events
}

pub trait Sink {
    fn spec() -> ConnectorSpec;
    fn check(&self) -> CheckResult;
    fn write(&self, events: Stream) -> WriteResult;
}
```

### Transform Pipeline

Events flow through optional transforms:

```
Source → [Transform₁] → [Transform₂] → ... → Sink
```

Built-in transforms include:
- Field masking (PII redaction)
- Field renaming
- Timestamp conversion
- Content-based routing
- Filtering

---

## Observability

### Prometheus Metrics

All components export metrics in Prometheus format:

```
rivven_messages_total{topic="orders"} 12345
rivven_consumer_lag_seconds{group="app"} 0.5
rivven_cdc_events_total{connector="postgres"} 9999
```

### Key Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_messages_total` | Messages published |
| `rivven_bytes_total` | Bytes written |
| `rivven_consumer_lag_seconds` | Consumer group lag |
| `rivven_cdc_lag_milliseconds` | CDC replication lag |
| `rivven_connections_active` | Active client connections |

### Web Dashboard

The optional dashboard is a **lightweight static HTML/JavaScript** UI embedded in the binary:

- Real-time cluster overview (auto-refresh every 5s)
- Topic management and monitoring
- Consumer group status with lag indicators
- Cluster node visualization
- Prometheus metrics integration

**Enable the dashboard:**

```bash
# Start rivvend with dashboard enabled
rivvend --data-dir ./data --dashboard

# Dashboard available at http://localhost:8080/
```

**Security:** The dashboard is disabled by default. In production, enable authentication via reverse proxy or mTLS.

---

## Next Steps

- [CDC Guide](cdc) — Set up database replication
- [Security](security) — Configure authentication and TLS
- [Kubernetes](kubernetes) — Deploy to Kubernetes
