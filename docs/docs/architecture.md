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
rivven-connect = { version = "0.0.11", features = ["postgres", "s3"] }

# Or include everything
[dependencies]
rivven-connect = { version = "0.0.11", features = ["full"] }
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
let frozen = buffer.freeze();  // Zero-copy transfer to consumer
```

#### Lock-Free Data Structures

Optimized for streaming workloads:

| Structure | Use Case | Performance |
|:----------|:---------|:------------|
| `LockFreeQueue` | MPMC message passing | O(1) push/pop |
| `ConcurrentHashMap` | Partition lookup | Sharded RwLocks |
| `AppendOnlyLog` | Sequential writes | Single-writer |
| `ConcurrentSkipList` | Range queries | Lock-free traversal |
| `TokenBucketRateLimiter` | Connector throughput control | Fully lock-free (AtomicU64 CAS for refill + acquire) |

#### Partition Append Optimization

The partition append path avoids unnecessary copies:

- **No message clone**: When tiered storage is enabled, the message is serialized once before being consumed by the log manager. The pre-serialized bytes are reused for the tiered storage write path.
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

### Async I/O (Portable, io_uring-style API)

Rivven provides a portable async I/O layer with an io_uring-style API. The current implementation uses `std::fs::File` behind `parking_lot::Mutex` as a portable fallback; the API is designed for a future true io_uring backend on Linux 5.6+:

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

Rivven uses a **format-prefixed** binary protocol for cross-language support:

```
┌────────────┬─────────────┬──────────────────────────┐
│ Length (4B)│ Format (1B) │ Serialized payload       │
│ Big-endian │ 0x00=post   │ Request or Response      │
│ u32        │ 0x01=proto  │                          │
└────────────┴─────────────┴──────────────────────────┘
```

- **Length**: 4-byte big-endian unsigned integer (includes format byte)
- **Format**: 1-byte wire format identifier
  - `0x00`: postcard (Rust-native, ~50ns serialize)
  - `0x01`: protobuf (cross-language compatible)
- **Payload**: Serialized Request or Response

### Format Auto-Detection

The server **auto-detects** the wire format from the first byte and responds in the same format:

```rust
// Rust client (postcard - fastest)
let wire_bytes = request.to_wire(WireFormat::Postcard)?;
let (response, format) = Response::from_wire(&response_bytes)?;

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
