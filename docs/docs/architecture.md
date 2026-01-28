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

### Single-Binary Simplicity

Rivven is designed as a **single binary** that contains everything needed to run:

- No JVM or runtime dependencies
- No ZooKeeper or external coordinators
- Self-contained storage engine
- Embedded Raft consensus

### Two Binaries, Clear Responsibilities

| Binary | Purpose |
|:-------|:--------|
| `rivvend` | Message broker with storage, replication, and auth |
| `rivven-connect` | Connector framework for sources and sinks |

This separation provides **security isolation**—database credentials stay in the connector process, never reaching the broker.

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
| `rivven-client` | Rust client | Native async client library |
| `rivven` | CLI | Command-line interface |
| `rivven-cluster` | Distributed | Raft consensus, SWIM gossip, partitioning |
| `rivven-cdc` | CDC library | PostgreSQL and MySQL replication |
| `rivven-connect` | Connector CLI & SDK | Configuration-driven connectors + traits |
| `rivven-queue` | Message queues | Kafka and MQTT connectors |
| `rivven-storage` | Object storage | S3, GCS, Azure Blob sinks |
| `rivven-warehouse` | Data warehouses | Snowflake, BigQuery, Redshift sinks |
| `rivven-operator` | Kubernetes | CRDs and controller |
| `rivven-python` | Python bindings | PyO3-based Python SDK |

---

## Grouped Connector Crates

Rivven organizes connectors into **domain-specific crates** to isolate heavy dependencies
and allow users to include only the connectors they need. This follows a modular architecture
where each crate focuses on a specific integration domain.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    rivven-connect (SDK)                         │
│  SourceFactory, SinkFactory, AnySource, AnySink, Registry       │
└─────────────────────────────────────────────────────────────────┘
        ↑ implement traits
┌─────────────┬─────────────┬───────────────┬─────────────────────┐
│ rivven-cdc  │ rivven-     │ rivven-       │ rivven-queue        │
│ (sources:   │ storage     │ warehouse     │ (source/sink:       │
│ pg, mysql)  │ (sink: s3,  │ (sink: bq,    │ kafka, mqtt)        │
│             │ gcs, azure) │ snowflake)    │                     │
└─────────────┴─────────────┴───────────────┴─────────────────────┘
        ↑ compose
┌─────────────────────────────────────────────────────────────────┐
│                     rivven-connect (CLI)                         │
│  Composes adapters, runs pipelines, no adapter code              │
└─────────────────────────────────────────────────────────────────┘
```

### Grouped Crates

| Crate | Domain | Connectors | Feature Flags |
|:------|:-------|:-----------|:--------------|
| `rivven-cdc` | Databases | postgres-cdc, mysql-cdc | `postgres`, `mysql` |
| `rivven-storage` | Object Storage | S3, GCS, Azure Blob | `s3`, `gcs`, `azure` |
| `rivven-warehouse` | Data Warehouses | BigQuery, Redshift, Snowflake | `bigquery`, `redshift`, `snowflake` |
| `rivven-queue` | Message Queues | Kafka, MQTT | `kafka`, `mqtt` |

### Benefits of Grouped Crates

1. **Dependency Isolation** - Heavy dependencies (Kafka, cloud SDKs) only included when needed
2. **Build Time Reduction** - Compile only the connectors you use
3. **Single Binary Option** - Enable all features for complete functionality
4. **Pluggable Architecture** - Add new connector crates without modifying core

### Usage Example

```toml
# Only include what you need
[dependencies]
rivven-connect = "0.0.1"
rivven-cdc = { version = "0.0.1", features = ["postgres"] }
rivven-queue = { version = "0.0.1", features = ["kafka"] }

# Or include everything
[dependencies]
rivven-connect = { version = "0.0.1", features = ["full"] }
rivven-queue = { version = "0.0.1", features = ["full"] }
rivven-storage = { version = "0.0.1", features = ["full"] }
rivven-warehouse = { version = "0.0.1", features = ["full"] }
```

### Registering Connectors

Each crate provides registration functions to add connectors to the runtime:

```rust
use rivven_connect::connectors::{create_source_registry, create_sink_registry};

let mut sources = create_source_registry();
let mut sinks = create_sink_registry();

// Add queue connectors
rivven_queue::register_all_connectors(&mut sources, &mut sinks);

// Add storage sinks
rivven_storage::register_all_sinks(&mut sinks);

// Add warehouse sinks
rivven_warehouse::register_all_sinks(&mut sinks);
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

## Distributed Coordination

### Raft Consensus

For clusters, Rivven uses **Raft** for:

- Leader election
- Log replication
- Membership changes

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
- Efficient O(log n) convergence

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

### Idempotent Producer (KIP-98)

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

### Sticky Partitioner (Kafka 2.4+ Style)

Rivven implements **sticky partitioning** for optimal throughput and distribution:

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition (Kafka-compatible) |
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

Keyed messages use **Murmur2** hash for Kafka compatibility:

```rust
// Same key always routes to same partition
partition = murmur2_hash(key) % num_partitions
```

This ensures:
- Ordering guarantee for messages with same key
- Compatibility with Kafka client expectations
- Deterministic routing across restarts

---

## Protocol

### Wire Format

Rivven uses a simple length-prefixed binary protocol:

```
┌────────────┬──────────────────────────┐
│ Length (4B)│ Postcard-encoded payload │
└────────────┴──────────────────────────┘
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

The optional dashboard is built with **Leptos** (Rust → WebAssembly) and provides:

- Topic overview and message counts
- Consumer group status with lag indicators
- Cluster health and node membership
- Raft state visualization
- Prometheus metrics integration

**Build the dashboard:**

```bash
# Install trunk (Leptos build tool)
cargo install trunk
rustup target add wasm32-unknown-unknown

# Build and install dashboard assets
just dashboard-install

# Rebuild server with embedded dashboard
cargo build -p rivvend --release
```

---

## Next Steps

- [CDC Guide](cdc) — Set up database replication
- [Security](security) — Configure authentication and TLS
- [Kubernetes](kubernetes) — Deploy to Kubernetes
