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
| `rivven-server` | Message broker with storage, replication, and auth |
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
│  │  rivven-server   │       │       rivven-connect           │ │
│  │    (broker)      │◄─────►│  (connectors CLI)              │ │
│  │                  │native │                                 │ │
│  │  • Storage       │proto  │  Sources: postgres-cdc, mysql  │ │
│  │  • Replication   │       │  Sinks:   stdout, s3, http     │ │
│  │  • Consumer Grps │       │  Transforms: field-mask, etc   │ │
│  │  • Auth/RBAC     │       │                                 │ │
│  └──────────────────┘       └────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │               rivven-connect-sdk (library)                │  │
│  │  • Source trait   • Sink trait   • Transform trait        │  │
│  │  • Airbyte-style: spec/check/discover/read/write          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

| Crate | Purpose | Description |
|:------|:--------|:------------|
| `rivven-core` | Storage engine | Topics, partitions, consumer groups, compression |
| `rivven-server` | Broker binary | TCP server, protocol handling, auth |
| `rivven-client` | Rust client | Native async client library |
| `rivven-cluster` | Distributed | Raft consensus, SWIM gossip, partitioning |
| `rivven-cdc` | CDC library | PostgreSQL and MySQL replication |
| `rivven-connect` | Connector CLI & SDK | Configuration-driven connectors + traits |
| `rivven-storage` | Object storage | S3, GCS, Azure Blob sinks |
| `rivven-warehouse` | Data warehouses | Snowflake, BigQuery, Redshift sinks |
| `rivven-operator` | Kubernetes | CRDs and controller |
| `rivven-python` | Python bindings | PyO3-based Python SDK |

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
┌────────────┬─────────────────────────┐
│ Length (4B)│ Bincode-encoded payload │
└────────────┴─────────────────────────┘
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

The optional dashboard provides:

- Topic overview
- Consumer group status
- Cluster health
- Raft state visualization

---

## Next Steps

- [CDC Guide](/rivven/docs/cdc) - Set up database replication
- [Security](/rivven/docs/security) - Configure authentication and TLS
- [Kubernetes](/rivven/docs/kubernetes) - Deploy to Kubernetes
