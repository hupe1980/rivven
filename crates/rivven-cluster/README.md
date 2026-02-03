# rivven-cluster

> Distributed cluster coordination for the Rivven event streaming platform.

## Overview

`rivven-cluster` provides the distributed coordination layer for Rivven, including leader election, failure detection, and partition assignment.

## Features

| Feature | Description |
|:--------|:------------|
| **Raft Consensus** | Leader election and log replication using OpenRaft |
| **redb Storage** | Pure Rust persistent storage (zero C dependencies) |
| **SWIM Gossip** | Failure detection and membership management |
| **Partitioning** | Consistent hashing for partition assignment |
| **QUIC Transport** | Efficient, secure node-to-node communication |

## Why redb?

Rivven uses **redb** instead of RocksDB for Raft log storage:

| Aspect | redb | RocksDB |
|:-------|:-----|:--------|
| **Build time** | ~10s | 2-5 min |
| **Binary size** | Minimal | +10-15 MB |
| **Cross-compile** | ✅ Works everywhere | ❌ Needs C++ toolchain |
| **Docker musl** | ✅ Works | ❌ Needs musl-g++ |
| **ACID** | ✅ Full | ✅ Full |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        rivven-cluster                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │    Raft      │    │    SWIM      │    │  Partition   │       │
│  │  Consensus   │    │   Gossip     │    │  Manager     │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                   │                   │               │
│         └───────────────────┼───────────────────┘               │
│                             │                                   │
│                    ┌────────────────┐                           │
│                    │ QUIC Transport │                           │
│                    └────────────────┘                           │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### Raft Consensus

Used for metadata replication and leader election:

```rust
use rivven_cluster::{RaftNode, RaftConfig};

let config = RaftConfig {
    node_id: 1,
    peers: vec![2, 3],
    election_timeout_ms: 150..300,
    heartbeat_interval_ms: 50,
};

let node = RaftNode::new(config).await?;
```

### SWIM Gossip

Decentralized failure detection:

```rust
use rivven_cluster::{SwimNode, SwimConfig};

let config = SwimConfig {
    bind_addr: "0.0.0.0:7946".parse()?,
    known_peers: vec!["node2:7946".parse()?],
    protocol_period_ms: 1000,
    suspect_timeout_ms: 5000,
};

let node = SwimNode::new(config).await?;
```

### Partition Assignment

Consistent hashing with virtual nodes:

```rust
use rivven_cluster::{PartitionManager, HashRing};

let ring = HashRing::new(128); // 128 virtual nodes per physical node
ring.add_node("node1")?;
ring.add_node("node2")?;

let owner = ring.get_partition_owner("my-topic", 0)?;
```

## Configuration

```yaml
cluster:
  node_id: 1
  bind_address: "0.0.0.0:9093"
  
  raft:
    peers:
      - "node2:9093"
      - "node3:9093"
    election_timeout_min_ms: 150
    election_timeout_max_ms: 300
    
  gossip:
    bind_port: 7946
    known_peers:
      - "node2:7946"
    protocol_period_ms: 1000
    
  partitioning:
    virtual_nodes: 128
    replication_factor: 3
```

## Documentation

- [Distributed Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)
- [Kubernetes Deployment](https://rivven.hupe1980.github.io/rivven/docs/kubernetes)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
