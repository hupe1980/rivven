# rivven-cluster

> Distributed cluster coordination for the Rivven event streaming platform.

## Overview

`rivven-cluster` provides the distributed coordination layer for Rivven, implementing consensus, membership, and partition management with hot paths optimized for streaming workloads.

## Features

| Feature | Description |
|:--------|:------------|
| **Raft Consensus** | Authenticated leader election and log replication using OpenRaft |
| **redb Storage** | Pure Rust persistent storage (zero C dependencies) |
| **SWIM Gossip** | HMAC-authenticated failure detection and membership management |
| **ISR Replication** | In-Sync Replica tracking with high watermark |
| **Partitioning** | Consistent hashing with rack awareness |
| **QUIC Transport** | 0-RTT, multiplexed streams, BBR congestion control |
| **Consumer Coordination** | Consumer group management with Raft persistence |

## Why redb?

Rivven uses **redb** instead of RocksDB for Raft log storage:

| Aspect | redb | RocksDB |
|:-------|:-----|:--------|
| **Build time** | ~10s | 2-5 min |
| **Binary size** | Minimal | +10-15 MB |
| **Cross-compile** | ✅ Works everywhere | ❌ Needs C++ toolchain |
| **Docker musl** | ✅ Works | ❌ Needs musl-g++ |
| **ACID** | ✅ Full | ✅ Full |
| **Memory usage** | Lower | Higher (bloom filters) |

## Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           rivven-cluster                                   │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐   │
│  │  ClusterCoord  │  │  Replication   │  │    Consumer Coordinator    │   │
│  │  (orchestrate) │  │   Manager      │  │    (group management)      │   │
│  └───────┬────────┘  └───────┬────────┘  └─────────────┬──────────────┘   │
│          │                   │                         │                   │
│  ┌───────┴────────┐  ┌───────┴────────┐  ┌─────────────┴──────────────┐   │
│  │  Raft Consensus│  │ ISR Tracking   │  │   Offset Management        │   │
│  │  (metadata)    │  │ (replication)  │  │   (commit/fetch)           │   │
│  └───────┬────────┘  └───────┬────────┘  └─────────────┬──────────────┘   │
│          │                   │                         │                   │
│  ┌───────┴────────┐  ┌───────┴────────┐  ┌─────────────┴──────────────┐   │
│  │  SWIM Gossip   │  │  Partition     │  │   Metadata Store           │   │
│  │  (membership)  │  │  Placer        │  │   (state machine)          │   │
│  └───────┬────────┘  └───────┬────────┘  └─────────────┬──────────────┘   │
│          │                   │                         │                   │
│          └───────────────────┼─────────────────────────┘                   │
│                              │                                             │
│                   ┌──────────┴──────────┐                                  │
│                   │  Transport Layer    │                                  │
│                   │  (TCP / QUIC)       │                                  │
│                   └─────────────────────┘                                  │
│                                                                            │
└───────────────────────────────────────────────────────────────────────────┘
```

## Components

### Raft Consensus

Metadata replication with OpenRaft and redb storage:

```rust
use rivven_cluster::{RaftNode, RaftNodeConfig};

let config = RaftNodeConfig {
    node_id: "node-1".to_string(),
    standalone: false,
    data_dir: "/var/lib/rivven/raft".into(),
    heartbeat_interval_ms: 50,
    election_timeout_min_ms: 150,
    election_timeout_max_ms: 300,
    snapshot_threshold: 10000,
    initial_members: vec![],
};

let mut node = RaftNode::with_config(config).await?;
node.start().await?;
```

### SWIM Gossip

Decentralized failure detection with O(N) protocol-period dissemination:

```rust
use rivven_cluster::{Membership, SwimConfig, NodeInfo};

let config = SwimConfig {
    ping_interval: Duration::from_millis(100),
    ping_timeout: Duration::from_millis(50),
    indirect_probes: 3,
    suspicion_multiplier: 3,
    ..Default::default()
};

let membership = Membership::new(local_node, config, shutdown_rx).await?;
membership.join(&seeds).await?;
```

### ISR Replication

Kafka-style ISR tracking with high watermark:

```rust
use rivven_cluster::{ReplicationManager, ReplicationConfig, PartitionReplication};

let config = ReplicationConfig {
    min_isr: 2,
    replica_lag_max_messages: 1000,
    replica_lag_max_time: Duration::from_secs(10),
    ..Default::default()
};

let manager = ReplicationManager::new(node_id, config);
let partition = manager.get_or_create(partition_id, is_leader);

// Handle follower fetch and update ISR
manager.handle_replica_fetch(&partition_id, &replica_id, fetch_offset).await?;
```

**Follower Persistence**: `FollowerFetcher` persists records via `Partition::append_replicated_batch()` before advancing the fetch offset. This ensures ISR followers hold real data — leader failure does not cause data loss. Replica state reporting tracks consecutive failures and logs warnings after 5+ failures for operator visibility.

### Partition Placement

Consistent hashing with rack awareness:

```rust
use rivven_cluster::{PartitionPlacer, PlacementConfig, PlacementStrategy};

let config = PlacementConfig {
    strategy: PlacementStrategy::ConsistentHash,
    rack_aware: true,
    virtual_nodes: 150,
    max_partitions_per_node: 0,
};

let mut placer = PartitionPlacer::new(config);
placer.add_node(&node);

let replicas = placer.assign_partition("orders", 0, 3)?;
```

### QUIC Transport (Optional)

High-performance transport with 0-RTT and multiplexing:

```rust
use rivven_cluster::quic_transport::{QuicTransport, QuicConfig, TlsConfig};

let config = QuicConfig::high_throughput();
let tls = TlsConfig::self_signed("rivven-cluster")?;

let transport = QuicTransport::new(bind_addr, config, tls).await?;
transport.start().await?;

let response = transport.send(&peer_id, request).await?;
```

## Feature Flags

```toml
[features]
default = ["raft", "swim", "metrics-prometheus", "compression"]
raft = ["openraft", "redb", "reqwest"]
swim = []
quic = ["quinn", "rustls", "rcgen"]
full = ["raft", "swim", "metrics-prometheus", "compression", "quic"]
```

## Configuration

```yaml
cluster:
  node_id: "node-1"
  mode: cluster  # or "standalone"
  rack: "rack-1"
  
  client_addr: "0.0.0.0:9092"
  cluster_addr: "0.0.0.0:9093"
  
  seeds:
    - "node-2:9093"
    - "node-3:9093"
  
  swim:
    ping_interval_ms: 100
    ping_timeout_ms: 50
    indirect_probes: 3
    suspicion_multiplier: 3
    
  raft:
    heartbeat_interval_ms: 50
    election_timeout_min_ms: 150
    election_timeout_max_ms: 300
    snapshot_threshold: 10000
    
  replication:
    min_isr: 2
    replica_lag_max_messages: 1000
    replica_lag_max_time_secs: 10
    fetch_interval_ms: 50
    
  topic_defaults:
    partitions: 6
    replication_factor: 3
```

## Testing

```bash
# Unit tests (68 tests)
cargo test -p rivven-cluster --lib

# Integration tests (36 tests, 4 ignored for chaos testing)
cargo test -p rivven-cluster --test '*'

# Standalone stress tests
cargo test -p rivven-cluster --test three_node_cluster -- --nocapture
```

## Documentation

- [Architecture Overview](https://rivven.hupe1980.github.io/rivven/docs/architecture)
- [Kubernetes Deployment](https://rivven.hupe1980.github.io/rivven/docs/kubernetes)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
