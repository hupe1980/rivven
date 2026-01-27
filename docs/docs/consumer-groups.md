---
layout: default
title: Consumer Groups
nav_order: 17
---

# Consumer Groups
{: .no_toc }

Coordinated consumption with automatic partition assignment and exactly-once delivery.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Consumer groups enable **multiple consumers** to share the load of processing messages from a topic:

- **Automatic partition assignment** — Partitions distributed across group members
- **Rebalancing** — Partitions reassigned when members join/leave
- **Offset tracking** — Progress checkpointed for exactly-once delivery
- **Failure detection** — Heartbeat-based health monitoring

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     CONSUMER GROUP: "order-processors"              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Topic: orders (6 partitions)                                      │
│   ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐                        │
│   │ P0 │ │ P1 │ │ P2 │ │ P3 │ │ P4 │ │ P5 │                        │
│   └──┬─┘ └──┬─┘ └──┬─┘ └──┬─┘ └──┬─┘ └──┬─┘                        │
│      │      │      │      │      │      │                           │
│      └──────┼──────┘      └──────┼──────┘                           │
│             │                    │                                  │
│             ▼                    ▼                                  │
│      ┌────────────┐       ┌────────────┐       ┌────────────┐      │
│      │ Consumer 1 │       │ Consumer 2 │       │ Consumer 3 │      │
│      │ P0, P1, P2 │       │   P3, P4   │       │     P5     │      │
│      └────────────┘       └────────────┘       └────────────┘      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Basic Usage

### CLI

```bash
# Consume with consumer group
rivvenctl consume orders \
  --group order-processors \
  --from-beginning

# List consumer groups
rivvenctl group list

# Describe group
rivvenctl group describe order-processors

# Reset offsets
rivvenctl group reset-offsets order-processors \
  --topic orders \
  --to-earliest
```

### Rust Client

```rust
use rivven_client::{Consumer, ConsumerConfig};

let config = ConsumerConfig::builder()
    .bootstrap_servers(vec!["rivven:9092".into()])
    .group_id("order-processors")
    .auto_offset_reset("earliest")
    .build()?;

let consumer = Consumer::new(config).await?;
consumer.subscribe(&["orders"]).await?;

loop {
    let records = consumer.poll(Duration::from_millis(100)).await?;
    for record in records {
        process(&record).await?;
    }
    consumer.commit().await?;
}
```

---

## Assignment Strategies

### Range (Default)

Assigns contiguous partition ranges to each consumer:

```
Topic: orders (6 partitions), 3 consumers

Consumer 1: P0, P1  (partitions 0-1)
Consumer 2: P2, P3  (partitions 2-3)  
Consumer 3: P4, P5  (partitions 4-5)
```

**Best for**: Even partition distribution with sorted data

### Round-Robin

Distributes partitions one-by-one across consumers:

```
Topic: orders (6 partitions), 3 consumers

Consumer 1: P0, P3
Consumer 2: P1, P4
Consumer 3: P2, P5
```

**Best for**: Uniform load distribution across multiple topics

### Sticky

Minimizes partition movement during rebalances:

```
Before (2 consumers):
  Consumer 1: P0, P1, P2
  Consumer 2: P3, P4, P5

After adding Consumer 3 (sticky):
  Consumer 1: P0, P1      ← kept P0, P1
  Consumer 2: P3, P4      ← kept P3, P4
  Consumer 3: P2, P5      ← only moved partitions
```

**Best for**: Stateful consumers, minimizing rebalance impact

### Configuration

```yaml
# Consumer configuration
consumer:
  group_id: order-processors
  partition_assignment_strategy: sticky  # range, round_robin, sticky
```

---

## Static Membership (KIP-345)

Static membership provides **stable consumer identity** across restarts, essential for Kubernetes deployments.

### The Problem

Without static membership, every pod restart triggers a full rebalance:

```
Pod restart → New member_id → REBALANCE → All consumers stop → Reassign ALL partitions
```

This causes:
- Service interruption during rebalance
- Duplicate processing (uncommitted work)
- Wasted computation rebuilding state

### The Solution

With static membership, pods maintain identity:

```
Pod restart → Same group.instance.id → REJOIN → Keep previous assignment
```

### Configuration

```yaml
# Each consumer instance needs a unique, stable ID
consumer:
  group_id: order-processors
  group_instance_id: order-processor-0  # Stable across restarts
  session_timeout_ms: 45000             # Longer timeout for planned restarts
```

### Kubernetes StatefulSet Example

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: order-processor
spec:
  replicas: 3
  selector:
    matchLabels:
      app: order-processor
  template:
    spec:
      containers:
        - name: processor
          env:
            - name: RIVVEN_GROUP_INSTANCE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name  # order-processor-0, order-processor-1, etc.
            - name: RIVVEN_GROUP_ID
              value: order-processors
```

### Behavior

| Event | Without Static Membership | With Static Membership |
|:------|:--------------------------|:-----------------------|
| Pod restart | Full rebalance | Rejoin, keep assignment |
| Rolling update | N rebalances | 0 rebalances (if within session timeout) |
| Scale down | Rebalance after leave | Rebalance after session timeout |
| Network partition | Quick failover | Wait for session timeout |

---

## Cooperative Rebalancing (KIP-429)

Cooperative rebalancing eliminates **stop-the-world** rebalances by using incremental partition reassignment.

### Traditional (Eager) Rebalancing

```
1. Rebalance triggered
2. ALL consumers STOP and revoke ALL partitions
3. Leader computes new assignment
4. ALL consumers receive new assignment and RESTART
   
   ────────────────────────────────────
   Time →
   [===STOP===][===IDLE===][===START===]
            Total downtime
```

### Cooperative Rebalancing

```
1. First rebalance: Identify partitions that need to move
2. ONLY affected consumers revoke ONLY moving partitions
3. Second rebalance: Assign revoked partitions to new owners
4. Non-moving partitions continue processing throughout

   ────────────────────────────────────
   Time →
   Consumer 1: [======RUNNING======]  (no change)
   Consumer 2: [===][REVOKE][======]  (brief pause for P3)
   Consumer 3: [===][......][======]  (waits for P3)
```

### Configuration

```yaml
consumer:
  group_id: order-processors
  partition_assignment_strategy: cooperative_sticky
  # OR
  rebalance_protocol: cooperative
```

### Supported Strategies

| Strategy | Protocol | Behavior |
|:---------|:---------|:---------|
| `range` | Eager | Full revoke |
| `round_robin` | Eager | Full revoke |
| `sticky` | Eager | Minimizes movement, but full revoke |
| `cooperative_sticky` | Cooperative | Incremental, minimal disruption |

### Requirements

- **All group members** must support cooperative protocol
- If any member uses eager protocol, the entire group falls back to eager
- Protocol negotiated during JoinGroup

---

## Offset Management

### Automatic Commit

```yaml
consumer:
  enable_auto_commit: true
  auto_commit_interval_ms: 5000
```

### Manual Commit

```rust
// Commit after processing each batch
loop {
    let records = consumer.poll(Duration::from_millis(100)).await?;
    for record in records {
        process(&record).await?;
    }
    consumer.commit().await?;  // Synchronous commit
}

// Or commit specific offsets
consumer.commit_offsets(&[
    TopicPartitionOffset::new("orders", 0, 12345),
]).await?;
```

### Commit Within Transaction

For exactly-once semantics, commit offsets atomically with output:

```rust
let txn = producer.begin_transaction().await?;

// Process and produce
for record in consumer.poll(timeout).await? {
    let output = transform(&record);
    producer.send(&output).await?;
}

// Commit offsets and output atomically
txn.commit_offsets(consumer.position()).await?;
txn.commit().await?;
```

---

## Rebalance Listeners

React to partition assignments and revocations:

```rust
use rivven_client::{Consumer, RebalanceListener};

struct MyListener;

impl RebalanceListener for MyListener {
    fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        println!("Assigned: {:?}", partitions);
        // Initialize state for new partitions
    }
    
    fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        println!("Revoked: {:?}", partitions);
        // Commit offsets, flush state
    }
}

consumer.set_rebalance_listener(MyListener);
```

---

## Group Coordinator

Each consumer group has a **coordinator** broker responsible for:

- Managing group membership
- Triggering rebalances
- Storing committed offsets

### Finding the Coordinator

```bash
rivvenctl group describe order-processors

# Output:
# Group: order-processors
# Coordinator: broker-2 (10.0.0.3:9092)
# State: Stable
# Members: 3
```

### Coordinator Selection

The coordinator is determined by hashing the group ID:

```
coordinator_partition = hash(group_id) % __consumer_offsets_partitions
coordinator_broker = leader_of(coordinator_partition)
```

---

## Failure Detection

### Heartbeats

Consumers send periodic heartbeats to prove liveness:

```yaml
consumer:
  heartbeat_interval_ms: 3000   # Send heartbeat every 3s
  session_timeout_ms: 45000     # Coordinator waits 45s before declaring dead
```

### Session Timeout

If no heartbeat received within `session_timeout_ms`:

1. Coordinator marks member as dead
2. Triggers rebalance
3. Partitions reassigned to remaining members

### Max Poll Interval

Protects against stuck consumers:

```yaml
consumer:
  max_poll_interval_ms: 300000  # 5 minutes max between polls
```

If `poll()` not called within this interval, consumer is considered failed.

---

## Monitoring

### Key Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_consumer_group_members` | Number of active members |
| `rivven_consumer_group_lag` | Messages behind head |
| `rivven_consumer_group_rebalances_total` | Rebalance count |
| `rivven_consumer_group_rebalance_duration_seconds` | Rebalance time |
| `rivven_consumer_commit_latency_seconds` | Offset commit latency |

### Alerting

```yaml
# Alert on consumer lag
- alert: RivvenConsumerLag
  expr: rivven_consumer_group_lag > 10000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Consumer group {{ $labels.group }} is lagging"

# Alert on frequent rebalances
- alert: RivvenFrequentRebalances
  expr: rate(rivven_consumer_group_rebalances_total[1h]) > 10
  labels:
    severity: warning
  annotations:
    summary: "Frequent rebalances in {{ $labels.group }}"
```

### CLI Monitoring

```bash
# Watch consumer lag
rivvenctl group lag order-processors --watch

# Output:
# TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# orders   0          12345           12350           5
# orders   1          23456           23460           4
# orders   2          34567           34600           33
```

---

## Troubleshooting

### Frequent Rebalances

**Symptoms**: Consumer constantly rejoining, processing interrupted

**Causes**:
- `max.poll.interval.ms` too short for processing time
- Network instability
- Long GC pauses

**Solutions**:
```yaml
consumer:
  max_poll_interval_ms: 600000  # Increase to 10 minutes
  session_timeout_ms: 60000     # Longer session timeout
  heartbeat_interval_ms: 10000  # Less frequent heartbeats
```

---

### Consumer Lag Growing

**Symptoms**: Lag increases over time

**Causes**:
- Processing too slow
- Not enough consumers
- Partition imbalance

**Solutions**:
1. Add more consumers (up to partition count)
2. Increase partition count
3. Optimize processing logic
4. Use `cooperative_sticky` to reduce rebalance impact

---

### Duplicate Processing

**Symptoms**: Same messages processed multiple times

**Causes**:
- Auto-commit with processing failure
- Rebalance before commit

**Solutions**:
1. Use manual commit after processing
2. Use transactional consumers for exactly-once
3. Make processing idempotent
