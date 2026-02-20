---
layout: default
title: Log Compaction
nav_order: 18
---

# Log Compaction
{: .no_toc }

Keep only the latest value per key for changelog and state store topics.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Log compaction is a **retention mechanism** that keeps the latest record for each key:

| Cleanup Policy | Behavior | Use Case |
|:---------------|:---------|:---------|
| `delete` | Remove records older than retention | Event logs, metrics |
| `compact` | Keep latest per key, remove old values | Changelogs, state stores |
| `compact,delete` | Compact + enforce retention | CDC, bounded state |

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                          LOG COMPACTION                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   Before Compaction:                                                │
│   ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐          │
│   │K1:A │K2:B │K1:C │K3:D │K2:E │K1:F │K3:G │K2:H │K1:I │          │
│   └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘          │
│     ↓     ↓     ↓     ↓     ↓     ↓     ↓     ↓     ↓               │
│   del   del   del   del   del   keep  keep  keep  keep             │
│                                                                     │
│   After Compaction:                                                 │
│   ┌─────┬─────┬─────┬─────┐                                        │
│   │K1:F │K3:G │K2:H │K1:I │  ← Latest value for each key           │
│   └─────┴─────┴─────┴─────┘                                        │
│                                                                     │
│   Tombstones (K=null value):                                        │
│   ┌─────┬─────┬─────┐                                              │
│   │K4:X │K4:∅ │     │  → K4 deleted after tombstone retention      │
│   └─────┴─────┴─────┘                                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Background Compaction Worker

The server runs a background compaction worker that automatically compacts eligible topics:

- **Interval**: Every 5 minutes (with a 60-second initial delay after startup)
- **Eligibility**: Only topics with `cleanup.policy` set to `compact` or `compact,delete`
- **Scope**: Compacts only sealed (non-active) segments; the active segment is never compacted
- **Shutdown**: Worker responds to the server's shutdown signal for graceful termination

The compaction process for each segment:

1. **Key scan**: Reads all messages from sealed segments, building a map of key → latest offset
2. **Dedup**: For each segment, filters to keep only the latest-keyed messages
3. **Tombstone removal**: Messages with empty values (deletion markers) are removed
4. **Segment rewrite**: If compaction removed any messages, surviving messages are written to a **temporary file** in a `.compacting/` subdirectory, then **atomically renamed** to replace the old segment. A crash during the write phase leaves the original segment intact.

Compaction is safe under concurrent reads — the partition write lock is held only during the atomic rename, and deferred fsync ensures the lock is released before `fdatasync`.

---

## Configuration

### Enable Compaction

```bash
# Create compacted topic
rivven topic create user-profiles \
  --partitions 12 \
  --config cleanup.policy=compact

# Or update existing topic
rivven topic alter user-profiles \
  --config cleanup.policy=compact
```

### Compaction Settings

| Config | Default | Description |
|:-------|:--------|:------------|
| `cleanup.policy` | `delete` | `delete`, `compact`, or `compact,delete` |
| `min.cleanable.dirty.ratio` | `0.5` | Ratio of dirty records before compaction |
| `min.compaction.lag.ms` | `0` | Minimum time before record can be compacted |
| `max.compaction.lag.ms` | `∞` | Maximum time before compaction is forced |
| `delete.retention.ms` | `86400000` | How long tombstones are retained (24h) |
| `segment.ms` | `604800000` | Segment roll interval (7 days) |

### Example: User Profiles Topic

```bash
rivven topic create user-profiles \
  --partitions 24 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.3 \
  --config delete.retention.ms=604800000 \
  --config segment.bytes=104857600
```

---

## Use Cases

### 1. Database CDC Changelog

Capture table changes with only latest state per primary key:

```yaml
# CDC connector configuration
sources:
  users:
    connector: postgres-cdc
    topic: db.users
    config:
      table: users
      primary_key: id

# Topic auto-created with compaction
settings:
  topic:
    auto_create: true
    default_config:
      cleanup.policy: compact
```

### 2. Key-Value State Store

Materialized state for stream processing:

```rust
// Produce state updates
producer.send(Record::new()
    .topic("user-sessions")
    .key(&user_id)
    .value(&session_state)  // Latest state
).await?;

// Delete session (tombstone)
producer.send(Record::new()
    .topic("user-sessions")
    .key(&user_id)
    .value(None)  // Tombstone
).await?;
```

### 3. Configuration Store

Latest configuration per key:

```rust
// Store configs
producer.send(Record::new()
    .topic("configs")
    .key("feature.new-ui")
    .value(json!({"enabled": true, "rollout_pct": 50}))
).await?;

// Consumer rebuilds full config state from compacted topic
consumer.subscribe(&["configs"]).await?;
consumer.seek_to_beginning().await?;

let mut configs = HashMap::new();
for record in consumer.poll_all().await? {
    if let Some(value) = record.value() {
        configs.insert(record.key().to_string(), value);
    } else {
        configs.remove(record.key());  // Tombstone
    }
}
```

---

## Tombstones

**Tombstones** are records with a `null` value that mark a key for deletion:

```rust
// Delete user (produce tombstone)
producer.send(Record::new()
    .topic("users")
    .key(&user_id)
    .value(None)  // null value = tombstone
).await?;
```

### Tombstone Retention

Tombstones are kept for `delete.retention.ms` (default 24 hours) before removal:

```
Timeline:
├─ T=0: Tombstone produced (K1:∅)
├─ T=12h: Tombstone still visible to consumers
├─ T=24h: Tombstone eligible for deletion
└─ T=next compaction: Tombstone removed
```

**Why retain tombstones?**
- Allow downstream consumers to see the deletion
- Support exactly-once semantics during replay
- Enable CDC delete propagation

---

## Compaction Guarantees

### What IS Guaranteed

1. **Latest value preserved** — Most recent record per key always kept
2. **Order within key** — Records for same key maintain order
3. **Tombstone visibility** — Deletes visible for retention period
4. **Active segment safety** — Active (head) segment never compacted
5. **Crash safety** — Compacted data is written to a **temporary file** and then **atomically renamed** over the old segment. A crash during the write phase leaves the original segment fully intact; a crash after rename leaves the compacted segment in place.

### What IS NOT Guaranteed

1. **Compaction timing** — Background process, not real-time
2. **Cross-key order** — Records for different keys may reorder
3. **Immediate deletion** — Tombstoned keys kept until next compaction

---

## Compaction Process

### Triggering Compaction

Compaction runs when:

```
dirty_ratio = (dirty_bytes) / (total_bytes) > min.cleanable.dirty.ratio
```

Or when `max.compaction.lag.ms` exceeded.

### Segments and Compaction

```
Log structure:
┌──────────────┬──────────────┬──────────────┬──────────────┐
│  Segment 0   │  Segment 1   │  Segment 2   │  Segment 3   │
│  (closed)    │  (closed)    │  (closed)    │   (active)   │
│  compactable │  compactable │  compactable │  NOT touched │
└──────────────┴──────────────┴──────────────┴──────────────┘

Compaction process:
1. Select closed segments with high dirty ratio
2. Build offset map (latest offset per key)
3. Copy records, skipping superseded entries
4. Atomically replace old segments via temp-file rename
```

---

## Combined Policies

### `compact,delete`

Use both compaction and time-based retention:

```bash
rivven topic create orders \
  --config cleanup.policy=compact,delete \
  --config retention.ms=2592000000 \  # 30 days
  --config min.cleanable.dirty.ratio=0.5
```

**Behavior**:
1. Records older than `retention.ms` are deleted
2. Within retention, only latest per key is kept
3. Useful for CDC with bounded history

---

## Monitoring

### Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_log_cleaner_compactions_total` | Compaction runs |
| `rivven_log_cleaner_cleaned_bytes_total` | Bytes reclaimed |
| `rivven_log_cleaner_dirty_ratio` | Current dirty ratio per partition |
| `rivven_log_cleaner_duration_seconds` | Compaction duration |

### CLI

```bash
# Check topic compaction status
rivven topic describe user-profiles

# Output:
# Topic: user-profiles
# Partitions: 12
# Config:
#   cleanup.policy: compact
#   min.cleanable.dirty.ratio: 0.5
# 
# Partition  Dirty-Ratio  Last-Compacted
# 0          0.23         2026-01-27 10:30:00
# 1          0.67         2026-01-27 09:15:00  ← Due for compaction
# 2          0.12         2026-01-27 10:45:00
```

---

## Best Practices

### 1. Use Meaningful Keys

Compaction groups by key — choose keys wisely:

```rust
// Good: User ID as key for user state
Record::new().key(&user_id).value(&user_state)

// Bad: Timestamp as key (never compacts effectively)
Record::new().key(&timestamp).value(&event)
```

### 2. Size Tombstone Retention Appropriately

```yaml
# Fast propagation of deletes (short retention)
delete.retention.ms: 3600000    # 1 hour

# Long-running consumers need more time
delete.retention.ms: 604800000  # 7 days
```

### 3. Tune Compaction Frequency

```yaml
# Aggressive compaction (more CPU, less disk)
min.cleanable.dirty.ratio: 0.2
segment.bytes: 52428800  # 50MB segments

# Conservative compaction (less CPU, more disk)
min.cleanable.dirty.ratio: 0.7
segment.bytes: 1073741824  # 1GB segments
```

### 4. Monitor Disk Usage

Compacted topics can grow if:
- Too many unique keys
- Compaction not keeping up
- Large values per key

```yaml
# Alert on compacted topic growth
- alert: CompactedTopicGrowing
  expr: rate(rivven_topic_bytes_total{cleanup="compact"}[1h]) > 1000000
  labels:
    severity: warning
```

---

## Troubleshooting

### Topic Not Compacting

**Symptoms**: Disk usage growing, dirty ratio high

**Causes**:
- Active segment too large (not closed)
- Compaction disabled
- Resources constrained

**Solutions**:
```yaml
# Force smaller segments
segment.bytes: 104857600     # 100MB
segment.ms: 3600000          # 1 hour

# Check compaction is enabled
cleanup.policy: compact
```

---

### Old Records Still Visible

**Symptoms**: Consumers see superseded values

**Causes**:
- Records in active segment (not compactable)
- `min.compaction.lag.ms` not elapsed
- Compaction backlog

**Solutions**:
1. Wait for segment roll
2. Reduce `min.compaction.lag.ms`
3. Check compaction metrics for backlog
