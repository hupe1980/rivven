---
layout: default
title: Change Data Capture
nav_order: 4
has_children: true
---

# Change Data Capture (CDC)
{: .no_toc }

Stream database changes in real-time with native CDC connectors.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven provides **native CDC support** for PostgreSQL and MySQL/MariaDB, enabling real-time change data capture with a lightweight, high-performance Rust implementation.

### Key Features

| Feature | Description |
|:--------|:------------|
| **Zero Dependencies** | Native Rust implementation — no JVM, no external connectors |
| **Standard Event Format** | Industry-standard CDC envelope format |
| **17 Built-in Transforms** | Filter, mask, route, and transform events in-flight |
| **Production Ready** | TLS/mTLS, SCRAM-SHA-256, circuit breakers, rate limiting |
| **Full Observability** | Comprehensive Prometheus metrics |
| **Health Monitoring** | Auto-recovery, liveness probes, lag monitoring |
| **Notifications** | Snapshot progress, streaming status, webhook alerts |

### Supported Databases

| Database | Version | Protocol |
|:---------|:--------|:---------|
| PostgreSQL | 10+ | Logical replication (pgoutput) |
| MySQL | 5.7+ | Binary log with GTID |
| MariaDB | 10.2+ | Binary log with MariaDB GTID |

### Documentation

| Guide | Description |
|:------|:------------|
| [PostgreSQL CDC Guide](cdc-postgres) | Complete PostgreSQL setup, TLS, signal tables, incremental snapshots |
| [MySQL/MariaDB CDC Guide](cdc-mysql) | MySQL and MariaDB binary log replication setup |
| [Configuration Reference](cdc-configuration) | All CDC configuration options and environment variables |
| [Troubleshooting Guide](cdc-troubleshooting) | Diagnose and resolve common issues |

---

## Quick Start

### PostgreSQL

```yaml
# rivven-connect.yaml
version: "1.0"

sources:
  orders_db:
    connector: postgres-cdc
    topic: cdc.orders
    config:
      host: localhost
      port: 5432
      database: shop
      user: rivven
      password: ${POSTGRES_PASSWORD}
      slot_name: rivven_slot
      publication_name: rivven_pub
```

```bash
rivven-connect --config rivven-connect.yaml
```

See [PostgreSQL CDC Guide](cdc-postgres) for complete setup instructions.

### MySQL

```yaml
sources:
  orders_db:
    connector: mysql-cdc
    topic: cdc.orders
    config:
      host: localhost
      port: 3306
      database: shop
      user: rivven
      password: ${MYSQL_PASSWORD}
      server_id: 12345
```

See [MySQL/MariaDB CDC Guide](cdc-mysql) for complete setup instructions.

---

## Event Format

CDC events follow the **standard CDC envelope format**:

```json
{
  "before": null,
  "after": {
    "id": 1001,
    "customer_id": 42,
    "total": 99.99,
    "created_at": "2026-01-25T10:30:00Z"
  },
  "source": {
    "connector": "postgres-cdc",
    "db": "shop",
    "schema": "public",
    "table": "orders",
    "lsn": "0/16B3748",
    "ts_ms": 1737802200000
  },
  "op": "c",
  "ts_ms": 1737802200123
}
```

### Operation Types

| `op` | Meaning |
|:-----|:--------|
| `c` | Create (INSERT) |
| `u` | Update (UPDATE) |
| `d` | Delete (DELETE) |
| `r` | Read (snapshot) |

### Key Fields

| Field | Description |
|:------|:------------|
| `before` | Row state before change (null for INSERT) |
| `after` | Row state after change (null for DELETE) |
| `source` | Metadata about the source database |
| `op` | Operation type |
| `ts_ms` | Event timestamp (milliseconds) |

---

## Snapshots

Rivven supports **initial snapshots** to capture the full state of tables before streaming changes. The snapshot is executed automatically based on the configured mode.

### Snapshot Modes

| Mode | Description |
|:-----|:------------|
| `initial` | Snapshot on first start (when no stored offsets exist) |
| `always` | Snapshot on every connector start |
| `never` | Never snapshot — streaming only |
| `when_needed` | Same as `initial` |
| `initial_only` | Snapshot once, then stop (no streaming) |
| `schema_only` | Capture schema metadata only (no data) |
| `recovery` | Force re-snapshot for disaster recovery |

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      snapshot:
        mode: initial          # Snapshot mode
        batch_size: 10000      # Rows per SELECT batch
        parallel_tables: 4     # Tables to snapshot in parallel
        progress_dir: /var/lib/rivven/snapshot  # Resumable progress
        query_timeout_secs: 60 # SELECT timeout
        throttle_delay_ms: 0   # Delay between batches
        max_retries: 3         # Retry failed batches
        include_tables:        # Only snapshot these tables
          - public.orders
          - public.customers
        exclude_tables:        # Skip these tables
          - public.audit_logs
```

### Snapshot Flow

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Connector Start                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│  1. Check snapshot mode (initial/always/never/etc.)                          │
│  2. Check for stored offsets (prior state)                                   │
│  3. If snapshot needed:                                                      │
│     a. Get watermark (WAL LSN / binlog position)                            │
│     b. SELECT tables in batches (keyset pagination)                         │
│     c. Emit events with op='r' (read/snapshot)                              │
│     d. Save progress for resumability                                        │
│  4. Transition to streaming from watermark position                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Event Example (Snapshot)

```json
{
  "before": null,
  "after": {
    "id": 1001,
    "customer_id": 42,
    "total": 99.99
  },
  "source": {
    "connector": "postgres-cdc",
    "snapshot": "true",
    "db": "shop",
    "schema": "public",
    "table": "orders"
  },
  "op": "r",
  "ts_ms": 1737802200000
}
```

### Resumable Snapshots

When `progress_dir` is configured, snapshot progress is persisted to disk:

```bash
$ ls /var/lib/rivven/snapshot/
public.orders.json        # {"table": "public.orders", "last_key": "5000", "rows": 5000}
public.customers.json     # {"table": "public.customers", "last_key": "1000", "rows": 1000}
```

If the connector restarts during a snapshot, it resumes from the last checkpoint rather than starting over.

### Best Practices

1. **Use `initial` mode** for most use cases — snapshots only when needed
2. **Set `progress_dir`** for large tables to enable resumability
3. **Tune `batch_size`** based on row size (10K-50K typical)
4. **Use `parallel_tables`** for multiple small tables
5. **Use `exclude_tables`** to skip audit/log tables
6. **Monitor `snapshot_duration_ms`** metric for performance

---

## Incremental Snapshots

Rivven supports **incremental (non-blocking) snapshots** that run while CDC streaming continues. This approach enables:

- Adding new tables to capture without stopping the connector
- Re-syncing tables after schema changes
- Recovering from data inconsistencies
- Ad-hoc data refresh on demand

### Signal Table Setup

Create a signal table in your database:

```sql
CREATE TABLE IF NOT EXISTS rivven_signal (
    id VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048) NULL
);
```

Enable the signal table in your CDC publication:

```sql
-- PostgreSQL
ALTER PUBLICATION rivven_pub ADD TABLE rivven_signal;

-- MySQL - ensure table has binlog enabled
```

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      signal:
        enabled: true
        data_collection: public.rivven_signal
        
      incremental_snapshot:
        chunk_size: 1024          # Rows per chunk
        watermark_strategy: insert_delete  # open/close watermark mode
        max_concurrent_chunks: 1  # Chunks to process in parallel
```

### Triggering a Snapshot

Insert a signal row to trigger an incremental snapshot:

```sql
-- Snapshot specific tables
INSERT INTO rivven_signal (id, type, data) VALUES (
    'sig-001',
    'execute-snapshot',
    '{"data-collections": ["public.orders", "public.customers"]}'
);

-- Stop an in-progress snapshot
INSERT INTO rivven_signal (id, type, data) VALUES (
    'sig-002',
    'stop-snapshot',
    NULL
);

-- Pause streaming and snapshot
INSERT INTO rivven_signal (id, type, data) VALUES (
    'sig-003',
    'pause-snapshot',
    NULL
);

-- Resume
INSERT INTO rivven_signal (id, type, data) VALUES (
    'sig-004',
    'resume-snapshot',
    NULL
);
```

### How It Works

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                   Incremental Snapshot Flow                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Signal INSERT flows through CDC stream                                   │
│  2. Connector detects signal and starts incremental snapshot                 │
│  3. For each chunk:                                                          │
│     a. Open watermark window (INSERT to signal table)                        │
│     b. Execute SELECT for chunk                                              │
│     c. Buffer results locally                                                │
│     d. Close watermark window (DELETE from signal table)                     │
│     e. Deduplicate streaming events against buffer                           │
│     f. Emit remaining buffer entries as op='r' events                        │
│  4. Continue to next chunk until table complete                              │
│  5. Streaming continues uninterrupted throughout                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Watermark Strategy

The watermark strategy determines how chunks are bounded:

| Strategy | Description |
|:---------|:------------|
| `insert_delete` | INSERT/DELETE pairs bracket each chunk (default) |
| `update` | UPDATE statement brackets chunks |

### Event Deduplication

During incremental snapshots, the same row may appear in both:
- The snapshot chunk (op='r')
- The streaming CDC events (op='c/u/d')

Rivven automatically deduplicates these events using the DBLog watermark algorithm, ensuring each row is emitted exactly once.

#### Deduplication Algorithm (DBLog)

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DBLog Watermark Deduplication                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. OPEN WINDOW                                                              │
│     - Record timestamp (watermark_ts)                                        │
│     - Insert open watermark to signal table                                  │
│                                                                              │
│  2. SNAPSHOT QUERY                                                           │
│     - SELECT rows for this chunk                                             │
│     - Buffer rows locally with primary keys                                  │
│                                                                              │
│  3. STREAMING CONTINUES (parallel)                                           │
│     - For each streaming event:                                              │
│       • If event.key exists in buffer AND event.ts >= watermark_ts:          │
│         → REMOVE from buffer (streaming wins)                                │
│       • If DELETE event and key in buffer:                                   │
│         → REMOVE from buffer (deletes always win)                            │
│                                                                              │
│  4. CLOSE WINDOW                                                             │
│     - Insert close watermark to signal table                                 │
│     - Emit remaining buffer entries as op='r'                                │
│     - These are rows NOT modified during the window                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Key Deduplication Rules

| Scenario | Action |
|:---------|:-------|
| Snapshot row + no streaming event | Emit snapshot row (op='r') |
| Snapshot row + INSERT during window | Drop snapshot row, streaming INSERT already emitted |
| Snapshot row + UPDATE during window | Drop snapshot row, streaming UPDATE already emitted |
| Snapshot row + DELETE during window | Drop snapshot row, row no longer exists |
| Streaming event before window opens | Ignore (stale), not in buffer |

### Parallel Chunk Execution

For high throughput, Rivven supports parallel chunk processing via `max_concurrent_chunks`:

```yaml
incremental_snapshot:
  chunk_size: 1024
  max_concurrent_chunks: 4   # Process 4 chunks simultaneously
```

**How Parallel Processing Works:**

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                   Parallel Chunk Execution                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  Time →                                                                      │
│                                                                              │
│  Chunk 1: [OPEN]────────[QUERY]────────[BUFFER]────────[CLOSE]              │
│  Chunk 2:       [OPEN]────────[QUERY]────────[BUFFER]────────[CLOSE]        │
│  Chunk 3:             [OPEN]────────[QUERY]────────[BUFFER]────────[CLOSE]  │
│  Chunk 4:                   [OPEN]────────[QUERY]────────[BUFFER]───[CLOSE] │
│                                                                              │
│  Streaming Events: ─────────────────────────────────────────────────────►   │
│  (checked against ALL open windows)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key Points:**
- Each chunk maintains its own deduplication buffer
- Streaming events are checked against ALL open windows
- Total memory = `max_concurrent_chunks × max_buffer_memory`
- Higher concurrency = better throughput but more memory

### Programmatic API (Rust)

For advanced use cases, use the executor API directly:

```rust
use rivven_connect::connectors::cdc_snapshot::{
    IncrementalSnapshotExecutor, IncrementalSnapshotExecutorConfig, SnapshotChunk
};

let executor = IncrementalSnapshotExecutor::new(config);
executor.initialize().await?;
executor.request_snapshot(&["public.orders"]).await?;

// High-level: execute_chunk convenience method
let events = executor.execute_chunk(
    &chunk,
    watermark_ts,
    || async { execute_query_and_return_rows(&chunk) }
).await?;

// Or low-level: full control over deduplication window
while let Some(chunk) = executor.next_chunk().await? {
    // Open window with watermark
    executor.open_window(&chunk, watermark_ts).await?;
    
    // Execute query and buffer rows
    for row in query_chunk(&chunk).await? {
        executor.buffer_row(&chunk.chunk_id, event, key).await;
    }
    
    // Close and get deduplicated events
    let events = executor.close_window(&chunk.chunk_id).await?;
    emit_events(events).await?;
}

// Monitor parallel execution
let stats = executor.buffer_stats_aggregate().await;
println!("Open windows: {}, Total rows: {}", 
    stats.open_windows, stats.total_rows);
```

### Advanced: Parallel Chunks with Automatic Deduplication

Execute multiple chunks simultaneously with a single API call:

```rust
// Get batch of chunks
let chunks = vec![chunk1, chunk2, chunk3, chunk4];
let watermark_ts = get_db_timestamp().await?;

// Execute ALL chunks in parallel with automatic deduplication
let all_events = executor.execute_chunks_parallel(
    &chunks,
    watermark_ts,
    |chunk| async move {
        let rows = db.query(&build_chunk_query(&chunk)).await?;
        rows.into_iter()
            .map(|r| (r.id.to_string(), CdcEvent::from_row(&r)))
            .collect::<Result<Vec<_>>>()
    }
).await?;
emit_events(all_events).await?;
```

### Advanced: Streaming Event Deduplication

Automatically deduplicate streaming events during incremental snapshots:

```rust
// During streaming while incremental snapshot is active
for event in cdc_stream {
    // Deduplicate streaming events against snapshot buffer
    if executor.is_active() && executor.is_window_open().await {
        executor.process_streaming_event(&event, &["id"]).await;
    }
    
    // Always emit streaming events (they always win)
    emit(event);
}
```

### Advanced: Backpressure Control

Implement automatic backpressure based on memory utilization:

```rust
while let Some(chunk) = executor.next_chunk().await? {
    // Wait if memory pressure is high (>80% utilization)
    while executor.should_throttle().await {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // Monitor utilization
    let utilization = executor.memory_utilization_percent().await;
    println!("Memory utilization: {:.1}%", utilization);
    
    executor.execute_chunk(&chunk, watermark_ts, query_fn).await?;
}
```

### Best Practices

1. **Keep chunk_size reasonable** (1024-4096 rows) to minimize watermark window
2. **Use `insert_delete` strategy** for better compatibility
3. **Monitor chunk processing** via metrics
4. **Test signal table setup** before production deployment
5. **Avoid very large tables** for incremental snapshots — use initial snapshot instead

---

## Event Routing

Route CDC events to different destinations based on content, table, operation, or custom conditions.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      router:
        enabled: true
        default_destination: default-topic
        dead_letter_queue: dlq-topic      # Unroutable events go here
        drop_unroutable: false            # Or drop silently
        rules:
          - name: high_priority
            priority: 100
            condition:
              type: And
              conditions:
                - type: Table
                  table: public.orders
                - type: FieldValue
                  field: priority
                  value: high
            destinations: [priority-orders]
            continue_matching: false      # Stop on first match
            
          - name: customer_changes
            priority: 50
            condition:
              type: TablePattern
              pattern: "public\\.customer.*"
            destinations: [customer-events]
            
          - name: deletes_audit
            priority: 10
            condition:
              type: Operation
              op: delete
            destinations: [audit-topic, delete-archive]
```

### Route Conditions

| Condition | Description |
|:----------|:------------|
| `Always` | Always matches |
| `Table` | Match specific table name |
| `TablePattern` | Match table via regex |
| `Schema` | Match database schema |
| `Operation` | Match op type (insert/update/delete) |
| `FieldExists` | Check if field exists |
| `FieldValue` | Match field to specific value |
| `FieldPattern` | Match field via regex |
| `Header` | Match event header value |
| `And` | Combine multiple conditions with AND |
| `Or` | Combine multiple conditions with OR |
| `Not` | Negate a condition |

### Routing Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Event Routing Flow                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Event arrives from CDC stream                                            │
│  2. Evaluate rules in priority order (highest first)                         │
│  3. For each matching rule:                                                  │
│     • Add destinations to route list                                         │
│     • If continue_matching=false, stop evaluation                            │
│  4. If no rules match:                                                       │
│     • Route to default_destination (if configured)                           │
│     • Or route to dead_letter_queue                                          │
│     • Or drop if drop_unroutable=true                                        │
│  5. Emit event to all collected destinations                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Partitioning

Control how events are distributed across topic partitions for ordering and parallelism.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      partitioner:
        enabled: true
        num_partitions: 16
        strategy:
          type: KeyHash           # Hash primary key
          # Or:
          # type: TableHash       # Hash table name
          # type: FullTableHash   # Hash full table path (schema.table)
          # type: RoundRobin      # Distribute evenly
          # type: Sticky          # Same partition per batch
```

### Partition Strategies

| Strategy | Description | Use Case |
|:---------|:------------|:---------|
| `RoundRobin` | Distribute events evenly | Maximum parallelism |
| `KeyHash` | Hash primary key | Maintain per-row ordering |
| `TableHash` | Hash table name | Keep table events together |
| `FullTableHash` | Hash schema.table | Multi-schema environments |
| `Sticky` | Same partition per batch | Batch locality |

### Best Practices

1. **Use `KeyHash`** when consumers need per-key ordering
2. **Use `TableHash`** when consumers process entire tables
3. **Set `num_partitions`** to match consumer parallelism
4. **Monitor partition distribution** via metrics

---

## Pipeline Processing

Build composable CDC processing pipelines with stages for filtering, transformation, and routing.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      pipeline:
        enabled: true
        name: order-processing
        dead_letter_queue: dlq-topic
        concurrency: 4
        stages:
          - type: Filter
            condition:
              type: Operation
              op: delete
            
          - type: Transform
            transforms:
              - type: ExtractNewRecordState
              - type: MaskField
                config:
                  fields: [credit_card]
                  
          - type: Route
            rules:
              - condition:
                  type: FieldValue
                  field: status
                  value: urgent
                destinations: [urgent-orders]
```

### Pipeline Stages

| Stage | Description |
|:------|:------------|
| `Filter` | Drop events matching condition |
| `Transform` | Apply SMT transforms |
| `Route` | Content-based routing |

### Pipeline Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│  Event → [Filter] → [Transform] → [Route] → Destination                      │
│                                                                              │
│  If any stage fails:                                                         │
│    • Event goes to dead_letter_queue (if configured)                         │
│    • Processing continues with next event                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Log Compaction

Reduce storage and replay time by keeping only the latest state per key.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      compaction:
        enabled: true
        key_columns: [id]            # Columns forming the compaction key
        min_cleanable_ratio: 0.5     # Trigger compaction at 50% duplicates
        segment_size: 104857600      # 100MB segments
        delete_retention_ms: 86400000 # Keep tombstones for 24 hours
        min_compaction_lag_ms: 0     # Minimum age before compaction
        max_compaction_lag_ms: 0     # Force compaction after this age
        cleanup_policy: compact      # compact, delete, or compact_delete
```

### Compaction Strategies

| Strategy | Description |
|:---------|:------------|
| `compact` | Keep latest value per key |
| `delete` | Delete segments after retention |
| `compact_delete` | Compact, then delete after retention |

### Key Strategy

| Key Strategy | Description |
|:-------------|:------------|
| `PrimaryKey` | Use table's primary key |
| `AllColumns` | Hash all columns |
| `CustomColumns` | Specify columns via `key_columns` |

---

## Parallel CDC Processing

Process multiple tables concurrently for maximum throughput.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      parallel:
        enabled: true
        concurrency: 8               # Max concurrent table streams
        per_table_buffer: 1000       # Events to buffer per table
        work_stealing: true          # Rebalance work across threads
        backpressure_threshold: 0.8  # Throttle at 80% buffer utilization
        batch_timeout_ms: 100        # Max time to accumulate batch
```

### How It Works

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Parallel CDC Processing                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐                      │
│  │ Table A │   │ Table B │   │ Table C │   │ Table D │                      │
│  └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘                      │
│       │             │             │             │                            │
│       ▼             ▼             ▼             ▼                            │
│  ┌─────────────────────────────────────────────────────────────┐            │
│  │                   Worker Pool (concurrency=8)                │            │
│  │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐   │            │
│  │  │ W1 │ │ W2 │ │ W3 │ │ W4 │ │ W5 │ │ W6 │ │ W7 │ │ W8 │   │            │
│  │  └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘   │            │
│  └─────────────────────────────────────────────────────────────┘            │
│                              │                                               │
│                              ▼                                               │
│                    ┌──────────────────┐                                     │
│                    │   Merged Output   │                                     │
│                    └──────────────────┘                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Work Stealing

When `work_stealing: true`, idle workers steal work from busy workers:

```text
Worker 1: [████████████████████] ← heavy table
Worker 2: [████]                 ← light table, steals from Worker 1
Worker 3: [██████]               ← medium table
Worker 4: [██]                   ← light table, steals from Worker 1
```

---

## Transactional Outbox Pattern

Reliably publish events from your application using the transactional outbox pattern.

### Configuration

```yaml
sources:
  orders_db:
    connector: postgres-cdc
    config:
      # ... connection config ...
      
      outbox:
        enabled: true
        table_name: outbox          # Outbox table name
        poll_interval_ms: 1000      # Polling interval
        batch_size: 100             # Events per batch
        max_retries: 3              # Retry failed events
        retry_delay_ms: 1000        # Delay between retries
        delete_after_publish: true  # Clean up processed events
```

### Outbox Table Schema

```sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,    -- e.g., "Order", "Customer"
    aggregate_id VARCHAR(255) NOT NULL,      -- Business key
    event_type VARCHAR(255) NOT NULL,        -- e.g., "OrderCreated"
    payload JSONB NOT NULL,                  -- Event data
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    published BOOLEAN DEFAULT FALSE,
    retries INTEGER DEFAULT 0
);

-- Index for efficient polling
CREATE INDEX idx_outbox_unpublished ON outbox (timestamp) WHERE NOT published;
```

### How It Works

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Transactional Outbox Flow                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Application                      Rivven CDC                                 │
│  ┌─────────────────────┐         ┌─────────────────────┐                    │
│  │  BEGIN TRANSACTION  │         │                     │                    │
│  │  INSERT INTO orders │         │  1. Poll outbox     │                    │
│  │  INSERT INTO outbox │◄────────│  2. Read events     │                    │
│  │  COMMIT             │         │  3. Publish to topic│                    │
│  └─────────────────────┘         │  4. Mark published  │                    │
│                                  │  5. (Optional) Delete│                    │
│                                  └─────────────────────┘                    │
│                                                                              │
│  Benefits:                                                                   │
│  • Atomic: event inserted in same TX as business data                       │
│  • Reliable: no events lost if app crashes after commit                     │
│  • Ordered: events processed in timestamp order                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Best Practices

1. **Use CDC** on the outbox table for lowest latency
2. **Set reasonable `batch_size`** to balance throughput and latency
3. **Enable `delete_after_publish`** to prevent table bloat
4. **Add index** on unpublished events for efficient polling
5. **Monitor outbox lag** to detect publishing issues

---

## Transforms

Rivven provides 17 built-in Single Message Transforms (SMTs) for in-flight data manipulation.

### Transform Reference

| Transform | Description |
|:----------|:------------|
| `ExtractNewRecordState` | Flatten envelope to just `after` state |
| `MaskField` | Redact sensitive fields |
| `ReplaceField` | Rename, include, or exclude fields |
| `InsertField` | Add static or computed fields |
| `Filter` | Drop events based on condition |
| `Cast` | Convert field types |
| `Flatten` | Flatten nested structures |
| `TimestampConverter` | Convert timestamp formats |
| `TimezoneConverter` | Convert between timezones |
| `RegexRouter` | Route based on regex patterns |
| `ContentRouter` | Route based on field values |
| `ValueToKey` | Extract key fields from value |
| `HeaderToValue` | Move envelope fields into record |
| `Unwrap` | Extract nested field to top level |
| `ComputeField` | Compute new fields |
| `SetNull` | Conditionally nullify fields |
| `ConditionalSmt` | Apply transforms conditionally |

### Example: Flatten and Mask

```yaml
sources:
  orders:
    connector: postgres-cdc
    topic: cdc.orders
    transforms:
      - type: ExtractNewRecordState
      - type: MaskField
        config:
          fields: [credit_card, ssn]
          mask_char: "*"
      - type: ReplaceField
        config:
          exclude: [internal_notes]
```

### Example: Content-Based Routing

```yaml
transforms:
  - type: ContentRouter
    config:
      field: priority
      routes:
        high: priority-orders
        normal: standard-orders
      default: other-orders
```

---

## Sinks

Route CDC events to various destinations.

### Console (stdout)

```yaml
sinks:
  console:
    connector: stdout
    topics: [cdc.orders]
    consumer_group: console-sink
    config:
      format: json
      pretty: true
```

### S3 / Data Lake

```yaml
sinks:
  data_lake:
    connector: s3
    topics: [cdc.orders]
    consumer_group: s3-sink
    config:
      bucket: my-data-lake
      prefix: cdc/orders
      region: us-east-1
      format: jsonl
      partition_by: day
      compression: gzip
      batch_size: 1000
```

### HTTP Webhook

```yaml
sinks:
  webhook:
    connector: http-webhook
    topics: [cdc.orders]
    consumer_group: webhook-sink
    config:
      url: https://api.example.com/events
      method: POST
      headers:
        Authorization: "Bearer ${API_TOKEN}"
      batch_size: 100
      timeout_secs: 30
```

### Snowflake

```yaml
sinks:
  warehouse:
    connector: snowflake
    topics: [cdc.orders]
    consumer_group: snowflake-sink
    config:
      account: myorg-account123
      user: RIVVEN_USER
      private_key_path: /path/to/rsa_key.p8
      database: MY_DATABASE
      schema: MY_SCHEMA
      table: MY_TABLE
      batch_size: 1000
```

---

## Rate Limiting

Prevent overwhelming downstream systems:

```yaml
sinks:
  s3:
    connector: s3
    rate_limit:
      events_per_second: 10000
      burst_capacity: 1000
    config:
      # ...
```

---

## Monitoring

Rivven CDC provides comprehensive Prometheus metrics for full observability.

### Core Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_cdc_events_total` | Total events processed |
| `rivven_cdc_lag_milliseconds` | Replication lag |
| `rivven_cdc_errors_total` | Error count |
| `rivven_cdc_connected` | Connection status (1/0) |

### Extended Metrics

| Category | Example Metrics |
|:---------|:----------------|
| **Snapshot** | `snapshot_running`, `snapshot_duration_ms`, `snapshot_rows_scanned` |
| **Streaming** | `streaming_lag_ms`, `create_events`, `update_events`, `delete_events` |
| **Incremental Snapshot** | `incremental_snapshot_chunks_processed`, `incremental_snapshot_rows_captured` |
| **Performance** | `processing_time_p99_us`, `average_batch_size`, `batches_processed` |

See [Troubleshooting Guide](cdc-troubleshooting) for alert rules and health checks.

### Example Prometheus Alerts

```yaml
groups:
- name: rivven-cdc
  rules:
  - alert: CDCLagHigh
    expr: rivven_cdc_lag_milliseconds > 10000
    for: 5m
    labels:
      severity: warning

  - alert: CDCDisconnected
    expr: rivven_cdc_connected == 0
    for: 1m
    labels:
      severity: critical
```

---

## Next Steps

- [PostgreSQL CDC Guide](cdc-postgres) — Complete PostgreSQL setup
- [MySQL/MariaDB CDC Guide](cdc-mysql) — MySQL and MariaDB setup
- [Configuration Reference](cdc-configuration) — All configuration options
- [Troubleshooting Guide](cdc-troubleshooting) — Debug common issues
- [Connectors](connectors) — All connector configurations
- [Security](security) — TLS and authentication
- [Kubernetes](kubernetes) — Production deployment
