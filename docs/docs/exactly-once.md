---
layout: default
title: Exactly-Once Semantics
nav_order: 10
---

# Exactly-Once Semantics
{: .no_toc }

Guaranteed delivery without duplicates.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven provides exactly-once semantics through two complementary features:

1. **Idempotent Producer** — Exactly-once delivery within a single topic
2. **Native Transactions** — Atomic writes across multiple topics

---

## Idempotent Producer

The idempotent producer eliminates duplicate messages during retries without requiring full transactions.

### How It Works

```text
Producer                           Broker
   │                                  │
   │─── InitProducerId ──────────────>│  (Request PID)
   │<── PID=123, Epoch=0 ─────────────│
   │                                  │
   │─── Produce(PID=123,Seq=0) ──────>│  (First message)
   │<── Success(offset=0) ────────────│
   │                                  │
   │─── Produce(PID=123,Seq=0) ──────>│  (Retry - duplicate!)
   │<── DuplicateSequence(offset=0) ──│  (Returns cached offset)
   │                                  │
   │─── Produce(PID=123,Seq=1) ──────>│  (Next message)
   │<── Success(offset=1) ────────────│
```

### Key Concepts

- **Producer ID (PID)**: Unique 64-bit identifier for each producer instance
- **Epoch**: Increments on producer restart, fencing old instances
- **Sequence Number**: Per-partition counter starting at 0
- **Offset Cache**: The broker maintains a ring buffer of the 5 most recent
  `(sequence, offset)` pairs per producer per partition. When a duplicate is
  detected, the cached offset is returned to the client so it knows the exact
  position of the original write — no second lookup required.

### Protocol

```rust
// Initialize producer (get PID and epoch)
Request::InitProducerId { producer_id: None }
Response::ProducerIdInitialized { producer_id: 123, producer_epoch: 0 }

// Publish with idempotent semantics
Request::IdempotentPublish {
    topic: "orders".to_string(),
    partition: Some(0),
    key: Some(key_bytes),
    value: message_bytes,
    producer_id: 123,
    producer_epoch: 0,
    sequence: 0,
    leader_epoch: None,
}
Response::IdempotentPublished { offset: 42, partition: 0, duplicate: false }
```

### Producer Fencing

When a producer restarts, it receives an incremented epoch. Any messages from the old instance (with lower epoch) are rejected:

```text
Producer A (Epoch=0)    starts producing
Producer A crashes
Producer A restarts    → InitProducerId → Epoch=1
Old instance (Epoch=0) → Produce → PRODUCER_FENCED error
New instance (Epoch=1) → Produce → Success
```

### Error Handling

| Error | Cause | Action |
|-------|-------|--------|
| `OUT_OF_ORDER_SEQUENCE` | Gap in sequence numbers | Retry from last known sequence |
| `PRODUCER_FENCED` | Epoch too old | Re-initialize producer |
| `UNKNOWN_PRODUCER_ID` | PID not initialized | Call InitProducerId first |

---

## Native Transactions

Transactions provide atomicity across multiple topics and partitions, enabling exactly-once semantics for consume-transform-produce patterns.

### Durability & Crash Recovery

All transaction state transitions are persisted to a **CRC32-protected write-ahead log** (WAL) **before** modifying in-memory state. This strict WAL-before-memory ordering ensures that every acknowledged state transition is recoverable after a crash. The WAL captures:

- `Begin`, `AddPartition`, `RecordWrite` — written before the coordinator updates in-memory maps
- `OffsetCommit` — consumer offset commits for exactly-once consume-transform-produce
- `PrepareCommit` / `PrepareAbort` — the 2PC decision is durable before Phase 2
- `CompleteCommit` / `CompleteAbort` — final resolution logged before cleanup
- `TimedOut` — zombie cleanup recorded before removing the transaction from memory

**On WAL write failure**, in-memory state is never modified — the coordinator returns `TransactionResult::LogWriteError` to the caller, keeping WAL and in-memory state consistent. For multi-partition `AddPartition` writes, if any partition's WAL entry fails, none are added to in-memory state (all-or-nothing).

**On startup**, `TransactionCoordinator::recover(path)` replays the WAL sequentially. Entries after the first CRC mismatch are truncated (Kafka-style). In-doubt transactions (state = `PrepareCommit` or `PrepareAbort`) are left for the operator to resolve, with a warning logged. Completed or aborted transactions are replayed into the `AbortedTransactionIndex` for correct `read_committed` filtering.

### Data Integrity Guarantees

1. **Validate-before-write**: `TransactionalPublish` validates partition membership in the transaction BEFORE appending data to the partition log. This prevents orphaned records if the partition wasn't added via `AddPartitionsToTxn`.

2. **Atomic COMMIT markers**: If any COMMIT marker write fails, compensating ABORT markers are written to all partitions that already received COMMIT markers, then the transaction is aborted. This prevents `read_committed` consumers from seeing partial commits.

3. **ABORT marker failure returns error**: When any ABORT marker write fails, the broker returns an `ABORT_PARTIAL_FAILURE` error to the client instead of a success response. The error includes the affected partition list so clients know which partitions may expose uncommitted data under `read_committed` isolation.

### Transaction Protocol

```text
Producer                     Transaction Coordinator            Partitions
   │                                   │                            │
   │─── InitProducerId ───────────────>│                            │
   │<── PID=123, Epoch=0 ──────────────│                            │
   │                                   │                            │
   │─── BeginTransaction(TxnId) ──────>│                            │
   │<── OK ────────────────────────────│                            │
   │                                   │                            │
   │─── AddPartitionsToTxn(p1,p2) ────>│                            │
   │<── OK ────────────────────────────│                            │
   │                                   │                            │
   │─── TransactionalPublish(p1) ──────────────────────────────────>│
   │<── OK ─────────────────────────────────────────────────────────│
   │                                   │                            │
   │─── TransactionalPublish(p2) ──────────────────────────────────>│
   │<── OK ─────────────────────────────────────────────────────────│
   │                                   │                            │
   │─── CommitTransaction(TxnId) ─────>│                            │
   │                                   │─── WriteTxnMarker(COMMIT) ─>│
   │                                   │<── OK ─────────────────────│
   │<── OK ────────────────────────────│                            │
```

### Transaction States

```text
Empty ──────> Ongoing ──────> PrepareCommit ──────> CompleteCommit
                 │                  │                     │
                 │                  v                     v
                 └───────> PrepareAbort ───────> CompleteAbort
```

**Limits:** The broker enforces a maximum of 5 concurrent pending transactions per producer to prevent resource exhaustion. Exceeding this limit returns `TooManyTransactions`.

### API Reference

#### Begin Transaction

```rust
Request::BeginTransaction {
    txn_id: "order-processing-txn-1".to_string(),
    producer_id: 123,
    producer_epoch: 0,
    timeout_ms: Some(60000),  // Optional, defaults to 60s
}
Response::TransactionStarted { txn_id: "order-processing-txn-1".to_string() }
```

#### Add Partitions to Transaction

```rust
Request::AddPartitionsToTxn {
    txn_id: "order-processing-txn-1".to_string(),
    producer_id: 123,
    producer_epoch: 0,
    partitions: vec![
        ("orders".to_string(), 0),
        ("inventory".to_string(), 0),
        ("payments".to_string(), 0),
    ],
}
Response::PartitionsAddedToTxn { 
    txn_id: "order-processing-txn-1".to_string(),
    partition_count: 3,
}
```

#### Transactional Publish

```rust
Request::TransactionalPublish {
    txn_id: "order-processing-txn-1".to_string(),
    topic: "orders".to_string(),
    partition: Some(0),
    key: Some(key_bytes),
    value: message_bytes,
    producer_id: 123,
    producer_epoch: 0,
    sequence: 0,
    leader_epoch: None,
}
Response::TransactionalPublished { offset: 100, partition: 0, sequence: 0 }
```

#### Add Consumer Offsets to Transaction

For exactly-once consume-transform-produce patterns:

```rust
Request::AddOffsetsToTxn {
    txn_id: "order-processing-txn-1".to_string(),
    producer_id: 123,
    producer_epoch: 0,
    group_id: "order-processor-group".to_string(),
    offsets: vec![
        ("input-orders".to_string(), 0, 42),  // topic, partition, offset
    ],
}
Response::OffsetsAddedToTxn { txn_id: "order-processing-txn-1".to_string() }
```

#### Commit Transaction

```rust
Request::CommitTransaction {
    txn_id: "order-processing-txn-1".to_string(),
    producer_id: 123,
    producer_epoch: 0,
}
Response::TransactionCommitted { txn_id: "order-processing-txn-1".to_string() }
```

#### Abort Transaction

```rust
Request::AbortTransaction {
    txn_id: "order-processing-txn-1".to_string(),
    producer_id: 123,
    producer_epoch: 0,
}
Response::TransactionAborted { txn_id: "order-processing-txn-1".to_string() }
```

### Consumer Isolation Levels

Rivven supports standard isolation levels for consumers:

| Level | Value | Behavior |
|-------|-------|----------|
| `read_uncommitted` | 0 | Read all data messages (default for backward compatibility) |
| `read_committed` | 1 | Only read committed transactional messages |

#### Read Uncommitted (Default)

The default isolation level returns all messages except control records (transaction markers).
This is backward compatible with non-transactional consumers:

```rust
// Default consume - read_uncommitted
let messages = client.consume("topic", 0, 0, 100).await?;
```

#### Read Committed

For exactly-once semantics, transactional consumers should use `read_committed` isolation.
This filters out:

1. **Control records** (COMMIT/ABORT transaction markers)
2. **Messages from aborted transactions**

```rust
// Explicit read_committed for transactional consumers
let messages = client.consume_read_committed("topic", 0, 0, 100).await?;

// Or with explicit isolation level parameter
let messages = client.consume_with_isolation("topic", 0, 0, 100, Some(1)).await?;
```

#### How Read Committed Filtering Works

```text
Partition Log:
┌─────┬─────────────────────────────────────────────┐
│ Off │ Message                                     │
├─────┼─────────────────────────────────────────────┤
│  0  │ [PID=1] "order-1" (TXN started)             │ ← included
│  1  │ [PID=2] "order-2" (TXN started)             │ ← FILTERED (aborted)
│  2  │ [PID=1] "order-1-update"                    │ ← included
│  3  │ [PID=2] ABORT marker                        │ ← FILTERED (control)
│  4  │ [PID=1] COMMIT marker                       │ ← FILTERED (control)
│  5  │ [PID=3] "order-3" (non-transactional)       │ ← included
└─────┴─────────────────────────────────────────────┘

read_uncommitted returns: 0, 1, 2, 5 (all data messages)
read_committed returns: 0, 2, 5 (filters aborted PID=2 messages)
```

#### AbortedTransactionIndex

The broker maintains an `AbortedTransactionIndex` per partition that tracks:

- **Producer ID** that aborted
- **First offset** of the aborted transaction
- **Last offset** of the aborted transaction

The index uses **bounded offset ranges** (`first_offset..=last_offset`) so that only
messages actually written by the aborted transaction are filtered. This prevents
false positives where later messages from the same producer would have been
incorrectly hidden under the previous unbounded scheme.

When a consumer requests `read_committed`, the broker cross-references transactional
messages against this index to filter out aborted transaction data.

### Transaction Timeout

Transactions have a configurable timeout (default 60 seconds). If not committed within this window:

1. Transaction is marked as `Dead`
2. Subsequent operations fail with `TRANSACTION_TIMED_OUT`
3. Transaction coordinator cleans up state

### Error Handling

| Error | Cause | Action |
|-------|-------|--------|
| `INVALID_TXN_ID` | Transaction not found | Begin new transaction |
| `INVALID_TXN_STATE` | Wrong state for operation | Check transaction state |
| `CONCURRENT_TRANSACTIONS` | Producer has active txn | Commit/abort existing |
| `PARTITION_NOT_IN_TXN` | Partition not registered | Call AddPartitionsToTxn |
| `TRANSACTION_TIMED_OUT` | Timeout exceeded | Begin new transaction |
| `PRODUCER_FENCED` | Epoch too old | Re-initialize producer |
| `ABORT_PARTIAL_FAILURE` | ABORT markers failed on some partitions | Retry abort or check affected partitions |

---

## Best Practices

### 1. Use Idempotent Producer by Default

Always enable idempotent producer for production workloads:

```rust
// Initialize once at startup
let response = client.request(Request::InitProducerId { producer_id: None }).await?;
let (pid, epoch) = match response {
    Response::ProducerIdInitialized { producer_id, producer_epoch } => 
        (producer_id, producer_epoch),
    _ => return Err("Failed to init producer"),
};

// Use for all publishes
client.request(Request::IdempotentPublish {
    topic: "events".to_string(),
    partition: None,
    key,
    value,
    producer_id: pid,
    producer_epoch: epoch,
    sequence: get_next_sequence(),
    leader_epoch: None,
}).await?;
```

### 2. Transaction Boundaries

Keep transactions small and fast:

- Process single input message per transaction
- Avoid external I/O within transactions
- Set appropriate timeouts

### 3. Exactly-Once Consume-Transform-Produce

```rust
// 1. Consume from input topic
let messages = consume("input-topic", partition, offset, max_messages).await?;

// 2. Begin transaction
begin_transaction(txn_id, producer_id, epoch).await?;

// 3. Register all output partitions
add_partitions_to_txn(txn_id, &output_partitions).await?;

// 4. Process and publish
for msg in messages {
    let output = transform(msg);
    transactional_publish(txn_id, "output-topic", output).await?;
}

// 5. Commit consumer offset within transaction
add_offsets_to_txn(txn_id, group_id, &consumed_offsets).await?;

// 6. Commit everything atomically
commit_transaction(txn_id).await?;
```

### 4. Handle Producer Fencing

```rust
loop {
    match transactional_publish(/* ... */).await {
        Ok(_) => break,
        Err(e) if e.contains("PRODUCER_FENCED") => {
            // Re-initialize producer
            let (new_pid, new_epoch) = init_producer().await?;
            // Retry with new credentials
        }
        Err(e) => return Err(e),
    }
}
```

---

## Authorization

Transaction operations require `IdempotentWrite` permission on `Cluster`:

```rust
// ACL for transactional producer
AclEntry {
    principal: "user:producer",
    resource: ResourceType::Cluster,
    permission: Permission::IdempotentWrite,
    action: AclAction::Allow,
}
```

Additionally, `TransactionalPublish` requires `Write` permission on the target topic.

---

## Monitoring

### Metrics

| Metric | Description |
|--------|-------------|
| `rivven_transactions_started_total` | Total transactions initiated |
| `rivven_transactions_committed_total` | Total transactions committed |
| `rivven_transactions_aborted_total` | Total transactions aborted |
| `rivven_transactions_timed_out_total` | Total transactions timed out |
| `rivven_transactions_active` | Currently active transactions |
| `rivven_idempotent_producers_active` | Active idempotent producers |
| `rivven_idempotent_duplicates_total` | Duplicate messages detected |

### Transaction Coordinator Stats

```rust
let stats = transaction_coordinator.stats();
println!("Started: {}", stats.transactions_started());
println!("Committed: {}", stats.transactions_committed());
println!("Aborted: {}", stats.transactions_aborted());
println!("Timed out: {}", stats.transactions_timed_out());
println!("Active: {}", stats.active_transactions());
```

---


