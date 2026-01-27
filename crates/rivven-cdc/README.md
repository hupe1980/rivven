# Rivven CDC

Native Change Data Capture for PostgreSQL, MySQL, and MariaDB.

## Features

- 🚀 **Native Implementation** - Direct TCP connections, no external dependencies
- 🐘 **PostgreSQL** - Logical replication via pgoutput plugin (v10+)
- 🐬 **MySQL/MariaDB** - Binlog replication with GTID support (MySQL 5.7+, MariaDB 10.2+)
- 🔒 **TLS/mTLS** - Secure connections with optional client certificate auth
- 📦 **Zero-Copy** - Efficient binary protocol parsing
- ⚡ **Async** - Built on Tokio for high-performance streaming
- 📡 **Signal Table** - Runtime control with Debezium-compatible signaling
- 🔄 **Incremental Snapshots** - Re-snapshot tables while streaming continues

## Debezium Comparison

Rivven-cdc provides ~100% feature parity with Debezium:

| Feature | Rivven-cdc | Debezium | Notes |
|---------|------------|----------|-------|
| Logical Replication | ✅ | ✅ | pgoutput plugin |
| Binlog Streaming | ✅ | ✅ | MySQL/MariaDB GTID |
| Initial Snapshot | ✅ | ✅ | Parallel, resumable |
| TLS/SSL | ✅ | ✅ | rustls-based |
| Table/Column Filtering | ✅ | ✅ | Glob patterns |
| Column Masking | ✅ | ✅ | Redacted |
| Heartbeats | ✅ | ✅ | WAL acknowledgment |
| Heartbeat Action Query | ✅ | ✅ | Multi-database support |
| Checkpointing | ✅ | ✅ | LSN/binlog position |
| Schema Inference | ✅ | ✅ | Avro schema generation |
| **Tombstone Events** | ✅ | ✅ | `TombstoneEmitter` for log compaction |
| **REPLICA IDENTITY** | ✅ | ✅ | `ReplicaIdentityEnforcer` with warn/skip/fail |
| **Schema Change Topic** | ✅ | ✅ | `SchemaChangeEmitter` for DDL tracking |
| **SCRAM-SHA-256** | ✅ | ✅ | RFC 5802 PostgreSQL auth |
| **Signal Table** | ✅ | ✅ | Multi-channel (source/topic/file) |
| **Transaction Metadata Topic** | ✅ | ✅ | `TransactionTopicEmitter` BEGIN/END events |
| **Read-Only Replicas** | ✅ | ✅ | Heartbeat-based watermarking for standbys |
| **Incremental Snapshots** | ✅ | ✅ | DBLog watermarks, chunk-based |
| Circuit Breaker | ✅ | - | Rivven advantage |
| Rate Limiting | ✅ | - | Token bucket algorithm |

### Signal Table (Runtime Control)

Control CDC connectors at runtime without restarting - trigger snapshots, pause/resume streaming:

```rust
use rivven_cdc::common::signal::{SignalConfig, SignalChannelType};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};

// Configure signal table via CDC stream (default)
let signal_config = SignalConfig::builder()
    .enabled_channels(vec![SignalChannelType::Source])
    .signal_data_collection("public.debezium_signal")
    .build();

let config = PostgresCdcConfig::builder()
    .connection_string("postgres://user:pass@localhost/db")
    .slot_name("rivven_slot")
    .publication_name("rivven_pub")
    .signal_config(signal_config)
    .build()?;

let mut cdc = PostgresCdc::new(config);

// Register custom signal handler
cdc.signal_processor()
    .register_handler("execute-snapshot", |signal| async move {
        println!("Snapshot requested for: {:?}", signal.data.data_collections);
        rivven_cdc::common::SignalResult::Success
    })
    .await;

cdc.start().await?;
```

**Signal Types:**

| Signal | Description |
|--------|-------------|
| `execute-snapshot` | Trigger ad-hoc incremental/blocking snapshot |
| `stop-snapshot` | Stop in-progress snapshot |
| `pause-snapshot` | Pause streaming |
| `resume-snapshot` | Resume streaming |
| `log` | Log a diagnostic message |

**Signal Channels:**

| Channel | Description |
|---------|-------------|
| `source` | Signal table captured via CDC stream (default, recommended) |
| `topic` | Signals from Rivven/Kafka topic |
| `file` | Signals from JSON file |

### Incremental Snapshots

Re-snapshot tables while streaming continues using DBLog-style watermark deduplication:

```rust
use rivven_cdc::common::{
    IncrementalSnapshotConfig, IncrementalSnapshotCoordinator,
    IncrementalSnapshotRequest, WatermarkStrategy
};

// Configure incremental snapshots
let config = IncrementalSnapshotConfig::builder()
    .chunk_size(1024)  // Rows per chunk
    .watermark_strategy(WatermarkStrategy::InsertInsert)
    .max_buffer_memory(64 * 1024 * 1024)  // 64MB buffer
    .signal_table("debezium_signal")
    .build();

let coordinator = IncrementalSnapshotCoordinator::new(config);

// Start incremental snapshot for tables
let request = IncrementalSnapshotRequest::new(vec![
    "public.orders".to_string(),
    "public.order_items".to_string(),
])
.with_condition("public.orders", "created_at >= '2024-01-01'")
.with_surrogate_key("public.orders", "order_id");

let snapshot_id = coordinator.start(request).await?;

// Process chunks with watermark-based deduplication
while let Some(chunk) = coordinator.next_chunk().await? {
    // Open deduplication window
    let open_signal = coordinator.open_window_signal(&chunk);
    // INSERT into signal table: open_signal.to_insert_sql("debezium_signal")
    
    coordinator.open_window(&chunk).await;
    
    // Execute chunk query and buffer results
    // for row in execute_query(&chunk.to_sql(1024)) {
    //     coordinator.buffer_row(cdc_event, primary_key).await;
    // }
    
    // Close window - returns events that weren't superseded by streaming
    let events = coordinator.close_window().await?;
    let close_signal = coordinator.close_window_signal(&chunk);
    // INSERT into signal table: close_signal.to_insert_sql("debezium_signal")
    
    // Emit remaining snapshot events as READ operations
    for event in events {
        // emit_event(event).await;
    }
}

// Check statistics
let stats = coordinator.stats();
println!("Rows: {}, Dropped: {}", stats.rows_snapshotted, stats.events_dropped);
```

**Watermark Deduplication:**

During the window, streaming events with matching primary keys cause buffered snapshot entries to be dropped:

```rust
// While window is open, check streaming events for conflicts
let key = "123";  // Primary key from streaming event
if coordinator.check_streaming_conflict("public.orders", key).await {
    // Streaming event supersedes snapshot - buffer entry dropped
}
```

### Tombstone Events

Tombstones are emitted after DELETE events for Kafka log compaction:

```rust
use rivven_cdc::common::tombstone::{TombstoneConfig, TombstoneEmitter};
use rivven_cdc::CdcEvent;

// Enable tombstones (default)
let config = TombstoneConfig::default();
let emitter = TombstoneEmitter::new(config);

// Process events - DELETE will be followed by a TOMBSTONE
let delete = CdcEvent::delete("pg", "db", "public", "users", serde_json::json!({"id": 1}), 1000);
let events = emitter.process(delete);
// events = [DELETE, TOMBSTONE]
```

### REPLICA IDENTITY Enforcement

Ensure PostgreSQL tables have proper REPLICA IDENTITY for complete before images:

```rust
use rivven_cdc::common::replica_identity::{
    ReplicaIdentity, ReplicaIdentityConfig, ReplicaIdentityEnforcer, EnforcementMode
};

// Warn when tables don't have REPLICA IDENTITY FULL
let config = ReplicaIdentityConfig::builder()
    .require_full(true)
    .enforcement_mode(EnforcementMode::Warn)
    .exclude_table("audit.*")  // Skip audit tables
    .build();

let enforcer = ReplicaIdentityEnforcer::new(config);

// Check table identity (from RELATION message)
let result = enforcer.check_sync("public", "users", ReplicaIdentity::Default);
// Logs: Table "public"."users" has REPLICA IDENTITY DEFAULT (not FULL).
//       Fix with: ALTER TABLE "public"."users" REPLICA IDENTITY FULL
```

### Schema Change Topic

Publish DDL events to a dedicated topic for downstream schema synchronization:

```rust
use rivven_cdc::common::{
    SchemaChangeConfig, SchemaChangeEmitter, ColumnDefinition
};

// Configure schema change publishing
let config = SchemaChangeConfig::builder()
    .enabled(true)
    .topic_prefix("mydb")  // -> mydb.schema_changes
    .include_ddl(true)     // Include DDL SQL
    .include_columns(true) // Include column details
    .exclude_tables(vec!["temp_*".to_string()])
    .build();

let emitter = SchemaChangeEmitter::new(config);

// Detect schema changes from PostgreSQL RELATION messages
let columns = vec![
    ColumnDefinition::new("id", "integer", 1).with_primary_key(true),
    ColumnDefinition::new("name", "text", 2),
];
let event = emitter.detect_postgres_change(
    "mydb", "public", "users", 12345, columns, Some("0/16B3748")
).await;

// Or detect from MySQL DDL queries
let event = emitter.detect_mysql_change(
    "mydb", "ALTER TABLE users ADD COLUMN email VARCHAR(255)", Some("mysql-bin.000001:12345")
).await;
```

### Transaction Metadata Topic

Publish transaction BEGIN/END markers to a dedicated topic for downstream correlation:

```rust
use rivven_cdc::common::{
    TransactionTopicConfig, TransactionTopicEmitter, TransactionMetadata
};
use rivven_cdc::CdcEvent;

// Configure transaction metadata publishing
let config = TransactionTopicConfig::builder()
    .enabled(true)
    .topic_suffix("transaction")    // -> cdc.orders.transaction
    .enrich_events(true)            // Add tx context to events
    .build();

let emitter = TransactionTopicEmitter::new(config);

// Begin a transaction
emitter.begin_transaction("txid:1234567", 1737802200000);

// Process events within the transaction
let event = CdcEvent::insert(
    "pg", "db", "public", "orders",
    serde_json::json!({"id": 1}),
    1737802200100
);
let tx_meta = TransactionMetadata::new("txid:1234567", "public.orders");

// Enrich event with transaction context (total_order, data_collection_order)
if let Some(enriched) = emitter.enrich_event(&event, &tx_meta) {
    // enriched.transaction = {id: "txid:1234567", total_order: 1, data_collection_order: 1}
}

// End transaction - returns END event with per-table counts
if let Some(end_event) = emitter.end_transaction("txid:1234567", 1737802200500) {
    // end_event.event_count = 3
    // end_event.data_collections = [{public.orders: 2}, {public.items: 1}]
}
```

### Read-Only Replica Support

CDC from PostgreSQL standbys without write access:

```rust
use rivven_cdc::common::{
    ReadOnlyConfig, ReadOnlyWatermarkTracker, ReadOnlyGuard, 
    ReadOnlyFeature, WatermarkSource, DeduplicationResult
};
use rivven_cdc::common::signal::SignalChannelType;

// Configure for read-only replica
let config = ReadOnlyConfig::builder()
    .read_only(true)
    .min_postgres_version(13)  // Required for pg_current_xact_id_if_assigned()
    .allowed_channel(SignalChannelType::Topic)  // Source channel disabled
    .allowed_channel(SignalChannelType::File)
    .heartbeat_watermarking(true)  // Use heartbeat-based watermarks
    .build();

// Check what channels are available
let guard = ReadOnlyGuard::new(&config);
assert!(guard.is_channel_allowed(&SignalChannelType::Topic));
assert!(!guard.is_channel_allowed(&SignalChannelType::Source)); // Blocked

// Verify features available in read-only mode
assert!(guard.is_available(&ReadOnlyFeature::Streaming));
assert!(!guard.is_available(&ReadOnlyFeature::SignalTableSource)); // Blocked

// Track watermarks for incremental snapshot deduplication
let mut tracker = ReadOnlyWatermarkTracker::new(
    WatermarkSource::Heartbeat,
    100 // initial transaction ID
);

// Open snapshot window
tracker.snapshot_started(105);

// Process streaming events - deduplicate against snapshot
let result = tracker.should_keep_event(110);
match result {
    DeduplicationResult::KeepEvent => println!("Event from streaming"),
    DeduplicationResult::KeepSnapshot => println!("Event from snapshot"),
}

// Close snapshot window  
tracker.snapshot_completed(150);
```

**Restricted Features in Read-Only Mode:**

| Feature | Status |
|---------|--------|
| `SignalTableSource` | ❌ Requires INSERT |
| `IncrementalSnapshotWrite` | ❌ Requires signal table writes |
| `AutoCreateSlot` | ❌ Requires `pg_create_logical_replication_slot()` |
| `AutoCreatePublication` | ❌ Requires CREATE PUBLICATION |
| `Streaming` | ✅ Available |
| `IncrementalSnapshotRead` | ✅ Via heartbeat watermarking |
| `TopicSignals` | ✅ Available |
| `FileSignals` | ✅ Available |

## Quick Start

### PostgreSQL

```rust
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_cdc::common::{TlsConfig, SslMode};
use rivven_cdc::CdcSource;

// With TLS
let tls = TlsConfig::new(SslMode::Require);
let config = PostgresCdcConfig::builder()
    .connection_string("postgres://user:pass@localhost/mydb")
    .slot_name("rivven_slot")
    .publication_name("rivven_pub")
    .tls_config(tls)
    .build()?;

let mut cdc = PostgresCdc::new(config);
cdc.start().await?;
```

### MySQL / MariaDB

```rust
use rivven_cdc::mysql::{MySqlCdc, MySqlCdcConfig};
use rivven_cdc::common::{TlsConfig, SslMode};

// With TLS
let tls = TlsConfig::new(SslMode::Require);
let config = MySqlCdcConfig::new("localhost", "root")
    .with_password("password")
    .with_database("mydb")
    .with_server_id(1001)
    .with_tls(tls);

let cdc = MySqlCdc::new(config);
```

## Documentation

- [CDC Architecture](../../docs/CDC_ARCHITECTURE.md) - Protocol deep-dive and internals
- [CDC Quickstart](../../docs/CDC_QUICKSTART.md) - Setup guides for each database
- [CDC Production](../../docs/CDC_PRODUCTION.md) - Performance tuning and monitoring

## License

See root [LICENSE](../../LICENSE) file.
