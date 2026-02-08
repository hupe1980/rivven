# rivven-cdc

> Native Change Data Capture for PostgreSQL, MySQL, MariaDB, and SQL Server.

## Overview

`rivven-cdc` captures database changes and outputs `CdcEvent` structs with JSON data. The serialization format (Avro, Protobuf, JSON Schema) is decided by the consumer (typically rivven-connect).

### Supported Databases

| Database | Version | Protocol | Status |
|----------|---------|----------|--------|
| PostgreSQL | 10+ | WAL streaming (pgoutput) | ✅ Available |
| MySQL | 5.7+ | Binlog streaming (GTID) | ✅ Available |
| MariaDB | 10.2+ | Binlog streaming (GTID) | ✅ Available |
| SQL Server | 2016 SP1+ | CDC table polling | ✅ Available |
| Oracle | 19c+ | LogMiner | 📋 Planned |

## Design Philosophy

**rivven-cdc is format-agnostic.** This separation of concerns allows:
- Using CDC events without any schema registry
- Choosing serialization format at the connector level, not CDC level
- Simpler CDC library with fewer dependencies

```text
rivven-cdc                          rivven-connect
┌──────────────────┐                ┌──────────────────────┐
│ Database         │                │ CDC Source Connector │
│    ↓             │                │    ↓                 │
│ CdcSource        │ ──CdcEvent──►  │ Schema Inference     │
│    ↓             │                │    ↓                 │
│ CdcConnector     │                │ Schema Registry      │
│    ↓             │                │    ↓                 │
│ Topic Routing    │                │ Serialization        │
└──────────────────┘                └──────────────────────┘
```

## Public API Organization

The crate exports types in three tiers for clarity:

### Tier 1: Core Types (crate root)
Essential types for basic CDC operations:
- `CdcEvent`, `CdcOp`, `CdcSource` - Core event types
- `CdcError`, `Result`, `ErrorCategory` - Error handling
- `CdcFilter`, `CdcFilterConfig`, `TableColumnConfig` - Filtering

### Tier 2: Feature Types (crate root)
Optional features for production use:
- **SMT Transforms**: `SmtChain`, `MaskField`, `Filter`, `Cast`, etc.
- **Deduplication**: `Deduplicator`, `DeduplicatorConfig`
- **Encryption**: `FieldEncryptor`, `EncryptionConfig`, `MemoryKeyProvider`
- **Tombstones**: `TombstoneEmitter`, `TombstoneConfig`
- **Schema Changes**: `SchemaChangeEmitter`, `SchemaChangeEvent`
- **Transactions**: `TransactionTopicEmitter`, `TransactionEvent`
- **Signals**: `SignalProcessor`, `Signal`, `SignalResult`

### Tier 3: Advanced Types (`common::` module)
Internal/advanced types for custom implementations:
- Resilience: `CircuitBreaker`, `RateLimiter`, `RetryConfig`
- Routing: `EventRouter`, `RouteRule`
- Snapshot: `SnapshotCoordinator`, `SnapshotConfig`
- And many more via `use rivven_cdc::common::*`

## Documentation

| Guide | Description |
|-------|-------------|
| [PostgreSQL CDC Guide](../../docs/docs/cdc-postgres.md) | Complete PostgreSQL setup, TLS, authentication, production deployment |
| [MySQL/MariaDB CDC Guide](../../docs/docs/cdc-mysql.md) | MySQL and MariaDB binary log replication setup |
| [Configuration Reference](../../docs/docs/cdc-configuration.md) | All CDC configuration options and environment variables |
| [Troubleshooting Guide](../../docs/docs/cdc-troubleshooting.md) | Diagnose and resolve common CDC issues |
| [CDC Overview](../../docs/docs/cdc.md) | Feature overview and quick start |

## Features

- 🚀 **Native Implementation** - Direct TCP connections, no external dependencies
- 🐘 **PostgreSQL** - Logical replication via pgoutput plugin (v14+ recommended, v10+ supported)
- 🐬 **MySQL/MariaDB** - Binlog replication with GTID support (MySQL 8.0+, MariaDB 10.5+)
- 🔒 **TLS/mTLS** - Secure connections with optional client certificate auth
- 🔑 **Full Auth Support** - SCRAM-SHA-256 (PostgreSQL), caching_sha2_password + ed25519 (MySQL/MariaDB)
- 📦 **Zero-Copy** - Efficient binary protocol parsing
- ⚡ **Async** - Built on Tokio for high-performance streaming
- 📡 **Signal Table** - Runtime control with ad-hoc snapshots and pause/resume
- 🔄 **Incremental Snapshots** - Re-snapshot tables while streaming continues
- 🎯 **Format-Agnostic** - No schema registry coupling; serialization handled by consumers
- 📊 **Avro Serialization** - Built-in `CdcEventSerializer` with JSON and Avro binary formats, Confluent wire format support

## Supported Versions

### PostgreSQL

| Version | Status | EOL | Notes |
|---------|--------|-----|-------|
| 14.x | ✅ Tested | Nov 2026 | Streaming large transactions |
| 15.x | ✅ Tested | Nov 2027 | Row filters, column lists |
| 16.x | ✅ **Recommended** | Nov 2028 | Parallel apply |
| 17.x | ✅ Tested | Nov 2029 | Enhanced logical replication |

### MySQL / MariaDB

#### MySQL

| Version | Status | EOL | Notes |
|---------|--------|-----|-------|
| 8.0.x | ✅ Tested | Apr 2026 | GTID, caching_sha2_password |
| 8.4.x | ✅ **Recommended** | Apr 2032 | LTS, enhanced replication |
| 9.0.x | ✅ Tested | TBD | Innovation release (latest) |

#### MariaDB

| Version | Status | EOL | Notes |
|---------|--------|-----|-------|
| 10.6.x | ✅ Tested | Jul 2026 | LTS, GTID improvements |
| 10.11.x | ✅ **Recommended** | Feb 2028 | LTS, enhanced JSON, parallel replication |
| 11.4.x | ✅ Tested | May 2029 | LTS, latest features |

### SQL Server

| Version | Status | EOL | Notes |
|---------|--------|-----|-------|
| 2016 SP1+ | ✅ Tested | Jul 2026 | CDC support introduced |
| 2019 | ✅ **Recommended** | Jan 2030 | Enhanced CDC performance |
| 2022 | ✅ Tested | TBD | Latest features |
| Azure SQL | ✅ Tested | N/A | Managed CDC support |

## Optional Features

| Feature | Description |
|---------|-------------|
| `postgres` | PostgreSQL CDC (default) |
| `mysql` | MySQL/MariaDB CDC (default) |
| `sqlserver` | SQL Server CDC |
| `postgres-tls` | PostgreSQL with TLS |
| `mysql-tls` | MySQL/MariaDB with TLS |
| `sqlserver-tls` | SQL Server with TLS |
| `mariadb` | MariaDB CDC (alias for mysql with MariaDB extensions) |

## Feature Matrix

| Feature | Supported | Notes |
|---------|-----------|-------|
| Logical Replication | ✅ | pgoutput plugin |
| Binlog Streaming | ✅ | MySQL/MariaDB GTID |
| CDC Table Polling | ✅ | SQL Server CDC tables |
| Binlog Checksum (CRC32) | ✅ | Auto-negotiation, MySQL 8+/MariaDB 10+ |
| Schema Metadata | ✅ | Real column names from INFORMATION_SCHEMA |
| Initial Snapshot | ✅ | Parallel, resumable |
| TLS/SSL | ✅ | rustls-based |
| Table/Column Filtering | ✅ | Glob patterns (regex-backed) |
| Column Masking | ✅ | Redacted |
| Heartbeats | ✅ | WAL acknowledgment |
| Heartbeat Action Query | ✅ | Multi-database support |
| Checkpointing | ✅ | LSN/binlog position |
| Schema Inference | ✅ | Via rivven-connect |
| **Tombstone Events** | ✅ | `TombstoneEmitter` for log compaction |
| **REPLICA IDENTITY** | ✅ | `ReplicaIdentityEnforcer` with warn/skip/fail |
| **Schema Change Topic** | ✅ | `SchemaChangeEmitter` for DDL tracking |
| **SCRAM-SHA-256** | ✅ | RFC 5802 PostgreSQL auth |
| **caching_sha2_password** | ✅ | MySQL 8.0+ default auth plugin |
| **client_ed25519** | ✅ | MariaDB Ed25519 auth |
| **Signal Table** | ✅ | Multi-channel (source/topic/file) |
| **Transaction Metadata Topic** | ✅ | `TransactionTopicEmitter` BEGIN/END events |
| **Read-Only Replicas** | ✅ | Heartbeat-based watermarking for standbys |
| **Incremental Snapshots** | ✅ | DBLog watermarks, chunk-based |
| **Event Routing** | ✅ | `EventRouter` with content-based routing, DLQ |
| **Partitioning** | ✅ | `Partitioner` with KeyHash, TableHash, RoundRobin |
| **Log Compaction** | ✅ | `Compactor` for key-based deduplication |
| **Parallel CDC** | ✅ | `ParallelSource` for multi-table concurrency |
| **Outbox Pattern** | ✅ | `OutboxProcessor` for transactional messaging |
| **Health Monitoring** | ✅ | `HealthMonitor` with auto-recovery, lag monitoring |
| **Notifications** | ✅ | `Notifier` for snapshot/streaming progress alerts |
| **Field Encryption** | ✅ | AES-256-GCM column-level encryption |
| **Error Classification** | ✅ | Categorized errors with retry detection |

### Error Handling

The crate provides comprehensive error classification for intelligent retry and alerting:

```rust
use rivven_cdc::{CdcError, ErrorCategory};

// Error categories for metrics/alerting
match error.category() {
    ErrorCategory::Database => { /* Connection, query, auth errors */ }
    ErrorCategory::Replication => { /* WAL, binlog, slot errors */ }
    ErrorCategory::Network => { /* Timeout, connection lost */ }
    ErrorCategory::Schema => { /* DDL, type mapping errors */ }
    ErrorCategory::Serialization => { /* JSON, encryption errors */ }
    ErrorCategory::Configuration => { /* Invalid settings */ }
    ErrorCategory::Other => { /* Unclassified */ }
}

// Check if error is retriable
if error.is_retriable() {
    // Exponential backoff retry
}

// Get metric-safe error code
println!("Error code: {}", error.error_code()); // e.g., "connection_closed", "timeout"
```

**Error Types:**

| Error | Category | Retriable | Description |
|-------|----------|-----------|-------------|
| `ConnectionClosed` | Network | ✅ | Connection lost |
| `ConnectionRefused` | Network | ✅ | Host unreachable |
| `Timeout` | Network | ✅ | Operation timed out |
| `DeadlockDetected` | Database | ✅ | Transaction deadlock |
| `ReplicationSlotInUse` | Replication | ✅ | Slot occupied |
| `RateLimitExceeded` | Network | ✅ | Throttled |
| `SnapshotFailed` | Database | Conditional | Snapshot error (retryable if timeout/lock) |
| `Authentication` | Database | ❌ | Auth failure |
| `Schema` | Schema | ❌ | Invalid schema |
| `Config` | Configuration | ❌ | Bad configuration |
| `Transform` | Serialization | ❌ | SMT transform error |
| `Encryption` | Serialization | ❌ | Encrypt/decrypt error |
| `Checkpoint` | Database | ❌ | Offset store error |

### Signal Table (Runtime Control)

Control CDC connectors at runtime without restarting - trigger snapshots, pause/resume streaming:

```rust
use rivven_cdc::common::signal::{SignalConfig, SignalChannelType};
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};

// Configure signal table via CDC stream (default)
let signal_config = SignalConfig::builder()
    .enabled_channels(vec![SignalChannelType::Source])
    .signal_data_collection("public.rivven_signal")
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
| `topic` | Signals from Rivven topic |
| `file` | Signals from JSON file |

### Incremental Snapshots

Re-snapshot tables while streaming continues using DBLog-style watermark deduplication:

```rust
use rivven_cdc::common::{
    IncrementalSnapshotConfig, IncrementalSnapshotCoordinator,
    IncrementalSnapshotRequest, WatermarkStrategy, SnapshotKey
};

// Configure incremental snapshots with parallel processing
let config = IncrementalSnapshotConfig::builder()
    .chunk_size(1024)  // Rows per chunk
    .max_concurrent_chunks(4)  // Process 4 chunks in parallel
    .watermark_strategy(WatermarkStrategy::InsertInsert)
    .max_buffer_memory(64 * 1024 * 1024)  // 64MB buffer per chunk
    .signal_table("rivven_signal")
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
    // Get DB timestamp for watermark (prevents race conditions)
    let watermark_ts = get_db_timestamp().await?; // SELECT EXTRACT(EPOCH FROM NOW()) * 1000
    
    // Generate OPEN signal with watermark
    let open_signal = coordinator.open_window_signal_with_watermark(&chunk, watermark_ts);
    // Write signal using parameterized query:
    // INSERT INTO rivven_signal (id, type, data) VALUES ($1, $2, $3)
    
    // Open deduplication window with watermark
    coordinator.open_window_with_watermark(&chunk, watermark_ts).await?;
    
    // Execute chunk query and buffer results
    // for row in execute_query(&chunk) {
    //     let key = SnapshotKey::from_row(&row, &["id"])?;
    //     coordinator.buffer_row_for_chunk_with_key(&chunk.chunk_id, event, key).await;
    // }
    
    // Meanwhile, check streaming events for conflicts
    // for event in streaming_events {
    //     let key = SnapshotKey::from_row(&event.after, &["id"])?;
    //     let is_delete = event.op == "d";
    //     coordinator.check_streaming_conflict_with_timestamp(
    //         "public.orders", &key, event.ts_ms, is_delete
    //     ).await;
    // }
    
    // Close window - returns events not superseded by streaming
    let events = coordinator.close_window_for_chunk(&chunk.chunk_id).await?;
    let close_signal = coordinator.close_window_signal(&chunk);
    // Write close signal using parameterized query
    
    // Emit remaining snapshot events as READ operations
    for event in events {
        // emit_event(event).await;
    }
}

// Check statistics
let stats = coordinator.stats();
println!("Rows: {}, Dropped: {}", stats.rows_snapshotted, stats.events_dropped);
```

**Deduplication Logic:**

| Scenario | Action |
|----------|--------|
| Streaming event with `ts >= watermark_ts` | Drop snapshot row (streaming wins) |
| Streaming event with `ts < watermark_ts` | Keep snapshot row (snapshot is fresher) |
| DELETE event | Always drop snapshot row (deletes win) |

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

Tombstones are emitted after DELETE events for log compaction:

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

### SQL Server

```rust
use rivven_cdc::sqlserver::{SqlServerCdc, SqlServerCdcConfig, SnapshotMode};
use rivven_cdc::CdcSource;

// SQL Server CDC uses poll-based change capture
let config = SqlServerCdcConfig::builder()
    .host("localhost")
    .port(1433)
    .username("sa")
    .password("YourStrong@Passw0rd")
    .database("mydb")
    .poll_interval_ms(100)
    .snapshot_mode(SnapshotMode::Initial) // or Never, Always, WhenNeeded
    .include_table("dbo", "users")
    .include_table("dbo", "orders")
    .build()?;

let mut cdc = SqlServerCdc::new(config);
let rx = cdc.take_event_receiver().expect("receiver already taken");

cdc.start().await?;

// Access metrics for observability
let metrics = cdc.metrics();
println!("Events: {}, Polls: {}, Efficiency: {:.1}%", 
    metrics.events_captured,
    metrics.poll_cycles,
    100.0 * (1.0 - metrics.empty_polls as f64 / metrics.poll_cycles.max(1) as f64)
);

while let Some(event) = rx.recv().await {
    println!("Change: {:?}", event);
}
```

**SQL Server Setup:**

```sql
-- Enable CDC on database
USE mydb;
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on tables
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'users',
    @role_name = NULL;

-- Start SQL Server Agent (required for CDC capture job)
-- In container: /opt/mssql/bin/sqlservr
```

## Documentation

- [CDC Overview](../../docs/docs/cdc.md) - Feature overview and concepts
- [PostgreSQL CDC Guide](../../docs/docs/cdc-postgres.md) - PostgreSQL setup and configuration
- [MySQL/MariaDB CDC Guide](../../docs/docs/cdc-mysql.md) - MySQL and MariaDB setup
- [SQL Server CDC Guide](../../docs/docs/cdc-sqlserver.md) - SQL Server setup and configuration
- [Configuration Reference](../../docs/docs/cdc-configuration.md) - All configuration options
- [Troubleshooting Guide](../../docs/docs/cdc-troubleshooting.md) - Diagnose and resolve issues

## Documentation

- [CDC Guide](https://rivven.hupe1980.github.io/rivven/docs/cdc)
- [PostgreSQL CDC](https://rivven.hupe1980.github.io/rivven/docs/cdc-postgres)
- [MySQL CDC](https://rivven.hupe1980.github.io/rivven/docs/cdc-mysql)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
