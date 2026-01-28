---
layout: default
title: Change Data Capture
nav_order: 4
---

# Change Data Capture (CDC)
{: .no_toc }

Stream database changes in real-time.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven provides **native CDC support** for PostgreSQL and MySQL, enabling you to stream database changes as events without external tools like Debezium.

### Key Features

- **Zero-config setup** with auto-provisioning
- **Debezium-compatible** event format
- **17 built-in transforms** for data manipulation
- **TLS/mTLS** for secure connections
- **Exactly-once semantics** via offset tracking

---

## PostgreSQL CDC

### Prerequisites

1. **PostgreSQL 10+** with logical replication enabled
2. User with `REPLICATION` privilege

```sql
-- Enable logical replication in postgresql.conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4

-- Create replication user
CREATE ROLE rivven WITH REPLICATION LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivven;
```

### Basic Configuration

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
      slot_name: rivven_orders
      publication_name: rivven_orders_pub
```

### Auto-Provisioning

Rivven automatically creates replication slots and publications:

| Option | Default | Description |
|:-------|:--------|:------------|
| `auto_create_slot` | `true` | Create replication slot if missing |
| `auto_create_publication` | `true` | Create publication if missing |
| `drop_slot_on_stop` | `false` | Delete slot on shutdown (dev only) |

```yaml
config:
  auto_create_slot: true
  auto_create_publication: true
  drop_slot_on_stop: false  # Set true for development
```

### Table Selection

```yaml
config:
  # All tables (default)
  publication_tables: []
  
  # Specific tables
  publication_tables:
    - public.orders
    - public.customers
    - inventory.products
```

### TLS Configuration

```yaml
config:
  tls:
    mode: require  # disable, prefer, require, verify-ca, verify-full
    ca_cert_path: /path/to/ca.pem
    # For mTLS (client certificate auth)
    client_cert_path: /path/to/client.pem
    client_key_path: /path/to/client-key.pem
```

### Authentication Methods

Rivven CDC supports all modern PostgreSQL authentication methods:

| Method | Supported | Notes |
|:-------|:----------|:------|
| `password` | ✅ | Cleartext (use only with TLS) |
| `md5` | ✅ | Legacy hashed auth |
| `scram-sha-256` | ✅ | **Recommended** — modern secure auth (RFC 5802) |
| Certificate (mTLS) | ✅ | Client certificate auth |

**SCRAM-SHA-256** is the recommended authentication method for PostgreSQL 10+:

```ini
# In pg_hba.conf, use scram-sha-256 for replication:
host    replication     rivven    0.0.0.0/0    scram-sha-256
```

```sql
-- Ensure password_encryption is set to scram-sha-256
ALTER SYSTEM SET password_encryption = 'scram-sha-256';
SELECT pg_reload_conf();

-- Create/update user with SCRAM password
ALTER ROLE rivven PASSWORD 'secure_password';
```

Rivven automatically detects and uses SCRAM-SHA-256 when the server requires it — no configuration needed.

### REPLICA IDENTITY

PostgreSQL's REPLICA IDENTITY setting determines what columns are included in UPDATE/DELETE events:

| Setting | Before Image | Use Case |
|:--------|:-------------|:---------|
| `DEFAULT` | Primary key only | Basic CDC (may miss columns) |
| `FULL` | All columns | **Recommended** - full before/after |
| `NOTHING` | None | Not recommended for CDC |
| `INDEX` | Unique index columns | Alternative to FULL |

**We recommend `REPLICA IDENTITY FULL` for proper CDC:**

```sql
-- Set REPLICA IDENTITY FULL on your tables
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;

-- Check current settings
SELECT relname, relreplident 
FROM pg_class 
WHERE relname IN ('orders', 'customers');
-- 'f' = FULL, 'd' = DEFAULT, 'n' = NOTHING, 'i' = INDEX
```

**Enforcement Configuration:**

```yaml
config:
  replica_identity:
    # Require FULL for all tables
    require_full: true
    
    # What to do when table doesn't have FULL:
    # - warn: Log warning but continue (default)
    # - skip: Skip events from non-FULL tables
    # - fail: Stop connector with error
    enforcement_mode: warn
    
    # Allow REPLICA IDENTITY INDEX as alternative
    allow_index: true
    
    # Tables to exclude from enforcement
    exclude_tables:
      - audit.*      # Skip audit schema
      - logs.events  # Skip specific table
```

### Schema Change Topic

Rivven CDC can publish DDL events (CREATE/ALTER/DROP TABLE) to a dedicated schema change topic, enabling downstream systems to track schema evolution and trigger migrations.

**Enable Schema Change Publishing:**

```yaml
config:
  schema_change:
    # Enable DDL publishing
    enabled: true
    
    # Topic name (default: {prefix}.schema_changes)
    topic_prefix: mydb
    
    # Include original DDL SQL statement
    include_ddl: true
    
    # Include column details
    include_columns: true
    
    # Include previous column state for ALTERs
    include_previous: true
    
    # Tables to exclude from schema change tracking
    exclude_tables:
      - temp_*       # Skip temporary tables
      - _flyway_*    # Skip migration tables
    
    # DDL types to exclude
    exclude_types:
      - CREATE_INDEX
      - DROP_INDEX
```

**Schema Change Event Format:**

```json
{
  "source_type": "postgres",
  "database": "shop",
  "schema": "public",
  "table": "orders",
  "change_type": "ALTER",
  "ddl": "ALTER TABLE orders ADD COLUMN discount NUMERIC(5,2)",
  "columns": [
    {"name": "id", "type_name": "integer", "position": 1, "nullable": false, "primary_key": true},
    {"name": "total", "type_name": "numeric(10,2)", "position": 2, "nullable": true},
    {"name": "discount", "type_name": "numeric(5,2)", "position": 3, "nullable": true}
  ],
  "previous_columns": [
    {"name": "id", "type_name": "integer", "position": 1, "nullable": false, "primary_key": true},
    {"name": "total", "type_name": "numeric(10,2)", "position": 2, "nullable": true}
  ],
  "timestamp_ms": 1737802200000,
  "position": "0/16B3748"
}
```

**Change Types:**

| Type | Description |
|:-----|:------------|
| `CREATE` | New table created |
| `ALTER` | Table structure changed |
| `DROP` | Table deleted |
| `TRUNCATE` | Table truncated |
| `RENAME` | Table renamed |
| `CREATE_INDEX` | Index created |
| `DROP_INDEX` | Index deleted |

### Heartbeat Configuration

Heartbeats keep replication slots active and detect stalled connections. They're essential for:

- **WAL retention** — PostgreSQL releases WAL files only when acknowledged
- **Health monitoring** — Detect lagging or disconnected connectors
- **Multi-database deployments** — Keep slots active across low-traffic databases

**Basic Configuration:**

```yaml
config:
  heartbeat:
    # Heartbeat interval (default: 10s)
    interval: 10s
    
    # Topic to publish heartbeat events (optional)
    topic: __debezium-heartbeat.mydb
    
    # Emit heartbeat events to topic
    emit_events: true
    
    # Maximum lag before unhealthy (default: 5 minutes)
    max_lag: 300s
```

**Multi-Database Heartbeat (Action Query):**

For multi-database PostgreSQL deployments, you can execute a SQL query on each heartbeat to keep replication slots active in databases with low write activity:

```yaml
config:
  heartbeat:
    interval: 10s
    
    # Execute this query on each heartbeat
    action_query: |
      INSERT INTO heartbeat (id, ts) VALUES (1, now())
      ON CONFLICT (id) DO UPDATE SET ts = now()
    
    # Execute against additional databases (besides main CDC database)
    action_query_databases:
      - inventory
      - analytics
      - audit_logs
```

**Heartbeat Table Setup:**

```sql
-- Create a heartbeat table in each database
CREATE TABLE IF NOT EXISTS heartbeat (
    id INTEGER PRIMARY KEY DEFAULT 1,
    ts TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Grant access to replication user
GRANT INSERT, UPDATE ON heartbeat TO rivven;
```

**Why Action Query Matters:**

In multi-database PostgreSQL deployments with a single replication slot, databases with no write activity can cause the slot to lag. The action query ensures:

1. **Slot advancement** — WAL is acknowledged even for idle databases
2. **Health indication** — Heartbeat table shows connector is alive
3. **Monitoring** — Easy to query heartbeat timestamp for alerting

### Signal Table (Runtime Control)

Rivven supports **Debezium-compatible signal tables** for runtime control of CDC connectors. Send signals to trigger ad-hoc snapshots, pause/resume streaming, or execute custom actions — all without restarting the connector.

**Signal Channels:**

| Channel | Description | Use Case |
|:--------|:------------|:---------|
| `source` | Signal table captured via CDC stream | **Default** — signal ordering guaranteed |
| `topic` | Signals from a Rivven/Kafka topic | Avoids database writes |
| `file` | Signals from a JSON file | Simple deployments |
| `api` | HTTP/gRPC API calls | Programmatic control |

The **source channel is enabled by default** because the signal table is captured through the normal CDC replication stream. This ensures:
- Signal ordering is guaranteed with the change stream
- No separate database connection required
- Works with read replicas (signals replicate like other data)
- Watermarking for incremental snapshot deduplication

**Create the Signal Table:**

```sql
-- Debezium-compatible signal table format
CREATE TABLE debezium_signal (
    id VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data VARCHAR(2048) NULL
);

-- Add to publication
ALTER PUBLICATION rivven_pub ADD TABLE debezium_signal;

-- Grant access to replication user
GRANT INSERT, DELETE ON debezium_signal TO rivven;
```

**Enable Signal Table in Configuration:**

```yaml
config:
  signal:
    # Enabled channels (default: source, topic)
    enabled_channels: [source]
    
    # Signal table name (source channel)
    data_collection: public.debezium_signal
    
    # Topic for signals (topic channel)
    # topic: cdc-signals
    
    # File path (file channel)
    # file: /var/rivven/signals.json
```

**Signal Types:**

| Signal | Description | Data |
|:-------|:------------|:-----|
| `execute-snapshot` | Trigger ad-hoc snapshot | `{"data-collections": ["schema.table"], "type": "incremental"}` |
| `stop-snapshot` | Stop in-progress snapshot | `{"type": "incremental"}` |
| `pause-snapshot` | Pause streaming | (none) |
| `resume-snapshot` | Resume streaming | (none) |
| `log` | Log a message | `{"message": "Your message here"}` |

**Example: Trigger Incremental Snapshot**

```sql
-- Trigger incremental snapshot for the orders table
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'ad-hoc-snapshot-1',
    'execute-snapshot',
    '{"data-collections": ["public.orders"], "type": "incremental"}'
);
```

**Example: Pause and Resume Streaming**

```sql
-- Pause the connector
INSERT INTO debezium_signal (id, type, data)
VALUES ('pause-1', 'pause-snapshot', NULL);

-- Resume later
INSERT INTO debezium_signal (id, type, data)
VALUES ('resume-1', 'resume-snapshot', NULL);
```

**Example: Using Topic Channel**

For environments where database writes should be avoided, use the topic channel:

```yaml
config:
  signal:
    enabled_channels: [topic]
    topic: cdc-signals
```

Then send signals via Rivven CLI:

```bash
# Trigger snapshot via topic
rivven produce cdc-signals \
  --key mydb-connector \
  --value '{"type": "execute-snapshot", "data": {"data-collections": ["public.orders"]}}'
```

**File Channel for Simple Deployments:**

```yaml
config:
  signal:
    enabled_channels: [file]
    file: /var/rivven/signals.json
    poll_interval_ms: 1000
```

Signal file format (one JSON per line):
```json
{"id":"snap-1","type":"execute-snapshot","data":{"data-collections":["public.orders"]}}
{"id":"pause-1","type":"pause-snapshot"}
```

### Incremental Snapshots

Rivven supports **Debezium-compatible incremental snapshots** that allow re-snapshotting tables while streaming continues — no connector restart required.

**Why Incremental Snapshots?**

- **Large table changes** — Add backfill data without blocking streaming
- **Schema evolution** — Re-capture historical data after schema changes
- **Data repair** — Fix inconsistencies without full re-sync
- **Zero downtime** — Streaming continues during snapshot

**How It Works:**

Incremental snapshots use **DBLog watermark-based deduplication** to avoid duplicate events:

```
Timeline:
────────────────────────────────────────────────────────────────►
    │         │                     │         │
    │  OPEN   │   Chunk Query       │  CLOSE  │
    │ Window  │   (buffer results)  │ Window  │
    │         │                     │         │

1. Open window marker written to signal table
2. Chunk query executed, rows buffered in memory
3. Streaming events with matching PKs cause buffer entries to be discarded
4. Close window marker written
5. Remaining buffer entries emitted as READ events
```

**Configuration:**

```yaml
config:
  incremental_snapshot:
    # Rows per chunk (default: 1024)
    chunk_size: 1024
    
    # Watermark strategy: insert_insert or insert_delete
    watermark_strategy: insert_insert
    
    # Maximum buffer memory (default: 64MB)
    max_buffer_memory: 67108864
    
    # Timeout per chunk query
    chunk_timeout: 60s
    
    # Delay between chunks (backpressure)
    inter_chunk_delay: null
    
    # Signal table for watermarks
    signal_table: debezium_signal
```

**Trigger via Signal Table:**

```sql
-- Snapshot multiple tables
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'incr-snap-1',
    'execute-snapshot',
    '{"data-collections": ["public.orders", "public.order_items"], "type": "incremental"}'
);
```

**Trigger with Additional Conditions:**

Filter the snapshot to specific rows:

```sql
-- Snapshot only recent orders
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'incr-snap-2',
    'execute-snapshot',
    '{
      "data-collections": ["public.orders"],
      "type": "incremental",
      "additional-conditions": [
        {
          "data-collection": "public.orders",
          "filter": "created_at >= ''2024-01-01''"
        }
      ]
    }'
);
```

**Trigger with Surrogate Key:**

Use a different column for chunking (useful for composite primary keys):

```sql
-- Use order_id as the chunking key instead of composite PK
INSERT INTO debezium_signal (id, type, data)
VALUES (
    'incr-snap-3',
    'execute-snapshot',
    '{
      "data-collections": ["public.orders"],
      "type": "incremental",
      "surrogate-keys": {
        "public.orders": "order_id"
      }
    }'
);
```

**Control In-Progress Snapshots:**

```sql
-- Pause snapshot
INSERT INTO debezium_signal (id, type, data)
VALUES ('pause-1', 'pause-snapshot', '{"type": "incremental"}');

-- Resume snapshot
INSERT INTO debezium_signal (id, type, data)
VALUES ('resume-1', 'resume-snapshot', NULL);

-- Stop snapshot entirely
INSERT INTO debezium_signal (id, type, data)
VALUES ('stop-1', 'stop-snapshot', '{"type": "incremental"}');
```

**Watermark Strategies:**

| Strategy | Description | Signal Table Growth |
|:---------|:------------|:--------------------|
| `insert_insert` | Insert open marker, insert close marker | Grows (markers persist) |
| `insert_delete` | Insert open marker, delete on close | No growth (cleanup automatic) |

Use `insert_delete` for long-running systems to prevent signal table bloat.

**Monitoring:**

Rivven emits metrics for incremental snapshot progress:

| Metric | Description |
|:-------|:------------|
| `cdc_incremental_snapshot_chunks_processed` | Total chunks completed |
| `cdc_incremental_snapshot_rows_snapshotted` | Total rows captured |
| `cdc_incremental_snapshot_events_dropped` | Events deduplicated (streaming conflicts) |
| `cdc_incremental_snapshot_window_time_ms` | Total time with window open |
| `cdc_incremental_snapshot_active` | Whether snapshot is in progress |

**Resume After Restart:**

Incremental snapshot state is persisted to offsets. If the connector restarts:
- Current chunk position is saved
- Snapshot resumes from the next chunk on restart
- No data loss or duplicates

### Read-Only Replica Support

Rivven CDC supports capturing changes from **PostgreSQL read replicas** (hot standbys) using the `read_only` mode. This is useful for:

- **Reducing load on primary** — Offload CDC traffic to replicas
- **Disaster recovery** — CDC from standby without impacting primary
- **Multi-region deployments** — Stream from the closest replica

**PostgreSQL Requirements:**

- PostgreSQL **13+** (for `pg_current_xact_id_if_assigned()` function)
- Hot standby with `hot_standby = on`
- `hot_standby_feedback = on` (recommended to prevent query conflicts)

**Enable Read-Only Mode:**

```yaml
config:
  read_only:
    enabled: true
    
    # Allowed signal channels (source channel disabled in read-only mode)
    allowed_channels: [topic, file, api]
    
    # Use heartbeat-based watermarking for incremental snapshots
    heartbeat_watermarking: true
```

**Restricted Features:**

In read-only mode, certain features that require database writes are automatically disabled:

| Feature | Status | Alternative |
|:--------|:-------|:------------|
| Signal table (source channel) | ❌ Disabled | Use `topic`, `file`, or `api` channel |
| Incremental snapshots (write mode) | ❌ Disabled | Use heartbeat-based watermarking |
| Auto-create slot | ❌ Disabled | Pre-create slot on primary |
| Auto-create publication | ❌ Disabled | Pre-create publication on primary |

**Heartbeat-Based Watermarking:**

Read-only replicas cannot write to the signal table for watermarking during incremental snapshots. Instead, Rivven uses **heartbeat-based watermarking** that extracts transaction IDs from WAL events:

```yaml
config:
  read_only:
    enabled: true
    heartbeat_watermarking: true
    
  heartbeat:
    interval: 10s
    # No action_query since we can't write to the database
```

The watermark tracker:
1. Records the low watermark (XID) at snapshot start
2. Tracks streaming events with transaction IDs
3. Records high watermark when snapshot window closes
4. Deduplicates events that fall within the snapshot window

**Pre-Setup Requirements:**

Since read-only mode cannot auto-provision, you must create the replication slot and publication on the **primary** before pointing CDC at the replica:

```sql
-- On the PRIMARY database
-- 1. Create publication
CREATE PUBLICATION rivven_pub FOR TABLE public.orders, public.customers;

-- 2. Create replication slot (this replicates to standby)
SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');
```

Then configure Rivven to connect to the **replica**:

```yaml
config:
  host: replica.example.com  # Connect to standby, not primary
  port: 5432
  slot_name: rivven_slot
  publication_name: rivven_pub
  
  read_only:
    enabled: true
    allowed_channels: [topic, file, api]
  
  # Disable auto-provisioning (read-only can't create these)
  auto_create_slot: false
  auto_create_publication: false
```

**Signal Channel Alternatives:**

Since the `source` channel requires INSERT access to the signal table, use alternative channels:

```yaml
config:
  read_only:
    enabled: true
    allowed_channels: [topic, file]
  
  signal:
    enabled_channels: [topic]
    topic: cdc-signals
```

Send signals via Rivven CLI instead of SQL:

```bash
# Trigger snapshot via topic channel
rivven produce cdc-signals \
  --key mydb-connector \
  --value '{"type": "execute-snapshot", "data": {"data-collections": ["public.orders"]}}'
```

### Transaction Metadata Topic

Rivven supports **Debezium-compatible transaction metadata events**, publishing BEGIN/END markers to a dedicated topic. This enables downstream consumers to track transaction boundaries and correlate change events within the same transaction.

**Enable Transaction Metadata Topic:**

```yaml
config:
  transaction_topic:
    enabled: true
    topic_suffix: transaction  # Results in: cdc.orders.transaction
    enrich_events: true        # Add transaction context to each event
```

**Transaction Events:**

When enabled, Rivven publishes two event types to the transaction topic:

| Event | When | Contents |
|:------|:-----|:---------|
| `BEGIN` | Transaction starts | `id`, `ts_ms`, `status: "BEGIN"` |
| `END` | Transaction commits | `id`, `ts_ms`, `event_count`, `data_collections[]` |

**BEGIN Event Example:**

```json
{
  "id": "txid:1234567",
  "status": "BEGIN",
  "ts_ms": 1737802200000
}
```

**END Event Example:**

```json
{
  "id": "txid:1234567",
  "status": "END",
  "ts_ms": 1737802200123,
  "event_count": 5,
  "data_collections": [
    {"data_collection": "public.orders", "event_count": 3},
    {"data_collection": "public.order_items", "event_count": 2}
  ]
}
```

**Event Enrichment:**

When `enrich_events: true`, each CDC event includes transaction context:

```json
{
  "before": null,
  "after": {"id": 1001, "customer_id": 42},
  "source": {...},
  "op": "c",
  "ts_ms": 1737802200123,
  "transaction": {
    "id": "txid:1234567",
    "total_order": 3,
    "data_collection_order": 2
  }
}
```

| Field | Description |
|:------|:------------|
| `id` | Transaction identifier |
| `total_order` | Event position across all tables in transaction (1-indexed) |
| `data_collection_order` | Event position within this table (1-indexed) |

**Use Cases:**

1. **Exactly-once processing** — Detect and handle multi-event transactions atomically
2. **Audit trails** — Correlate all changes within a single database transaction  
3. **Downstream aggregation** — Group events by transaction for batch processing
4. **Monitoring** — Track transaction throughput and event counts per transaction

---

## MySQL CDC

### Prerequisites

1. **MySQL 5.7+** or **MariaDB 10.2+**
2. Binary logging enabled

```sql
-- Enable binary logging in my.cnf
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL

-- Create replication user
CREATE USER 'rivven'@'%' IDENTIFIED BY 'secret';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivven'@'%';
GRANT SELECT ON shop.* TO 'rivven'@'%';
```

### Configuration

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
      server_id: 12345  # Unique ID for this connector
```

### Table Filtering

```yaml
config:
  # Include specific tables
  tables:
    - orders
    - customers
  
  # Or use regex patterns
  table_regex: "^(orders|customers)$"
```

### TLS Configuration

```yaml
config:
  tls:
    mode: require  # disable, prefer, require, verify-ca, verify-full
    ca_cert_path: /path/to/ca.pem
    # For mTLS (client certificate auth)
    client_cert_path: /path/to/client.pem
    client_key_path: /path/to/client-key.pem
```

---

## Event Format

CDC events follow a **Debezium-compatible** envelope format:

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

---

## Transforms

### Built-in Transforms

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

### S3

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

### Key Metrics

| Metric | Description |
|:-------|:------------|
| `rivven_cdc_events_total` | Total events processed |
| `rivven_cdc_lag_milliseconds` | Replication lag |
| `rivven_cdc_errors_total` | Error count |
| `rivven_cdc_connected` | Connection status (1/0) |

### Alerting Thresholds

```yaml
# Example Prometheus alerts
- alert: CDCLagHigh
  expr: rivven_cdc_lag_milliseconds > 10000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "CDC replication lag > 10s"

- alert: CDCDisconnected
  expr: rivven_cdc_connected == 0
  for: 1m
  labels:
    severity: critical
```

---

## Production Recommendations

### PostgreSQL

```yaml
# Production: Explicit resource management
sources:
  prod_db:
    connector: postgres-cdc
    config:
      auto_create_slot: false      # DBA creates slot
      auto_create_publication: false  # DBA creates pub
      slot_name: rivven_prod
      publication_name: rivven_prod_pub
      tls:
        mode: verify-full
        ca_cert_path: /etc/ssl/certs/rds-ca.pem
```

### MySQL

```yaml
sources:
  prod_db:
    connector: mysql-cdc
    config:
      server_id: 12345  # Must be unique across all replicating clients
      tls:
        mode: require
```

---

## Next Steps

- [Connectors](/rivven/docs/connectors) - All connector configurations
- [Security](/rivven/docs/security) - TLS and authentication
- [Kubernetes](/rivven/docs/kubernetes) - Production deployment
