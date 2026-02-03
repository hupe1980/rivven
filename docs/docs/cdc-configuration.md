---
layout: default
title: CDC Configuration Reference
parent: Change Data Capture
nav_order: 4
---

# CDC Configuration Reference
{: .no_toc }

Complete reference for all CDC configuration options.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Configuration Methods

### Rust Builder API

```rust
use rivven_cdc::{CdcConfig, PostgresCdcConfig, Snapshot};

let config = PostgresCdcConfig::builder()
    .host("localhost")
    .port(5432)
    .user("rivven")
    .password("secret")
    .database("mydb")
    .slot_name("rivven_slot")
    .publication_name("rivven_pub")
    .snapshot(Snapshot::Initial)
    .build()?;
```

### YAML Configuration

```yaml
version: "1.0"

sources:
  my_source:
    connector: postgres-cdc
    topic: cdc.events
    config:
      host: localhost
      port: 5432
      # ... other options
```

### Environment Variables

All configuration options can be set via environment variables:

```bash
export RIVVEN_CDC_HOST=localhost
export RIVVEN_CDC_PORT=5432
export RIVVEN_CDC_PASSWORD=${DB_PASSWORD}
```

---

## Common Options

These options apply to all CDC connectors.

### Core Settings

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `name` | string | required | Unique connector name |
| `topic` | string | required | Output topic for events |
| `enabled` | bool | `true` | Enable/disable connector |

### Table Selection

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `tables` | list | `[]` | Tables to include (empty = all) |
| `exclude_tables` | list | `[]` | Tables to exclude |
| `table_filter` | regex | - | Regex pattern for table names |
| `schema_filter` | regex | - | Regex pattern for schema names |

**Example:**
```yaml
config:
  tables:
    - public.orders
    - public.customers
  exclude_tables:
    - "*_backup"
    - "*_archive"
    - "temp_*"
```

### Column Selection

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `columns` | map | - | Columns to include per table |
| `exclude_columns` | map | - | Columns to exclude per table |
| `column_masks` | list | `[]` | Columns to mask/redact |

**Example:**
```yaml
config:
  columns:
    public.users:
      - id
      - email
      - created_at
  exclude_columns:
    public.users:
      - internal_notes
  column_masks:
    - public.users.ssn
    - public.customers.credit_card
```

### Event Filtering

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `operations` | list | `[insert, update, delete]` | Operations to capture |
| `filter_expression` | string | - | SQL-like filter expression |

**Example:**
```yaml
config:
  operations:
    - insert
    - update
  filter_expression: "amount > 100 AND status != 'draft'"
```

---

## Snapshot Settings

Initial snapshots capture the full state of tables before streaming begins. This is essential for ensuring downstream systems have complete data.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `snapshot.mode` | enum | `initial` | Snapshot behavior (see modes below) |
| `snapshot.batch_size` | u64 | `10000` | Rows per SELECT batch |
| `snapshot.parallel_tables` | u32 | `4` | Tables to snapshot concurrently |
| `snapshot.progress_dir` | path | - | Directory for progress persistence |
| `snapshot.query_timeout_secs` | u64 | `300` | Query timeout in seconds |
| `snapshot.throttle_delay_ms` | u64 | `0` | Delay between batches (backpressure) |
| `snapshot.max_retries` | u32 | `3` | Retries per failed batch |
| `snapshot.include_tables` | list | `[]` | Tables to include (empty = all) |
| `snapshot.exclude_tables` | list | `[]` | Tables to exclude |

### Snapshot Modes

| Mode | Description |
|:-----|:------------|
| `initial` | Snapshot on first start, then stream (default) |
| `always` | Full snapshot on every start |
| `never` | No snapshot, stream only |
| `when_needed` | Snapshot only if no stored offsets |
| `initial_only` | Snapshot only, no streaming after |
| `schema_only` | Capture schema structure, no data |
| `recovery` | Force re-snapshot for disaster recovery |

### Example

```yaml
connector_type: postgres-cdc
config:
  host: localhost
  database: mydb
  user: replicator
  password: ${DB_PASSWORD}
  slot_name: rivven_slot
  publication_name: rivven_pub
  
  # Snapshot configuration
  snapshot:
    mode: initial
    batch_size: 50000
    parallel_tables: 4
    progress_dir: /var/lib/rivven/snapshot
    query_timeout_secs: 600
    include_tables:
      - public.users
      - public.orders
    exclude_tables:
      - public.audit_log
```

### Progress Persistence

When `progress_dir` is set, snapshot progress is persisted to JSON files. This enables:
- **Resumable snapshots**: If interrupted, resume from last completed batch
- **Incremental re-snapshots**: Skip already-captured tables
- **Debugging**: Inspect progress files for troubleshooting

```bash
# Progress file structure
/var/lib/rivven/snapshot/
  public.users.json    # {"table":"public.users","last_key":"1000","state":"in_progress"}
  public.orders.json   # {"table":"public.orders","last_key":null,"state":"completed"}
```

---

## Performance Settings

### Throughput

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `batch_size` | u32 | `1000` | Events per batch |
| `batch_timeout` | duration | `100ms` | Max wait for batch |
| `max_queue_size` | u32 | `10000` | Internal queue limit |
| `workers` | u32 | `4` | Processing workers |

### Memory

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `max_memory_mb` | u32 | `512` | Memory limit |
| `buffer_size` | u32 | `8192` | Network buffer size |

### Backpressure

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `backpressure.enabled` | bool | `true` | Enable backpressure |
| `backpressure.high_watermark` | u32 | `8000` | Pause threshold |
| `backpressure.low_watermark` | u32 | `2000` | Resume threshold |

**Example:**
```yaml
config:
  batch_size: 5000
  batch_timeout: 200ms
  max_queue_size: 20000
  workers: 8
  
  backpressure:
    enabled: true
    high_watermark: 15000
    low_watermark: 5000
```

---

## Resilience Settings

### Retry Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `retry.max_attempts` | u32 | `10` | Max retry attempts |
| `retry.initial_delay` | duration | `100ms` | First retry delay |
| `retry.max_delay` | duration | `30s` | Max retry delay |
| `retry.multiplier` | f64 | `2.0` | Exponential backoff |
| `retry.jitter` | f64 | `0.1` | Jitter factor (0-1) |

### Guardrails

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `guardrails.max_lag` | duration | `5m` | Max acceptable lag |
| `guardrails.max_queue_lag` | duration | `1m` | Max queue lag |
| `guardrails.max_batch_size` | u32 | `100000` | Max events per batch |
| `guardrails.max_event_size_bytes` | u64 | `10485760` | Max single event (10MB) |

**Example:**
```yaml
config:
  retry:
    max_attempts: 15
    initial_delay: 50ms
    max_delay: 60s
    multiplier: 2.5
    jitter: 0.2
    
  guardrails:
    max_lag: 10m
    max_queue_lag: 2m
    max_batch_size: 50000
    max_event_size_bytes: 52428800  # 50MB
```

---

## TLS Settings

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `tls.mode` | enum | `prefer` | TLS mode |
| `tls.ca_cert_path` | path | - | CA certificate file |
| `tls.ca_cert_pem` | string | - | CA certificate PEM (inline) |
| `tls.client_cert_path` | path | - | Client cert for mTLS |
| `tls.client_key_path` | path | - | Client key for mTLS |
| `tls.verify_hostname` | bool | `true` | Verify server hostname |
| `tls.sni_hostname` | string | - | SNI hostname override |

### TLS Modes

| Mode | Description |
|:-----|:------------|
| `disable` | No TLS |
| `prefer` | Use TLS if available |
| `require` | TLS required, no verification |
| `verify-ca` | Verify CA certificate |
| `verify-identity` | Verify CA and hostname |

**Example:**
```yaml
config:
  tls:
    mode: verify-identity
    ca_cert_path: /etc/ssl/certs/ca.pem
    client_cert_path: /etc/ssl/certs/client.pem
    client_key_path: /etc/ssl/private/client.key
    verify_hostname: true
```

---

## Event Format Settings

### Output Format

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `format` | enum | `json` | Event format |
| `envelope` | enum | `standard` | Message envelope |
| `key_format` | enum | `json` | Key serialization |

### Format Options

| Format | Description |
|:-------|:------------|
| `json` | JSON format |
| `avro` | Apache Avro |
| `protobuf` | Protocol Buffers |

### Envelope Styles

| Envelope | Description |
|:---------|:------------|
| `standard` | Industry-standard CDC format |
| `plain` | Just the data, no metadata |
| `full` | Rivven's extended format |

**Example:**
```yaml
config:
  format: avro
  envelope: standard
  key_format: json
```

### Schema Settings

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `schema_registry_url` | string | - | Schema registry URL |
| `schema_cache_size` | u32 | `1000` | Cache entries |
| `include_schema` | bool | `false` | Include schema in events |

---

## Type Handling

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `decimal_handling` | enum | `precise` | Decimal conversion |
| `time_precision` | enum | `microseconds` | Timestamp precision |
| `binary_handling` | enum | `base64` | Binary data encoding |
| `uuid_handling` | enum | `string` | UUID representation |

### Type Handling Options

| Option | Values | Description |
|:-------|:-------|:------------|
| `decimal_handling` | `precise`, `double`, `string` | Decimal representation |
| `time_precision` | `seconds`, `milliseconds`, `microseconds`, `nanoseconds` | Timestamp precision |
| `binary_handling` | `base64`, `hex`, `bytes` | Binary encoding |
| `uuid_handling` | `string`, `binary` | UUID format |

**Example:**
```yaml
config:
  decimal_handling: string  # Preserve precision
  time_precision: microseconds
  binary_handling: base64
  uuid_handling: string
```

---

## Field-Level Encryption

Encrypt sensitive fields at the source using AES-256-GCM encryption. This provides end-to-end encryption for PII, PCI, and other sensitive data.

### Encryption Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `cdc_features.encryption.enabled` | bool | `false` | Enable field encryption |
| `cdc_features.encryption.fields` | list | `[]` | Fields to encrypt |

### Example

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      # ... connection settings ...
      
      cdc_features:
        encryption:
          enabled: true
          fields:
            - credit_card
            - ssn
            - bank_account
            - tax_id
```

### Key Management

Encryption keys are managed via environment variables:

```bash
# Set the master encryption key (256-bit for AES-256-GCM)
export RIVVEN_ENCRYPTION_KEY="your-base64-encoded-32-byte-key"
```

### Security Features

- **AES-256-GCM**: Industry-standard authenticated encryption
- **Per-field encryption**: Only specified fields are encrypted
- **Transparent decryption**: Consumers with the same key can decrypt
- **Key rotation**: Support for versioned keys enables rotation

---

## Deduplication

Prevent duplicate events caused by network retries, replication lag, or connector restarts.

### Deduplication Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `cdc_features.deduplication.enabled` | bool | `false` | Enable deduplication |
| `cdc_features.deduplication.ttl_seconds` | u64 | `3600` | How long to remember seen events |
| `cdc_features.deduplication.max_entries` | u64 | `100000` | Max cache size (LRU eviction) |

### Example

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      cdc_features:
        deduplication:
          enabled: true
          ttl_seconds: 7200      # 2 hours
          max_entries: 500000    # 500K entries
```

### How It Works

Events are hashed using a combination of:
- Table name
- Primary key values
- Operation type
- Timestamp

If an event with the same hash is seen within the TTL window, it is dropped.

---

## Tombstone Handling

Properly emit tombstones (null-value records) for DELETE operations, enabling log compaction.

### Tombstone Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `cdc_features.tombstones.enabled` | bool | `false` | Enable tombstone emission |
| `cdc_features.tombstones.emit_tombstone_after_delete` | bool | `true` | Emit tombstone after delete event |

### Example

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      cdc_features:
        tombstones:
          enabled: true
          emit_tombstone_after_delete: true
```

### Why Tombstones Matter

In compacted topics, tombstones signal that a key should be deleted during compaction. Without proper tombstones:
- Deleted records may reappear after compaction
- Storage grows unbounded
- Downstream consumers see stale data

---

## Heartbeat Configuration

Heartbeat monitoring maintains replication slot health and provides lag detection for CDC connectors.

### Why Heartbeats Matter

- **PostgreSQL**: Without heartbeats, inactive replication slots accumulate WAL files, potentially filling disk
- **MySQL**: Keeps binlog position fresh during periods of no changes
- **Health Monitoring**: Detects stalled connections and replication lag

### Heartbeat Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `heartbeat_interval_secs` | int | `10` | Heartbeat interval in seconds |

The heartbeat tracks position updates on every event and calculates lag. If the lag exceeds a threshold (default: 30x interval = 5 minutes for 10s interval), the connector reports as unhealthy.

### Example

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      heartbeat_interval_secs: 10  # Update position every 10 seconds
```

### Monitoring Heartbeat Health

The heartbeat status is exposed via the connector's health check:
- **Healthy**: Lag is below threshold
- **Unhealthy**: Lag exceeds threshold (possible replication stall)

---

## Initial and Incremental Snapshots

Rivven provides comprehensive snapshot support for CDC connectors, including initial table synchronization and incremental (ad-hoc) snapshots for catching up on missed changes.

### Snapshot Modes

| Mode | Description |
|:-----|:------------|
| `initial` | Snapshot on first start, then stream (default) |
| `always` | Full snapshot on every start |
| `never` | Skip snapshot, stream only |
| `when_needed` | Snapshot only if no stored position available |
| `initial_only` | Snapshot and stop (no streaming) |
| `schema_only` | Capture schema, skip data |
| `recovery` | Snapshot for disaster recovery |

### Snapshot Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `snapshot_mode` | enum | `initial` | Snapshot behavior |
| `snapshot_batch_size` | int | `10000` | Rows per SELECT batch |
| `snapshot_parallel_tables` | int | `4` | Tables to snapshot concurrently |
| `snapshot_query_timeout_secs` | int | `300` | Query timeout |
| `snapshot_progress_dir` | string | - | Directory for progress persistence |

### Example Configuration

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      host: localhost
      port: 5432
      database: mydb
      
      # Snapshot settings
      snapshot_mode: initial
      snapshot_batch_size: 50000
      snapshot_parallel_tables: 4
      snapshot_progress_dir: /var/lib/rivven/progress
```

### Progress Persistence

Rivven stores progress in the **destination system** (broker or files), never in the source database:

| Storage Option | Use Case |
|:--------------|:---------|
| File-based | Single-node deployments |
| Broker topics | Distributed/HA deployments |

#### File-Based Progress (Default)

Rivven tracks snapshot progress to disk, enabling resumable snapshots:

```text
/var/lib/rivven/progress/
  public.users.json      # Progress for users table
  public.orders.json     # Progress for orders table
```

Each progress file contains:
- Rows processed
- Last key value (for resumption)
- Watermark (WAL position at snapshot start)
- State (pending, in_progress, completed, failed)

#### Broker-Based Progress (Distributed)

For distributed deployments, configure Rivven to store progress in dedicated topics:

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      # Use broker for offset storage (distributed mode)
      offset_storage: broker
      offset_topic: _rivven_cdc_offsets
```

**Why not store in the source database?**

1. Source databases might be read-only replicas
2. Avoid polluting source schemas with CDC metadata
3. Separation of concerns: CDC metadata belongs with CDC infrastructure
4. The broker is the natural location for consumer offset tracking

### Architecture

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Snapshot Coordinator                         │
├─────────────────────────────────────────────────────────────────┤
│  1. Get WAL position (watermark)                                │
│  2. Parallel SELECT with keyset pagination                      │
│  3. Emit events with op: Snapshot                               │
│  4. Save progress periodically                                  │
│  5. Start streaming from watermark                              │
└─────────────────────────────────────────────────────────────────┘
```

### Keyset Pagination

Rivven uses keyset pagination (cursor-based) instead of OFFSET for efficient, memory-safe batch fetching:

```sql
-- First batch
SELECT * FROM users ORDER BY id LIMIT 10000;

-- Subsequent batches (using last key)
SELECT * FROM users WHERE id > 'last_key' ORDER BY id LIMIT 10000;
```

This approach:
- Maintains consistent performance regardless of table size
- Avoids skipping or duplicating rows
- Allows resumption from any point

### Incremental Snapshots (Ad-hoc)

For catching up on missed data during streaming, Rivven supports DBLog-style incremental snapshots via signal tables:

```yaml
sources:
  production_postgres:
    connector: postgres-cdc
    config:
      signal_table: "rivven_signals"
      incremental_snapshot:
        enabled: true
        chunk_size: 10000
        watermark_strategy: insert_insert  # DBLog approach
```

Trigger an incremental snapshot by inserting a signal:

```sql
INSERT INTO rivven_signals (id, type, data)
VALUES (
  'snapshot-' || now()::text,
  'execute-snapshot',
  '{"data_collections": ["public.users", "public.orders"]}'::jsonb
);
```

---

## Single Message Transforms (SMT)

Apply transformations to CDC events before they reach their destination.
Transforms are applied in order, allowing powerful event processing pipelines.

### Available Transforms

| Transform | Description |
|:----------|:------------|
| `extract_new_record_state` | Flatten envelope, extract "after" state |
| `value_to_key` | Extract key fields from value |
| `mask_field` | Mask sensitive fields (PII, credit cards) |
| `insert_field` | Add static or computed fields |
| `replace_field` / `rename_field` | Rename, include, or exclude fields |
| `regex_router` | Route events based on regex patterns |
| `timestamp_converter` | Convert timestamp formats |
| `filter` | Filter events based on conditions |
| `cast` | Convert field types |
| `flatten` | Flatten nested JSON structures |
| `timezone_converter` | Convert timestamps between timezones |
| `content_router` | Route events based on field content |
| `header_to_value` | Copy envelope metadata into record |
| `unwrap` | Extract nested field to top level |
| `set_null` | Set fields to null conditionally |
| `compute_field` | Compute new fields (concat, hash, etc.) |

### Configuration Reference

#### extract_new_record_state

Flatten the CDC envelope to just the data, optionally adding metadata.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `drop_tombstones` | bool | `false` | Drop delete events |
| `add_table` | bool | `false` | Add `__table` field |
| `add_schema` | bool | `false` | Add `__schema` field |
| `add_op` | bool | `false` | Add `__op` field |
| `add_ts` | bool | `false` | Add `__ts` field |

```yaml
transforms:
  - type: extract_new_record_state
    config:
      drop_tombstones: false
      add_table: true
      add_op: true
```

#### mask_field

Mask sensitive data for compliance (GDPR, PCI-DSS, HIPAA).

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | list | required | Fields to mask |

```yaml
transforms:
  - type: mask_field
    config:
      fields:
        - ssn
        - credit_card_number
        - password_hash
```

#### timestamp_converter

Convert timestamps between formats.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | list | required | Fields to convert |
| `format` | enum | `iso8601` | Target format |

**Format Options:** `iso8601`, `epoch_seconds`, `epoch_millis`, `epoch_micros`, `date_only`, `time_only`

```yaml
transforms:
  - type: timestamp_converter
    config:
      fields:
        - created_at
        - updated_at
      format: iso8601
```

#### filter

Keep or drop events based on conditions.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `condition` | string | required | Filter condition |
| `drop` | bool | `false` | Drop matching (vs keep) |

**Supported Operators:** `=`, `!=`, `is_null`, `is_not_null`, `matches`, `in`

```yaml
transforms:
  # Keep only active users
  - type: filter
    config:
      condition: "status = active"
      drop: false
  
  # Drop test records
  - type: filter
    config:
      condition: "email matches .*@test\\.com"
      drop: true
```

#### cast

Convert field types.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `specs` | map | required | Field -> type mapping |

**Type Options:** `string`, `integer`, `float`, `boolean`, `json`

```yaml
transforms:
  - type: cast
    config:
      specs:
        price: float
        quantity: integer
        is_active: boolean
        metadata: json
```

#### flatten

Flatten nested JSON structures.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `delimiter` | string | `.` | Key separator |
| `max_depth` | int | `0` | Max depth (0=unlimited) |

```yaml
transforms:
  - type: flatten
    config:
      delimiter: "_"
      max_depth: 3
```

**Before:**
```json
{"user": {"address": {"city": "NYC"}}}
```

**After:**
```json
{"user_address_city": "NYC"}
```

#### insert_field

Add static or computed fields.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `static_fields` | map | - | Static values |
| `timestamp_field` | string | - | Add current timestamp |
| `date_field` | string | - | Add current date |

```yaml
transforms:
  - type: insert_field
    config:
      static_fields:
        source: "postgres-cdc"
        version: "1.0"
      timestamp_field: _ingested_at
```

#### replace_field / rename_field

Rename fields or filter to specific fields.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `renames` | map | - | old_name -> new_name |
| `include` | list | - | Only keep these fields |
| `exclude` | list | - | Remove these fields |

```yaml
transforms:
  - type: replace_field
    config:
      renames:
        usr_id: user_id
        ts: timestamp
      exclude:
        - internal_notes
        - debug_info
```

#### value_to_key

Extract fields from value to use as message key.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | list | required | Fields for key |

```yaml
transforms:
  - type: value_to_key
    config:
      fields:
        - id
        - tenant_id
```

#### regex_router

Route events to different topics based on patterns.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `default_topic` | string | `default` | Fallback topic |
| `routes` | list | - | Pattern -> topic rules |

```yaml
transforms:
  - type: regex_router
    config:
      default_topic: events.other
      routes:
        - pattern: "^orders.*"
          topic: events.orders
        - pattern: "^users.*"
          topic: events.users
```

#### timezone_converter

Convert timestamp fields between timezones (IANA timezone names).

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | list | required | Fields to convert |
| `from` | string | `UTC` | Source timezone |
| `to` | string | required | Target timezone |
| `date_only` | bool | `false` | Output date only |

```yaml
transforms:
  - type: timezone_converter
    config:
      fields:
        - created_at
        - updated_at
      from: UTC
      to: America/New_York
```

#### content_router

Route events based on field values or patterns.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `default_topic` | string | `default` | Fallback topic |
| `routes` | list | - | Field/value/pattern -> topic rules |

```yaml
transforms:
  - type: content_router
    config:
      default_topic: events.default
      routes:
        # Exact value matching
        - field: region
          value: us-east
          topic: events.us-east
        # Pattern matching
        - field: email
          pattern: ".*@enterprise\\.com"
          topic: events.enterprise
```

#### header_to_value

Copy envelope metadata (source, table, operation, etc.) into the record.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | map | - | source_field -> target_field mapping |
| `all_headers_prefix` | string | - | Add all headers with this prefix |
| `move` | bool | `false` | Move (remove from envelope) vs copy |

**Source Fields:** `source_type`, `database`, `schema`, `table`, `operation`, `timestamp`, `transaction_id`

```yaml
transforms:
  - type: header_to_value
    config:
      fields:
        source_type: db_source
        table: source_table
        operation: op_type
      # Or add all with prefix:
      # all_headers_prefix: "__"
```

#### unwrap

Extract a nested field to the top level.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `field` | string | required | Nested field path |

```yaml
transforms:
  - type: unwrap
    config:
      field: payload.data
```

**Before:**
```json
{"id": 1, "payload": {"data": {"name": "test", "value": 42}}}
```

**After:**
```json
{"id": 1, "name": "test", "value": 42}
```

#### set_null

Set specified fields to null, optionally with conditions.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `fields` | list | required | Fields to nullify |
| `condition` | string/object | `always` | When to nullify |

**Conditions:** `always`, `if_empty`, `{ "equals": value }`, `{ "matches": "pattern" }`

```yaml
transforms:
  # Always nullify
  - type: set_null
    config:
      fields:
        - password
        - api_key
  
  # Conditionally nullify empty strings
  - type: set_null
    config:
      fields:
        - description
      condition: if_empty
```

#### compute_field

Compute new fields from existing data.

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `computations` | list | required | List of computation specs |

**Computation Types:**
- `concat`: Concatenate field values
- `hash`: Hash field values (SHA-256)
- `upper` / `lower`: Case conversion
- `coalesce`: First non-null value
- `uuid`: Generate UUID
- `timestamp`: Current timestamp

```yaml
transforms:
  - type: compute_field
    config:
      computations:
        # Concatenate fields
        - target: full_name
          type: concat
          parts:
            - first_name
            - " "
            - last_name
        
        # Hash for anonymization
        - target: email_hash
          type: hash
          fields:
            - email
        
        # Case conversion
        - target: email_lower
          type: lower
          source: email
        
        # Default value
        - target: status
          type: coalesce
          fields:
            - status
            - default_status
        
        # Generate ID
        - target: event_id
          type: uuid
        
        # Add processing timestamp
        - target: processed_at
          type: timestamp
```

### Transform Pipeline Example

Chain multiple transforms for complex processing:

```yaml
config:
  transforms:
    # 1. Flatten envelope
    - type: extract_new_record_state
      config:
        add_table: true
        add_op: true
    
    # 2. Mask PII
    - type: mask_field
      config:
        fields: [ssn, credit_card]
    
    # 3. Filter out test data
    - type: filter
      config:
        condition: "email matches .*@test\\.com"
        drop: true
    
    # 4. Normalize timestamps
    - type: timestamp_converter
      config:
        fields: [created_at, updated_at]
        format: iso8601
    
    # 5. Route to topics
    - type: regex_router
      config:
        routes:
          - pattern: "^public\\.orders.*"
            topic: cdc.orders
          - pattern: "^public\\.users.*"
            topic: cdc.users
```

---

## Monitoring Settings

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `metrics.enabled` | bool | `true` | Enable metrics |
| `metrics.port` | u16 | `8080` | Metrics HTTP port |
| `metrics.path` | string | `/metrics` | Metrics endpoint |
| `metrics.labels` | map | `{}` | Custom labels |

**Example:**
```yaml
config:
  metrics:
    enabled: true
    port: 9090
    path: /prometheus/metrics
    labels:
      environment: production
      team: platform
```

---

## PostgreSQL-Specific Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `slot_name` | string | `rivven_slot` | Replication slot name |
| `publication_name` | string | `rivven_pub` | Publication name |
| `plugin` | enum | `pgoutput` | Output plugin |
| `replication_mode` | enum | `logical` | Replication mode |

### PostgreSQL Signal Table

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `signal_table.enabled` | bool | `false` | Enable signal table |
| `signal_table.schema` | string | `rivven` | Signal table schema |
| `signal_table.name` | string | `signals` | Signal table name |

**Example:**
```yaml
config:
  slot_name: my_app_slot
  publication_name: my_app_pub
  plugin: pgoutput
  
  signal_table:
    enabled: true
    schema: rivven
    name: signals
```

---

## MySQL-Specific Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `server_id` | u32 | required | Unique server ID |
| `gtid_mode` | bool | `true` | Use GTID |
| `gtid_set` | string | - | Starting GTID set |
| `binlog_filename` | string | - | Starting binlog file |
| `binlog_position` | u64 | - | Starting position |

### MariaDB Options

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `mariadb_gtid` | bool | `false` | Use MariaDB GTID format |
| `gtid_domain_id` | u32 | - | MariaDB domain ID |

**Example:**
```yaml
config:
  server_id: 12345
  gtid_mode: true
  # MySQL GTID set
  gtid_set: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
```

---

## Environment Variable Reference

All options can be set via environment variables using this pattern:
`RIVVEN_CDC_<SECTION>_<OPTION>` (uppercase, underscores)

| Config Path | Environment Variable |
|:------------|:--------------------|
| `host` | `RIVVEN_CDC_HOST` |
| `password` | `RIVVEN_CDC_PASSWORD` |
| `tls.mode` | `RIVVEN_CDC_TLS_MODE` |
| `retry.max_attempts` | `RIVVEN_CDC_RETRY_MAX_ATTEMPTS` |
| `guardrails.max_lag` | `RIVVEN_CDC_GUARDRAILS_MAX_LAG` |

**Priority:** Environment variables > YAML config > Defaults

---

## Full Configuration Example

```yaml
version: "1.0"

sources:
  production_postgres:
    connector: postgres-cdc
    topic: cdc.production
    enabled: true
    
    config:
      # Connection
      host: postgres.internal
      port: 5432
      database: production
      user: rivven_cdc
      password: ${POSTGRES_PASSWORD}
      
      # TLS
      tls:
        mode: verify-identity
        ca_cert_path: /etc/ssl/certs/ca.pem
        client_cert_path: /etc/ssl/certs/client.pem
        client_key_path: /etc/ssl/private/client.key
      
      # PostgreSQL-specific
      slot_name: rivven_prod_slot
      publication_name: rivven_prod_pub
      plugin: pgoutput
      
      signal_table:
        enabled: true
        schema: rivven
        name: signals
      
      # Table selection
      tables:
        - public.orders
        - public.order_items
        - public.customers
      exclude_tables:
        - "*_backup"
        - "*_archive"
      
      column_masks:
        - public.customers.ssn
        - public.customers.credit_card_number
      
      # Snapshot
      snapshot_mode: initial
      snapshot_batch_size: 50000
      snapshot_parallel_tables: 4
      
      # Performance
      batch_size: 5000
      batch_timeout: 200ms
      max_queue_size: 20000
      workers: 8
      
      backpressure:
        enabled: true
        high_watermark: 15000
        low_watermark: 5000
      
      # Resilience
      retry:
        max_attempts: 15
        initial_delay: 100ms
        max_delay: 60s
        multiplier: 2.0
        jitter: 0.1
      
      guardrails:
        max_lag: 5m
        max_queue_lag: 1m
        max_batch_size: 100000
        max_event_size_bytes: 10485760
      
      # Event format
      format: json
      envelope: standard
      decimal_handling: string
      time_precision: microseconds
      
      # Monitoring
      metrics:
        enabled: true
        port: 9090
        labels:
          environment: production
          service: cdc
```

---

## Event Routing

Route CDC events to different destinations based on content, table, operation, or custom conditions.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `router.enabled` | bool | `false` | Enable event routing |
| `router.default_destination` | string | - | Default destination for unmatched events |
| `router.dead_letter_queue` | string | - | Topic for unroutable events |
| `router.drop_unroutable` | bool | `false` | Drop events that don't match any rule |
| `router.rules` | list | `[]` | Routing rules (evaluated in priority order) |

### Route Rule Options

| Option | Type | Description |
|:-------|:-----|:------------|
| `name` | string | Rule name for logging/debugging |
| `priority` | int | Higher priority rules evaluated first (default: 0) |
| `condition` | object | Matching condition |
| `destinations` | list | Target topics for matched events |
| `continue_matching` | bool | Continue to next rule after match (default: false) |

### Condition Types

| Type | Description | Fields |
|:-----|:------------|:-------|
| `Always` | Always matches | - |
| `Table` | Match specific table | `table` |
| `TablePattern` | Match table via regex | `pattern` |
| `Schema` | Match database schema | `schema` |
| `Operation` | Match operation type | `op` (insert/update/delete) |
| `FieldExists` | Check field existence | `field` |
| `FieldValue` | Match field value | `field`, `value` |
| `FieldPattern` | Match field via regex | `field`, `pattern` |
| `Header` | Match event header | `header`, `value` |
| `And` | All conditions must match | `conditions` |
| `Or` | Any condition must match | `conditions` |
| `Not` | Negate a condition | `condition` |

**Example:**
```yaml
config:
  router:
    enabled: true
    default_destination: default-events
    dead_letter_queue: dlq-events
    rules:
      - name: high_priority_orders
        priority: 100
        condition:
          type: And
          conditions:
            - type: Table
              table: public.orders
            - type: FieldValue
              field: priority
              value: high
        destinations:
          - priority-orders
        continue_matching: false
      
      - name: customer_changes
        priority: 50
        condition:
          type: TablePattern
          pattern: "public\\.customer.*"
        destinations:
          - customer-events
      
      - name: audit_deletes
        priority: 10
        condition:
          type: Operation
          op: delete
        destinations:
          - audit-topic
          - delete-archive
        continue_matching: true
```

---

## Partitioning

Control how events are distributed across topic partitions for ordering guarantees and parallelism.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `partitioner.enabled` | bool | `false` | Enable custom partitioning |
| `partitioner.num_partitions` | u32 | `1` | Number of partitions |
| `partitioner.strategy` | object | - | Partitioning strategy |

### Partition Strategies

| Strategy | Description | Use Case |
|:---------|:------------|:---------|
| `RoundRobin` | Distribute evenly | Maximum throughput |
| `KeyHash` | Hash primary key | Per-row ordering |
| `TableHash` | Hash table name | Per-table ordering |
| `FullTableHash` | Hash schema.table | Multi-schema environments |
| `Sticky` | Same partition per batch | Batch locality |

**Example:**
```yaml
config:
  partitioner:
    enabled: true
    num_partitions: 16
    strategy:
      type: KeyHash
      # Or: type: TableHash
      # Or: type: RoundRobin
```

---

## Pipeline Processing

Build composable CDC processing pipelines with stages for filtering, transformation, and routing.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `pipeline.enabled` | bool | `false` | Enable pipeline processing |
| `pipeline.name` | string | - | Pipeline name for logging |
| `pipeline.dead_letter_queue` | string | - | Topic for failed events |
| `pipeline.concurrency` | int | `1` | Parallel processing workers |
| `pipeline.stages` | list | `[]` | Processing stages |

### Stage Types

| Stage | Description |
|:------|:------------|
| `Filter` | Drop events matching condition |
| `Transform` | Apply SMT transforms |
| `Route` | Content-based routing |

**Example:**
```yaml
config:
  pipeline:
    enabled: true
    name: order-processing
    dead_letter_queue: pipeline-dlq
    concurrency: 4
    stages:
      - type: Filter
        condition:
          type: FieldValue
          field: status
          value: draft
      
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
              field: priority
              value: urgent
            destinations:
              - urgent-orders
```

---

## Log Compaction

Keep only the latest state per key to reduce storage and replay time.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `compaction.enabled` | bool | `false` | Enable log compaction |
| `compaction.key_columns` | list | `[]` | Columns forming the compaction key |
| `compaction.min_cleanable_ratio` | f64 | `0.5` | Trigger compaction at this duplicate ratio |
| `compaction.segment_size` | u64 | `104857600` | Segment size in bytes (100MB) |
| `compaction.delete_retention_ms` | u64 | `86400000` | Keep tombstones for 24 hours |
| `compaction.min_compaction_lag_ms` | u64 | `0` | Minimum age before compaction |
| `compaction.max_compaction_lag_ms` | u64 | `0` | Force compaction after this age (0 = disabled) |
| `compaction.cleanup_policy` | string | `compact` | `compact`, `delete`, or `compact_delete` |

**Example:**
```yaml
config:
  compaction:
    enabled: true
    key_columns:
      - id
    min_cleanable_ratio: 0.5
    segment_size: 104857600
    delete_retention_ms: 86400000
    cleanup_policy: compact
```

---

## Parallel CDC Processing

Process multiple tables concurrently for maximum throughput.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `parallel.enabled` | bool | `false` | Enable parallel processing |
| `parallel.concurrency` | int | `4` | Max concurrent table streams |
| `parallel.per_table_buffer` | int | `1000` | Events to buffer per table |
| `parallel.work_stealing` | bool | `true` | Rebalance work across threads |
| `parallel.backpressure_threshold` | f64 | `0.8` | Throttle at this buffer utilization |
| `parallel.batch_timeout_ms` | u64 | `100` | Max time to accumulate batch |

**Example:**
```yaml
config:
  parallel:
    enabled: true
    concurrency: 8
    per_table_buffer: 1000
    work_stealing: true
    backpressure_threshold: 0.8
    batch_timeout_ms: 100
```

---

## Transactional Outbox Pattern

Reliably publish events using the transactional outbox pattern.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `outbox.enabled` | bool | `false` | Enable outbox pattern |
| `outbox.table_name` | string | `outbox` | Outbox table name |
| `outbox.poll_interval_ms` | u64 | `1000` | Polling interval in ms |
| `outbox.batch_size` | int | `100` | Events per batch |
| `outbox.max_retries` | int | `3` | Retry failed events |
| `outbox.retry_delay_ms` | u64 | `1000` | Delay between retries |
| `outbox.delete_after_publish` | bool | `true` | Delete processed events |

### Outbox Table Schema

```sql
CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    published BOOLEAN DEFAULT FALSE,
    retries INTEGER DEFAULT 0
);

CREATE INDEX idx_outbox_unpublished ON outbox (timestamp) WHERE NOT published;
```

**Example:**
```yaml
config:
  outbox:
    enabled: true
    table_name: outbox
    poll_interval_ms: 1000
    batch_size: 100
    max_retries: 3
    delete_after_publish: true
```

---

## Health Monitoring

Monitor CDC connector health with automatic failure detection and recovery.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `health.enabled` | bool | `false` | Enable health monitoring |
| `health.check_interval_secs` | u64 | `10` | Interval between health checks |
| `health.max_lag_ms` | u64 | `30000` | Maximum allowed replication lag |
| `health.failure_threshold` | u32 | `3` | Failed checks before unhealthy |
| `health.success_threshold` | u32 | `2` | Successful checks to recover |
| `health.check_timeout_secs` | u64 | `5` | Timeout for health checks |
| `health.auto_recovery` | bool | `true` | Enable automatic recovery |
| `health.recovery_delay_secs` | u64 | `1` | Initial recovery delay |
| `health.max_recovery_delay_secs` | u64 | `60` | Maximum recovery delay (backoff) |

### Health States

| State | Description |
|:------|:------------|
| `Healthy` | All checks passing |
| `Degraded` | Some checks passing with warnings |
| `Unhealthy` | Failure threshold exceeded |
| `Recovering` | Auto-recovery in progress |

**Example:**
```yaml
config:
  health:
    enabled: true
    check_interval_secs: 10
    max_lag_ms: 30000
    failure_threshold: 3
    success_threshold: 2
    auto_recovery: true
    recovery_delay_secs: 1
    max_recovery_delay_secs: 60
```

### Auto-Recovery Flow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Health Monitoring Flow                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  HEALTHY ──[3 failures]──► UNHEALTHY ──[auto_recovery]──► RECOVERING        │
│     ▲                                                          │            │
│     │                                                          │            │
│     └──────────────────[2 successes]───────────────────────────┘            │
│                                                                              │
│  Recovery uses exponential backoff:                                          │
│    1s → 2s → 4s → 8s → ... → max_recovery_delay_secs                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Prometheus Metrics

The health monitor emits the following Prometheus metrics:

| Metric | Type | Description |
|:-------|:-----|:------------|
| `rivven_cdc_health_monitoring_enabled` | Gauge | Whether health monitoring is enabled |
| `rivven_cdc_health_state_healthy` | Gauge | Current health state (1=healthy, 0=unhealthy) |
| `rivven_cdc_health_state_ready` | Gauge | Current readiness state |
| `rivven_cdc_health_checks_passed_total` | Counter | Total health checks that passed |
| `rivven_cdc_health_checks_failed_total` | Counter | Total health checks that failed |
| `rivven_cdc_health_state_transitions_total` | Counter | Health state transitions (by direction) |
| `rivven_cdc_health_recoveries_succeeded_total` | Counter | Successful recovery attempts |
| `rivven_cdc_health_recoveries_failed_total` | Counter | Failed recovery attempts |
| `rivven_cdc_health_unhealthy_time_ms_total` | Counter | Total time spent in unhealthy state |

### Kubernetes Probes

The health monitor provides K8s-compatible liveness and readiness probes:

```rust
// Liveness probe (is the process alive?)
let (status, msg) = processor.liveness_probe().await;
// Returns (200, "OK") or (503, "Service Unavailable")

// Readiness probe (can we serve traffic?)
let (status, msg) = processor.readiness_probe().await;

// JSON health endpoint
let json = processor.health_json().await;
// {"status":"healthy","ready":true,"lag_ms":100,"uptime_secs":3600,...}
```

### Custom Health Checks

Register custom health checks for application-specific monitoring:

```rust
// Register a database connectivity check
processor.register_health_check("database", || async {
    match check_database_connection().await {
        Ok(_) => HealthCheckResult::Healthy,
        Err(e) => HealthCheckResult::Unhealthy(e.to_string()),
    }
}).await;

// Unregister when no longer needed
processor.unregister_health_check("database").await;
```

---

## Notifications

Subscribe to CDC progress and status notifications for operational visibility.

### Configuration

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `notifications.enabled` | bool | `false` | Enable notifications |
| `notifications.channels` | list | `[]` | Notification channels |
| `notifications.snapshot_progress` | bool | `true` | Snapshot progress events |
| `notifications.streaming_status` | bool | `true` | Streaming status events |
| `notifications.error_notifications` | bool | `true` | Error notifications |
| `notifications.min_interval_ms` | u64 | `1000` | Debounce interval |

### Channel Types

| Type | Description |
|:-----|:------------|
| `log` | Log notifications (configurable level) |
| `webhook` | HTTP webhook notifications |
| `metrics` | Emit as Prometheus metrics |

### Notification Types

| Type | Description |
|:-----|:------------|
| `InitialSnapshotStarted` | Initial snapshot began |
| `InitialSnapshotInProgress` | Snapshot progress update |
| `InitialSnapshotTableCompleted` | Single table completed |
| `InitialSnapshotCompleted` | All tables completed |
| `StreamingStarted` | Streaming began |
| `StreamingLagUpdate` | Replication lag changed |
| `StreamingError` | Streaming error occurred |
| `ConnectorPaused` | Connector paused via signal |
| `ConnectorResumed` | Connector resumed |
| `SchemaChangeDetected` | DDL change detected |

**Example:**
```yaml
config:
  notifications:
    enabled: true
    snapshot_progress: true
    streaming_status: true
    error_notifications: true
    min_interval_ms: 1000
    channels:
      - type: log
        level: info
      
      - type: webhook
        url: https://api.example.com/cdc-events
        authorization: "Bearer ${CDC_WEBHOOK_TOKEN}"
        timeout_secs: 10
      
      - type: metrics
        prefix: rivven_cdc
```

### Webhook Payload

```json
{
  "type": "INITIAL_SNAPSHOT_IN_PROGRESS",
  "connector_id": "orders-cdc",
  "timestamp": "2026-02-02T10:30:00Z",
  "data": {
    "table": "public.orders",
    "rows_completed": 50000,
    "total_rows": 100000,
    "percent_complete": 50.0
  }
}
```

---

## Next Steps

- [PostgreSQL CDC Guide](cdc-postgres) - PostgreSQL setup
- [MySQL CDC Guide](cdc-mysql) - MySQL/MariaDB setup
- [CDC Troubleshooting](cdc-troubleshooting) - Debug issues
