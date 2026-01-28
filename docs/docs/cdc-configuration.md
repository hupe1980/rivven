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

| Option | Type | Default | Description |
|:-------|:-----|:--------|:------------|
| `snapshot_mode` | enum | `initial` | Snapshot behavior |
| `snapshot_batch_size` | u64 | `10000` | Rows per batch |
| `snapshot_parallel_tables` | u32 | `1` | Tables to snapshot in parallel |
| `snapshot_lock_timeout` | duration | `30s` | Lock acquisition timeout |
| `snapshot_query_timeout` | duration | `1h` | Query timeout |
| `snapshot_select_override` | map | - | Custom SELECT per table |

### Snapshot Modes

| Mode | Description |
|:-----|:------------|
| `initial` | Snapshot on first start, then stream |
| `always` | Full snapshot on every start |
| `never` | No snapshot, stream only |
| `when_needed` | Snapshot only if position unavailable |
| `recovery` | Snapshot for disaster recovery |

**Example:**
```yaml
config:
  snapshot_mode: initial
  snapshot_batch_size: 50000
  snapshot_parallel_tables: 4
  snapshot_select_override:
    public.large_table: "SELECT id, name FROM large_table WHERE archived = false"
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
| `envelope` | enum | `debezium` | Message envelope |
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
| `debezium` | Debezium-compatible format |
| `plain` | Just the data, no metadata |
| `full` | Rivven's extended format |

**Example:**
```yaml
config:
  format: avro
  envelope: debezium
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
      envelope: debezium
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

## Next Steps

- [PostgreSQL CDC Guide](cdc-postgres) - PostgreSQL setup
- [MySQL CDC Guide](cdc-mysql) - MySQL/MariaDB setup
- [CDC Troubleshooting](cdc-troubleshooting) - Debug issues
