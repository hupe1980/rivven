---
layout: default
title: PostgreSQL CDC Guide
parent: Change Data Capture
nav_order: 1
---

# PostgreSQL CDC Guide
{: .no_toc }

Complete guide to Change Data Capture from PostgreSQL databases.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven CDC for PostgreSQL uses **logical replication** via the `pgoutput` plugin to stream row-level changes in real-time. This guide covers:

- Prerequisites and database setup
- Configuration options
- TLS/mTLS security
- Advanced features (signals, incremental snapshots)
- Troubleshooting

---

## Prerequisites

### PostgreSQL Version

PostgreSQL **10+** is required (logical replication support).

| Version | Support | Notes |
|:--------|:--------|:------|
| 9.x | ❌ | No logical replication |
| 10.x | ✅ | Basic pgoutput |
| 11.x | ✅ | Truncate events added |
| 12.x | ✅ | Binary protocol optimization |
| 13.x | ✅ | Logical decoding on standby |
| 14.x | ✅ | Streaming large transactions |
| 15.x | ✅ | Row filters, column lists |
| 16.x | ✅ | Parallel apply |

### Database Configuration

Edit `postgresql.conf`:

```ini
# Required settings
wal_level = logical              # Enable logical decoding
max_replication_slots = 4        # At least 1 per CDC connector
max_wal_senders = 4              # At least 1 per CDC connector

# Recommended settings
wal_keep_size = 1GB              # Prevent slot from blocking WAL recycling
hot_standby_feedback = on        # If using read replicas
```

Restart PostgreSQL after changes.

### User Permissions

Create a dedicated replication user:

```sql
-- Create user with replication privilege
CREATE ROLE rivven WITH REPLICATION LOGIN PASSWORD 'secure_password';

-- Grant SELECT on tables to capture
GRANT SELECT ON ALL TABLES IN SCHEMA public TO rivven;
GRANT USAGE ON SCHEMA public TO rivven;

-- For auto-provisioning (optional)
GRANT CREATE ON DATABASE mydb TO rivven;
```

**Superuser Alternative** (development only):

```sql
ALTER ROLE rivven SUPERUSER;
```

---

## Basic Configuration

### Minimal Setup

```rust
use rivven_cdc::postgres::{PostgresCdc, PostgresCdcConfig};
use rivven_cdc::CdcSource;

let config = PostgresCdcConfig::builder()
    .connection_string("postgres://rivven:password@localhost:5432/mydb")
    .slot_name("rivven_slot")
    .publication_name("rivven_pub")
    .build()?;

let mut cdc = PostgresCdc::new(config);

// Process events
while let Some(event) = cdc.next().await? {
    println!("Event: {:?}", event);
}
```

### YAML Configuration (rivven-connect)

```yaml
version: "1.0"

sources:
  orders_cdc:
    connector: postgres-cdc
    topic: cdc.orders
    config:
      # Connection
      host: localhost
      port: 5432
      database: shop
      user: rivven
      password: ${POSTGRES_PASSWORD}
      
      # Replication
      slot_name: rivven_orders
      publication_name: rivven_orders_pub
      
      # Tables (optional, default: all)
      tables:
        - public.orders
        - public.order_items
```

---

## Configuration Reference

### Connection Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `host` | string | `localhost` | PostgreSQL host |
| `port` | u16 | `5432` | PostgreSQL port |
| `database` | string | required | Database name |
| `user` | string | required | Username |
| `password` | string | required | Password |
| `connection_string` | string | - | Alternative: full connection URI |
| `connect_timeout` | duration | `30s` | Connection timeout |
| `socket_timeout` | duration | `60s` | Socket read/write timeout |

### Replication Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `slot_name` | string | required | Replication slot name (unique) |
| `publication_name` | string | required | Publication name |
| `auto_create_slot` | bool | `true` | Auto-create slot if missing |
| `auto_create_publication` | bool | `true` | Auto-create publication if missing |
| `drop_slot_on_stop` | bool | `false` | Drop slot on shutdown (dev only) |

### Table Selection

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `tables` | list | `[]` | Tables to include (empty = all) |
| `exclude_tables` | list | `[]` | Tables to exclude |
| `table_pattern` | regex | - | Regex pattern for table names |

### Snapshot Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `snapshot_mode` | enum | `initial` | Snapshot behavior (see below) |
| `snapshot_batch_size` | u64 | `10000` | Rows per snapshot batch |
| `snapshot_parallel` | u32 | `1` | Parallel snapshot threads |
| `snapshot_fetch_size` | u32 | `1024` | Server-side cursor fetch size |

**Snapshot Modes:**

| Mode | Description |
|:-----|:------------|
| `always` | Full snapshot on every start |
| `initial` | Snapshot on first start only (default) |
| `initial_only` | Snapshot and stop (no streaming) |
| `schema_only` | Capture schema, no data |
| `when_needed` | Snapshot if no valid offsets |
| `never` | No snapshot, streaming only |

### TLS Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `tls.mode` | enum | `prefer` | TLS mode |
| `tls.ca_cert_path` | path | - | CA certificate file |
| `tls.client_cert_path` | path | - | Client certificate (mTLS) |
| `tls.client_key_path` | path | - | Client private key (mTLS) |
| `tls.verify_hostname` | bool | `true` | Verify server hostname |

**TLS Modes:**

| Mode | Description |
|:-----|:------------|
| `disable` | No TLS |
| `prefer` | TLS if available (default) |
| `require` | TLS required, no cert validation |
| `verify-ca` | TLS + CA certificate validation |
| `verify-full` | TLS + CA + hostname validation |

### Advanced Settings

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `heartbeat_interval` | duration | `10s` | Heartbeat frequency |
| `heartbeat_action_query` | string | - | Custom SQL on heartbeat |
| `max_batch_size` | u64 | `8192` | Max events per batch |
| `lsn_commit_timeout` | duration | `5s` | LSN commit timeout |
| `wal_sender_timeout` | duration | `60s` | Replication timeout |

---

## TLS Configuration

### Basic TLS (verify-ca)

```yaml
config:
  tls:
    mode: verify-ca
    ca_cert_path: /etc/ssl/certs/postgresql-ca.crt
```

### Mutual TLS (mTLS)

```yaml
config:
  tls:
    mode: verify-full
    ca_cert_path: /etc/ssl/certs/ca.crt
    client_cert_path: /etc/ssl/certs/client.crt
    client_key_path: /etc/ssl/private/client.key
```

### AWS RDS

```yaml
config:
  tls:
    mode: verify-full
    ca_cert_path: /etc/ssl/certs/rds-ca-2019-root.pem
```

Download RDS CA certificate:
```bash
curl -o /etc/ssl/certs/rds-ca-2019-root.pem \
  https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem
```

---

## Authentication

### SCRAM-SHA-256 (Recommended)

Rivven supports modern PostgreSQL SCRAM authentication:

```sql
-- PostgreSQL 10+: Ensure scram-sha-256 is configured
-- In pg_hba.conf:
host    all    rivven    0.0.0.0/0    scram-sha-256
```

No additional configuration needed - Rivven auto-negotiates.

### MD5 (Legacy)

MD5 authentication is supported but not recommended:

```sql
-- In pg_hba.conf:
host    all    rivven    0.0.0.0/0    md5
```

---

## Signal Table

Control CDC connectors at runtime without restarts.

### Setup

```sql
-- Create signal table
CREATE TABLE public.debezium_signal (
    id VARCHAR(64) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data JSONB
);

-- Grant permissions
GRANT SELECT, INSERT, DELETE ON public.debezium_signal TO rivven;
```

### Configuration

```yaml
config:
  signal_config:
    enabled: true
    data_collection: public.debezium_signal
    channels:
      - source  # Read from database
      - topic   # Read from Rivven topic
      - file    # Read from file
```

### Signal Types

| Signal | Description | Example |
|:-------|:------------|:--------|
| `execute-snapshot` | Trigger incremental snapshot | `{"data-collections": ["public.users"]}` |
| `stop-snapshot` | Cancel running snapshot | `{}` |
| `pause` | Pause CDC streaming | `{}` |
| `resume` | Resume CDC streaming | `{}` |

### Example: Trigger Snapshot

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (
    'signal-001',
    'execute-snapshot',
    '{"data-collections": ["public.orders"]}'::jsonb
);
```

---

## Incremental Snapshots

Re-snapshot specific tables while streaming continues (DBLog algorithm).

### Configuration

```yaml
config:
  incremental_snapshot:
    enabled: true
    chunk_size: 10000        # Rows per chunk
    watermark_strategy: insert_delete  # or insert_insert
    signal_data_collection: public.debezium_signal
```

### Trigger via Signal

```sql
INSERT INTO public.debezium_signal (id, type, data)
VALUES (
    'incr-snap-001',
    'execute-snapshot',
    '{"type": "incremental", "data-collections": ["public.orders"]}'::jsonb
);
```

### Watermark Strategies

| Strategy | Description | Signal Table Impact |
|:---------|:------------|:--------------------|
| `insert_insert` | Insert open marker, insert close marker | Table grows |
| `insert_delete` | Insert open marker, delete on close | Table stays small |

---

## Read Replica Support

Capture changes from PostgreSQL standbys (PostgreSQL 13+).

### Requirements

1. PostgreSQL 13+ on primary
2. `hot_standby_feedback = on` on replica
3. Replication slot created on primary

### Configuration

```yaml
config:
  read_only: true
  heartbeat_watermarking: true
  signal_config:
    enabled: true
    channels:
      - topic  # Cannot write to source on replica
      - file
```

---

## Monitoring

### Prometheus Metrics

```
# Core metrics
rivven_cdc_events_total{connector="orders_cdc",op="insert"}
rivven_cdc_events_total{connector="orders_cdc",op="update"}
rivven_cdc_events_total{connector="orders_cdc",op="delete"}
rivven_cdc_lag_milliseconds{connector="orders_cdc"}
rivven_cdc_connected{connector="orders_cdc"}

# Snapshot metrics
rivven_cdc_snapshot_running{connector="orders_cdc"}
rivven_cdc_snapshot_rows_scanned{connector="orders_cdc",table="orders"}
rivven_cdc_snapshot_duration_ms{connector="orders_cdc"}

# Performance metrics
rivven_cdc_processing_time_p99_us{connector="orders_cdc"}
rivven_cdc_batch_size{connector="orders_cdc"}
```

### Grafana Dashboard

Import the Rivven CDC dashboard (ID: TBD) or use these panels:

1. **Event Rate**: `rate(rivven_cdc_events_total[5m])`
2. **Lag**: `rivven_cdc_lag_milliseconds`
3. **Error Rate**: `rate(rivven_cdc_errors_total[5m])`
4. **Connection Status**: `rivven_cdc_connected`

---

## Troubleshooting

### Common Issues

#### "replication slot does not exist"

**Cause:** Slot was dropped or never created.

**Solution:**
```yaml
config:
  auto_create_slot: true  # Or create manually
```

Manual creation:
```sql
SELECT pg_create_logical_replication_slot('rivven_slot', 'pgoutput');
```

#### "publication does not exist"

**Cause:** Publication was dropped or never created.

**Solution:**
```yaml
config:
  auto_create_publication: true  # Or create manually
```

Manual creation:
```sql
CREATE PUBLICATION rivven_pub FOR ALL TABLES;
-- Or specific tables:
CREATE PUBLICATION rivven_pub FOR TABLE orders, customers;
```

#### "must be superuser or replication role"

**Cause:** User lacks replication privileges.

**Solution:**
```sql
ALTER ROLE rivven WITH REPLICATION;
```

#### "WAL segment has been removed"

**Cause:** CDC fell too far behind and WAL was recycled.

**Solution:**
1. Increase `wal_keep_size` in `postgresql.conf`
2. Use replication slots (they prevent WAL recycling)
3. Ensure CDC is consuming fast enough

```ini
# postgresql.conf
wal_keep_size = 5GB  # Increase from default
```

#### High Replication Lag

**Cause:** CDC not keeping up with change rate.

**Solution:**
1. Increase batch size:
   ```yaml
   config:
     max_batch_size: 16384
   ```
2. Check downstream sink performance
3. Consider parallel processing

#### TLS Connection Failures

**Cause:** Certificate mismatch or path issues.

**Solution:**
1. Verify certificate paths exist and are readable
2. Check CA chain is complete
3. Use `verify-ca` first, then upgrade to `verify-full`

```bash
# Test certificate
openssl s_client -connect localhost:5432 -starttls postgres
```

---

## Best Practices

### Production Checklist

- [ ] Dedicated replication user (not superuser)
- [ ] TLS enabled (`verify-full` recommended)
- [ ] Replication slot created manually (not auto)
- [ ] Publication includes only needed tables
- [ ] Monitoring and alerting configured
- [ ] WAL retention sized appropriately
- [ ] Snapshot mode is `initial` or `never`
- [ ] `drop_slot_on_stop: false` in production

### Performance Tuning

```yaml
config:
  # Larger batches for throughput
  max_batch_size: 16384
  snapshot_batch_size: 50000
  
  # Parallel snapshots
  snapshot_parallel: 4
  
  # Reduce heartbeat overhead
  heartbeat_interval: 30s
```

### High Availability

1. **Primary failover:** Replication slots don't survive failover. Use `when_needed` snapshot mode to recover.

2. **Read replica CDC:** Use PostgreSQL 13+ with logical decoding on standby.

3. **Multiple connectors:** Use unique slot names per connector.

---

## Examples

### Full Production Configuration

```yaml
version: "1.0"

sources:
  production_cdc:
    connector: postgres-cdc
    topic: cdc.production
    config:
      # Connection
      host: postgres-primary.internal
      port: 5432
      database: production
      user: rivven_cdc
      password: ${POSTGRES_CDC_PASSWORD}
      connect_timeout: 30s
      socket_timeout: 60s
      
      # TLS
      tls:
        mode: verify-full
        ca_cert_path: /etc/ssl/certs/db-ca.crt
        client_cert_path: /etc/ssl/certs/client.crt
        client_key_path: /etc/ssl/private/client.key
      
      # Replication
      slot_name: rivven_prod_slot
      publication_name: rivven_prod_pub
      auto_create_slot: false
      auto_create_publication: false
      
      # Tables
      tables:
        - public.orders
        - public.order_items
        - public.customers
      exclude_tables:
        - "*_audit"
        - "*_log"
      
      # Snapshot
      snapshot_mode: initial
      snapshot_batch_size: 50000
      snapshot_parallel: 4
      
      # Filtering
      column_masks:
        - public.customers.ssn
        - public.customers.credit_card
      
      # Advanced
      heartbeat_interval: 30s
      max_batch_size: 16384
      
      # Signal
      signal_config:
        enabled: true
        data_collection: public.cdc_signals
        channels:
          - source
          - topic
```

---

## Next Steps

- [MySQL CDC Guide](cdc-mysql) - MySQL/MariaDB setup
- [CDC Troubleshooting](cdc-troubleshooting) - Debug common issues
- [CDC Configuration Reference](cdc-configuration) - All parameters
