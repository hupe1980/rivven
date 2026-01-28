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

Rivven provides **native CDC support** for PostgreSQL and MySQL/MariaDB, enabling real-time change data capture without external tools like Debezium or Kafka Connect.

### Key Features

| Feature | Description |
|:--------|:------------|
| **Zero Dependencies** | Native Rust implementation — no JVM, no Kafka Connect |
| **Debezium Compatible** | Drop-in replacement with identical event format |
| **17 Built-in Transforms** | Filter, mask, route, and transform events in-flight |
| **Production Ready** | TLS/mTLS, SCRAM-SHA-256, circuit breakers, rate limiting |
| **Full Observability** | Prometheus metrics with Debezium JMX parity |

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

### Key Fields

| Field | Description |
|:------|:------------|
| `before` | Row state before change (null for INSERT) |
| `after` | Row state after change (null for DELETE) |
| `source` | Metadata about the source database |
| `op` | Operation type |
| `ts_ms` | Event timestamp (milliseconds) |

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

Rivven CDC provides comprehensive metrics with full Debezium JMX parity.

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
