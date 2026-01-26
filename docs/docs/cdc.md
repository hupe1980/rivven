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
