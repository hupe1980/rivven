---
layout: default
title: Connectors
nav_order: 5
---

# Connectors
{: .no_toc }

Airbyte-compatible source and sink connectors.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven provides a **native connector framework** that's Airbyte-compatible, allowing you to:

- Use existing Airbyte connectors
- Build custom connectors with the Plugin SDK
- Run connectors as WASM modules for isolation

---

## Broker Configuration

### Bootstrap Servers

Connect to one or more brokers with automatic failover:

```yaml
version: "1.0"

broker:
  # Multiple bootstrap servers for high availability
  bootstrap_servers:
    - broker1:9092
    - broker2:9092
    - broker3:9092
  
  # Connection settings (optional)
  metadata_refresh_ms: 300000     # Refresh broker list every 5 min
  connection_timeout_ms: 10000    # Per-server connection timeout
  request_timeout_ms: 30000       # Overall request deadline
  
  # TLS configuration (optional)
  tls:
    enabled: true
    cert_path: /certs/client.crt
    key_path: /certs/client.key
    ca_path: /certs/ca.crt
```

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `bootstrap_servers` | - | List of broker addresses (required) |
| `metadata_refresh_ms` | `300000` | Metadata refresh interval (5 min) |
| `connection_timeout_ms` | `10000` | Per-server connection timeout (10 sec) |
| `request_timeout_ms` | `30000` | Request deadline (30 sec) |
| `tls.enabled` | `false` | Enable TLS |

### Auto-Create Topics

Automatically create topics when connectors start:

```yaml
settings:
  topic:
    auto_create: true              # Enable auto-create (default: true)
    default_partitions: 3          # Partitions for new topics
    default_replication_factor: 1  # Replication (1 for single-node)
    require_topic_exists: true     # Fail if topic missing & auto_create=false
    validate_existing: false       # Warn if existing topic config differs
```

**Per-source overrides:**

```yaml
sources:
  high-throughput:
    connector: postgres-cdc
    topic: cdc.orders
    topic_config:
      partitions: 12               # Override for high-volume topics
      auto_create: true            # Enable/disable per source
```

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `auto_create` | `true` | Auto-create topics on startup |
| `default_partitions` | `1` | Default partition count |
| `default_replication_factor` | `1` | Default replication factor |
| `require_topic_exists` | `true` | Fail if topic missing when auto_create=false |
| `validate_existing` | `false` | Warn on config mismatch |

---

## Sources

### PostgreSQL CDC

Stream changes from PostgreSQL using logical replication.

```yaml
sources:
  postgres:
    connector: postgres-cdc
    topic: cdc.database
    config:
      host: localhost
      port: 5432
      database: mydb
      user: replication_user
      password: ${POSTGRES_PASSWORD}
      slot_name: rivven_slot
      publication_name: rivven_pub
      publication_tables:
        - public.orders
        - public.customers
      tls:
        mode: require
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `host` | ✓ | - | Database host |
| `port` | | `5432` | Database port |
| `database` | ✓ | - | Database name |
| `user` | ✓ | - | Username |
| `password` | ✓ | - | Password |
| `slot_name` | ✓ | - | Replication slot name |
| `publication_name` | ✓ | - | Publication name |
| `publication_tables` | | `[]` | Tables to include (empty = all) |
| `auto_create_slot` | | `true` | Auto-create slot |
| `auto_create_publication` | | `true` | Auto-create publication |

### MySQL CDC

Stream changes from MySQL/MariaDB using binary log.

```yaml
sources:
  mysql:
    connector: mysql-cdc
    topic: cdc.database
    config:
      host: localhost
      port: 3306
      database: mydb
      user: replication_user
      password: ${MYSQL_PASSWORD}
      server_id: 12345
      tables:
        - orders
        - customers
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `host` | ✓ | - | Database host |
| `port` | | `3306` | Database port |
| `database` | ✓ | - | Database name |
| `user` | ✓ | - | Username |
| `password` | ✓ | - | Password |
| `server_id` | ✓ | - | Unique server ID |
| `tables` | | `[]` | Tables to include (empty = all) |
| `table_regex` | | - | Regex filter for tables |

### File Source

Read events from files (CSV, JSON, Parquet).

```yaml
sources:
  files:
    connector: file
    topic: imported-data
    config:
      path: /data/input
      format: json
      watch: true
      poll_interval_secs: 10
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `path` | ✓ | - | File or directory path |
| `format` | | `json` | File format (json, csv, parquet) |
| `watch` | | `false` | Watch directory for new files |
| `poll_interval_secs` | | `30` | Poll interval when watching |

### HTTP Polling

Poll an HTTP endpoint for data.

```yaml
sources:
  api:
    connector: http-poll
    topic: api-events
    config:
      url: https://api.example.com/events
      method: GET
      headers:
        Authorization: "Bearer ${API_TOKEN}"
      poll_interval_secs: 60
      response_path: "$.data[*]"
```

### Datagen (Synthetic Data)

Generate synthetic data for testing and demos - no external dependencies needed.

```yaml
sources:
  demo:
    connector: datagen
    topic: demo-events
    config:
      pattern: orders           # Data pattern (orders, users, pageviews)
      events_per_second: 10     # Generation rate
      max_events: 0             # 0 = unlimited
      stream_name: demo_orders  # Stream identifier
      seed: 42                  # Reproducible data (optional)
      cdc_mode: true            # Simulate INSERT/UPDATE/DELETE
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `pattern` | ✓ | - | Data pattern: `orders`, `users`, `pageviews` |
| `events_per_second` | | `1` | Events generated per second |
| `max_events` | | `0` | Maximum events (0 = unlimited) |
| `stream_name` | | `datagen` | Stream name in events |
| `seed` | | random | RNG seed for reproducibility |
| `cdc_mode` | | `false` | Generate CDC events (insert/update/delete) |

**Available patterns:**

| Pattern | Description | Fields |
|:--------|:------------|:-------|
| `orders` | E-commerce orders | order_id, customer_id, product, quantity, total, status |
| `users` | User profiles | user_id, name, email, created_at |
| `pageviews` | Web analytics | page_url, user_id, timestamp, referrer |

---

## Sinks

### S3

Write events to Amazon S3 or compatible storage.

```yaml
sinks:
  s3:
    connector: s3
    topics: [events, logs]
    consumer_group: s3-sink
    config:
      bucket: my-data-lake
      prefix: events
      region: us-east-1
      format: jsonl
      partition_by: day
      compression: gzip
      batch_size: 1000
      batch_timeout_secs: 60
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `bucket` | ✓ | - | S3 bucket name |
| `prefix` | | - | Object key prefix |
| `region` | ✓ | - | AWS region |
| `format` | | `jsonl` | Output format |
| `partition_by` | | - | Partitioning (hour, day, month) |
| `compression` | | `none` | Compression (gzip, zstd, snappy) |
| `batch_size` | | `1000` | Events per batch |
| `batch_timeout_secs` | | `60` | Max wait time |

**S3-Compatible Storage:**

```yaml
config:
  endpoint: https://minio.local:9000
  force_path_style: true
  access_key_id: ${MINIO_ACCESS_KEY}
  secret_access_key: ${MINIO_SECRET_KEY}
```

### Snowflake

Load data into Snowflake data warehouse.

```yaml
sinks:
  snowflake:
    connector: snowflake
    topics: [cdc.orders]
    consumer_group: snowflake-sink
    config:
      account: myorg-account123
      user: RIVVEN_USER
      private_key_path: /secrets/rsa_key.p8
      database: ANALYTICS
      schema: CDC
      table: ORDERS
      warehouse: COMPUTE_WH
      batch_size: 5000
      auto_create_table: true
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `account` | ✓ | - | Snowflake account identifier |
| `user` | ✓ | - | Username |
| `private_key_path` | ✓ | - | Path to RSA private key |
| `database` | ✓ | - | Target database |
| `schema` | ✓ | - | Target schema |
| `table` | ✓ | - | Target table |
| `warehouse` | | - | Compute warehouse |
| `batch_size` | | `1000` | Events per batch |
| `auto_create_table` | | `false` | Auto-create table |

### HTTP Webhook

Send events to an HTTP endpoint.

```yaml
sinks:
  webhook:
    connector: http-webhook
    topics: [alerts]
    consumer_group: webhook-sink
    config:
      url: https://api.example.com/webhook
      method: POST
      headers:
        Content-Type: application/json
        Authorization: "Bearer ${TOKEN}"
      batch_size: 100
      timeout_secs: 30
      retry:
        max_attempts: 3
        initial_backoff_ms: 1000
        max_backoff_ms: 30000
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `url` | ✓ | - | Webhook URL |
| `method` | | `POST` | HTTP method |
| `headers` | | - | Request headers |
| `batch_size` | | `1` | Events per request |
| `timeout_secs` | | `30` | Request timeout |
| `retry.max_attempts` | | `3` | Retry count |

### Console (stdout)

Output events to console (debugging).

```yaml
sinks:
  console:
    connector: stdout
    topics: [debug]
    consumer_group: console-sink
    config:
      format: json
      pretty: true
```

### File Sink

Write events to local files.

```yaml
sinks:
  files:
    connector: file
    topics: [events]
    consumer_group: file-sink
    config:
      path: /data/output
      format: jsonl
      rotation:
        max_size_mb: 100
        max_age_hours: 24
```

### Kafka Sink

Forward events to Apache Kafka.

```yaml
sinks:
  kafka:
    connector: kafka
    topics: [events]
    consumer_group: kafka-sink
    config:
      bootstrap_servers: kafka:9092
      target_topic: downstream-events
      acks: all
      compression: lz4
      security:
        protocol: SASL_SSL
        mechanism: SCRAM-SHA-512
        username: ${KAFKA_USER}
        password: ${KAFKA_PASSWORD}
```

---

## Rate Limiting

Control throughput to protect downstream systems:

```yaml
sinks:
  api:
    connector: http-webhook
    rate_limit:
      events_per_second: 1000
      burst_capacity: 100
    config:
      url: https://api.example.com/ingest
```

---

## Transforms

Apply transformations between source and sink.

### Chain Multiple Transforms

```yaml
sources:
  orders:
    connector: postgres-cdc
    topic: cdc.orders
    transforms:
      - type: ExtractNewRecordState
      - type: MaskField
        config:
          fields: [credit_card]
          mask_char: "X"
      - type: ReplaceField
        config:
          rename:
            old_name: new_name
          exclude: [internal_field]
      - type: InsertField
        config:
          static:
            source: "rivven"
          timestamp:
            field: processed_at
```

### Filter Events

```yaml
transforms:
  - type: Filter
    config:
      condition: "value.amount > 100"
      # keep: matches condition are kept
      # drop: matches condition are dropped
      action: keep
```

### Content-Based Routing

```yaml
transforms:
  - type: ContentRouter
    config:
      field: region
      routes:
        us: us-events
        eu: eu-events
        apac: apac-events
      default: other-events
```

---

## Custom Connectors

### Plugin SDK

Build custom connectors in Rust:

```rust
use rivven_plugin_sdk::prelude::*;

#[rivven_plugin]
pub struct MyConnector {
    config: MyConfig,
}

#[async_trait]
impl SourceConnector for MyConnector {
    async fn poll(&mut self) -> Result<Vec<Event>> {
        // Fetch events from your source
        Ok(vec![])
    }
}
```

### WASM Modules

Run connectors in isolated WASM sandboxes:

```yaml
sources:
  custom:
    connector: wasm
    topic: custom-events
    config:
      module_path: /plugins/my-connector.wasm
      memory_limit_mb: 256
      timeout_secs: 30
      plugin_config:
        # Passed to WASM module
        api_url: https://api.example.com
```

### Airbyte Compatibility

Use existing Airbyte connectors via Docker:

```yaml
sources:
  stripe:
    connector: airbyte-docker
    topic: stripe-events
    config:
      image: airbyte/source-stripe:latest
      config:
        account_id: ${STRIPE_ACCOUNT_ID}
        client_secret: ${STRIPE_SECRET_KEY}
```

---

## Configuration Reference

### Environment Variables

Use `${VAR}` syntax to reference environment variables:

```yaml
config:
  password: ${DATABASE_PASSWORD}
  api_key: ${API_KEY:-default_value}  # With default
```

### Secrets Management

Integrate with secret stores:

```yaml
secrets:
  provider: vault
  config:
    address: https://vault.example.com
    role_id: ${VAULT_ROLE_ID}
    secret_id: ${VAULT_SECRET_ID}

sources:
  db:
    connector: postgres-cdc
    config:
      password: vault:secret/data/postgres#password
```

---

## Next Steps

- [CDC Guide](/rivven/docs/cdc) - Database replication details
- [Security](/rivven/docs/security) - TLS and authentication
- [Kubernetes](/rivven/docs/kubernetes) - Production deployment
