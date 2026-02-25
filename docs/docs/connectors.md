---
layout: default
title: Connectors
nav_order: 5
---

# Connectors
{: .no_toc }

Scalable connector framework designed for 300+ connectors.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Rivven provides a **native connector framework** that scales to 300+ connectors:

- **Hierarchical Categories**: Database, Messaging, Storage, Warehouse, AI/ML, Utility
- **Rich Metadata**: Tags and search capabilities
- **Connector Inventory**: Auto-registration with metadata indexing
- **Feature Gating**: Compile only the connectors you need

### Connector Categories

```
┌─────────────────────────────────────────────────────────────────┐
│                 Connector Inventory (Scalable)                   │
├─────────────────────────────────────────────────────────────────┤
│  Database                                                        │
│  ├── CDC (postgres_cdc, mysql_cdc, sqlserver_cdc, ...)          │
│  ├── RDBC (rdbc source, rdbc sink)                              │
│  └── Batch (sql_select, sql_insert, ...)                        │
│                                                                  │
│  Messaging                                                       │
│  ├── Queues (kafka, sqs, pubsub, ...)                           │
│  └── MQTT (mqtt, ...)                                           │
│                                                                  │
│  Storage                                                         │
│  └── Object (s3, gcs, azure_blob, minio, ...)                   │
│                                                                  │
│  Warehouse                                                       │
│  └── (snowflake, bigquery, redshift, databricks, clickhouse, ...)│
│                                                                  │
│  Lakehouse                                                       │
│  └── (iceberg, delta-lake, ...)                     │
│                                                                  │
│  AI/ML                                                           │
│  ├── LLM (openai, anthropic, ollama, bedrock, ...)              │
│  └── Vector (pinecone, qdrant, weaviate, ...)                   │
│                                                                  │
│  Utility                                                         │
│  ├── Generate (datagen, faker, ...)                             │
│  └── Debug (stdout, log, drop, ...)                             │
└─────────────────────────────────────────────────────────────────┘
```

### Listing Available Connectors

```bash
# Show all available connectors with categories
rivven-connect connectors

# Output:
# ╭─────────────────────────────────────────────────────────────────────╮
# │               Rivven Connect - Connector Catalog                     │
# ╰─────────────────────────────────────────────────────────────────────╯
#
# 📊 Total: 15 connectors (7 sources, 8 sinks)
#
# 📁 DATABASE
# ─────────────────────────────────────────────────────────────────────
# Name               Type       Description
# ─────────────────────────────────────────────────────────────────────
# postgres-cdc       Source     Change Data Capture from PostgreSQL...
# mysql-cdc          Source     Change Data Capture from MySQL/MariaDB...
```

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

### SQL Server CDC

Capture changes from SQL Server tables using Change Data Capture.
See [SQL Server CDC](cdc-sqlserver) for full configuration.

```yaml
sources:
  sqlserver:
    connector: sqlserver-cdc
    topic: cdc-events
    config:
      host: db.example.com
      port: 1433
      database: production
      username: cdc_user
      password: ${SQLSERVER_PASSWORD}
      schema: dbo
      include_tables:
        - orders
        - customers
      snapshot_mode: initial
      poll_interval_ms: 500
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `host` | ✓ | - | SQL Server host |
| `port` | | `1433` | SQL Server port |
| `database` | ✓ | - | Database name |
| `username` | ✓ | - | Username |
| `password` | ✓ | - | Password (redacted in logs) |
| `schema` | | `dbo` | Schema name |
| `include_tables` | | `[]` | Tables to include (empty = all CDC-enabled) |
| `exclude_tables` | | `[]` | Tables to exclude |
| `snapshot_mode` | | `initial` | Snapshot mode: initial, always, never, when_needed, initial_only, schema_only, recovery |
| `poll_interval_ms` | | `500` | CDC poll interval in milliseconds |
| `encrypt` | | `false` | Enable TLS |
| `trust_server_certificate` | | `false` | Trust self-signed certs |

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

### Kafka Source

Consume from Apache Kafka topics for migration or hybrid deployments. See [Kafka Connector](kafka-connector) for full documentation.

```yaml
sources:
  kafka:
    connector: kafka-source
    topic: kafka-events           # Rivven topic (where consumed messages go)
    config:
      brokers: ["kafka1:9092", "kafka2:9092"]
      topic: orders               # Kafka topic (external source to consume from)
      consumer_group: rivven-migration
      start_offset: earliest
      security:
        protocol: sasl_ssl
        mechanism: SCRAM_SHA_256
        username: ${KAFKA_USER}
        password: ${KAFKA_PASSWORD}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `topic` (outer) | ✓ | - | Rivven topic for consumed messages |
| `brokers` | ✓ | - | Kafka broker addresses |
| `topic` (config) | ✓ | - | Kafka topic to consume from |
| `consumer_group` | ✓ | - | Consumer group ID |
| `start_offset` | | `latest` | `earliest`, `latest`, or specific offset |
| `security.protocol` | | `plaintext` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |

**Metrics:**

| Metric | Type | Description |
|:-------|:-----|:------------|
| `kafka.source.messages_consumed_total` | Counter | Messages consumed |
| `kafka.source.bytes_consumed_total` | Counter | Bytes consumed |
| `kafka.source.polls_total` | Counter | Poll operations |
| `kafka.source.poll_latency_avg_ms` | Gauge | Average poll latency |

### MQTT Source

Subscribe to MQTT topics for IoT data ingestion. Uses rumqttc (pure Rust MQTT client)
for production connectivity with TLS support and exponential backoff. See [MQTT Connector](mqtt-connector) for full documentation.

**Features:**
- Real MQTT 3.1.1/5.0 protocol support via rumqttc
- TLS/SSL encryption (`mqtts://` URLs) with rustls
- Wildcard topics (`+` single-level, `#` multi-level)
- Lock-free metrics with atomic counters (`Ordering::Relaxed`)
- Exponential backoff for reconnection
- Last Will and Testament (LWT) for connection monitoring
- Prometheus-exportable metrics

```yaml
sources:
  mqtt:
    connector: mqtt
    topic: iot-events  # Rivven destination topic
    config:
      # MQTT broker settings
      broker_url: mqtts://broker.example.com:8883
      topics:
        - sensors/+/temperature
        - sensors/+/humidity
      client_id: rivven-mqtt-client
      qos: at_least_once   # at_most_once | at_least_once | exactly_once
      clean_session: true
      
      # Authentication
      auth:
        username: ${MQTT_USER}
        password: ${MQTT_PASSWORD}
      
      # Connection tuning
      keep_alive_secs: 60
      connect_timeout_secs: 30
      max_inflight: 100
      
      # Exponential backoff for reconnection
      retry_initial_ms: 100
      retry_max_ms: 10000
      retry_multiplier: 2.0
      
      # Payload handling
      parse_json_payload: true
      include_metadata: true
      
      # Last Will and Testament (optional)
      last_will:
        topic: clients/rivven/status
        message: offline
        qos: at_least_once
        retain: true
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `broker_url` | ✓ | - | MQTT broker URL (`mqtt://` or `mqtts://` for TLS) |
| `topics` | ✓ | - | MQTT topic patterns (wildcards supported) |
| `client_id` | | auto | Client identifier |
| `qos` | | `at_most_once` | Quality of Service: `at_most_once`, `at_least_once`, `exactly_once` |
| `clean_session` | | `true` | Start with clean session |
| `keep_alive_secs` | | `60` | Keep-alive interval in seconds |
| `connect_timeout_secs` | | `30` | Connection timeout |
| `max_inflight` | | `100` | Max in-flight messages |
| `retry_initial_ms` | | `100` | Initial backoff delay |
| `retry_max_ms` | | `10000` | Maximum backoff delay |
| `retry_multiplier` | | `2.0` | Backoff multiplier |
| `parse_json_payload` | | `true` | Parse payload as JSON |
| `include_metadata` | | `true` | Include message metadata |
| `last_will.topic` | | - | LWT topic for disconnect notification |
| `last_will.message` | | - | LWT message payload |
| `last_will.qos` | | `at_most_once` | LWT QoS level |
| `last_will.retain` | | `false` | LWT retain flag |

**Observability:**

The MQTT source provides lock-free metrics exportable to Prometheus:

```rust
// Get metrics snapshot
let snapshot = source.metrics().snapshot();

// Derived metrics
println!("Throughput: {:.2} msg/s", snapshot.messages_per_second(elapsed));
println!("Bandwidth: {:.2} B/s", snapshot.bytes_per_second(elapsed));
println!("Latency: {:.2} ms", snapshot.avg_receive_latency_ms());
println!("Error rate: {:.2}%", snapshot.error_rate_percent());

// Export to Prometheus
let prometheus_text = snapshot.to_prometheus_format("myapp");
```

**Graceful Shutdown:**

```rust
// Signal shutdown
source.shutdown();

// Check shutdown state
if source.is_shutting_down() {
    println!("Source is terminating...");
}
```

### Google Cloud Pub/Sub Source

Stream messages from Google Cloud Pub/Sub subscriptions with flow control and batch acknowledgment. See [Pub/Sub Connector](pubsub-connector) for full documentation.

```yaml
sources:
  pubsub:
    connector: pubsub-source
    topic: events
    config:
      project_id: my-gcp-project
      subscription_id: my-subscription
      max_messages: 100
      ack_deadline_seconds: 60
      flow_control:
        max_outstanding_messages: 1000
        max_outstanding_bytes: 104857600
      auth:
        credentials_path: /path/to/service-account.json
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `project_id` | ✓ | - | GCP project ID |
| `subscription_id` | ✓ | - | Pub/Sub subscription ID |
| `max_messages` | | `100` | Messages per pull (1–1000) |
| `ack_deadline_seconds` | | `60` | Acknowledgment deadline |
| `batch_ack_size` | | `50` | Messages before batch ack |
| `flow_control.max_outstanding_messages` | | `1000` | Max in-flight messages |
| `flow_control.max_outstanding_bytes` | | `104857600` | Max in-flight bytes (100MB) |
| `include_attributes` | | `true` | Include message attributes |

**Observability:**

The Pub/Sub source provides lock-free metrics:

```rust
let snapshot = source.metrics().snapshot();

println!("Messages: {}", snapshot.messages_received);
println!("In-flight: {}", snapshot.in_flight_messages);
println!("Avg latency: {:.2}ms", snapshot.avg_poll_latency_ms());
println!("Empty polls: {:.1}%", snapshot.empty_poll_rate_percent());

// Export to Prometheus
let prom = snapshot.to_prometheus_format("rivven");
```

---

## Sinks

### Unified Object Storage

All object storage connectors (S3, GCS, Azure Blob) use a unified implementation
powered by the `object_store` crate. This provides:

- **Consistent API** across all cloud providers
- **Local filesystem support** for testing
- **S3-compatible storage** (MinIO, R2, DigitalOcean Spaces)
- **Reduced dependencies** - single unified crate instead of separate SDKs

```yaml
sinks:
  events:
    connector: object-storage
    topics: [events, logs]
    consumer_group: storage-sink
    config:
      provider: s3              # s3 | gcs | azure | local
      bucket: my-data-lake
      prefix: events
      format: jsonl             # json | jsonl | avro | csv
      compression: gzip         # none | gzip
      partitioning: day         # none | day | hour
      batch_size: 1000
      flush_interval_secs: 60
      
      # Provider-specific configuration
      s3:
        region: us-east-1
        # Optional for S3-compatible storage:
        # endpoint: https://minio.local:9000
        # force_path_style: true
        # access_key_id: ${AWS_ACCESS_KEY_ID}
        # secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `provider` | ✓ | `s3` | Storage provider: s3, gcs, azure, local |
| `bucket` | ✓ | - | Bucket/container name |
| `prefix` | | - | Object key prefix |
| `format` | | `json` | Output format (json, jsonl, avro, csv) |
| `compression` | | `none` | Compression (none, gzip) |
| `partitioning` | | `none` | Time partitioning (none, day, hour) |
| `batch_size` | | `1000` | Events per batch |
| `flush_interval_secs` | | `60` | Max wait before flush |

#### S3 Configuration

```yaml
config:
  provider: s3
  bucket: my-bucket
  s3:
    region: us-east-1
    endpoint: https://s3.us-east-1.amazonaws.com  # Optional custom endpoint
    force_path_style: false                        # Set true for MinIO/R2
    access_key_id: ${AWS_ACCESS_KEY_ID}            # Optional (uses IAM role if not set)
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

#### GCS Configuration

```yaml
config:
  provider: gcs
  bucket: my-gcs-bucket
  gcs:
    service_account_path: /path/to/sa.json  # Optional
    use_adc: true                            # Use Application Default Credentials
```

#### Azure Blob Configuration

```yaml
config:
  provider: azure
  bucket: my-container
  azure:
    account: mystorageaccount
    access_key: ${AZURE_STORAGE_KEY}         # Or use connection_string
    # connection_string: ${AZURE_CONNECTION_STRING}
    # sas_token: ${AZURE_SAS_TOKEN}
    # use_managed_identity: true
```

#### Local Filesystem (Testing)

```yaml
config:
  provider: local
  bucket: test-bucket
  prefix: events/
  local:
    root: /tmp/test-storage
```

### Legacy S3/GCS/Azure Connectors

For backward compatibility, the legacy connector names (`s3`, `gcs`, `azure-blob`) still work
and map to the unified object-storage connector. Update your configs to use `object-storage`
with the `provider` field for new deployments.

### Snowflake

Load data into Snowflake data warehouse using **Snowpipe Streaming API** with JWT authentication.

#### Authentication Setup

Snowflake uses RSA key-pair authentication with JWT tokens. Generate a key pair:

```bash
# Generate RSA private key (unencrypted PKCS#8 format)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8

# Extract public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Get public key fingerprint (for Snowflake user assignment)
openssl rsa -in rsa_key.p8 -pubout -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

Register the public key in Snowflake:

```sql
-- As ACCOUNTADMIN
ALTER USER RIVVEN_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';

-- Grant necessary privileges
GRANT USAGE ON DATABASE ANALYTICS TO ROLE RIVVEN_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS.CDC TO ROLE RIVVEN_ROLE;
GRANT INSERT, SELECT ON TABLE ANALYTICS.CDC.ORDERS TO ROLE RIVVEN_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE RIVVEN_ROLE;
```

#### Configuration

```yaml
sinks:
  snowflake:
    connector: snowflake
    topics: [cdc.orders]
    consumer_group: snowflake-sink
    config:
      # Account and authentication
      account: myorg-account123
      user: RIVVEN_USER
      private_key_path: /secrets/rsa_key.p8
      role: RIVVEN_ROLE              # Optional: role to assume
      
      # Target location
      database: ANALYTICS
      schema: CDC
      table: ORDERS
      warehouse: COMPUTE_WH
      
      # Batching and performance
      batch_size: 5000               # Rows per Snowpipe insert
      flush_interval_secs: 10        # Max seconds before flush
      request_timeout_secs: 30       # HTTP request timeout
      
      # Compression (recommended for large batches)
      compression_enabled: true      # Enable gzip compression
      compression_threshold_bytes: 8192  # Compress if payload > 8KB
      
      # Retry configuration (exponential backoff with jitter)
      retry:
        max_retries: 3               # Maximum retry attempts
        initial_backoff_ms: 1000     # Initial backoff (1 second)
        max_backoff_ms: 30000        # Maximum backoff (30 seconds)
        backoff_multiplier: 2.0      # Exponential multiplier
        jitter_factor: 0.1           # 10% randomization to prevent thundering herd
      
      # Circuit breaker (protects against cascading failures)
      circuit_breaker:
        enabled: true                # Enable circuit breaker
        failure_threshold: 5         # Consecutive failures to open circuit
        reset_timeout_secs: 30       # Seconds before testing if service recovered
        success_threshold: 2         # Successes needed to close circuit
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `account` | ✓ | - | Snowflake account identifier (e.g., `myorg-account123`) |
| `user` | ✓ | - | Snowflake username |
| `private_key_path` | ✓ | - | Path to RSA private key (PKCS#8 PEM format, unencrypted) |
| `role` | | - | Role to assume after authentication |
| `database` | ✓ | - | Target database |
| `schema` | ✓ | - | Target schema |
| `table` | ✓ | - | Target table |
| `warehouse` | | - | Compute warehouse |
| `batch_size` | | `1000` | Rows per Snowpipe Streaming insert |
| `flush_interval_secs` | | `1` | Maximum seconds between flushes |
| `request_timeout_secs` | | `30` | HTTP request timeout |
| `compression_enabled` | | `true` | Enable gzip compression for large payloads |
| `compression_threshold_bytes` | | `8192` | Minimum payload size to trigger compression |
| `retry.max_retries` | | `3` | Maximum retry attempts for transient errors |
| `retry.initial_backoff_ms` | | `1000` | Initial backoff duration |
| `retry.max_backoff_ms` | | `30000` | Maximum backoff duration |
| `retry.backoff_multiplier` | | `2.0` | Exponential backoff multiplier |
| `retry.jitter_factor` | | `0.1` | Randomization factor (0.0-1.0) to prevent thundering herd |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker pattern |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before allowing test request |
| `circuit_breaker.success_threshold` | | `2` | Successes needed to close circuit |

#### Retry Behavior

The Snowflake connector automatically retries transient failures with exponential backoff and jitter:

- **Retryable errors**: 408 (timeout), 429 (rate limit), 500-504 (server errors), network errors
- **Non-retryable errors**: 400 (bad request), 401 (auth), 403 (forbidden), 404 (not found)
- **Jitter**: Adds randomization to backoff timing to prevent thundering herd
- **Retry-After**: Respects `Retry-After` header from Snowflake for 429 responses

Each retry includes a unique `X-Request-ID` header for end-to-end tracing.

#### Circuit Breaker

The connector implements a circuit breaker pattern to prevent cascading failures:

| State | Description |
|:------|:------------|
| **Closed** | Normal operation - requests go through |
| **Open** | After consecutive failures - requests fail fast with "circuit breaker is open" error |
| **Half-Open** | After reset timeout - allows one test request to check if service recovered |

#### Observability

The connector exports the following metrics:

| Metric | Type | Description |
|:-------|:-----|:------------|
| `snowflake.requests.success` | Counter | Successful API requests |
| `snowflake.requests.failed` | Counter | Failed API requests (after retries) |
| `snowflake.requests.retried` | Counter | Retried requests |
| `snowflake.request.duration_ms` | Histogram | Request latency in milliseconds |
| `snowflake.batch.size` | Gauge | Rows in current batch |
| `snowflake.circuit_breaker.rejected` | Counter | Requests rejected by open circuit |

{: .note }
> **Key Format**: The private key must be in **unencrypted PKCS#8 PEM format** (begins with `-----BEGIN PRIVATE KEY-----`). Encrypted keys (PKCS#5) are not supported. Use `openssl pkcs8 -topk8 -nocrypt` to convert.

### Databricks

Stream events into Databricks Delta tables via the **Zerobus Ingest SDK** — a high-performance, async-first gRPC client that handles OAuth 2.0 authentication, retries, stream recovery, and acknowledgment tracking automatically.

**Key features**: Circuit breaker protection, connector-level batch retry with exponential backoff, timer-based flush via `tokio::select!`, full metrics integration (14 counters/gauges/histograms), SDK-native ack callbacks, endpoint format validation, byte-level throughput tracking, error classification (Auth/Timeout/RateLimited/Transient/Fatal).

#### Authentication Setup

Databricks uses **OAuth 2.0 client credentials** with a Unity Catalog service principal.

1. In your Databricks workspace, go to **Settings** > **Identity and Access**
2. Create a service principal (or use an existing one)
3. Generate OAuth credentials (client ID and secret)
4. Grant the service principal these permissions on your target table:
   - `SELECT` — read table schema
   - `MODIFY` — write data to the table
   - `USE CATALOG` and `USE SCHEMA` — access the catalog and schema

#### Configuration

```yaml
sinks:
  databricks:
    connector: databricks
    topics: [cdc.orders]
    consumer_group: databricks-sink
    config:
      # Zerobus endpoint (region-specific)
      endpoint: "<shard-id>.zerobus.<region>.cloud.databricks.com"
      
      # Unity Catalog workspace URL
      unity_catalog_url: "https://<workspace>.cloud.databricks.com"
      
      # Fully qualified table name
      table_name: "catalog.schema.orders"
      
      # OAuth 2.0 credentials
      client_id: "${DATABRICKS_CLIENT_ID}"
      client_secret: "${DATABRICKS_CLIENT_SECRET}"
      
      # Batching and performance
      batch_size: 500                   # Records per batch (all-or-nothing)
      flush_interval_secs: 5            # Max seconds before flushing partial batch
      max_inflight_requests: 100000     # Unacknowledged records in flight
      
      # Acknowledgment
      wait_for_ack: true                # Wait for server acknowledgment per batch
      
      # Recovery (automatic retry on transient failures)
      recovery: true                    # Enable automatic stream recovery
      recovery_retries: 4              # Maximum recovery attempts
      recovery_timeout_ms: 15000       # Timeout per recovery attempt (ms)
      recovery_backoff_ms: 2000        # Backoff between recovery retries (ms)
      server_ack_timeout_ms: 60000     # Server ack timeout before recovery (ms)
      flush_timeout_ms: 300000         # Timeout for flush operations (ms)
      
      # Connector-level batch retry (on top of SDK recovery)
      max_batch_retries: 2             # Retries per batch on retryable errors
      initial_backoff_ms: 200          # Initial retry backoff (ms)
      max_backoff_ms: 5000             # Maximum retry backoff (ms)
      
      # Circuit breaker
      circuit_breaker:
        enabled: true                   # Protect against cascading failures
        failure_threshold: 5            # Consecutive failures before opening
        reset_timeout_secs: 30          # Seconds before half-open test
        success_threshold: 2            # Successes to close from half-open
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `endpoint` | ✓ | - | Zerobus API endpoint (`<shard>.zerobus.<region>.cloud.databricks.com` or `.azuredatabricks.net`) |
| `unity_catalog_url` | ✓ | - | Workspace URL for OAuth (`https://<workspace>.cloud.databricks.com`) |
| `table_name` | ✓ | - | Fully qualified table name (`catalog.schema.table`) |
| `client_id` | ✓ | - | OAuth 2.0 client ID (service principal) |
| `client_secret` | ✓ | - | OAuth 2.0 client secret |
| `batch_size` | | `500` | Records per batch (1–100,000) |
| `flush_interval_secs` | | `5` | Maximum seconds before flushing partial batch |
| `max_inflight_requests` | | `100000` | Maximum unacknowledged records in flight |
| `wait_for_ack` | | `true` | Wait for server acknowledgment per batch |
| `recovery` | | `true` | Enable automatic stream recovery on transient failures |
| `recovery_retries` | | `4` | Maximum number of recovery attempts |
| `recovery_timeout_ms` | | `15000` | Timeout per recovery attempt (ms) |
| `recovery_backoff_ms` | | `2000` | Backoff between recovery retries (ms) |
| `server_ack_timeout_ms` | | `60000` | Timeout for server acks before triggering recovery (ms) |
| `flush_timeout_ms` | | `300000` | Timeout for flush operations (ms) |
| `max_batch_retries` | | `2` | Connector-level retries per batch (0 to disable) |
| `initial_backoff_ms` | | `200` | Initial backoff between batch retries (ms) |
| `max_backoff_ms` | | `5000` | Maximum backoff between batch retries (ms) |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker for batch failures |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before testing half-open |
| `circuit_breaker.success_threshold` | | `2` | Successes to close from half-open |

#### Cloud Support

| Cloud | Endpoint Format | UC URL Format |
|:------|:----------------|:--------------|
| **AWS** | `<shard>.zerobus.<region>.cloud.databricks.com` | `https://<workspace>.cloud.databricks.com` |
| **Azure** | `<shard>.zerobus.<region>.azuredatabricks.net` | `https://<workspace>.azuredatabricks.net` |

#### Metrics

The connector records the following metrics (via the `metrics` crate):

| Metric | Type | Description |
|:-------|:-----|:------------|
| `databricks.batches.success` | Counter | Successfully ingested batches |
| `databricks.batches.failed` | Counter | Failed batch ingestions |
| `databricks.batches.retried` | Counter | Batch retry attempts |
| `databricks.records.written` | Counter | Total records written |
| `databricks.records.failed` | Counter | Total records failed |
| `databricks.records.serialization_errors` | Counter | JSON serialization failures |
| `databricks.acks.success` | Counter | Server-side offset acknowledgments |
| `databricks.acks.error` | Counter | Server-side ack errors |
| `databricks.circuit_breaker.rejected` | Counter | Batches rejected by circuit breaker |
| `databricks.batch.size` | Gauge | Current batch size (records) |
| `databricks.batch.duration_ms` | Histogram | End-to-end batch ingest latency |
| `databricks.ack.duration_ms` | Histogram | Server acknowledgment latency |

#### Error Handling

- **Circuit breaker**: After consecutive batch failures, the circuit opens and batches are dropped immediately until the reset timeout. This prevents cascading failures from overwhelming the Zerobus service.
- **Connector-level batch retry**: If `ingest_records_offset` returns a retryable error, the connector retries the batch up to `max_batch_retries` times with exponential backoff (capped at `max_backoff_ms`). This complements the SDK's stream-level recovery.
- **Timer-based flush**: Uses `tokio::select!` with an interval timer — partial batches are flushed even when no new events arrive, preventing stale data.
- **Error classification**: Zerobus errors are classified into Auth, Timeout, RateLimited, Transient, or Fatal categories for precise monitoring and alerting.
- **Retryable errors**: Network failures, connection timeouts, temporary server errors, stream closed by server. Automatically recovered when `recovery: true`.
- **Non-retryable errors**: Invalid OAuth credentials, table not found, permission denied, schema mismatch. These abort the connector immediately.
- **Unacknowledged records**: On stream failure, the connector logs the count of unacked records and adjusts metrics accordingly.
- **Stream ID uniqueness**: Stream IDs include an atomic counter (`AtomicU64`) to guarantee uniqueness across rapid successive calls, preventing stream collisions during recovery scenarios.

### ClickHouse

High-throughput inserts into ClickHouse via the **official `clickhouse-rs` pure Rust client** — uses native `RowBinary` encoding over HTTP for maximum performance with LZ4 wire compression.

**Key features**: RowBinary encoding (zero JSON overhead on the server), LZ4 compression, batch inserts with configurable size and flush interval, exponential backoff retry with jitter, structured error classification into typed `ConnectorError` variants, lock-free circuit breaker, metrics integration (`metrics` crate), parameterized SQL (no injection), session-scoped structured logging, schema validation toggle, ClickHouse Cloud support with access tokens.

#### Authentication

The connector supports two authentication methods:

1. **Username/Password** — Standard ClickHouse authentication (default)
2. **Access Token** — JWT-based auth for ClickHouse Cloud

#### Configuration

```yaml
sinks:
  clickhouse:
    connector: clickhouse
    topics: [cdc.orders]
    consumer_group: clickhouse-sink
    config:
      # ClickHouse HTTP endpoint
      url: "http://localhost:8123"
      
      # Target database and table
      database: "default"
      table: "events"
      
      # Authentication
      username: "default"
      password: "${CLICKHOUSE_PASSWORD}"
      
      # Batching and performance
      batch_size: 10000                 # Rows per batch (1–1,000,000)
      flush_interval_secs: 5            # Max seconds before flushing partial batch
      
      # Wire compression
      compression: lz4                  # lz4 (default) or none
      
      # Schema validation (5-10% overhead, clearer error messages)
      validation: true
      
      # Error handling
      skip_invalid_rows: false          # Abort batch on bad row (default)
      
      # Retry configuration
      max_retries: 3                    # Retries on transient failures
      initial_backoff_ms: 200           # Initial retry backoff
      max_backoff_ms: 5000              # Maximum retry backoff

      # Circuit breaker
      circuit_breaker:
        enabled: true                   # Enable circuit breaker (default)
        failure_threshold: 5            # Consecutive failures before opening
        reset_timeout_secs: 30          # Seconds before half-open probe
        success_threshold: 2            # Successes to close circuit
```

#### ClickHouse Cloud Configuration

```yaml
sinks:
  clickhouse-cloud:
    connector: clickhouse
    topics: [cdc.orders]
    consumer_group: ch-cloud-sink
    config:
      url: "https://abc123.clickhouse.cloud:8443"
      database: "production"
      table: "events"
      username: "default"
      access_token: "${CLICKHOUSE_CLOUD_TOKEN}"
      batch_size: 50000
      compression: lz4
      validation: false                 # Disable for ~10-300% throughput gain
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `url` | ✓ | - | ClickHouse HTTP(S) endpoint (e.g. `http://localhost:8123`) |
| `database` | | `default` | Target database name (validated with regex) |
| `table` | ✓ | - | Target table name (must already exist, validated with regex) |
| `username` | | `default` | Authentication username |
| `password` | | - | Authentication password |
| `access_token` | | - | JWT access token (mutually exclusive with `password`) |
| `batch_size` | | `10000` | Rows per batch (1–1,000,000) |
| `flush_interval_secs` | | `5` | Maximum seconds before flushing partial batch |
| `compression` | | `lz4` | Wire compression: `lz4` or `none` |
| `validation` | | `true` | Validate rows against ClickHouse schema |
| `skip_invalid_rows` | | `false` | Skip bad rows instead of aborting batch |
| `max_retries` | | `3` | Maximum retries on transient failures (0 to disable) |
| `initial_backoff_ms` | | `200` | Initial retry backoff (ms) |
| `max_backoff_ms` | | `5000` | Maximum retry backoff (ms) |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before half-open probe |
| `circuit_breaker.success_threshold` | | `2` | Successes in half-open to close circuit |

#### Table Schema

The connector inserts rows with the following columns. Create your table accordingly:

```sql
CREATE TABLE events (
    event_type   String,
    stream       String,
    namespace    String,
    timestamp    String,
    data         String,      -- JSON-encoded event payload
    metadata     String,      -- JSON-encoded event metadata
    _ingested_at String
) ENGINE = MergeTree()
ORDER BY (stream, timestamp);
```

{: .note }
> **Performance tip**: For maximum throughput, set `validation: false` and `compression: lz4`. This eliminates schema round-trips and uses ClickHouse's native LZ4 wire compression. Measure the impact for your dataset — gains range from 10% to 300%.

#### Circuit Breaker

The ClickHouse connector includes a lock-free circuit breaker (using `AtomicU32`/`AtomicU64` with `Acquire`/`Release` ordering) that prevents cascading failures:

- **Closed**: All batches flow through normally.
- **Open**: After `failure_threshold` consecutive insert failures, the circuit opens. Batches fail fast without contacting ClickHouse, counted as `records_failed`.
- **Half-Open**: After `reset_timeout_secs`, one test batch is let through. If it succeeds (`success_threshold` times), the circuit closes. The Open→HalfOpen transition uses `compare_exchange` to prevent TOCTOU races under concurrent access.

The circuit breaker is **always active by default** — both `new()` and `with_config()` constructors initialize it (matching the Databricks connector pattern). Disable with `circuit_breaker.enabled: false` if you prefer pure retry-based recovery.

Circuit breaker configuration is validated at startup (`failure_threshold: 1–100`, `reset_timeout_secs: 1–3600`, `success_threshold: 1–10`).

#### Metrics

The connector emits the following metrics via the `metrics` crate:

| Metric | Type | Description |
|:-------|:-----|:------------|
| `clickhouse.batches.success` | Counter | Successfully inserted batches |
| `clickhouse.batches.failed` | Counter | Failed batch inserts (after all retries) |
| `clickhouse.batches.retried` | Counter | Batch retry attempts |
| `clickhouse.records.written` | Counter | Total records written |
| `clickhouse.records.failed` | Counter | Total records failed |
| `clickhouse.circuit_breaker.rejected` | Counter | Batches rejected by open circuit breaker |
| `clickhouse.batch.size` | Gauge | Batch size before flush |
| `clickhouse.batch.duration_ms` | Histogram | End-to-end batch insert latency |

#### Error Handling

Errors are classified into typed `ConnectorError` variants:

- **Retryable**: `Connection`, `Timeout`, `RateLimited` — retried with exponential backoff and jitter (prevents thundering herd).
- **Non-retryable**: `Auth`, `NotFound`, `Schema`, `Fatal` — abort the connector immediately.
- **Batch semantics**: When `skip_invalid_rows: false` (default), a single bad row aborts the entire batch. When `true`, bad rows are skipped and counted in `records_failed`.

### Apache Iceberg

Write streaming events to Apache Iceberg tables for analytics and lakehouse workloads. Uses the **official Apache Iceberg Rust SDK** for catalog operations and data file writing.

#### Features

- **Catalog Support**: REST (Polaris, Tabular, Lakekeeper), Memory (testing)
- **Automatic Table Creation**: Auto-create namespaces and tables with schema inference
- **Transaction Support**: Atomic commits via Iceberg SDK Transaction API
- **Storage Backends**: S3, GCS, Azure, Local filesystem

#### Basic Configuration

```yaml
sinks:
  lakehouse:
    connector: iceberg
    topics: [cdc.orders]
    consumer_group: iceberg-sink
    config:
      catalog:
        type: rest
        rest:
          uri: http://localhost:8181
          warehouse: s3://my-bucket/warehouse
      namespace: analytics
      table: events
      
      # Batching
      batch_size: 10000
      flush_interval_secs: 60
      
      # File configuration
      target_file_size_mb: 128
      compression: snappy
```

#### REST Catalog (Polaris, Tabular)

```yaml
config:
  catalog:
    type: rest
    rest:
      uri: http://polaris.example.com:8181
      warehouse: s3://bucket/warehouse
      credential: ${ICEBERG_CATALOG_TOKEN}
```

#### S3 Storage

```yaml
config:
  catalog:
    type: rest
    rest:
      uri: http://localhost:8181
  namespace: analytics
  table: events
  s3:
    region: us-west-2
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

#### MinIO / S3-Compatible

```yaml
config:
  s3:
    region: us-east-1
    endpoint: http://minio:9000
    path_style_access: true
    access_key_id: ${MINIO_ACCESS_KEY}
    secret_access_key: ${MINIO_SECRET_KEY}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `catalog.type` | ✓ | - | Catalog type: `rest`, `memory` |
| `catalog.rest.uri` | ✓ | - | REST catalog URI |
| `catalog.rest.warehouse` | | - | Warehouse location |
| `catalog.rest.credential` | | - | OAuth token for authentication |
| `namespace` | ✓ | - | Iceberg namespace/database |
| `table` | ✓ | - | Table name |
| `batch_size` | | `10000` | Events per batch |
| `flush_interval_secs` | | `60` | Max flush interval |
| `target_file_size_mb` | | `128` | Target Parquet file size |
| `compression` | | `snappy` | Compression: `none`, `snappy`, `gzip`, `lz4`, `zstd`, `brotli` |
| `partitioning` | | `none` | Partition strategy: `none`, `identity`, `bucket`, `time` |
| `commit_mode` | | `append` | Commit mode: `append`, `overwrite`, `upsert` |
| `schema_evolution` | | `add_columns` | Schema evolution: `strict`, `add_columns`, `full` |

#### Observability

| Metric | Type | Description |
|:-------|:-----|:------------|
| `iceberg.records_written_total` | Counter | Total records written |
| `iceberg.batches_committed_total` | Counter | Total batches committed |
| `iceberg.bytes_written_total` | Counter | Total bytes written |
| `iceberg.commit_duration_seconds` | Histogram | Commit latency |

{: .note }
> For advanced configuration including partitioning strategies, commit modes, and query examples, see the [Apache Iceberg Sink Guide](iceberg-sink).

### Delta Lake

Write streaming events to Delta Lake tables with ACID transactions. Uses the **delta-rs** native Rust implementation — no JVM required.

- **ACID Transactions**: Atomic commits with snapshot isolation via the Delta log
- **Multiple Storage Backends**: S3, GCS, Azure, local filesystem
- **Auto Table Creation**: Creates tables on first write
- **Commit Retry**: Exponential backoff on transaction conflicts
- **Compression**: Snappy, Gzip, LZ4, Zstd

```yaml
sinks:
  delta:
    connector: delta-lake
    topics: [cdc.events]
    consumer_group: delta-sink
    config:
      table_uri: s3://my-bucket/warehouse/events
      auto_create_table: true
      batch_size: 10000
      flush_interval_secs: 60
      compression: snappy
      s3:
        region: us-east-1
```

**Configuration Reference:**

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `table_uri` | ✓ | - | Delta table location (local, s3://, gs://, az://) |
| `auto_create_table` | | `true` | Create table if it doesn't exist |
| `batch_size` | | `10000` | Records per batch |
| `flush_interval_secs` | | `60` | Max seconds between flushes |
| `compression` | | `snappy` | snappy, gzip, lz4, zstd, none |
| `partition_columns` | | `[]` | Hive-style partition columns |
| `max_commit_retries` | | `3` | Commit retry attempts |

**Metrics:**

| Metric | Type | Description |
|:-------|:-----|:------------|
| `delta.records_written` | Counter | Total records written |
| `delta.commits_success` | Counter | Successful Delta commits |
| `delta.bytes_written` | Counter | Total bytes written |
| `delta.commit_latency_us` | Counter | Cumulative commit latency |

{: .note }
> For advanced configuration including storage backends, partitioning, and query examples, see the [Delta Lake Sink Guide](delta-lake-sink).

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

### Kafka Sink

Produce events to Apache Kafka topics. See [Kafka Connector](kafka-connector) for full documentation.

```yaml
sinks:
  kafka:
    connector: kafka-sink
    topics: [events]              # Rivven topics to consume from
    consumer_group: kafka-producer
    config:
      brokers: ["kafka1:9092"]
      topic: orders-replica       # Kafka topic (external destination)
      acks: all
      compression: lz4
      idempotent: true
      security:
        protocol: sasl_ssl
        mechanism: PLAIN
        username: ${KAFKA_USER}
        password: ${KAFKA_PASSWORD}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `topics` (outer) | ✓ | - | Rivven topics to consume from |
| `brokers` | ✓ | - | Kafka broker addresses |
| `topic` (config) | ✓ | - | Kafka topic to produce to |
| `acks` | | `all` | `none`, `leader`, `all` |
| `compression` | | `none` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `idempotent` | | `false` | Enable idempotent producer |
| `transactional_id` | | - | Enable exactly-once with transactions |

**Metrics:**

| Metric | Type | Description |
|:-------|:-----|:------------|
| `kafka.sink.messages_produced_total` | Counter | Messages produced |
| `kafka.sink.bytes_produced_total` | Counter | Bytes produced |
| `kafka.sink.batches_sent_total` | Counter | Batches sent |
| `kafka.sink.success_rate` | Gauge | Producer success rate |

### SQS Sink

Produce events to Amazon SQS queues with batch sending. See [SQS Connector](sqs-connector) for full documentation.

```yaml
sinks:
  sqs:
    connector: sqs-sink
    topics: [events]              # Rivven topics to consume from
    consumer_group: sqs-producer
    config:
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
      region: us-east-1
      batch_size: 10
      batch_timeout_ms: 1000
      include_metadata: true
      auth:
        access_key_id: ${AWS_ACCESS_KEY_ID}
        secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `topics` (outer) | ✓ | - | Rivven topics to consume from |
| `queue_url` | ✓ | - | Full SQS queue URL |
| `region` | | `us-east-1` | AWS region |
| `batch_size` | | `10` | Messages per batch (1–10) |
| `batch_timeout_ms` | | `1000` | Max wait before flush |
| `message_group_id` | | - | Static group ID for FIFO queues |
| `message_group_id_field` | | - | Event field for dynamic group ID |
| `deduplication_id_field` | | - | Event field for deduplication ID |
| `delay_seconds` | | `0` | Message delay (0–900s) |
| `include_metadata` | | `true` | Include metadata as message attributes |

**FIFO Queue Example:**

```yaml
sinks:
  orders:
    connector: sqs-sink
    topics: [orders]
    config:
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789/orders.fifo
      region: us-east-1
      message_group_id_field: customer_id    # Dynamic grouping by customer
      deduplication_id_field: order_id       # Deduplication by order ID
```

**Metrics:**

| Metric | Type | Description |
|:-------|:-----|:------------|
| `sqs.sink.messages_sent_total` | Counter | Messages sent |
| `sqs.sink.bytes_sent_total` | Counter | Bytes sent |
| `sqs.sink.batches_sent_total` | Counter | Batches sent |
| `sqs.sink.success_rate` | Gauge | Send success rate |
| `sqs.sink.messages_failed_total` | Counter | Failed messages |

### Google Cloud Pub/Sub Sink

Publish events to Google Cloud Pub/Sub topics with batching, compression, and ordering. See [Pub/Sub Connector](pubsub-connector) for full documentation.

```yaml
sinks:
  pubsub:
    connector: pubsub-sink
    topics: [events]              # Rivven topics to consume from
    consumer_group: pubsub-producer
    config:
      project_id: my-gcp-project
      topic_id: my-topic
      batch_size: 100
      batch_timeout_ms: 1000
      compression: gzip
      auth:
        credentials_path: /path/to/service-account.json
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `topics` (outer) | ✓ | - | Rivven topics to consume from |
| `project_id` | ✓ | - | GCP project ID |
| `topic_id` | ✓ | - | Pub/Sub topic ID |
| `batch_size` | | `100` | Messages per batch |
| `batch_timeout_ms` | | `1000` | Max wait before flush |
| `compression` | | `none` | `none`, `gzip`, or `zstd` |
| `body_format` | | `json` | `json`, `raw`, or `base64` |
| `ordering_key.mode` | | - | `static`, `field`, or `stream` |
| `oversized_behavior` | | `fail` | `fail`, `skip`, or `truncate` |

**With Ordering Keys:**

```yaml
sinks:
  orders:
    connector: pubsub-sink
    topics: [orders]
    config:
      project_id: my-project
      topic_id: orders-topic
      ordering_key:
        mode: field
        field_path: customer_id
        fallback: default
```

**Metrics:**

| Metric | Type | Description |
|:-------|:-----|:------------|
| `pubsub.sink.messages_published_total` | Counter | Messages published |
| `pubsub.sink.bytes_published_total` | Counter | Bytes published |
| `pubsub.sink.batches_published_total` | Counter | Batches published |
| `pubsub.sink.publish_failures_total` | Counter | Failed publishes |
| `pubsub.sink.compression_savings_bytes` | Counter | Bytes saved by compression |

---

### Qdrant (Vector Database)

High-throughput vector upserts into [Qdrant](https://qdrant.tech/) via the **official `qdrant-client` pure Rust gRPC client** — zero-copy Protobuf encoding over gRPC for maximum performance.

**Key features**: gRPC/Protobuf encoding (zero JSON overhead), batch upserts with configurable size and flush interval, exponential backoff retry with jitter, structured error classification into typed `ConnectorError` variants, lock-free circuit breaker, metrics integration (`metrics` crate), API key auth via `SensitiveString`, named vector support, multiple point ID strategies (field, UUID, deterministic FNV-1a hash), session-scoped structured logging.

#### Point ID Strategies

The connector supports three strategies for generating Qdrant point IDs:

1. **Field** (`field`) — Extract a `u64` ID from a field in the event data (e.g. `id`, `row_id`)
2. **UUID** (`uuid`) — Extract a UUID string from a field (e.g. `uuid`, `event_id`)
3. **Hash** (`hash`) — Deterministic FNV-1a hash of the event data (stable across restarts, no field extraction needed)

#### Configuration

```yaml
sinks:
  qdrant:
    connector: qdrant
    topics: [embeddings.products]
    consumer_group: qdrant-sink
    config:
      # Qdrant gRPC endpoint
      url: "http://localhost:6334"
      
      # Target collection (must already exist)
      collection: "products"
      
      # Vector field in event data
      vector_field: "embedding"
      
      # Optional: named vector (for collections with multiple vector spaces)
      # vector_name: "dense"
      
      # Authentication (optional)
      api_key: "${QDRANT_API_KEY}"
      
      # Point ID strategy
      id_strategy: "field"            # field | uuid | hash
      id_field: "id"                  # Field to extract ID from (field/uuid strategies)
      
      # Batching and performance
      batch_size: 1000                # Points per batch (1–100,000)
      flush_interval_secs: 5          # Max seconds before flushing partial batch
      
      # Qdrant options
      timeout_secs: 30                # gRPC deadline per request
      wait: true                      # Wait for upsert acknowledgement
      
      # Error handling
      skip_invalid_events: false      # Skip events with missing/invalid vectors
      
      # Retry configuration
      max_retries: 3                  # Retries on transient failures
      initial_backoff_ms: 200         # Initial retry backoff
      max_backoff_ms: 5000            # Maximum retry backoff

      # Circuit breaker
      circuit_breaker:
        enabled: true                 # Enable circuit breaker (default)
        failure_threshold: 5          # Consecutive failures before opening
        reset_timeout_secs: 30        # Seconds before half-open probe
        success_threshold: 2          # Successes to close circuit
```

#### Qdrant Cloud Configuration

```yaml
sinks:
  qdrant-cloud:
    connector: qdrant
    topics: [embeddings.products]
    consumer_group: qdrant-cloud-sink
    config:
      url: "https://xyz-abc.cloud.qdrant.io:6334"
      collection: "products"
      vector_field: "embedding"
      api_key: "${QDRANT_CLOUD_API_KEY}"
      id_strategy: "hash"
      batch_size: 5000
      timeout_secs: 60
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `url` | ✓ | - | Qdrant gRPC endpoint (e.g. `http://localhost:6334`) |
| `collection` | ✓ | - | Target collection name (validated with regex) |
| `vector_field` | ✓ | - | Field in event data containing the vector (array of floats) |
| `vector_name` | | - | Named vector space (for multi-vector collections) |
| `api_key` | | - | API key for Qdrant Cloud or self-hosted with auth |
| `id_strategy` | | `hash` | Point ID strategy: `field`, `uuid`, or `hash` |
| `id_field` | | `id` | Field to extract point ID from (`field`/`uuid` strategies) |
| `batch_size` | | `1000` | Points per batch (1–100,000) |
| `flush_interval_secs` | | `5` | Maximum seconds before flushing partial batch |
| `timeout_secs` | | `30` | gRPC deadline per request (seconds) |
| `wait` | | `true` | Wait for Qdrant acknowledgement before advancing |
| `skip_invalid_events` | | `false` | Skip events with missing or invalid vectors |
| `max_retries` | | `3` | Maximum retries on transient failures (0 to disable) |
| `initial_backoff_ms` | | `200` | Initial retry backoff (ms) |
| `max_backoff_ms` | | `5000` | Maximum retry backoff (ms) |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before half-open probe |
| `circuit_breaker.success_threshold` | | `2` | Successes in half-open to close circuit |

#### Circuit Breaker

The Qdrant connector includes a lock-free circuit breaker (using `AtomicU32`/`AtomicU64` with `Acquire`/`Release` ordering) that prevents cascading failures:

- **Closed**: All batches flow through normally.
- **Open**: After `failure_threshold` consecutive upsert failures, the circuit opens. Batches fail fast without contacting Qdrant, counted as `records_failed`.
- **Half-Open**: After `reset_timeout_secs`, one test batch is let through. If it succeeds (`success_threshold` times), the circuit closes. If the probe fails, the circuit immediately re-opens. The Open→HalfOpen transition uses `compare_exchange` to prevent TOCTOU races under concurrent access.

The circuit breaker is **always active by default**. Disable with `circuit_breaker.enabled: false` if you prefer pure retry-based recovery.

Circuit breaker configuration is validated at startup (`failure_threshold: 1–100`, `reset_timeout_secs: 1–3600`, `success_threshold: 1–10`).

#### Metrics

The connector emits the following metrics via the `metrics` crate:

| Metric | Type | Description |
|:-------|:-----|:------------|
| `qdrant.batches.success` | Counter | Successfully upserted batches |
| `qdrant.batches.failed` | Counter | Failed batch upserts (after all retries) |
| `qdrant.batches.retried` | Counter | Batch retry attempts |
| `qdrant.records.written` | Counter | Total points written |
| `qdrant.records.failed` | Counter | Failed/skipped records |
| `qdrant.circuit_breaker.rejected` | Counter | Batches rejected by open circuit |
| `qdrant.batch.size` | Gauge | Current batch size |
| `qdrant.batch.duration_ms` | Histogram | Batch upsert latency |

---

### Pinecone (Vector Database)

High-throughput vector upserts into [Pinecone](https://www.pinecone.io/) via a **lightweight gRPC client** built on `tonic` with rustls — no OpenSSL dependency, fully static-linkable for musl targets.

**Key features**: gRPC/protobuf data plane (persistent HTTP/2 channel via `connect_lazy`), batch upserts with configurable size and timer-based flush via `tokio::select!`, exponential backoff retry with jitter, structured error classification into typed `ConnectorError` variants (all 16 gRPC status codes explicitly mapped + message-based fallback), lock-free circuit breaker (`AtomicU32`/`AtomicU64`), metrics integration (`metrics` crate), API key auth via gRPC metadata interceptor (`SensitiveString`, zeroize-on-drop), namespace support, sparse vector support for hybrid search, multiple vector ID strategies (field, UUID with single-pass validation, deterministic FNV-1a hash), per-request timeout with `tokio::time::timeout`, HTTP/2 connection tuning (PING keep-alive every 30 s, adaptive flow control, enlarged window sizes for large batches, TCP_NODELAY), session-scoped structured logging, early rejection of empty/non-object/f32-overflow vectors.

#### Vector ID Strategies

The connector supports three strategies for generating Pinecone vector IDs:

1. **Field** (`field`) — Extract a string/numeric ID from a field in the event data (e.g. `id`, `doc_id`)
2. **UUID** (`uuid`) — Extract a UUID string from a field (validated as exactly 32 or 36 hex characters with optional hyphens; single-pass validation)
3. **Hash** (`hash`) — Deterministic FNV-1a hash of the event data (stable across restarts, no field extraction needed)

#### Configuration

```yaml
sinks:
  pinecone:
    connector: pinecone
    topics: [embeddings.products]
    consumer_group: pinecone-sink
    config:
      # Pinecone API key (required)
      api_key: "${PINECONE_API_KEY}"
      
      # Index host URL (required, from Pinecone console)
      index_host: "https://my-index-abc123.svc.us-east1-gcp.pinecone.io"
      
      # Optional control plane host override
      # control_plane_host: "https://api.pinecone.io"
      
      # Optional namespace for logical partitioning
      # namespace: "production"
      
      # Vector field in event data
      vector_field: "embedding"
      
      # Optional: sparse vector fields for hybrid search
      # sparse_vector_indices_field: "sparse_idx"
      # sparse_vector_values_field: "sparse_vals"
      
      # Vector ID strategy
      id_strategy: "field"            # field | uuid | hash
      id_field: "id"                  # Field to extract ID from (field/uuid strategies)
      
      # Batching and performance
      batch_size: 1000                # Vectors per batch (1–100,000)
      flush_interval_secs: 5          # Max seconds before flushing partial batch
      timeout_secs: 30                # Per-request timeout (seconds)
      
      # Error handling
      skip_invalid_events: false      # Skip events with missing/invalid vectors
      
      # Retry configuration
      max_retries: 3                  # Retries on transient failures
      initial_backoff_ms: 200         # Initial retry backoff
      max_backoff_ms: 5000            # Maximum retry backoff

      # Circuit breaker
      circuit_breaker:
        enabled: true                 # Enable circuit breaker (default)
        failure_threshold: 5          # Consecutive failures before opening
        reset_timeout_secs: 30        # Seconds before half-open probe
        success_threshold: 2          # Successes to close circuit
```

#### Pinecone Serverless Configuration

```yaml
sinks:
  pinecone-serverless:
    connector: pinecone
    topics: [embeddings.products]
    consumer_group: pinecone-serverless-sink
    config:
      api_key: "${PINECONE_API_KEY}"
      index_host: "https://my-index-xyz.svc.aped-4627-b74a.pinecone.io"
      vector_field: "embedding"
      namespace: "prod"
      id_strategy: "hash"
      batch_size: 5000
      timeout_secs: 60
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `api_key` | ✓ | - | Pinecone API key (from console) |
| `index_host` | ✓ | - | Index host URL (e.g. `https://my-index-abc.svc.pinecone.io`) |
| `control_plane_host` | | - | Retained for backward compatibility (unused by REST client) |
| `namespace` | | - | Namespace for logical partitioning |
| `vector_field` | | `embedding` | Field containing the dense vector (array of f32-range floats; values exceeding ±3.4e38 are rejected) |
| `sparse_vector_indices_field` | | - | Field for sparse vector indices (array of u32) |
| `sparse_vector_values_field` | | - | Field for sparse vector values (array of f32) |
| `id_strategy` | | `field` | Vector ID strategy: `field`, `uuid`, or `hash` |
| `id_field` | | `id` | Field to extract vector ID from (`field`/`uuid` strategies) |
| `batch_size` | | `1000` | Vectors per batch (1–100,000) |
| `flush_interval_secs` | | `5` | Maximum seconds before flushing partial batch |
| `timeout_secs` | | `30` | Per-request timeout (seconds, 1–300) |
| `skip_invalid_events` | | `false` | Skip events with missing or invalid vectors |
| `max_retries` | | `3` | Maximum retries on transient failures (0 to disable) |
| `initial_backoff_ms` | | `200` | Initial retry backoff (ms) |
| `max_backoff_ms` | | `5000` | Maximum retry backoff (ms) |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before half-open probe |
| `circuit_breaker.success_threshold` | | `2` | Successes in half-open to close circuit |

#### Circuit Breaker

The Pinecone connector includes a lock-free circuit breaker (using `AtomicU32`/`AtomicU64` with `Acquire`/`Release` ordering) that prevents cascading failures:

- **Closed**: All batches flow through normally.
- **Open**: After `failure_threshold` consecutive upsert failures, the circuit opens. Batches fail fast without contacting Pinecone, counted as `records_failed`.
- **Half-Open**: After `reset_timeout_secs`, one test batch is let through. If it succeeds (`success_threshold` times), the circuit closes. If the probe fails, the circuit immediately re-opens. The Open→HalfOpen transition uses `compare_exchange` to prevent TOCTOU races.

Circuit breaker configuration is validated at startup (`failure_threshold: 1–100`, `reset_timeout_secs: 1–3600`, `success_threshold: 1–10`).

#### Sparse Vector Support

Pinecone supports hybrid search combining dense and sparse vectors. Configure both fields together:

```yaml
config:
  vector_field: "embedding"
  sparse_vector_indices_field: "sparse_idx"
  sparse_vector_values_field: "sparse_vals"
```

Both `sparse_vector_indices_field` and `sparse_vector_values_field` must be set together. The indices array must contain u32 values and must match the length of the values array.

#### Metrics

| Metric | Type | Description |
|:-------|:-----|:------------|
| `pinecone.batches.success` | Counter | Successfully upserted batches |
| `pinecone.batches.failed` | Counter | Failed batch upserts (after all retries) |
| `pinecone.batches.retried` | Counter | Batch retry attempts |
| `pinecone.records.written` | Counter | Total vectors written |
| `pinecone.records.failed` | Counter | Failed/skipped records |
| `pinecone.circuit_breaker.rejected` | Counter | Batches rejected by open circuit |
| `pinecone.batch.size` | Gauge | Current batch size |
| `pinecone.batch.duration_ms` | Histogram | Batch upsert latency |

---

### Amazon S3 Vectors (Vector Database)

High-throughput vector upserts into [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html) via the official **AWS SDK for Rust** (`aws-sdk-s3vectors`).

**Key features**: PutVectors batch API via AWS SDK (SigV4 auth, HTTPS), batch upserts with configurable size and timer-based flush via `tokio::select!`, exponential backoff retry with jitter, structured error classification into typed `ConnectorError` variants (SDK error decomposition: `DispatchFailure`, `ConstructionFailure`, `ResponseError`, `TimeoutError` + message-based fallback for throttle/auth/network), lock-free circuit breaker (`AtomicU32`/`AtomicU64` with `compare_exchange` TOCTOU-safe Open→HalfOpen transition, already-Open guard preventing reset-timer starvation, HalfOpen failure counter), metrics integration (`metrics` crate, 9 metrics incl. retry counter), `#[inline]` on all hot-path functions (10 annotations), IAM-based credential resolution (instance profile, environment, SSO, etc.), auto-provisioning of vector bucket and index on first write (ConflictException-safe for concurrent replicas), multiple vector ID strategies (field, UUID with single-pass validation, deterministic FNV-1a hash), per-request timeout via SDK config, metadata support (arbitrary JSON mapped to `aws_smithy_types::Document` + auto-injected event metadata: `_event_type`, `_stream`, `_namespace`, `_timestamp`, `_key` with reserved-key collision validation), dotted-path field traversal with empty-segment rejection, `#[validate(length)]` on field paths, session-scoped structured logging, early rejection of empty/non-object/f32-overflow vectors.

#### Vector ID Strategies

The connector supports three strategies for generating S3 Vectors keys:

1. **Field** (`field`) — Extract a string/numeric ID from a field in the event data (e.g. `id`, `doc_id`)
2. **UUID** (`uuid`) — Extract a UUID string from a field (validated as exactly 32 or 36 hex characters with optional hyphens; single-pass validation)
3. **Hash** (`hash`) — Deterministic FNV-1a hash of the event data (stable across restarts, no field extraction needed)

#### Configuration

```yaml
sinks:
  s3vectors:
    connector: s3-vectors
    topics: [embeddings.products]
    consumer_group: s3vectors-sink
    config:
      # AWS region (required)
      region: "us-east-1"

      # Optional: custom endpoint URL (for LocalStack / testing)
      # endpoint_url: "http://localhost:4566"

      # S3 Vectors bucket name (required, 3–63 chars, lowercase/digits/hyphens)
      vector_bucket_name: "my-vectors"

      # Index name within the bucket (required, 1–512 chars)
      index_name: "products"

      # Vector dimension (required, 1–10,000)
      dimension: 1536

      # Distance metric (optional, default: cosine)
      distance_metric: "cosine"         # cosine | euclidean

      # Auto-create bucket and index on startup (default: true)
      auto_create: true

      # Vector field in event data
      vector_field: "embedding"

      # Vector ID strategy
      id_strategy: "field"              # field | uuid | hash
      id_field: "id"                    # Field to extract ID from (field/uuid strategies)

      # Optional: metadata fields to include with each vector
      # metadata_fields: ["category", "title", "price"]

      # Batching and performance
      batch_size: 100                   # Vectors per batch (1–10,000)
      flush_interval_secs: 5            # Max seconds before flushing partial batch
      timeout_secs: 30                  # Per-request timeout (seconds)

      # Error handling
      skip_invalid_events: false        # Skip events with missing/invalid vectors

      # Retry configuration
      max_retries: 3                    # Retries on transient failures
      initial_backoff_ms: 200           # Initial retry backoff
      max_backoff_ms: 5000              # Maximum retry backoff

      # Circuit breaker
      circuit_breaker:
        enabled: true                   # Enable circuit breaker (default)
        failure_threshold: 5            # Consecutive failures before opening
        reset_timeout_secs: 30          # Seconds before half-open probe
        success_threshold: 2            # Successes to close circuit
```

#### Auto-Provisioning

When `auto_create: true` (default), the connector automatically creates the vector bucket and index on first write if they don't exist. Existing resources are detected and reused (idempotent). Set `auto_create: false` in production to require pre-provisioned infrastructure.

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `region` | ✓ | - | AWS region (e.g. `us-east-1`) |
| `endpoint_url` | | - | Custom endpoint URL (for LocalStack, testing) |
| `vector_bucket_name` | ✓ | - | S3 Vectors bucket name (3–63 chars, `^[a-z0-9][a-z0-9-]*[a-z0-9]$`) |
| `index_name` | ✓ | - | Index name within the bucket (1–512 chars) |
| `dimension` | ✓ | - | Vector dimension (1–10,000) |
| `distance_metric` | | `cosine` | Distance metric: `cosine` or `euclidean` |
| `auto_create` | | `true` | Auto-create bucket and index on startup |
| `vector_field` | | `embedding` | Field containing the dense vector (array of f32-range floats; values exceeding ±3.4e38 are rejected) |
| `id_strategy` | | `field` | Vector ID strategy: `field`, `uuid`, or `hash` |
| `id_field` | | `id` | Field to extract vector ID from (`field`/`uuid` strategies) |
| `metadata_fields` | | - | Fields to include as vector metadata (mapped to S3V Document type) |
| `batch_size` | | `100` | Vectors per batch (1–10,000) |
| `flush_interval_secs` | | `5` | Maximum seconds before flushing partial batch |
| `timeout_secs` | | `30` | Per-request timeout (seconds, 1–300) |
| `skip_invalid_events` | | `false` | Skip events with missing or invalid vectors |
| `max_retries` | | `3` | Maximum retries on transient failures (0 to disable) |
| `initial_backoff_ms` | | `200` | Initial retry backoff (ms) |
| `max_backoff_ms` | | `5000` | Maximum retry backoff (ms) |
| `circuit_breaker.enabled` | | `true` | Enable circuit breaker |
| `circuit_breaker.failure_threshold` | | `5` | Consecutive failures before circuit opens |
| `circuit_breaker.reset_timeout_secs` | | `30` | Seconds before half-open probe |
| `circuit_breaker.success_threshold` | | `2` | Successes in half-open to close circuit |

#### Circuit Breaker

The S3 Vectors connector includes a lock-free circuit breaker (using `AtomicU32`/`AtomicU64` with `Acquire`/`Release` ordering) that prevents cascading failures:

- **Closed**: All batches flow through normally.
- **Open**: After `failure_threshold` consecutive upsert failures, the circuit opens. Batches fail fast without contacting S3 Vectors, counted as `records_failed`.
- **Half-Open**: After `reset_timeout_secs`, one test batch is let through. If it succeeds (`success_threshold` times), the circuit closes. If the probe fails, the circuit immediately re-opens. The Open→HalfOpen transition uses `compare_exchange` to prevent TOCTOU races.

Circuit breaker configuration is validated at startup (`failure_threshold: 1–100`, `reset_timeout_secs: 1–3600`, `success_threshold: 1–10`).

#### Metadata Support

S3 Vectors supports metadata on each vector. Specify which event fields to include:

```yaml
config:
  vector_field: "embedding"
  metadata_fields: ["category", "title", "price"]
```

Metadata values are automatically mapped from JSON to the S3 Vectors `Document` type (strings, numbers, booleans, arrays, nested objects, and null).

#### Metrics

| Metric | Type | Description |
|:-------|:-----|:------------|
| `s3vectors.batches.success` | Counter | Successfully upserted batches |
| `s3vectors.batches.failed` | Counter | Failed batch upserts (after all retries) |
| `s3vectors.batches.retried` | Counter | Batch retry attempts |
| `s3vectors.records.written` | Counter | Total vectors written |
| `s3vectors.records.failed` | Counter | Failed/skipped records |
| `s3vectors.circuit_breaker.rejected` | Counter | Batches rejected by open circuit |
| `s3vectors.batch.size` | Gauge | Current batch size |
| `s3vectors.batch.duration_ms` | Histogram | Batch upsert latency |

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

## Output Formats

Storage sinks (S3, GCS, Azure Blob) support multiple output formats. The format determines **how** data is serialized, while the storage connector determines **where** it is written.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Storage Connectors                           │
│  S3Sink, GcsSink, AzureBlobSink, LocalFileSink                  │
│                    (WHERE to write)                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Format Writers                               │
│  JsonWriter, JsonlWriter, ParquetWriter, AvroWriter, CsvWriter  │
│                    (HOW to serialize)                           │
└─────────────────────────────────────────────────────────────────┘
```

### Available Formats

| Format | Extension | MIME Type | Use Case | Compression | Append |
|--------|-----------|-----------|----------|-------------|--------|
| `json` | `.json` | `application/json` | Human-readable, debugging | gzip | ❌ |
| `jsonl` | `.jsonl` | `application/x-ndjson` | Streaming, log files | gzip | ✅ |
| `parquet` | `.parquet` | `application/vnd.apache.parquet` | Analytics, data lakes | snappy, zstd | ❌ |
| `avro` | `.avro` | `application/avro` | Schema Registry | deflate | ❌ |
| `csv` | `.csv` | `text/csv` | Spreadsheets, exports | gzip | ✅ |

### JSON Lines (JSONL)

Default format for most use cases. Each event is written as a separate line.

```yaml
config:
  format: jsonl
  compression: gzip
```

**Output:**
```json
{"id":1,"event":"order.created","timestamp":"2024-01-15T10:30:00Z"}
{"id":2,"event":"order.shipped","timestamp":"2024-01-15T11:00:00Z"}
```

**Advantages:**
- Streamable (can be processed line by line)
- Easy to append to existing files
- Simple to parse with standard tools (`jq`, `grep`)

### JSON

Single JSON array containing all events.

```yaml
config:
  format: json
```

**Output:**
```json
[
  {"id":1,"event":"order.created","timestamp":"2024-01-15T10:30:00Z"},
  {"id":2,"event":"order.shipped","timestamp":"2024-01-15T11:00:00Z"}
]
```

### Parquet

Apache Parquet columnar format for analytics workloads.

```yaml
config:
  format: parquet
  parquet:
    compression: snappy  # Options: none, snappy, gzip, lz4, zstd, brotli
    row_group_size: 10000
    enable_statistics: true
```

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `compression` | `snappy` | Compression codec |
| `row_group_size` | `10000` | Rows per row group |
| `enable_statistics` | `true` | Write column statistics in footer |
| `data_page_size` | `1048576` | Data page size in bytes |
| `dictionary_page_size` | `1048576` | Dictionary page size limit |

**Compression Comparison:**

| Codec | Speed | Ratio | CPU Usage | Use Case |
|-------|-------|-------|-----------|----------|
| `none` | ⚡⚡⚡ | - | Low | Testing, already compressed data |
| `snappy` | ⚡⚡⚡ | Good | Low | **Default** - balanced |
| `lz4` | ⚡⚡⚡ | Good | Low | High throughput |
| `zstd` | ⚡⚡ | Better | Medium | Cold storage, archives |
| `gzip` | ⚡ | Better | High | Maximum compatibility |
| `brotli` | ⚡ | Best | High | Maximum compression |

**Schema Inference:**

Parquet requires a schema. Rivven automatically infers the Arrow schema from JSON events:

```json
{"name": "Alice", "age": 30, "active": true}
```

Becomes:
```
name: Utf8 (nullable)
age: Int64 (nullable)
active: Boolean (nullable)
```

**Type Promotion:**

When types are mixed across records, Rivven promotes to the wider type:

| Type A | Type B | Result |
|--------|--------|--------|
| Int64 | Float64 | Float64 |
| Int64 | Utf8 | Utf8 |
| Null | Any | Any (nullable) |

### Avro

Apache Avro binary format with Object Container File (OCF) support. Ideal for Schema Registry integration.

```yaml
config:
  format: avro
  # Optional: provide explicit schema (otherwise inferred from data)
  # avro:
  #   schema: '{"type":"record","name":"Event","fields":[...]}'
  #   compression: deflate  # Options: none, deflate
  #   wire_format: true  # Add schema ID header
  #   schema_id: 12345
```

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `schema` | (inferred) | Explicit Avro schema JSON string |
| `compression` | `none` | Compression codec (`none`, `deflate`) |
| `wire_format` | `false` | Add magic byte + schema ID prefix |
| `schema_id` | - | Schema ID for wire format |
| `namespace` | `rivven.events` | Namespace for inferred schemas |
| `record_name` | `Event` | Record name for inferred schemas |

**Automatic Schema Inference:**

```json
{"id": 1, "name": "Alice", "active": true}
```

Becomes:
```json
{
  "type": "record",
  "name": "Event",
  "namespace": "rivven.events",
  "fields": [
    {"name": "active", "type": "boolean"},
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
  ]
}
```

**Use Cases:**
- Integration with Schema Registry
- Schema evolution with backward/forward compatibility
- Compact binary format with schema ID reference (wire format)
- Enterprise platform integration

### CSV

Comma-Separated Values format for spreadsheet exports and legacy system integration.

```yaml
config:
  format: csv
  compression: gzip
  # csv:
  #   delimiter: ","      # Options: ",", "\t", ";", "|"
  #   include_header: true
  #   quote_char: '"'
  #   null_value: "NULL"
```

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `delimiter` | `,` | Field delimiter (`,`, `\t`, `;`, `\|`) |
| `include_header` | `true` | Include header row with column names |
| `quote_char` | `"` | Quote character for escaping |
| `always_quote` | `false` | Always quote all fields |
| `line_ending` | `lf` | Line ending style (`lf`, `crlf`) |
| `null_value` | `NULL` | Representation for null values |
| `columns` | (auto) | Explicit column order |

**Output Example:**
```csv
active,id,name
true,1,Alice
false,2,Bob
```

**Nested Object Handling:**

Nested JSON objects are serialized as JSON strings with escaped quotes:

```json
{"id": 1, "data": {"nested": "value"}}
```

Becomes:
```csv
data,id
"{""nested"":""value""}",1
```

**Use Cases:**
- Excel/spreadsheet exports
- Legacy system integration
- Simple data exchange
- Human-readable tabular data

### Format Selection Guide

```
                      ┌─────────────────────┐
                      │   What's your use   │
                      │       case?         │
                      └─────────┬───────────┘
                                │
       ┌────────────────┬───────┼───────┬────────────────┐
       │                │       │       │                │
       ▼                ▼       ▼       ▼                ▼
┌───────────┐   ┌───────────┐ ┌─────┐ ┌───────────┐ ┌───────────┐
│  Analytics│   │  Streaming│ │Schema │  │  Debugging│ │Spreadsheet│
│  Data Lake│   │  Real-time│ │Reg.  │  │  Logs     │ │  Export   │
└─────┬─────┘   └─────┬─────┘ └──┬──┘  └─────┬─────┘ └─────┬─────┘
      ▼               ▼          ▼          ▼             ▼
┌───────────┐   ┌───────────┐ ┌─────┐ ┌───────────┐ ┌───────────┐
│  Parquet  │   │   JSONL   │ │Avro │ │   JSON    │ │   CSV     │
│  +zstd    │   │   +gzip   │ │     │ │  (pretty) │ │  +gzip    │
└───────────┘   └───────────┘ └─────┘ └───────────┘ └───────────┘
```

### Output Compression

Text-based formats (JSON, JSONL, CSV) can have an additional compression layer applied to the output file. This is separate from format-internal compression (Parquet and Avro have their own built-in compression).

```yaml
config:
  format: jsonl
  compression: gzip  # Apply gzip compression to the output file
```

**Available Compression Algorithms:**

| Algorithm | Extension | Speed | Ratio | Use Case |
|-----------|-----------|-------|-------|----------|
| `none` | - | N/A | 1x | No compression |
| `gzip` | `.gz` | Medium | Good | Universal compatibility |
| `zstd` | `.zst` | Fast | Excellent | Modern systems, best balance |
| `snappy` | `.snappy` | Very Fast | Moderate | Interoperability |
| `lz4` | `.lz4` | Fastest | Moderate | Real-time streaming |

**Example: Gzip-compressed JSONL to S3:**

```yaml
sinks:
  s3:
    connector: s3
    config:
      bucket: my-bucket
      prefix: events/
      format: jsonl
      compression: gzip  # Output files: events/2024/01/15/events_001.jsonl.gz
```

**Example: Snappy-compressed Parquet:**

```yaml
sinks:
  s3:
    connector: s3
    config:
      bucket: analytics-lake
      format: parquet
      parquet:
        compression: snappy  # Built-in Parquet compression (not output_compression)
```

**Compression Type Summary:**

| Format | Internal Compression | Output Compression |
|--------|---------------------|-------------------|
| JSON/JSONL | ❌ | ✅ gzip, zstd, snappy, lz4 |
| CSV/TSV | ❌ | ✅ gzip, zstd, snappy, lz4 |
| Parquet | ✅ snappy, zstd, lz4, gzip, brotli | ❌ (use internal) |
| Avro | ✅ deflate, snappy | ❌ (use internal) |

---

## Transforms

Apply transformations between source and sink. Transforms are validated at connector startup — invalid transform types or configurations cause immediate startup failure rather than silent runtime errors.

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

### LLM Chat Transform

Enrich events by sending data through an LLM (Large Language Model) and writing the response into the event. Supports OpenAI-compatible APIs and AWS Bedrock.

```yaml
transforms:
  classify:
    transform: llm-chat
    config:
      provider: openai           # openai | bedrock
      api_key: "${OPENAI_API_KEY}"
      model: gpt-4o-mini
      system_prompt: "Classify sentiment as: positive, negative, neutral. Reply with one word."
      prompt_template: "{{text}}"
      output_field: sentiment
      temperature: 0.0
      max_tokens: 10
      timeout_secs: 30
      skip_on_error: false       # pass events through on LLM failure
```

**Configuration:**

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `provider` | `openai` \| `bedrock` | `openai` | LLM provider |
| `api_key` | string | — | API key (required for OpenAI; Bedrock uses AWS credential chain) |
| `base_url` | string | — | Base URL override (Azure OpenAI, Ollama, vLLM) |
| `region` | string | `us-east-1` | AWS region (Bedrock only) |
| `model` | string | **required** | Model identifier |
| `system_prompt` | string | — | System prompt for LLM persona/behavior |
| `prompt_template` | string | **required** | Template with `\{\{field\}\}` placeholders |
| `output_field` | string | `llm_response` | Field name for the LLM response |
| `temperature` | float | — | Sampling temperature (0.0–2.0) |
| `max_tokens` | integer | — | Maximum response tokens |
| `timeout_secs` | integer | `60` | Request timeout in seconds |
| `skip_on_error` | boolean | `false` | Pass events through on LLM failure |

**Template Placeholders:**

- `\{\{field\}\}` — event data field (supports nested dot-notation: `\{\{address.city\}\}`)
- `\{\{_data\}\}` — entire event data as JSON
- `\{\{_stream\}\}` — stream/topic name
- `\{\{_timestamp\}\}` — event timestamp (RFC 3339)

Template rendering is injection-safe: field values containing placeholder syntax are never re-expanded.

**Metrics (chat):** `llm.chat.requests.success`, `llm.chat.requests.failed`, `llm.chat.requests.timeout`, `llm.chat.duration_ms`, `llm.chat.tokens.prompt`, `llm.chat.tokens.completion`, `llm.chat.tokens.total`, `llm.chat.finish_reason.length` (truncated), `llm.chat.finish_reason.content_filter` (censored), `llm.chat.finish_reason.other`

### LLM Embedding Transform

Generate vector embeddings from event text fields for downstream vector DB sinks (Qdrant, Pinecone, etc.).

```yaml
transforms:
  vectorize:
    transform: llm-embedding
    config:
      provider: openai
      api_key: "${OPENAI_API_KEY}"
      model: text-embedding-3-small
      input_field: text
      output_field: embedding
      dimensions: 1536           # validate output dimensions (0 = skip)
      timeout_secs: 30
      skip_on_missing: true      # pass through if input field missing
      skip_on_error: false       # pass through on embedding failure
```

**Configuration:**

| Field | Type | Default | Description |
|:------|:-----|:--------|:------------|
| `provider` | `openai` \| `bedrock` | `openai` | LLM provider |
| `api_key` | string | — | API key (required for OpenAI) |
| `base_url` | string | — | Base URL override |
| `region` | string | `us-east-1` | AWS region (Bedrock only) |
| `model` | string | **required** | Embedding model identifier |
| `input_field` | string | `text` | Field containing text to embed |
| `output_field` | string | `embedding` | Field for the embedding vector |
| `dimensions` | integer | `0` | Embedding dimensions — passed to the API as a hint; also validates output (0 = skip) |
| `input_type` | string | — | Input type hint (`search_document`, `search_query`) — Cohere Bedrock models |
| `timeout_secs` | integer | `60` | Request timeout |
| `skip_on_missing` | boolean | `false` | Pass through if input field is missing |
| `skip_on_error` | boolean | `false` | Pass through on embedding failure |

**Metrics (embedding):** `llm.embedding.requests.success`, `llm.embedding.requests.failed`, `llm.embedding.requests.timeout`, `llm.embedding.duration_ms`, `llm.embedding.tokens.prompt`, `llm.embedding.tokens.total`

**Example Pipeline — Sentiment + Embeddings:**

```yaml
sources:
  reviews:
    connector: postgres-cdc
    topic: cdc.reviews
    transforms:
      - type: ExtractNewRecordState
      - transform: llm-chat
        config:
          provider: openai
          api_key: "${OPENAI_API_KEY}"
          model: gpt-4o-mini
          system_prompt: "Classify as positive/negative/neutral. One word."
          prompt_template: "{{text}}"
          output_field: sentiment
          temperature: 0.0
      - transform: llm-embedding
        config:
          provider: openai
          api_key: "${OPENAI_API_KEY}"
          model: text-embedding-3-small
          input_field: text
          output_field: embedding

sinks:
  vectors:
    connector: qdrant
    config:
      url: http://localhost:6334
      collection: reviews
```

---

## Sink Reliability

### Consecutive Error Threshold

Sinks track consecutive write failures. After a configurable number of consecutive errors (default: 10), the sink transitions to a `Failed` state and stops consuming, preventing silent data loss from persistent downstream failures.

### Health Checks

Before a connector transitions to `Running` state, rivven-connect performs a health check (e.g., testing database connectivity or verifying bucket access). This ensures connectors do not report as healthy before the downstream system is actually reachable.

### Epoch Fencing

During failover or rebalancing, sinks use epoch-based fencing to prevent stale connector instances from writing duplicate data. A new epoch is assigned on each restart, and writes from previous epochs are rejected by the coordinator.

### Automatic Runner Restart

Source and sink runner tasks automatically restart on failure with exponential backoff. If a connector crashes or encounters a fatal error, the framework restarts it without manual intervention:

- **Initial delay:** 1 second
- **Maximum delay:** 60 seconds
- **Backoff multiplier:** 2×
- **Shutdown-aware:** Restart loops respect shutdown signals — no restarts during graceful shutdown

The connector transitions to `Failed` state only if shutdown is explicitly requested or recovery is impossible. Health state is updated on each restart attempt for monitoring visibility.

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

- [CDC Guide](cdc) — Database replication details
- [Security](security) — TLS and authentication
- [Kubernetes](kubernetes) — Production deployment
