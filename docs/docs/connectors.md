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
│  ├── CDC (postgres_cdc, mysql_cdc, mongodb_cdc, ...)            │
│  ├── Batch (sql_select, sql_insert, ...)                        │
│  └── NoSQL (redis, cassandra, ...)                              │
│                                                                  │
│  Messaging                                                       │
│  ├── Queues (kafka, external, ...)                              │
│  ├── MQTT (mqtt, rabbitmq, ...)                                 │
│  └── Cloud (sqs, pubsub, azure_queue, ...)                      │
│                                                                  │
│  Storage                                                         │
│  ├── Object (s3, gcs, azure_blob, minio, ...)                   │
│  └── File (file, sftp, hdfs, ...)                               │
│                                                                  │
│  Warehouse                                                       │
│  └── (snowflake, bigquery, redshift, clickhouse, ...)           │
│                                                                  │
│  Lakehouse                                                       │
│  └── (iceberg, ...)                           │
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

### External Queue Source

Consume events from external message queues (for migration or hybrid deployments).

```yaml
sources:
  external-queue:
    connector: external-queue
    topic: rivven-events
    config:
      bootstrap_servers: queue-broker:9092
      source_topics:
        - upstream-topic-1
        - upstream-topic-2
      consumer_group: rivven-consumer
      auto_offset_reset: earliest
      security:
        protocol: sasl_ssl
        mechanism: SCRAM_SHA_512
        username: ${QUEUE_USER}
        password: ${QUEUE_PASSWORD}
```

| Parameter | Required | Default | Description |
|:----------|:---------|:--------|:------------|
| `bootstrap_servers` | ✓ | - | Broker addresses |
| `source_topics` | ✓ | - | Topics to consume |
| `consumer_group` | ✓ | - | Consumer group ID |
| `auto_offset_reset` | | `latest` | Offset reset policy |
| `security.protocol` | | `plaintext` | Security protocol |

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

### External Queue Sink

Forward events to external message queues.

```yaml
sinks:
  external-queue:
    connector: external-queue
    topics: [events]
    consumer_group: queue-sink
    config:
      bootstrap_servers: queue-broker:9092
      target_topic: downstream-events
      acks: all
      compression: lz4
      security:
        protocol: sasl_ssl
        mechanism: SCRAM_SHA_512
        username: ${QUEUE_USER}
        password: ${QUEUE_PASSWORD}
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

- [CDC Guide](cdc) — Database replication details
- [Security](security) — TLS and authentication
- [Kubernetes](kubernetes) — Production deployment
