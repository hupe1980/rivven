# rivven-connect

> Config-driven connector runtime for the Rivven event streaming platform.

## Overview

`rivven-connect` provides a single binary that handles all data movement operations:

- **Sources**: Extract data from external systems (PostgreSQL CDC, MySQL, APIs)
- **Sinks**: Load data into destinations (Rivven broker, S3, HTTP APIs)
- **Transforms**: Process events in-flight (filter, mask PII, enrich)

## Scalable Architecture (300+ Connectors)

rivven-connect is designed to scale to **300+ connectors** with:

- **Hierarchical Categories**: Connectors organized by type (database, messaging, storage, warehouse, AI/ML)
- **Rich Metadata**: Tags and search capabilities
- **Connector Inventory**: Auto-registration with metadata indexing for fast discovery
- **Feature Gating**: Compile only the connectors you need

```
┌─────────────────────────────────────────────────────────────────┐
│                 Connector Inventory (Scalable)                  │
├─────────────────────────────────────────────────────────────────┤
│  Categories                                                     │
│  ├── Database (postgres_cdc, mysql_cdc, sqlserver_cdc, ...).    │
│  ├── Messaging (mqtt, sqs, pubsub, nats, ...)                   │
│  ├── Storage (s3, gcs, azure, minio, ...)                       │
│  ├── Warehouse (snowflake, bigquery, redshift, databricks, ...)  │
│  └── Utility (datagen, stdout, http, ...)                       │
└─────────────────────────────────────────────────────────────────┘
```

### Runtime Registry Dispatch

When `rivven-connect` encounters a connector type that isn't hardcoded (e.g., a community or custom connector), the `SourceRunner` and `SinkRunner` automatically fall back to the **connector registry**. The registry uses type-erased `AnySource`/`AnySink` dispatch so third-party connectors get the same pipeline features as built-in ones:

- **Check → Discover → Read/Write** lifecycle via `SourceFactory`/`SinkFactory`
- Same rate limiting, backpressure, and transform pipeline as built-in connectors
- Zero code changes needed — just register your factory in `create_source_registry()` / `create_sink_registry()`

### Lock-Free Rate Limiter

The token-bucket rate limiter on source and sink hot paths is **fully lock-free**. It uses `AtomicU64` with CAS loops for both refill timestamp tracking and token management — zero `RwLock` or `Mutex` contention under concurrent workloads.

### Zero-Allocation Batch Sizing

Batch size estimation uses `SourceEvent::estimated_size()` — a heuristic that walks field lengths without serialization. This eliminates the `serde_json::to_vec()` allocation that was previously required per event during batch splitting, keeping the hot path allocation-free.

## Security Architecture

`rivven-connect` runs as a **separate process** from the Rivven broker. This design:

1. **Security**: Database credentials are isolated - if the broker is compromised, attackers don't get DB access
2. **Scalability**: CDC connectors can run on different machines, closer to source databases
3. **Simplicity**: Broker stays lean and focused on messaging

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   PostgreSQL    │─────▶│  rivven-connect │─────▶│     rivvend     │
│   MySQL         │      │  (config-driven)│      │    (broker)     │
│   HTTP API      │      │                 │      │                 │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

## Quick Start

### 1. Generate Example Configuration

```bash
rivven-connect init > rivven-connect.yaml
```

### 2. Edit Configuration

```yaml
version: "1.0"

broker:
  bootstrap_servers:
    - localhost:9092

sources:
  postgres_db:
    connector: postgres-cdc
    topic: cdc.postgres.events       # Required: destination topic
    config:
      host: localhost
      port: 5432
      database: mydb
      user: postgres
      password: ${POSTGRES_PASSWORD}
      slot_name: rivven_slot
      publication: rivven_pub
      topic_routing: "cdc.{schema}.{table}"  # Optional: dynamic routing
      schemas:
        - public
      tables:
        - users
        - orders

sinks:
  s3_archive:
    connector: s3
    topics: [cdc.postgres.events]    # Required: topics to consume
    consumer_group: s3-archive       # Required: for offset tracking
    config:
      bucket: my-data-lake
      region: us-east-1
      format: parquet
```

### 3. Check Configuration

```bash
rivven-connect check --config rivven-connect.yaml
```

### 4. Run Pipelines

```bash
rivven-connect run --config rivven-connect.yaml
```

## Commands

| Command | Description |
|---------|-------------|
| `run` | Run all enabled pipelines |
| `check` | Validate config and test connectivity |
| `discover` | Discover available streams from a source |
| `spec` | Show connector specifications |
| `init` | Generate example configuration |
| `status` | Show status of running pipelines |
| `postgres` | Legacy mode: direct PostgreSQL CDC |

## Available Connectors

### Sources

| Connector | Feature | Description |
|-----------|---------|-------------|
| `datagen` | (default) | Synthetic data generator for testing |
| `postgres-cdc` | `postgres` | PostgreSQL logical replication CDC |
| `mysql-cdc` | `mysql` | MySQL/MariaDB binlog CDC |
| `sqlserver-cdc` | `sqlserver` | SQL Server Change Data Capture |
| `rdbc-source` | `rdbc` | Query-based polling source (PostgreSQL, MySQL, SQL Server) |
| `kafka-source` | `kafka` | Apache Kafka consumer (pure Rust, zero C deps) |
| `external-queue` | `external-queue` | External message queue consumer |
| `mqtt` | `mqtt` | MQTT broker subscriber (rumqttc, TLS, exponential backoff) |
| `sqs` | `sqs` | AWS SQS queue consumer |
| `pubsub` | `pubsub` | Google Cloud Pub/Sub subscriber |

### Sinks

| Connector | Feature | Description |
|-----------|---------|-------------|
| `stdout` | (default) | Console output for debugging |
| `http-webhook` | `http` | HTTP/HTTPS webhooks with signing |
| `rdbc-sink` | `rdbc` | Batch database sink (PostgreSQL, MySQL, SQL Server) |
| `kafka-sink` | `kafka` | Apache Kafka producer (pure Rust, zero C deps) |
| `sqs-sink` | `sqs` | AWS SQS queue producer (batch sending, FIFO support) |
| `external-queue` | `external-queue` | External message queue producer |
| `object-storage` | `cloud-storage` | Unified object storage (S3, GCS, Azure, local) |
| `s3` | `s3` | Amazon S3 / MinIO / R2 (via object-storage) |
| `gcs` | `gcs` | Google Cloud Storage (via object-storage) |
| `azure-blob` | `azure` | Azure Blob Storage (via object-storage) |
| `iceberg` | `iceberg` | Apache Iceberg lakehouse format |
| `delta-lake` | `delta-lake` | Delta Lake with ACID transactions |
| `snowflake` | `snowflake` | Snowflake Data Cloud |
| `bigquery` | `bigquery` | Google BigQuery |
| `redshift` | `redshift` | Amazon Redshift |
| `databricks` | `databricks` | Databricks Delta tables via Zerobus |

### Unified Object Storage

All object storage connectors (S3, GCS, Azure Blob) now use a single unified implementation
powered by the `object_store` crate. Configure with the `provider` field:

```yaml
sinks:
  events:
    connector: object-storage
    topics: [app.events]             # Required: topics to consume
    consumer_group: object-storage   # Required: for offset tracking
    config:
      provider: s3    # s3 | gcs | azure | local
      bucket: my-bucket
      prefix: events/
      format: jsonl
      compression: gzip
      partitioning: day
      s3:
        region: us-east-1
```

### Apache Iceberg

The Iceberg sink writes events to Apache Iceberg tables using the **official Apache Iceberg Rust SDK** (`iceberg` crate v0.8.0). Features include:

- **Full Writer Stack**: Uses `ParquetWriterBuilder`, `RollingFileWriterBuilder`, and `DataFileWriterBuilder`
- **Catalog Management**: REST (Polaris, Tabular, Lakekeeper) and Memory catalogs
- **Table Management**: Automatic namespace and table creation with schema inference
- **Atomic Transactions**: Fast append via Transaction API with snapshot isolation
- **Configurable Compression**: Snappy (default), Gzip, LZ4, Zstd, Brotli, or None
- **Production-Ready**: Arrow/Parquet v57.x with structured logging

```yaml
sinks:
  lakehouse:
    connector: iceberg
    topics: [cdc.events]             # Required: topics to consume
    consumer_group: iceberg-sink     # Required: for offset tracking
    config:
      catalog:
        type: rest            # rest | glue | hive | memory
        rest:
          uri: http://polaris:8181
          warehouse: s3://bucket/warehouse
      namespace: analytics
      table: events
      batch_size: 10000
      target_file_size_mb: 128  # Rolling file size
      compression: snappy       # none | snappy | gzip | lz4 | zstd | brotli
      partitioning: time        # none | identity | bucket | time
      partition_fields: [event_date]
      commit_mode: append       # append | overwrite | upsert
      s3:
        region: us-west-2
        endpoint: http://minio:9000  # For MinIO
        path_style_access: true
```

See [docs/ICEBERG_SINK.md](../../docs/ICEBERG_SINK.md) for complete configuration reference.

### Delta Lake

The Delta Lake sink writes events to Delta Lake tables using the **delta-rs** native Rust implementation (`deltalake` crate v0.30). Features include:

- **ACID Transactions**: Every write is atomic with snapshot isolation via the Delta log
- **No JVM Required**: Pure Rust via delta-rs (3k+ stars)
- **Storage Backends**: S3, GCS, Azure, local filesystem
- **RecordBatchWriter**: Arrow-native write path with Parquet compression
- **Auto Table Creation**: Creates tables on first write with configurable schema
- **Commit Retry**: Exponential backoff on transaction conflicts

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
      partition_columns:
        - event_type
      s3:
        region: us-east-1
```

See [docs/delta-lake-sink.md](../../docs/docs/delta-lake-sink.md) for complete configuration reference.

### Apache Kafka (Pure Rust)

The Kafka connector provides bidirectional Kafka integration using [rskafka](https://crates.io/crates/rskafka), a pure Rust Kafka client with **zero C dependencies**:

- **Pure Rust**: No librdkafka, no C compiler needed
- **Lock-free Metrics**: Atomic counters with Prometheus export
- **Exponential Backoff**: Configurable retry with backoff
- **Compression**: None, Gzip, Snappy, LZ4, Zstd
- **Security**: SASL PLAIN with SSL/TLS

```yaml
# Source: Consume from Kafka
sources:
  kafka_orders:
    connector: kafka-source
    topic: kafka-orders           # Rivven topic (destination)
    config:
      brokers: ["kafka1:9092", "kafka2:9092"]
      topic: orders               # Kafka topic (external source)
      consumer_group: rivven-migration
      start_offset: earliest
      fetch_max_messages: 500
      retry_initial_ms: 100
      retry_max_ms: 10000
      security:
        protocol: sasl_ssl
        mechanism: PLAIN
        username: ${KAFKA_USER}
        password: ${KAFKA_PASSWORD}

# Sink: Produce to Kafka
sinks:
  kafka_replica:
    connector: kafka-sink
    topics: [orders]              # Rivven topics to consume from
    consumer_group: kafka-producer
    config:
      brokers: ["kafka1:9092"]
      topic: orders-replica       # Kafka topic (external destination)
      compression: lz4
      acks: all
      batch_size_bytes: 16384
      linger_ms: 5
```

See [docs/kafka-connector.md](../../docs/docs/kafka-connector.md) for complete configuration reference.

### HTTP Webhook Sink

Best-in-class HTTP webhook sink with enterprise features:

- **Methods**: POST, PUT, PATCH, DELETE
- **Authentication**: Bearer, Basic, API Key
- **Signing**: HMAC-SHA256 webhook signatures
- **Compression**: gzip for payloads above threshold
- **Circuit Breaker**: Fail fast when endpoint is unavailable
- **Retry Strategy**: Exponential backoff with jitter
- **URL Templates**: Dynamic URLs with `{stream}`, `{event_type}`, `{namespace}`
- **Batching**: Configurable size and timeout
- **TLS**: Custom CA certificates

```yaml
sinks:
  events:
    connector: http-webhook
    topics: [orders, users]          # Required: topics to consume
    consumer_group: http-webhook     # Required: for offset tracking
    config:
      url: https://api.example.com/{stream}/events
      method: POST
      batch_size: 100
      batch_timeout_ms: 5000

      # Authentication
      auth:
        type: bearer
        token: ${WEBHOOK_TOKEN}

      # HMAC Signing (for webhook verification)
      signing:
        secret: ${WEBHOOK_SECRET}
        header_name: X-Webhook-Signature
        include_timestamp: true

      # Compression (for large payloads)
      compression_threshold_bytes: 1024

      # Resilience
      max_retries: 3
      retry_backoff_ms: 1000
      retry_jitter: true
      circuit_breaker_threshold: 5
      circuit_breaker_reset_secs: 30

      # Advanced
      max_concurrency: 4
      timeout_secs: 30
      health_check_url: https://api.example.com/health
      headers:
        X-Custom-Header: value
```

**Metrics** (via `/metrics`):
- `requests_sent`, `requests_success`, `requests_failed`
- `circuit_breaker_rejections`, `rate_limited`
- `bytes_sent`, `bytes_compressed`, `avg_latency_ms`

### RDBC Connectors

Query-based connectors powered by `rivven-rdbc`. Features hot path optimizations including true batch operations (10-100x throughput vs per-row), connection lifecycle management, and optional transactional writes for exactly-once semantics.

#### RDBC Source

Query-based polling source supporting bulk, incrementing, timestamp, and combined modes:

```yaml
sources:
  users:
    connector: rdbc-source
    topic: user-events                # Required: destination topic
    config:
      connection_url: postgres://user:pass@localhost/db
      schema: public
      table: users
      mode: incrementing              # bulk | incrementing | timestamp | timestamp_incrementing
      incrementing_column: id
      poll_interval_ms: 1000
      batch_size: 1000
      # Pool configuration
      pool_size: 1                    # Optional: max connections (default: 1)
      min_pool_size: 1                # Optional: warm-up connections (default: 1)
      max_lifetime_secs: 3600         # Optional: max connection age (default: 1 hour)
      idle_timeout_secs: 600          # Optional: idle timeout (default: 10 minutes)
      acquire_timeout_ms: 30000       # Optional: pool wait timeout (default: 30 seconds)
```

#### RDBC Sink

High-performance batch writer with connection pooling, upsert, insert, and delete support:

```yaml
sinks:
  users_copy:
    connector: rdbc-sink
    topics: [user-events]             # Required: topics to consume
    consumer_group: rdbc-warehouse    # Required: for offset tracking
    config:
      connection_url: postgres://user:pass@localhost/warehouse
      schema: public
      table: users_snapshot
      write_mode: upsert              # insert | update | upsert
      pk_columns: [id]
      batch_size: 1000
      batch_timeout_ms: 5000
      delete_enabled: true
      transactional: true             # Optional: exactly-once semantics
      # Pool configuration with lifecycle management
      pool_size: 4                    # Optional: max connections (default: 4)
      min_pool_size: 2                # Optional: warm-up connections (default: 1)
      max_lifetime_secs: 3600         # Optional: max connection age (default: 1 hour)
      idle_timeout_secs: 600          # Optional: idle timeout (default: 10 minutes)
      acquire_timeout_ms: 30000       # Optional: pool wait timeout (default: 30 seconds)
```

#### Pool Metrics

RDBC connectors expose pool metrics via the Prometheus endpoint (`/metrics`):

| Metric | Description |
|--------|-------------|
| `rivven_connect_pool_connections_total` | Total connections created |
| `rivven_connect_pool_acquisitions_total` | Total pool acquisitions |
| `rivven_connect_pool_reuse_ratio` | Connection reuse ratio (0-1) |
| `rivven_connect_pool_avg_wait_ms` | Average acquisition wait time |
| `rivven_connect_pool_recycled_total` | Connections recycled (by reason) |
| `rivven_connect_pool_health_failures_total` | Failed health checks |

#### Multi-Database Support

Use the `rdbc-*` features to enable database drivers:

```toml
rivven-connect = { version = "0.0.13", features = ["rdbc-postgres"] }       # PostgreSQL
rivven-connect = { version = "0.0.13", features = ["rdbc-mysql"] }          # MySQL/MariaDB  
rivven-connect = { version = "0.0.13", features = ["rdbc-sqlserver"] }      # SQL Server
rivven-connect = { version = "0.0.13", features = ["rdbc-full"] }           # All databases
```

### Feature Bundles

```toml
# In Cargo.toml
rivven-connect = { version = "0.0.13", features = ["full"] }

# Or selective features
rivven-connect = { version = "0.0.13", features = ["postgres", "s3"] }
```

| Bundle | Includes |
|--------|----------|
| `rdbc-full` | rdbc-postgres, rdbc-mysql, rdbc-sqlserver |
| `queue-full` | mqtt, sqs, pubsub |
| `storage-full` | s3, gcs, azure, parquet |
| `lakehouse-full` | iceberg, delta-lake |
| `warehouse-full` | snowflake, bigquery, redshift, databricks |
| `full` | All connectors (including rdbc-full) |

### Single Message Transforms (SMT)

CDC connectors support 17 built-in transforms applied via YAML configuration.
No code required - fully configurable at deployment time.

| Transform | Description |
|-----------|-------------|
| `extract_new_record_state` | Flatten envelope, extract "after" state |
| `value_to_key` | Extract key fields from value |
| `mask_field` | Mask sensitive fields (PII, credit cards) |
| `insert_field` | Add static or computed fields |
| `replace_field` / `rename_field` | Rename, include, or exclude fields |
| `regex_router` | Route events based on regex patterns |
| `timestamp_converter` | Convert timestamp formats |
| `timezone_converter` | Convert between timezones |
| `filter` | Filter events based on conditions |
| `cast` | Convert field types |
| `flatten` | Flatten nested JSON structures |
| `content_router` | Route based on field content |
| `header_to_value` | Copy envelope metadata into record |
| `unwrap` | Extract nested field to top level |
| `set_null` | Set fields to null conditionally |
| `compute_field` | Compute new fields (concat, hash, etc.) |
| `externalize_blob` | Store large blobs in object storage (S3/GCS/Azure/local) |

#### Predicates (Conditional Transforms)

Apply transforms only to events matching specific conditions:

```yaml
transforms:
  # Mask SSN only for users table
  - type: mask_field
    predicate:
      table: "users"
    config:
      fields: [ssn, credit_card]
  
  # Externalize blobs only for documents table
  - type: externalize_blob
    predicate:
      table: "documents"
      operations: [insert, update]
    config:
      storage_type: s3
      bucket: my-blobs
```

**Predicate Options:**
- `table` / `tables` - Filter by table name (regex)
- `schema` / `schemas` - Filter by schema name (regex)
- `operations` - Filter by operation (insert, update, delete, snapshot)
- `field_exists` - Apply only if field exists
- `field_value` - Apply only if field matches value
- `negate` - Invert the predicate

**Example Configuration:**

```yaml
sources:
  postgres_cdc:
    connector: postgres-cdc
    config:
      host: localhost
      port: 5432
      database: mydb
      user: replicator
      password: ${DB_PASSWORD}
      
      # Column-level filtering
      column_filters:
        public.users:
          include: [id, email, name]
          exclude: [password_hash]
      
      # Transform pipeline
      transforms:
        - type: mask_field
          config:
            fields: [ssn, credit_card]
        
        - type: timestamp_converter
          config:
            fields: [created_at, updated_at]
            format: iso8601
        
        - type: filter
          config:
            condition: "status = active"
            drop: false
        
        - type: extract_new_record_state
          config:
            add_table: true
            add_op: true
```

See the full [CDC Configuration Reference](../../docs/docs/cdc-configuration.md) for all options.

### CDC Features

CDC connectors include advanced built-in features for production workloads:

| Feature | Description |
|---------|-------------|
| **Field-Level Encryption** | AES-256-GCM / ChaCha20-Poly1305 encryption for sensitive fields (PII, PCI) |
| **Deduplication** | Hash-based event deduplication with configurable TTL |
| **Tombstone Handling** | Proper CDC tombstone emission for compacted topics |
| **Column Masking** | Real-time masking of sensitive data |
| **Event Filtering** | SQL-like filter expressions |

#### Field-Level Encryption

Encrypt sensitive fields at the source using AES-256-GCM or ChaCha20-Poly1305 encryption with versioned key rotation. Keys are managed via environment variables or external key providers.

```yaml
sources:
  postgres_cdc:
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
```

Encrypted fields are transparently decrypted by downstream consumers with the same key configuration. Supports key rotation via versioned encryption keys.

#### Deduplication

Prevent duplicate events caused by network retries, replication lag, or connector restarts:

```yaml
sources:
  postgres_cdc:
    connector: postgres-cdc
    config:
      cdc_features:
        deduplication:
          enabled: true
          ttl_seconds: 3600        # How long to remember seen events
          max_entries: 100000      # Max cache size (LRU eviction)
```

#### Tombstone Handling

Properly emit tombstones (null-value records) for DELETE operations, enabling log compaction:

```yaml
sources:
  postgres_cdc:
    connector: postgres-cdc
    config:
      cdc_features:
        tombstones:
          enabled: true
          emit_tombstone_after_delete: true  # Emit tombstone after delete event
```

## Datagen Source (Testing & Demos)

Generate synthetic data without any external dependencies. Perfect for testing and demos.

### Available Patterns

| Pattern | Description |
|---------|-------------|
| `sequence` | Sequential integers (0, 1, 2, ...) |
| `random` | Random JSON objects |
| `users` | Fake user records with names, emails |
| `orders` | E-commerce order data |
| `events` | Generic event stream (page_view, click, etc.) |
| `metrics` | Time-series metric data |
| `key_value` | Simple key-value pairs |

### Example Configuration

```yaml
version: "1.0"

broker:
  address: localhost:9092

sources:
  demo-data:
    connector: datagen
    topic: demo-events
    config:
      pattern: orders          # Data pattern
      events_per_second: 100   # Rate limit (0 = unlimited)
      max_events: 10000        # Stop after N events (0 = unlimited)
      stream_name: orders      # Stream name in events
      seed: 42                 # For reproducible data
      cdc_mode: true           # Generate INSERT/UPDATE/DELETE cycles
```

### CDC Simulation Mode

When `cdc_mode: true`, the datagen connector simulates realistic CDC event streams:

- **60% INSERT**: New records added to the stream
- **30% UPDATE**: Existing records modified (with before/after values)
- **10% DELETE**: Records removed

This is perfect for testing CDC pipelines without setting up a real database.

### All Configuration Options

```yaml
config:
  # Data pattern: sequence, random, users, orders, events, metrics, key_value
  pattern: orders

  # Events per second (0 = as fast as possible)
  events_per_second: 10

  # Maximum events (0 = unlimited)
  max_events: 0

  # Stream name for events
  stream_name: datagen

  # Namespace (optional)
  namespace: demo

  # Include sequence number in data
  include_sequence: true

  # Include timestamp in data
  include_timestamp: true

  # Add custom fields to every event
  custom_fields:
    env: "test"
    version: 1

  # Seed for reproducibility (optional)
  seed: 12345

  # CDC mode: generate INSERT/UPDATE/DELETE cycles (default: false)
  cdc_mode: false
```

## PostgreSQL CDC Setup

Configure your PostgreSQL database:

```sql
-- Create publication for tables you want to capture
CREATE PUBLICATION rivven_pub FOR TABLE users, orders, products;

-- Or for all tables
CREATE PUBLICATION rivven_pub FOR ALL TABLES;
```

The replication slot is created automatically on first run.

## SQL Server CDC Setup

SQL Server CDC uses the native Change Data Capture feature which tracks changes via CDC tables.

### Enable CDC on Database and Tables

```sql
-- Enable CDC on the database
USE mydb;
GO
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on tables (creates capture instances)
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'users',
  @role_name = NULL,
  @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'orders',
  @role_name = NULL,
  @supports_net_changes = 1;
GO
```

### Example Configuration

```yaml
sources:
  sqlserver_db:
    connector: sqlserver-cdc
    config:
      host: localhost
      port: 1433
      database: mydb
      username: sa
      password: ${MSSQL_PASSWORD}
      schema: dbo
      poll_interval_ms: 500
      snapshot_mode: initial  # initial, always, never, when_needed
      encrypt: false
      trust_server_certificate: true
      include_tables:
        - users
        - orders
    streams:
      - name: users
        namespace: dbo
        sync_mode: incremental
      - name: orders
        namespace: dbo
        sync_mode: incremental
```

### Supported Versions

- SQL Server 2016 SP1+ (on-premises)
- SQL Server 2019 (recommended)
- SQL Server 2022
- Azure SQL Database (with CDC enabled)

## Snowflake Sink Setup

The Snowflake sink uses **Snowpipe Streaming API** with JWT (RSA key-pair) authentication and production-grade resilience features:

- **Exponential backoff retry** with configurable jitter and Retry-After support
- **Circuit breaker pattern** to prevent cascading failures
- **Request tracing** with unique `X-Request-ID` headers for end-to-end debugging
- **Metrics integration** for observability (success/failed/retried counts, latencies)
- **Compression support** for large payloads (gzip)

### Generate RSA Key Pair

```bash
# Generate RSA private key (unencrypted PKCS#8 format)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8

# Extract public key for Snowflake
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Configure Snowflake User

```sql
-- As ACCOUNTADMIN
ALTER USER RIVVEN_USER SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';

-- Grant privileges
GRANT USAGE ON DATABASE ANALYTICS TO ROLE RIVVEN_ROLE;
GRANT INSERT, SELECT ON TABLE ANALYTICS.CDC.EVENTS TO ROLE RIVVEN_ROLE;
```

### Example Configuration

```yaml
sinks:
  snowflake:
    connector: snowflake
    topics: [cdc.orders]             # Required: topics to consume
    consumer_group: snowflake-sink   # Required: for offset tracking
    config:
      account: myorg-account123
      user: RIVVEN_USER
      private_key_path: /secrets/rsa_key.p8
      role: RIVVEN_ROLE
      database: ANALYTICS
      schema: CDC
      table: ORDERS
      warehouse: COMPUTE_WH
      batch_size: 5000
      
      # Compression (recommended for large batches)
      compression_enabled: true
      compression_threshold_bytes: 8192
      
      # Retry configuration (optional)
      retry:
        max_retries: 3
        initial_backoff_ms: 1000
        max_backoff_ms: 30000
        jitter_factor: 0.1          # 10% randomization
      
      # Circuit breaker (optional)
      circuit_breaker:
        enabled: true
        failure_threshold: 5        # Failures before opening circuit
        reset_timeout_secs: 30      # Seconds before testing recovery
```

> **Note**: Private key must be **unencrypted PKCS#8 PEM format** (begins with `-----BEGIN PRIVATE KEY-----`).

### Retry Behavior

Transient failures are automatically retried with exponential backoff and jitter:
- **Retryable**: 408, 429, 500-504, network errors
- **Non-retryable**: 400, 401, 403, 404
- **Jitter**: Adds randomization to prevent thundering herd
- **Retry-After**: Respects Snowflake's `Retry-After` header for 429 responses

### Circuit Breaker

The circuit breaker prevents cascading failures when Snowflake is unavailable:
- **Closed**: Normal operation
- **Open**: After 5 consecutive failures, requests fail fast
- **Half-Open**: After 30 seconds, allows test request to check recovery

### Metrics

The connector exports metrics via the `metrics` crate facade:
- `snowflake.requests.success` - Successful API requests
- `snowflake.requests.failed` - Failed API requests
- `snowflake.requests.retried` - Retried requests
- `snowflake.request.duration_ms` - Request latency histogram
- `snowflake.circuit_breaker.rejected` - Requests rejected by circuit breaker

## Configuration Reference

See [examples/connect-config.yaml](../../examples/connect-config.yaml) for a complete example.

### Sources

```yaml
sources:
  <name>:
    connector: <connector-type>
    topic: <output-topic>            # Required: destination topic
    config:
      # Connector-specific configuration
      topic_routing: <pattern>       # Optional: dynamic topic routing (CDC only)
      schemas:                       # Optional: schemas to include
        - <schema-name>
      tables:                        # Optional: tables to include
        - <table-name>
```

### Topic Routing (CDC Connectors)

Route CDC events to different topics based on source metadata. The `topic_routing` option is configured inside the connector's `config` section since it uses CDC-specific placeholders.

```yaml
sources:
  postgres:
    connector: postgres-cdc
    topic: cdc.default              # Fallback topic
    config:
      # ... postgres config
      topic_routing: "cdc.{schema}.{table}"
```

**Supported Placeholders:**

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{database}` | Database name | `mydb` |
| `{schema}` | Schema name | `public` |
| `{table}` | Table name | `users` |

**Examples:**

- `"cdc.{schema}.{table}"` → `cdc.public.users`
- `"{database}.{schema}.{table}"` → `mydb.public.users`
- `"events.{table}"` → `events.orders`

**Topic Name Normalization:**

Rivven provides comprehensive normalization following industry standards:

| Original | Normalized | Reason |
|----------|------------|--------|
| `my schema` | `my_schema` | Spaces → underscore |
| `user@data` | `user_data` | Special chars → underscore |
| `schema.with.dots` | `schema_with_dots` | Dots replaced |
| `UserProfiles` | `user_profiles` | snake_case conversion |
| `my-table` | `my_table` | Avro-compatible mode |
| `dbo_users` | `users` | Prefix stripping |

**Normalization Options:**

```yaml
sources:
  postgres:
    connector: postgres-cdc
    topic: cdc.default
    config:
      topic_routing: "cdc.{schema}.{table}"
      normalization:
        case_conversion: snake_case    # none, lower, upper, snake_case, kebab-case
        avro_compatible: true          # Stricter naming for Avro
        strip_prefixes: ["dbo_"]       # Remove common prefixes
        strip_suffixes: ["_table"]     # Remove common suffixes
```

Topic names longer than 249 characters are truncated with a hash suffix for uniqueness.

**Programmatic Usage:**

```rust
use rivven_connect::{TopicResolver, TopicMetadata, CaseConversion, NormalizationConfig};

// Simple snake_case conversion
let resolver = TopicResolver::builder("cdc.{schema}.{table}")
    .snake_case()
    .build()
    .unwrap();

// Production configuration with all options
let resolver = TopicResolver::builder("{database}.{schema}.{table}")
    .snake_case()
    .avro_compatible()
    .strip_prefixes(vec!["dbo_", "public_"])
    .build()
    .unwrap();

// Resolve topic name
let metadata = TopicMetadata::new("SalesDB", "dbo_MySchema", "UserProfiles");
let topic = resolver.resolve(&metadata);  // "sales_db.my_schema.user_profiles"

// Custom normalization config
let config = NormalizationConfig::new()
    .with_case_conversion(CaseConversion::SnakeCase)
    .with_avro_compatible()
    .with_strip_prefixes(vec!["dbo_".to_string()]);

let resolver = TopicResolver::builder("cdc.{table}")
    .normalization(config)
    .build()
    .unwrap();
```

### Sinks

```yaml
sinks:
  <name>:
    connector: <connector-type>
    topics: [<topic1>, <topic2>]     # Required: topics to consume
    consumer_group: <group-id>       # Required: for offset tracking
    config:
      # Connector-specific configuration
```

## Security

- Database credentials are isolated to `rivven-connect` - the broker never sees them
- Supports environment variable substitution: `${VAR_NAME}`
- TLS support for sink connections

## Schema Registry

`rivven-connect` provides **3 schema registry modes** for maximum deployment flexibility, with native support for JSON Schema, Apache Avro, and Protocol Buffers.

> **Note**: The broker (rivvend) is schema-agnostic. All schema operations are handled by rivven-schema or external registries.

**Type Architecture**: Schema types are re-exported from `rivven-schema` for consistency:
```rust
// All types come from rivven-schema
use rivven_connect::schema::{
    SchemaId, SchemaType, Subject, SchemaVersion,  // Core identifiers
    Schema, SchemaReference, SubjectVersion,        // Schema metadata
    CompatibilityLevel,                             // Evolution rules
    SchemaRegistryConfig, ExternalConfig, GlueConfig, // Configuration
};
```

| Mode | Description | Use Case |
|------|-------------|----------|
| **External** | Connect to external Schema Registry (incl. rivven-schema) | Production, multi-cluster |
| **External (AWS Glue)** | Connect to AWS Glue | AWS-native deployments |
| **Disabled** | No schema validation | Development, plain JSON |

### Mode 1: External

Connect to an external Schema Registry or any compatible implementation:

```yaml
schema_registry:
  mode: external
  external:
    url: http://schema-registry:8081
    username: ${SCHEMA_REGISTRY_USER}
    password: ${SCHEMA_REGISTRY_PASSWORD}
```

### Mode 2: AWS Glue

Connect to AWS Glue Schema Registry for AWS-native deployments:

```yaml
schema_registry:
  mode: glue
  glue:
    region: us-east-1
    registry_name: my-registry  # optional, defaults to "default-registry"
    cache_ttl_secs: 300
```

**Features:**
- Full AWS Glue Schema Registry API support
- IAM authentication (uses default AWS credential chain)
- Schema versioning and compatibility checking
- Automatic UUID-to-integer ID mapping for standard wire format
- Schema caching for performance

### Mode 3: Disabled (No Registry)

For development or simple use cases, disable schema validation:

```yaml
schema_registry:
  mode: disabled
```

Events are sent as plain JSON without the 5-byte schema ID header.

### Native Avro Support

Full Apache Avro support with standard wire format compatibility:

```rust
use rivven_connect::schema::{AvroSchema, AvroCodec, AvroCompatibility};

let schema = AvroSchema::parse(r#"{
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"}
    ]
}"#)?;

let codec = AvroCodec::new(schema);
let avro_bytes = codec.encode(&json!({"id": 1, "name": "Alice"}))?;
let wire_bytes = codec.encode_wire_format(&json, schema_id)?;
```

### Native Protobuf Support

Full Protocol Buffers support with dynamic schema parsing:

```rust
use rivven_connect::schema::{ProtobufSchema, ProtobufCodec};

let schema = ProtobufSchema::parse(r#"
    syntax = "proto3";
    message User {
        int64 id = 1;
        string name = 2;
    }
"#)?;

let codec = ProtobufCodec::new(schema);
let proto_bytes = codec.encode(&json!({"id": 1, "name": "Alice"}))?;
```

### Features

- **Schema Evolution**: Backward/forward/full compatibility checking
- **Multiple Formats**: JSON Schema, Avro, Protobuf (all ✅ supported)
- **Standard Wire Format**: Compatible with common Schema Registries
- **Schema Fingerprinting**: MD5 and SHA-256 fingerprints for caching
- **Auto-inference**: Automatically infer schemas from data samples
- **Caching**: Built-in schema caching for performance

## Output Formats

The `format` module provides pluggable serialization for storage sinks.

### Architecture

Formats are **how** data is serialized; storage connectors are **where** it is written:

```
┌─────────────────────────────────────────────────────────────────┐
│            Storage Connectors (WHERE to write)                  │
│  S3Sink, GcsSink, AzureBlobSink                                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Format Writers (HOW to serialize)                  │
│  JsonWriter, ParquetWriter, AvroWriter, CsvWriter               │
└─────────────────────────────────────────────────────────────────┘
```

### Available Formats

| Format | Extension | Best For | Append |
|--------|-----------|----------|--------|
| `jsonl` | `.jsonl` | Streaming, logs, real-time | ✅ |
| `json` | `.json` | Human-readable, debugging | ❌ |
| `parquet` | `.parquet` | Analytics, data lakes | ❌ |
| `avro` | `.avro` | Schema Registry integration | ❌ |
| `csv` | `.csv` | Spreadsheets, exports | ✅ |

### Using Format Writers

```rust
use rivven_connect::format::{
    FormatWriter, 
    ParquetWriter, ParquetWriterConfig,
    AvroWriter, AvroWriterConfig,
    CsvWriter, CsvWriterConfig,
};

// Create a Parquet writer
let writer = ParquetWriter::new(ParquetWriterConfig {
    compression: ParquetCompression::Zstd,
    row_group_size: 10000,
    ..Default::default()
});

// Create an Avro writer with schema inference
let avro_writer = AvroWriter::new(AvroWriterConfig::default())?;

// Create a CSV writer
let csv_writer = CsvWriter::new(CsvWriterConfig {
    delimiter: CsvDelimiter::Comma,
    include_header: true,
    ..Default::default()
});

// Use via FormatWriter trait
println!("Format: {}", writer.name());       // "parquet"
println!("Extension: {}", writer.extension()); // ".parquet"

let bytes = writer.write_batch(&events)?;
```

### Configuration

```yaml
sinks:
  s3:
    connector: s3
    config:
      bucket: my-bucket
      format: parquet  # Options: json, jsonl, parquet, avro, csv
      parquet:
        compression: zstd
        row_group_size: 10000
```

### Output Compression

Text-based formats (JSON, JSONL, CSV) support output file compression:

```yaml
sinks:
  s3:
    connector: s3
    config:
      bucket: my-bucket
      format: jsonl
      compression: gzip  # Options: none, gzip, zstd, snappy, lz4
```

| Algorithm | Extension | Speed | Use Case |
|-----------|-----------|-------|----------|
| `none` | - | N/A | No compression |
| `gzip` | `.gz` | Medium | Universal compatibility |
| `zstd` | `.zst` | Fast | Best balance |
| `snappy` | `.snappy` | Very Fast | Interoperability |
| `lz4` | `.lz4` | Fastest | Real-time streaming |

**Note**: Parquet and Avro have built-in compression - use their internal compression settings instead of output compression.

## Distributed Mode

Run `rivven-connect` across multiple nodes for scalability and high availability:

```yaml
distributed:
  enabled: true
  node_id: node-1
  cluster_topic: _connect_status
  
  # Singleton connectors (like CDC) get automatic failover
  failover:
    heartbeat_interval_ms: 1000
    failure_timeout_ms: 10000
```

### Connector Modes

| Mode | Description | Example |
|------|-------------|---------|
| `singleton` | One instance with failover | CDC connectors |
| `scalable` | Multiple instances, load balanced | HTTP sources |
| `partitioned` | One instance per partition | Topic consumers |

### Singleton Enforcement (CDC)

CDC connectors **cannot run in parallel** because:
- Database replication slots allow only one active consumer
- Multiple instances would cause data duplication

When a singleton leader fails, the coordinator automatically promotes a standby:

```
Node-1 (Leader)  ──fails──►  Node-2 (Standby) becomes Leader
                              Node-3 (Standby) watches
```

## Health Monitoring

Monitor connector health with automatic failure detection and auto-recovery:

```yaml
sources:
  postgres_db:
    connector: postgres-cdc
    config:
      # ... database config ...
      
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

### Health States

| State | Description |
|:------|:------------|
| `Healthy` | All checks passing, lag within threshold |
| `Degraded` | Some checks passing with warnings |
| `Unhealthy` | Failure threshold exceeded |
| `Recovering` | Auto-recovery with exponential backoff |

### Kubernetes Health Probes

The `CdcFeatureProcessor` provides K8s-compatible health endpoints:

```rust
use rivven_connect::connectors::CdcFeatureProcessor;

// Liveness probe (is the process alive?)
let (status, message) = processor.liveness_probe().await;  // (200, "OK") or (503, "Service Unavailable")

// Readiness probe (can we serve traffic?)
let (status, message) = processor.readiness_probe().await;

// Detailed health JSON
let json = processor.health_json().await;
// {"status":"healthy","ready":true,"lag_ms":100,"uptime_secs":3600,...}
```

### Programmatic Health Monitoring

```rust
use rivven_connect::connectors::{CdcFeatureConfig, CdcFeatureProcessor, HealthMonitorConfig};

let config = CdcFeatureConfig::builder()
    .health(HealthMonitorConfig {
        enabled: true,
        check_interval_secs: 5,
        max_lag_ms: 10_000,
        auto_recovery: true,
        ..Default::default()
    })
    .build();

let processor = CdcFeatureProcessor::new(config);

// Register custom health checks
processor.register_health_check("database", || async {
    // Check database connection
    rivven_cdc::common::HealthCheckResult::Healthy
}).await;

// Monitor health
processor.mark_connected();
let is_healthy = processor.is_healthy();

// Auto-recovery with custom function
let recovered = processor.attempt_recovery(|| async {
    // Attempt reconnection
    true // or false if failed
}).await;

// Unregister health check when no longer needed
processor.unregister_health_check("database").await;
```

### Health Monitoring Prometheus Metrics

The health monitor emits comprehensive Prometheus metrics for observability:

| Metric | Type | Description |
|--------|------|-------------|
| `rivven_cdc_health_monitoring_enabled` | Gauge | Health monitoring enabled status |
| `rivven_cdc_health_state_healthy` | Gauge | Current health state (1/0) |
| `rivven_cdc_health_state_ready` | Gauge | Current readiness state (1/0) |
| `rivven_cdc_health_checks_passed_total` | Counter | Passed health checks |
| `rivven_cdc_health_checks_failed_total` | Counter | Failed health checks |
| `rivven_cdc_health_state_transitions_total` | Counter | State transitions |
| `rivven_cdc_health_recoveries_succeeded_total` | Counter | Successful recoveries |
| `rivven_cdc_health_recoveries_failed_total` | Counter | Failed recoveries |
| `rivven_cdc_health_unhealthy_time_ms_total` | Counter | Time spent unhealthy |

## Legacy Mode

For simple PostgreSQL CDC without a config file:

```bash
rivven-connect postgres \
  --connection "postgres://user:pass@localhost:5432/mydb" \
  --slot rivven_slot \
  --publication rivven_pub \
  --topic cdc.mydb \
  --broker localhost:9092
```

## Development

### Building Custom Connectors

Use the derive macros to reduce boilerplate when building custom connectors:

```rust
use rivven_connect::prelude::*;
use rivven_connect::SourceConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SourceConfigDerive)]
#[source(
    name = "my-source",
    version = "1.0.0",
    description = "My custom source connector",
    author = "Your Name",
    license = "Apache-2.0",
    incremental  // Supports incremental sync
)]
pub struct MySourceConfig {
    pub endpoint: String,
}

// Generates: MySourceConfigSpec with spec(), name(), version() methods
// The spec() includes automatic JSON Schema generation
```

For sinks with batching support:

```rust
use rivven_connect::prelude::*;
use rivven_connect::SinkConfigDerive;

#[derive(Debug, Deserialize, Validate, JsonSchema, SinkConfigDerive)]
#[sink(
    name = "my-sink",
    version = "1.0.0",
    description = "My custom sink connector",
    author = "Your Name",
    license = "Apache-2.0",
    batching,
    batch_size = 1000
)]
pub struct MySinkConfig {
    pub destination: String,
}

// Generates: MySinkConfigSpec with spec(), name(), version(), batch_config() methods
```

See [Connector Development Guide](https://rivven.dev/docs/connector-development) for complete documentation.

### Build Commands

```bash
# Build
cargo build -p rivven-connect

# Run with verbose logging
cargo run -p rivven-connect -- -v run --config examples/connect-config.yaml

# Run unit tests
cargo test -p rivven-connect

# Run integration tests (requires Docker)
cargo test -p rivven-integration-tests --test kafka_connector
cargo test -p rivven-integration-tests --test mqtt_connector
```

## Documentation

- [Connectors](https://rivven.hupe1980.github.io/rivven/docs/connectors)
- [Kafka Connector](https://rivven.hupe1980.github.io/rivven/docs/kafka-connector)
- [MQTT Connector](https://rivven.hupe1980.github.io/rivven/docs/mqtt-connector)
- [Connector Development](https://rivven.hupe1980.github.io/rivven/docs/connector-development)
- [CDC Guide](https://rivven.hupe1980.github.io/rivven/docs/cdc)

## License

Apache-2.0. See [LICENSE](../../LICENSE).

