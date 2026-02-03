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
- **Rich Metadata**: Tags, support levels, and search capabilities
- **Connector Inventory**: Auto-registration with metadata indexing for fast discovery
- **Feature Gating**: Compile only the connectors you need

```
┌─────────────────────────────────────────────────────────────────┐
│                 Connector Inventory (Scalable)                   │
├─────────────────────────────────────────────────────────────────┤
│  Categories                                                      │
│  ├── Database (postgres_cdc, mysql_cdc, mongodb, redis, ...)    │
│  ├── Messaging (mqtt, sqs, pubsub, nats, ...)                     │
│  ├── Storage (s3, gcs, azure, minio, ...)                       │
│  ├── Warehouse (snowflake, bigquery, redshift, ...)             │
│  ├── AI/ML (openai, anthropic, pinecone, ...)                   │
│  └── Utility (datagen, stdout, http, ...)                       │
└─────────────────────────────────────────────────────────────────┘
```

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

sources:
  postgres_db:
    connector: postgres-cdc
    config:
      host: localhost
      port: 5432
      database: mydb
      user: postgres
      password: ${POSTGRES_PASSWORD}
      slot_name: rivven_slot
      publication: rivven_pub
    streams:
      - name: users
        namespace: public
        sync_mode: incremental

sinks:
  broker:
    connector: rivven
    config:
      address: localhost:9092

pipelines:
  cdc_to_broker:
    source: postgres_db
    sink: broker
    enabled: true
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
| `external-queue` | `external-queue` | External message queue consumer |
| `mqtt` | `mqtt` | MQTT broker subscriber |
| `sqs` | `sqs` | AWS SQS queue consumer |
| `pubsub` | `pubsub` | Google Cloud Pub/Sub subscriber |

### Sinks

| Connector | Feature | Description |
|-----------|---------|-------------|
| `stdout` | (default) | Console output for debugging |
| `http-webhook` | `http` | HTTP/HTTPS POST webhooks |
| `external-queue` | `external-queue` | External message queue producer |
| `object-storage` | `cloud-storage` | Unified object storage (S3, GCS, Azure, local) |
| `s3` | `s3` | Amazon S3 / MinIO / R2 (via object-storage) |
| `gcs` | `gcs` | Google Cloud Storage (via object-storage) |
| `azure-blob` | `azure` | Azure Blob Storage (via object-storage) |
| `iceberg` | `iceberg` | Apache Iceberg lakehouse format |
| `snowflake` | `snowflake` | Snowflake Data Cloud |
| `bigquery` | `bigquery` | Google BigQuery |
| `redshift` | `redshift` | Amazon Redshift |

### Unified Object Storage

All object storage connectors (S3, GCS, Azure Blob) now use a single unified implementation
powered by the `object_store` crate. Configure with the `provider` field:

```yaml
sinks:
  events:
    connector: object-storage
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

### Feature Bundles

```toml
# In Cargo.toml
rivven-connect = { version = "0.0.5", features = ["full"] }

# Or selective features
rivven-connect = { version = "0.0.5", features = ["postgres", "s3"] }
```

| Bundle | Includes |
|--------|----------|
| `queue-full` | mqtt, sqs, pubsub |
| `storage-full` | s3, gcs, azure, parquet |
| `lakehouse-full` | iceberg |
| `warehouse-full` | snowflake, bigquery, redshift |
| `full` | All connectors |

### Single Message Transforms (SMT)

CDC connectors support 10 built-in transforms applied via YAML configuration.
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
| `filter` | Filter events based on conditions |
| `cast` | Convert field types |
| `flatten` | Flatten nested JSON structures |

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
| **Field-Level Encryption** | AES-256-GCM encryption for sensitive fields (PII, PCI) |
| **Deduplication** | Hash-based event deduplication with configurable TTL |
| **Tombstone Handling** | Proper CDC tombstone emission for compacted topics |
| **Column Masking** | Real-time masking of sensitive data |
| **Event Filtering** | SQL-like filter expressions |

#### Field-Level Encryption

Encrypt sensitive fields at the source using AES-256-GCM encryption. Keys are managed via environment variables or external key providers.

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

## Configuration Reference

See [examples/connect-config.yaml](../../examples/connect-config.yaml) for a complete example.

### Sources

```yaml
sources:
  <name>:
    connector: <connector-type>
    config:
      # Connector-specific configuration
    streams:
      - name: <stream-name>
        namespace: <optional-namespace>
        sync_mode: full_refresh | incremental
        cursor_field: <optional-field>
```

### Sinks

```yaml
sinks:
  <name>:
    connector: <connector-type>
    config:
      # Connector-specific configuration
    batch:
      max_records: 10000
      timeout_ms: 5000
```

### Pipelines

```yaml
pipelines:
  <name>:
    source: <source-name>
    sink: <sink-name>
    transforms:
      - <transform-name>
    enabled: true
    settings:
      rate_limit: 10000  # events/sec, 0 = unlimited
      on_error: skip | stop | dead_letter
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

# Run tests
cargo test -p rivven-connect
```

## Documentation

- [Connectors](https://rivven.hupe1980.github.io/rivven/docs/connectors)
- [Connector Development](https://rivven.hupe1980.github.io/rivven/docs/connector-development)
- [CDC Guide](https://rivven.hupe1980.github.io/rivven/docs/cdc)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
