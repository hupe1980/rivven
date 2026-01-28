# rivven-connect

Config-driven connector runtime for Rivven - a unified binary for data ingestion and distribution.

## Overview

`rivven-connect` provides a single binary that handles all data movement operations:

- **Sources**: Extract data from external systems (PostgreSQL CDC, MySQL, APIs)
- **Sinks**: Load data into destinations (Rivven broker, S3, HTTP APIs)
- **Transforms**: Process events in-flight (filter, mask PII, enrich)

## Architecture

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

| Connector | Description | Features |
|-----------|-------------|----------|
| `datagen` | Synthetic data generator | Testing, demos, no external deps |
| `postgres-cdc` | PostgreSQL logical replication | CDC, incremental sync |

### Sinks

| Connector | Description | Features |
|-----------|-------------|----------|
| `rivven` | Rivven broker | High throughput |
| `stdout` | Console output | Debugging |

### Transforms

| Transform | Description |
|-----------|-------------|
| `filter` | Filter events by conditions |
| `field-mask` | Mask PII fields |

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

`rivven-connect` includes a comprehensive schema registry with native support for JSON Schema, Apache Avro, and Protocol Buffers.

### Embedded Mode (Default)

Schemas are stored in Rivven topics (`_schemas`), requiring no external infrastructure:

```yaml
schema_registry:
  mode: embedded
  compatibility: backward  # backward, forward, full, none
```

### External Mode (Confluent-compatible)

Connect to external schema registries:

```yaml
schema_registry:
  mode: external
  url: http://localhost:8081
  auth:
    type: basic
    username: ${SCHEMA_REGISTRY_USER}
    password: ${SCHEMA_REGISTRY_PASSWORD}
```

### Native Avro Support

Full Apache Avro support with Confluent wire format compatibility:

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
let confluent_bytes = codec.encode_confluent(&json, schema_id)?;
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
- **Confluent Wire Format**: Compatible with Confluent Schema Registry
- **Schema Fingerprinting**: MD5 and SHA-256 fingerprints for caching
- **Auto-inference**: Automatically infer schemas from data samples
- **Caching**: Built-in schema caching for performance

## WASM Plugins

Load custom connectors at runtime via WebAssembly:

```yaml
plugins:
  my-custom-source:
    path: /path/to/my-connector.wasm
    config:
      api_key: ${MY_API_KEY}
```

### Plugin Development

Implement the Rivven Connect ABI:

```rust
// Plugin manifest
#[no_mangle]
pub extern "C" fn rivven_get_manifest() -> i64 { ... }

// Initialize plugin
#[no_mangle]
pub extern "C" fn rivven_init(config_ptr: u32, config_len: u32) -> i32 { ... }

// Poll for records (sources)
#[no_mangle]
pub extern "C" fn rivven_poll(buf_ptr: u32, buf_len: u32) -> i64 { ... }
```

### Sandbox Security

Plugins run with configurable resource limits:

- Memory limits
- Execution time limits
- Restricted filesystem access
- Network capability controls

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

```bash
# Build
cargo build -p rivven-connect

# Run with verbose logging
cargo run -p rivven-connect -- -v run --config examples/connect-config.yaml

# Run tests
cargo test -p rivven-connect
```

## License

See root [LICENSE](../../LICENSE) file.
