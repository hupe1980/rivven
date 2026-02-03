---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started
{: .no_toc }

Get Rivven up and running in minutes.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Installation

### From Crates.io

```bash
cargo install rivven rivvend rivven-connect rivven-schema
```

### From Source

```bash
git clone https://github.com/hupe1980/rivven
cd rivven
cargo build --release

# Binaries are in target/release/
./target/release/rivvend --help
./target/release/rivven --help
./target/release/rivven-connect --help
./target/release/rivven-schema --help
```

### Docker

```bash
# Pull all images
docker pull ghcr.io/hupe1980/rivvend:latest
docker pull ghcr.io/hupe1980/rivven-connect:latest
docker pull ghcr.io/hupe1980/rivven-schema:latest

# Start broker
docker run -d -p 9092:9092 -p 9094:9094 ghcr.io/hupe1980/rivvend:latest --dashboard

# Start schema registry (optional)
docker run -d -p 8081:8081 ghcr.io/hupe1980/rivven-schema:latest serve --port 8081
```

---

## Starting the Broker

### Basic Startup

```bash
rivvend
```

The broker starts with sensible defaults:
- **Listen address**: `0.0.0.0:9092`
- **Data directory**: `./data`
- **Max message size**: 10 MB

### Custom Configuration

```bash
rivvend \
  --bind 0.0.0.0:9092 \
  --data-dir /var/lib/rivven \
  --max-message-size 16777216
```

### With Web Dashboard

The dashboard is embedded in the binary when built with the `dashboard` feature:

```bash
# Start server with dashboard enabled
rivvend --data-dir ./data

# Dashboard available at http://localhost:8080/
```

**Note**: The dashboard is embedded during the build. See [Dashboard](dashboard.md) for build instructions.

---

## Rivven Connect

Rivven Connect manages data pipelines with sources (data ingestion) and sinks (data export).

### Quick Start

```bash
# Start broker
rivvend --data-dir ./data

# Run connectors (topics auto-created!)
rivven-connect run --config connect.yaml
```

### Example Configuration

The configuration defines **sources** (publish to broker) and **sinks** (consume from broker):

```yaml
# Architecture: Sources → Broker Topics → Sinks
# The broker is ALWAYS in the middle for durability and replay

version: "1.0"

broker:
  address: localhost:9092

# Sources: read from external systems, publish to broker topics
sources:
  demo:
    connector: datagen
    topic: demo-events
    config:
      pattern: orders
      events_per_second: 3
      cdc_mode: true

# Sinks: consume from broker topics, write to external systems
sinks:
  console:
    connector: stdout
    topics: [demo-events]
    consumer_group: demo-sink
    config:
      format: pretty
```

### Validate Configuration

```bash
rivven-connect validate --config connect.yaml
```

Output:
```
✓ Configuration valid!

Broker:
  Bootstrap servers:
    - 127.0.0.1:9092

Topic Settings:
  Auto-create: enabled
  Default partitions: 3

Sources (1 enabled):
  ✓ demo (datagen) → topic: demo-events (3 partitions)

Sinks (1 enabled):
  ✓ console (stdout) ← topics: ["demo-events"]
```

---

## Basic Operations

### Topic Management

```bash
# Create a topic
rivven topic create events

# Create with partitions
rivven topic create orders --partitions 3

# List topics
rivven topic list

# Delete a topic
rivven topic delete events
```

### Publishing Messages

```bash
# Simple message
rivven produce events "Hello, World!"

# From stdin
echo '{"user": "alice", "action": "login"}' | rivven produce events

# Multiple messages
cat events.jsonl | rivven produce events
```

### Consuming Messages

```bash
# Consume from beginning
rivven consume events

# Consume from a specific offset
rivven consume events --offset 100

# Consume with consumer group
rivven consume events --group my-app
```

### Consumer Groups

```bash
# List consumer groups
rivven group list

# Describe a group
rivven group describe my-app

# Delete a group
rivven group delete my-app
```

---

## Rust Client

### Add Dependency

```toml
[dependencies]
rivven-client = "0.0.5"
tokio = { version = "1", features = ["full"] }
```

### Producer Example

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Create topic
    client.create_topic("events", Some(3)).await?;
    
    // Publish message
    let offset = client.publish("events", b"Hello, Rivven!").await?;
    println!("Published at offset: {}", offset);
    
    Ok(())
}
```

### Consumer Example

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Consume messages
    let messages = client.consume("events", 0, 0, 100).await?;
    
    for msg in messages {
        println!("Offset {}: {:?}", msg.offset, msg.value);
    }
    
    Ok(())
}
```

---

## Python Client

### Installation

```bash
pip install rivven
```

### Usage

```python
from rivven import Client

# Connect
client = Client("localhost:9092")

# Publish
offset = client.publish("events", b"Hello from Python!")
print(f"Published at offset: {offset}")

# Consume
messages = client.consume("events", partition=0, offset=0, max_messages=10)
for msg in messages:
    print(f"Offset {msg.offset}: {msg.value}")
```

---

## Schema Registry

Rivven includes a high-performance Schema Registry for schema management.

### Start the Registry

```bash
# Start schema registry (in-memory storage for development)
rivven-schema serve --port 8081

# With broker-backed storage (production)
rivven-schema serve --port 8081 --broker localhost:9092
```

### Register and Query Schemas

```bash
# Register a schema
rivven-schema register --url http://localhost:8081 --subject user-value \
  --schema '{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}'

# List subjects
rivven-schema subjects --url http://localhost:8081

# Get schema by ID
rivven-schema get --url http://localhost:8081 --id 1
```

### Programmatic Usage

```rust
use rivven_schema::{SchemaRegistry, RegistryConfig, SchemaType};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let registry = SchemaRegistry::new(RegistryConfig::memory()).await?;
    
    let schema_id = registry.register(
        "user-value",
        SchemaType::Avro,
        r#"{"type":"record","name":"User","fields":[...]}"#
    ).await?;
    
    println!("Registered schema ID: {}", schema_id.0);
    Ok(())
}
```

For more details, see the [Schema Registry](schema-registry) guide.

---

## Next Steps

- [Architecture](architecture) — Understand system design
- [Schema Registry](schema-registry) — Avro, Protobuf, and JSON Schema
- [CDC Guide](cdc) — Set up Change Data Capture
- [Connectors](connectors) — Configure sources and sinks
- [Security](security) — Enable TLS and authentication
