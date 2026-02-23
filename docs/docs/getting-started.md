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
- **Authentication**: required (use `--no-require-auth` for local development)

{: .note }
> Authentication is enabled by default. For quick local development without credentials, start with `rivvend --no-require-auth`.

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
rivven-client = "0.0.20"
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

### High-Performance Producer

```rust
use rivven_client::{Producer, ProducerConfig, CompressionType};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create producer with batching, compression, and auth
    let config = ProducerConfig::builder()
        .bootstrap_servers(vec!["localhost:9092".to_string()])
        .batch_size(16384)           // 16 KB batches
        .linger_ms(5)                // 5ms linger for batching
        .compression_type(CompressionType::Lz4)  // LZ4 batch compression
        .auth("producer-app", "secure-password")  // SCRAM-SHA-256 auth
        .metadata_max_age(std::time::Duration::from_secs(300)) // 5 min metadata cache
        .max_in_flight_requests(5)   // Memory-bounded backpressure
        .build();
    
    // Producer::new() connects with auto-handshake and auto-authentication
    let producer = Arc::new(Producer::new(config).await?);
    
    // Simple send (uses murmur2 partitioning like Kafka)
    let metadata = producer.send("events", b"value").await?;
    println!("Published at partition {} offset {}", metadata.partition, metadata.offset);
    
    // Send with key for consistent partitioning
    let metadata = producer.send_with_key("events", Some("user-123"), b"event").await?;
    
    // Share across tasks for parallel publishing
    for i in 0..100 {
        let producer = Arc::clone(&producer);
        tokio::spawn(async move {
            producer.send("events", format!("msg-{}", i)).await
        });
    }
    
    // Flush ensures all pending records are delivered
    producer.flush().await?;
    
    // Check statistics
    let stats = producer.stats();
    println!("Published {} records, {} delivered", stats.records_sent, stats.records_delivered);
    
    producer.close().await;
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

High-performance Python bindings with full async/await support.

### Installation

```bash
pip install rivven
```

### Basic Usage

```python
import asyncio
import rivven

async def main():
    # Connect to Rivven
    client = await rivven.connect("localhost:9092")
    
    # Create a topic
    await client.create_topic("events", partitions=3)
    
    # Produce messages
    producer = client.producer("events")
    offset = await producer.send(b"Hello from Python!", key=b"key-1")
    print(f"Published at offset: {offset}")
    
    # Consume messages
    consumer = client.consumer("events", group_id="my-group")
    messages = await consumer.fetch(max_messages=10)
    for msg in messages:
        print(f"Offset {msg.offset}: {msg.value_str()}")
    await consumer.commit()

asyncio.run(main())
```

### Async Iterator Pattern

```python
import asyncio
import rivven

async def stream():
    client = await rivven.connect("localhost:9092")
    consumer = client.consumer("events", group_id="my-group")
    
    async for message in consumer:
        print(f"Received: {message.value_str()}")
        await consumer.commit()

asyncio.run(stream())
```

### Authentication

```python
import asyncio
import rivven

async def authenticated():
    client = await rivven.connect("localhost:9092")
    
    # Simple authentication
    await client.authenticate("username", "password")
    
    # Or SCRAM-SHA-256
    await client.authenticate_scram("username", "password")
    
    topics = await client.list_topics()

asyncio.run(authenticated())
```

### TLS Connection

```python
import asyncio
import rivven

async def secure():
    client = await rivven.connect_tls(
        "localhost:9093",
        ca_cert="/path/to/ca.crt",
        client_cert="/path/to/client.crt",  # Optional: mTLS
        client_key="/path/to/client.key",   # Optional: mTLS
    )
    
    topics = await client.list_topics()

asyncio.run(secure())
```

### Transactions (Exactly-Once Semantics)

```python
import asyncio
import rivven

async def transactional():
    client = await rivven.connect("localhost:9092")
    
    # Initialize transactional producer
    producer_id, epoch = await client.init_producer_id("my-txn-id")
    
    try:
        await client.begin_transaction("my-txn-id", producer_id, epoch)
        
        await client.publish_idempotent(
            topic="events",
            value=b"message",
            producer_id=producer_id,
            epoch=epoch,
            sequence=0,
            key=b"key"
        )
        
        await client.commit_transaction("my-txn-id", producer_id, epoch)
    except Exception:
        await client.abort_transaction("my-txn-id", producer_id, epoch)
        raise

asyncio.run(transactional())
```

### Admin Operations

```python
import asyncio
import rivven

async def admin():
    client = await rivven.connect("localhost:9092")
    
    # Topic management
    await client.create_topic("my-topic", partitions=3)
    topics = await client.list_topics()
    
    # Topic configuration
    configs = await client.describe_topic_configs("my-topic")
    await client.alter_topic_config("my-topic", "retention.ms", "86400000")
    
    # Partition management
    await client.create_partitions("my-topic", new_total=6)
    
    # Offset management
    offset = await client.get_offset_for_timestamp("my-topic", 0, 1699900000000)
    await client.delete_records("my-topic", 0, before_offset=100)
    
    # Consumer groups
    groups = await client.list_groups()
    await client.describe_group("my-group")
    await client.commit_offset("my-group", "my-topic", 0, 100)
    committed = await client.get_offset("my-group", "my-topic", 0)

asyncio.run(admin())
```

For complete API reference, see the [Python SDK README](https://github.com/hupe1980/rivven/tree/main/python).

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
