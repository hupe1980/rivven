# rivven-client

> Native Rust client library for the Rivven event streaming platform.

## Overview

`rivven-client` is a production-grade async client with connection pooling, automatic failover, circuit breakers, and exactly-once semantics.

## Features

| Category | Features |
|:---------|:---------|
| **Connectivity** | Connection pooling, request pipelining, automatic failover |
| **Resilience** | Circuit breaker, exponential backoff, health monitoring |
| **Security** | TLS/mTLS, SCRAM-SHA-256 authentication |
| **Semantics** | Transactions, idempotent producer, exactly-once delivery |

## Installation

```toml
[dependencies]
rivven-client = "0.2"
# With TLS support
rivven-client = { version = "0.2", features = ["tls"] }
```

## Usage

### Basic Client

For simple use cases, use the basic `Client`:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Publish a message
    client.publish("my-topic", b"value").await?;
    
    // Consume messages
    let messages = client.consume("my-topic", 0, 0, 100).await?;
    
    Ok(())
}
```

### Authentication

Rivven supports multiple authentication methods:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Simple authentication (use with TLS in production)
    let session = client.authenticate("alice", "password123").await?;
    println!("Session ID: {}", session.session_id);
    
    // SCRAM-SHA-256 authentication (recommended)
    // Password never sent over the wire, mutual authentication
    let session = client.authenticate_scram("alice", "password123").await?;
    println!("Authenticated! Expires in {}s", session.expires_in);
    
    // Now use the authenticated session for operations
    client.publish("my-topic", b"secure message").await?;
    
    Ok(())
}
```

### Production-Grade Resilient Client

For production deployments, use `ResilientClient` which provides:
- **Connection pooling** across multiple servers
- **Automatic retry** with exponential backoff and jitter
- **Circuit breaker** pattern for fault isolation
- **Real-time health monitoring**

```rust
use rivven_client::{ResilientClient, ResilientClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the resilient client
    let config = ResilientClientConfig::builder()
        .servers(vec![
            "node1:9092".to_string(),
            "node2:9092".to_string(),
            "node3:9092".to_string(),
        ])
        .pool_size_per_server(5)
        .max_retries(3)
        .retry_initial_delay(Duration::from_millis(100))
        .retry_max_delay(Duration::from_secs(5))
        .circuit_breaker_failure_threshold(5)
        .circuit_breaker_recovery_timeout(Duration::from_secs(30))
        .build();

    // Create the resilient client
    let client = ResilientClient::new(config);
    
    // All operations automatically use connection pooling, 
    // retries, and circuit breakers
    client.publish("my-topic", Some(b"key"), b"value").await?;
    
    // Check client health
    let stats = client.stats().await;
    println!("Active connections: {}", stats.active_connections);
    println!("Healthy servers: {}", stats.healthy_servers);
    
    Ok(())
}
```

### Circuit Breaker Behavior

The circuit breaker protects against cascading failures:

1. **Closed (Normal)**: Requests flow normally. Failures are counted.
2. **Open (Failing)**: After threshold failures, the circuit opens. All requests fail fast without attempting connection.
3. **Half-Open (Recovery)**: After recovery timeout, one request is allowed through. If successful, circuit closes; if failed, circuit reopens.

```rust
// Circuit breaker configuration
let config = ResilientClientConfig::builder()
    .servers(vec!["localhost:9092".to_string()])
    .circuit_breaker_failure_threshold(5)     // Open after 5 failures
    .circuit_breaker_recovery_timeout(Duration::from_secs(30))  // Try recovery after 30s
    .build();
```

### Retry with Exponential Backoff

Failed operations are automatically retried with exponential backoff and jitter:

```rust
let config = ResilientClientConfig::builder()
    .servers(vec!["localhost:9092".to_string()])
    .max_retries(3)                              // Retry up to 3 times
    .retry_initial_delay(Duration::from_millis(100))  // Start with 100ms delay
    .retry_max_delay(Duration::from_secs(5))     // Cap at 5 seconds
    .retry_multiplier(2.0)                       // Double delay each retry
    .build();
```

### High-Throughput Pipelined Client

For maximum throughput, use `PipelinedClient` which allows multiple in-flight requests over a single connection. Supports optional **TLS** and **authentication**:

```rust
use rivven_client::{PipelinedClient, PipelineConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // High-throughput configuration
    let config = PipelineConfig::high_throughput();
    let client = PipelinedClient::connect("localhost:9092", config).await?;
    
    // Send 1000 requests concurrently - all pipelined over single connection
    let handles: Vec<_> = (0..1000)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                client.publish("topic", format!("msg-{}", i)).await
            })
        })
        .collect();
    
    for handle in handles {
        handle.await??;
    }
    
    // Check pipeline statistics
    let stats = client.stats();
    println!("Requests sent: {}", stats.requests_sent);
    println!("Responses received: {}", stats.responses_received);
    println!("Success rate: {:.1}%", stats.success_rate() * 100.0);
    
    Ok(())
}
```

### Pipeline Configuration

| Config | Default | High-Throughput | Low-Latency |
|--------|---------|-----------------|-------------|
| `max_in_flight` | 100 | 1000 | 32 |
| `batch_linger_us` | 1000 | 5000 | 0 |
| `max_batch_size` | 64 | 256 | 1 |
| `request_timeout` | 30s | 60s | 10s |

### High-Performance Producer

For maximum throughput with all best practices, use `Producer`:

```rust
use rivven_client::{Producer, ProducerConfig, CompressionType};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure with Kafka-like semantics
    let config = ProducerConfig::builder()
        .bootstrap_servers(vec!["localhost:9092".to_string()])
        .batch_size(16384)          // Batch up to 16KB
        .linger_ms(5)               // Wait 5ms for batch
        .buffer_memory(32 * 1024 * 1024)  // 32MB buffer
        .compression_type(CompressionType::Lz4)  // LZ4 batch compression
        .enable_idempotence(true)   // Exactly-once semantics
        .auth("producer-app", "password")  // SCRAM-SHA-256 auth
        .build();

    // Producer::new() connects with auto-handshake and authentication
    let producer = Arc::new(Producer::new(config).await?);

    // Share across tasks (sticky partitioning for keyless messages)
    for i in 0..1000 {
        let producer = Arc::clone(&producer);
        tokio::spawn(async move {
            producer.send("topic", format!("msg-{}", i)).await
        });
    }

    // With key (consistent partition assignment)
    producer.send_with_key("topic", Some("user-123"), "event").await?;

    // Flush ensures all pending records are delivered
    producer.flush().await?;

    // Check producer statistics
    let stats = producer.stats();
    println!("Records sent: {}", stats.records_sent);
    println!("Success rate: {:.1}%", stats.success_rate() * 100.0);
    
    Ok(())
}
```

#### Producer Features

| Feature | Description |
|---------|-------------|
| **Authentication** | SCRAM-SHA-256 auto-auth via `ProducerAuthConfig` |
| **Auto-Handshake** | Protocol version negotiated on connect |
| **Compression** | LZ4/Snappy/Zstd batch compression (feature-gated) |
| **Idempotency** | Sequence tracking + `IdempotentPublish` wire type; `is_idempotent()` detects silent degradation |
| **Metadata Cache** | TTL-based caching with persistent metadata client (avoids per-topic connection churn) |
| **Sticky Partitioning** | Batches keyless messages to same partition |
| **Backpressure** | Memory-bounded buffers prevent OOM; applies to standard, idempotent, and transactional publish paths |
| **Murmur2 Hashing** | Kafka-compatible key partitioning (optimized) |
| **Batched I/O** | Single flush per batch minimizes syscalls |
| **Pipelined Responses** | Write-all, then read-all for throughput |
| **Multi-Server Failover** | Tries all bootstrap servers on connect |
| **Flush Safety** | `pending_records` correctly decremented on batch failure; `flush()` always terminates |
| **Completion Tracking** | `flush()` waits for all pending records |
| **Metadata Refresh** | `refresh_metadata()` fetches partition info |

#### Producer Configuration

| Config | Default | High-Throughput | Low-Latency | Exactly-Once |
|--------|---------|-----------------|-------------|--------------|
| `batch_size` | 16KB | 64KB | 1 | 16KB |
| `linger_ms` | 0 | 10 | 0 | 0 |
| `compression_type` | None | Lz4 | None | None |
| `max_in_flight_requests` | 5 | 10 | 1 | 5 |
| `enable_idempotence` | false | false | false | true |
| `acks` | 1 | 1 | 1 | -1 (all) |
| `auth` | None | — | — | — |

### Health Monitoring

Monitor client and server health in real-time:

```rust
let stats = client.stats().await;

println!("Client Statistics:");
println!("  Total servers: {}", stats.total_servers);
println!("  Healthy servers: {}", stats.healthy_servers);
println!("  Active connections: {}", stats.active_connections);
println!("  Available connections: {}", stats.available_connections);

for server in &stats.servers {
    println!("\n  Server: {}", server.address);
    println!("    Circuit state: {:?}", server.circuit_state);
    println!("    Active connections: {}", server.active_connections);
    println!("    Available connections: {}", server.available_connections);
}
```

### Admin Operations

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Create topic
    client.create_topic("my-topic", Some(3)).await?;
    
    // List topics
    let topics = client.list_topics().await?;
    for topic in topics {
        println!("Topic: {}", topic);
    }
    
    // Delete topic
    client.delete_topic("my-topic").await?;
    
    Ok(())
}
```

### Advanced Admin API

Rivven supports advanced admin operations for topic configuration management:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Create topic
    client.create_topic("events", Some(3)).await?;
    
    // Describe topic configurations
    let configs = client.describe_topic_configs(&["events"]).await?;
    for (topic, config) in &configs {
        println!("Topic '{}' configuration:", topic);
        for (key, value) in config {
            println!("  {}: {}", key, value);
        }
    }
    
    // Alter topic configuration
    let result = client.alter_topic_config("events", &[
        ("retention.ms", Some("86400000")),    // 1 day retention
        ("cleanup.policy", Some("compact")),   // Log compaction
        ("max.message.bytes", Some("2097152")), // 2 MB max message
    ]).await?;
    println!("Changed {} config entries", result.changed_count);
    
    // Reset configuration to default
    client.alter_topic_config("events", &[
        ("retention.ms", None),  // Reset to broker default
    ]).await?;
    
    // Increase partition count
    let new_count = client.create_partitions("events", 6).await?;
    println!("Topic now has {} partitions", new_count);
    
    // Delete records before offset (log truncation)
    let results = client.delete_records("events", &[
        (0, 1000),  // Delete records before offset 1000 in partition 0
        (1, 500),   // Delete records before offset 500 in partition 1
    ]).await?;
    for result in results {
        println!("Partition {}: low watermark now {}", 
            result.partition, result.low_watermark);
    }
    
    Ok(())
}
```

#### Supported Topic Configurations

| Configuration | Description | Example |
|---------------|-------------|---------|
| `retention.ms` | Message retention time | `86400000` (1 day) |
| `retention.bytes` | Max partition size | `1073741824` (1 GB) |
| `max.message.bytes` | Max message size | `2097152` (2 MB) |
| `segment.bytes` | Segment file size | `536870912` (512 MB) |
| `segment.ms` | Segment rotation time | `604800000` (7 days) |
| `cleanup.policy` | `delete` or `compact` | `compact` |
| `min.insync.replicas` | Min ISR for acks=all | `2` |
| `compression.type` | `lz4`, `zstd`, `snappy`, `gzip` | `lz4` |
```

### Schema Registration

Register schemas with the Rivven Schema Registry (`rivven-schema`) directly from the client using a lightweight HTTP/1.1 call — no external HTTP dependencies required:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;

    // Register an Avro schema with the schema registry
    let schema_id = client.register_schema(
        "http://localhost:8081",       // Schema registry URL
        "users-value",                 // Subject name
        "AVRO",                        // Schema type: AVRO, PROTOBUF, or JSON
        r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#,
    ).await?;

    println!("Registered schema with ID: {}", schema_id);
    Ok(())
}
```

> **Note:** For advanced schema registry operations (compatibility checks, Glue integration, codec management), use `rivven-connect`'s `SchemaRegistryClient`. The `Client::register_schema()` method is designed for quick schema bootstrapping without additional dependencies.

> **Security:** HTTP responses from the registry are bounded by `MAX_CHUNK_SIZE` (16 MB per chunk) and `MAX_RESPONSE_SIZE` (16 MB total) to prevent memory exhaustion from malicious or misconfigured registries.

### Transactions & Idempotent Producer

Rivven supports native transactions and idempotent producers for exactly-once semantics:

#### Idempotent Producer

Automatic message deduplication using producer IDs and sequence numbers:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Initialize idempotent producer (assigns producer_id and epoch)
    let mut producer = client.init_producer_id(None).await?;
    println!("Producer ID: {}, Epoch: {}", producer.producer_id, producer.producer_epoch);
    
    // Publish with deduplication
    let (offset, partition, was_duplicate) = client
        .publish_idempotent("orders", None::<Vec<u8>>, b"order-data".to_vec(), &mut producer)
        .await?;
    
    if was_duplicate {
        println!("Message was a duplicate (already delivered)");
    } else {
        println!("Published to partition {} at offset {}", partition, offset);
    }
    
    Ok(())
}
```

#### Transactions

Atomic, all-or-nothing message delivery across partitions and topics:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    
    // Initialize transactional producer
    let mut producer = client.init_producer_id(None).await?;
    
    // Begin transaction
    let txn_id = "payment-processor";
    client.begin_transaction(txn_id, &producer, None).await?;
    
    // Register partitions before writing
    client.add_partitions_to_txn(txn_id, &producer, &[
        ("orders", 0),
        ("payments", 0),
    ]).await?;
    
    // Atomic writes across multiple topics
    client.publish_transactional(txn_id, "orders", None::<Vec<u8>>, b"order-created".to_vec(), &mut producer).await?;
    client.publish_transactional(txn_id, "payments", None::<Vec<u8>>, b"payment-processed".to_vec(), &mut producer).await?;
    
    // Commit (all-or-nothing)
    client.commit_transaction(txn_id, &producer).await?;
    println!("Transaction committed atomically!");
    
    Ok(())
}
```

#### Exactly-Once Consume-Transform-Produce

For stream processing with exactly-once semantics:

```rust
use rivven_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("localhost:9092").await?;
    let mut producer = client.init_producer_id(None).await?;
    
    let txn_id = "stream-processor";
    let consumer_group = "processor-group";
    
    // Begin transaction
    client.begin_transaction(txn_id, &producer, None).await?;
    
    // Add output partition to transaction
    client.add_partitions_to_txn(txn_id, &producer, &[("output-topic", 0)]).await?;
    
    // Consume input
    let messages = client.consume("input-topic", 0, 0, 100).await?;
    
    // Transform and produce
    for msg in &messages {
        let transformed = format!("processed: {:?}", msg.value);
        client.publish_transactional(
            txn_id, "output-topic", None::<Vec<u8>>, 
            transformed.into_bytes(), &mut producer
        ).await?;
    }
    
    // Commit consumer offsets as part of transaction
    client.add_offsets_to_txn(
        txn_id, &producer, consumer_group,
        &[("input-topic", 0, messages.len() as i64)]
    ).await?;
    
    // Atomic commit (output messages + consumed offsets)
    client.commit_transaction(txn_id, &producer).await?;
    
    Ok(())
}
```

#### Transaction Error Handling

```rust
use rivven_client::{Client, Error};

// On error, abort the transaction
match client.commit_transaction(txn_id, &producer).await {
    Ok(()) => println!("Committed successfully"),
    Err(e) => {
        eprintln!("Commit failed: {}", e);
        // Abort to discard all writes
        client.abort_transaction(txn_id, &producer).await?;
    }
}
```

## Configuration Reference

### ResilientClientConfig

| Option | Default | Description |
|--------|---------|-------------|
| `servers` | Required | List of server addresses |
| `pool_size_per_server` | 10 | Maximum connections per server |
| `connection_timeout` | 10s | Timeout for establishing connections |
| `request_timeout` | 30s | Timeout for individual requests |
| `max_retries` | 3 | Maximum retry attempts |
| `retry_initial_delay` | 100ms | Initial retry delay |
| `retry_max_delay` | 5s | Maximum retry delay |
| `retry_multiplier` | 2.0 | Delay multiplier between retries |
| `circuit_breaker_failure_threshold` | 5 | Failures before circuit opens |
| `circuit_breaker_recovery_timeout` | 30s | Time before attempting recovery |
| `max_connection_lifetime` | 300s | Maximum time a pooled connection can be reused before recycling |

## Error Handling

The client provides typed errors for different failure modes:

```rust
use rivven_client::{ResilientClient, Error};

match client.publish("topic", None, b"data").await {
    Ok(offset) => println!("Published at offset {}", offset),
    Err(Error::CircuitBreakerOpen(server)) => {
        println!("Server {} is unhealthy, circuit breaker open", server);
    }
    Err(Error::AllServersUnavailable) => {
        println!("All servers are unavailable");
    }
    Err(Error::ConnectionError(msg)) => {
        println!("Connection failed: {}", msg);
    }
    Err(e) => println!("Other error: {}", e),
}
```

## TLS Configuration

Enable TLS for secure connections:

```toml
rivven-client = { version = "0.2", features = ["tls"] }
```

```rust
use rivven_client::{Client, TlsConfig};

let tls_config = TlsConfig::builder()
    .ca_cert_path("/path/to/ca.crt")
    .client_cert_path("/path/to/client.crt")
    .client_key_path("/path/to/client.key")
    .build()?;

let client = Client::connect_with_tls("localhost:9093", tls_config).await?;
```

## Documentation

- [Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started)
- [Security](https://rivven.hupe1980.github.io/rivven/docs/security)
- [Exactly-Once Semantics](https://rivven.hupe1980.github.io/rivven/docs/exactly-once)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
