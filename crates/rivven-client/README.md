# rivven-client

Native Rust client library for Rivven.

## Features

- **Async/Await** - Built on Tokio for high-performance async I/O
- **Connection Pooling** - Efficient connection management with configurable pool sizes
- **Automatic Failover** - Bootstrap server failover and reconnection
- **Circuit Breaker** - Fault tolerance with automatic recovery
- **Retry with Exponential Backoff** - Automatic retry with jitter for transient failures
- **Health Monitoring** - Real-time statistics and health checks
- **TLS Support** - Secure connections with rustls
- **SCRAM-SHA-256 Authentication** - Secure challenge-response authentication

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
use rivven_client::ResilientClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ResilientClient::new(config);
    
    // Create topic
    client.create_topic("my-topic", 3, 2).await?;
    
    // List topics
    let topics = client.list_topics().await?;
    for topic in topics {
        println!("Topic: {}", topic);
    }
    
    // Get topic info
    let info = client.get_topic_info("my-topic").await?;
    
    // Delete topic
    client.delete_topic("my-topic").await?;
    
    Ok(())
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

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.

See root [LICENSE](../../LICENSE) file.
