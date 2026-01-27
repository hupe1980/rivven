# rivven

[![Crates.io](https://img.shields.io/crates/v/rivven.svg)](https://crates.io/crates/rivven)
[![Documentation](https://docs.rs/rivven/badge.svg)](https://docs.rs/rivven)
[![License](https://img.shields.io/crates/l/rivven.svg)](LICENSE)

High-performance distributed event streaming platform.

## Installation

```toml
[dependencies]
rivven = "0.0.1"
```

## Quick Start

```rust,no_run
use rivven::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a client
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()
        .await?;

    // Produce messages
    let producer = client.producer("my-topic").await?;
    producer.send(b"Hello, Rivven!").await?;

    // Consume messages
    let consumer = client.consumer("my-topic", "my-group").await?;
    while let Some(record) = consumer.recv().await? {
        println!("Received: {:?}", record);
    }

    Ok(())
}
```

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `client` | ✓ | Async client for connecting to Rivven brokers |
| `full` | | Enable all features |

## Crate Structure

The Rivven ecosystem consists of several crates:

- [`rivven`](https://crates.io/crates/rivven) - This crate, the main entry point
- [`rivven-core`](https://crates.io/crates/rivven-core) - Core types and traits
- [`rivven-client`](https://crates.io/crates/rivven-client) - Async client library
- [`rivven-server`](https://crates.io/crates/rivven-server) - Broker server
- [`rivven-connect`](https://crates.io/crates/rivven-connect) - Connector framework

## License

Licensed under Apache License, Version 2.0 ([LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>)
