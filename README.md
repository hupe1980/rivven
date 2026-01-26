# Rivven

[![CI](https://github.com/hupe1980/rivven/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rivven/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/rivven.svg)](https://crates.io/crates/rivven)
[![Documentation](https://img.shields.io/badge/docs-hupe1980.github.io%2Frivven-blue)](https://hupe1980.github.io/rivven)

> **Production-grade**, high-performance, single-binary distributed event streaming platform written in Rust.

**[Documentation](https://hupe1980.github.io/rivven)** | **[Getting Started](https://hupe1980.github.io/rivven/docs/getting-started)** | **[Architecture](https://hupe1980.github.io/rivven/docs/architecture)**

## Overview

Rivven is a distributed event streaming platform inspired by Apache Kafka, designed for **extreme performance**, simplicity, and production readiness.

| Feature | Rivven | Kafka | Redpanda |
|:--------|:-------|:------|:---------|
| Language | Rust | Java | C++ |
| Memory | 20-50 MB | 4-14 GB | 1-8 GB |
| Startup | <1 sec | 60-120 sec | 10-30 sec |
| Single Binary | ✅ | ❌ | ✅ |
| Native CDC | ✅ | ❌ | ❌ |
| WASM Plugins | ✅ | ❌ | ✅ |

## Quick Start

```bash
# Install from crates.io
cargo install rivven-cli rivven-server rivven-connect

# Start the broker with dashboard
rivven-server --dashboard --data-dir ./data

# Create a topic
rivven topic create events --partitions 3

# Publish
rivven publish events \"Hello, Rivven!\"

# Consume
rivven consume events --from-beginning
```

### Rivven Connect (Data Pipelines)

```yaml
# pipeline.yaml
version: \"1.0\"

broker:
  bootstrap_servers:
    - 127.0.0.1:9092

settings:
  topic:
    auto_create: true          # Auto-create topics
    default_partitions: 3

sources:
  demo:
    connector: datagen         # Built-in data generator
    topic: demo-events
    config:
      pattern: orders
      cdc_mode: true

sinks:
  console:
    connector: stdout
    topics: [demo-events]
    consumer_group: demo-sink
```

```bash
# Run pipeline (topic auto-created!)
rivven-connect run --config pipeline.yaml
```

### Docker

```bash
docker run -d --name rivven \\
  -p 9092:9092 -p 9094:9094 \\
  -v rivven-data:/data \\
  ghcr.io/hupe1980/rivven:latest --dashboard
```

## Features

- **🚀 High Performance**: Lock-free architecture, zero-copy I/O, batch APIs
- **📦 Single Binary**: No JVM, no ZooKeeper, <1s startup
- **🔌 Native CDC**: PostgreSQL and MySQL change data capture
- **⚡ Auto-Create Topics**: Topics created automatically with configurable defaults
- **🔀 Bootstrap Failover**: Multiple brokers with automatic failover
- **🎯 Sticky Partitioning**: Kafka 2.4+ style partitioner for optimal throughput
- **🧩 Plugin System**: WASM-based transforms and connectors
- **📊 Built-in Dashboard**: Real-time monitoring at `/dashboard`
- **☁️ Cloud Native**: Kubernetes operator, Prometheus metrics
- **🔒 Secure**: TLS/mTLS, SCRAM-SHA-256, ACLs, Cedar policies, audit logging
- **🛡️ Resilient Client**: Connection pooling, circuit breaker, automatic retries

## Message Partitioning

Rivven uses **sticky partitioning** (Kafka 2.4+ style) for optimal performance:

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition (Kafka-compatible) |
| Without key | Sticky batching | Batches to one partition, rotates periodically |
| Explicit | Direct | Uses specified partition |

Configure via CLI:
```bash
rivven-server --partitioner-batch-size 16384 --partitioner-linger-ms 100
```

## Documentation

Visit **[hupe1980.github.io/rivven](https://hupe1980.github.io/rivven)** for:

- [Getting Started](https://hupe1980.github.io/rivven/docs/getting-started) - Installation and first steps
- [Architecture](https://hupe1980.github.io/rivven/docs/architecture) - System design
- [CDC Guide](https://hupe1980.github.io/rivven/docs/cdc) - Database replication
- [Connectors](https://hupe1980.github.io/rivven/docs/connectors) - Sources and sinks
- [Security](https://hupe1980.github.io/rivven/docs/security) - TLS, auth, RBAC
- [Kubernetes](https://hupe1980.github.io/rivven/docs/kubernetes) - Production deployment

## Crate Structure

| Crate | Description |
|:------|:------------|
| `rivven-core` | Storage engine, messages, partitions |
| `rivven-server` | TCP server, protocol handling |
| `rivven-client` | Rust client library |
| `rivven-cli` | Command-line interface |
| `rivven-cdc` | PostgreSQL and MySQL CDC |
| `rivven-connect` | Connector framework |
| `rivven-cluster` | Distributed coordination |
| `rivven-plugin-sdk` | Plugin development SDK |
| `rivven-plugin-runtime` | WASM plugin runtime |

## Build from Source

```bash
# Clone
git clone https://github.com/hupe1980/rivven.git
cd rivven

# Build
cargo build --release

# Run server
./target/release/rivven-server --data-dir ./data
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

