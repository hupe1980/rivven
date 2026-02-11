# Rivven

[![CI](https://github.com/hupe1980/rivven/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rivven/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/rivven.svg)](https://crates.io/crates/rivven)
[![Documentation](https://img.shields.io/badge/docs-rivven.hupe1980.github.io%2Frivven-blue)](https://rivven.hupe1980.github.io/rivven)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-blue)](https://github.com/hupe1980/rivven)

> **A production-grade, high-performance distributed event streaming platform written in Rust.**  
> Lightweight binaries. Zero runtime dependencies. Sub-second startup.

**[📖 Documentation](https://rivven.hupe1980.github.io/rivven)** • **[🚀 Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started)** • **[🏗️ Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)**

---

## Why Rivven?

Modern event streaming shouldn't require a JVM, ZooKeeper, or a team of dedicated operators. Rivven delivers enterprise-grade capabilities with lightweight, focused binaries:

| Traditional Platforms | Rivven |
|:---------------------|:-------|
| JVM + ZooKeeper + heavy dependencies | Lightweight native binaries, no runtime dependencies |
| Minutes to start | <1 second startup |
| Complex configuration | Sensible defaults, auto-create topics |
| Separate CDC tools | Native CDC with `rivven-connect` |
| External schema registry | Integrated `rivven-schema` registry |

---

## ✨ Key Features

<table>
<tr>
<td width="50%" valign="top">

### 🚀 High Performance
- Lock-free architecture with zero-copy I/O
- Batch APIs for maximum throughput  
- LZ4/Zstd/Snappy compression
- Sticky partitioning for optimal batching

</td>
<td width="50%" valign="top">

### 📦 Operational Simplicity
- Lightweight native binaries — no JVM, no ZooKeeper
- <1 second startup time
- Auto-create topics with configurable defaults
- Built-in web dashboard

</td>
</tr>
<tr>
<td width="50%" valign="top">

### 🔄 Change Data Capture
- PostgreSQL CDC with logical replication
- MySQL CDC with binlog streaming
- 17 built-in transforms (SMTs)
- Schema inference and evolution

</td>
<td width="50%" valign="top">

### 📊 Schema Registry
- Avro, Protobuf, and JSON Schema
- Standard wire format and REST API
- Compatibility checking and evolution
- Multi-tenant schema contexts

</td>
</tr>
<tr>
<td width="50%" valign="top">

### 🔒 Enterprise Security
- TLS/mTLS with automatic certificate hot-reload
- SCRAM-SHA-256 (600K PBKDF2 iterations), OIDC, API key auth
- Cedar policy engine for fine-grained RBAC
- Password complexity enforcement
- HMAC-authenticated cluster protocol (SWIM gossip)
- Credential isolation between components

</td>
<td width="50%" valign="top">

### ☁️ Cloud Native
- Kubernetes operator with CRDs
- Prometheus metrics built-in
- Tiered storage (S3, GCS, Azure, MinIO)
- Consumer groups with cooperative rebalancing

</td>
</tr>
</table>

---

## 🚀 Quick Start

```bash
# Install
cargo install rivven rivvend rivven-connect rivven-schema

# Start broker with dashboard
rivvend --dashboard --data-dir ./data

# Create topic, produce, consume
rivven topic create events --partitions 3
rivven produce events "Hello, Rivven!"
rivven consume events --from-beginning
```

### Docker

```bash
# Start broker
docker run -d -p 9092:9092 -p 9094:9094 ghcr.io/hupe1980/rivvend:latest --dashboard

# Start schema registry
docker run -d -p 8081:8081 ghcr.io/hupe1980/rivven-schema:latest serve --port 8081
```

---

## 🔄 Rivven Connect

Stream data between systems with declarative YAML:

```yaml
# connect.yaml
sources:
  orders:
    connector: postgres-cdc
    topic: cdc.orders
    config:
      host: localhost
      database: shop

sinks:
  s3:
    connector: s3
    topics: [cdc.orders]
    config:
      bucket: my-data-lake
      region: us-east-1
```

```bash
rivven-connect run --config connect.yaml
```

**Built-in connectors**: PostgreSQL CDC • MySQL CDC • MQTT • SQS • Pub/Sub • S3 • GCS • Azure • Snowflake • BigQuery • Redshift • HTTP

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      RIVVEN ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐       ┌────────────────────────────────┐  │
│  │     rivvend      │       │       rivven-connect           │  │
│  │    (broker)      │◄─────►│  (connector runtime)           │  │
│  │                  │ proto │                                │  │
│  │  • Storage       │       │  Sources: postgres-cdc, mysql  │  │
│  │  • Replication   │       │  Sinks:   s3, snowflake, http  │  │
│  │  • Auth/RBAC     │       │  Transforms: SMTs, filters     │  │
│  └──────────────────┘       └───────────────┬────────────────┘  │
│                                             │                   │
│                                             ▼                   │
│  ┌──────────────────┐       ┌────────────────────────────────┐  │
│  │ rivven-operator  │       │        rivven-schema           │  │
│  │ (kubernetes CRDs)│       │      (schema registry)         │  │
│  └──────────────────┘       └────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Crate Overview

| Crate | Binary | Description |
|:------|:-------|:------------|
| `rivvend` | `rivvend` | Message broker with storage, replication, and auth |
| `rivven-connect` | `rivven-connect` | Connector runtime for CDC and data pipelines |
| `rivven-schema` | `rivven-schema` | Schema registry with Avro, Protobuf, JSON Schema |
| `rivven` | `rivven` | Command-line interface |
| `rivven-core` | — | Storage engine, partitions, consumer groups |
| `rivven-client` | — | Native async Rust client library |
| `rivven-cluster` | — | Raft consensus and distributed coordination |
| `rivven-operator` | — | Kubernetes operator with CRDs |

<details>
<summary>Additional crates</summary>

| Crate | Description |
|:------|:------------|
| `rivven-cdc` | PostgreSQL and MySQL CDC primitives |
| `rivven-connect-derive` | Proc macros for connector SDK |
| `rivven-protocol` | Wire protocol implementation |
| `rivven-python` | Python bindings (PyO3) |
| `rivven-integration-tests` | End-to-end integration tests |

</details>

---

## 📚 Documentation

| Guide | Description |
|:------|:------------|
| [Getting Started](https://rivven.hupe1980.github.io/rivven/docs/getting-started) | Installation and first steps |
| [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture) | System design and internals |
| [CDC Guide](https://rivven.hupe1980.github.io/rivven/docs/cdc) | Database change data capture |
| [Connectors](https://rivven.hupe1980.github.io/rivven/docs/connectors) | Sources, sinks, and transforms |
| [Schema Registry](https://rivven.hupe1980.github.io/rivven/docs/schema-registry) | Avro, Protobuf, and JSON Schema |
| [Security](https://rivven.hupe1980.github.io/rivven/docs/security) | TLS, authentication, and RBAC |
| [Kubernetes](https://rivven.hupe1980.github.io/rivven/docs/kubernetes) | Production deployment with operator |
| [Tiered Storage](https://rivven.hupe1980.github.io/rivven/docs/tiered-storage) | Hot/warm/cold storage tiers |

---

## 🖥️ Platform Support

| Platform | Status |
|:---------|:-------|
| Linux (x86_64, aarch64) | ✅ Full support |
| macOS (x86_64, aarch64) | ✅ Full support |
| Windows | ❌ Not supported |

---

## 🔧 Build from Source

```bash
git clone https://github.com/hupe1980/rivven.git
cd rivven
cargo build --release

# Binaries are in target/release/
./target/release/rivvend --help        # Broker
./target/release/rivven --help         # CLI
./target/release/rivven-connect --help # Connectors
./target/release/rivven-schema --help  # Schema Registry
```

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).
