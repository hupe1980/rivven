# Rivven

[![CI](https://github.com/hupe1980/rivven/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/rivven/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![crates.io](https://img.shields.io/crates/v/rivven.svg)](https://crates.io/crates/rivven)
[![Documentation](https://img.shields.io/badge/docs-hupe1980.github.io%2Frivven-blue)](https://hupe1980.github.io/rivven)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-blue)](https://github.com/hupe1980/rivven)

> **Production-grade**, high-performance, single-binary distributed event streaming platform written in Rust.

**[Documentation](https://hupe1980.github.io/rivven)** | **[Getting Started](https://hupe1980.github.io/rivven/docs/getting-started)** | **[Architecture](https://hupe1980.github.io/rivven/docs/architecture)**

---

## ✨ Features

### 🚀 Performance
- Lock-free architecture with zero-copy I/O
- Batch APIs for maximum throughput
- Sticky partitioning (Kafka 2.4+ compatible)
- LZ4/Zstd compression built-in

### 📦 Simplicity
- Single binary — no JVM, no ZooKeeper
- <1s startup time
- Auto-create topics with configurable defaults
- Built-in dashboard at `/dashboard`

### 🗄️ Storage
- Tiered storage: hot/warm/cold tiers
- S3, GCS, Azure, MinIO backends
- Log compaction for stateful topics
- Exactly-once semantics (KIP-98)

### 🔒 Security
- TLS/mTLS transport encryption
- SCRAM-SHA-256 authentication
- OIDC authentication support
- Cedar policies for fine-grained RBAC

### 🔄 Change Data Capture
- PostgreSQL CDC with logical replication
- MySQL CDC with binlog streaming
- 17 built-in transforms (SMTs)

### 📊 Schema Registry
- Avro, Protobuf, and JSON Schema support
- Confluent-compatible wire format
- Embedded or external deployment modes

### ☁️ Cloud Native
- Kubernetes operator with CRDs
- Prometheus metrics built-in
- Consumer groups with KIP-345/429

---

## 🚀 Quick Start

```bash
# Install
cargo install rivven rivvend rivven-connect

# Start broker with dashboard
rivvend --dashboard --data-dir ./data

# Create topic, produce, consume
rivven topic create events --partitions 3
rivven produce events "Hello, Rivven!"
rivven consume events --from-beginning
```

### Docker

```bash
docker run -d -p 9092:9092 -p 9094:9094 ghcr.io/hupe1980/rivven:latest --dashboard
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

**Built-in connectors**: PostgreSQL CDC, MySQL CDC, Kafka, MQTT, SQS, Pub/Sub, S3, GCS, Azure, Snowflake, BigQuery, Redshift, HTTP

---

## 📚 Documentation

| Guide | Description |
|:------|:------------|
| [Getting Started](https://hupe1980.github.io/rivven/docs/getting-started) | Installation and first steps |
| [Architecture](https://hupe1980.github.io/rivven/docs/architecture) | System design and internals |
| [CDC Guide](https://hupe1980.github.io/rivven/docs/cdc) | Database change data capture |
| [Connectors](https://hupe1980.github.io/rivven/docs/connectors) | Sources, sinks, and transforms |
| [Schema Registry](https://hupe1980.github.io/rivven/docs/schema-registry) | Avro, Protobuf, and JSON Schema |
| [Security](https://hupe1980.github.io/rivven/docs/security) | TLS, authentication, and RBAC |
| [Tiered Storage](https://hupe1980.github.io/rivven/docs/tiered-storage) | Hot/warm/cold storage tiers |
| [Kubernetes](https://hupe1980.github.io/rivven/docs/kubernetes) | Production deployment |

---

## 🏗️ Architecture

| Crate | Binary | Description |
|:------|:-------|:------------|
| `rivven` | `rivven` | Command-line interface |
| `rivvend` | `rivvend` | Broker with TCP server, protocol handling |
| `rivven-connect` | `rivven-connect` | Connector framework for CDC & ETL |
| `rivven-core` | — | Storage engine, partitions, consumers |
| `rivven-client` | — | Rust client library |
| `rivven-cluster` | — | Raft-based distributed coordination |

<details>
<summary>View all crates</summary>

| Crate | Description |
|:------|:------------|
| `rivven-cdc` | PostgreSQL and MySQL CDC |
| `rivven-dashboard` | Leptos web dashboard |
| `rivven-storage` | Cloud storage (S3, GCS, Azure) |
| `rivven-warehouse` | Data warehouses (Snowflake, BigQuery, Redshift) |
| `rivven-operator` | Kubernetes operator |
| `rivven-python` | Python bindings (PyO3) |

</details>

---

## 🖥️ Supported Platforms

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
./target/release/rivvend --data-dir ./data
```

---

## 📄 License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
