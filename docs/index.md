---
layout: default
title: Home
nav_order: 1
description: "Rivven is a high-performance distributed event streaming platform written in Rust."
permalink: /
---

# Rivven
{: .fs-9 }

A high-performance distributed event streaming platform.
{: .fs-6 .fw-300 }

[Get Started](/rivven/docs/getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/hupe1980/rivven){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Why Rivven?

{: .highlight }
> **Lightweight binaries. Zero dependencies. Sub-second startup. Production ready.**

Modern event streaming shouldn't require a JVM, ZooKeeper, or a team of dedicated operators. Rivven delivers enterprise-grade capabilities with lightweight, focused binaries:

| Traditional Platforms | Rivven |
|:---------------------|:-------|
| JVM + ZooKeeper + heavy dependencies | Lightweight native binaries, no runtime dependencies |
| Minutes to start | <1 second startup |
| Separate CDC tools required | Native CDC with `rivven-connect` |
| External schema registry | Integrated `rivven-schema` registry |
| Complex operational overhead | Sensible defaults, auto-create topics |

---

## Key Features

### 🚀 High Performance
- **Lock-free architecture** with zero-copy I/O
- **Batch APIs** for maximum throughput
- **LZ4/Zstd/Snappy compression** for efficient storage
- **Sticky partitioning** for optimal batching

### 🔄 Change Data Capture
- **PostgreSQL CDC** with logical replication
- **MySQL CDC** with binlog streaming
- **Auto-provisioning** of replication slots and publications
- **17 built-in transforms** (SMTs) for data transformation

### 🔌 Connectors
- **Sources**: PostgreSQL, MySQL, MQTT, SQS, Pub/Sub, HTTP
- **Sinks**: S3, GCS, Azure, Snowflake, BigQuery, Redshift, HTTP
- **SDK** for building custom connectors

### 📊 Schema Registry
- **Avro**, **Protobuf**, and **JSON Schema** support
- **Standard wire format** and REST API
- **External registry** integration (AWS Glue)
- **Schema evolution** with compatibility checking

### 🔒 Enterprise Security
- **TLS/mTLS** for transport encryption
- **SCRAM-SHA-256** and **OIDC** authentication
- **Cedar policy engine** for fine-grained RBAC
- **Credential isolation** between components

### ☸️ Cloud Native
- **Kubernetes Operator** with CRDs
- **Prometheus metrics** built-in
- **Tiered storage** (S3, GCS, Azure, MinIO)
- **Web dashboard** for monitoring

---

## Quick Start

### Installation

```bash
# From crates.io
cargo install rivven rivvend rivven-connect rivven-schema

# Or with Docker
docker pull ghcr.io/hupe1980/rivvend:latest
```

### Start the Broker

```bash
rivvend --dashboard
```

### Publish & Consume

```bash
# Create a topic
rivven topic create events --partitions 3

# Publish messages
rivven produce events "Hello, Rivven!"

# Consume messages
rivven consume events --from-beginning
```

### Stream CDC to Broker

```yaml
# rivven-connect.yaml
version: "1.0"

broker:
  bootstrap_servers:
    - localhost:9092

sources:
  orders:
    connector: postgres-cdc
    topic: cdc.orders
    config:
      host: localhost
      database: shop
      user: postgres
      password: ${POSTGRES_PASSWORD}

sinks:
  console:
    connector: stdout
    topics: [cdc.orders]
```

```bash
rivven-connect -c rivven-connect.yaml
```

---

## Architecture

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

---

## Documentation

| Guide | Description |
|:------|:------------|
| [Getting Started](/rivven/docs/getting-started) | Installation and first steps |
| [Architecture](/rivven/docs/architecture) | System design and components |
| [CDC Guide](/rivven/docs/cdc) | Change Data Capture setup |
| [Connectors](/rivven/docs/connectors) | Source and sink configuration |
| [Schema Registry](/rivven/docs/schema-registry) | Avro, Protobuf, and JSON Schema |
| [Security](/rivven/docs/security) | Authentication, TLS, and RBAC |
| [Kubernetes](/rivven/docs/kubernetes) | Operator and Helm deployment |
| [Tiered Storage](/rivven/docs/tiered-storage) | Hot/warm/cold storage tiers |

---

## License

Licensed under the [Apache License, Version 2.0](https://github.com/hupe1980/rivven/blob/main/LICENSE).
