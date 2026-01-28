---
layout: default
title: Home
nav_order: 1
description: "Rivven is a lightweight, cross-platform, single-binary distributed event streaming platform built in Rust."
permalink: /
---

# Rivven
{: .fs-9 }

A lightweight, cross-platform, single-binary distributed event streaming platform.
{: .fs-6 .fw-300 }

[Get Started](/rivven/docs/getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/hupe1980/rivven){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Why Rivven?

Rivven brings **Kafka-class event streaming** to the modern era—without the JVM complexity, ZooKeeper dependencies, or operational overhead.

{: .highlight }
> **Single binary. Zero dependencies. Production ready.**

---

## Features at a Glance

### 🚀 Core Streaming
- **High-throughput message broker** with partitioned topics
- **Consumer groups** with automatic offset management
- **Raft consensus** for distributed coordination
- **LZ4/Zstd compression** for efficient storage

### 🔄 Change Data Capture
- **PostgreSQL CDC** with logical replication
- **MySQL CDC** with binlog streaming
- **Auto-provisioning** of replication slots and publications
- **17 built-in transforms** (SMTs) for data transformation

### 🔌 Connectors
- **Sources**: PostgreSQL, MySQL
- **Sinks**: S3, HTTP Webhook, Snowflake, stdout
- **SDK** for building custom connectors

### 🔒 Security
- **TLS/mTLS** for transport encryption
- **SCRAM-SHA-256** authentication
- **RBAC** with Cedar policy engine
- **Credential isolation** between components

### ☸️ Cloud Native
- **Kubernetes Operator** with CRDs
- **Prometheus metrics** built-in
- **Web dashboard** for monitoring

---

## Quick Start

### Installation

```bash
# From crates.io
cargo install rivven

# Or build from source
git clone https://github.com/hupe1980/rivven
cd rivven
cargo build --release
```

### Start the Broker

```bash
rivvend
```

### Publish & Consume

```bash
# Create a topic
rivven topic create events

# Publish messages
rivven produce events "Hello, Rivven!"

# Consume messages
rivven consume events
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
│                                                                 │
│  ┌──────────────────┐       ┌────────────────────────────────┐ │
│  │     rivvend      │       │       rivven-connect           │ │
│  │    (broker)      │◄─────►│  (SDK + CLI)                   │ │
│  │                  │native │                                 │ │
│  │  • Storage       │proto  │  SDK:   Source, Sink, Transform │ │
│  │  • Replication   │       │  CLI:   Config-driven pipelines │ │
│  │  • Consumer Grps │       │  Sources: postgres-cdc, mysql   │ │
│  │  • Auth/RBAC     │       │  Sinks:   stdout, s3, http      │ │
│  └──────────────────┘       └────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Performance

| Workload | Result | Notes |
|:---------|:-------|:------|
| Append 100KB | 46 µs | ~2 GiB/s throughput |
| Batch read 1K | 1.2 ms | ~836K messages/s |
| Schema lookup | 29 ns | O(1) with caching |

---

## Documentation

<div class="code-example" markdown="1">

| Guide | Description |
|:------|:------------|
| [Getting Started](/rivven/docs/getting-started) | Installation and first steps |
| [Architecture](/rivven/docs/architecture) | System design and components |
| [CDC Guide](/rivven/docs/cdc) | Change Data Capture setup |
| [Connectors](/rivven/docs/connectors) | Source and sink configuration |
| [Security](/rivven/docs/security) | Authentication, TLS, and RBAC |
| [Kubernetes](/rivven/docs/kubernetes) | Operator and Helm deployment |
| [Tiered Storage](/rivven/docs/tiered-storage) | Hot/warm/cold storage tiers |
| [Consumer Groups](/rivven/docs/consumer-groups) | Static membership & cooperative rebalancing |
| [OIDC Authentication](/rivven/docs/oidc) | OpenID Connect integration |
| [Cedar Authorization](/rivven/docs/cedar) | Policy-as-code with AWS Cedar |
| [Log Compaction](/rivven/docs/log-compaction) | Kafka-compatible compaction |
| [Compression](/rivven/docs/compression) | LZ4 and Zstd compression |

</div>

---

## License

Licensed under the [Apache License, Version 2.0](https://github.com/hupe1980/rivven/blob/main/LICENSE).
