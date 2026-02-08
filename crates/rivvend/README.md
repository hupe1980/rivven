# rivvend

> The Rivven message broker daemon — a high-performance, single-binary event streaming server.

## Overview

`rivvend` is the core message broker that handles storage, replication, authentication, and message routing. It's designed to be schema-agnostic, handling raw bytes while delegating schema management to `rivven-schema`.

## Features

| Category | Features |
|:---------|:---------|
| **Performance** | Lock-free I/O, zero-copy reads, sticky partitioning |
| **Protocol** | Native binary protocol, HTTP REST API |
| **Storage** | Append-only log, tiered storage, log compaction |
| **Clustering** | Raft consensus, multi-node replication |
| **Security** | SCRAM-SHA-256 (600K PBKDF2), mTLS, API keys, Cedar RBAC, password complexity enforcement |
| **Observability** | Prometheus metrics, web dashboard |

## Quick Start

```bash
# Install
cargo install rivvend

# Start with defaults
rivvend --data-dir ./data

# Start with dashboard
rivvend --data-dir ./data --dashboard
# Dashboard available at http://localhost:8080/
```

## Building from Source

```bash
# Standard build
cargo build -p rivvend --release

# Build with embedded dashboard (requires trunk)
cd crates/rivven-dashboard && trunk build --release
cp -r dist/* ../rivvend/static/
cd ../..
cargo build -p rivvend --release --features dashboard
```

## Message Partitioning

Rivven uses **sticky partitioning** for optimal throughput:

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition |
| Without key | Sticky batching | Batches to one partition, rotates periodically |
| Explicit partition | Direct | Uses specified partition |

Configure via CLI or environment:
```bash
rivvend --partitioner-batch-size 16384 --partitioner-linger-ms 100

# Or via environment
export RIVVEN_PARTITIONER_BATCH_SIZE=16384
export RIVVEN_PARTITIONER_LINGER_MS=100
```

## Configuration

```yaml
# rivvend.yaml
server:
  bind_address: "0.0.0.0:9092"
  http_port: 8080

storage:
  data_dir: /var/lib/rivven
  segment_bytes: 1073741824  # 1GB

cluster:
  node_id: 1
  peers:
    - "node2.rivven:9093"
    - "node3.rivven:9093"

auth:
  enabled: true
  mechanism: scram-sha-256

tls:
  enabled: true
  cert_file: /etc/rivven/server.crt
  key_file: /etc/rivven/server.key
```

## CLI Reference

```
rivvend [OPTIONS]

OPTIONS:
    -c, --config <FILE>           Configuration file path
    -d, --data-dir <DIR>          Data directory [default: ./data]
    -b, --bind <ADDR>             Bind address [default: 0.0.0.0:9092]
        --dashboard               Enable web dashboard
        --http-port <PORT>        HTTP port [default: 8080]
    -h, --help                    Print help
    -V, --version                 Print version
```

## Quota Enforcement

All quota enforcement (request, produce, consume) flows through a unified `handle_with_principal()` entry point. The authenticated handler extracts the principal and delegates directly, avoiding double-counting:

| Path | Principal | Enforcement |
|:-----|:----------|:------------|
| Authenticated | `session.principal_name` | Per-user quotas |
| Anonymous | `None` | Global quotas |

## Metrics

Prometheus metrics are exposed on the HTTP port:

```bash
curl http://localhost:8080/metrics
```

Key metrics include:
- `rivven_messages_received_total` — Total messages received
- `rivven_messages_sent_total` — Total messages sent
- `rivven_storage_bytes` — Storage size by topic
- `rivven_replication_lag_ms` — Replication lag

## Documentation

- [Architecture](https://rivven.hupe1980.github.io/rivven/docs/architecture)
- [Security](https://rivven.hupe1980.github.io/rivven/docs/security)
- [Kubernetes](https://rivven.hupe1980.github.io/rivven/docs/kubernetes)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
