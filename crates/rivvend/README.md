# rivvend

The Rivven broker server daemon.

## Features

- **Native Protocol** - High-performance binary protocol
- **HTTP Gateway** - REST API for management
- **Sticky Partitioner** - Kafka 2.4+ style partitioning for optimal throughput
- **Replication** - Multi-node replication with Raft consensus
- **Authentication** - SCRAM-SHA-256, mTLS, API keys
- **Authorization** - Cedar policy engine
- **Dashboard** - Leptos/WASM web UI (embedded when built with `--features dashboard`)

## Installation

```bash
# Build from source (without dashboard)
cargo build -p rivvend --release

# Build with embedded dashboard
# First, build the dashboard assets:
cd crates/rivven-dashboard
trunk build --release
cp -r dist/* ../rivvend/static/
cd ../..

# Then build the server with dashboard feature:
cargo build -p rivvend --release --features dashboard

# Run the server
./target/release/rivvend --data-dir ./data
# Dashboard available at http://localhost:8080/
```

**Note**: The dashboard requires building with the `dashboard` feature, which embeds the WASM assets compiled by trunk. See [rivven-dashboard README](../rivven-dashboard/README.md) for details.

## Message Partitioning

Rivven uses **sticky partitioning** (Kafka 2.4+ compatible):

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition |
| Without key | Sticky batching | Batches to one partition, rotates periodically |
| Explicit partition | Direct | Uses specified partition |

Configure via CLI:
```bash
rivvend --partitioner-batch-size 16384 --partitioner-linger-ms 100
```

Or environment variables:
```bash
export RIVVEN_PARTITIONER_BATCH_SIZE=16384
export RIVVEN_PARTITIONER_LINGER_MS=100
```

## Configuration

```yaml
# rivven.yaml
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

## CLI Options

```bash
rivvend --help

USAGE:
    rivvend [OPTIONS]

OPTIONS:
    -c, --config <FILE>    Configuration file path
    -d, --data-dir <DIR>   Data directory
    -b, --bind <ADDR>      Bind address [default: 0.0.0.0:9092]
    -h, --help             Print help
    -V, --version          Print version
```

## Metrics

The server exposes Prometheus metrics on the HTTP port:

```bash
curl http://localhost:8080/metrics
```

## License

See root [LICENSE](../../LICENSE) file.
