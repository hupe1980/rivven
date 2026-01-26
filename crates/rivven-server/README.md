# rivven-server

The Rivven broker server binary.

## Features

- **Native Protocol** - High-performance binary protocol
- **HTTP Gateway** - REST API for management
- **Sticky Partitioner** - Kafka 2.4+ style partitioning for optimal throughput
- **Replication** - Multi-node replication with Raft consensus
- **Authentication** - SCRAM-SHA-256, mTLS, API keys
- **Authorization** - Cedar policy engine
- **Dashboard** - Built-in web UI (enabled by default)

## Installation

```bash
# Build from source
cargo build -p rivven-server --release

# Run the server
./target/release/rivven-server --data-dir ./data
```

## Message Partitioning

Rivven uses **sticky partitioning** (Kafka 2.4+ compatible):

| Message Type | Strategy | Behavior |
|:-------------|:---------|:---------|
| With key | Murmur2 hash | Same key → same partition |
| Without key | Sticky batching | Batches to one partition, rotates periodically |
| Explicit partition | Direct | Uses specified partition |

Configure via CLI:
```bash
rivven-server --partitioner-batch-size 16384 --partitioner-linger-ms 100
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
rivven-server --help

USAGE:
    rivven-server [OPTIONS]

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
