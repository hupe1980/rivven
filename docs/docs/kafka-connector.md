# Kafka Connector

The Kafka connector provides bidirectional integration with Apache Kafka for migration
and hybrid deployment scenarios using a **pure Rust implementation** (no C dependencies).

## Overview

| Component | Description |
|-----------|-------------|
| **KafkaSource** | Consume from Kafka topics, stream to Rivven |
| **KafkaSink** | Produce from Rivven topics to Kafka |

## Architecture

The Kafka connector uses [krafka](https://crates.io/crates/krafka), an async-native
pure Rust Kafka client with zero librdkafka or C dependencies:

| Benefit | Description |
|---------|-------------|
| **Zero C deps** | No librdkafka installation required |
| **Simple builds** | No C compiler or linker configuration |
| **Consistent** | Same behavior across all platforms |
| **Native consumer groups** | Full consumer group protocol via krafka |
| **Producer batching** | Built-in linger/batch-size batching |
| **Compression** | None, Gzip, Snappy, LZ4, Zstd |
| **Full auth** | SASL on all clients (Admin, Consumer, Producer) |
| **AWS MSK IAM** | Native IAM authentication for Amazon MSK |

Enable with the `kafka` feature:

```toml
rivven-connect = { version = "0.0.21", features = ["kafka"] }
```

For AWS MSK IAM authentication, enable the `kafka-msk` feature:

```toml
rivven-connect = { version = "0.0.21", features = ["kafka-msk"] }
```

## Use Cases

- **Migration**: Gradually migrate from Kafka to Rivven
- **Hybrid Deployments**: Run Kafka and Rivven side-by-side
- **Cross-Datacenter Replication**: Bridge between Kafka clusters

## Features

| Feature | Description |
|---------|-------------|
| Pure Rust client | Zero C dependencies via krafka |
| Lock-free metrics | `AtomicU64` counters with zero contention |
| Batch size tracking | Min/max/avg with CAS operations |
| Prometheus export | `to_prometheus_format()` for scraping |
| JSON serialization | Serde derives on `MetricsSnapshot` |
| Multiple security | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL |
| SASL mechanisms | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM |
| AWS MSK IAM | Native IAM authentication for Amazon MSK |
| OAUTHBEARER | OAuth 2.0 bearer token authentication |
| Compression | None, Gzip, Snappy, LZ4, Zstd |
| Exactly-once | Idempotent producer support |
| Isolation level | `read_uncommitted` / `read_committed` |
| Hot-path inlining | `#[inline]` on all metrics methods |

## Configuration

### Kafka Source

```yaml
type: kafka-source
topic: kafka-events             # Rivven topic (destination for consumed messages)
config:
  brokers: "kafka1:9092,kafka2:9092"
  topic: orders                 # Kafka topic (external source to consume from)
  consumer_group: rivven-migration  # default: "rivven-connect"
  start_offset: earliest        # earliest (default), latest

  # Consumer tuning
  max_poll_records: 500           # default: 500
  max_poll_interval_ms: 300000    # default: 300000
  session_timeout_ms: 30000       # default: 30000
  heartbeat_interval_ms: 3000     # default: 3000
  fetch_min_bytes: 1              # default: 1
  fetch_max_bytes: 52428800       # default: 50 MB
  request_timeout_ms: 30000       # default: 30000
  empty_poll_delay_ms: 100        # default: 100 — delay when no messages

  # Transactional reads
  isolation_level: read_uncommitted  # read_uncommitted (default), read_committed

  # Consumer offsets
  enable_auto_commit: true        # default: true
  auto_commit_interval_ms: 5000   # default: 5000

  # Data options
  include_headers: true           # include Kafka headers in event metadata
  include_key: true               # include message key in event metadata
  client_id: my-consumer          # optional client ID

  # Security (optional — see Security section)
  security:
    protocol: PLAINTEXT
```

### Kafka Sink

```yaml
type: kafka-sink
topics: [events]                # Rivven topics to consume from
consumer_group: kafka-producer
config:
  brokers: "kafka1:9092"
  topic: orders-replica         # Kafka topic (external destination)

  # Producer settings
  acks: all                     # none, leader, all (default: all)
  compression: lz4              # none, gzip, snappy, lz4, zstd (default: none)
  batch_size_bytes: 16384       # default: 16384 (16 KB)
  linger_ms: 5                  # default: 5
  request_timeout_ms: 30000     # default: 30000
  retries: 3                    # default: 3
  retry_backoff_ms: 100         # default: 100

  # Exactly-once (optional)
  idempotent: true
  transactional_id: rivven-producer-1

  # Message options
  key_field: order_id           # extract key from event JSON data
  include_headers: true         # include Rivven metadata as Kafka headers
  custom_headers:               # static headers added to every message
    source: rivven
    environment: production
  client_id: my-producer        # optional client ID

  # Security (optional — see Security section)
  security:
    protocol: PLAINTEXT
```

## Security Protocols

| Protocol | Description |
|----------|-------------|
| `PLAINTEXT` | No encryption, no authentication (default) |
| `SSL` | TLS encryption, no SASL authentication |
| `SASL_PLAINTEXT` | SASL authentication without TLS |
| `SASL_SSL` | TLS encryption + SASL authentication |

### SASL Configuration

```yaml
security:
  protocol: SASL_SSL
  sasl_mechanism: PLAIN         # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, AWS_MSK_IAM
  sasl_username: user
  sasl_password: ${KAFKA_PASSWORD}
  ssl_ca_cert: /etc/ssl/ca.pem          # optional CA cert (PEM)
  ssl_client_cert: /etc/ssl/client.pem  # optional client cert for mTLS
  ssl_client_key: /etc/ssl/client-key.pem
```

### SASL Mechanisms

| Mechanism | Description | Status |
|-----------|-------------|--------|
| `PLAIN` | Username/password in cleartext — use with SSL | ✅ Supported |
| `SCRAM-SHA-256` | SCRAM with SHA-256 | ✅ Supported |
| `SCRAM-SHA-512` | SCRAM with SHA-512 | ✅ Supported |
| `OAUTHBEARER` | OAuth 2.0 bearer token authentication | ✅ Supported |
| `AWS_MSK_IAM` | AWS IAM authentication for Amazon MSK | ✅ Supported |

All SASL mechanisms are supported on **all client types** (AdminClient, Consumer,
and Producer) via krafka v0.2.

### OAUTHBEARER Configuration

Use OAuth 2.0 bearer tokens for authentication:

```yaml
security:
  protocol: SASL_SSL
  sasl_mechanism: OAUTHBEARER
  oauth_token: ${OAUTH_TOKEN}
```

### AWS MSK IAM Configuration

For Amazon MSK clusters with IAM authentication, enable the `kafka-msk` feature
and configure IAM credentials:

```yaml
security:
  protocol: SASL_SSL
  sasl_mechanism: AWS_MSK_IAM
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
  aws_region: us-east-1
```

> **Note**: The `kafka-msk` feature flag must be enabled in your `Cargo.toml`
> to use AWS MSK IAM authentication. This pulls in the AWS SDK dependencies
> needed for IAM signing.
>
> **Security**: AWS MSK IAM requires `SASL_SSL` protocol. Using `SASL_PLAINTEXT`
> with MSK IAM is rejected at configuration time because IAM credentials must
> not be transmitted without TLS encryption.

## Isolation Level

The source connector supports Kafka's isolation level for transactional reads:

| Level | Description |
|-------|-------------|
| `read_uncommitted` | Return all records, including from aborted transactions (default) |
| `read_committed` | Return only committed transactional records |

Use `read_committed` when consuming from topics written by a transactional producer
and you want exactly-once semantics.

## Metrics & Observability

All metrics use lock-free `AtomicU64` counters with `Ordering::Relaxed` for zero
contention on hot paths. Derived metrics (averages, rates) are computed from
snapshots — never on the write path.

### Source Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_consumed` | counter | Total messages consumed |
| `bytes_consumed` | counter | Total bytes consumed |
| `polls` | counter | Total poll operations |
| `empty_polls` | counter | Polls that returned no messages |
| `errors` | counter | Total errors encountered |
| `commits` | counter | Successful offset commits |
| `commit_failures` | counter | Failed offset commits |
| `rebalances` | counter | Consumer group rebalance events |
| `min_batch_size` | gauge | Smallest non-empty batch size seen |
| `max_batch_size` | gauge | Largest batch size seen |

**Derived metrics** (computed from snapshot):

| Method | Description |
|--------|-------------|
| `avg_batch_size()` | Average batch size (0.0 if no batches) |
| `rate(elapsed)` | Messages per second over the given duration |
| `empty_poll_rate()` | Ratio of empty polls to total polls (0.0–1.0) |

### Sink Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_produced` | counter | Total messages produced |
| `bytes_produced` | counter | Total bytes produced |
| `sends` | counter | Total send operations |
| `errors` | counter | Total errors encountered |
| `flushes` | counter | Total flush operations |
| `retries` | counter | Total retry attempts |
| `min_batch_size` | gauge | Smallest non-empty batch size seen |
| `max_batch_size` | gauge | Largest batch size seen |

**Derived metrics** (computed from snapshot):

| Method | Description |
|--------|-------------|
| `avg_batch_size()` | Average batch size (0.0 if no batches) |
| `rate(elapsed)` | Messages per second over the given duration |
| `success_rate()` | Ratio of produced to total (produced + errors), 1.0 if no activity |

### Prometheus Export

Export metrics in Prometheus text format for scraping:

```rust
let snapshot: KafkaSourceMetricsSnapshot = /* from connector state */;
let prometheus_output = snapshot.to_prometheus_format("rivven");

// Output:
// # HELP rivven_messages_consumed_total Total messages consumed
// # TYPE rivven_messages_consumed_total counter
// rivven_messages_consumed_total 10000
// # HELP rivven_bytes_consumed_total Total bytes consumed
// # TYPE rivven_bytes_consumed_total counter
// rivven_bytes_consumed_total 500000
// ...
```

### JSON Export

Metrics snapshots implement `Serialize` / `Deserialize` for JSON:

```rust
let json = serde_json::to_string(&snapshot)?;
// {"messages_consumed":10000,"bytes_consumed":500000,...}

// Structured logging
tracing::info!(metrics = ?snapshot, "Kafka source metrics");
```

### Interval-Based Reporting

Use `snapshot_and_reset()` for periodic reporting:

```rust
// Snapshot current metrics and reset all counters
let interval_snapshot = metrics.snapshot_and_reset();
```

## Start Offset Options

| Mode | Description |
|------|-------------|
| `earliest` | Start from the first available message (default) |
| `latest` | Start from the newest message only |

## Best Practices

### Migration from Kafka

1. **Start with earliest offset** to replay full history (or `latest` to skip)
2. **Enable idempotent producer** on the sink for exactly-once
3. **Monitor consumer lag** using source metrics
4. **Use SASL_SSL** for production security with full authentication

### High Throughput

1. **Increase `batch_size_bytes`** for better batching
2. **Use LZ4 compression** for best speed/ratio
3. **Tune `linger_ms`** to accumulate batches (higher = more throughput, more latency)
4. **Monitor `empty_poll_rate`** — high values indicate low message volume
5. **Increase `max_poll_records`** for larger consumer batches

### Exactly-Once Semantics

```yaml
# Sink: enable idempotent and transactional producer
idempotent: true
transactional_id: rivven-producer-unique-id
acks: all

# Source: read only committed transactional records
isolation_level: read_committed
```

## Troubleshooting

### Connection Issues

Check broker connectivity:
```bash
# Test TCP connection
nc -zv kafka1 9092

# Check DNS resolution
nslookup kafka1
```

### Authentication Errors

If you see authentication failures:

1. Verify credentials (username/password, OAuth token, or IAM keys)
2. Ensure the correct `sasl_mechanism` matches your broker configuration
3. For AWS MSK IAM, verify the `kafka-msk` feature is enabled and credentials are valid
4. Check that `protocol` is set to `SASL_PLAINTEXT` or `SASL_SSL`

### Performance Issues

Monitor these metrics:
- `empty_poll_rate` > 0.5 indicates low message volume — tune `empty_poll_delay_ms`
- `success_rate` < 0.95 on sink indicates frequent producer errors
- High `retries` count suggests broker instability — check `retry_backoff_ms`

## Testing

### Unit Tests (77 tests)

```bash
cargo test -p rivven-connect --features kafka -- kafka
```

Covers configuration parsing, enum conversions, security validation (PLAIN,
SCRAM-SHA-256/512, OAUTHBEARER, AWS MSK IAM, MSK+SASL_PLAINTEXT rejection),
metrics operations, Prometheus export, JSON round-trips, header capacity
pre-allocation, and spec generation.

### Integration Tests

```bash
cargo test -p rivven-integration-tests --test kafka_connector
```

**Infrastructure Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_container_starts` | Verifies Kafka container lifecycle |
| `test_kafka_create_topic` | Topic creation via admin API |
| `test_kafka_produce_consume` | End-to-end message round-trip |

**Source Connector Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_source_check` | Source `check()` connectivity |
| `test_kafka_source_discover` | Source `discover()` catalog discovery |
| `test_kafka_source_read` | Source `read()` message streaming |
| `test_kafka_source_offset_modes` | Earliest/Latest start modes |
| `test_kafka_source_high_throughput` | 1000+ message performance |
| `test_kafka_source_multiple_partitions` | Multi-partition topic support |
| `test_kafka_source_empty_topic` | Empty topic handling |
| `test_kafka_source_invalid_broker` | Invalid broker error handling |
| `test_kafka_multiple_consumer_groups` | Independent consumer groups |

**Sink Connector Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_sink_check` | Sink `check()` validation |
| `test_kafka_sink_compression` | Gzip/LZ4/Snappy/Zstd codecs |
| `test_kafka_sink_custom_headers` | Custom header injection |
| `test_kafka_sink_idempotent` | Idempotent producer config |

**Batch & Edge Case Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_batch_consumption` | Batch consumption across varying sizes |

### Hot-Path Optimizations

Critical methods are annotated with `#[inline]` for optimal performance:

- `record_batch()`, `record_error()`, `record_commit()`, `record_flush()`, `record_retry()`
- `avg_batch_size()`, `rate()`, `empty_poll_rate()`, `success_rate()`
- `snapshot()` — snapshot capture on both source and sink metrics

The source `read()` loop uses a reusable `String` buffer for position keys
instead of per-record `format!()` allocations.

The sink `build_headers()` returns `Vec<(String, Vec<u8>)>` directly — no
intermediate `HashMap` or `Bytes` allocation per message.

Prometheus `to_prometheus_format()` uses `write!()` into a pre-allocated
buffer — zero intermediate `String` allocations per scrape.
