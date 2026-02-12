# Kafka Connector

The Kafka connector provides bidirectional integration with Apache Kafka for migration
and hybrid deployment scenarios using a **pure Rust implementation** (no C dependencies).

## Overview

| Component | Description |
|-----------|-------------|
| **KafkaSource** | Consume from Kafka topics, stream to Rivven |
| **KafkaSink** | Produce from Rivven topics to Kafka |

## Architecture

The Kafka connector uses [rskafka](https://crates.io/crates/rskafka), a pure Rust
Kafka client with zero librdkafka or C dependencies:

| Benefit | Description |
|---------|-------------|
| **Zero C deps** | No librdkafka installation required |
| **Simple builds** | No C compiler or linker configuration |
| **Consistent** | Same behavior across all platforms |
| **SASL auth** | Full support for authenticated clusters |

Enable with the `kafka` feature:

```toml
rivven-connect = { version = "0.0.15", features = ["kafka"] }
```

## Use Cases

- **Migration**: Gradually migrate from Kafka to Rivven
- **Hybrid Deployments**: Run Kafka and Rivven side-by-side
- **Cross-Datacenter Replication**: Bridge between Kafka clusters

## Features

| Feature | Description |
|---------|-------------|
| Pure Rust client | Zero C dependencies via rskafka |
| Lock-free metrics | Atomic counters with zero contention |
| Batch size tracking | Min/max/avg with CAS operations |
| Latency tracking | Poll/produce latency measurements |
| Exponential backoff | Configurable retry with backoff |
| Prometheus export | `to_prometheus_format()` for scraping |
| JSON serialization | Serde derives on MetricsSnapshot |
| Multiple security | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL |
| Compression | None, Gzip, Snappy, LZ4, Zstd |
| Exactly-once | Idempotent/transactional producer support |

## Configuration

### Kafka Source

```yaml
type: kafka-source
topic: kafka-events             # Rivven topic (destination for consumed messages)
config:
  brokers: ["kafka1:9092", "kafka2:9092"]
  topic: orders                 # Kafka topic (external source to consume from)
  consumer_group: rivven-migration
  start_offset: earliest  # earliest, latest, offset, timestamp
  
  # Consumer tuning
  max_poll_interval_ms: 300000
  session_timeout_ms: 10000
  heartbeat_interval_ms: 3000
  fetch_max_messages: 500
  
  # Resilience & backoff
  retry_initial_ms: 100       # Initial retry delay
  retry_max_ms: 10000         # Maximum retry delay
  retry_multiplier: 2.0       # Exponential backoff multiplier
  empty_poll_delay_ms: 50     # Delay when no messages
  
  # Data options
  include_headers: true
  include_key: true
  
  # Security (optional)
  security:
    protocol: sasl_ssl
    mechanism: PLAIN          # Note: rskafka v0.5 supports PLAIN only
    username: user
    password: ${KAFKA_PASSWORD}
    ssl_ca_cert: /etc/ssl/ca.pem
```

### Kafka Sink

```yaml
type: kafka-sink
topics: [events]                # Rivven topics to consume from
consumer_group: kafka-producer
config:
  brokers: ["kafka1:9092"]
  topic: orders-replica         # Kafka topic (external destination)
  
  # Producer settings
  acks: all           # none, leader, all
  compression: lz4    # none, gzip, snappy, lz4, zstd
  batch_size_bytes: 16384
  linger_ms: 5
  request_timeout_ms: 30000
  retries: 3
  
  # Exactly-once (optional)
  idempotent: true
  transactional_id: rivven-producer-1
  
  # Message options
  key_field: order_id         # Extract key from event data
  include_headers: true       # Include Rivven metadata
  custom_headers:
    source: rivven
    environment: production
  
  # Security (optional)
  security:
    protocol: sasl_ssl
    mechanism: PLAIN
    username: producer
    password: ${KAFKA_PASSWORD}
```

## Security Protocols

| Protocol | Description |
|----------|-------------|
| `plaintext` | No encryption, no authentication |
| `ssl` | TLS encryption, no authentication |
| `sasl_plaintext` | No encryption, SASL authentication |
| `sasl_ssl` | TLS encryption + SASL authentication |

### SASL Mechanisms

| Mechanism | Description | Status |
|-----------|-------------|--------|
| `PLAIN` | Username/password (production use with SSL) | ✅ Supported |
| `SCRAM_SHA256` | SCRAM with SHA-256 | ⚠️ Falls back to PLAIN |
| `SCRAM_SHA512` | SCRAM with SHA-512 | ⚠️ Falls back to PLAIN |

> **Note**: rskafka v0.5 currently supports SASL PLAIN only. SCRAM mechanisms
> are configured for future compatibility but will use PLAIN authentication.
> For production security, always use `sasl_ssl` protocol.

## Metrics & Observability

### Source Metrics

The `KafkaSourceMetrics` struct provides lock-free observability:

```rust
let source = KafkaSource::new();
// ... consume messages ...

let snapshot = source.metrics().snapshot();
println!("Messages consumed: {}", snapshot.messages_consumed);
println!("Bytes consumed: {}", snapshot.bytes_consumed);
println!("Polls: {}", snapshot.polls);
println!("Empty polls: {}", snapshot.empty_polls);
println!("Errors: {}", snapshot.errors);
println!("Rebalances: {}", snapshot.rebalances);

// Computed metrics
println!("Avg poll latency: {:.2}ms", snapshot.avg_poll_latency_ms());
println!("Avg batch size: {:.1}", snapshot.avg_batch_size());
println!("Empty poll rate: {:.1}%", snapshot.empty_poll_rate() * 100.0);
println!("Throughput: {:.0} msg/s", snapshot.messages_per_second(elapsed));
```

### Sink Metrics

The `KafkaSinkMetrics` struct provides equivalent observability for producers:

```rust
let sink = KafkaSink::new();
// ... produce messages ...

let snapshot = sink.metrics().snapshot();
println!("Messages produced: {}", snapshot.messages_produced);
println!("Bytes produced: {}", snapshot.bytes_produced);
println!("Messages failed: {}", snapshot.messages_failed);
println!("Batches sent: {}", snapshot.batches_sent);
println!("Retries: {}", snapshot.retries);

// Computed metrics
println!("Avg produce latency: {:.2}ms", snapshot.avg_produce_latency_ms());
println!("Success rate: {:.1}%", snapshot.success_rate() * 100.0);
println!("Retry rate: {:.2}", snapshot.retry_rate());
```

### Batch Size Tracking

Both source and sink track batch size statistics using lock-free CAS operations:

```rust
// After consuming/producing
let snapshot = source.metrics().snapshot();
println!("Batch min: {}", snapshot.batch_size_min);
println!("Batch max: {}", snapshot.batch_size_max);
println!("Batch avg: {:.1}", snapshot.avg_batch_size());
```

### Prometheus Export

Export metrics in Prometheus text format for scraping:

```rust
let snapshot = source.metrics().snapshot();
let prometheus_output = snapshot.to_prometheus_format("rivven");

// Output:
// # HELP rivven_kafka_source_messages_consumed_total Total messages consumed
// # TYPE rivven_kafka_source_messages_consumed_total counter
// rivven_kafka_source_messages_consumed_total 10000
// # HELP rivven_kafka_source_bytes_consumed_total Total bytes consumed
// # TYPE rivven_kafka_source_bytes_consumed_total counter
// rivven_kafka_source_bytes_consumed_total 500000
// ...
```

### JSON Export

Metrics snapshots support JSON serialization via Serde:

```rust
let snapshot = source.metrics().snapshot();
let json = serde_json::to_string(&snapshot)?;
// {"messages_consumed":10000,"bytes_consumed":500000,...}

// Or for structured logging
tracing::info!(metrics = ?snapshot, "Kafka source metrics");
```

### Interval-Based Reporting

For periodic reporting, use `snapshot_and_reset()`:

```rust
loop {
    tokio::time::sleep(Duration::from_secs(60)).await;
    let interval_metrics = source.metrics().snapshot_and_reset();
    
    // Report only the last 60 seconds of activity
    report_to_monitoring(interval_metrics);
}
```

## Start Offset Options

| Mode | Description |
|------|-------------|
| `earliest` | Start from the first available message |
| `latest` | Start from the newest message (default) |
| `offset(N)` | Start from specific offset |
| `timestamp(ms)` | Start from timestamp (milliseconds) |

## Best Practices

### Migration from Kafka

1. **Start with latest offset** to avoid replaying history
2. **Enable idempotent producer** on the sink for exactly-once
3. **Monitor consumer lag** using metrics
4. **Use SASL_SSL** for production security

### High Throughput

1. **Increase batch_size_bytes** for better batching
2. **Use LZ4 compression** for best speed/ratio
3. **Tune linger_ms** to accumulate batches
4. **Monitor poll latency** for bottlenecks

### Exactly-Once Semantics

```yaml
# On sink side
idempotent: true
transactional_id: rivven-producer-unique-id
acks: all
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

### Authentication Failures

1. Verify credentials in configuration
2. Check SASL mechanism matches broker config
3. Ensure SSL certificates are valid and accessible

### Performance Issues

Monitor these metrics:
- `avg_poll_latency_ms` > 100ms indicates slow consumers
- `empty_poll_rate` > 0.5 indicates low message volume
- `retry_rate` > 0.1 indicates producer issues

## Integration Testing

The Kafka connector includes comprehensive integration tests using testcontainers:

```bash
# Run Kafka connector integration tests
cargo test -p rivven-integration-tests --test kafka_connector
```

### Test Coverage (21 tests)

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
| `test_kafka_source_metrics` | Lock-free metrics gathering |
| `test_kafka_source_shutdown` | Graceful shutdown signaling |
| `test_kafka_source_offset_modes` | Earliest/Latest/Offset start modes |
| `test_kafka_source_high_throughput` | 1000+ message performance |
| `test_kafka_source_multiple_partitions` | Multi-partition topic support |
| `test_kafka_source_empty_topic` | Empty topic handling |
| `test_kafka_source_invalid_broker` | Invalid broker error handling |
| `test_kafka_multiple_consumer_groups` | Independent consumer groups |

**Sink Connector Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_sink_check` | Sink `check()` validation |
| `test_kafka_sink_metrics` | Sink metrics tracking |
| `test_kafka_sink_compression` | Gzip/LZ4/Snappy/Zstd codecs |
| `test_kafka_sink_custom_headers` | Custom header injection |
| `test_kafka_sink_idempotent` | Idempotent producer config |

**Batch & Error Tests:**

| Test | Description |
|------|-------------|
| `test_kafka_batch_metrics` | Batch size min/max/avg tracking |
| `test_kafka_source_nonexistent_topic` | Missing topic handling |

### Hot Path Optimizations

Critical methods are annotated with `#[inline]` for optimal performance:

```rust
// Metrics methods are inlined for zero-cost abstraction
impl KafkaSourceMetricsSnapshot {
    #[inline]
    pub fn avg_poll_latency_ms(&self) -> f64 { /* ... */ }
    
    #[inline]
    pub fn avg_batch_size(&self) -> f64 { /* ... */ }
    
    #[inline]
    pub fn empty_poll_rate(&self) -> f64 { /* ... */ }
    
    #[inline]
    pub fn messages_per_second(&self, elapsed: Duration) -> f64 { /* ... */ }
}
```

### Graceful Shutdown

The `KafkaSource` supports graceful shutdown:

```rust
let source = KafkaSource::new(config.clone())?;

// Start consuming in background
let handle = tokio::spawn(async move {
    while let Some(_event) = source.read().await? {
        if source.is_shutting_down() {
            break;
        }
        // Process event...
    }
    Ok::<_, Error>(())
});

// Signal shutdown
source.shutdown();

// Wait for clean exit
handle.await?;
```
