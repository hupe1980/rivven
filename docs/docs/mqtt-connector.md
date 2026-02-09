---
layout: docs
title: MQTT Connector
description: IoT and message streaming integration for Rivven
---

# MQTT Connector

The MQTT connector provides IoT and message streaming integration for sensor data,
device telemetry, and real-time event ingestion using a **pure Rust implementation**.

## Overview

| Component | Description |
|-----------|-------------|
| **MqttSource** | Subscribe to MQTT topics, stream to Rivven |

## Architecture

The MQTT connector uses [rumqttc](https://crates.io/crates/rumqttc), a pure Rust
MQTT client with zero C dependencies:

| Benefit | Description |
|---------|-------------|
| **Zero C deps** | No libmosquitto installation required |
| **Simple builds** | No C compiler or linker configuration |
| **Consistent** | Same behavior across all platforms |
| **Protocol support** | MQTT 3.1.1 and 5.0 compatible |

Enable with the `mqtt` feature:

```toml
rivven-connect = { version = "0.0.11", features = ["mqtt"] }
```

## Use Cases

- **IoT Data Ingestion**: Stream sensor and device data
- **Industrial Automation**: Collect SCADA and PLC data
- **Telemetry Processing**: Real-time metrics and events
- **Smart Home Integration**: Device state and commands

## Features

| Feature | Description |
|---------|-------------|
| Pure Rust client | Zero C dependencies via rumqttc |
| Lock-free metrics | Atomic counters with zero contention |
| Batch size tracking | Min/max/avg with CAS operations |
| Latency tracking | Message receive latency measurements |
| Prometheus export | `to_prometheus_format()` for scraping |
| JSON serialization | Serde derives on MetricsSnapshot |
| QoS levels | At most once, at least once, exactly once |
| TLS support | MQTT over TLS (mqtts://) |
| Wildcard topics | `+` (single level) and `#` (multi-level) |
| Last Will & Testament | Configurable LWT messages |
| Graceful shutdown | Clean disconnect signaling |
| Exponential backoff | Configurable retry with multiplier |

## Configuration

### MQTT Source

```yaml
type: mqtt-source
topic: iot-events                # Rivven topic (destination)
config:
  broker_url: "mqtt://broker.hivemq.com:1883"
  topics:                        # MQTT topics to subscribe to
    - "sensors/+/temperature"
    - "devices/#"
  client_id: rivven-mqtt-client
  qos: at_least_once             # at_most_once, at_least_once, exactly_once
  clean_session: true
  
  # Authentication (optional)
  auth:
    username: user
    password: ${MQTT_PASSWORD}
    
  # TLS (optional)
  tls:
    ca_cert: /etc/ssl/ca.pem
    client_cert: /etc/ssl/client.pem
    client_key: /etc/ssl/client.key
    
  # Last Will and Testament (optional)
  last_will:
    topic: "clients/rivven/status"
    payload: "disconnected"
    qos: at_least_once
    retain: true
    
  # Resilience & backoff
  retry_initial_ms: 100         # Initial retry delay
  retry_max_ms: 10000           # Maximum retry delay
  retry_multiplier: 2.0         # Exponential backoff multiplier
  empty_poll_delay_ms: 50       # Delay when no messages
```

## Topic Patterns

MQTT supports powerful wildcard subscriptions:

| Pattern | Description | Example |
|---------|-------------|---------|
| `+` | Single level wildcard | `sensors/+/temp` matches `sensors/room1/temp` |
| `#` | Multi-level wildcard | `devices/#` matches `devices/a/b/c` |
| Literal | Exact match | `home/kitchen/light` |

### Examples

```yaml
topics:
  # All temperature sensors
  - "sensors/+/temperature"
  
  # All device data
  - "devices/#"
  
  # Specific location
  - "building/floor1/room101/hvac"
  
  # All command topics
  - "commands/+/+/action"
```

## QoS Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | At Most Once | Fire-and-forget, no acknowledgment |
| 1 | At Least Once | Acknowledged delivery, possible duplicates |
| 2 | Exactly Once | Four-way handshake, guaranteed single delivery |

```yaml
# For IoT sensors (high volume, some loss acceptable)
qos: at_most_once

# For commands and critical data
qos: at_least_once

# For financial or transactional data
qos: exactly_once
```

## Metrics & Observability

### Source Metrics

The `MqttSourceMetrics` struct provides lock-free observability:

```rust
let source = MqttSource::new(config)?;
// ... receive messages ...

let snapshot = source.metrics().snapshot();
println!("Messages received: {}", snapshot.messages_received);
println!("Bytes received: {}", snapshot.bytes_received);
println!("Connections: {}", snapshot.connections);
println!("Disconnections: {}", snapshot.disconnections);
println!("Errors: {}", snapshot.errors);

// Computed metrics
println!("Avg receive latency: {:.2}ms", snapshot.avg_receive_latency_ms());
println!("Avg batch size: {:.1}", snapshot.avg_batch_size());
println!("Throughput: {:.0} msg/s", snapshot.messages_per_second(elapsed_secs));
println!("Error rate: {:.1}%", snapshot.error_rate_percent());
```

### Prometheus Export

Export metrics in Prometheus text format for scraping:

```rust
let snapshot = source.metrics().snapshot();
let prometheus_output = snapshot.to_prometheus_format("rivven");

// Output:
// # HELP rivven_mqtt_source_messages_received_total Total messages received
// # TYPE rivven_mqtt_source_messages_received_total counter
// rivven_mqtt_source_messages_received_total 10000
// # HELP rivven_mqtt_source_bytes_received_total Total bytes received
// # TYPE rivven_mqtt_source_bytes_received_total counter
// rivven_mqtt_source_bytes_received_total 500000
// ...
```

### JSON Export

Metrics snapshots support JSON serialization via Serde:

```rust
let snapshot = source.metrics().snapshot();
let json = serde_json::to_string(&snapshot)?;
// {"messages_received":10000,"bytes_received":500000,...}
```

## Graceful Shutdown

The `MqttSource` supports graceful shutdown with proper disconnect:

```rust
let source = MqttSource::new(config)?;

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

// Signal shutdown (sends DISCONNECT packet)
source.shutdown();

// Wait for clean exit
handle.await?;
```

## Integration Testing

The MQTT connector includes comprehensive integration tests using testcontainers:

```bash
# Run MQTT connector integration tests
cargo test -p rivven-integration-tests --test mqtt_connector
```

### Test Coverage (24 tests)

**Infrastructure Tests:**

| Test | Description |
|------|-------------|
| `test_mqtt_container_starts` | Container lifecycle and connectivity |
| `test_mqtt_publish_subscribe` | Basic pub/sub message flow |
| `test_mqtt_wildcard_subscription` | Wildcard topic matching (`+`, `#`) |

**Source Connector Tests:**

| Test | Description |
|------|-------------|
| `test_mqtt_source_check` | Source `check()` connectivity |
| `test_mqtt_source_discover` | Source `discover()` catalog discovery |
| `test_mqtt_source_read` | Source `read()` message streaming |
| `test_mqtt_source_metrics` | Lock-free metrics gathering |
| `test_mqtt_source_shutdown` | Graceful shutdown signaling |
| `test_mqtt_high_throughput` | 1000+ message performance |

**QoS Level Tests:**

| Test | Description |
|------|-------------|
| `test_mqtt_qos_0` | At most once delivery (fire-and-forget) |
| `test_mqtt_qos_1` | At least once delivery (acknowledged) |
| `test_mqtt_qos_2` | Exactly once delivery (assured) |

**Advanced Feature Tests:**

| Test | Description |
|------|-------------|
| `test_mqtt_last_will_config` | Last Will and Testament configuration |
| `test_mqtt_retained_message` | Retained message support |
| `test_mqtt_persistent_session` | clean_session=false support |
| `test_mqtt_multi_level_wildcard` | Multi-level wildcard (#) matching |
| `test_mqtt_single_level_wildcard_positions` | Single-level (+) wildcard positions |

**Edge Case & Resilience Tests:**

| Test | Description |
|------|-------------|
| `test_mqtt_special_topic_names` | Special character topic handling |
| `test_mqtt_binary_payload` | Binary/non-UTF8 payload support |
| `test_mqtt_large_payload` | Large message handling (64KB+) |
| `test_mqtt_connection_churn` | Multiple sequential connections |
| `test_mqtt_config_defaults` | Default configuration values |
| `test_mqtt_invalid_broker_url` | Invalid broker error handling |
| `test_mqtt_metrics_snapshot_and_reset` | Metrics snapshot functionality |

## Hot Path Optimizations

Critical methods are annotated with `#[inline]` for optimal performance:

```rust
impl MqttSourceMetricsSnapshot {
    #[inline]
    pub fn avg_receive_latency_ms(&self) -> f64 { /* ... */ }
    
    #[inline]
    pub fn avg_batch_size(&self) -> f64 { /* ... */ }
    
    #[inline]
    pub fn messages_per_second(&self, elapsed_secs: f64) -> f64 { /* ... */ }
    
    #[inline]
    pub fn error_rate_percent(&self) -> f64 { /* ... */ }
}
```

## Best Practices

### IoT Data Ingestion

1. **Use QoS 0** for high-volume sensor data where some loss is acceptable
2. **Use wildcards** to subscribe to device hierarchies efficiently
3. **Monitor connection stats** to detect network issues
4. **Configure LWT** to detect client disconnections

### High Throughput

1. **Use QoS 0** for maximum throughput
2. **Batch processing** with larger poll intervals
3. **Monitor receive latency** for bottleneck detection
4. **Use clean sessions** to avoid stored message replay

### Reliability

1. **Use QoS 1 or 2** for critical data
2. **Configure retry backoff** for resilience
3. **Monitor error rates** for early issue detection
4. **Use persistent sessions** for message recovery

## Troubleshooting

### Connection Issues

```bash
# Test TCP connection
nc -zv broker.hivemq.com 1883

# Test with mosquitto_sub
mosquitto_sub -h broker.hivemq.com -t "test/#" -v
```

### Authentication Failures

1. Verify credentials in configuration
2. Check broker ACL configuration
3. Ensure TLS certificates are valid

### Performance Issues

Monitor these metrics:
- `avg_receive_latency_ms` > 100ms indicates slow processing
- `error_rate_percent` > 1% indicates connection issues
- `disconnections` increasing rapidly indicates network problems

## Related Documentation

- [Kafka Connector](kafka-connector.md) - Apache Kafka integration
- [Connectors Overview](connectors.md) - All connector types
- [Testing](testing.md) - Integration test documentation
