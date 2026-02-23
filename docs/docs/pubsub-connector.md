# Google Cloud Pub/Sub Connector

The Google Cloud Pub/Sub connector provides both source and sink implementations for streaming messages between Rivven and Google Cloud Pub/Sub topics.

> **Note:** The Pub/Sub connector currently runs in **simulation mode** for testing and development. All message processing, metrics, and flow control are production-ready; only the actual GCP API calls are simulated. Real SDK integration is prepared for future releases.

## Features

| Feature | Description |
|---------|-------------|
| **Source + Sink** | Full bidirectional support |
| **Lock-free metrics** | Atomic counters with zero contention |
| **Flow control** | Configurable backpressure limits |
| **Batch acknowledgment** | Configurable batch ack for throughput |
| **Ordering keys** | Message ordering within groups |
| **Compression** | Gzip and Zstd body compression |
| **Prometheus export** | `to_prometheus_format()` for scraping |
| **GCP auth chain** | Service accounts, ADC, workload identity |
| **Size validation** | Handles 10MB Pub/Sub message limit |

## Quick Start

The Pub/Sub connector is always available (no feature flag required).

```toml
rivven-connect = { version = "0.0.20" }
```

### Source Configuration

```yaml
type: pubsub-source
config:
  project_id: my-gcp-project
  subscription_id: my-subscription
  max_messages: 100
  ack_deadline_seconds: 60
  auth:
    credentials_path: /path/to/service-account.json
```

### Sink Configuration

```yaml
type: pubsub-sink
config:
  project_id: my-gcp-project
  topic_id: my-topic
  batch_size: 100
  batch_timeout_ms: 1000
  compression: gzip
  auth:
    credentials_path: /path/to/service-account.json
```

## Source Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | string | **required** | GCP project ID |
| `subscription_id` | string | **required** | Pub/Sub subscription ID |
| `auth` | object | `{}` | Authentication config |
| `max_messages` | int | `100` | Messages per pull (1–1000) |
| `ack_deadline_seconds` | int | `60` | Acknowledgment deadline |
| `batch_ack_size` | int | `50` | Messages before batch ack |
| `flow_control.max_outstanding_messages` | int | `1000` | Max in-flight messages |
| `flow_control.max_outstanding_bytes` | int | `104857600` | Max in-flight bytes (100MB) |
| `retry_initial_ms` | int | `200` | Initial retry delay (ms) |
| `retry_max_ms` | int | `30000` | Maximum retry delay (ms) |
| `retry_multiplier` | float | `2.0` | Backoff multiplier |
| `include_attributes` | bool | `true` | Include message attributes |
| `checkpoint_interval` | int | `10` | State checkpoint every N polls |

## Sink Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_id` | string | **required** | GCP project ID |
| `topic_id` | string | **required** | Pub/Sub topic ID |
| `auth` | object | `{}` | Authentication config |
| `batch_size` | int | `100` | Messages per publish batch |
| `batch_timeout_ms` | int | `1000` | Max time before flush |
| `compression` | string | `none` | `none`, `gzip`, or `zstd` |
| `body_format` | string | `json` | `json`, `raw`, or `base64` |
| `ordering_key` | object | `null` | Ordering key configuration |
| `oversized_behavior` | string | `fail` | `fail`, `skip`, or `truncate` |
| `retry_initial_ms` | int | `200` | Initial retry delay (ms) |
| `retry_max_ms` | int | `30000` | Maximum retry delay (ms) |
| `retry_multiplier` | float | `2.0` | Backoff multiplier |

## Authentication

The connector supports multiple GCP authentication methods:

1. **Service account file** — `credentials_path` 
2. **Application Default Credentials (ADC)** — automatic in GCP environments
3. **Workload Identity** — automatic on GKE with configured service account
4. **Environment variable** — `GOOGLE_APPLICATION_CREDENTIALS`

```yaml
# Service account file
auth:
  credentials_path: /path/to/service-account.json

# Explicit credentials (not recommended for production)
auth:
  credentials_json: '{"type":"service_account",...}'

# Use ADC (default when no auth specified)
auth: {}
```

## Flow Control

The source connector implements backpressure via flow control:

```yaml
config:
  flow_control:
    max_outstanding_messages: 1000
    max_outstanding_bytes: 104857600  # 100MB
```

When limits are reached, the connector pauses pulling until in-flight messages are acknowledged.

## Ordering Keys

For ordered delivery within message groups:

```yaml
# Static ordering key
config:
  ordering_key:
    mode: static
    value: "my-ordering-key"

# Dynamic from message field
config:
  ordering_key:
    mode: field
    field_path: "partition_key"
    fallback: "default-key"

# Use stream name
config:
  ordering_key:
    mode: stream
```

## Compression

The sink supports body compression for bandwidth reduction:

```yaml
config:
  compression: gzip   # ~70% size reduction typical
  # or
  compression: zstd   # Higher ratio, faster decompression
```

Compression stats are tracked in metrics:
- `compression_savings_bytes` — Total bytes saved by compression

## Message Size Handling

Pub/Sub has a 10MB message limit. Configure behavior for oversized messages:

```yaml
config:
  oversized_behavior: skip  # Log warning and skip
  # or
  oversized_behavior: fail       # Fail the entire batch
  # or  
  oversized_behavior: truncate   # Truncate to fit
```

## Observability

### Source Metrics Snapshot

```rust
let source = PubSubSource::new();
// ... after running ...
let snapshot = source.metrics().snapshot();

println!("Messages: {}", snapshot.messages_received);
println!("Avg poll latency: {:.2}ms", snapshot.avg_poll_latency_ms());
println!("Empty poll rate: {:.1}%", snapshot.empty_poll_rate_percent());
println!("Avg batch size: {:.1}", snapshot.avg_batch_size());
```

### Sink Metrics Snapshot

```rust
let sink = PubSubSink::new();
// ... after running ...
let snapshot = sink.metrics().snapshot();

println!("Published: {}", snapshot.messages_published);
println!("Publish errors: {}", snapshot.publish_failures);
println!("Avg latency: {:.2}ms", snapshot.avg_publish_latency_ms());
```

### Prometheus Export

```rust
let prom = snapshot.to_prometheus_format("rivven");
// Outputs:
// # HELP rivven_pubsub_source_messages_received_total Total messages received
// # TYPE rivven_pubsub_source_messages_received_total counter
// rivven_pubsub_source_messages_received_total 1234
// ...
```

### Source Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_received` | counter | Total messages received |
| `bytes_received` | counter | Total body bytes received |
| `messages_acked` | counter | Messages acknowledged |
| `ack_failures` | counter | Failed acknowledgments |
| `messages_nacked` | counter | Messages NACKed (retry) |
| `polls` | counter | Total pull requests |
| `empty_polls` | counter | Pulls with no messages |
| `errors` | counter | API errors |
| `poll_latency_us` | counter | Cumulative poll latency (μs) |
| `ack_latency_us` | counter | Cumulative ack latency (μs) |
| `in_flight_messages` | gauge | Current in-flight count |
| `in_flight_bytes` | gauge | Current in-flight bytes |
| `flow_control_pauses` | counter | Flow control pause events |
| `duplicates_detected` | counter | Duplicate messages detected |

### Sink Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_published` | counter | Total messages published |
| `bytes_published` | counter | Total body bytes published |
| `publish_failures` | counter | Failed publish attempts |
| `batches_published` | counter | Total publish batches |
| `publish_latency_us` | counter | Cumulative publish latency (μs) |
| `batch_latency_us` | counter | Cumulative batch latency (μs) |
| `retries` | counter | Total retry attempts |
| `oversized_skipped` | counter | Oversized messages skipped |
| `oversized_truncated` | counter | Oversized messages truncated |
| `compression_savings_bytes` | counter | Bytes saved by compression |

### Derived Metrics

| Method | Description |
|--------|-------------|
| `avg_poll_latency_ms()` | Average poll latency in milliseconds |
| `avg_ack_latency_ms()` | Average ack latency in milliseconds |
| `empty_poll_rate_percent()` | Percentage of empty polls |
| `avg_batch_size()` | Average messages per non-empty poll |
| `avg_publish_latency_ms()` | Average publish latency (sink) |
| `avg_batch_size()` | Average batch size (sink) |
| `error_rate_percent()` | Publish error rate (sink) |

## Error Handling

The connector implements exponential backoff for transient errors:

```yaml
config:
  retry_initial_ms: 200     # First retry after 200ms
  retry_max_ms: 30000       # Cap at 30s
  retry_multiplier: 2.0     # Double each retry
```

Errors tracked include:
- Network failures
- Authentication errors (logged, not retried)
- Rate limiting (backoff + retry)
- Deadline exceeded

## Example: High-Throughput Source

```yaml
type: pubsub-source
config:
  project_id: my-project
  subscription_id: high-volume-sub
  max_messages: 1000
  batch_ack_size: 100
  ack_deadline_seconds: 600
  flow_control:
    max_outstanding_messages: 10000
    max_outstanding_bytes: 1073741824  # 1GB
  auth:
    credentials_path: /secrets/gcp-sa.json
```

## Example: Compressed Sink with Ordering

```yaml
type: pubsub-sink
config:
  project_id: my-project
  topic_id: events-topic
  batch_size: 500
  batch_timeout_ms: 500
  compression: zstd
  body_format: json
  ordering_key:
    mode: field
    field_path: "user_id"
  oversized_behavior: skip
  auth:
    credentials_path: /secrets/gcp-sa.json
```
