# SQS Source Connector

The SQS source connector consumes messages from Amazon Simple Queue Service (SQS) queues and streams them into Rivven as `SourceEvent` records.

## Features

| Feature | Description |
|---------|-------------|
| **AWS SDK v1** | Official `aws-sdk-sqs` for real SQS operations |
| **Lock-free metrics** | Atomic counters with zero contention |
| **Batch processing** | Receive up to 10 messages per poll |
| **Batch deletion** | `DeleteMessageBatch` for efficient cleanup |
| **Visibility timeout** | Configurable per-queue message hide time |
| **Long polling** | Native SQS long-poll (up to 20s wait) |
| **FIFO support** | Message group ordering + deduplication |
| **DLQ awareness** | Tracks approximate receive count for redrive |
| **Exponential backoff** | Configurable retry with multiplier |
| **Prometheus export** | `to_prometheus_format()` for scraping |
| **Full auth chain** | IAM roles, profiles, STS AssumeRole, explicit creds |
| **LocalStack support** | Custom endpoint URL override |

## Quick Start

### Enable the feature

```toml
rivven-connect = { version = "0.0.13", features = ["sqs"] }
```

### Configuration

```yaml
type: sqs-source
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  region: us-east-1
  max_messages: 10
  wait_time_seconds: 20
  visibility_timeout: 30
  delete_after_receive: true
  auth:
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

## Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queue_url` | string | **required** | Full SQS queue URL |
| `region` | string | `us-east-1` | AWS region |
| `auth` | object | `{}` | Authentication config (see below) |
| `max_messages` | int | `10` | Messages per poll (1–10) |
| `wait_time_seconds` | int | `20` | Long-poll wait (0–20s) |
| `visibility_timeout` | int | `30` | Message hide time (0–43200s) |
| `delete_after_receive` | bool | `true` | Delete messages after yield |
| `include_attributes` | bool | `true` | Include message attributes in metadata |
| `include_system_attributes` | bool | `false` | Include system attributes |
| `attribute_names` | list | `[]` | Filter specific attributes (empty = all) |
| `fifo_deduplication_scope` | string | `null` | `queue` or `message_group` |
| `endpoint_url` | string | `null` | Custom endpoint (LocalStack, ElasticMQ) |
| `retry_initial_ms` | int | `200` | Initial retry delay (ms) |
| `retry_max_ms` | int | `30000` | Maximum retry delay (ms) |
| `retry_multiplier` | float | `2.0` | Backoff multiplier |
| `checkpoint_interval` | int | `10` | State checkpoint every N polls |

## Authentication

The connector supports the full AWS credential chain:

1. **Explicit credentials** — `access_key_id` + `secret_access_key` (+ optional `session_token`)
2. **Environment variables** — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. **AWS profile** — `profile: my-profile` (reads `~/.aws/credentials`)
4. **IAM instance profile** — automatic on EC2/ECS/EKS
5. **STS AssumeRole** — `role_arn` + optional `external_id`

```yaml
# Explicit credentials
auth:
  access_key_id: AKIAIOSFODNN7EXAMPLE
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Named profile
auth:
  profile: production

# Cross-account role assumption
auth:
  role_arn: arn:aws:iam::123456789:role/rivven-reader
  external_id: my-external-id
```

## FIFO Queue Support

FIFO queues (`.fifo` suffix) get additional schema fields:

- `message_group_id` — Message group for ordered delivery
- `sequence_number` — SQS-assigned sequence number
- `message_deduplication_id` — Deduplication ID

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/events.fifo
  fifo_deduplication_scope: message_group
```

## Observability

### Metrics Snapshot

```rust
let source = SqsSource::new();
// ... after running ...
let snapshot = source.metrics().snapshot();

println!("Messages: {}", snapshot.messages_received);
println!("Avg poll latency: {:.2}ms", snapshot.avg_poll_latency_ms());
println!("Avg batch size: {:.1}", snapshot.avg_batch_size());
println!("Error rate: {:.2}%", snapshot.error_rate_percent());
```

### Prometheus Export

```rust
let prom = snapshot.to_prometheus_format("myapp");
// Outputs:
// # HELP myapp_sqs_source_messages_received_total Total messages received from SQS
// # TYPE myapp_sqs_source_messages_received_total counter
// myapp_sqs_source_messages_received_total 1234
// ...
```

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_received` | counter | Total messages received |
| `bytes_received` | counter | Total body bytes received |
| `messages_deleted` | counter | Messages deleted after processing |
| `delete_failures` | counter | Failed deletions (may redeliver) |
| `polls` | counter | Total receive polls |
| `empty_polls` | counter | Polls with no messages |
| `errors` | counter | API errors |
| `poll_latency_us` | counter | Cumulative poll latency (μs) |
| `batch_size_min` | gauge | Smallest batch received |
| `batch_size_max` | gauge | Largest batch received |

### Derived Metrics

| Method | Description |
|--------|-------------|
| `avg_poll_latency_ms()` | Average poll latency in milliseconds |
| `avg_batch_size()` | Average batch size per non-empty poll |
| `messages_per_second(elapsed)` | Message throughput |
| `bytes_per_second(elapsed)` | Bytes throughput |
| `error_rate_percent()` | Error rate as percentage |
| `empty_poll_rate_percent()` | Empty poll rate as percentage |

## Architecture

```
┌──────────────────────────────────────────────────┐
│  SqsSource                                       │
│                                                  │
│  ┌─────────────┐  ┌──────────────────────────┐   │
│  │ SqsClient   │  │ SqsSourceMetrics         │   │
│  │ (aws-sdk)   │  │ (lock-free AtomicU64)    │   │
│  └─────┬───────┘  └──────────────────────────┘   │
│        │                                         │
│  ReceiveMessage (long-poll, batch 1-10)          │
│        │                                         │
│  ┌─────▼───────┐                                 │
│  │ Convert to  │──▶ yield SourceEvent::record()  │
│  │ SourceEvent │                                 │
│  └─────┬───────┘                                 │
│        │                                         │
│  DeleteMessageBatch (if delete_after_receive)    │
│        │                                         │
│  State checkpoint (every N polls)                │
│        │                                         │
│  Exponential backoff on errors                   │
└──────────────────────────────────────────────────┘
```

## Fallback Mode

When compiled **without** the `sqs` feature, the connector operates in simulation mode — generating test messages for integration testing without AWS credentials. This follows the same pattern as the MQTT and Kafka connectors.

## Error Handling

- **Transient errors**: Exponential backoff (200ms → 30s default)
- **Delete failures**: Counted in metrics, messages will be redelivered by SQS after visibility timeout
- **Invalid credentials**: Detected at `check()` time via `GetQueueAttributes`
- **Timeout**: 10s timeout on `check()` connectivity test

---

# SQS Sink Connector

The SQS sink connector produces messages to Amazon Simple Queue Service (SQS) queues with high-performance batch sending.

## Features

| Feature | Description |
|---------|-------------|
| **AWS SDK v1** | Official `aws-sdk-sqs` for real SQS operations |
| **Lock-free metrics** | Atomic counters with zero contention |
| **Batch sending** | `SendMessageBatch` for up to 10 messages per request |
| **FIFO support** | Message group ID + deduplication ID for ordered delivery |
| **Content-based deduplication** | Let SQS generate dedup ID from message hash |
| **Message delay** | Configurable delay before message visibility |
| **Compression** | Gzip or Zstd compression for large payloads |
| **Body format options** | JSON, raw string, or Base64 encoding |
| **Size validation** | 256KB message limit enforcement with configurable behavior |
| **Circuit breaker** | Automatic failure protection with recovery |
| **Exponential backoff** | Configurable retry with multiplier |
| **Prometheus export** | `to_prometheus_format()` for scraping |
| **Full auth chain** | IAM roles, profiles, STS AssumeRole, explicit creds |
| **LocalStack support** | Custom endpoint URL override |

## Quick Start

### Enable the feature

```toml
rivven-connect = { version = "0.0.13", features = ["sqs"] }
```

### Configuration

```yaml
type: sqs-sink
topics: [events]                    # Rivven topics to consume from
consumer_group: sqs-producer
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  region: us-east-1
  batch_size: 10
  batch_timeout_ms: 1000
  include_metadata: true
  auth:
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

## Configuration Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queue_url` | string | **required** | Full SQS queue URL |
| `region` | string | `us-east-1` | AWS region |
| `auth` | object | `{}` | Authentication config (see Source section) |
| `batch_size` | int | `10` | Messages per batch (1–10) |
| `batch_timeout_ms` | int | `1000` | Max wait before flushing batch |
| `message_group_id` | string | `null` | Static message group ID for FIFO queues |
| `message_group_id_field` | string | `null` | Event field to use as message group ID |
| `deduplication_id_field` | string | `null` | Event field to use as deduplication ID |
| `content_based_deduplication` | bool | `false` | Let SQS generate dedup ID from message body |
| `delay_seconds` | int | `0` | Message delay (0–900s) |
| `include_metadata` | bool | `true` | Include event metadata as message attributes |
| `custom_attributes` | map | `{}` | Custom message attributes |
| `endpoint_url` | string | `null` | Custom endpoint (LocalStack, ElasticMQ) |
| `body_format` | string | `json` | Body format: `json`, `raw`, or `base64` |
| `compression` | string | `none` | Compression: `none`, `gzip`, or `zstd` |
| `oversized_behavior` | string | `fail` | 256KB limit: `fail`, `skip`, or `truncate` |
| `circuit_breaker_threshold` | int | `0` | Consecutive failures to trip (0 = disabled) |
| `circuit_breaker_recovery_secs` | int | `30` | Seconds before recovery attempt |
| `retry_initial_ms` | int | `200` | Initial retry delay (ms) |
| `retry_max_ms` | int | `30000` | Maximum retry delay (ms) |
| `retry_multiplier` | float | `2.0` | Backoff multiplier |
| `max_retries` | int | `3` | Maximum retries before failing |

## FIFO Queue Support

FIFO queues (`.fifo` suffix) require message group ID and support deduplication:

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/orders.fifo
  # Static group ID
  message_group_id: order-processing
  
  # Or dynamic group ID from event field
  message_group_id_field: customer_id
  
  # Optional: deduplication from event field
  deduplication_id_field: order_id
```

If no message group ID is configured, the stream name is used as the message group ID.

## Sink Observability

### Metrics Snapshot

```rust
let sink = SqsSink::new();
// ... after running ...
let snapshot = sink.metrics().snapshot();

println!("Messages sent: {}", snapshot.messages_sent);
println!("Avg send latency: {:.2}ms", snapshot.avg_send_latency_ms());
println!("Success rate: {:.2}%", snapshot.success_rate_percent());
```

### Prometheus Export

```rust
let prom = snapshot.to_prometheus_format("myapp");
// Outputs:
// # HELP myapp_sqs_sink_messages_sent_total Total messages sent to SQS
// # TYPE myapp_sqs_sink_messages_sent_total counter
// myapp_sqs_sink_messages_sent_total 1234
// ...
```

### Sink Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_sent` | counter | Total messages sent |
| `bytes_sent` | counter | Total body bytes sent |
| `messages_failed` | counter | Total failed messages |
| `batches_sent` | counter | Total batches sent |
| `partial_failures` | counter | Batches with partial failures |
| `errors` | counter | API errors |
| `retries` | counter | Total retries |
| `oversized_messages` | counter | Messages exceeding 256KB |
| `skipped_messages` | counter | Messages skipped (size/circuit breaker) |
| `compression_savings_bytes` | counter | Bytes saved by compression |
| `circuit_breaker_trips` | counter | Circuit breaker trip count |
| `send_latency_us` | counter | Cumulative send latency (μs) |
| `batch_size_min` | gauge | Smallest batch sent |
| `batch_size_max` | gauge | Largest batch sent |

### Derived Sink Metrics

| Method | Description |
|--------|-------------|
| `avg_send_latency_ms()` | Average send latency in milliseconds |
| `avg_batch_size()` | Average batch size |
| `messages_per_second(elapsed)` | Message throughput |
| `bytes_per_second(elapsed)` | Bytes throughput |
| `success_rate_percent()` | Success rate as percentage |
| `error_rate_percent()` | Error rate as percentage |
| `compression_ratio()` | Compression ratio (1.0 = no compression) |

## Compression

Enable compression to reduce message size and costs:

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  compression: gzip   # or 'zstd' for better ratio
```

When compression is enabled:
- Body is compressed with the selected codec
- Result is base64-encoded for SQS transport
- `rivven.compression` message attribute is added
- Metrics track `compression_savings_bytes`

| Codec | Best For | Trade-off |
|-------|----------|-----------|
| `gzip` | Text/JSON data | Good compatibility, moderate speed |
| `zstd` | Large payloads | Best ratio, fastest decompression |

## Oversized Message Handling

SQS has a 256KB message size limit. Configure behavior for oversized messages:

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  oversized_behavior: skip   # Options: fail, skip, truncate
```

| Behavior | Description |
|----------|-------------|
| `fail` | Increment `messages_failed` counter, report error |
| `skip` | Silently drop message, increment `skipped_messages` |
| `truncate` | Truncate message body to fit (lossy) |

## Circuit Breaker

Protect against cascading failures with the circuit breaker:

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  circuit_breaker_threshold: 5      # Trip after 5 consecutive failures
  circuit_breaker_recovery_secs: 60 # Wait 60s before recovery attempt
```

When tripped:
- New messages are skipped and counted in `skipped_messages`
- After recovery period, one batch is attempted
- Success resets the circuit breaker
- Failure re-trips and restarts recovery timer

## Body Format Options

Control how event data is serialized:

```yaml
config:
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
  body_format: raw  # Options: json, raw, base64
```

| Format | Description |
|--------|-------------|
| `json` | JSON-serialize event data (default) |
| `raw` | Use string value directly (for string events) |
| `base64` | Base64-encode JSON for binary-safe transport |

## Sink Architecture

```
┌──────────────────────────────────────────────────┐
│  SqsSink                                         │
│                                                  │
│  ┌─────────────┐  ┌──────────────────────────┐   │
│  │ SqsClient   │  │ SqsSinkMetrics           │   │
│  │ (aws-sdk)   │  │ (lock-free AtomicU64)    │   │
│  └─────┬───────┘  └──────────────────────────┘   │
│        │                                         │
│  SourceEvent stream ──▶ Circuit breaker check    │
│        │                                         │
│  Format body (json/raw/base64)                   │
│        │                                         │
│  Apply compression (gzip/zstd)                   │
│        │                                         │
│  Check size limit (256KB)                        │
│        │                                         │
│  ┌─────▼───────┐                                 │
│  │ Batch       │  (batch_size or timeout)        │
│  │ accumulator │                                 │
│  └─────┬───────┘                                 │
│        │                                         │
│  SendMessageBatch (up to 10 messages)            │
│        │                                         │
│  Handle partial failures                         │
│        │                                         │
│  Retry with exponential backoff                  │
└──────────────────────────────────────────────────┘
```

## Error Handling

- **Transient errors**: Exponential backoff with configurable max retries
- **Partial failures**: Individual message failures tracked in metrics
- **Invalid credentials**: Detected at `check()` time
- **Batch failures**: Entire batch retried before giving up
- **Circuit breaker**: Trips after consecutive failures, recovers automatically
