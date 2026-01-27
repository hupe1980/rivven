# rivven-queue

Message queue connectors for Rivven Connect.

## Overview

`rivven-queue` provides connectors for message queue systems, enabling bidirectional data flow between Rivven and external messaging platforms. This follows the grouped connector crates architecture where each crate focuses on a specific domain:

- **rivven-cdc** - Database CDC connectors (PostgreSQL, MySQL)
- **rivven-storage** - Object storage connectors (S3, GCS, Azure Blob)
- **rivven-warehouse** - Data warehouse connectors (BigQuery, Redshift, Snowflake)
- **rivven-queue** - Message queue connectors (Kafka, MQTT, SQS, etc.)

## Features

Enable connectors via Cargo features:

```toml
[dependencies]
rivven-queue = { version = "0.0.1", features = ["kafka", "mqtt"] }
```

### Available Features

| Feature | Description | Status |
|---------|-------------|--------|
| `kafka` | Apache Kafka source and sink | ✅ Implemented |
| `mqtt` | MQTT source for IoT data | ✅ Implemented |
| `sqs` | AWS SQS source | ✅ Implemented |
| `pubsub` | Google Cloud Pub/Sub source | ✅ Implemented |
| `rabbitmq` | RabbitMQ source and sink | 🔜 Planned |
| `nats` | NATS source and sink | 🔜 Planned |
| `full` | Enable all connectors | ✅ Available |

## Kafka Connector

The Kafka connector enables migration from Kafka to Rivven or bidirectional integration.

### Kafka Source

```yaml
name: kafka-source
connector_type:
  type: source
  config:
    type: Kafka
    bootstrap_servers: localhost:9092
    topic: my-topic
    consumer_group: my-group
```

### Kafka Sink

```yaml
name: kafka-sink
connector_type:
  type: sink
  config:
    type: Kafka
    bootstrap_servers: localhost:9092
    topic: my-topic
```

### Security Configuration

Supports PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, and SSL security protocols with SCRAM-SHA-256/512 authentication.

## MQTT Connector

The MQTT connector enables IoT data ingestion from MQTT brokers.

### MQTT Source

```yaml
name: mqtt-source
connector_type:
  type: source
  config:
    type: Mqtt
    broker_url: tcp://localhost:1883
    topics:
      - sensors/+/temperature
      - devices/#
    qos: 1
```

### Features

- QoS levels 0, 1, 2
- Wildcard subscriptions (`+`, `#`)
- TLS/SSL support
- Client authentication

## AWS SQS Connector

The SQS connector enables ingestion of messages from Amazon SQS queues.

### SQS Source

```yaml
name: sqs-source
connector_type:
  type: source
  config:
    type: Sqs
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789/my-queue
    region: us-east-1
    max_messages: 10
    wait_time_seconds: 20
    visibility_timeout: 30
```

### Features

- **Long Polling** — Configurable wait time (0-20 seconds)
- **FIFO Queues** — Message group ordering and deduplication
- **Message Attributes** — Custom and system attributes included in events
- **Flexible Authentication**:
  - Explicit credentials (access key + secret)
  - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
  - IAM roles (for EC2/EKS workloads)
  - AWS profile from credentials file
  - Assume role with external ID
- **LocalStack Support** — Custom endpoint URL for testing

### Authentication Examples

```yaml
# Explicit credentials
auth:
  access_key_id: AKIAIOSFODNN7EXAMPLE
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# AWS profile
auth:
  profile: production

# IAM role assumption
auth:
  role_arn: arn:aws:iam::123456789:role/MySqsRole
  external_id: my-external-id
```

## Google Cloud Pub/Sub Connector

The Pub/Sub connector enables subscription to Google Cloud Pub/Sub topics.

### Pub/Sub Source

```yaml
name: pubsub-source
connector_type:
  type: source
  config:
    type: PubSub
    project_id: my-gcp-project
    subscription: my-subscription
    max_messages: 100
    ack_deadline_seconds: 30
```

### Features

- **Streaming Pull** — Efficient message consumption for high-throughput
- **Exactly-Once Delivery** — With ordering key support
- **Message Filtering** — CEL expressions for server-side filtering
- **Dead Letter Topics** — Configurable failure handling
- **Flexible Authentication**:
  - Service account JSON key file
  - Application Default Credentials (ADC)
  - Workload Identity (for GKE)
  - Service account impersonation
- **Emulator Support** — Custom endpoint URL for testing

### Authentication Examples

```yaml
# Service account key file
auth:
  credentials_file: /path/to/service-account.json

# Application Default Credentials (default)
auth:
  use_adc: true

# Service account impersonation
auth:
  use_adc: true
  impersonate_service_account: target@project.iam.gserviceaccount.com
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
