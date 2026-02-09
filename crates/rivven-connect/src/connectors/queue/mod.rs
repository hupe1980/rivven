//! Message queue connectors for Rivven Connect
//!
//! This module provides connectors for message queue systems:
//! - **Kafka** - Apache Kafka source/sink with lock-free metrics
//! - **MQTT** - MQTT broker source
//! - **SQS** - AWS SQS source/sink with compression, circuit breaker, and lock-free metrics
//! - **Pub/Sub** - Google Cloud Pub/Sub source/sink with compression, flow control, and lock-free metrics
//!
//! # Features (Kafka)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Lock-free metrics | Atomic counters with zero contention |
//! | Batch size tracking | Min/max/avg with CAS operations |
//! | Latency tracking | Poll/produce latency measurements |
//! | Prometheus export | `to_prometheus_format()` for scraping |
//! | JSON serialization | Serde derives on MetricsSnapshot |
//!
//! # Features (SQS)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Batch sending | SendMessageBatch for up to 10 messages |
//! | Compression | Gzip and Zstd compression support |
//! | Circuit breaker | Automatic failure protection with recovery |
//! | Size validation | 256KB limit enforcement with configurable behavior |
//! | Content-based dedup | FIFO queue content-based deduplication |
//! | Body format options | JSON, raw string, or Base64 encoding |
//!
//! # Features (Pub/Sub)
//!
//! | Feature | Description |
//! |---------|-------------|
//! | Lock-free metrics | Atomic counters with zero contention |
//! | Batch acknowledgment | Configurable batch ack for throughput |
//! | Flow control | Backpressure with outstanding message/byte limits |
//! | Compression | Gzip and Zstd compression support |
//! | Size validation | 10MB limit enforcement with configurable behavior |
//! | Ordering keys | Ordered message delivery support |
//! | Body format options | JSON, raw string, or Base64 encoding |
//! | Prometheus export | `to_prometheus_format()` for scraping |

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "sqs")]
pub mod sqs;

// Pub/Sub connector is always available (runs in simulation mode)
pub mod pubsub;

// Re-exports
#[cfg(feature = "kafka")]
pub use kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSinkFactory, KafkaSinkMetrics, KafkaSinkMetricsSnapshot,
    KafkaSource, KafkaSourceConfig, KafkaSourceFactory, KafkaSourceMetrics,
    KafkaSourceMetricsSnapshot,
};

#[cfg(feature = "mqtt")]
pub use mqtt::{MqttSource, MqttSourceConfig, MqttSourceFactory};

#[cfg(feature = "sqs")]
pub use sqs::{
    OversizedMessageBehavior, SqsBodyFormat, SqsCompression, SqsSink, SqsSinkConfig,
    SqsSinkFactory, SqsSinkMetrics, SqsSinkMetricsSnapshot, SqsSource, SqsSourceConfig,
    SqsSourceFactory, SqsSourceMetrics, SqsSourceMetricsSnapshot,
};

// Pub/Sub is always exported (simulation mode)
pub use pubsub::{
    FlowControlConfig, GcpAuthConfig, PubSubBodyFormat, PubSubCompression, PubSubMessage,
    PubSubSink, PubSubSinkConfig, PubSubSinkFactory, PubSubSinkMetrics, PubSubSinkMetricsSnapshot,
    PubSubSource, PubSubSourceConfig, PubSubSourceFactory, PubSubSourceMetrics,
    PubSubSourceMetricsSnapshot,
};
