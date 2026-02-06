//! Message queue connectors for Rivven Connect
//!
//! This module provides connectors for message queue systems:
//! - **Kafka** - Apache Kafka source/sink with lock-free metrics
//! - **MQTT** - MQTT broker source
//! - **SQS** - AWS SQS source
//! - **Pub/Sub** - Google Cloud Pub/Sub source
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

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "sqs")]
pub mod sqs;

#[cfg(feature = "pubsub")]
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
pub use sqs::{SqsSource, SqsSourceConfig, SqsSourceFactory};

#[cfg(feature = "pubsub")]
pub use pubsub::{PubSubSource, PubSubSourceConfig, PubSubSourceFactory};
