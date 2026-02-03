//! Message queue connectors for Rivven Connect
//!
//! This module provides connectors for message queue systems:
//! - **Kafka** - Apache Kafka source/sink
//! - **MQTT** - MQTT broker source
//! - **SQS** - AWS SQS source
//! - **Pub/Sub** - Google Cloud Pub/Sub source

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
    KafkaSink, KafkaSinkConfig, KafkaSinkFactory, KafkaSource, KafkaSourceConfig,
    KafkaSourceFactory,
};

#[cfg(feature = "mqtt")]
pub use mqtt::{MqttSource, MqttSourceConfig, MqttSourceFactory};

#[cfg(feature = "sqs")]
pub use sqs::{SqsSource, SqsSourceConfig, SqsSourceFactory};

#[cfg(feature = "pubsub")]
pub use pubsub::{PubSubSource, PubSubSourceConfig, PubSubSourceFactory};
