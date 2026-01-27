//! # rivven-queue - Message Queue Connectors for Rivven Connect
//!
//! This crate provides connectors for message queue systems, enabling bidirectional
//! data flow between Rivven and external messaging platforms.
//!
//! ## Grouped Connector Crates Architecture
//!
//! Rivven Connect uses a modular architecture where connectors are organized into
//! domain-specific crates:
//!
//! - **rivven-cdc** - Database CDC connectors (PostgreSQL, MySQL)
//! - **rivven-storage** - Object storage connectors (S3, GCS, Azure Blob)
//! - **rivven-warehouse** - Data warehouse connectors (BigQuery, Redshift, Snowflake)
//! - **rivven-queue** - Message queue connectors (Kafka, MQTT, SQS, etc.)
//!
//! This separation keeps heavy dependencies isolated and allows users to include
//! only the connectors they need.
//!
//! ## Available Connectors
//!
//! Enable connectors via Cargo features:
//!
//! ```toml
//! [dependencies]
//! rivven-queue = { version = "0.0.1", features = ["kafka", "mqtt"] }
//! ```
//!
//! ### Kafka Connector (feature: `kafka`)
//!
//! Source and sink connectors for Apache Kafka integration:
//! - Migration from Kafka to Rivven
//! - Hybrid deployments
//! - Cross-datacenter replication
//!
//! ### MQTT Connector (feature: `mqtt`)
//!
//! Source connector for MQTT brokers:
//! - IoT device data ingestion
//! - Sensor data streaming
//! - Real-time telemetry
//!
//! ## Example
//!
//! ```ignore
//! use rivven_queue::kafka::{KafkaSource, KafkaSourceConfig};
//! use rivven_connect::traits::source::Source;
//!
//! let source = KafkaSource::new();
//! let config = KafkaSourceConfig {
//!     brokers: vec!["localhost:9092".to_string()],
//!     topic: "orders".to_string(),
//!     consumer_group: "rivven-migration".to_string(),
//!     ..Default::default()
//! };
//! ```

// Re-export common traits for convenience
pub use rivven_connect::error::{ConnectorError, Result};
pub use rivven_connect::traits::catalog::{Catalog, ConfiguredCatalog, Stream, SyncMode};
pub use rivven_connect::traits::event::SourceEvent;
pub use rivven_connect::traits::registry::{AnySink, AnySource, SinkFactory, SourceFactory};
pub use rivven_connect::traits::sink::{Sink, WriteResult};
pub use rivven_connect::traits::source::{CheckResult, Source};
pub use rivven_connect::traits::spec::ConnectorSpec;
pub use rivven_connect::traits::state::State;

// ============================================================================
// Feature-gated Connector Modules
// ============================================================================

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "mqtt")]
pub mod mqtt;

#[cfg(feature = "sqs")]
pub mod sqs;

#[cfg(feature = "pubsub")]
pub mod pubsub;

// ============================================================================
// Factory Registration
// ============================================================================

/// Register Kafka connectors with the given registries
#[cfg(feature = "kafka")]
pub fn register_kafka_connectors(
    sources: &mut rivven_connect::traits::registry::SourceRegistry,
    sinks: &mut rivven_connect::traits::registry::SinkRegistry,
) {
    use std::sync::Arc;
    sources.register("kafka-source", Arc::new(kafka::KafkaSourceFactory));
    sinks.register("kafka-sink", Arc::new(kafka::KafkaSinkFactory));
}

/// Register MQTT connectors with the given registry
#[cfg(feature = "mqtt")]
pub fn register_mqtt_connectors(sources: &mut rivven_connect::traits::registry::SourceRegistry) {
    use std::sync::Arc;
    sources.register("mqtt-source", Arc::new(mqtt::MqttSourceFactory));
}

/// Register SQS connectors with the given registry
#[cfg(feature = "sqs")]
pub fn register_sqs_connectors(sources: &mut rivven_connect::traits::registry::SourceRegistry) {
    use std::sync::Arc;
    sources.register("sqs-source", Arc::new(sqs::SqsSourceFactory));
}

/// Register Pub/Sub connectors with the given registry
#[cfg(feature = "pubsub")]
pub fn register_pubsub_connectors(sources: &mut rivven_connect::traits::registry::SourceRegistry) {
    use std::sync::Arc;
    sources.register("pubsub-source", Arc::new(pubsub::PubSubSourceFactory));
}

/// Register all enabled queue connectors with the given registries
#[allow(unused_variables)]
pub fn register_all_connectors(
    sources: &mut rivven_connect::traits::registry::SourceRegistry,
    sinks: &mut rivven_connect::traits::registry::SinkRegistry,
) {
    #[cfg(feature = "kafka")]
    register_kafka_connectors(sources, sinks);

    #[cfg(feature = "mqtt")]
    register_mqtt_connectors(sources);

    #[cfg(feature = "sqs")]
    register_sqs_connectors(sources);

    #[cfg(feature = "pubsub")]
    register_pubsub_connectors(sources);
}
