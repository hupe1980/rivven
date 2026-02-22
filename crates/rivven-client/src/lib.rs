//! # rivven-client
//!
//! Native async Rust client library for [Rivven](https://rivven.hupe1980.github.io/rivven/),
//! the high-performance, single-binary event streaming platform.
//!
//! ## Features
//!
//! - **Async/Await**: Built on Tokio for high-performance async I/O
//! - **Connection Pooling**: Efficient connection management with [`ResilientClient`]
//! - **Circuit Breaker**: Automatic failure detection and recovery
//! - **Automatic Retries**: Exponential backoff with configurable limits
//! - **Multi-Server Failover**: Bootstrap server list with automatic failover
//! - **SCRAM-SHA-256 Authentication**: Secure password-based authentication
//! - **TLS Support**: Optional TLS encryption (requires `tls` feature)
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use rivven_client::{ResilientClient, ResilientClientConfig};
//!
//! # async fn example() -> rivven_client::Result<()> {
//! // Create a client with connection pooling and circuit breaker
//! let config = ResilientClientConfig::builder()
//!     .servers(vec!["127.0.0.1:9092".to_string()])
//!     .max_connections(10)
//!     .build()?;
//!
//! let client = ResilientClient::new(config).await?;
//!
//! // Publish a message
//! client.publish("my-topic", b"Hello, Rivven!").await?;
//!
//! // Consume messages
//! let messages = client.consume("my-topic", 0, 0, 100).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authentication
//!
//! The client supports SCRAM-SHA-256 authentication:
//!
//! ```rust,no_run
//! use rivven_client::{Client, AuthSession};
//!
//! # async fn example() -> rivven_client::Result<()> {
//! let mut client = Client::connect("127.0.0.1:9092").await?;
//!
//! // Authenticate with SCRAM-SHA-256
//! client.authenticate_scram("username", "password").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Resilient Client
//!
//! For production use, [`ResilientClient`] provides:
//!
//! - **Connection Pooling**: Reuses connections across requests
//! - **Circuit Breaker**: Stops sending requests to failing servers
//! - **Retries**: Automatic retries with exponential backoff
//! - **Failover**: Tries multiple bootstrap servers
//!
//! ```rust,ignore
//! use rivven_client::{ResilientClient, ResilientClientConfig};
//!
//! # async fn example() -> rivven_client::Result<()> {
//! let config = ResilientClientConfig::builder()
//!     .servers(vec![
//!         "broker1:9092".to_string(),
//!         "broker2:9092".to_string(),
//!         "broker3:9092".to_string(),
//!     ])
//!     .max_connections(20)
//!     .max_retries(5)
//!     .circuit_breaker_threshold(3)
//!     .circuit_breaker_timeout(std::time::Duration::from_secs(30))
//!     .build()?;
//!
//! let client = ResilientClient::new(config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Request Pipelining
//!
//! For maximum throughput, use [`PipelinedClient`] which allows multiple
//! in-flight requests over a single connection:
//!
//! ```rust,ignore
//! use rivven_client::{PipelinedClient, PipelineConfig};
//!
//! # async fn example() -> rivven_client::Result<()> {
//! // High-throughput configuration
//! let config = PipelineConfig::high_throughput();
//! let client = PipelinedClient::connect("127.0.0.1:9092", config).await?;
//!
//! // Send 1000 requests concurrently
//! let handles: Vec<_> = (0..1000)
//!     .map(|i| {
//!         let client = client.clone();
//!         tokio::spawn(async move {
//!             client.publish("topic", format!("msg-{}", i)).await
//!         })
//!     })
//!     .collect();
//!
//! for handle in handles {
//!     handle.await??;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! - `tls` - Enable TLS support via rustls
//!
//! ## High-Performance Producer
//!
//! For maximum throughput with all best practices, use [`Producer`]:
//!
//! ```rust,ignore
//! use rivven_client::{Producer, ProducerConfig};
//! use std::sync::Arc;
//!
//! let config = ProducerConfig::builder()
//!     .bootstrap_servers(vec!["localhost:9092".to_string()])
//!     .batch_size(16384)
//!     .linger_ms(5)
//!     .enable_idempotence(true)
//!     .build()?;
//!
//! let producer = Arc::new(Producer::new(config).await?);
//!
//! // Thread-safe sharing
//! for i in 0..1000 {
//!     let producer = Arc::clone(&producer);
//!     tokio::spawn(async move {
//!         producer.send("topic", format!("msg-{}", i)).await
//!     });
//! }
//! ```

pub mod client;
pub mod consumer;
pub mod error;
pub mod pipeline;
pub mod producer;
pub mod resilient;

pub use client::{AlterTopicConfigResult, AuthSession, Client, DeleteRecordsResult, ProducerState};
pub use consumer::{
    Consumer, ConsumerAuthConfig, ConsumerConfig, ConsumerConfigBuilder, ConsumerRecord,
};
pub use error::{Error, Result};
pub use pipeline::{
    PipelineAuthConfig, PipelineConfig, PipelineConfigBuilder, PipelineStatsSnapshot,
    PipelinedClient,
};
pub use producer::{
    CompressionType, Producer, ProducerAuthConfig, ProducerConfig, ProducerConfigBuilder,
    ProducerStatsSnapshot, RecordMetadata,
};
pub use resilient::{
    ResilientAuthConfig, ResilientClient, ResilientClientConfig, ResilientClientConfigBuilder,
};

// Re-export TLS configuration when available
#[cfg(feature = "tls")]
pub use rivven_core::tls::TlsConfig;

// Re-export protocol types from rivven-protocol
pub use rivven_protocol::{
    BrokerInfo, MessageData, PartitionMetadata, Request, Response, SchemaType, TopicConfigEntry,
    TopicMetadata, WireFormat, MAX_MESSAGE_SIZE, PROTOCOL_VERSION,
};
