//! # Rivven
//!
//! High-performance distributed event streaming platform.
//!
//! This crate provides a unified API for the Rivven ecosystem, re-exporting
//! commonly used types from [`rivven_core`] and [`rivven_client`].
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use rivven::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create a client
//!     let client = Client::connect("localhost:9092").await?;
//!
//!     // Create a topic
//!     client.create_topic("my-topic", 3, 1).await?;
//!
//!     // Produce messages
//!     client.produce("my-topic", b"Hello, Rivven!").await?;
//!
//!     // Consume messages
//!     let messages = client.consume("my-topic", 0, 0, 100).await?;
//!     for msg in messages {
//!         println!("Received: {:?}", msg);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Flags
//!
//! - `client` (default): Include the async client for connecting to Rivven brokers
//! - `full`: Enable all features including compression, TLS, and metrics

#![doc(html_logo_url = "https://raw.githubusercontent.com/hupe1980/rivven/main/docs/logo.svg")]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

// Re-export core crate
pub use rivven_core as core;

/// Message types for event streaming.
pub mod message {
    pub use rivven_core::message::*;
}

/// Topic management.
pub mod topic {
    pub use rivven_core::topic::*;
}

/// Topic configuration.
pub mod topic_config {
    pub use rivven_core::topic_config::*;
}

/// Error types.
pub mod error {
    pub use rivven_core::error::*;
}

// Re-export client when feature is enabled
#[cfg(feature = "client")]
pub use rivven_client as client;

#[cfg(feature = "client")]
pub use rivven_client::Client;

/// Prelude module for convenient imports.
///
/// ```rust
/// use rivven::prelude::*;
/// ```
pub mod prelude {
    pub use rivven_core::message::Message;
    pub use rivven_core::topic::Topic;
    pub use rivven_core::topic_config::TopicConfig;

    #[cfg(feature = "client")]
    pub use rivven_client::{Client, ResilientClient, ResilientClientConfig};
}
