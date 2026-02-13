//! Vector database connectors for Rivven Connect
//!
//! This module provides sink connectors for vector databases:
//! - **Qdrant** - Qdrant vector search engine via gRPC
//! - **Pinecone** - Pinecone managed vector database via gRPC (rustls)

#[cfg(feature = "qdrant")]
pub mod qdrant;

#[cfg(feature = "pinecone")]
pub mod pinecone;
#[cfg(feature = "pinecone")]
pub mod pinecone_client;

// Re-exports
#[cfg(feature = "qdrant")]
pub use qdrant::{QdrantSink, QdrantSinkConfig, QdrantSinkFactory};

#[cfg(feature = "pinecone")]
pub use pinecone::{PineconeSink, PineconeSinkConfig, PineconeSinkFactory};
