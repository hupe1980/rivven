//! Vector database connectors for Rivven Connect
//!
//! This module provides sink connectors for vector databases:
//! - **Qdrant** - Qdrant vector search engine via gRPC
//! - **Pinecone** - Pinecone managed vector database via gRPC (rustls)
//! - **S3 Vectors** - Amazon S3 Vectors purpose-built vector storage

#[cfg(feature = "qdrant")]
pub mod qdrant;

#[cfg(feature = "pinecone")]
pub mod pinecone;
#[cfg(feature = "pinecone")]
pub mod pinecone_client;

#[cfg(feature = "s3-vectors")]
pub mod s3_vectors;

// Re-exports
#[cfg(feature = "qdrant")]
pub use qdrant::{QdrantSink, QdrantSinkConfig, QdrantSinkFactory};

#[cfg(feature = "pinecone")]
pub use pinecone::{PineconeSink, PineconeSinkConfig, PineconeSinkFactory};

#[cfg(feature = "s3-vectors")]
pub use s3_vectors::{S3VectorSink, S3VectorSinkConfig, S3VectorSinkFactory};
