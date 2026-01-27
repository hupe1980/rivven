//! Schema Registry for Rivven Connect
//!
//! Provides schema management with two modes:
//! - **Embedded**: Schemas stored in Rivven topics (`_schemas`)
//! - **External**: Connect to Confluent-compatible schema registries
//!
//! # Features
//!
//! - Schema evolution with compatibility checking
//! - Support for JSON Schema, Avro, and Protobuf
//! - Native Avro serialization/deserialization
//! - Automatic schema inference from data
//! - Caching for performance
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::schema::{SchemaRegistry, SchemaRegistryConfig};
//!
//! // Embedded mode (schemas stored in Rivven topics)
//! let config = SchemaRegistryConfig::embedded();
//! let registry = SchemaRegistry::new(config).await?;
//!
//! // External mode (Confluent-compatible)
//! let config = SchemaRegistryConfig::external("http://localhost:8081");
//! let registry = SchemaRegistry::new(config).await?;
//!
//! // Register a schema
//! let schema_id = registry.register("users-value", json_schema).await?;
//!
//! // Get schema by ID
//! let schema = registry.get_by_id(schema_id).await?;
//! ```

pub mod avro;
mod client;
mod compatibility;
mod embedded;
mod external;
mod inference;
pub mod protobuf;
mod types;

pub use avro::{AvroCodec, AvroCompatibility, AvroError, AvroField, AvroSchema};
pub use client::{SchemaRegistry, SchemaRegistryClient};
pub use compatibility::{CompatibilityChecker, CompatibilityResult};
pub use embedded::{EmbeddedRegistry, EmbeddedRegistryConfig};
pub use external::{ExternalRegistry, ExternalRegistryConfig};
pub use inference::{infer_schema, infer_schema_from_samples, InferredSchema, SchemaInference};
pub use protobuf::{
    ProtobufCodec, ProtobufCompatibility, ProtobufError, ProtobufField, ProtobufSchema,
};
pub use types::{
    CompatibilityLevel, Schema, SchemaId, SchemaRegistryConfig, SchemaRegistryError,
    SchemaRegistryResult, SchemaType, SchemaVersion, Subject, SubjectVersion,
};
