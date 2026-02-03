//! Schema Registry for Rivven Connect
//!
//! Provides schema management with multiple modes:
//! - **External**: Connect to Confluent-compatible schema registries (including rivven-schema)
//! - **AWS Glue**: Connect to AWS Glue Schema Registry
//! - **Disabled**: No schema registry (default)
//!
//! # Architecture
//!
//! The broker (rivvend) is **schema-agnostic** — it only handles raw bytes.
//! Schema management is handled externally:
//!
//! - **rivven-schema**: Standalone Confluent-compatible Schema Registry
//! - **Confluent Schema Registry**: External Kafka-compatible registry
//! - **AWS Glue Schema Registry**: AWS-native schema management
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
//! // External mode (Confluent-compatible, including rivven-schema)
//! let config = SchemaRegistryConfig::external("http://localhost:8081");
//! let registry = SchemaRegistry::new(config)?;
//!
//! // AWS Glue mode
//! let config = SchemaRegistryConfig::glue("us-east-1");
//! let registry = SchemaRegistry::new(config)?;
//!
//! // Register a schema
//! let schema_id = registry.register("users-value", json_schema).await?;
//!
//! // Get schema by ID
//! let schema = registry.get_by_id(schema_id).await?;
//! ```

pub mod avro;
mod client;
pub mod codec;
mod external;
pub mod glue;
mod inference;
pub mod protobuf;
mod schema_aware;
mod types;

pub use avro::{AvroCodec, AvroCompatibility, AvroError, AvroField, AvroSchema};
pub use client::{SchemaRegistry, SchemaRegistryClient};
pub use codec::{
    decode_schema_id_header, encode_schema_id_header, Codec, CodecConfig, CodecError, CodecResult,
    JsonCodec, SerializationFormat,
};
// CompatibilityResult re-exported from rivven-schema (the source of truth for compatibility)
pub use external::{ExternalRegistry, ExternalRegistryConfig};
pub use glue::{GlueConfig, GlueRegistry};
pub use inference::{infer_schema, infer_schema_from_samples, InferredSchema, SchemaInference};
pub use protobuf::{
    ProtobufCodec, ProtobufCompatibility, ProtobufError, ProtobufField, ProtobufSchema,
};
pub use rivven_schema::CompatibilityResult;
pub use schema_aware::{SchemaAwareConfig, SchemaAwareSink, SchemaAwareSource, SubjectStrategy};
pub use types::{
    CompatibilityLevel, ExternalConfig, RegistryAuth, RegistryTlsConfig, Schema, SchemaId,
    SchemaRegistryConfig, SchemaRegistryError, SchemaRegistryResult, SchemaType, SchemaVersion,
    Subject, SubjectVersion,
};
