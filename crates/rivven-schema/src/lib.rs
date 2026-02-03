//! # Rivven Schema Registry
//!
//! A high-performance, Confluent-compatible Schema Registry for the Rivven
//! event streaming platform.
//!
//! ## Features
//!
//! - **Confluent API Compatible**: Drop-in replacement for Confluent Schema Registry
//! - **Multiple Schema Formats**: Avro, JSON Schema, Protobuf
//! - **Schema Evolution**: Forward, backward, full, and transitive compatibility checking
//! - **Schema Fingerprinting**: MD5 and SHA-256 for deduplication
//! - **Storage Backends**: In-memory (dev), Broker-backed (production)
//! - **External Registries**: Confluent Schema Registry client, AWS Glue integration
//! - **High Performance**: In-memory caching with async operations
//! - **Production Ready**: K8s health checks, graceful shutdown
//!
//! ## Architecture
//!
//! The Schema Registry is responsible for:
//! - **Storing** schema definitions (as text strings)
//! - **Versioning** schemas with subjects
//! - **Checking compatibility** between schema versions
//! - **Serving** schemas via REST API
//!
//! The registry does **NOT** encode/decode data — that's the job of producers
//! and consumers. Use `rivven-connect` for codecs (Avro, Protobuf, JSON).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Rivven Schema Registry                    │
//! ├─────────────────────────────────────────────────────────────┤
//! │  REST API (Confluent-compatible)                            │
//! │  ├── POST /subjects/{subject}/versions                      │
//! │  ├── GET  /schemas/ids/{id}                                 │
//! │  ├── GET  /subjects/{subject}/versions/{version}            │
//! │  ├── POST /compatibility/subjects/{subject}/versions/{ver}  │
//! │  ├── GET  /config                                           │
//! │  └── GET  /health, /health/live, /health/ready              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Schema Management Layer                                     │
//! │  ├── Schema Parsing (syntax validation)                     │
//! │  ├── Compatibility Checking (all Confluent modes)           │
//! │  ├── Fingerprint/ID Generation (MD5, SHA-256)               │
//! │  └── Caching with deduplication                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Storage Backends                                            │
//! │  ├── Memory (development/testing)                           │
//! │  ├── Broker (production - durable, replicated)              │
//! │  └── External (Confluent, AWS Glue)                         │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Quick Start
//!
//! ### As a Library
//!
//! ```rust,ignore
//! use rivven_schema::{SchemaRegistry, RegistryConfig, SchemaType};
//!
//! // Create an in-memory registry
//! let config = RegistryConfig::memory();
//! let registry = SchemaRegistry::new(config).await?;
//!
//! // Register a schema
//! let avro_schema = r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
//! let schema_id = registry.register("user-value", SchemaType::Avro, avro_schema).await?;
//!
//! // Retrieve the schema
//! let schema = registry.get_by_id(schema_id).await?;
//! println!("Schema: {}", schema.schema);
//! ```
//!
//! ### As a Standalone Server
//!
//! ```bash
//! # Start with in-memory storage (development/testing)
//! rivven-schema serve --port 8081
//! ```
//!
//! For production, use broker-backed storage (requires `broker` feature):
//!
//! ```rust,ignore
//! use rivven_schema::{RegistryConfig, BrokerStorageConfig};
//!
//! let config = RegistryConfig::broker(
//!     BrokerStorageConfig::new("localhost:9092")
//!         .with_topic("_schemas")
//!         .with_replication_factor(3)
//! );
//! ```
//!
//! ## Compatibility Modes
//!
//! | Mode | Description |
//! |------|-------------|
//! | `BACKWARD` | New schema can read old data (default) |
//! | `BACKWARD_TRANSITIVE` | New schema can read all previous versions |
//! | `FORWARD` | Old schema can read new data |
//! | `FORWARD_TRANSITIVE` | All previous schemas can read new data |
//! | `FULL` | Both backward and forward compatible |
//! | `FULL_TRANSITIVE` | Full compatibility with all versions |
//! | `NONE` | No compatibility checking |
//!
//! ## Confluent Wire Format
//!
//! When using Avro with Schema Registry, data is encoded with a 5-byte header:
//!
//! ```text
//! [0x00][schema_id: 4 bytes big-endian][avro_binary_data]
//! ```
//!
//! This is compatible with Kafka producers/consumers using Confluent serializers.
//!
//! ## Best Practices
//!
//! 1. **Use Avro in Production**: Schema evolution with compatibility checking
//! 2. **Subject Naming**: Use `{topic}-key` and `{topic}-value` convention
//! 3. **Compatibility Level**: Start with `BACKWARD` for safe evolution
//! 4. **Versioning**: Never delete schemas, only deprecate
//!
//! ## Note on Codecs
//!
//! The Schema Registry does **not** include codecs for encoding/decoding data.
//! That's the responsibility of producers and consumers (connectors).
//! Use `rivven-connect` for Avro/Protobuf/JSON codecs with Confluent wire format.

#[cfg(feature = "server")]
pub mod auth;
pub mod compatibility;
pub mod config;
pub mod error;
pub mod fingerprint;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod registry;
#[cfg(feature = "server")]
pub mod server;
pub mod storage;
pub mod types;
pub mod validation;

// Re-exports for convenience
#[cfg(feature = "cedar")]
pub use auth::check_subject_permission_cedar;
#[cfg(feature = "server")]
pub use auth::{AuthConfig, AuthState, SchemaPermission};
pub use compatibility::{CompatibilityChecker, CompatibilityLevel, CompatibilityResult};
#[cfg(feature = "broker")]
pub use config::{BrokerAuthConfig, BrokerStorageConfig, BrokerTlsConfig};
pub use config::{RegistryConfig, StorageConfig};
pub use error::{error_codes, SchemaError, SchemaResult};
pub use fingerprint::SchemaFingerprint;
#[cfg(feature = "metrics")]
pub use metrics::{create_shared_metrics, MetricsConfig, RegistryMetrics, SharedMetrics};
pub use registry::SchemaRegistry;
#[cfg(feature = "server")]
pub use server::{RegistryMode, SchemaServer, ServerConfig, ServerState};
#[cfg(feature = "broker")]
pub use storage::BrokerStorage;
pub use storage::{MemoryStorage, Storage, StorageBackend};
pub use types::{
    Mode, Schema, SchemaContext, SchemaId, SchemaMetadata, SchemaReference, SchemaType,
    SchemaVersion, Subject, SubjectVersion, ValidationLevel, ValidationReport, ValidationResult,
    ValidationRule, ValidationRuleType, ValidationSummary, VersionState,
};
pub use validation::{ValidationEngine, ValidationEngineConfig};

// Re-export Cedar types when enabled
#[cfg(feature = "cedar")]
pub use rivven_core::{AuthzContext, CedarAuthorizer, RivvenAction, RivvenResource};
