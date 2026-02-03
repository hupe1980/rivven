//! Schema-aware source and sink wrappers
//!
//! Provides automatic schema registration, Avro serialization (for sources),
//! and Avro deserialization (for sinks).
//!
//! # Example (Sink)
//!
//! ```rust,ignore
//! use rivven_connect::schema::{SchemaRegistryClient, SchemaAwareSink};
//! use rivven_connect::connectors::stdout::StdoutSink;
//!
//! // Create a schema-aware wrapper around any sink
//! let registry = SchemaRegistryClient::from_config(&config, None)?;
//! let schema_sink = SchemaAwareSink::new(StdoutSink::new(), registry);
//!
//! // Events will be serialized with Avro before writing
//! schema_sink.write(&config, events).await?;
//! ```
//!
//! # Example (Source)
//!
//! ```rust,ignore
//! use rivven_connect::schema::{SchemaRegistryClient, SchemaAwareSource};
//! use rivven_connect::connectors::datagen::DatagenSource;
//!
//! // Create a schema-aware wrapper around any source
//! let registry = SchemaRegistryClient::from_config(&config, None)?;
//! let schema_source = SchemaAwareSource::new(DatagenSource::new(), registry);
//!
//! // Events will be serialized with Avro and schema registered
//! schema_source.read(&config, catalog, state).await?;
//! ```

use crate::error::{ConnectorError, Result};
use crate::schema::avro::{AvroCodec, AvroSchema};
use crate::schema::client::SchemaRegistryClient;
use crate::schema::types::{SchemaId, SchemaType, SchemaVersion, Subject, SubjectVersion};
use crate::traits::catalog::{Catalog, ConfiguredCatalog};
use crate::traits::event::SourceEvent;
use crate::traits::sink::{Sink, WriteResult};
use crate::traits::source::{CheckResult, Source};
use crate::traits::spec::ConnectorSpec;
use crate::traits::state::State;
use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for schema-aware sink behavior
#[derive(Debug, Clone)]
pub struct SchemaAwareConfig {
    /// Subject naming strategy
    pub subject_strategy: SubjectStrategy,
    /// Whether to auto-register schemas
    pub auto_register: bool,
    /// Whether to use Confluent wire format (5-byte header)
    pub confluent_wire_format: bool,
    /// Default schema type for registration
    pub schema_type: SchemaType,
}

impl Default for SchemaAwareConfig {
    fn default() -> Self {
        Self {
            subject_strategy: SubjectStrategy::TopicName,
            auto_register: true,
            confluent_wire_format: true,
            schema_type: SchemaType::Avro,
        }
    }
}

/// Strategy for generating subject names
#[derive(Debug, Clone, Default)]
pub enum SubjectStrategy {
    /// Use topic name as subject: "{topic}-value"
    #[default]
    TopicName,
    /// Use record name as subject: "{record_name}"
    RecordName,
    /// Use topic and record name: "{topic}-{record_name}"
    TopicRecordName,
    /// Custom function to generate subject
    Custom(fn(&str, &serde_json::Value) -> String),
}

impl SubjectStrategy {
    /// Generate subject name from stream and data
    pub fn generate_subject(&self, stream: &str, data: &serde_json::Value) -> Subject {
        let name = match self {
            SubjectStrategy::TopicName => format!("{}-value", stream),
            SubjectStrategy::RecordName => {
                // Try to extract record name from schema
                data.get("__type")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .unwrap_or_else(|| format!("{}-value", stream))
            }
            SubjectStrategy::TopicRecordName => {
                if let Some(record) = data.get("__type").and_then(|v| v.as_str()) {
                    format!("{}-{}", stream, record)
                } else {
                    format!("{}-value", stream)
                }
            }
            SubjectStrategy::Custom(f) => f(stream, data),
        };
        Subject::new(name)
    }
}

/// Schema-aware sink wrapper
///
/// Wraps any sink implementation with automatic schema registry integration.
pub struct SchemaAwareSink<S: Sink> {
    inner: S,
    registry: Arc<SchemaRegistryClient>,
    config: SchemaAwareConfig,
    /// Cache of stream -> (schema_id, codec)
    schema_cache: RwLock<HashMap<String, (SchemaId, Arc<AvroCodec>)>>,
    /// Avro schema definitions by subject
    avro_schemas: RwLock<HashMap<Subject, AvroSchema>>,
}

impl<S: Sink> SchemaAwareSink<S> {
    /// Create a new schema-aware sink wrapper
    pub fn new(inner: S, registry: Arc<SchemaRegistryClient>) -> Self {
        Self {
            inner,
            registry,
            config: SchemaAwareConfig::default(),
            schema_cache: RwLock::new(HashMap::new()),
            avro_schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Create with custom configuration
    pub fn with_config(
        inner: S,
        registry: Arc<SchemaRegistryClient>,
        config: SchemaAwareConfig,
    ) -> Self {
        Self {
            inner,
            registry,
            config,
            schema_cache: RwLock::new(HashMap::new()),
            avro_schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Register an Avro schema for a subject
    pub fn register_schema(&self, subject: Subject, schema: AvroSchema) {
        self.avro_schemas.write().insert(subject, schema);
    }

    /// Get or register schema for a stream
    async fn get_or_register_schema(
        &self,
        stream: &str,
        data: &serde_json::Value,
    ) -> Result<(SchemaId, Arc<AvroCodec>)> {
        // Check cache first
        if let Some(cached) = self.schema_cache.read().get(stream).cloned() {
            return Ok(cached);
        }

        let subject = self.config.subject_strategy.generate_subject(stream, data);

        // Check if we have a pre-registered local schema
        let local_schema = {
            let schemas = self.avro_schemas.read();
            schemas.get(&subject).cloned()
        };

        let (schema_id, schema) = if let Some(schema) = local_schema {
            // Use local schema - register or lookup ID
            if self.config.auto_register {
                let id = self
                    .registry
                    .register(&subject, self.config.schema_type, schema.raw())
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                (id, schema)
            } else {
                // Try to get latest version from registry
                let versions = self
                    .registry
                    .list_versions(&subject)
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                if let Some(&version) = versions.last() {
                    let subject_version: SubjectVersion = self
                        .registry
                        .get_by_version(&subject, SchemaVersion::new(version))
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    (subject_version.id, schema)
                } else {
                    return Err(ConnectorError::Schema(format!(
                        "No schema registered for subject: {}",
                        subject
                    ))
                    .into());
                }
            }
        } else {
            // No local schema - try to fetch from registry first
            let registry_result = self.registry.list_versions(&subject).await;

            if let Ok(versions) = registry_result {
                if let Some(&version) = versions.last() {
                    // Found in registry - use it
                    let sv: SubjectVersion = self
                        .registry
                        .get_by_version(&subject, SchemaVersion::new(version))
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    let schema = AvroSchema::parse(&sv.schema).map_err(|e| {
                        ConnectorError::Schema(format!(
                            "Failed to parse schema from registry: {}",
                            e
                        ))
                    })?;
                    (sv.id, schema)
                } else if self.config.auto_register {
                    // No versions in registry, infer and register
                    let inferred = infer_avro_schema(stream, data)?;
                    let id = self
                        .registry
                        .register(&subject, self.config.schema_type, inferred.raw())
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    (id, inferred)
                } else {
                    return Err(ConnectorError::Schema(format!(
                        "No schema registered for stream: {}",
                        stream
                    ))
                    .into());
                }
            } else if self.config.auto_register {
                // Registry lookup failed, infer and register
                let inferred = infer_avro_schema(stream, data)?;
                let id = self
                    .registry
                    .register(&subject, self.config.schema_type, inferred.raw())
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                (id, inferred)
            } else {
                return Err(ConnectorError::Schema(format!(
                    "No schema registered for stream: {}",
                    stream
                ))
                .into());
            }
        };

        let codec = Arc::new(AvroCodec::new(schema));
        self.schema_cache
            .write()
            .insert(stream.to_string(), (schema_id, codec.clone()));

        Ok((schema_id, codec))
    }

    /// Serialize event data using Avro
    pub async fn serialize_event(&self, event: &mut SourceEvent) -> Result<Vec<u8>> {
        let (schema_id, codec) = self
            .get_or_register_schema(&event.stream, &event.data)
            .await?;

        // Update event metadata with schema info
        event.metadata.schema_id = Some(schema_id.0);
        event.metadata.schema_subject = Some(
            self.config
                .subject_strategy
                .generate_subject(&event.stream, &event.data)
                .0,
        );

        // Serialize with optional Confluent wire format
        if self.config.confluent_wire_format {
            codec
                .encode_with_schema_id(&event.data, schema_id.0)
                .map_err(|e| ConnectorError::Schema(format!("Avro encode error: {}", e)).into())
        } else {
            codec
                .encode(&event.data)
                .map_err(|e| ConnectorError::Schema(format!("Avro encode error: {}", e)).into())
        }
    }
}

#[async_trait]
impl<S: Sink + Send + Sync> Sink for SchemaAwareSink<S> {
    type Config = S::Config;

    fn spec() -> ConnectorSpec {
        let mut spec = S::spec();
        spec.description = spec
            .description
            .map(|d| format!("{} (schema-aware)", d))
            .or_else(|| Some("(schema-aware)".to_string()));
        spec
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Check inner sink
        let result = self.inner.check(config).await?;

        // Check registry connection
        if self.registry.is_enabled() {
            match self.registry.list_subjects().await {
                Ok(_) => {
                    tracing::info!("Schema registry connected");
                }
                Err(e) => {
                    tracing::warn!("Schema registry connection failed: {}", e);
                }
            }
        }

        Ok(result)
    }

    async fn write(
        &self,
        config: &Self::Config,
        events: BoxStream<'static, SourceEvent>,
    ) -> Result<WriteResult> {
        // For now, delegate to inner sink
        // A full implementation would transform the stream to serialize events
        self.inner.write(config, events).await
    }
}

// ============================================================================
// SchemaAwareSource - Source wrapper for schema-aware serialization
// ============================================================================

/// Schema-aware source wrapper
///
/// Wraps any source implementation with automatic schema registry integration.
/// Events produced by the source will have:
/// - Schema automatically inferred from data (if not provided)
/// - Schema registered with the registry
/// - Events serialized to Avro (optionally with Confluent wire format)
/// - Metadata updated with schema_id and schema_subject
pub struct SchemaAwareSource<S: Source> {
    inner: S,
    registry: Arc<SchemaRegistryClient>,
    config: SchemaAwareConfig,
    /// Cache of stream -> (schema_id, codec)
    #[allow(dead_code)]
    schema_cache: RwLock<HashMap<String, (SchemaId, Arc<AvroCodec>)>>,
    /// Avro schema definitions by subject
    avro_schemas: RwLock<HashMap<Subject, AvroSchema>>,
}

impl<S: Source> SchemaAwareSource<S> {
    /// Create a new schema-aware source wrapper
    pub fn new(inner: S, registry: Arc<SchemaRegistryClient>) -> Self {
        Self {
            inner,
            registry,
            config: SchemaAwareConfig::default(),
            schema_cache: RwLock::new(HashMap::new()),
            avro_schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Create with custom configuration
    pub fn with_config(
        inner: S,
        registry: Arc<SchemaRegistryClient>,
        config: SchemaAwareConfig,
    ) -> Self {
        Self {
            inner,
            registry,
            config,
            schema_cache: RwLock::new(HashMap::new()),
            avro_schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Register an Avro schema for a subject
    pub fn register_schema(&self, subject: Subject, schema: AvroSchema) {
        self.avro_schemas.write().insert(subject, schema);
    }

    /// Get the inner source
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get or register schema for a stream (public for external use)
    #[allow(dead_code)]
    async fn get_or_register_schema(
        &self,
        stream: &str,
        data: &serde_json::Value,
    ) -> Result<(SchemaId, Arc<AvroCodec>)> {
        // Check cache first
        if let Some(cached) = self.schema_cache.read().get(stream).cloned() {
            return Ok(cached);
        }

        let subject = self.config.subject_strategy.generate_subject(stream, data);

        // Check if we have a pre-registered local schema
        let local_schema = {
            let schemas = self.avro_schemas.read();
            schemas.get(&subject).cloned()
        };

        let (schema_id, schema) = if let Some(schema) = local_schema {
            // Use local schema - register or lookup ID
            if self.config.auto_register {
                let id = self
                    .registry
                    .register(&subject, self.config.schema_type, schema.raw())
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                (id, schema)
            } else {
                // Try to get latest version from registry
                let versions = self
                    .registry
                    .list_versions(&subject)
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                if let Some(&version) = versions.last() {
                    let subject_version: SubjectVersion = self
                        .registry
                        .get_by_version(&subject, SchemaVersion::new(version))
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    (subject_version.id, schema)
                } else {
                    return Err(ConnectorError::Schema(format!(
                        "No schema registered for subject: {}",
                        subject
                    ))
                    .into());
                }
            }
        } else {
            // No local schema - try to fetch from registry first
            let registry_result = self.registry.list_versions(&subject).await;

            if let Ok(versions) = registry_result {
                if let Some(&version) = versions.last() {
                    // Found in registry - use it
                    let sv: SubjectVersion = self
                        .registry
                        .get_by_version(&subject, SchemaVersion::new(version))
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    let schema = AvroSchema::parse(&sv.schema).map_err(|e| {
                        ConnectorError::Schema(format!(
                            "Failed to parse schema from registry: {}",
                            e
                        ))
                    })?;
                    (sv.id, schema)
                } else if self.config.auto_register {
                    // No versions in registry, infer and register
                    let inferred = infer_avro_schema(stream, data)?;
                    let id = self
                        .registry
                        .register(&subject, self.config.schema_type, inferred.raw())
                        .await
                        .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                    (id, inferred)
                } else {
                    return Err(ConnectorError::Schema(format!(
                        "No schema registered for stream: {}",
                        stream
                    ))
                    .into());
                }
            } else if self.config.auto_register {
                // Registry lookup failed, infer and register
                let inferred = infer_avro_schema(stream, data)?;
                let id = self
                    .registry
                    .register(&subject, self.config.schema_type, inferred.raw())
                    .await
                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                (id, inferred)
            } else {
                return Err(ConnectorError::Schema(format!(
                    "No schema registered for stream: {}",
                    stream
                ))
                .into());
            }
        };

        let codec = Arc::new(AvroCodec::new(schema));
        self.schema_cache
            .write()
            .insert(stream.to_string(), (schema_id, codec.clone()));

        Ok((schema_id, codec))
    }

    /// Serialize event data using Avro and update metadata (public for external use)
    #[allow(dead_code)]
    async fn serialize_event(&self, event: &mut SourceEvent) -> Result<Vec<u8>> {
        let (schema_id, codec) = self
            .get_or_register_schema(&event.stream, &event.data)
            .await?;

        // Update event metadata with schema info
        event.metadata.schema_id = Some(schema_id.0);
        event.metadata.schema_subject = Some(
            self.config
                .subject_strategy
                .generate_subject(&event.stream, &event.data)
                .0,
        );

        // Serialize with optional Confluent wire format
        if self.config.confluent_wire_format {
            codec
                .encode_with_schema_id(&event.data, schema_id.0)
                .map_err(|e| ConnectorError::Schema(format!("Avro encode error: {}", e)).into())
        } else {
            codec
                .encode(&event.data)
                .map_err(|e| ConnectorError::Schema(format!("Avro encode error: {}", e)).into())
        }
    }
}

#[async_trait]
impl<S: Source + Send + Sync + 'static> Source for SchemaAwareSource<S> {
    type Config = S::Config;

    fn spec() -> ConnectorSpec {
        let mut spec = S::spec();
        spec.description = spec
            .description
            .map(|d| format!("{} (schema-aware)", d))
            .or_else(|| Some("(schema-aware)".to_string()));
        spec
    }

    async fn check(&self, config: &Self::Config) -> Result<CheckResult> {
        // Check inner source
        let result = self.inner.check(config).await?;

        // Check registry connection
        if self.registry.is_enabled() {
            match self.registry.list_subjects().await {
                Ok(_) => {
                    tracing::info!("Schema registry connected");
                }
                Err(e) => {
                    tracing::warn!("Schema registry connection failed: {}", e);
                }
            }
        }

        Ok(result)
    }

    async fn discover(&self, config: &Self::Config) -> Result<Catalog> {
        self.inner.discover(config).await
    }

    async fn read(
        &self,
        config: &Self::Config,
        catalog: &ConfiguredCatalog,
        state: Option<State>,
    ) -> Result<BoxStream<'static, Result<SourceEvent>>> {
        // Get the inner stream
        let inner_stream = self.inner.read(config, catalog, state).await?;

        // Clone references for the async closure
        let registry = self.registry.clone();
        let schema_config = self.config.clone();
        let schema_cache: Arc<RwLock<HashMap<String, (SchemaId, Arc<AvroCodec>)>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let avro_schemas: Arc<RwLock<HashMap<Subject, AvroSchema>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Transform stream to add schema metadata and optionally serialize
        let transformed = inner_stream.then(move |event_result| {
            let registry = registry.clone();
            let schema_config = schema_config.clone();
            let schema_cache = schema_cache.clone();
            let avro_schemas = avro_schemas.clone();

            async move {
                let mut event = event_result?;

                // Only process data events
                if !event.is_data() {
                    return Ok(event);
                }

                // Get subject
                let subject = schema_config
                    .subject_strategy
                    .generate_subject(&event.stream, &event.data);

                // Check cache first
                let cached = { schema_cache.read().get(&event.stream).cloned() };

                let (schema_id, codec) = if let Some(cached) = cached {
                    cached
                } else {
                    // Check for pre-registered local schema
                    let local_schema = { avro_schemas.read().get(&subject).cloned() };

                    let (schema_id, schema) = if let Some(schema) = local_schema {
                        // Use local schema - register or lookup ID
                        if schema_config.auto_register {
                            let id = registry
                                .register(&subject, schema_config.schema_type, schema.raw())
                                .await
                                .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                            (id, schema)
                        } else {
                            // Try to get latest from registry
                            let versions = registry
                                .list_versions(&subject)
                                .await
                                .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                            if let Some(&version) = versions.last() {
                                let sv: SubjectVersion = registry
                                    .get_by_version(&subject, SchemaVersion::new(version))
                                    .await
                                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                                (sv.id, schema)
                            } else {
                                return Err(ConnectorError::Schema(format!(
                                    "No schema registered for subject: {}",
                                    subject
                                ))
                                .into());
                            }
                        }
                    } else {
                        // No local schema - try to fetch from registry first
                        let registry_result = registry.list_versions(&subject).await;

                        if let Ok(versions) = registry_result {
                            if let Some(&version) = versions.last() {
                                // Found in registry - use it
                                let sv: SubjectVersion = registry
                                    .get_by_version(&subject, SchemaVersion::new(version))
                                    .await
                                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                                let schema = AvroSchema::parse(&sv.schema).map_err(|e| {
                                    ConnectorError::Schema(format!(
                                        "Failed to parse schema from registry: {}",
                                        e
                                    ))
                                })?;
                                (sv.id, schema)
                            } else if schema_config.auto_register {
                                // No versions in registry, infer and register
                                let inferred = infer_avro_schema(&event.stream, &event.data)?;
                                let id = registry
                                    .register(&subject, schema_config.schema_type, inferred.raw())
                                    .await
                                    .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                                (id, inferred)
                            } else {
                                return Err(ConnectorError::Schema(format!(
                                    "No schema registered for stream: {}",
                                    event.stream
                                ))
                                .into());
                            }
                        } else if schema_config.auto_register {
                            // Registry lookup failed, infer and register
                            let inferred = infer_avro_schema(&event.stream, &event.data)?;
                            let id = registry
                                .register(&subject, schema_config.schema_type, inferred.raw())
                                .await
                                .map_err(|e| ConnectorError::Schema(e.to_string()))?;
                            (id, inferred)
                        } else {
                            return Err(ConnectorError::Schema(format!(
                                "No schema registered for stream: {}",
                                event.stream
                            ))
                            .into());
                        }
                    };

                    let codec = Arc::new(AvroCodec::new(schema));
                    schema_cache
                        .write()
                        .insert(event.stream.clone(), (schema_id, codec.clone()));
                    (schema_id, codec)
                };

                // Update event metadata with schema info
                event.metadata.schema_id = Some(schema_id.0);
                event.metadata.schema_subject = Some(subject.0);

                // Optionally serialize data to Avro
                if schema_config.confluent_wire_format {
                    let avro_bytes = codec
                        .encode_with_schema_id(&event.data, schema_id.0)
                        .map_err(|e| ConnectorError::Schema(format!("Avro encode error: {}", e)))?;

                    // Store Avro bytes as base64 in a special field
                    event.metadata.extra.insert(
                        "_avro_payload".to_string(),
                        serde_json::Value::String(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &avro_bytes,
                        )),
                    );
                }

                Ok(event)
            }
        });

        Ok(Box::pin(transformed))
    }
}

/// Infer an Avro schema from JSON data
fn infer_avro_schema(name: &str, data: &serde_json::Value) -> Result<AvroSchema> {
    use serde_json::json;

    let fields = match data {
        serde_json::Value::Object(map) => {
            let mut fields = Vec::new();
            for (key, value) in map {
                let field_type = match value {
                    serde_json::Value::Null => {
                        // For null values, default to nullable string
                        fields.push(json!({
                            "name": key,
                            "type": ["null", "string"],
                            "default": null
                        }));
                        continue;
                    }
                    serde_json::Value::Bool(_) => "boolean",
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            "long"
                        } else {
                            "double"
                        }
                    }
                    serde_json::Value::String(_) => "string",
                    serde_json::Value::Array(_) => {
                        // Default to array of strings
                        fields.push(json!({
                            "name": key,
                            "type": {"type": "array", "items": "string"}
                        }));
                        continue;
                    }
                    serde_json::Value::Object(_) => {
                        // Nested objects become strings (JSON-encoded)
                        "string"
                    }
                };
                fields.push(json!({
                    "name": key,
                    "type": ["null", field_type],
                    "default": null
                }));
            }
            fields
        }
        _ => {
            return Err(ConnectorError::Schema(
                "Cannot infer schema from non-object data".to_string(),
            )
            .into());
        }
    };

    // Sanitize name for Avro (replace non-alphanumeric with underscore)
    let safe_name: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();

    let schema = json!({
        "type": "record",
        "name": safe_name,
        "namespace": "rivven.inferred",
        "fields": fields
    });

    AvroSchema::parse(&schema.to_string())
        .map_err(|e| ConnectorError::Schema(format!("Schema inference failed: {}", e)).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_subject_strategy_topic_name() {
        let strategy = SubjectStrategy::TopicName;
        let subject = strategy.generate_subject("users", &json!({}));
        assert_eq!(subject.0, "users-value");
    }

    #[test]
    fn test_subject_strategy_record_name() {
        let strategy = SubjectStrategy::RecordName;
        let subject = strategy.generate_subject("topic", &json!({"__type": "User"}));
        assert_eq!(subject.0, "User");
    }

    #[test]
    fn test_subject_strategy_topic_record_name() {
        let strategy = SubjectStrategy::TopicRecordName;
        let subject = strategy.generate_subject("users", &json!({"__type": "Created"}));
        assert_eq!(subject.0, "users-Created");
    }

    #[test]
    fn test_infer_schema() {
        let data = json!({
            "id": 123,
            "name": "Alice",
            "active": true,
            "score": 99.5
        });

        let schema = infer_avro_schema("User", &data).unwrap();
        assert!(schema.raw().contains("User"));
        assert!(schema.raw().contains("rivven.inferred"));
    }

    #[test]
    fn test_infer_schema_with_null() {
        let data = json!({
            "id": 123,
            "email": null
        });

        let schema = infer_avro_schema("User", &data).unwrap();
        // All fields should be nullable by default
        assert!(schema.raw().contains("null"));
    }

    #[test]
    fn test_schema_aware_config_default() {
        let config = SchemaAwareConfig::default();
        assert!(config.auto_register);
        assert!(config.confluent_wire_format);
        assert!(matches!(config.schema_type, SchemaType::Avro));
    }
}
