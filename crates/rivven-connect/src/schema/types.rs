//! Schema Registry types and error handling

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

/// Unique identifier for a schema version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(pub u32);

impl SchemaId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Schema type (format)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    /// JSON Schema
    #[default]
    #[serde(alias = "json")]
    Json,
    /// Apache Avro
    #[serde(alias = "avro")]
    Avro,
    /// Protocol Buffers
    #[serde(alias = "protobuf")]
    Protobuf,
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Json => write!(f, "JSON"),
            SchemaType::Avro => write!(f, "AVRO"),
            SchemaType::Protobuf => write!(f, "PROTOBUF"),
        }
    }
}

impl std::str::FromStr for SchemaType {
    type Err = SchemaRegistryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "JSON" | "JSONSCHEMA" => Ok(SchemaType::Json),
            "AVRO" => Ok(SchemaType::Avro),
            "PROTOBUF" | "PROTO" => Ok(SchemaType::Protobuf),
            _ => Err(SchemaRegistryError::InvalidSchemaType(s.to_string())),
        }
    }
}

/// A schema with its metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Unique schema ID (global across all subjects)
    pub id: SchemaId,
    /// Schema type/format
    pub schema_type: SchemaType,
    /// The schema definition
    pub schema: String,
    /// Schema references (for nested schemas)
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

impl Schema {
    pub fn new(id: SchemaId, schema_type: SchemaType, schema: String) -> Self {
        Self {
            id,
            schema_type,
            schema,
            references: Vec::new(),
        }
    }

    /// Parse JSON schema
    pub fn parse_json(&self) -> SchemaRegistryResult<serde_json::Value> {
        if self.schema_type != SchemaType::Json {
            return Err(SchemaRegistryError::InvalidSchemaType(
                format!("Expected JSON, got {}", self.schema_type),
            ));
        }
        serde_json::from_str(&self.schema)
            .map_err(|e| SchemaRegistryError::ParseError(e.to_string()))
    }
}

/// Reference to another schema (for composition)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReference {
    /// Reference name (used in the schema)
    pub name: String,
    /// Subject containing the referenced schema
    pub subject: String,
    /// Version of the referenced schema
    pub version: u32,
}

/// Subject (typically topic-name + "-key" or "-value")
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Subject(pub String);

impl Subject {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Create a key subject for a topic
    pub fn key(topic: &str) -> Self {
        Self(format!("{}-key", topic))
    }

    /// Create a value subject for a topic
    pub fn value(topic: &str) -> Self {
        Self(format!("{}-value", topic))
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Subject {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Subject {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Version number for a schema within a subject
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaVersion(pub u32);

impl SchemaVersion {
    pub fn new(version: u32) -> Self {
        Self(version)
    }

    pub fn latest() -> Self {
        Self(u32::MAX)
    }

    pub fn is_latest(&self) -> bool {
        self.0 == u32::MAX
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_latest() {
            write!(f, "latest")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

/// A specific version of a schema for a subject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectVersion {
    /// Subject name
    pub subject: Subject,
    /// Version within the subject
    pub version: SchemaVersion,
    /// Global schema ID
    pub id: SchemaId,
    /// The schema definition
    pub schema: String,
    /// Schema type
    pub schema_type: SchemaType,
}

/// Schema Registry configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum SchemaRegistryConfig {
    /// Embedded mode: schemas stored in Rivven topics
    Embedded(EmbeddedConfig),
    /// External mode: connect to external schema registry
    External(ExternalConfig),
    /// Disabled: no schema registry (default)
    #[default]
    Disabled,
}

impl SchemaRegistryConfig {
    /// Create embedded registry config
    pub fn embedded() -> Self {
        Self::Embedded(EmbeddedConfig::default())
    }

    /// Create external registry config
    pub fn external(url: impl Into<String>) -> Self {
        Self::External(ExternalConfig {
            url: url.into(),
            ..Default::default()
        })
    }

    /// Check if schema registry is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }
}

/// Embedded schema registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddedConfig {
    /// Topic to store schemas (default: `_schemas`)
    #[serde(default = "default_schemas_topic")]
    pub topic: String,
    /// Cache TTL in seconds (default: 300)
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
    /// Default compatibility level
    #[serde(default)]
    pub compatibility: CompatibilityLevel,
}

impl Default for EmbeddedConfig {
    fn default() -> Self {
        Self {
            topic: default_schemas_topic(),
            cache_ttl_secs: default_cache_ttl(),
            compatibility: CompatibilityLevel::default(),
        }
    }
}

fn default_schemas_topic() -> String {
    "_schemas".to_string()
}

fn default_cache_ttl() -> u64 {
    300
}

/// External schema registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalConfig {
    /// Schema registry URL
    pub url: String,
    /// Basic auth username
    pub username: Option<String>,
    /// Basic auth password
    pub password: Option<String>,
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Cache TTL in seconds (default: 300)
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_secs: u64,
}

impl Default for ExternalConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8081".to_string(),
            username: None,
            password: None,
            timeout_secs: default_timeout(),
            cache_ttl_secs: default_cache_ttl(),
        }
    }
}

fn default_timeout() -> u64 {
    30
}

/// Compatibility level for schema evolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum CompatibilityLevel {
    /// No compatibility checking
    None,
    /// New schema can read data written with old schema
    #[default]
    Backward,
    /// New schema can read data written with all previous versions
    BackwardTransitive,
    /// Old schema can read data written with new schema
    Forward,
    /// Old schema can read data written with all newer versions
    ForwardTransitive,
    /// Both backward and forward compatible
    Full,
    /// Both backward and forward compatible with all versions
    FullTransitive,
}

impl std::fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompatibilityLevel::None => write!(f, "NONE"),
            CompatibilityLevel::Backward => write!(f, "BACKWARD"),
            CompatibilityLevel::BackwardTransitive => write!(f, "BACKWARD_TRANSITIVE"),
            CompatibilityLevel::Forward => write!(f, "FORWARD"),
            CompatibilityLevel::ForwardTransitive => write!(f, "FORWARD_TRANSITIVE"),
            CompatibilityLevel::Full => write!(f, "FULL"),
            CompatibilityLevel::FullTransitive => write!(f, "FULL_TRANSITIVE"),
        }
    }
}

/// Schema registry error types
#[derive(Debug, Error)]
pub enum SchemaRegistryError {
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    #[error("Version not found: {0}")]
    VersionNotFound(String),

    #[error("Invalid schema type: {0}")]
    InvalidSchemaType(String),

    #[error("Schema parse error: {0}")]
    ParseError(String),

    #[error("Compatibility check failed: {0}")]
    IncompatibleSchema(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Broker error: {0}")]
    BrokerError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Schema registry is disabled")]
    Disabled,
}

impl From<reqwest::Error> for SchemaRegistryError {
    fn from(err: reqwest::Error) -> Self {
        SchemaRegistryError::NetworkError(err.to_string())
    }
}

impl From<serde_json::Error> for SchemaRegistryError {
    fn from(err: serde_json::Error) -> Self {
        SchemaRegistryError::SerializationError(err.to_string())
    }
}

pub type SchemaRegistryResult<T> = Result<T, SchemaRegistryError>;

/// Metadata about schemas in a subject
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectMetadata {
    /// Subject name
    pub subject: Subject,
    /// All versions
    pub versions: Vec<u32>,
    /// Current compatibility level
    pub compatibility: CompatibilityLevel,
    /// ID mappings: version -> schema_id
    pub id_map: HashMap<u32, SchemaId>,
}
