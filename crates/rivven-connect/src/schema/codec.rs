//! Unified codec abstraction for serialization formats
//!
//! Provides a single interface for encoding/decoding data in different formats:
//! - **JSON**: Human-readable, schema-optional (good for development)
//! - **Avro**: Binary, schema-required (best for production)
//! - **Protobuf**: Binary, schema-required (alternative to Avro)
//!
//! # Kafka Best Practices
//!
//! In production Kafka deployments, **Avro is the recommended format** because:
//! - Schema evolution with compatibility checking
//! - Compact binary encoding (50-70% smaller than JSON)
//! - Centralized schema management via Schema Registry
//! - Type safety and validation
//!
//! However, JSON is useful for:
//! - Development and debugging
//! - Systems without Schema Registry
//! - Human-readable audit logs
//! - Interoperability with legacy systems
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::schema::{Codec, SerializationFormat, CodecConfig};
//!
//! // JSON codec (no schema registry needed)
//! let json_codec = Codec::json();
//! let bytes = json_codec.encode(&data)?;
//! let decoded = json_codec.decode(&bytes)?;
//!
//! // Avro codec with schema registry (production recommended)
//! let avro_codec = Codec::avro(schema_registry, schema_id);
//! let bytes = avro_codec.encode(&data)?;  // Includes 5-byte header
//! let decoded = avro_codec.decode(&bytes)?;
//!
//! // Create from config
//! let codec = Codec::from_config(&config)?;
//! ```

use super::avro::{AvroCodec, AvroError, AvroSchema};
use super::protobuf::{ProtobufCodec, ProtobufError, ProtobufSchema};
use super::types::{SchemaId, SchemaType};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur during codec operations
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Avro error: {0}")]
    Avro(#[from] AvroError),

    #[error("Protobuf error: {0}")]
    Protobuf(#[from] ProtobufError),

    #[error("Schema required for format {0}")]
    SchemaRequired(String),

    #[error("Invalid wire format: {0}")]
    InvalidWireFormat(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),
}

pub type CodecResult<T> = Result<T, CodecError>;

/// Serialization format for event data
///
/// # Production Recommendations
///
/// | Format   | Use Case                           | Schema Registry |
/// |----------|------------------------------------| --------------- |
/// | JSON     | Development, debugging, logs       | Optional        |
/// | Avro     | Production (recommended)           | Required        |
/// | Protobuf | High-performance, cross-language   | Required        |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SerializationFormat {
    /// JSON format - human-readable, no schema required
    ///
    /// Best for: development, debugging, audit logs
    #[default]
    Json,

    /// Apache Avro - binary format with schema evolution
    ///
    /// Best for: production Kafka deployments
    /// Requires: Schema Registry
    Avro,

    /// Protocol Buffers - binary format, cross-language
    ///
    /// Best for: high-performance, polyglot systems
    /// Requires: Schema Registry
    Protobuf,
}

impl SerializationFormat {
    /// Check if this format requires a schema
    pub fn requires_schema(&self) -> bool {
        matches!(self, Self::Avro | Self::Protobuf)
    }

    /// Check if this format is binary
    pub fn is_binary(&self) -> bool {
        matches!(self, Self::Avro | Self::Protobuf)
    }

    /// Get the content type for this format
    pub fn content_type(&self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Avro => "application/avro",
            Self::Protobuf => "application/x-protobuf",
        }
    }
}

impl std::fmt::Display for SerializationFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json => write!(f, "json"),
            Self::Avro => write!(f, "avro"),
            Self::Protobuf => write!(f, "protobuf"),
        }
    }
}

impl std::str::FromStr for SerializationFormat {
    type Err = CodecError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "avro" => Ok(Self::Avro),
            "protobuf" | "proto" => Ok(Self::Protobuf),
            _ => Err(CodecError::UnsupportedFormat(s.to_string())),
        }
    }
}

impl From<SchemaType> for SerializationFormat {
    fn from(schema_type: SchemaType) -> Self {
        match schema_type {
            SchemaType::Json => Self::Json,
            SchemaType::Avro => Self::Avro,
            SchemaType::Protobuf => Self::Protobuf,
        }
    }
}

impl From<SerializationFormat> for SchemaType {
    fn from(format: SerializationFormat) -> Self {
        match format {
            SerializationFormat::Json => Self::Json,
            SerializationFormat::Avro => Self::Avro,
            SerializationFormat::Protobuf => Self::Protobuf,
        }
    }
}

/// Configuration for codec behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodecConfig {
    /// Serialization format
    #[serde(default)]
    pub format: SerializationFormat,

    /// Use Confluent wire format for Avro (5-byte header with schema ID)
    ///
    /// Format: `[0x00][schema_id: 4 bytes big-endian][avro_payload]`
    #[serde(default = "default_true")]
    pub confluent_wire_format: bool,

    /// Pretty-print JSON output
    #[serde(default)]
    pub pretty_json: bool,

    /// Include null fields in JSON output
    #[serde(default = "default_true")]
    pub include_nulls: bool,
}

fn default_true() -> bool {
    true
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            format: SerializationFormat::Json,
            confluent_wire_format: true,
            pretty_json: false,
            include_nulls: true,
        }
    }
}

impl CodecConfig {
    /// Create JSON codec config
    pub fn json() -> Self {
        Self {
            format: SerializationFormat::Json,
            ..Default::default()
        }
    }

    /// Create Avro codec config (production recommended)
    pub fn avro() -> Self {
        Self {
            format: SerializationFormat::Avro,
            confluent_wire_format: true,
            ..Default::default()
        }
    }

    /// Create Protobuf codec config
    pub fn protobuf() -> Self {
        Self {
            format: SerializationFormat::Protobuf,
            ..Default::default()
        }
    }
}

/// Unified codec for encoding/decoding data
///
/// Supports JSON, Avro, and Protobuf formats with a consistent interface.
pub enum Codec {
    /// JSON codec - no schema required
    Json(JsonCodec),
    /// Avro codec - requires schema, with optional schema ID for Confluent wire format
    Avro {
        codec: Arc<AvroCodec>,
        schema_id: Option<SchemaId>,
    },
    /// Protobuf codec - requires schema
    Protobuf(Arc<ProtobufCodec>),
}

impl Codec {
    /// Create a JSON codec
    pub fn json() -> Self {
        Self::Json(JsonCodec::new())
    }

    /// Create a JSON codec with config
    pub fn json_with_config(pretty: bool, include_nulls: bool) -> Self {
        Self::Json(JsonCodec {
            pretty,
            include_nulls,
        })
    }

    /// Create an Avro codec with schema
    pub fn avro(schema: AvroSchema) -> Self {
        Self::Avro {
            codec: Arc::new(AvroCodec::new(schema)),
            schema_id: None,
        }
    }

    /// Create an Avro codec with schema ID header (Confluent wire format)
    pub fn avro_with_schema_id(schema: AvroSchema, schema_id: SchemaId) -> Self {
        Self::Avro {
            codec: Arc::new(AvroCodec::new(schema)),
            schema_id: Some(schema_id),
        }
    }

    /// Create a Protobuf codec with schema
    pub fn protobuf(schema: ProtobufSchema) -> Self {
        Self::Protobuf(Arc::new(ProtobufCodec::new(schema)))
    }

    /// Get the serialization format
    pub fn format(&self) -> SerializationFormat {
        match self {
            Self::Json(_) => SerializationFormat::Json,
            Self::Avro { .. } => SerializationFormat::Avro,
            Self::Protobuf(_) => SerializationFormat::Protobuf,
        }
    }

    /// Encode a value to bytes
    pub fn encode(&self, value: &serde_json::Value) -> CodecResult<Bytes> {
        match self {
            Self::Json(codec) => codec.encode(value),
            Self::Avro { codec, schema_id } => {
                if let Some(id) = schema_id {
                    codec
                        .encode_with_schema_id(value, id.0)
                        .map(Bytes::from)
                        .map_err(CodecError::Avro)
                } else {
                    codec
                        .encode(value)
                        .map(Bytes::from)
                        .map_err(CodecError::Avro)
                }
            }
            Self::Protobuf(codec) => codec
                .encode(value)
                .map(Bytes::from)
                .map_err(CodecError::Protobuf),
        }
    }

    /// Decode bytes to a value
    pub fn decode(&self, bytes: &[u8]) -> CodecResult<serde_json::Value> {
        match self {
            Self::Json(codec) => codec.decode(bytes),
            Self::Avro { codec, .. } => codec.decode(bytes).map_err(CodecError::Avro),
            Self::Protobuf(codec) => codec.decode(bytes).map_err(CodecError::Protobuf),
        }
    }

    /// Decode bytes with Confluent wire format (returns schema ID and value)
    pub fn decode_with_schema_id(
        &self,
        bytes: &[u8],
    ) -> CodecResult<(Option<SchemaId>, serde_json::Value)> {
        match self {
            Self::Json(codec) => Ok((None, codec.decode(bytes)?)),
            Self::Avro { codec, .. } => {
                // Check for Confluent wire format header
                if bytes.len() >= 5 && bytes[0] == 0x00 {
                    let (schema_id, value) = codec
                        .decode_with_schema_id(bytes)
                        .map_err(CodecError::Avro)?;
                    Ok((Some(SchemaId::new(schema_id)), value))
                } else {
                    let value = codec.decode(bytes).map_err(CodecError::Avro)?;
                    Ok((None, value))
                }
            }
            Self::Protobuf(codec) => {
                let value = codec.decode(bytes).map_err(CodecError::Protobuf)?;
                Ok((None, value))
            }
        }
    }
}

/// JSON codec for human-readable serialization
#[derive(Debug, Clone)]
pub struct JsonCodec {
    /// Pretty-print output
    pub pretty: bool,
    /// Include null fields
    pub include_nulls: bool,
}

impl Default for JsonCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonCodec {
    /// Create a new JSON codec
    pub fn new() -> Self {
        Self {
            pretty: false,
            include_nulls: true,
        }
    }

    /// Set pretty printing
    pub fn with_pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }

    /// Set null field inclusion
    pub fn with_include_nulls(mut self, include_nulls: bool) -> Self {
        self.include_nulls = include_nulls;
        self
    }

    /// Encode a value to JSON bytes
    pub fn encode(&self, value: &serde_json::Value) -> CodecResult<Bytes> {
        let json = if self.include_nulls {
            if self.pretty {
                serde_json::to_vec_pretty(value)?
            } else {
                serde_json::to_vec(value)?
            }
        } else {
            // Remove null fields
            let filtered = self.remove_nulls(value.clone());
            if self.pretty {
                serde_json::to_vec_pretty(&filtered)?
            } else {
                serde_json::to_vec(&filtered)?
            }
        };
        Ok(Bytes::from(json))
    }

    /// Decode JSON bytes to a value
    pub fn decode(&self, bytes: &[u8]) -> CodecResult<serde_json::Value> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Remove null fields from a JSON value
    fn remove_nulls(&self, value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let filtered: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k, self.remove_nulls(v)))
                    .collect();
                serde_json::Value::Object(filtered)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.into_iter().map(|v| self.remove_nulls(v)).collect())
            }
            other => other,
        }
    }
}

/// Helper to create schema ID header bytes (Confluent wire format)
pub fn encode_schema_id_header(schema_id: SchemaId) -> [u8; 5] {
    let mut header = [0u8; 5];
    header[0] = 0x00; // Magic byte
    header[1..5].copy_from_slice(&schema_id.0.to_be_bytes());
    header
}

/// Helper to decode schema ID from header (Confluent wire format)
pub fn decode_schema_id_header(bytes: &[u8]) -> Option<SchemaId> {
    if bytes.len() >= 5 && bytes[0] == 0x00 {
        let id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        Some(SchemaId::new(id))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_codec_roundtrip() {
        let codec = JsonCodec::new();
        let value = json!({
            "id": 123,
            "name": "Alice",
            "email": "alice@example.com"
        });

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_json_codec_remove_nulls() {
        let codec = JsonCodec::new().with_include_nulls(false);
        let value = json!({
            "id": 123,
            "name": "Alice",
            "email": null
        });

        let encoded = codec.encode(&value).unwrap();
        let decoded: serde_json::Value = serde_json::from_slice(&encoded).unwrap();

        assert!(decoded.get("email").is_none());
        assert_eq!(decoded.get("id").unwrap(), &json!(123));
    }

    #[test]
    fn test_json_codec_pretty() {
        let codec = JsonCodec::new().with_pretty(true);
        let value = json!({"id": 123});

        let encoded = codec.encode(&value).unwrap();
        let text = String::from_utf8_lossy(&encoded);

        assert!(text.contains('\n')); // Pretty-printed has newlines
    }

    #[test]
    fn test_serialization_format_parse() {
        assert_eq!(
            "json".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Json
        );
        assert_eq!(
            "avro".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Avro
        );
        assert_eq!(
            "protobuf".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Protobuf
        );
        assert_eq!(
            "proto".parse::<SerializationFormat>().unwrap(),
            SerializationFormat::Protobuf
        );
    }

    #[test]
    fn test_schema_id_header() {
        let schema_id = SchemaId::new(12345);
        let header = encode_schema_id_header(schema_id);

        assert_eq!(header[0], 0x00); // Magic byte
        let decoded = decode_schema_id_header(&header).unwrap();
        assert_eq!(decoded.0, 12345);
    }

    #[test]
    fn test_codec_format() {
        let json_codec = Codec::json();
        assert_eq!(json_codec.format(), SerializationFormat::Json);
    }

    #[test]
    fn test_unified_codec_json() {
        let codec = Codec::json();
        let value = json!({"test": "data"});

        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }
}
