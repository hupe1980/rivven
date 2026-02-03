//! Avro Format Writer for Rivven
//!
//! This module provides Apache Avro format support for writing events to object storage.
//! It leverages the existing `AvroCodec` from the schema module and adds the `FormatWriter`
//! trait implementation for seamless integration with storage sinks.
//!
//! # Features
//!
//! - Automatic schema inference from JSON records
//! - Schema-driven serialization with type coercion
//! - Configurable compression (None, Deflate, Snappy)
//! - Confluent wire format support (optional schema ID prefix)
//! - Object Container File (OCF) format for batch writes
//! - Implements `FormatWriter` trait for use with any storage sink
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::format::{AvroWriter, AvroWriterConfig, AvroCompression, FormatWriter};
//!
//! // With explicit schema
//! let schema = r#"
//!     {
//!         "type": "record",
//!         "name": "Event",
//!         "fields": [
//!             {"name": "id", "type": "long"},
//!             {"name": "name", "type": "string"},
//!             {"name": "timestamp", "type": "long"}
//!         ]
//!     }
//! "#;
//!
//! let config = AvroWriterConfig {
//!     schema: Some(schema.to_string()),
//!     compression: AvroCompression::Snappy,
//!     ..Default::default()
//! };
//!
//! let writer = AvroWriter::new(config)?;
//!
//! // Use via FormatWriter trait
//! let avro_bytes = writer.write_batch(&events)?;
//! println!("Format: {}, Extension: {}", writer.name(), writer.extension());
//! ```

use super::{FormatError, FormatWriter};
use crate::schema::avro::{AvroCodec, AvroSchema};
use apache_avro::{Codec, Writer};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

/// Errors that can occur during Avro operations
#[derive(Error, Debug)]
pub enum AvroWriterError {
    #[error("Schema parse error: {0}")]
    SchemaParse(String),

    #[error("Schema inference failed: {0}")]
    SchemaInference(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Empty batch: cannot infer schema from zero records")]
    EmptyBatch,

    #[error("Type coercion error: {0}")]
    TypeCoercion(String),

    #[error("Apache Avro error: {0}")]
    ApacheAvro(String),
}

pub type AvroWriterResult<T> = Result<T, AvroWriterError>;

/// Compression codec for Avro files
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AvroCompression {
    /// No compression
    #[default]
    None,
    /// Deflate compression (zlib)
    Deflate,
}

impl AvroCompression {
    fn to_avro_codec(&self) -> Codec {
        match self {
            AvroCompression::None => Codec::Null,
            AvroCompression::Deflate => Codec::Deflate,
        }
    }
}

/// Configuration for Avro writer
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct AvroWriterConfig {
    /// Explicit Avro schema (JSON string). If not provided, schema is inferred from data.
    #[serde(default)]
    pub schema: Option<String>,

    /// Compression codec to use
    #[serde(default)]
    pub compression: AvroCompression,

    /// Include Confluent wire format header (magic byte + schema ID)
    #[serde(default)]
    pub confluent_wire_format: bool,

    /// Schema ID for Confluent wire format (required if confluent_wire_format is true)
    #[serde(default)]
    pub schema_id: Option<u32>,

    /// Namespace for inferred schemas
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Record name for inferred schemas
    #[serde(default = "default_record_name")]
    pub record_name: String,
}

fn default_namespace() -> String {
    "rivven.events".to_string()
}

fn default_record_name() -> String {
    "Event".to_string()
}

impl Default for AvroWriterConfig {
    fn default() -> Self {
        Self {
            schema: None,
            compression: AvroCompression::default(),
            confluent_wire_format: false,
            schema_id: None,
            namespace: default_namespace(),
            record_name: default_record_name(),
        }
    }
}

/// Schema inference helper for JSON to Avro conversion
#[derive(Debug)]
pub struct AvroSchemaInference;

impl AvroSchemaInference {
    /// Infer Avro schema from a batch of JSON values
    pub fn infer_schema(
        records: &[Value],
        namespace: &str,
        record_name: &str,
    ) -> AvroWriterResult<String> {
        if records.is_empty() {
            return Err(AvroWriterError::EmptyBatch);
        }

        // Collect all field names and their types across all records
        let mut field_types: BTreeMap<String, AvroFieldType> = BTreeMap::new();

        for record in records {
            if let Value::Object(map) = record {
                for (key, value) in map {
                    let inferred_type = Self::infer_type(value);

                    // Merge types (nullable union if we see null)
                    if let Some(existing) = field_types.get(key) {
                        let merged = Self::merge_types(existing, &inferred_type);
                        field_types.insert(key.clone(), merged);
                    } else {
                        field_types.insert(key.clone(), inferred_type);
                    }
                }
            }
        }

        // Build Avro schema JSON
        let fields: Vec<String> = field_types
            .iter()
            .map(|(name, field_type)| {
                format!(
                    r#"{{"name": "{}", "type": {}}}"#,
                    name,
                    field_type.to_avro_type()
                )
            })
            .collect();

        let schema = format!(
            r#"{{"type": "record", "name": "{}", "namespace": "{}", "fields": [{}]}}"#,
            record_name,
            namespace,
            fields.join(", ")
        );

        Ok(schema)
    }

    /// Infer Avro type from a JSON value
    fn infer_type(value: &Value) -> AvroFieldType {
        match value {
            Value::Null => AvroFieldType::Null,
            Value::Bool(_) => AvroFieldType::Boolean,
            Value::Number(n) => {
                if n.is_i64() {
                    AvroFieldType::Long
                } else {
                    AvroFieldType::Double
                }
            }
            Value::String(_) => AvroFieldType::String,
            Value::Array(arr) => {
                if arr.is_empty() {
                    AvroFieldType::Array(Box::new(AvroFieldType::String))
                } else {
                    let item_type = Self::merge_array_types(arr);
                    AvroFieldType::Array(Box::new(item_type))
                }
            }
            Value::Object(_) => AvroFieldType::Map(Box::new(AvroFieldType::String)),
        }
    }

    /// Merge types from an array to find common type
    fn merge_array_types(arr: &[Value]) -> AvroFieldType {
        arr.iter()
            .map(Self::infer_type)
            .reduce(|a, b| Self::merge_types(&a, &b))
            .unwrap_or(AvroFieldType::String)
    }

    /// Merge two types, returning the wider/nullable type
    fn merge_types(a: &AvroFieldType, b: &AvroFieldType) -> AvroFieldType {
        match (a, b) {
            // Same type
            (t1, t2) if t1 == t2 => t1.clone(),

            // Null + any type = nullable union
            (AvroFieldType::Null, t) | (t, AvroFieldType::Null) => {
                AvroFieldType::Union(vec![AvroFieldType::Null, t.clone()])
            }

            // Numeric promotions
            (AvroFieldType::Long, AvroFieldType::Double)
            | (AvroFieldType::Double, AvroFieldType::Long) => AvroFieldType::Double,

            // String absorbs other types
            (AvroFieldType::String, _) | (_, AvroFieldType::String) => AvroFieldType::String,

            // Union handling
            (AvroFieldType::Union(types), t) | (t, AvroFieldType::Union(types)) => {
                let mut new_types = types.clone();
                if !new_types.contains(t) {
                    new_types.push(t.clone());
                }
                AvroFieldType::Union(new_types)
            }

            // Default: union of both
            _ => AvroFieldType::Union(vec![a.clone(), b.clone()]),
        }
    }
}

/// Internal representation of Avro field types
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum AvroFieldType {
    Null,
    Boolean,
    Long,
    Double,
    String,
    Bytes,
    Array(Box<AvroFieldType>),
    Map(Box<AvroFieldType>),
    Union(Vec<AvroFieldType>),
}

impl AvroFieldType {
    fn to_avro_type(&self) -> String {
        match self {
            AvroFieldType::Null => r#""null""#.to_string(),
            AvroFieldType::Boolean => r#""boolean""#.to_string(),
            AvroFieldType::Long => r#""long""#.to_string(),
            AvroFieldType::Double => r#""double""#.to_string(),
            AvroFieldType::String => r#""string""#.to_string(),
            AvroFieldType::Bytes => r#""bytes""#.to_string(),
            AvroFieldType::Array(item) => {
                format!(r#"{{"type": "array", "items": {}}}"#, item.to_avro_type())
            }
            AvroFieldType::Map(value) => {
                format!(r#"{{"type": "map", "values": {}}}"#, value.to_avro_type())
            }
            AvroFieldType::Union(types) => {
                let type_strs: Vec<String> = types.iter().map(|t| t.to_avro_type()).collect();
                format!("[{}]", type_strs.join(", "))
            }
        }
    }
}

/// Avro writer that converts JSON events to Avro format
///
/// Uses the existing `AvroCodec` from the schema module for serialization,
/// and wraps output in Object Container File (OCF) format for batch writes.
pub struct AvroWriter {
    config: AvroWriterConfig,
    schema: Option<AvroSchema>,
}

impl AvroWriter {
    /// Create a new Avro writer with the given configuration
    pub fn new(config: AvroWriterConfig) -> AvroWriterResult<Self> {
        let schema = if let Some(schema_str) = &config.schema {
            Some(
                AvroSchema::parse(schema_str)
                    .map_err(|e| AvroWriterError::SchemaParse(e.to_string()))?,
            )
        } else {
            None
        };

        Ok(Self { config, schema })
    }

    /// Write a batch of JSON events to Avro Object Container File format
    pub fn write_batch_internal(&self, events: &[Value]) -> AvroWriterResult<Vec<u8>> {
        if events.is_empty() {
            return Err(AvroWriterError::EmptyBatch);
        }

        // Get or infer schema
        let schema = if let Some(schema) = &self.schema {
            schema.clone()
        } else {
            let schema_str = AvroSchemaInference::infer_schema(
                events,
                &self.config.namespace,
                &self.config.record_name,
            )?;
            AvroSchema::parse(&schema_str)
                .map_err(|e| AvroWriterError::SchemaParse(e.to_string()))?
        };

        // Create codec for encoding
        let codec = AvroCodec::new(schema.clone());

        // Create Avro OCF writer using the new() method
        let mut writer = Writer::with_codec(
            schema.inner(),
            Vec::new(),
            self.config.compression.to_avro_codec(),
        );

        // Encode and write each event using AvroCodec, then decode to Avro value for writer
        for event in events {
            // Use AvroCodec to convert JSON -> binary
            let avro_bytes = codec
                .encode(event)
                .map_err(|e| AvroWriterError::Serialization(e.to_string()))?;

            // Decode to Avro value
            let avro_value = apache_avro::from_avro_datum(
                schema.inner(),
                &mut std::io::Cursor::new(&avro_bytes),
                None,
            )
            .map_err(|e| AvroWriterError::ApacheAvro(e.to_string()))?;

            // Append to writer
            writer
                .append(avro_value)
                .map_err(|e| AvroWriterError::ApacheAvro(e.to_string()))?;
        }

        // Flush and get bytes
        let bytes = writer
            .into_inner()
            .map_err(|e| AvroWriterError::ApacheAvro(e.to_string()))?;

        // Optionally add Confluent wire format header
        if self.config.confluent_wire_format {
            let schema_id = self.config.schema_id.unwrap_or(0);
            let mut result = Vec::with_capacity(5 + bytes.len());
            result.push(0u8); // Magic byte
            result.extend_from_slice(&schema_id.to_be_bytes());
            result.extend_from_slice(&bytes);
            Ok(result)
        } else {
            Ok(bytes)
        }
    }

    /// Write a single event to Avro binary (without OCF container)
    pub fn write_single(&self, event: &Value) -> AvroWriterResult<Vec<u8>> {
        let schema = if let Some(schema) = &self.schema {
            schema.clone()
        } else {
            let schema_str = AvroSchemaInference::infer_schema(
                std::slice::from_ref(event),
                &self.config.namespace,
                &self.config.record_name,
            )?;
            AvroSchema::parse(&schema_str)
                .map_err(|e| AvroWriterError::SchemaParse(e.to_string()))?
        };

        let codec = AvroCodec::new(schema);
        codec
            .encode(event)
            .map_err(|e| AvroWriterError::Serialization(e.to_string()))
    }
}

/// Implement FormatWriter trait for AvroWriter
impl FormatWriter for AvroWriter {
    fn name(&self) -> &'static str {
        "avro"
    }

    fn extension(&self) -> &'static str {
        ".avro"
    }

    fn content_type(&self) -> &'static str {
        "application/avro"
    }

    fn write_batch(&self, events: &[Value]) -> Result<Vec<u8>, FormatError> {
        self.write_batch_internal(events)
            .map_err(|e| FormatError::Write(e.to_string()))
    }

    fn supports_append(&self) -> bool {
        // Avro OCF doesn't support appending; each write creates a complete file
        false
    }
}

/// Helper to write events directly to Avro bytes
pub fn write_events_to_avro(
    events: &[Value],
    config: Option<AvroWriterConfig>,
) -> AvroWriterResult<Vec<u8>> {
    let writer = AvroWriter::new(config.unwrap_or_default())?;
    writer.write_batch_internal(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_schema_inference_simple() {
        let records = vec![
            json!({"name": "Alice", "age": 30, "active": true}),
            json!({"name": "Bob", "age": 25, "active": false}),
        ];

        let schema =
            AvroSchemaInference::infer_schema(&records, "test.namespace", "TestRecord").unwrap();

        // Parse the inferred schema to verify it's valid
        let parsed = AvroSchema::parse(&schema).unwrap();
        assert!(parsed.schema_type() == "record");
    }

    #[test]
    fn test_schema_inference_with_nulls() {
        let records = vec![
            json!({"name": "Alice", "email": null}),
            json!({"name": "Bob", "email": "bob@example.com"}),
        ];

        let schema = AvroSchemaInference::infer_schema(&records, "test", "Record").unwrap();

        // Should create a union type for email
        assert!(schema.contains("null"));
        assert!(schema.contains("string"));
    }

    #[test]
    fn test_schema_inference_empty_batch() {
        let records: Vec<Value> = vec![];
        let result = AvroSchemaInference::infer_schema(&records, "test", "Record");
        assert!(matches!(result, Err(AvroWriterError::EmptyBatch)));
    }

    #[test]
    fn test_write_simple_batch() {
        let events = vec![
            json!({"id": 1, "name": "Event 1"}),
            json!({"id": 2, "name": "Event 2"}),
        ];

        let config = AvroWriterConfig::default();
        let writer = AvroWriter::new(config).unwrap();

        let avro_bytes = writer.write_batch_internal(&events).unwrap();

        // Verify it's a valid Avro OCF (starts with "Obj\x01")
        assert!(avro_bytes.len() > 4);
        assert_eq!(&avro_bytes[0..4], b"Obj\x01");
    }

    #[test]
    fn test_write_with_explicit_schema() {
        let schema = r#"
            {
                "type": "record",
                "name": "Event",
                "namespace": "test",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            }
        "#;

        let config = AvroWriterConfig {
            schema: Some(schema.to_string()),
            ..Default::default()
        };

        let writer = AvroWriter::new(config).unwrap();

        let events = vec![
            json!({"id": 1, "name": "Event 1"}),
            json!({"id": 2, "name": "Event 2"}),
        ];

        let avro_bytes = writer.write_batch_internal(&events).unwrap();
        assert!(avro_bytes.len() > 4);
    }

    #[test]
    fn test_write_with_compression() {
        let events = vec![
            json!({"message": "Hello, World!".repeat(100)}),
            json!({"message": "Test message".repeat(100)}),
        ];

        let config = AvroWriterConfig {
            compression: AvroCompression::Deflate,
            ..Default::default()
        };

        let writer = AvroWriter::new(config).unwrap();
        let avro_bytes = writer.write_batch_internal(&events).unwrap();

        // Compressed should still be valid Avro
        assert!(avro_bytes.len() > 4);
        assert_eq!(&avro_bytes[0..4], b"Obj\x01");
    }

    #[test]
    fn test_confluent_wire_format() {
        let events = vec![json!({"id": 1, "name": "Test"})];

        let config = AvroWriterConfig {
            confluent_wire_format: true,
            schema_id: Some(12345),
            ..Default::default()
        };

        let writer = AvroWriter::new(config).unwrap();
        let bytes = writer.write_batch_internal(&events).unwrap();

        // Should have magic byte + schema ID prefix
        assert_eq!(bytes[0], 0); // Magic byte
        let schema_id = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        assert_eq!(schema_id, 12345);
    }

    #[test]
    fn test_format_writer_trait() {
        let config = AvroWriterConfig::default();
        let writer = AvroWriter::new(config).unwrap();

        assert_eq!(writer.name(), "avro");
        assert_eq!(writer.extension(), ".avro");
        assert_eq!(writer.content_type(), "application/avro");
        assert!(!writer.supports_append());

        let events = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        // Use trait method
        let format_writer: &dyn FormatWriter = &writer;
        let bytes = format_writer.write_batch(&events).unwrap();

        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"Obj\x01");
    }

    #[test]
    fn test_format_writer_empty_batch_error() {
        let writer = AvroWriter::new(AvroWriterConfig::default()).unwrap();
        let format_writer: &dyn FormatWriter = &writer;

        let result = format_writer.write_batch(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_helper_function() {
        let events = vec![json!({"key": "value"})];

        let bytes = write_events_to_avro(&events, None).unwrap();
        assert!(bytes.len() > 4);
    }
}
