//! Parquet Format Writer for Rivven
//!
//! This module provides Parquet format support for writing events to object storage.
//! It includes automatic schema inference from JSON events and efficient columnar storage.
//!
//! # Features
//!
//! - Automatic Arrow schema inference from JSON records
//! - Configurable compression (Snappy, Gzip, LZ4, Zstd, Brotli)
//! - Row group batching for efficient writes
//! - Support for nested JSON structures
//! - Implements `FormatWriter` trait for use with any storage sink
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::format::{ParquetWriter, ParquetWriterConfig, ParquetCompression, FormatWriter};
//!
//! let config = ParquetWriterConfig {
//!     compression: ParquetCompression::Snappy,
//!     row_group_size: 10000,
//!     ..Default::default()
//! };
//!
//! let writer = ParquetWriter::new(config);
//!
//! // Use via FormatWriter trait
//! let parquet_bytes = writer.write_batch(&events)?;
//! println!("Format: {}, Extension: {}", writer.name(), writer.extension());
//! ```

use super::{FormatError, FormatWriter};
use arrow_array::{
    builder::{BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder},
    ArrayRef, RecordBatch, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, io::Cursor, sync::Arc};
use thiserror::Error;

/// Errors that can occur during Parquet operations
#[derive(Error, Debug)]
pub enum ParquetError {
    #[error("Schema inference failed: {0}")]
    SchemaInference(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("Parquet write error: {0}")]
    Write(#[from] parquet::errors::ParquetError),

    #[error("Empty batch: cannot infer schema from zero records")]
    EmptyBatch,

    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type ParquetResult<T> = Result<T, ParquetError>;

/// Compression codec for Parquet files
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    /// No compression
    None,
    /// Snappy compression (fast, moderate ratio)
    #[default]
    Snappy,
    /// Gzip compression (slower, better ratio)
    Gzip,
    /// LZ4 compression (very fast, lower ratio)
    Lz4,
    /// Zstd compression (good balance of speed and ratio)
    Zstd,
    /// Brotli compression (best ratio, slowest)
    Brotli,
}

impl ParquetCompression {
    fn to_parquet_compression(&self) -> Compression {
        match self {
            ParquetCompression::None => Compression::UNCOMPRESSED,
            ParquetCompression::Snappy => Compression::SNAPPY,
            ParquetCompression::Gzip => Compression::GZIP(Default::default()),
            ParquetCompression::Lz4 => Compression::LZ4,
            ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
            ParquetCompression::Brotli => Compression::BROTLI(Default::default()),
        }
    }
}

/// Configuration for Parquet writer
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ParquetWriterConfig {
    /// Compression codec to use
    #[serde(default)]
    pub compression: ParquetCompression,

    /// Number of rows per row group (default: 10000)
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,

    /// Maximum size of string/binary columns in bytes (for schema inference)
    #[serde(default = "default_max_string_size")]
    pub max_string_size: usize,

    /// Whether to write statistics in footer
    #[serde(default = "default_enable_statistics")]
    pub enable_statistics: bool,

    /// Data page size in bytes (default: 1MB)
    #[serde(default = "default_data_page_size")]
    pub data_page_size: usize,

    /// Dictionary page size limit in bytes (default: 1MB)
    #[serde(default = "default_dictionary_page_size")]
    pub dictionary_page_size: usize,
}

fn default_row_group_size() -> usize {
    10_000
}

fn default_max_string_size() -> usize {
    1_048_576 // 1MB
}

fn default_enable_statistics() -> bool {
    true
}

fn default_data_page_size() -> usize {
    1_048_576 // 1MB
}

fn default_dictionary_page_size() -> usize {
    1_048_576 // 1MB
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        Self {
            compression: ParquetCompression::default(),
            row_group_size: default_row_group_size(),
            max_string_size: default_max_string_size(),
            enable_statistics: default_enable_statistics(),
            data_page_size: default_data_page_size(),
            dictionary_page_size: default_dictionary_page_size(),
        }
    }
}

/// Schema inference helper for JSON to Arrow conversion
#[derive(Debug)]
pub struct SchemaInference;

impl SchemaInference {
    /// Infer Arrow schema from a batch of JSON values
    pub fn infer_schema(records: &[Value]) -> ParquetResult<Schema> {
        if records.is_empty() {
            return Err(ParquetError::EmptyBatch);
        }

        // Collect all field names and their types across all records
        let mut field_types: HashMap<String, DataType> = HashMap::new();

        for record in records {
            if let Value::Object(map) = record {
                for (key, value) in map {
                    let inferred_type = Self::infer_type(value);

                    // Merge types (wider type wins)
                    if let Some(existing) = field_types.get(key) {
                        let merged = Self::merge_types(existing, &inferred_type);
                        field_types.insert(key.clone(), merged);
                    } else {
                        field_types.insert(key.clone(), inferred_type);
                    }
                }
            }
        }

        // Sort fields for consistent schema
        let mut fields: Vec<_> = field_types
            .into_iter()
            .map(|(name, dtype)| Field::new(name, dtype, true))
            .collect();
        fields.sort_by(|a, b| a.name().cmp(b.name()));

        Ok(Schema::new(fields))
    }

    /// Infer Arrow DataType from a JSON value
    fn infer_type(value: &Value) -> DataType {
        match value {
            Value::Null => DataType::Utf8, // Default null to string
            Value::Bool(_) => DataType::Boolean,
            Value::Number(n) => {
                if n.is_i64() {
                    DataType::Int64
                } else {
                    DataType::Float64
                }
            }
            Value::String(_) => DataType::Utf8,
            Value::Array(arr) => {
                if arr.is_empty() {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                } else {
                    let inner_type = Self::merge_array_types(arr);
                    DataType::List(Arc::new(Field::new("item", inner_type, true)))
                }
            }
            Value::Object(map) => {
                let fields: Vec<Field> = map
                    .iter()
                    .map(|(k, v)| Field::new(k, Self::infer_type(v), true))
                    .collect();
                DataType::Struct(fields.into())
            }
        }
    }

    /// Merge types from an array to find common type
    fn merge_array_types(arr: &[Value]) -> DataType {
        arr.iter()
            .map(Self::infer_type)
            .reduce(|a, b| Self::merge_types(&a, &b))
            .unwrap_or(DataType::Utf8)
    }

    /// Merge two types, returning the wider type
    fn merge_types(a: &DataType, b: &DataType) -> DataType {
        match (a, b) {
            // Same type
            (t1, t2) if t1 == t2 => t1.clone(),

            // Null can be any type
            (DataType::Null, t) | (t, DataType::Null) => t.clone(),

            // Numeric promotions
            (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
                DataType::Float64
            }

            // Everything else becomes string
            _ => DataType::Utf8,
        }
    }
}

/// Parquet writer that converts JSON events to Parquet format
pub struct ParquetWriter {
    config: ParquetWriterConfig,
}

impl ParquetWriter {
    /// Create a new Parquet writer with the given configuration
    pub fn new(config: ParquetWriterConfig) -> Self {
        Self { config }
    }

    /// Write a batch of JSON events to Parquet format
    pub fn write_batch(&self, events: &[Value]) -> ParquetResult<Vec<u8>> {
        if events.is_empty() {
            return Err(ParquetError::EmptyBatch);
        }

        // Infer schema from the batch
        let schema = SchemaInference::infer_schema(events)?;
        let schema_arc = Arc::new(schema);

        // Build Arrow arrays from JSON
        let record_batch = self.build_record_batch(events, schema_arc.clone())?;

        // Write to Parquet
        let mut buffer = Cursor::new(Vec::new());
        let props = self.build_writer_properties();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema_arc, Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        Ok(buffer.into_inner())
    }

    /// Build writer properties from config
    fn build_writer_properties(&self) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(self.config.compression.to_parquet_compression())
            .set_data_page_size_limit(self.config.data_page_size)
            .set_dictionary_page_size_limit(self.config.dictionary_page_size)
            .set_max_row_group_size(self.config.row_group_size);

        if !self.config.enable_statistics {
            builder =
                builder.set_statistics_enabled(parquet::file::properties::EnabledStatistics::None);
        }

        builder.build()
    }

    /// Build a RecordBatch from JSON events
    fn build_record_batch(
        &self,
        events: &[Value],
        schema: Arc<Schema>,
    ) -> ParquetResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::new();

        for field in schema.fields() {
            let array = self.build_array(events, field.name(), field.data_type())?;
            columns.push(array);
        }

        RecordBatch::try_new(schema, columns).map_err(ParquetError::Arrow)
    }

    /// Build an Arrow array from a JSON field
    fn build_array(
        &self,
        events: &[Value],
        field_name: &str,
        data_type: &DataType,
    ) -> ParquetResult<ArrayRef> {
        match data_type {
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(events.len());
                for event in events {
                    match event.get(field_name) {
                        Some(Value::Bool(b)) => builder.append_value(*b),
                        Some(Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            // Coerce to boolean
                            let b = match v {
                                Value::String(s) => !s.is_empty() && s != "false" && s != "0",
                                Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
                                _ => true,
                            };
                            builder.append_value(b);
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(events.len());
                for event in events {
                    match event.get(field_name) {
                        Some(Value::Number(n)) => {
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else if let Some(f) = n.as_f64() {
                                builder.append_value(f as i64);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(Value::String(s)) => {
                            if let Ok(i) = s.parse::<i64>() {
                                builder.append_value(i);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(events.len());
                for event in events {
                    match event.get(field_name) {
                        Some(Value::Number(n)) => {
                            if let Some(f) = n.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(Value::String(s)) => {
                            if let Ok(f) = s.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        Some(Value::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder =
                    StringBuilder::with_capacity(events.len(), self.config.max_string_size);
                for event in events {
                    match event.get(field_name) {
                        Some(Value::String(s)) => builder.append_value(s),
                        Some(Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            // Convert any value to string
                            let s = serde_json::to_string(v)
                                .map_err(|e| ParquetError::Serialization(e.to_string()))?;
                            builder.append_value(&s);
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::List(inner_field) => self.build_list_array(events, field_name, inner_field),
            DataType::Struct(fields) => self.build_struct_array(events, field_name, fields),
            _ => {
                // Fallback: convert to string
                let mut builder =
                    StringBuilder::with_capacity(events.len(), self.config.max_string_size);
                for event in events {
                    match event.get(field_name) {
                        Some(Value::Null) | None => builder.append_null(),
                        Some(v) => {
                            let s = serde_json::to_string(v)
                                .map_err(|e| ParquetError::Serialization(e.to_string()))?;
                            builder.append_value(&s);
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }

    /// Build a List array from JSON arrays
    fn build_list_array(
        &self,
        events: &[Value],
        field_name: &str,
        _inner_field: &Arc<Field>,
    ) -> ParquetResult<ArrayRef> {
        // For simplicity, convert list items to strings
        let mut builder = ListBuilder::new(StringBuilder::new());

        for event in events {
            match event.get(field_name) {
                Some(Value::Array(arr)) => {
                    for item in arr {
                        let s = match item {
                            Value::String(s) => s.clone(),
                            Value::Null => {
                                builder.values().append_null();
                                continue;
                            }
                            v => serde_json::to_string(v)
                                .map_err(|e| ParquetError::Serialization(e.to_string()))?,
                        };
                        builder.values().append_value(&s);
                    }
                    builder.append(true);
                }
                Some(Value::Null) | None => {
                    builder.append(false);
                }
                _ => {
                    builder.append(false);
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    /// Build a Struct array from JSON objects
    fn build_struct_array(
        &self,
        events: &[Value],
        field_name: &str,
        fields: &arrow_schema::Fields,
    ) -> ParquetResult<ArrayRef> {
        // Extract the sub-object for each event; null/missing → Value::Null
        let sub_events: Vec<Value> = events
            .iter()
            .map(|event| event.get(field_name).cloned().unwrap_or(Value::Null))
            .collect();

        // Recursively build a child array for each struct field
        let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            let child = self.build_array(&sub_events, field.name(), field.data_type())?;
            child_arrays.push(child);
        }

        // Null buffer: struct is null when the source value is null/missing
        let nulls: Vec<bool> = sub_events.iter().map(|v| !v.is_null()).collect();

        let struct_array = StructArray::try_new(
            fields.clone(),
            child_arrays,
            Some(arrow::buffer::NullBuffer::from(nulls)),
        )
        .map_err(|e| ParquetError::Serialization(e.to_string()))?;

        Ok(Arc::new(struct_array))
    }
}

/// Implement FormatWriter trait for ParquetWriter
impl FormatWriter for ParquetWriter {
    fn name(&self) -> &'static str {
        "parquet"
    }

    fn extension(&self) -> &'static str {
        ".parquet"
    }

    fn content_type(&self) -> &'static str {
        "application/vnd.apache.parquet"
    }

    fn write_batch(&self, events: &[serde_json::Value]) -> Result<Vec<u8>, FormatError> {
        // Delegate to the internal write_batch implementation
        ParquetWriter::write_batch(self, events).map_err(|e| FormatError::Write(e.to_string()))
    }

    fn supports_append(&self) -> bool {
        // Parquet files don't support appending; each write creates a complete file
        false
    }
}

/// Helper to write events directly to Parquet bytes
pub fn write_events_to_parquet(
    events: &[Value],
    config: Option<ParquetWriterConfig>,
) -> ParquetResult<Vec<u8>> {
    let writer = ParquetWriter::new(config.unwrap_or_default());
    writer.write_batch(events)
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

        let schema = SchemaInference::infer_schema(&records).unwrap();
        assert_eq!(schema.fields().len(), 3);

        // Fields are sorted alphabetically
        assert_eq!(schema.field(0).name(), "active");
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);

        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);

        assert_eq!(schema.field(2).name(), "name");
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_schema_inference_with_floats() {
        let records = vec![
            json!({"value": 1.5, "count": 10}),
            json!({"value": 2.0, "count": 20}),
        ];

        let schema = SchemaInference::infer_schema(&records).unwrap();

        let count_field = schema.field_with_name("count").unwrap();
        assert_eq!(count_field.data_type(), &DataType::Int64);

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_schema_inference_type_promotion() {
        // First record has int, second has float - should promote to float
        let records = vec![json!({"value": 10}), json!({"value": 1.5})];

        let schema = SchemaInference::infer_schema(&records).unwrap();
        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_schema_inference_with_nulls() {
        let records = vec![
            json!({"name": "Alice", "email": null}),
            json!({"name": "Bob", "email": "bob@example.com"}),
        ];

        let schema = SchemaInference::infer_schema(&records).unwrap();

        let email_field = schema.field_with_name("email").unwrap();
        assert_eq!(email_field.data_type(), &DataType::Utf8);
        assert!(email_field.is_nullable());
    }

    #[test]
    fn test_schema_inference_with_arrays() {
        let records = vec![
            json!({"tags": ["rust", "parquet"]}),
            json!({"tags": ["arrow"]}),
        ];

        let schema = SchemaInference::infer_schema(&records).unwrap();
        let tags_field = schema.field_with_name("tags").unwrap();

        match tags_field.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            _ => panic!("Expected List type"),
        }
    }

    #[test]
    fn test_schema_inference_empty() {
        let records: Vec<Value> = vec![];
        let result = SchemaInference::infer_schema(&records);
        assert!(matches!(result, Err(ParquetError::EmptyBatch)));
    }

    #[test]
    fn test_write_simple_batch() {
        let events = vec![
            json!({"id": 1, "name": "Event 1", "value": 100}),
            json!({"id": 2, "name": "Event 2", "value": 200}),
            json!({"id": 3, "name": "Event 3", "value": 300}),
        ];

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config);

        let parquet_bytes = writer.write_batch(&events).unwrap();

        // Verify it's valid Parquet (starts with PAR1 magic)
        assert!(parquet_bytes.len() > 4);
        assert_eq!(&parquet_bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_with_compression() {
        let events = vec![
            json!({"message": "Hello, World!".repeat(100)}),
            json!({"message": "Test message".repeat(100)}),
        ];

        let config = ParquetWriterConfig {
            compression: ParquetCompression::Snappy,
            ..Default::default()
        };
        let writer = ParquetWriter::new(config);

        let parquet_bytes = writer.write_batch(&events).unwrap();
        assert!(parquet_bytes.len() > 4);
        assert_eq!(&parquet_bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_write_with_nulls() {
        let events = vec![
            json!({"id": 1, "optional": "value"}),
            json!({"id": 2, "optional": null}),
            json!({"id": 3}), // missing field
        ];

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config);

        let parquet_bytes = writer.write_batch(&events).unwrap();
        assert!(parquet_bytes.len() > 4);
    }

    #[test]
    fn test_write_mixed_types() {
        let events = vec![
            json!({"value": 10}),
            json!({"value": 20.5}), // Mixed int/float
        ];

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config);

        let parquet_bytes = writer.write_batch(&events).unwrap();
        assert!(parquet_bytes.len() > 4);
    }

    #[test]
    fn test_helper_function() {
        let events = vec![json!({"key": "value"})];

        let bytes = write_events_to_parquet(&events, None).unwrap();
        assert!(bytes.len() > 4);
    }

    #[test]
    fn test_format_writer_trait() {
        use crate::format::FormatWriter;

        let config = ParquetWriterConfig::default();
        let writer = ParquetWriter::new(config);

        // Test trait methods
        assert_eq!(writer.name(), "parquet");
        assert_eq!(writer.extension(), ".parquet");
        assert_eq!(writer.content_type(), "application/vnd.apache.parquet");
        assert!(!writer.supports_append());

        // Test write via trait
        let events = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob", "age": 25}),
        ];

        // Use trait method instead of inherent method
        let format_writer: &dyn FormatWriter = &writer;
        let bytes = format_writer.write_batch(&events).unwrap();

        // Verify it's a valid Parquet file (magic bytes: "PAR1")
        assert!(bytes.len() > 4);
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[test]
    fn test_format_writer_empty_batch_error() {
        use crate::format::FormatWriter;

        let writer = ParquetWriter::new(ParquetWriterConfig::default());
        let format_writer: &dyn FormatWriter = &writer;

        let result = format_writer.write_batch(&[]);
        assert!(result.is_err());
    }
}
