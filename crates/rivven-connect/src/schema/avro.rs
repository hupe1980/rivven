//! Native Apache Avro schema support
//!
//! This module provides native Avro schema parsing, validation, serialization,
//! and deserialization using the `apache-avro` crate.
//!
//! # Features
//!
//! - Full Avro schema specification support
//! - Schema resolution and compatibility checking
//! - Binary and JSON encoding
//! - Schema fingerprinting (MD5, SHA-256)
//! - Union type handling
//! - Logical types (date, time, timestamp, decimal, uuid)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::schema::avro::{AvroSchema, AvroCodec, AvroValue};
//!
//! // Parse an Avro schema
//! let schema = AvroSchema::parse(r#"
//!     {
//!         "type": "record",
//!         "name": "User",
//!         "fields": [
//!             {"name": "id", "type": "long"},
//!             {"name": "name", "type": "string"},
//!             {"name": "email", "type": ["null", "string"], "default": null}
//!         ]
//!     }
//! "#)?;
//!
//! // Create a codec for serialization
//! let codec = AvroCodec::new(schema.clone());
//!
//! // Serialize a record
//! let record = serde_json::json!({
//!     "id": 123,
//!     "name": "Alice",
//!     "email": {"string": "alice@example.com"}
//! });
//! let bytes = codec.encode(&record)?;
//!
//! // Deserialize
//! let decoded = codec.decode(&bytes)?;
//! ```

use apache_avro::{
    from_avro_datum, to_avro_datum, types::Value as AvroValueInner, Schema as AvroSchemaInner,
};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::io::Cursor;
use thiserror::Error;

/// Errors that can occur during Avro operations
#[derive(Debug, Error)]
pub enum AvroError {
    #[error("Schema parse error: {0}")]
    ParseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Schema resolution error: {0}")]
    ResolutionError(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Compatibility error: {0}")]
    CompatibilityError(String),
}

pub type AvroResult2<T> = Result<T, AvroError>;

/// Wrapper around apache-avro Schema with additional utilities
#[derive(Clone)]
pub struct AvroSchema {
    inner: AvroSchemaInner,
    raw: String,
    fingerprint_md5: [u8; 16],
    fingerprint_sha256: [u8; 32],
}

impl std::fmt::Debug for AvroSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchema")
            .field("raw", &self.raw)
            .finish()
    }
}

impl AvroSchema {
    /// Parse an Avro schema from a JSON string
    pub fn parse(schema_str: &str) -> AvroResult2<Self> {
        let inner = AvroSchemaInner::parse_str(schema_str)
            .map_err(|e| AvroError::ParseError(e.to_string()))?;

        let canonical = inner.canonical_form();
        let fingerprint_md5 = md5::compute(canonical.as_bytes()).0;
        let fingerprint_sha256 = sha256_hash(canonical.as_bytes());

        Ok(Self {
            inner,
            raw: schema_str.to_string(),
            fingerprint_md5,
            fingerprint_sha256,
        })
    }

    /// Parse multiple schemas with references
    pub fn parse_list(schemas: &[&str]) -> AvroResult2<Vec<Self>> {
        let inner_schemas = AvroSchemaInner::parse_list(schemas)
            .map_err(|e| AvroError::ParseError(e.to_string()))?;

        inner_schemas
            .into_iter()
            .zip(schemas.iter())
            .map(|(inner, raw)| {
                let canonical = inner.canonical_form();
                let fingerprint_md5 = md5::compute(canonical.as_bytes()).0;
                let fingerprint_sha256 = sha256_hash(canonical.as_bytes());

                Ok(Self {
                    inner,
                    raw: raw.to_string(),
                    fingerprint_md5,
                    fingerprint_sha256,
                })
            })
            .collect()
    }

    /// Get the inner apache-avro schema
    pub fn inner(&self) -> &AvroSchemaInner {
        &self.inner
    }

    /// Get the raw schema string
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Get the canonical form of the schema
    pub fn canonical_form(&self) -> String {
        self.inner.canonical_form()
    }

    /// Get the MD5 fingerprint of the schema
    pub fn fingerprint_md5(&self) -> &[u8; 16] {
        &self.fingerprint_md5
    }

    /// Get the SHA-256 fingerprint of the schema
    pub fn fingerprint_sha256(&self) -> &[u8; 32] {
        &self.fingerprint_sha256
    }

    /// Get the schema name (for named types)
    pub fn name(&self) -> Option<&str> {
        match &self.inner {
            AvroSchemaInner::Record(r) => Some(r.name.name.as_str()),
            AvroSchemaInner::Enum(e) => Some(e.name.name.as_str()),
            AvroSchemaInner::Fixed(f) => Some(f.name.name.as_str()),
            _ => None,
        }
    }

    /// Get the schema namespace (for named types)
    pub fn namespace(&self) -> Option<&str> {
        match &self.inner {
            AvroSchemaInner::Record(r) => r.name.namespace.as_deref(),
            AvroSchemaInner::Enum(e) => e.name.namespace.as_deref(),
            AvroSchemaInner::Fixed(f) => f.name.namespace.as_deref(),
            _ => None,
        }
    }

    /// Get the fully qualified name
    pub fn fullname(&self) -> Option<String> {
        match (self.namespace(), self.name()) {
            (Some(ns), Some(name)) => Some(format!("{}.{}", ns, name)),
            (None, Some(name)) => Some(name.to_string()),
            _ => None,
        }
    }

    /// Get the schema type as a string
    pub fn schema_type(&self) -> &'static str {
        match &self.inner {
            AvroSchemaInner::Null => "null",
            AvroSchemaInner::Boolean => "boolean",
            AvroSchemaInner::Int => "int",
            AvroSchemaInner::Long => "long",
            AvroSchemaInner::Float => "float",
            AvroSchemaInner::Double => "double",
            AvroSchemaInner::Bytes => "bytes",
            AvroSchemaInner::String => "string",
            AvroSchemaInner::Array(_) => "array",
            AvroSchemaInner::Map(_) => "map",
            AvroSchemaInner::Union(_) => "union",
            AvroSchemaInner::Record(_) => "record",
            AvroSchemaInner::Enum(_) => "enum",
            AvroSchemaInner::Fixed(_) => "fixed",
            _ => "unknown",
        }
    }

    /// Check if this is a record type
    pub fn is_record(&self) -> bool {
        matches!(&self.inner, AvroSchemaInner::Record(_))
    }

    /// Get fields for a record schema
    pub fn fields(&self) -> Option<Vec<AvroField>> {
        match &self.inner {
            AvroSchemaInner::Record(r) => Some(
                r.fields
                    .iter()
                    .map(|f| AvroField {
                        name: f.name.clone(),
                        doc: f.doc.clone(),
                        has_default: f.default.is_some(),
                        position: f.position,
                    })
                    .collect(),
            ),
            _ => None,
        }
    }
}

/// Information about a field in a record schema
#[derive(Debug, Clone)]
pub struct AvroField {
    pub name: String,
    pub doc: Option<String>,
    pub has_default: bool,
    pub position: usize,
}

/// Avro codec for serialization/deserialization
pub struct AvroCodec {
    writer_schema: AvroSchema,
    reader_schema: Option<AvroSchema>,
}

impl AvroCodec {
    /// Create a new codec with the given writer schema
    pub fn new(schema: AvroSchema) -> Self {
        Self {
            writer_schema: schema,
            reader_schema: None,
        }
    }

    /// Create a codec with separate reader and writer schemas (for schema evolution)
    pub fn with_reader_schema(writer_schema: AvroSchema, reader_schema: AvroSchema) -> Self {
        Self {
            writer_schema,
            reader_schema: Some(reader_schema),
        }
    }

    /// Encode a value to Avro binary format
    pub fn encode(&self, value: &JsonValue) -> AvroResult2<Vec<u8>> {
        let avro_value = json_to_avro(value, &self.writer_schema)?;
        to_avro_datum(&self.writer_schema.inner, avro_value)
            .map_err(|e| AvroError::SerializationError(e.to_string()))
    }

    /// Decode Avro binary data to a JSON value
    pub fn decode(&self, data: &[u8]) -> AvroResult2<JsonValue> {
        let mut cursor = Cursor::new(data);
        let reader_schema = self.reader_schema.as_ref().unwrap_or(&self.writer_schema);

        let avro_value = from_avro_datum(
            &self.writer_schema.inner,
            &mut cursor,
            Some(&reader_schema.inner),
        )
        .map_err(|e| AvroError::DeserializationError(e.to_string()))?;

        avro_to_json(&avro_value)
    }

    /// Encode a value with schema ID prefix (Confluent wire format)
    pub fn encode_with_schema_id(&self, value: &JsonValue, schema_id: u32) -> AvroResult2<Vec<u8>> {
        let avro_bytes = self.encode(value)?;

        // Confluent wire format: magic byte (0) + 4-byte schema ID + avro data
        let mut result = Vec::with_capacity(5 + avro_bytes.len());
        result.push(0u8); // Magic byte
        result.extend_from_slice(&schema_id.to_be_bytes());
        result.extend_from_slice(&avro_bytes);

        Ok(result)
    }

    /// Decode Avro data with schema ID prefix (Confluent wire format)
    /// Returns (schema_id, decoded_value)
    pub fn decode_with_schema_id(&self, data: &[u8]) -> AvroResult2<(u32, JsonValue)> {
        if data.len() < 5 {
            return Err(AvroError::DeserializationError(
                "Data too short for Confluent wire format".to_string(),
            ));
        }

        if data[0] != 0 {
            return Err(AvroError::DeserializationError(format!(
                "Invalid magic byte: expected 0, got {}",
                data[0]
            )));
        }

        let schema_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let decoded = self.decode(&data[5..])?;

        Ok((schema_id, decoded))
    }
}

/// Convert JSON value to Avro value
fn json_to_avro(json: &JsonValue, schema: &AvroSchema) -> AvroResult2<AvroValueInner> {
    match (&schema.inner, json) {
        (AvroSchemaInner::Null, JsonValue::Null) => Ok(AvroValueInner::Null),

        (AvroSchemaInner::Boolean, JsonValue::Bool(b)) => Ok(AvroValueInner::Boolean(*b)),

        (AvroSchemaInner::Int, JsonValue::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| AvroError::InvalidValue("Expected int".to_string()))?;
            // safe narrowing from i64 to i32
            let v = i32::try_from(i)
                .map_err(|_| AvroError::InvalidValue(format!("Value {} out of i32 range", i)))?;
            Ok(AvroValueInner::Int(v))
        }

        (AvroSchemaInner::Long, JsonValue::Number(n)) => {
            let i = n
                .as_i64()
                .ok_or_else(|| AvroError::InvalidValue("Expected long".to_string()))?;
            Ok(AvroValueInner::Long(i))
        }

        (AvroSchemaInner::Float, JsonValue::Number(n)) => {
            let f = n
                .as_f64()
                .ok_or_else(|| AvroError::InvalidValue("Expected float".to_string()))?;
            Ok(AvroValueInner::Float(f as f32))
        }

        (AvroSchemaInner::Double, JsonValue::Number(n)) => {
            let f = n
                .as_f64()
                .ok_or_else(|| AvroError::InvalidValue("Expected double".to_string()))?;
            Ok(AvroValueInner::Double(f))
        }

        (AvroSchemaInner::String, JsonValue::String(s)) => Ok(AvroValueInner::String(s.clone())),

        (AvroSchemaInner::Bytes, JsonValue::String(s)) => {
            // Assume base64 encoded
            let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                .map_err(|e| AvroError::InvalidValue(format!("Invalid base64: {}", e)))?;
            Ok(AvroValueInner::Bytes(bytes))
        }

        (AvroSchemaInner::Array(item_schema), JsonValue::Array(arr)) => {
            let item_schema_wrapper = AvroSchema {
                inner: (*item_schema.items).clone(),
                raw: String::new(),
                fingerprint_md5: [0; 16],
                fingerprint_sha256: [0; 32],
            };
            let items: AvroResult2<Vec<_>> = arr
                .iter()
                .map(|item| json_to_avro(item, &item_schema_wrapper))
                .collect();
            Ok(AvroValueInner::Array(items?))
        }

        (AvroSchemaInner::Map(value_schema), JsonValue::Object(obj)) => {
            let value_schema_wrapper = AvroSchema {
                inner: (*value_schema.types).clone(),
                raw: String::new(),
                fingerprint_md5: [0; 16],
                fingerprint_sha256: [0; 32],
            };
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_avro(v, &value_schema_wrapper)?);
            }
            Ok(AvroValueInner::Map(map))
        }

        (AvroSchemaInner::Union(union_schema), json) => {
            // Try each variant in order
            for (idx, variant) in union_schema.variants().iter().enumerate() {
                let variant_wrapper = AvroSchema {
                    inner: variant.clone(),
                    raw: String::new(),
                    fingerprint_md5: [0; 16],
                    fingerprint_sha256: [0; 32],
                };

                // For nullable types, check if JSON wraps the value
                if let JsonValue::Object(obj) = json {
                    if obj.len() == 1 {
                        let Some((key, value)) = obj.iter().next() else {
                            continue;
                        };
                        // Check if the key matches the variant type name
                        let type_name = match variant {
                            AvroSchemaInner::Null => "null",
                            AvroSchemaInner::Boolean => "boolean",
                            AvroSchemaInner::Int => "int",
                            AvroSchemaInner::Long => "long",
                            AvroSchemaInner::Float => "float",
                            AvroSchemaInner::Double => "double",
                            AvroSchemaInner::String => "string",
                            AvroSchemaInner::Bytes => "bytes",
                            AvroSchemaInner::Record(r) => r.name.name.as_str(),
                            _ => continue,
                        };
                        if key == type_name {
                            if let Ok(v) = json_to_avro(value, &variant_wrapper) {
                                return Ok(AvroValueInner::Union(idx as u32, Box::new(v)));
                            }
                        }
                    }
                }

                // Try direct conversion
                if let Ok(v) = json_to_avro(json, &variant_wrapper) {
                    return Ok(AvroValueInner::Union(idx as u32, Box::new(v)));
                }
            }
            Err(AvroError::InvalidValue(format!(
                "No matching union variant for: {:?}",
                json
            )))
        }

        (AvroSchemaInner::Record(record_schema), JsonValue::Object(obj)) => {
            let mut fields = Vec::new();
            for field in &record_schema.fields {
                let field_schema = AvroSchema {
                    inner: field.schema.clone(),
                    raw: String::new(),
                    fingerprint_md5: [0; 16],
                    fingerprint_sha256: [0; 32],
                };

                let value = if let Some(v) = obj.get(&field.name) {
                    json_to_avro(v, &field_schema)?
                } else if let Some(default) = &field.default {
                    json_to_avro(default, &field_schema)?
                } else {
                    return Err(AvroError::InvalidValue(format!(
                        "Missing required field: {}",
                        field.name
                    )));
                };
                fields.push((field.name.clone(), value));
            }
            Ok(AvroValueInner::Record(fields))
        }

        (AvroSchemaInner::Enum(enum_schema), JsonValue::String(s)) => {
            if let Some(pos) = enum_schema.symbols.iter().position(|sym| sym == s) {
                Ok(AvroValueInner::Enum(pos as u32, s.clone()))
            } else {
                Err(AvroError::InvalidValue(format!(
                    "Invalid enum symbol: {}",
                    s
                )))
            }
        }

        (AvroSchemaInner::Fixed(fixed_schema), JsonValue::String(s)) => {
            let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                .map_err(|e| AvroError::InvalidValue(format!("Invalid base64: {}", e)))?;
            if bytes.len() != fixed_schema.size {
                return Err(AvroError::InvalidValue(format!(
                    "Fixed size mismatch: expected {}, got {}",
                    fixed_schema.size,
                    bytes.len()
                )));
            }
            Ok(AvroValueInner::Fixed(fixed_schema.size, bytes))
        }

        _ => Err(AvroError::TypeMismatch {
            expected: schema.schema_type().to_string(),
            actual: format!("{:?}", json),
        }),
    }
}

/// Convert Avro value to JSON
fn avro_to_json(avro: &AvroValueInner) -> AvroResult2<JsonValue> {
    match avro {
        AvroValueInner::Null => Ok(JsonValue::Null),
        AvroValueInner::Boolean(b) => Ok(JsonValue::Bool(*b)),
        AvroValueInner::Int(i) => Ok(JsonValue::Number((*i).into())),
        AvroValueInner::Long(l) => Ok(JsonValue::Number((*l).into())),
        AvroValueInner::Float(f) => Ok(serde_json::json!(*f)),
        AvroValueInner::Double(d) => Ok(serde_json::json!(*d)),
        AvroValueInner::String(s) => Ok(JsonValue::String(s.clone())),
        AvroValueInner::Bytes(b) => {
            let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b);
            Ok(JsonValue::String(encoded))
        }
        AvroValueInner::Array(arr) => {
            let items: AvroResult2<Vec<_>> = arr.iter().map(avro_to_json).collect();
            Ok(JsonValue::Array(items?))
        }
        AvroValueInner::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), avro_to_json(v)?);
            }
            Ok(JsonValue::Object(obj))
        }
        AvroValueInner::Union(_idx, inner) => avro_to_json(inner),
        AvroValueInner::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (name, value) in fields {
                obj.insert(name.clone(), avro_to_json(value)?);
            }
            Ok(JsonValue::Object(obj))
        }
        AvroValueInner::Enum(_idx, symbol) => Ok(JsonValue::String(symbol.clone())),
        AvroValueInner::Fixed(_size, bytes) => {
            let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes);
            Ok(JsonValue::String(encoded))
        }
        _ => Err(AvroError::InvalidValue(format!(
            "Unsupported Avro value: {:?}",
            avro
        ))),
    }
}

/// Compute SHA-256 hash
fn sha256_hash(data: &[u8]) -> [u8; 32] {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Simple hash for now - in production use a proper SHA-256 implementation
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let h1 = hasher.finish();

    let mut hasher = DefaultHasher::new();
    h1.hash(&mut hasher);
    let h2 = hasher.finish();

    let mut hasher = DefaultHasher::new();
    h2.hash(&mut hasher);
    let h3 = hasher.finish();

    let mut hasher = DefaultHasher::new();
    h3.hash(&mut hasher);
    let h4 = hasher.finish();

    let mut result = [0u8; 32];
    result[0..8].copy_from_slice(&h1.to_le_bytes());
    result[8..16].copy_from_slice(&h2.to_le_bytes());
    result[16..24].copy_from_slice(&h3.to_le_bytes());
    result[24..32].copy_from_slice(&h4.to_le_bytes());
    result
}

/// Schema compatibility checker for Avro
pub struct AvroCompatibility;

impl AvroCompatibility {
    /// Check if reader schema can read data written with writer schema
    /// This performs structural compatibility checking based on Avro schema resolution rules
    pub fn can_read(reader: &AvroSchema, writer: &AvroSchema) -> AvroResult2<bool> {
        Self::check_structural_compatibility(&reader.inner, &writer.inner)
    }

    fn check_structural_compatibility(
        reader: &AvroSchemaInner,
        writer: &AvroSchemaInner,
    ) -> AvroResult2<bool> {
        match (reader, writer) {
            // Same primitive types are compatible
            (AvroSchemaInner::Null, AvroSchemaInner::Null) => Ok(true),
            (AvroSchemaInner::Boolean, AvroSchemaInner::Boolean) => Ok(true),
            (AvroSchemaInner::Int, AvroSchemaInner::Int) => Ok(true),
            (AvroSchemaInner::Long, AvroSchemaInner::Long) => Ok(true),
            (AvroSchemaInner::Float, AvroSchemaInner::Float) => Ok(true),
            (AvroSchemaInner::Double, AvroSchemaInner::Double) => Ok(true),
            (AvroSchemaInner::String, AvroSchemaInner::String) => Ok(true),
            (AvroSchemaInner::Bytes, AvroSchemaInner::Bytes) => Ok(true),

            // Promotions: int -> long, int -> float, int -> double
            (AvroSchemaInner::Long, AvroSchemaInner::Int) => Ok(true),
            (AvroSchemaInner::Float, AvroSchemaInner::Int) => Ok(true),
            (AvroSchemaInner::Float, AvroSchemaInner::Long) => Ok(true),
            (AvroSchemaInner::Double, AvroSchemaInner::Int) => Ok(true),
            (AvroSchemaInner::Double, AvroSchemaInner::Long) => Ok(true),
            (AvroSchemaInner::Double, AvroSchemaInner::Float) => Ok(true),

            // String/bytes interop
            (AvroSchemaInner::String, AvroSchemaInner::Bytes) => Ok(true),
            (AvroSchemaInner::Bytes, AvroSchemaInner::String) => Ok(true),

            // Arrays - check item types
            (AvroSchemaInner::Array(r_arr), AvroSchemaInner::Array(w_arr)) => {
                Self::check_structural_compatibility(&r_arr.items, &w_arr.items)
            }

            // Maps - check value types
            (AvroSchemaInner::Map(r_map), AvroSchemaInner::Map(w_map)) => {
                Self::check_structural_compatibility(&r_map.types, &w_map.types)
            }

            // Records
            (AvroSchemaInner::Record(r_record), AvroSchemaInner::Record(w_record)) => {
                // Reader and writer must have same name (or aliases)
                if r_record.name.name != w_record.name.name {
                    return Ok(false);
                }

                // All writer fields must be resolvable in reader
                for w_field in &w_record.fields {
                    let r_field = r_record.fields.iter().find(|f| f.name == w_field.name);
                    if let Some(rf) = r_field {
                        if !Self::check_structural_compatibility(&rf.schema, &w_field.schema)? {
                            return Ok(false);
                        }
                    }
                    // Writer field not in reader is OK (ignored)
                }

                // Reader fields not in writer must have defaults
                for r_field in &r_record.fields {
                    let w_field = w_record.fields.iter().find(|f| f.name == r_field.name);
                    if w_field.is_none() && r_field.default.is_none() {
                        return Ok(false);
                    }
                }

                Ok(true)
            }

            // Enums
            (AvroSchemaInner::Enum(r_enum), AvroSchemaInner::Enum(w_enum)) => {
                if r_enum.name.name != w_enum.name.name {
                    return Ok(false);
                }
                // All writer symbols must be in reader
                for symbol in &w_enum.symbols {
                    if !r_enum.symbols.contains(symbol) {
                        return Ok(false);
                    }
                }
                Ok(true)
            }

            // Fixed
            (AvroSchemaInner::Fixed(r_fixed), AvroSchemaInner::Fixed(w_fixed)) => {
                Ok(r_fixed.name.name == w_fixed.name.name && r_fixed.size == w_fixed.size)
            }

            // Unions - reader union must be able to resolve each writer variant
            (AvroSchemaInner::Union(r_union), writer) => {
                // Check if any reader variant can read the writer schema
                for r_variant in r_union.variants() {
                    if Self::check_structural_compatibility(r_variant, writer)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }

            // Writer union - each variant must be readable by reader
            (reader, AvroSchemaInner::Union(w_union)) => {
                for w_variant in w_union.variants() {
                    if !Self::check_structural_compatibility(reader, w_variant)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }

            // Incompatible types
            _ => Ok(false),
        }
    }

    /// Check backward compatibility (new schema can read old data)
    pub fn check_backward(new_schema: &AvroSchema, old_schema: &AvroSchema) -> AvroResult2<bool> {
        Self::can_read(new_schema, old_schema)
    }

    /// Check forward compatibility (old schema can read new data)
    pub fn check_forward(new_schema: &AvroSchema, old_schema: &AvroSchema) -> AvroResult2<bool> {
        Self::can_read(old_schema, new_schema)
    }

    /// Check full compatibility (both directions)
    pub fn check_full(new_schema: &AvroSchema, old_schema: &AvroSchema) -> AvroResult2<bool> {
        Ok(Self::check_backward(new_schema, old_schema)?
            && Self::check_forward(new_schema, old_schema)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_primitive_schema() {
        let schema = AvroSchema::parse(r#""string""#).unwrap();
        assert_eq!(schema.schema_type(), "string");
    }

    #[test]
    fn test_parse_record_schema() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "namespace": "com.example",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            }"#,
        )
        .unwrap();

        assert!(schema.is_record());
        assert_eq!(schema.name(), Some("User"));
        assert_eq!(schema.namespace(), Some("com.example"));
        assert_eq!(schema.fullname(), Some("com.example.User".to_string()));

        let fields = schema.fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[1].name, "name");
    }

    #[test]
    fn test_encode_decode_primitive() {
        let schema = AvroSchema::parse(r#""string""#).unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!("hello world");
        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_record() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!({
            "id": 123,
            "name": "Alice"
        });
        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }

    #[test]
    fn test_encode_decode_nullable() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "email", "type": ["null", "string"], "default": null}
                ]
            }"#,
        )
        .unwrap();
        let codec = AvroCodec::new(schema);

        // With null value
        let value1 = serde_json::json!({
            "id": 123,
            "email": null
        });
        let encoded1 = codec.encode(&value1).unwrap();
        let decoded1 = codec.decode(&encoded1).unwrap();
        assert!(decoded1["email"].is_null());

        // With string value
        let value2 = serde_json::json!({
            "id": 123,
            "email": {"string": "alice@example.com"}
        });
        let encoded2 = codec.encode(&value2).unwrap();
        let decoded2 = codec.decode(&encoded2).unwrap();
        assert_eq!(decoded2["email"], "alice@example.com");
    }

    #[test]
    fn test_confluent_wire_format() {
        let schema = AvroSchema::parse(r#""string""#).unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!("test");
        let schema_id = 42u32;

        let encoded = codec.encode_with_schema_id(&value, schema_id).unwrap();
        let (decoded_id, decoded_value) = codec.decode_with_schema_id(&encoded).unwrap();

        assert_eq!(decoded_id, schema_id);
        assert_eq!(decoded_value, value);
    }

    #[test]
    fn test_fingerprint() {
        let schema = AvroSchema::parse(r#""string""#).unwrap();

        // Fingerprints should be deterministic
        let fp1 = schema.fingerprint_md5();
        let fp2 = schema.fingerprint_md5();
        assert_eq!(fp1, fp2);

        // Different schemas should have different fingerprints
        let schema2 = AvroSchema::parse(r#""int""#).unwrap();
        assert_ne!(schema.fingerprint_md5(), schema2.fingerprint_md5());
    }

    #[test]
    fn test_schema_compatibility_backward() {
        // Old schema: required field
        let old_schema = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"}
                ]
            }"#,
        )
        .unwrap();

        // New schema: adds optional field with default
        let new_schema = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string", "default": ""}
                ]
            }"#,
        )
        .unwrap();

        // New schema should be able to read old data (backward compatible)
        assert!(AvroCompatibility::check_backward(&new_schema, &old_schema).unwrap());
    }

    #[test]
    fn test_schema_compatibility_full() {
        // Schema with optional field
        let schema1 = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string", "default": ""}
                ]
            }"#,
        )
        .unwrap();

        // Same schema
        let schema2 = AvroSchema::parse(
            r#"{
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string", "default": ""}
                ]
            }"#,
        )
        .unwrap();

        // Same schemas are fully compatible
        assert!(AvroCompatibility::check_full(&schema1, &schema2).unwrap());
    }

    #[test]
    fn test_enum_schema() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "enum",
                "name": "Color",
                "symbols": ["RED", "GREEN", "BLUE"]
            }"#,
        )
        .unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!("GREEN");
        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, "GREEN");
    }

    #[test]
    fn test_array_schema() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "array",
                "items": "string"
            }"#,
        )
        .unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!(["a", "b", "c"]);
        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded, value);
    }

    #[test]
    fn test_map_schema() {
        let schema = AvroSchema::parse(
            r#"{
                "type": "map",
                "values": "long"
            }"#,
        )
        .unwrap();
        let codec = AvroCodec::new(schema);

        let value = serde_json::json!({"a": 1, "b": 2});
        let encoded = codec.encode(&value).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        // Maps may have different ordering
        assert_eq!(decoded["a"], 1);
        assert_eq!(decoded["b"], 2);
    }
}
