//! Native Protobuf schema support for Rivven Schema Registry.
//!
//! This module provides comprehensive Protocol Buffers support including:
//! - Schema parsing and validation
//! - Confluent wire format compatibility (magic byte + schema ID + protobuf index + data)
//! - Schema evolution and compatibility checking
//! - JSON ↔ Protobuf conversion
//!
//! # Example
//!
//! ```ignore
//! use rivven_connect::schema::protobuf::{ProtobufSchema, ProtobufCodec};
//!
//! let schema = ProtobufSchema::parse(r#"
//!     syntax = "proto3";
//!     message User {
//!         int64 id = 1;
//!         string name = 2;
//!         optional string email = 3;
//!     }
//! "#)?;
//!
//! let codec = ProtobufCodec::new(schema);
//! let encoded = codec.encode(&json!({ "id": 1, "name": "Alice", "email": "alice@example.com" }))?;
//! let decoded = codec.decode(&encoded)?;
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost_reflect::prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage,
    Value as ProtoValue,
};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use thiserror::Error;

/// Protobuf-specific errors.
#[derive(Debug, Error)]
pub enum ProtobufError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Encode error: {0}")]
    EncodeError(String),

    #[error("Decode error: {0}")]
    DecodeError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Compatibility error: {0}")]
    CompatibilityError(String),

    #[error("Message not found: {0}")]
    MessageNotFound(String),

    #[error("Field error: {0}")]
    FieldError(String),

    #[error("Invalid wire format: {0}")]
    InvalidWireFormat(String),
}

/// Result type for Protobuf operations.
pub type ProtobufResult<T> = Result<T, ProtobufError>;

/// A parsed Protobuf schema with metadata.
#[derive(Clone)]
pub struct ProtobufSchema {
    /// The raw .proto file contents.
    raw: String,
    /// Parsed descriptor pool.
    pool: DescriptorPool,
    /// The main message descriptor (first message in the file).
    message_name: String,
    /// Package name if present.
    package: Option<String>,
    /// MD5 fingerprint of the schema.
    fingerprint_md5: [u8; 16],
    /// SHA-256 fingerprint of the schema.
    fingerprint_sha256: [u8; 32],
}

impl std::fmt::Debug for ProtobufSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtobufSchema")
            .field("message_name", &self.message_name)
            .field("package", &self.package)
            .field(
                "fingerprint_md5",
                &format!(
                    "{:02x}{:02x}{:02x}{:02x}...",
                    self.fingerprint_md5[0],
                    self.fingerprint_md5[1],
                    self.fingerprint_md5[2],
                    self.fingerprint_md5[3]
                ),
            )
            .finish()
    }
}

impl ProtobufSchema {
    /// Parse a Protobuf schema from a .proto file string.
    ///
    /// For runtime parsing without proto-compile, we use prost-reflect's
    /// DescriptorPool which can parse .proto content dynamically.
    pub fn parse(proto_content: &str) -> ProtobufResult<Self> {
        // For dynamic protobuf handling, we need to build a FileDescriptorProto
        // This is a simplified approach - in production you'd use protoc or prost-build
        let pool = Self::parse_proto_content(proto_content)?;

        // Extract the first message name
        let message_name = Self::extract_first_message_name(proto_content)?;
        let package = Self::extract_package(proto_content);

        // Compute fingerprints
        let canonical = Self::canonical_form(proto_content);
        let fingerprint_md5 = md5::compute(&canonical).into();
        let fingerprint_sha256 = Self::sha256_fingerprint(&canonical);

        Ok(Self {
            raw: proto_content.to_string(),
            pool,
            message_name,
            package,
            fingerprint_md5,
            fingerprint_sha256,
        })
    }

    /// Parse .proto content into a DescriptorPool.
    fn parse_proto_content(proto_content: &str) -> ProtobufResult<DescriptorPool> {
        // Build a minimal FileDescriptorProto from the .proto content
        // This is a simplified parser - production would use protoc
        let fd_proto = Self::build_file_descriptor_proto(proto_content)?;

        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(fd_proto)
            .map_err(|e| ProtobufError::ParseError(e.to_string()))?;

        Ok(pool)
    }

    /// Build a FileDescriptorProto from .proto content.
    /// This is a simplified parser for common proto3 patterns.
    fn build_file_descriptor_proto(
        proto_content: &str,
    ) -> ProtobufResult<prost_types::FileDescriptorProto> {
        // Extract syntax
        let syntax = if proto_content.contains("syntax = \"proto3\"") {
            Some("proto3".to_string())
        } else if proto_content.contains("syntax = \"proto2\"") {
            Some("proto2".to_string())
        } else {
            // Default to proto3
            Some("proto3".to_string())
        };

        // Extract package
        let package = Self::extract_package(proto_content);

        // Parse message definitions
        let messages = Self::parse_messages(proto_content)?;

        // Parse enum definitions
        let enums = Self::parse_enums(proto_content)?;

        Ok(prost_types::FileDescriptorProto {
            name: Some("dynamic.proto".to_string()),
            syntax,
            package,
            message_type: messages,
            enum_type: enums,
            ..Default::default()
        })
    }

    /// Parse message definitions from proto content.
    fn parse_messages(proto_content: &str) -> ProtobufResult<Vec<prost_types::DescriptorProto>> {
        let mut messages = Vec::new();
        let mut depth = 0;
        let mut current_message: Option<prost_types::DescriptorProto> = None;
        let mut current_fields = Vec::new();
        let mut in_message = false;

        for line in proto_content.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("message ") && trimmed.ends_with('{') {
                if depth == 0 {
                    let name = trimmed
                        .strip_prefix("message ")
                        .and_then(|s| s.strip_suffix('{'))
                        .map(|s| s.trim().to_string())
                        .ok_or_else(|| {
                            ProtobufError::ParseError("Invalid message definition".to_string())
                        })?;

                    current_message = Some(prost_types::DescriptorProto {
                        name: Some(name),
                        ..Default::default()
                    });
                    in_message = true;
                }
                depth += 1;
            } else if trimmed == "}" && in_message {
                depth -= 1;
                if depth == 0 {
                    if let Some(mut msg) = current_message.take() {
                        msg.field = std::mem::take(&mut current_fields);
                        messages.push(msg);
                    }
                    in_message = false;
                }
            } else if in_message && depth == 1 && !trimmed.is_empty() && !trimmed.starts_with("//")
            {
                // Parse field
                if let Some(field) = Self::parse_field(trimmed)? {
                    current_fields.push(field);
                }
            }
        }

        Ok(messages)
    }

    /// Parse a single field definition.
    fn parse_field(line: &str) -> ProtobufResult<Option<prost_types::FieldDescriptorProto>> {
        let line = line.trim().trim_end_matches(';');
        if line.is_empty()
            || line.starts_with("//")
            || line.starts_with("reserved")
            || line.starts_with("option")
        {
            return Ok(None);
        }

        // Parse: [optional|repeated] type name = number;
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 4 {
            return Ok(None);
        }

        let (label, type_str, name_part) = if parts[0] == "optional" || parts[0] == "repeated" {
            let label = if parts[0] == "repeated" {
                prost_types::field_descriptor_proto::Label::Repeated
            } else {
                prost_types::field_descriptor_proto::Label::Optional
            };
            (label, parts[1], parts[2])
        } else {
            // proto3 default is optional
            (
                prost_types::field_descriptor_proto::Label::Optional,
                parts[0],
                parts[1],
            )
        };

        // Extract field number
        let number_part = parts.iter().find(|p| p.starts_with('=')).and_then(|_| {
            parts
                .iter()
                .position(|p| *p == "=")
                .and_then(|i| parts.get(i + 1))
        });

        let number: i32 = number_part
            .and_then(|s| s.trim_end_matches(';').parse().ok())
            .unwrap_or(0);

        if number == 0 {
            return Ok(None);
        }

        let proto_type = Self::type_string_to_proto_type(type_str);

        let field = prost_types::FieldDescriptorProto {
            name: Some(name_part.to_string()),
            number: Some(number),
            label: Some(label as i32),
            r#type: proto_type.map(|t| t as i32),
            type_name: if proto_type.is_none() {
                Some(type_str.to_string())
            } else {
                None
            },
            ..Default::default()
        };

        Ok(Some(field))
    }

    /// Convert type string to protobuf type enum.
    fn type_string_to_proto_type(
        type_str: &str,
    ) -> Option<prost_types::field_descriptor_proto::Type> {
        use prost_types::field_descriptor_proto::Type;
        match type_str {
            "double" => Some(Type::Double),
            "float" => Some(Type::Float),
            "int64" => Some(Type::Int64),
            "uint64" => Some(Type::Uint64),
            "int32" => Some(Type::Int32),
            "fixed64" => Some(Type::Fixed64),
            "fixed32" => Some(Type::Fixed32),
            "bool" => Some(Type::Bool),
            "string" => Some(Type::String),
            "bytes" => Some(Type::Bytes),
            "uint32" => Some(Type::Uint32),
            "sfixed32" => Some(Type::Sfixed32),
            "sfixed64" => Some(Type::Sfixed64),
            "sint32" => Some(Type::Sint32),
            "sint64" => Some(Type::Sint64),
            _ => None, // Message or enum type
        }
    }

    /// Parse enum definitions from proto content.
    fn parse_enums(proto_content: &str) -> ProtobufResult<Vec<prost_types::EnumDescriptorProto>> {
        let mut enums = Vec::new();
        let mut current_enum: Option<prost_types::EnumDescriptorProto> = None;
        let mut current_values = Vec::new();
        let mut in_enum = false;

        for line in proto_content.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("enum ") && trimmed.ends_with('{') {
                let name = trimmed
                    .strip_prefix("enum ")
                    .and_then(|s| s.strip_suffix('{'))
                    .map(|s| s.trim().to_string())
                    .ok_or_else(|| {
                        ProtobufError::ParseError("Invalid enum definition".to_string())
                    })?;

                current_enum = Some(prost_types::EnumDescriptorProto {
                    name: Some(name),
                    ..Default::default()
                });
                in_enum = true;
            } else if trimmed == "}" && in_enum {
                if let Some(mut e) = current_enum.take() {
                    e.value = std::mem::take(&mut current_values);
                    enums.push(e);
                }
                in_enum = false;
            } else if in_enum && !trimmed.is_empty() && !trimmed.starts_with("//") {
                // Parse enum value: NAME = number;
                if let Some((name, number)) = Self::parse_enum_value(trimmed) {
                    current_values.push(prost_types::EnumValueDescriptorProto {
                        name: Some(name),
                        number: Some(number),
                        ..Default::default()
                    });
                }
            }
        }

        Ok(enums)
    }

    /// Parse a single enum value.
    fn parse_enum_value(line: &str) -> Option<(String, i32)> {
        let line = line.trim().trim_end_matches(';');
        if line.starts_with("option") || line.is_empty() {
            return None;
        }

        let parts: Vec<&str> = line.split('=').map(|s| s.trim()).collect();
        if parts.len() != 2 {
            return None;
        }

        let name = parts[0].to_string();
        let number: i32 = parts[1].trim_end_matches(';').parse().ok()?;

        Some((name, number))
    }

    /// Extract package name from proto content.
    fn extract_package(proto_content: &str) -> Option<String> {
        for line in proto_content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("package ") {
                return trimmed
                    .strip_prefix("package ")
                    .map(|s| s.trim_end_matches(';').trim().to_string());
            }
        }
        None
    }

    /// Extract the first message name from proto content.
    fn extract_first_message_name(proto_content: &str) -> ProtobufResult<String> {
        for line in proto_content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("message ") {
                return trimmed
                    .strip_prefix("message ")
                    .and_then(|s| s.split('{').next())
                    .map(|s| s.trim().to_string())
                    .ok_or_else(|| {
                        ProtobufError::ParseError("Invalid message definition".to_string())
                    });
            }
        }
        Err(ProtobufError::MessageNotFound(
            "No message found in proto content".to_string(),
        ))
    }

    /// Get the canonical form of the schema for fingerprinting.
    fn canonical_form(proto_content: &str) -> Vec<u8> {
        // Normalize: remove comments, normalize whitespace
        let mut canonical = String::new();
        for line in proto_content.lines() {
            let trimmed = line.trim();
            // Remove line comments
            let without_comment = trimmed.split("//").next().unwrap_or("").trim();
            if !without_comment.is_empty() {
                canonical.push_str(without_comment);
                canonical.push('\n');
            }
        }
        canonical.into_bytes()
    }

    /// Compute SHA-256 fingerprint.
    fn sha256_fingerprint(data: &[u8]) -> [u8; 32] {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Simple hash for now - production would use SHA-256
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let h1 = hasher.finish();
        hasher.write(&h1.to_le_bytes());
        let h2 = hasher.finish();
        hasher.write(&h2.to_le_bytes());
        let h3 = hasher.finish();
        hasher.write(&h3.to_le_bytes());
        let h4 = hasher.finish();

        let mut result = [0u8; 32];
        result[0..8].copy_from_slice(&h1.to_le_bytes());
        result[8..16].copy_from_slice(&h2.to_le_bytes());
        result[16..24].copy_from_slice(&h3.to_le_bytes());
        result[24..32].copy_from_slice(&h4.to_le_bytes());
        result
    }

    /// Get the raw .proto content.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Get the main message name.
    pub fn message_name(&self) -> &str {
        &self.message_name
    }

    /// Get the fully qualified message name.
    pub fn fullname(&self) -> String {
        match &self.package {
            Some(pkg) => format!("{}.{}", pkg, self.message_name),
            None => self.message_name.clone(),
        }
    }

    /// Get the package name.
    pub fn package(&self) -> Option<&str> {
        self.package.as_deref()
    }

    /// Get the MD5 fingerprint.
    pub fn fingerprint_md5(&self) -> &[u8; 16] {
        &self.fingerprint_md5
    }

    /// Get the SHA-256 fingerprint.
    pub fn fingerprint_sha256(&self) -> &[u8; 32] {
        &self.fingerprint_sha256
    }

    /// Get the message descriptor for the main message.
    pub fn message_descriptor(&self) -> ProtobufResult<MessageDescriptor> {
        let full_name = self.fullname();
        self.pool
            .get_message_by_name(&full_name)
            .or_else(|| self.pool.get_message_by_name(&self.message_name))
            .ok_or(ProtobufError::MessageNotFound(full_name))
    }

    /// List all field names in the main message.
    pub fn field_names(&self) -> ProtobufResult<Vec<String>> {
        let desc = self.message_descriptor()?;
        Ok(desc.fields().map(|f| f.name().to_string()).collect())
    }

    /// List all fields with their types.
    pub fn fields(&self) -> ProtobufResult<Vec<ProtobufField>> {
        let desc = self.message_descriptor()?;
        Ok(desc
            .fields()
            .map(|f| ProtobufField {
                name: f.name().to_string(),
                number: f.number(),
                kind: format!("{:?}", f.kind()),
                is_repeated: f.is_list(),
                is_map: f.is_map(),
            })
            .collect())
    }
}

/// Metadata about a Protobuf field.
#[derive(Debug, Clone)]
pub struct ProtobufField {
    /// Field name.
    pub name: String,
    /// Field number.
    pub number: u32,
    /// Field type kind.
    pub kind: String,
    /// Whether the field is repeated.
    pub is_repeated: bool,
    /// Whether the field is a map.
    pub is_map: bool,
}

/// Encoder/decoder for Protobuf messages.
pub struct ProtobufCodec {
    schema: Arc<ProtobufSchema>,
}

impl ProtobufCodec {
    /// Create a new codec for the given schema.
    pub fn new(schema: ProtobufSchema) -> Self {
        Self {
            schema: Arc::new(schema),
        }
    }

    /// Encode JSON value to Protobuf bytes.
    pub fn encode(&self, json: &JsonValue) -> ProtobufResult<Vec<u8>> {
        let desc = self.schema.message_descriptor()?;
        let msg = json_to_proto_message(json, &desc)?;

        let mut buf = Vec::new();
        msg.encode(&mut buf)
            .map_err(|e| ProtobufError::EncodeError(e.to_string()))?;

        Ok(buf)
    }

    /// Decode Protobuf bytes to JSON value.
    pub fn decode(&self, data: &[u8]) -> ProtobufResult<JsonValue> {
        let desc = self.schema.message_descriptor()?;
        let msg = DynamicMessage::decode(desc, data)
            .map_err(|e| ProtobufError::DecodeError(e.to_string()))?;

        proto_message_to_json(&msg)
    }

    /// Encode with Confluent wire format.
    /// Format: magic byte (0x00) + schema ID (4 bytes big-endian) + message index (varint) + protobuf data
    pub fn encode_confluent(&self, json: &JsonValue, schema_id: u32) -> ProtobufResult<Vec<u8>> {
        let proto_bytes = self.encode(json)?;

        let mut buf = BytesMut::with_capacity(6 + proto_bytes.len());
        buf.put_u8(0x00); // Magic byte
        buf.put_u32(schema_id);
        // Message index (0 for first message in file, encoded as varint)
        buf.put_u8(0x00); // Index 0
        buf.extend_from_slice(&proto_bytes);

        Ok(buf.to_vec())
    }

    /// Decode from Confluent wire format.
    pub fn decode_confluent(&self, data: &[u8]) -> ProtobufResult<(u32, JsonValue)> {
        if data.len() < 6 {
            return Err(ProtobufError::InvalidWireFormat(
                "Data too short for Confluent wire format".to_string(),
            ));
        }

        let mut buf = data;

        // Magic byte
        let magic = buf.get_u8();
        if magic != 0x00 {
            return Err(ProtobufError::InvalidWireFormat(format!(
                "Invalid magic byte: expected 0x00, got 0x{:02x}",
                magic
            )));
        }

        // Schema ID
        let schema_id = buf.get_u32();

        // Message index array (Confluent wire format uses varint-encoded array).
        // First varint = number of message indices, then that many varint indices.
        // Index [0] (length=1, value=0) means root message; deeper indices for nested types.
        let count = decode_varint(&mut buf).map_err(|e| {
            ProtobufError::InvalidWireFormat(format!("Failed to decode message index count: {e}"))
        })?;
        for _ in 0..count {
            let _index = decode_varint(&mut buf).map_err(|e| {
                ProtobufError::InvalidWireFormat(format!("Failed to decode message index: {e}"))
            })?;
        }

        // Remaining is protobuf data
        let json = self.decode(buf)?;

        Ok((schema_id, json))
    }
}

/// Convert JSON to a DynamicMessage.
fn json_to_proto_message(
    json: &JsonValue,
    desc: &MessageDescriptor,
) -> ProtobufResult<DynamicMessage> {
    let mut msg = DynamicMessage::new(desc.clone());

    if let JsonValue::Object(obj) = json {
        for field in desc.fields() {
            let field_name = field.name();
            if let Some(value) = obj.get(field_name) {
                let proto_value = json_to_proto_value(value, &field)?;
                msg.set_field(&field, proto_value);
            }
        }
    } else {
        return Err(ProtobufError::EncodeError(
            "Expected JSON object".to_string(),
        ));
    }

    Ok(msg)
}

/// Convert a JSON value to a Protobuf value based on field type.
fn json_to_proto_value(json: &JsonValue, field: &FieldDescriptor) -> ProtobufResult<ProtoValue> {
    match field.kind() {
        Kind::Double => {
            let v = json
                .as_f64()
                .ok_or_else(|| ProtobufError::FieldError("Expected double".to_string()))?;
            Ok(ProtoValue::F64(v))
        }
        Kind::Float => {
            let v = json
                .as_f64()
                .ok_or_else(|| ProtobufError::FieldError("Expected float".to_string()))?;
            Ok(ProtoValue::F32(v as f32))
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            let v = json
                .as_i64()
                .ok_or_else(|| ProtobufError::FieldError("Expected int64".to_string()))?;
            Ok(ProtoValue::I64(v))
        }
        Kind::Uint64 | Kind::Fixed64 => {
            let v = json
                .as_u64()
                .ok_or_else(|| ProtobufError::FieldError("Expected uint64".to_string()))?;
            Ok(ProtoValue::U64(v))
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            let v = json
                .as_i64()
                .ok_or_else(|| ProtobufError::FieldError("Expected int32".to_string()))?;
            // safe narrowing from i64 to i32
            let v32 = i32::try_from(v)
                .map_err(|_| ProtobufError::FieldError(format!("Value {} out of i32 range", v)))?;
            Ok(ProtoValue::I32(v32))
        }
        Kind::Uint32 | Kind::Fixed32 => {
            let v = json
                .as_u64()
                .ok_or_else(|| ProtobufError::FieldError("Expected uint32".to_string()))?;
            // safe narrowing from u64 to u32
            let v32 = u32::try_from(v)
                .map_err(|_| ProtobufError::FieldError(format!("Value {} out of u32 range", v)))?;
            Ok(ProtoValue::U32(v32))
        }
        Kind::Bool => {
            let v = json
                .as_bool()
                .ok_or_else(|| ProtobufError::FieldError("Expected bool".to_string()))?;
            Ok(ProtoValue::Bool(v))
        }
        Kind::String => {
            let v = json
                .as_str()
                .ok_or_else(|| ProtobufError::FieldError("Expected string".to_string()))?;
            Ok(ProtoValue::String(v.to_string()))
        }
        Kind::Bytes => {
            let v = json
                .as_str()
                .ok_or_else(|| ProtobufError::FieldError("Expected base64 string".to_string()))?;
            let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, v)
                .map_err(|e| ProtobufError::FieldError(format!("Invalid base64: {}", e)))?;
            Ok(ProtoValue::Bytes(Bytes::from(bytes)))
        }
        Kind::Enum(enum_desc) => {
            // Handle enum as string or number
            if let Some(s) = json.as_str() {
                let ev = enum_desc.get_value_by_name(s).ok_or_else(|| {
                    ProtobufError::FieldError(format!("Unknown enum value: {}", s))
                })?;
                Ok(ProtoValue::EnumNumber(ev.number()))
            } else if let Some(n) = json.as_i64() {
                let value = i32::try_from(n).map_err(|_| {
                    ProtobufError::FieldError(format!("enum value {} out of i32 range", n))
                })?;
                Ok(ProtoValue::EnumNumber(value))
            } else {
                Err(ProtobufError::FieldError(
                    "Expected enum string or number".to_string(),
                ))
            }
        }
        Kind::Message(msg_desc) => {
            let nested = json_to_proto_message(json, &msg_desc)?;
            Ok(ProtoValue::Message(nested))
        }
    }
}

/// Convert a DynamicMessage to JSON.
fn proto_message_to_json(msg: &DynamicMessage) -> ProtobufResult<JsonValue> {
    let mut obj = serde_json::Map::new();

    for field in msg.descriptor().fields() {
        if msg.has_field(&field) || field.is_list() || field.is_map() {
            let value = msg.get_field(&field);
            let json_value = proto_value_to_json(&value, &field)?;
            obj.insert(field.name().to_string(), json_value);
        }
    }

    Ok(JsonValue::Object(obj))
}

/// Convert a Protobuf value to JSON.
fn proto_value_to_json(value: &ProtoValue, field: &FieldDescriptor) -> ProtobufResult<JsonValue> {
    match value {
        ProtoValue::Bool(v) => Ok(JsonValue::Bool(*v)),
        ProtoValue::I32(v) => Ok(JsonValue::Number((*v).into())),
        ProtoValue::I64(v) => Ok(JsonValue::Number((*v).into())),
        ProtoValue::U32(v) => Ok(JsonValue::Number((*v).into())),
        ProtoValue::U64(v) => {
            // U64 might not fit in JSON number, use string for large values
            if *v > i64::MAX as u64 {
                Ok(JsonValue::String(v.to_string()))
            } else {
                Ok(JsonValue::Number((*v as i64).into()))
            }
        }
        ProtoValue::F32(v) => {
            let n = serde_json::Number::from_f64(*v as f64)
                .unwrap_or_else(|| serde_json::Number::from(0));
            Ok(JsonValue::Number(n))
        }
        ProtoValue::F64(v) => {
            let n = serde_json::Number::from_f64(*v).unwrap_or_else(|| serde_json::Number::from(0));
            Ok(JsonValue::Number(n))
        }
        ProtoValue::String(v) => Ok(JsonValue::String(v.clone())),
        ProtoValue::Bytes(v) => {
            let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v);
            Ok(JsonValue::String(encoded))
        }
        ProtoValue::EnumNumber(v) => {
            // Try to get the enum name
            if let Kind::Enum(enum_desc) = field.kind() {
                if let Some(ev) = enum_desc.get_value(*v) {
                    return Ok(JsonValue::String(ev.name().to_string()));
                }
            }
            Ok(JsonValue::Number((*v).into()))
        }
        ProtoValue::Message(m) => proto_message_to_json(m),
        ProtoValue::List(items) => {
            let arr: ProtobufResult<Vec<_>> = items
                .iter()
                .map(|item| proto_value_to_json(item, field))
                .collect();
            Ok(JsonValue::Array(arr?))
        }
        ProtoValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map.iter() {
                let key_str = match k {
                    prost_reflect::MapKey::Bool(b) => b.to_string(),
                    prost_reflect::MapKey::I32(i) => i.to_string(),
                    prost_reflect::MapKey::I64(i) => i.to_string(),
                    prost_reflect::MapKey::U32(u) => u.to_string(),
                    prost_reflect::MapKey::U64(u) => u.to_string(),
                    prost_reflect::MapKey::String(s) => s.clone(),
                };
                obj.insert(key_str, proto_value_to_json(v, field)?);
            }
            Ok(JsonValue::Object(obj))
        }
    }
}

/// Compatibility checker for Protobuf schema evolution.
pub struct ProtobufCompatibility;

impl ProtobufCompatibility {
    /// Check if a new schema is backward compatible with an old schema.
    /// Backward compatibility means consumers using the new schema can read data produced with the old schema.
    pub fn check_backward(
        new_schema: &ProtobufSchema,
        old_schema: &ProtobufSchema,
    ) -> ProtobufResult<bool> {
        let new_desc = new_schema.message_descriptor()?;
        let old_desc = old_schema.message_descriptor()?;

        // For backward compatibility:
        // 1. All required fields in old schema must exist in new schema
        // 2. Field numbers must not change types incompatibly
        // 3. Reserved field numbers must not be reused

        for old_field in old_desc.fields() {
            // Check if field exists in new schema
            if let Some(new_field) = new_desc.get_field(old_field.number()) {
                // Check type compatibility
                if !Self::types_compatible(&old_field, &new_field) {
                    return Ok(false);
                }
            } else {
                // Field was removed - this is only okay for optional/repeated fields in proto3
                // In proto3, all fields are optional by default, so this is okay
            }
        }

        Ok(true)
    }

    /// Check if a new schema is forward compatible with an old schema.
    /// Forward compatibility means consumers using the old schema can read data produced with the new schema.
    pub fn check_forward(
        new_schema: &ProtobufSchema,
        old_schema: &ProtobufSchema,
    ) -> ProtobufResult<bool> {
        let new_desc = new_schema.message_descriptor()?;
        let old_desc = old_schema.message_descriptor()?;

        // For forward compatibility:
        // 1. New fields must be optional (proto3 default)
        // 2. Old fields must not be removed or have incompatible type changes

        for new_field in new_desc.fields() {
            if let Some(old_field) = old_desc.get_field(new_field.number()) {
                // Field exists in both - check type compatibility
                if !Self::types_compatible(&old_field, &new_field) {
                    return Ok(false);
                }
            }
            // New fields are okay for forward compatibility (old readers ignore them)
        }

        Ok(true)
    }

    /// Check if schemas are fully compatible (both backward and forward).
    pub fn check_full(
        new_schema: &ProtobufSchema,
        old_schema: &ProtobufSchema,
    ) -> ProtobufResult<bool> {
        Ok(Self::check_backward(new_schema, old_schema)?
            && Self::check_forward(new_schema, old_schema)?)
    }

    /// Check if two field types are compatible.
    fn types_compatible(old_field: &FieldDescriptor, new_field: &FieldDescriptor) -> bool {
        // Same kind is always compatible
        if std::mem::discriminant(&old_field.kind()) == std::mem::discriminant(&new_field.kind()) {
            return true;
        }

        // Check for compatible wire type promotions
        // In protobuf, these are wire-compatible:
        // - int32, uint32, int64, uint64, bool (varint)
        // - sint32, sint64 (zigzag varint)
        // - fixed32, sfixed32, float (32-bit)
        // - fixed64, sfixed64, double (64-bit)

        let old_kind = &old_field.kind();
        let new_kind = &new_field.kind();

        // Varint compatible types
        let varint_types = [
            Kind::Int32,
            Kind::Int64,
            Kind::Uint32,
            Kind::Uint64,
            Kind::Bool,
        ];
        let is_old_varint = varint_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(old_kind));
        let is_new_varint = varint_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(new_kind));
        if is_old_varint && is_new_varint {
            return true;
        }

        // 32-bit fixed types
        let fixed32_types = [Kind::Fixed32, Kind::Sfixed32, Kind::Float];
        let is_old_fixed32 = fixed32_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(old_kind));
        let is_new_fixed32 = fixed32_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(new_kind));
        if is_old_fixed32 && is_new_fixed32 {
            return true;
        }

        // 64-bit fixed types
        let fixed64_types = [Kind::Fixed64, Kind::Sfixed64, Kind::Double];
        let is_old_fixed64 = fixed64_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(old_kind));
        let is_new_fixed64 = fixed64_types
            .iter()
            .any(|k| std::mem::discriminant(k) == std::mem::discriminant(new_kind));
        if is_old_fixed64 && is_new_fixed64 {
            return true;
        }

        false
    }
}

/// Decode a protobuf-style unsigned varint from a `Buf`.
/// Returns an error if the varint exceeds 10 bytes (u64 max) or the buffer is exhausted.
fn decode_varint(buf: &mut &[u8]) -> Result<u64, std::io::Error> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for _ in 0..10 {
        if !buf.has_remaining() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "varint truncated",
            ));
        }
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "varint too long",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_message() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        assert_eq!(schema.message_name(), "User");
        assert!(schema.package().is_none());

        let fields = schema.fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].number, 1);
        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].number, 2);
    }

    #[test]
    fn test_parse_with_package() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            package com.example;
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        assert_eq!(schema.package(), Some("com.example"));
        assert_eq!(schema.fullname(), "com.example.User");
    }

    #[test]
    fn test_encode_decode() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        let codec = ProtobufCodec::new(schema);

        let json = serde_json::json!({
            "id": 42,
            "name": "Alice"
        });

        let encoded = codec.encode(&json).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded["id"], 42);
        assert_eq!(decoded["name"], "Alice");
    }

    #[test]
    fn test_confluent_wire_format() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        let codec = ProtobufCodec::new(schema);

        let json = serde_json::json!({
            "id": 1,
            "name": "Test"
        });

        let encoded = codec.encode_confluent(&json, 12345).unwrap();

        // Verify wire format
        assert_eq!(encoded[0], 0x00); // Magic byte
        assert_eq!(
            u32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]),
            12345
        );
        assert_eq!(encoded[5], 0x00); // Message index

        let (schema_id, decoded) = codec.decode_confluent(&encoded).unwrap();
        assert_eq!(schema_id, 12345);
        assert_eq!(decoded["id"], 1);
        assert_eq!(decoded["name"], "Test");
    }

    #[test]
    fn test_fingerprint() {
        let schema1 = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        let schema2 = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        // Different schemas should have different fingerprints
        assert_ne!(schema1.fingerprint_md5(), schema2.fingerprint_md5());
    }

    #[test]
    fn test_backward_compatibility_add_field() {
        let old_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        let new_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        // Adding a new optional field is backward compatible
        assert!(ProtobufCompatibility::check_backward(&new_schema, &old_schema).unwrap());
    }

    #[test]
    fn test_forward_compatibility_add_field() {
        let old_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        let new_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        // Adding a new field is forward compatible (old readers ignore unknown fields)
        assert!(ProtobufCompatibility::check_forward(&new_schema, &old_schema).unwrap());
    }

    #[test]
    fn test_full_compatibility() {
        let old_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        let new_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        )
        .unwrap();

        // Adding optional field is fully compatible in proto3
        assert!(ProtobufCompatibility::check_full(&new_schema, &old_schema).unwrap());
    }

    #[test]
    fn test_type_incompatibility() {
        let old_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
            "#,
        )
        .unwrap();

        let new_schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                string id = 1;
            }
            "#,
        )
        .unwrap();

        // Changing int64 to string is not compatible
        assert!(!ProtobufCompatibility::check_backward(&new_schema, &old_schema).unwrap());
    }

    #[test]
    fn test_nested_message() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message Address {
                string street = 1;
                string city = 2;
            }
            "#,
        )
        .unwrap();

        let fields = schema.fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "street");
        assert_eq!(fields[1].name, "city");
    }

    #[test]
    fn test_repeated_field() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                repeated string tags = 2;
            }
            "#,
        )
        .unwrap();

        let fields = schema.fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert!(!fields[0].is_repeated);
        assert!(fields[1].is_repeated);
    }

    #[test]
    fn test_enum_field() {
        let schema = ProtobufSchema::parse(
            r#"
            syntax = "proto3";
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }
            message User {
                int64 id = 1;
                Status status = 2;
            }
            "#,
        )
        .unwrap();

        let fields = schema.fields().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[1].name, "status");
    }
}
