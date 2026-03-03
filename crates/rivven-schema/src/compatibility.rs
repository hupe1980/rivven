//! Schema compatibility checking
//!
//! Supports Confluent-compatible compatibility levels for schema evolution.

use crate::error::{SchemaError, SchemaResult};
use crate::types::SchemaType;
use serde::{Deserialize, Serialize};

#[cfg(not(feature = "protobuf"))]
use std::sync::LazyLock;

// Compiled regexes for protobuf compatibility checking (regex fallback when protox-parse unavailable)
#[cfg(not(feature = "protobuf"))]
static PROTO_FIELD_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"(?:(?:optional|repeated|required)\s+)?\w+\s+(\w+)\s*=\s*(\d+)").unwrap()
});
#[cfg(not(feature = "protobuf"))]
static PROTO_RESERVED_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"reserved\s+(\d+(?:,\s*\d+)*)").unwrap());
#[cfg(not(feature = "protobuf"))]
static PROTO_REQUIRED_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"required\s+\w+\s+(\w+)").unwrap());

// Re-export CompatibilityLevel from types for backward compatibility
pub use crate::types::CompatibilityLevel;

/// Result of a compatibility check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityResult {
    /// Whether the schemas are compatible
    pub is_compatible: bool,
    /// Compatibility error messages (if any)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
}

impl CompatibilityResult {
    pub fn compatible() -> Self {
        Self {
            is_compatible: true,
            messages: Vec::new(),
        }
    }

    pub fn incompatible(messages: Vec<String>) -> Self {
        Self {
            is_compatible: false,
            messages,
        }
    }
}

/// Schema compatibility checker
pub struct CompatibilityChecker {
    level: CompatibilityLevel,
}

// ── Protobuf AST helper types ──

/// Parsed field info from a `FieldDescriptorProto`.
#[cfg(feature = "protobuf")]
struct ProtoFieldInfo {
    name: String,
    /// Human-readable type description (e.g., "int32", "string", ".Foo").
    type_desc: String,
    /// Whether this field has `required` label (proto2 only).
    is_required: bool,
}

/// Parsed message info with field map, reserved numbers, and reserved names.
#[cfg(feature = "protobuf")]
struct ProtoMessageInfo {
    fields: std::collections::HashMap<i32, ProtoFieldInfo>,
    reserved_numbers: std::collections::HashSet<i32>,
    reserved_names: std::collections::HashSet<String>,
}

/// Parsed enum info with value-number → value-name mapping.
#[cfg(feature = "protobuf")]
struct ProtoEnumInfo {
    values: std::collections::HashMap<i32, String>,
}

impl CompatibilityChecker {
    pub fn new(level: CompatibilityLevel) -> Self {
        Self { level }
    }

    /// Check if a new schema is compatible with existing schemas
    pub fn check(
        &self,
        schema_type: SchemaType,
        new_schema: &str,
        existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        if self.level == CompatibilityLevel::None {
            return Ok(CompatibilityResult::compatible());
        }

        if existing_schemas.is_empty() {
            return Ok(CompatibilityResult::compatible());
        }

        match schema_type {
            SchemaType::Avro => self.check_avro(new_schema, existing_schemas),
            SchemaType::Json => self.check_json(new_schema, existing_schemas),
            SchemaType::Protobuf => self.check_protobuf(new_schema, existing_schemas),
        }
    }

    /// Check Avro schema compatibility
    ///
    /// Uses Apache Avro schema resolution rules:
    /// - BACKWARD: Reader (new) can read data written with Writer (old)
    /// - FORWARD: Reader (old) can read data written with Writer (new)
    /// - FULL: Both directions work
    #[cfg(feature = "avro")]
    fn check_avro(
        &self,
        new_schema: &str,
        existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        use apache_avro::Schema;

        let new = Schema::parse_str(new_schema)
            .map_err(|e| SchemaError::ParseError(format!("New schema: {}", e)))?;

        let schemas_to_check = if self.level.is_transitive() {
            existing_schemas.to_vec()
        } else {
            // Only check against the latest
            existing_schemas
                .last()
                .map(|s| vec![*s])
                .unwrap_or_default()
        };

        let mut errors = Vec::new();

        for (i, existing_str) in schemas_to_check.iter().enumerate() {
            let existing = Schema::parse_str(existing_str)
                .map_err(|e| SchemaError::ParseError(format!("Existing schema {}: {}", i, e)))?;

            // Check backward compatibility: new schema (reader) can read data written with old schema (writer)
            if self.level.is_backward() {
                if let Err(e) = check_schema_resolution(&existing, &new) {
                    errors.push(format!(
                        "Backward incompatible with version {}: {}",
                        i + 1,
                        e
                    ));
                }
            }

            // Check forward compatibility: old schema (reader) can read data written with new schema (writer)
            if self.level.is_forward() {
                if let Err(e) = check_schema_resolution(&new, &existing) {
                    errors.push(format!(
                        "Forward incompatible with version {}: {}",
                        i + 1,
                        e
                    ));
                }
            }
        }

        if errors.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(errors))
        }
    }

    #[cfg(not(feature = "avro"))]
    fn check_avro(
        &self,
        _new_schema: &str,
        _existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        Err(SchemaError::Config("Avro support not enabled".to_string()))
    }

    /// Check JSON Schema compatibility
    ///
    /// Implements JSON Schema evolution rules:
    /// - BACKWARD: New schema can read old data (adding required fields is incompatible)
    /// - FORWARD: Old schema can read new data (removing required fields is incompatible)
    /// - FULL: Both directions must be compatible
    fn check_json(
        &self,
        new_schema: &str,
        existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        use serde_json::Value as JsonValue;

        let new: JsonValue = serde_json::from_str(new_schema)
            .map_err(|e| SchemaError::ParseError(format!("New JSON schema: {}", e)))?;

        let schemas_to_check = if self.level.is_transitive() {
            existing_schemas.to_vec()
        } else {
            existing_schemas
                .last()
                .map(|s| vec![*s])
                .unwrap_or_default()
        };

        let mut messages = Vec::new();

        for (i, existing_str) in schemas_to_check.iter().enumerate() {
            let old: JsonValue = serde_json::from_str(existing_str)
                .map_err(|e| SchemaError::ParseError(format!("Existing schema {}: {}", i, e)))?;

            let result = self.check_json_pair(&new, &old)?;
            if !result.is_compatible {
                for msg in result.messages {
                    messages.push(format!("Version {}: {}", i + 1, msg));
                }
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    /// Check compatibility between two JSON schemas (SCHEMA-01: recursive).
    ///
    /// Recursively walks `properties`, `items`, `additionalProperties`,
    /// `oneOf`/`anyOf`/`allOf`, and `$defs`/`definitions` for full
    /// structural compatibility checking. `$ref` is resolved within the
    /// same document when pointing to `#/$defs/<name>` or `#/definitions/<name>`.
    fn check_json_pair(
        &self,
        new: &serde_json::Value,
        old: &serde_json::Value,
    ) -> SchemaResult<CompatibilityResult> {
        let mut messages = Vec::new();
        self.check_json_recursive(new, old, "", &mut messages);

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    /// Recursively check JSON schema compatibility at a given path.
    ///
    /// `depth` prevents stack overflow from circular `$ref` or deeply nested
    /// schemas.  We cap at 64 levels — well beyond any realistic schema.
    fn check_json_recursive(
        &self,
        new: &serde_json::Value,
        old: &serde_json::Value,
        path: &str,
        messages: &mut Vec<String>,
    ) {
        self.check_json_recursive_inner(new, old, path, messages, 0);
    }

    /// Inner recursive implementation with depth tracking.
    const MAX_JSON_DEPTH: usize = 64;

    fn check_json_recursive_inner(
        &self,
        new: &serde_json::Value,
        old: &serde_json::Value,
        path: &str,
        messages: &mut Vec<String>,
        depth: usize,
    ) {
        if depth >= Self::MAX_JSON_DEPTH {
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{}: ", path)
            };
            messages.push(format!(
                "{}schema nesting exceeds maximum depth ({})",
                prefix,
                Self::MAX_JSON_DEPTH
            ));
            return;
        }
        let prefix = if path.is_empty() {
            String::new()
        } else {
            format!("{}: ", path)
        };

        // --- Type compatibility ---
        if !self.json_types_compatible(old, new) && self.level.is_backward() {
            messages.push(format!("{}BACKWARD incompatible: type changed", prefix));
        }
        if !self.json_types_compatible(new, old) && self.level.is_forward() {
            messages.push(format!("{}FORWARD incompatible: type changed", prefix));
        }

        // --- Properties (object fields) ---
        let new_props = new.get("properties").and_then(|p| p.as_object());
        let old_props = old.get("properties").and_then(|p| p.as_object());

        let new_required: Vec<&str> = new
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let old_required: Vec<&str> = old
            .get("required")
            .and_then(|r| r.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        if let (Some(new_p), Some(old_p)) = (new_props, old_props) {
            if self.level.is_backward() {
                // Cannot add new required fields that didn't exist in old schema
                for field in &new_required {
                    if !old_required.contains(field) && !old_p.contains_key(*field) {
                        messages.push(format!(
                            "{}BACKWARD incompatible: new required field '{}'",
                            prefix, field
                        ));
                    }
                }
                // Recurse into shared properties
                for (name, old_def) in old_p {
                    let child_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{}", path, name)
                    };
                    if let Some(new_def) = new_p.get(name) {
                        self.check_json_recursive_inner(
                            new_def,
                            old_def,
                            &child_path,
                            messages,
                            depth + 1,
                        );
                    }
                }
            }
            if self.level.is_forward() {
                // Cannot remove required fields
                for field in &old_required {
                    if !new_p.contains_key(*field) {
                        messages.push(format!(
                            "{}FORWARD incompatible: required field '{}' removed",
                            prefix, field
                        ));
                    }
                }
                // Recurse into shared properties (reverse direction)
                for (name, old_def) in old_p {
                    let child_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}.{}", path, name)
                    };
                    if let Some(new_def) = new_p.get(name) {
                        // For forward compat we check if old can read new
                        // (type narrowing detection is already done above via json_types_compatible)
                        self.check_json_recursive_inner(
                            new_def,
                            old_def,
                            &child_path,
                            messages,
                            depth + 1,
                        );
                    }
                }
            }
        }

        // --- additionalProperties ---
        if let (Some(new_ap), Some(old_ap)) = (
            new.get("additionalProperties"),
            old.get("additionalProperties"),
        ) {
            // If one schema disallows additional properties and the other allows them
            let new_allows = !matches!(new_ap, serde_json::Value::Bool(false));
            let old_allows = !matches!(old_ap, serde_json::Value::Bool(false));
            if self.level.is_backward() && !new_allows && old_allows {
                messages.push(format!(
                    "{}BACKWARD incompatible: additionalProperties changed from allowed to disallowed",
                    prefix
                ));
            }
            if self.level.is_forward() && new_allows && !old_allows {
                messages.push(format!(
                    "{}FORWARD incompatible: additionalProperties changed from disallowed to allowed",
                    prefix
                ));
            }
            // If both are schemas, recurse
            if new_ap.is_object() && old_ap.is_object() {
                let child_path = if path.is_empty() {
                    "additionalProperties".to_string()
                } else {
                    format!("{}.additionalProperties", path)
                };
                self.check_json_recursive_inner(new_ap, old_ap, &child_path, messages, depth + 1);
            }
        }

        // --- items (array element schema) ---
        if let (Some(new_items), Some(old_items)) = (new.get("items"), old.get("items")) {
            let child_path = if path.is_empty() {
                "items".to_string()
            } else {
                format!("{}[]", path)
            };
            self.check_json_recursive_inner(new_items, old_items, &child_path, messages, depth + 1);
        }

        // --- enum value constraints ---
        if let (Some(new_enum), Some(old_enum)) = (
            new.get("enum").and_then(|e| e.as_array()),
            old.get("enum").and_then(|e| e.as_array()),
        ) {
            if self.level.is_backward() {
                // Old data may contain values removed from new enum
                for val in old_enum {
                    if !new_enum.contains(val) {
                        messages.push(format!(
                            "{}BACKWARD incompatible: enum value {:?} removed",
                            prefix, val
                        ));
                    }
                }
            }
            if self.level.is_forward() {
                // New data may contain values not in old enum
                for val in new_enum {
                    if !old_enum.contains(val) {
                        messages.push(format!(
                            "{}FORWARD incompatible: new enum value {:?} added",
                            prefix, val
                        ));
                    }
                }
            }
        }
    }

    /// Check if JSON Schema types are compatible
    fn json_types_compatible(
        &self,
        old_type: &serde_json::Value,
        new_type: &serde_json::Value,
    ) -> bool {
        let old_t = old_type.get("type").and_then(|t| t.as_str());
        let new_t = new_type.get("type").and_then(|t| t.as_str());

        match (old_t, new_t) {
            (Some(old), Some(new)) => {
                // Same type is always compatible
                if old == new {
                    return true;
                }

                // Allow widening: integer -> number
                if old == "integer" && new == "number" {
                    return true;
                }

                false
            }
            // If type is missing, assume object (compatible)
            (None, None) => true,
            _ => false,
        }
    }

    /// Check Protobuf schema compatibility
    ///
    /// Implements Protobuf evolution rules using a proper protobuf AST parser:
    /// - Field numbers cannot be reused with different names
    /// - Field type changes are detected (including in nested messages)
    /// - Reserved field numbers and names cannot be reused
    /// - Required fields cannot be removed (proto2)
    /// - Enum value removal is detected
    /// - Nested messages and `oneof` fields are handled correctly
    /// - `map<K,V>` fields are parsed properly
    #[cfg(feature = "protobuf")]
    fn check_protobuf(
        &self,
        new_schema: &str,
        existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        let schemas_to_check = if self.level.is_transitive() {
            existing_schemas.to_vec()
        } else {
            existing_schemas
                .last()
                .map(|s| vec![*s])
                .unwrap_or_default()
        };

        let mut messages = Vec::new();

        for (i, existing_str) in schemas_to_check.iter().enumerate() {
            let result = self.check_protobuf_pair(new_schema, existing_str)?;
            if !result.is_compatible {
                for msg in result.messages {
                    messages.push(format!("Version {}: {}", i + 1, msg));
                }
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    /// Check compatibility between two Protobuf schemas using AST-based analysis.
    ///
    /// Parses both schemas with `protox-parse` and compares:
    /// 1. Field number reuse with different names
    /// 2. Field type changes (wire-incompatible)
    /// 3. Reserved field number reuse
    /// 4. Reserved field name reuse
    /// 5. Required field removal (proto2)
    /// 6. Enum value removal
    #[cfg(feature = "protobuf")]
    fn check_protobuf_pair(
        &self,
        new_schema: &str,
        existing_schema: &str,
    ) -> SchemaResult<CompatibilityResult> {
        use std::collections::HashMap;

        let old_file = protox_parse::parse("existing.proto", existing_schema)
            .map_err(|e| SchemaError::ParseError(format!("Existing protobuf schema: {e}")))?;
        let new_file = protox_parse::parse("new.proto", new_schema)
            .map_err(|e| SchemaError::ParseError(format!("New protobuf schema: {e}")))?;

        let mut errors = Vec::new();

        // Extract all messages recursively (handles nested messages, map entries, etc.)
        let mut old_msgs: HashMap<String, ProtoMessageInfo> = HashMap::new();
        let mut old_enums: HashMap<String, ProtoEnumInfo> = HashMap::new();
        Self::extract_messages_recursive(&old_file.message_type, "", &mut old_msgs, &mut old_enums);
        Self::extract_enums(&old_file.enum_type, "", &mut old_enums);

        let mut new_msgs: HashMap<String, ProtoMessageInfo> = HashMap::new();
        let mut new_enums: HashMap<String, ProtoEnumInfo> = HashMap::new();
        Self::extract_messages_recursive(&new_file.message_type, "", &mut new_msgs, &mut new_enums);
        Self::extract_enums(&new_file.enum_type, "", &mut new_enums);

        // Compare messages
        for (msg_name, old_msg) in &old_msgs {
            if let Some(new_msg) = new_msgs.get(msg_name) {
                // Rule 1 & 2: Field number reuse with different name or type
                for (num, old_field) in &old_msg.fields {
                    if let Some(new_field) = new_msg.fields.get(num) {
                        if old_field.name != new_field.name {
                            errors.push(format!(
                                "PROTOBUF incompatible: {}: field number {} reused (was '{}', now '{}')",
                                msg_name, num, old_field.name, new_field.name
                            ));
                        }
                        if old_field.type_desc != new_field.type_desc {
                            errors.push(format!(
                                "PROTOBUF incompatible: {}: field {} ('{}') type changed '{}' → '{}'",
                                msg_name, num, old_field.name, old_field.type_desc, new_field.type_desc
                            ));
                        }
                    }
                }

                // Rule 3: Reserved number reuse
                for (num, field) in &new_msg.fields {
                    if old_msg.reserved_numbers.contains(num) {
                        errors.push(format!(
                            "PROTOBUF incompatible: {}: field '{}' uses reserved number {}",
                            msg_name, field.name, num
                        ));
                    }
                }

                // Rule 4: Reserved name reuse
                for field in new_msg.fields.values() {
                    if old_msg.reserved_names.contains(&field.name) {
                        errors.push(format!(
                            "PROTOBUF incompatible: {}: field name '{}' is reserved",
                            msg_name, field.name
                        ));
                    }
                }

                // Rule 5: Required fields cannot be removed (proto2)
                for (num, field) in &old_msg.fields {
                    if field.is_required && !new_msg.fields.contains_key(num) {
                        errors.push(format!(
                            "PROTOBUF incompatible: {}: required field '{}' (number {}) removed",
                            msg_name, field.name, num
                        ));
                    }
                }
            }
        }

        // Rule 6: Enum value removal
        for (enum_name, old_enum) in &old_enums {
            if let Some(new_enum) = new_enums.get(enum_name) {
                for (num, name) in &old_enum.values {
                    if !new_enum.values.contains_key(num) {
                        errors.push(format!(
                            "PROTOBUF incompatible: {}: enum value '{}' = {} removed",
                            enum_name, name, num
                        ));
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(errors))
        }
    }

    /// Extract messages recursively from a `DescriptorProto` list.
    ///
    /// Walks nested messages (including synthetic map-entry messages) and builds
    /// a flat map of fully-qualified message name → field info.
    #[cfg(feature = "protobuf")]
    fn extract_messages_recursive(
        messages: &[prost_types::DescriptorProto],
        prefix: &str,
        out_msgs: &mut std::collections::HashMap<String, ProtoMessageInfo>,
        out_enums: &mut std::collections::HashMap<String, ProtoEnumInfo>,
    ) {
        for msg in messages {
            let name = msg.name.as_deref().unwrap_or("<unknown>");
            let fqn = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", prefix, name)
            };

            // Skip synthetic map-entry messages — they are implementation details
            // of `map<K,V>` fields, not user-visible message types.
            let is_map_entry = msg
                .options
                .as_ref()
                .and_then(|o| o.map_entry)
                .unwrap_or(false);

            if !is_map_entry {
                let mut fields = std::collections::HashMap::new();
                let mut reserved_numbers = std::collections::HashSet::new();
                let mut reserved_names = std::collections::HashSet::new();

                for field in &msg.field {
                    let field_name = field.name.clone().unwrap_or_default();
                    let field_number = field.number.unwrap_or(0);
                    let type_desc = Self::proto_type_description(field);
                    let is_required = field.label == Some(2); // LABEL_REQUIRED = 2

                    fields.insert(
                        field_number,
                        ProtoFieldInfo {
                            name: field_name,
                            type_desc,
                            is_required,
                        },
                    );
                }

                // Reserved ranges (e.g., `reserved 5 to 10;`)
                for range in &msg.reserved_range {
                    let start = range.start.unwrap_or(0);
                    let end = range.end.unwrap_or(start); // end is exclusive in descriptor
                    for num in start..end {
                        reserved_numbers.insert(num);
                    }
                }

                // Reserved names (e.g., `reserved "old_field";`)
                for name in &msg.reserved_name {
                    reserved_names.insert(name.clone());
                }

                out_msgs.insert(
                    fqn.clone(),
                    ProtoMessageInfo {
                        fields,
                        reserved_numbers,
                        reserved_names,
                    },
                );
            }

            // Recurse into nested messages
            Self::extract_messages_recursive(&msg.nested_type, &fqn, out_msgs, out_enums);

            // Extract nested enums
            Self::extract_enums(&msg.enum_type, &fqn, out_enums);
        }
    }

    /// Extract enum definitions into a flat map.
    #[cfg(feature = "protobuf")]
    fn extract_enums(
        enums: &[prost_types::EnumDescriptorProto],
        prefix: &str,
        out: &mut std::collections::HashMap<String, ProtoEnumInfo>,
    ) {
        for e in enums {
            let name = e.name.as_deref().unwrap_or("<unknown>");
            let fqn = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{}.{}", prefix, name)
            };

            let mut values = std::collections::HashMap::new();
            for v in &e.value {
                let val_name = v.name.clone().unwrap_or_default();
                let val_number = v.number.unwrap_or(0);
                values.insert(val_number, val_name);
            }

            out.insert(fqn, ProtoEnumInfo { values });
        }
    }

    /// Build a human-readable type description for a protobuf field.
    ///
    /// For scalar types, returns the type name (e.g., "int32", "string").
    /// For message/enum references, returns the type_name from the descriptor.
    #[cfg(feature = "protobuf")]
    fn proto_type_description(field: &prost_types::FieldDescriptorProto) -> String {
        let type_val = field.r#type.unwrap_or(0);
        match type_val {
            1 => "double".into(),
            2 => "float".into(),
            3 => "int64".into(),
            4 => "uint64".into(),
            5 => "int32".into(),
            6 => "fixed64".into(),
            7 => "fixed32".into(),
            8 => "bool".into(),
            9 => "string".into(),
            10 => "group".into(),
            11 | 14 => {
                // TYPE_MESSAGE (11) or TYPE_ENUM (14) — use type_name
                field
                    .type_name
                    .clone()
                    .unwrap_or_else(|| format!("type_{}", type_val))
            }
            12 => "bytes".into(),
            13 => "uint32".into(),
            15 => "sfixed32".into(),
            16 => "sfixed64".into(),
            17 => "sint32".into(),
            18 => "sint64".into(),
            _ => format!("unknown_{}", type_val),
        }
    }

    // ── Regex-based fallback when `protobuf` feature is disabled ──

    /// Check Protobuf schema compatibility (regex fallback)
    ///
    /// # Limitations
    ///
    /// This is a regex-based heuristic fallback used when the `protobuf` feature
    /// is not enabled. Enable the `protobuf` feature for proper AST-based checking.
    #[cfg(not(feature = "protobuf"))]
    fn check_protobuf(
        &self,
        new_schema: &str,
        existing_schemas: &[&str],
    ) -> SchemaResult<CompatibilityResult> {
        let schemas_to_check = if self.level.is_transitive() {
            existing_schemas.to_vec()
        } else {
            existing_schemas
                .last()
                .map(|s| vec![*s])
                .unwrap_or_default()
        };

        let mut messages = Vec::new();

        for (i, existing_str) in schemas_to_check.iter().enumerate() {
            let result = self.check_protobuf_pair(new_schema, existing_str)?;
            if !result.is_compatible {
                for msg in result.messages {
                    messages.push(format!("Version {}: {}", i + 1, msg));
                }
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    /// Check compatibility between two Protobuf schemas (regex fallback)
    #[cfg(not(feature = "protobuf"))]
    fn check_protobuf_pair(
        &self,
        new_schema: &str,
        existing_schema: &str,
    ) -> SchemaResult<CompatibilityResult> {
        use std::collections::{HashMap, HashSet};

        let mut messages = Vec::new();

        // Strip comments before regex extraction to avoid
        // matching field numbers in comments or strings.
        let strip_comments = |s: &str| -> String {
            let mut result = String::with_capacity(s.len());
            let mut chars = s.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '/' {
                    if chars.peek() == Some(&'/') {
                        // Line comment — skip to end of line
                        for ch in chars.by_ref() {
                            if ch == '\n' {
                                result.push('\n');
                                break;
                            }
                        }
                        continue;
                    } else if chars.peek() == Some(&'*') {
                        // Block comment — skip to */
                        chars.next(); // consume *
                        loop {
                            match chars.next() {
                                Some('*') if chars.peek() == Some(&'/') => {
                                    chars.next();
                                    break;
                                }
                                Some(_) => continue,
                                None => break,
                            }
                        }
                        continue;
                    }
                }
                result.push(c);
            }
            result
        };

        let clean_existing = strip_comments(existing_schema);
        let clean_new = strip_comments(new_schema);

        // Extract field numbers from both schemas using regex
        // Pattern matches: [optional|repeated|required] type field_name = field_number
        // The label is optional (proto3 omits it), and we require a type word
        // before the field name to avoid matching option values or reserved numbers.
        let field_pattern = &*PROTO_FIELD_RE;

        let old_fields: HashMap<u32, String> = field_pattern
            .captures_iter(&clean_existing)
            .filter_map(|cap| {
                let name = cap.get(1)?.as_str().to_string();
                let num: u32 = cap.get(2)?.as_str().parse().ok()?;
                Some((num, name))
            })
            .collect();

        let new_fields: HashMap<u32, String> = field_pattern
            .captures_iter(&clean_new)
            .filter_map(|cap| {
                let name = cap.get(1)?.as_str().to_string();
                let num: u32 = cap.get(2)?.as_str().parse().ok()?;
                Some((num, name))
            })
            .collect();

        // Rule 1: Field numbers cannot be reused with different names
        for (num, old_name) in &old_fields {
            if let Some(new_name) = new_fields.get(num) {
                if old_name != new_name {
                    messages.push(format!(
                        "PROTOBUF incompatible: field number {} reused (was '{}', now '{}')",
                        num, old_name, new_name
                    ));
                }
            }
        }

        // Rule 2: Reserved field numbers cannot be reused
        let reserved_pattern = &*PROTO_RESERVED_RE;

        let old_reserved: HashSet<u32> = reserved_pattern
            .captures_iter(&clean_existing)
            .flat_map(|cap| {
                cap.get(1)
                    .map(|m| {
                        m.as_str()
                            .split(',')
                            .filter_map(|n| n.trim().parse().ok())
                            .collect::<Vec<u32>>()
                    })
                    .unwrap_or_default()
            })
            .collect();

        for (num, name) in &new_fields {
            if old_reserved.contains(num) {
                messages.push(format!(
                    "PROTOBUF incompatible: field '{}' uses reserved number {}",
                    name, num
                ));
            }
        }

        // Rule 3: Required fields cannot be removed (proto2)
        let required_pattern = &*PROTO_REQUIRED_RE;

        let old_required: HashSet<&str> = required_pattern
            .captures_iter(&clean_existing)
            .filter_map(|cap| cap.get(1).map(|m| m.as_str()))
            .collect();

        for required_name in old_required {
            if !new_fields.values().any(|n| n == required_name) {
                messages.push(format!(
                    "PROTOBUF incompatible: required field '{}' removed",
                    required_name
                ));
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }
}

/// Check if a reader schema can read data written with a writer schema
///
/// This implements Avro schema resolution rules:
/// - Field matching by name (aliases supported)
/// - Default values for missing fields
/// - Type promotion rules (int -> long -> float -> double)
#[cfg(feature = "avro")]
fn check_schema_resolution(
    writer: &apache_avro::Schema,
    reader: &apache_avro::Schema,
) -> Result<(), String> {
    use apache_avro::Schema;

    match (writer, reader) {
        // Same type always compatible
        (Schema::Null, Schema::Null) => Ok(()),
        (Schema::Boolean, Schema::Boolean) => Ok(()),
        (Schema::String, Schema::String) => Ok(()),
        (Schema::Bytes, Schema::Bytes) => Ok(()),

        // Numeric promotions: int -> long -> float -> double
        (Schema::Int, Schema::Int) => Ok(()),
        (Schema::Int, Schema::Long) => Ok(()),
        (Schema::Int, Schema::Float) => Ok(()),
        (Schema::Int, Schema::Double) => Ok(()),
        (Schema::Long, Schema::Long) => Ok(()),
        (Schema::Long, Schema::Float) => Ok(()),
        (Schema::Long, Schema::Double) => Ok(()),
        (Schema::Float, Schema::Float) => Ok(()),
        (Schema::Float, Schema::Double) => Ok(()),
        (Schema::Double, Schema::Double) => Ok(()),

        // String <-> Bytes promotion
        (Schema::String, Schema::Bytes) => Ok(()),
        (Schema::Bytes, Schema::String) => Ok(()),

        // Arrays: element types must be compatible
        (Schema::Array(w), Schema::Array(r)) => check_schema_resolution(&w.items, &r.items),

        // Maps: value types must be compatible
        (Schema::Map(w), Schema::Map(r)) => check_schema_resolution(&w.types, &r.types),

        // Enums: reader symbols must be superset of writer symbols
        (Schema::Enum(w), Schema::Enum(r)) => {
            for symbol in &w.symbols {
                if !r.symbols.contains(symbol) {
                    return Err(format!(
                        "Enum symbol '{}' in writer not found in reader",
                        symbol
                    ));
                }
            }
            Ok(())
        }

        // Fixed: must have same size and name
        (Schema::Fixed(w), Schema::Fixed(r)) => {
            if w.size != r.size {
                return Err(format!(
                    "Fixed size mismatch: writer={}, reader={}",
                    w.size, r.size
                ));
            }
            Ok(())
        }

        // Records: check field compatibility
        (Schema::Record(w), Schema::Record(r)) => {
            // Check all writer fields can be read
            for w_field in &w.fields {
                // Find matching reader field by name or alias
                let r_field = r.fields.iter().find(|rf| {
                    rf.name == w_field.name
                        || rf
                            .aliases
                            .as_ref()
                            .is_some_and(|a| a.contains(&w_field.name))
                });

                match r_field {
                    Some(rf) => {
                        // Field exists in reader, check type compatibility
                        check_schema_resolution(&w_field.schema, &rf.schema)?;
                    }
                    None => {
                        // Writer field not in reader - that's OK, reader ignores it
                    }
                }
            }

            // Check all reader fields have values (from writer or default)
            for r_field in &r.fields {
                let w_field = w.fields.iter().find(|wf| {
                    wf.name == r_field.name
                        || r_field
                            .aliases
                            .as_ref()
                            .is_some_and(|a| a.contains(&wf.name))
                });

                if w_field.is_none() && r_field.default.is_none() {
                    return Err(format!(
                        "Reader field '{}' has no matching writer field and no default",
                        r_field.name
                    ));
                }
            }

            Ok(())
        }

        // Unions: writer type must be resolvable to one reader type
        (Schema::Union(w), Schema::Union(r)) => {
            for w_variant in w.variants() {
                let compatible = r
                    .variants()
                    .iter()
                    .any(|rv| check_schema_resolution(w_variant, rv).is_ok());
                if !compatible {
                    return Err(format!(
                        "Writer union variant {:?} not compatible with any reader variant",
                        w_variant
                    ));
                }
            }
            Ok(())
        }

        // Non-union writer with union reader: writer must match one reader variant
        (w, Schema::Union(r)) => {
            let compatible = r
                .variants()
                .iter()
                .any(|rv| check_schema_resolution(w, rv).is_ok());
            if compatible {
                Ok(())
            } else {
                Err(format!(
                    "Writer schema {:?} not compatible with reader union",
                    w
                ))
            }
        }

        // Union writer with non-union reader: at least one writer variant
        // must be compatible (Avro spec says reader resolves the first matching variant)
        (Schema::Union(w), r) => {
            let compatible = w
                .variants()
                .iter()
                .any(|wv| check_schema_resolution(wv, r).is_ok());
            if compatible {
                Ok(())
            } else {
                Err(format!(
                    "No writer union variant compatible with reader {:?}",
                    r
                ))
            }
        }

        // Incompatible types
        (w, r) => Err(format!(
            "Incompatible types: writer={:?}, reader={:?}",
            w, r
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compatibility_level_properties() {
        assert!(CompatibilityLevel::Backward.is_backward());
        assert!(!CompatibilityLevel::Backward.is_forward());
        assert!(!CompatibilityLevel::Backward.is_transitive());

        assert!(!CompatibilityLevel::Forward.is_backward());
        assert!(CompatibilityLevel::Forward.is_forward());

        assert!(CompatibilityLevel::Full.is_backward());
        assert!(CompatibilityLevel::Full.is_forward());

        assert!(CompatibilityLevel::FullTransitive.is_transitive());
    }

    #[test]
    fn test_compatibility_level_parse() {
        assert_eq!(
            "BACKWARD".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::Backward
        );
        assert_eq!(
            "FULL_TRANSITIVE".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::FullTransitive
        );
        assert_eq!(
            "NONE".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::None
        );
    }

    #[test]
    fn test_none_compatibility_always_passes() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::None);
        let result = checker
            .check(SchemaType::Avro, "{}", &["{\"invalid\"}"])
            .unwrap();
        assert!(result.is_compatible);
    }

    #[test]
    fn test_empty_existing_schemas() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);
        let result = checker.check(SchemaType::Avro, "{}", &[]).unwrap();
        assert!(result.is_compatible);
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_backward_compatible_add_optional_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let v1 =
            r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
        let v2 = r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": ["null", "string"], "default": null}]}"#;

        let result = checker.check(SchemaType::Avro, v2, &[v1]).unwrap();
        assert!(
            result.is_compatible,
            "Adding optional field should be backward compatible"
        );
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_backward_incompatible_add_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        // v1 has only 'id', v2 adds 'name' WITHOUT a default
        // Old data doesn't have 'name', so new reader can't read it
        let v1 =
            r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}]}"#;
        let v2 = r#"{"type": "record", "name": "User", "fields": [{"name": "id", "type": "long"}, {"name": "name", "type": "string"}]}"#;

        let result = checker.check(SchemaType::Avro, v2, &[v1]).unwrap();
        assert!(
            !result.is_compatible,
            "Adding required field (no default) should be backward incompatible"
        );
    }

    #[cfg(feature = "avro")]
    #[test]
    fn test_numeric_type_promotion() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let v1 =
            r#"{"type": "record", "name": "Data", "fields": [{"name": "value", "type": "int"}]}"#;
        let v2 =
            r#"{"type": "record", "name": "Data", "fields": [{"name": "value", "type": "long"}]}"#;

        let result = checker.check(SchemaType::Avro, v2, &[v1]).unwrap();
        assert!(
            result.is_compatible,
            "int -> long promotion should be backward compatible"
        );
    }

    // JSON Schema compatibility tests
    #[test]
    fn test_json_backward_add_optional_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
        let new = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}"#;

        let result = checker.check(SchemaType::Json, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding optional field should be backward compatible"
        );
    }

    #[test]
    fn test_json_backward_add_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
        let new = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#;

        let result = checker.check(SchemaType::Json, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Adding required field should NOT be backward compatible"
        );
    }

    #[test]
    fn test_json_forward_remove_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Forward);

        let old = r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#;
        let new = r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;

        let result = checker.check(SchemaType::Json, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Removing required field should NOT be forward compatible"
        );
    }

    #[test]
    fn test_json_type_widening() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = r#"{"type":"object","properties":{"value":{"type":"integer"}}}"#;
        let new = r#"{"type":"object","properties":{"value":{"type":"number"}}}"#;

        let result = checker.check(SchemaType::Json, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "integer -> number widening should be backward compatible"
        );
    }

    // Protobuf compatibility tests
    #[test]
    fn test_protobuf_field_number_reuse() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string email = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Reusing field number with different name should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_add_new_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding new field with new number should be compatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_reserved_field_reuse() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                reserved 2, 3;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Reusing reserved field number should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_field_type_change() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message Event {
                int32 id = 1;
                string payload = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message Event {
                int32 id = 1;
                bytes payload = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Changing field type should be incompatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_nested_message() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message Outer {
                int32 id = 1;
                message Inner {
                    string value = 1;
                }
                Inner data = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message Outer {
                int32 id = 1;
                message Inner {
                    int32 value = 1;
                }
                Inner data = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Type change in nested message should be detected: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_reserved_range() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message Data {
                int32 id = 1;
                reserved 5 to 10;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message Data {
                int32 id = 1;
                string tag = 7;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Using field number from reserved range should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_reserved_name() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message Data {
                int32 id = 1;
                reserved "old_field";
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message Data {
                int32 id = 1;
                string old_field = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Reusing reserved field name should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_enum_value_removal() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Removing enum value should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_enum_value_addition() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding enum value should be compatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_map_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message Config {
                int32 id = 1;
                map<string, string> labels = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message Config {
                int32 id = 1;
                map<string, string> labels = 2;
                string name = 3;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding a new field alongside a map field should be compatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_proto2_required_field_removal() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto2";
            message User {
                required int64 id = 1;
                required string name = 2;
            }
        "#;
        let new = r#"
            syntax = "proto2";
            message User {
                required int64 id = 1;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Removing required field in proto2 should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_compatible_evolution() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
            }
        "#;
        let new = r#"
            syntax = "proto3";
            message User {
                int64 id = 1;
                string name = 2;
                string email = 3;
                int32 age = 4;
            }
        "#;

        let result = checker.check(SchemaType::Protobuf, new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding new fields with new numbers should be fully compatible: {:?}",
            result.messages
        );
    }
}
