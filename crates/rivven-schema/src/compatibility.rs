//! Schema compatibility checking
//!
//! Supports Confluent-compatible compatibility levels for schema evolution.

use crate::error::{SchemaError, SchemaResult};
use crate::types::SchemaType;
use serde::{Deserialize, Serialize};

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

    /// Check compatibility between two JSON schemas
    fn check_json_pair(
        &self,
        new: &serde_json::Value,
        old: &serde_json::Value,
    ) -> SchemaResult<CompatibilityResult> {
        let mut messages = Vec::new();

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
            // Check backward compatibility
            if self.level.is_backward() {
                // New schema must be able to read old data
                // Cannot add required fields that didn't exist in old schema
                for field in &new_required {
                    if !old_required.contains(field) && !old_p.contains_key(*field) {
                        messages.push(format!(
                            "BACKWARD incompatible: new required field '{}' not in old schema",
                            field
                        ));
                    }
                }

                // Check field type changes
                for (name, old_def) in old_p {
                    if let Some(new_def) = new_p.get(name) {
                        if !self.json_types_compatible(old_def, new_def) {
                            messages.push(format!(
                                "BACKWARD incompatible: field '{}' type changed incompatibly",
                                name
                            ));
                        }
                    }
                }
            }

            // Check forward compatibility
            if self.level.is_forward() {
                // Old schema must be able to read new data
                // Cannot remove required fields
                for field in &old_required {
                    if !new_p.contains_key(*field) {
                        messages.push(format!(
                            "FORWARD incompatible: required field '{}' removed from new schema",
                            field
                        ));
                    }
                }

                // Check field type changes (reverse direction)
                for (name, old_def) in old_p {
                    if let Some(new_def) = new_p.get(name) {
                        if !self.json_types_compatible(new_def, old_def) {
                            messages.push(format!(
                                "FORWARD incompatible: field '{}' type changed incompatibly",
                                name
                            ));
                        }
                    }
                }
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
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
    /// Implements Protobuf evolution rules:
    /// - Field numbers cannot be reused with different names
    /// - Reserved field numbers cannot be reused
    /// - Required fields cannot be removed (proto2)
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

    /// Check compatibility between two Protobuf schemas
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
        // Pattern matches: optional/repeated/required type field_name = field_number
        // More precise than bare `\w+ = \d+` to avoid matching option values
        let field_pattern =
            regex::Regex::new(r"(?:optional|repeated|required|^\s*)\s*\w+\s+(\w+)\s*=\s*(\d+)")
                .map_err(|e| SchemaError::ParseError(format!("Regex error: {}", e)))?;

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
        let reserved_pattern = regex::Regex::new(r"reserved\s+(\d+(?:,\s*\d+)*)")
            .map_err(|e| SchemaError::ParseError(format!("Regex error: {}", e)))?;

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
        let required_pattern = regex::Regex::new(r"required\s+\w+\s+(\w+)")
            .map_err(|e| SchemaError::ParseError(format!("Regex error: {}", e)))?;

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

        // Union writer with non-union reader: all writer variants must be compatible
        (Schema::Union(w), r) => {
            for w_variant in w.variants() {
                check_schema_resolution(w_variant, r)?;
            }
            Ok(())
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
            message User {
                int64 id = 1;
                string name = 2;
            }
        "#;
        let new = r#"
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
            message User {
                int64 id = 1;
            }
        "#;
        let new = r#"
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
            message User {
                int64 id = 1;
                reserved 2, 3;
            }
        "#;
        let new = r#"
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
}
