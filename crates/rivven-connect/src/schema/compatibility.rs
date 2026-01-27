//! Schema compatibility checking

use crate::schema::types::{
    CompatibilityLevel, Schema, SchemaRegistryError, SchemaRegistryResult, SchemaType,
};
use serde_json::Value as JsonValue;

/// Result of a compatibility check
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether the schemas are compatible
    pub is_compatible: bool,
    /// Detailed messages about compatibility issues
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
        new_schema: &Schema,
        existing_schemas: &[Schema],
    ) -> SchemaRegistryResult<CompatibilityResult> {
        if self.level == CompatibilityLevel::None {
            return Ok(CompatibilityResult::compatible());
        }

        if existing_schemas.is_empty() {
            return Ok(CompatibilityResult::compatible());
        }

        // Determine which schemas to check against
        let schemas_to_check: Vec<&Schema> = match self.level {
            CompatibilityLevel::None => return Ok(CompatibilityResult::compatible()),
            CompatibilityLevel::Backward
            | CompatibilityLevel::Forward
            | CompatibilityLevel::Full => {
                // Only check against the latest
                existing_schemas.last().into_iter().collect()
            }
            CompatibilityLevel::BackwardTransitive
            | CompatibilityLevel::ForwardTransitive
            | CompatibilityLevel::FullTransitive => {
                // Check against all versions
                existing_schemas.iter().collect()
            }
        };

        let mut messages = Vec::new();

        for existing in schemas_to_check {
            let result = match new_schema.schema_type {
                SchemaType::Json => self.check_json_compatibility(new_schema, existing)?,
                SchemaType::Avro => self.check_avro_compatibility(new_schema, existing)?,
                SchemaType::Protobuf => self.check_protobuf_compatibility(new_schema, existing)?,
            };

            if !result.is_compatible {
                messages.extend(result.messages);
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    fn check_json_compatibility(
        &self,
        new_schema: &Schema,
        existing: &Schema,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        let new: JsonValue = serde_json::from_str(&new_schema.schema)
            .map_err(|e| SchemaRegistryError::ParseError(format!("New schema: {}", e)))?;
        let old: JsonValue = serde_json::from_str(&existing.schema)
            .map_err(|e| SchemaRegistryError::ParseError(format!("Existing schema: {}", e)))?;

        let mut messages = Vec::new();

        // Extract properties from both schemas
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

        match self.level {
            CompatibilityLevel::Backward | CompatibilityLevel::BackwardTransitive => {
                // New schema must be able to read old data
                // - Can add optional fields
                // - Can remove fields
                // - Cannot add required fields that didn't exist
                // - Cannot change field types in incompatible ways

                if let (Some(new_p), Some(old_p)) = (new_props, old_props) {
                    // Check for new required fields
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
                            if !self.types_compatible(old_def, new_def) {
                                messages.push(format!(
                                    "BACKWARD incompatible: field '{}' type changed incompatibly",
                                    name
                                ));
                            }
                        }
                    }
                }
            }
            CompatibilityLevel::Forward | CompatibilityLevel::ForwardTransitive => {
                // Old schema must be able to read new data
                // - Can remove optional fields
                // - Can add fields
                // - Cannot remove required fields
                // - Cannot change field types in incompatible ways

                if let (Some(new_p), Some(old_p)) = (new_props, old_props) {
                    // Check for removed required fields
                    for field in &old_required {
                        if !new_p.contains_key(*field) {
                            messages.push(format!(
                                "FORWARD incompatible: required field '{}' removed from new schema",
                                field
                            ));
                        }
                    }

                    // Check field type changes
                    for (name, old_def) in old_p {
                        if let Some(new_def) = new_p.get(name) {
                            if !self.types_compatible(new_def, old_def) {
                                messages.push(format!(
                                    "FORWARD incompatible: field '{}' type changed incompatibly",
                                    name
                                ));
                            }
                        }
                    }
                }
            }
            CompatibilityLevel::Full | CompatibilityLevel::FullTransitive => {
                // Both backward and forward compatible
                let backward =
                    self.check_json_compat_direction(&new, &old, CompatibilityLevel::Backward)?;
                let forward =
                    self.check_json_compat_direction(&new, &old, CompatibilityLevel::Forward)?;

                messages.extend(backward.messages);
                messages.extend(forward.messages);
            }
            CompatibilityLevel::None => {}
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    fn check_json_compat_direction(
        &self,
        new: &JsonValue,
        old: &JsonValue,
        direction: CompatibilityLevel,
    ) -> SchemaRegistryResult<CompatibilityResult> {
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
            match direction {
                CompatibilityLevel::Backward => {
                    for field in &new_required {
                        if !old_required.contains(field) && !old_p.contains_key(*field) {
                            messages.push(format!(
                                "BACKWARD: new required field '{}' not in old schema",
                                field
                            ));
                        }
                    }
                }
                CompatibilityLevel::Forward => {
                    for field in &old_required {
                        if !new_p.contains_key(*field) {
                            messages.push(format!("FORWARD: required field '{}' removed", field));
                        }
                    }
                }
                _ => {}
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    fn types_compatible(&self, old_type: &JsonValue, new_type: &JsonValue) -> bool {
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
            // If type is missing, assume object
            (None, None) => true,
            _ => false,
        }
    }

    fn check_avro_compatibility(
        &self,
        new_schema: &Schema,
        existing: &Schema,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        // Avro schemas are JSON - parse and validate structure
        let new: JsonValue = serde_json::from_str(&new_schema.schema)
            .map_err(|e| SchemaRegistryError::ParseError(format!("New Avro schema: {}", e)))?;
        let old: JsonValue = serde_json::from_str(&existing.schema)
            .map_err(|e| SchemaRegistryError::ParseError(format!("Existing Avro schema: {}", e)))?;

        let mut messages = Vec::new();

        // Extract schema type and fields
        let new_type = new.get("type").and_then(|t| t.as_str());
        let old_type = old.get("type").and_then(|t| t.as_str());

        // Type must match
        if new_type != old_type {
            messages.push(format!(
                "AVRO incompatible: schema type changed from {:?} to {:?}",
                old_type, new_type
            ));
            return Ok(CompatibilityResult::incompatible(messages));
        }

        // For record types, check field compatibility
        if new_type == Some("record") {
            let new_fields = new.get("fields").and_then(|f| f.as_array());
            let old_fields = old.get("fields").and_then(|f| f.as_array());

            if let (Some(new_f), Some(old_f)) = (new_fields, old_fields) {
                // Build maps of field name -> field definition
                let old_field_map: std::collections::HashMap<&str, &JsonValue> = old_f
                    .iter()
                    .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(|n| (n, f)))
                    .collect();
                let new_field_map: std::collections::HashMap<&str, &JsonValue> = new_f
                    .iter()
                    .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(|n| (n, f)))
                    .collect();

                match self.level {
                    CompatibilityLevel::Backward | CompatibilityLevel::BackwardTransitive => {
                        // New schema reads old data
                        // - New required fields must have defaults
                        for (name, new_field) in &new_field_map {
                            if !old_field_map.contains_key(name) {
                                // New field - must have default for backward compatibility
                                if new_field.get("default").is_none() {
                                    messages.push(format!(
                                        "AVRO BACKWARD incompatible: new field '{}' has no default",
                                        name
                                    ));
                                }
                            }
                        }
                        // Check type compatibility for existing fields
                        for (name, old_field) in &old_field_map {
                            if let Some(new_field) = new_field_map.get(name) {
                                if !self.avro_types_compatible(old_field, new_field) {
                                    messages.push(format!(
                                        "AVRO BACKWARD incompatible: field '{}' type changed",
                                        name
                                    ));
                                }
                            }
                        }
                    }
                    CompatibilityLevel::Forward | CompatibilityLevel::ForwardTransitive => {
                        // Old schema reads new data
                        // - Removed fields must have had defaults
                        for (name, old_field) in &old_field_map {
                            if !new_field_map.contains_key(name) {
                                // Field removed - old schema must have had default
                                if old_field.get("default").is_none() {
                                    messages.push(format!(
                                        "AVRO FORWARD incompatible: removed field '{}' had no default",
                                        name
                                    ));
                                }
                            }
                        }
                        // Check type compatibility
                        for (name, new_field) in &new_field_map {
                            if let Some(old_field) = old_field_map.get(name) {
                                if !self.avro_types_compatible(new_field, old_field) {
                                    messages.push(format!(
                                        "AVRO FORWARD incompatible: field '{}' type changed",
                                        name
                                    ));
                                }
                            }
                        }
                    }
                    CompatibilityLevel::Full | CompatibilityLevel::FullTransitive => {
                        // Both directions - all added/removed fields must have defaults
                        for (name, new_field) in &new_field_map {
                            if !old_field_map.contains_key(name)
                                && new_field.get("default").is_none()
                            {
                                messages.push(format!(
                                    "AVRO FULL incompatible: new field '{}' has no default",
                                    name
                                ));
                            }
                        }
                        for (name, old_field) in &old_field_map {
                            if !new_field_map.contains_key(name)
                                && old_field.get("default").is_none()
                            {
                                messages.push(format!(
                                    "AVRO FULL incompatible: removed field '{}' had no default",
                                    name
                                ));
                            }
                        }
                    }
                    CompatibilityLevel::None => {}
                }
            }
        }

        if messages.is_empty() {
            Ok(CompatibilityResult::compatible())
        } else {
            Ok(CompatibilityResult::incompatible(messages))
        }
    }

    /// Check if Avro field types are compatible
    fn avro_types_compatible(&self, old_field: &JsonValue, new_field: &JsonValue) -> bool {
        let old_type = old_field.get("type");
        let new_type = new_field.get("type");

        match (old_type, new_type) {
            (Some(old_t), Some(new_t)) => {
                // Same type is always compatible
                if old_t == new_t {
                    return true;
                }

                // Handle union types (nullable fields)
                if let (Some(old_arr), Some(new_arr)) = (old_t.as_array(), new_t.as_array()) {
                    // Union can add types (make more permissive) but not remove
                    for old_elem in old_arr {
                        if !new_arr.contains(old_elem) {
                            return false;
                        }
                    }
                    return true;
                }

                // Avro type promotion rules
                matches!(
                    (old_t.as_str(), new_t.as_str()),
                    (Some("int"), Some("long"))
                        | (Some("int"), Some("float"))
                        | (Some("int"), Some("double"))
                        | (Some("long"), Some("float"))
                        | (Some("long"), Some("double"))
                        | (Some("float"), Some("double"))
                        | (Some("string"), Some("bytes"))
                        | (Some("bytes"), Some("string"))
                )
            }
            _ => true, // If type is missing, be permissive
        }
    }

    fn check_protobuf_compatibility(
        &self,
        new_schema: &Schema,
        existing: &Schema,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        // Protobuf compatibility rules based on text comparison
        // A proper implementation would parse .proto files
        let mut messages = Vec::new();

        // Extract field numbers from both schemas using regex
        let field_pattern = regex::Regex::new(r"(\w+)\s*=\s*(\d+)").unwrap();

        let old_fields: std::collections::HashMap<u32, String> = field_pattern
            .captures_iter(&existing.schema)
            .filter_map(|cap| {
                let name = cap.get(1)?.as_str().to_string();
                let num: u32 = cap.get(2)?.as_str().parse().ok()?;
                Some((num, name))
            })
            .collect();

        let new_fields: std::collections::HashMap<u32, String> = field_pattern
            .captures_iter(&new_schema.schema)
            .filter_map(|cap| {
                let name = cap.get(1)?.as_str().to_string();
                let num: u32 = cap.get(2)?.as_str().parse().ok()?;
                Some((num, name))
            })
            .collect();

        // Protobuf compatibility rules:
        // 1. Field numbers cannot be reused with different names
        // 2. Required fields cannot be removed (proto2)
        // 3. Field types should be wire-compatible

        // Check for field number reuse with different names
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

        // Check for reserved field numbers being reused
        let reserved_pattern = regex::Regex::new(r"reserved\s+(\d+(?:,\s*\d+)*)").unwrap();
        let old_reserved: std::collections::HashSet<u32> = reserved_pattern
            .captures_iter(&existing.schema)
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

        // Check for required fields being removed (proto2)
        let required_pattern = regex::Regex::new(r"required\s+\w+\s+(\w+)").unwrap();
        let old_required: std::collections::HashSet<&str> = required_pattern
            .captures_iter(&existing.schema)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::SchemaId;

    fn json_schema(schema: &str) -> Schema {
        Schema::new(SchemaId::new(1), SchemaType::Json, schema.to_string())
    }

    fn avro_schema(schema: &str) -> Schema {
        Schema::new(SchemaId::new(1), SchemaType::Avro, schema.to_string())
    }

    fn protobuf_schema(schema: &str) -> Schema {
        Schema::new(SchemaId::new(1), SchemaType::Protobuf, schema.to_string())
    }

    #[test]
    fn test_backward_add_optional_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#,
        );
        let new = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding optional field should be backward compatible"
        );
    }

    #[test]
    fn test_backward_add_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#,
        );
        let new = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Adding required field should NOT be backward compatible"
        );
    }

    #[test]
    fn test_forward_remove_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Forward);

        let old = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#,
        );
        let new = json_schema(
            r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Removing required field should NOT be forward compatible"
        );
    }

    #[test]
    fn test_compatibility_none() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::None);

        let old = json_schema(r#"{"type":"object","properties":{"a":{"type":"string"}}}"#);
        let new = json_schema(r#"{"type":"object","properties":{"b":{"type":"integer"}}}"#);

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "NONE compatibility should allow any changes"
        );
    }

    // Avro compatibility tests
    #[test]
    fn test_avro_backward_add_field_with_default() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = avro_schema(
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#,
        );
        let new = avro_schema(
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string","default":""}]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding field with default should be backward compatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_avro_backward_add_field_without_default() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = avro_schema(
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#,
        );
        let new = avro_schema(
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Adding field without default should NOT be backward compatible"
        );
    }

    #[test]
    fn test_avro_type_promotion() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old =
            avro_schema(r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#);
        let new = avro_schema(
            r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "int -> long promotion should be compatible: {:?}",
            result.messages
        );
    }

    // Protobuf compatibility tests
    #[test]
    fn test_protobuf_field_number_reuse() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        );
        let new = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
                string email = 2;
            }
            "#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Reusing field number with different name should be incompatible"
        );
    }

    #[test]
    fn test_protobuf_add_new_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
            }
            "#,
        );
        let new = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            result.is_compatible,
            "Adding new field with new number should be compatible: {:?}",
            result.messages
        );
    }

    #[test]
    fn test_protobuf_reserved_field_reuse() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Full);

        let old = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
                reserved 2, 3;
            }
            "#,
        );
        let new = protobuf_schema(
            r#"
            message User {
                int64 id = 1;
                string name = 2;
            }
            "#,
        );

        let result = checker.check(&new, &[old]).unwrap();
        assert!(
            !result.is_compatible,
            "Reusing reserved field number should be incompatible"
        );
    }
}
