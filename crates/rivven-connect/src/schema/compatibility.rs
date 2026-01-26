//! Schema compatibility checking

use crate::schema::types::{CompatibilityLevel, Schema, SchemaRegistryError, SchemaRegistryResult, SchemaType};
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
            CompatibilityLevel::Backward | CompatibilityLevel::Forward | CompatibilityLevel::Full => {
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
                            messages.push(format!(
                                "FORWARD: required field '{}' removed",
                                field
                            ));
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
        _new_schema: &Schema,
        _existing: &Schema,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        // TODO: Implement Avro compatibility checking
        // For now, allow all changes
        Ok(CompatibilityResult::compatible())
    }

    fn check_protobuf_compatibility(
        &self,
        _new_schema: &Schema,
        _existing: &Schema,
    ) -> SchemaRegistryResult<CompatibilityResult> {
        // TODO: Implement Protobuf compatibility checking
        // For now, allow all changes
        Ok(CompatibilityResult::compatible())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::SchemaId;

    fn json_schema(schema: &str) -> Schema {
        Schema::new(SchemaId::new(1), SchemaType::Json, schema.to_string())
    }

    #[test]
    fn test_backward_add_optional_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#);
        let new = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}"#);

        let result = checker.check(&new, &[old]).unwrap();
        assert!(result.is_compatible, "Adding optional field should be backward compatible");
    }

    #[test]
    fn test_backward_add_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Backward);

        let old = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#);
        let new = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#);

        let result = checker.check(&new, &[old]).unwrap();
        assert!(!result.is_compatible, "Adding required field should NOT be backward compatible");
    }

    #[test]
    fn test_forward_remove_required_field() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::Forward);

        let old = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id","name"]}"#);
        let new = json_schema(r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#);

        let result = checker.check(&new, &[old]).unwrap();
        assert!(!result.is_compatible, "Removing required field should NOT be forward compatible");
    }

    #[test]
    fn test_compatibility_none() {
        let checker = CompatibilityChecker::new(CompatibilityLevel::None);

        let old = json_schema(r#"{"type":"object","properties":{"a":{"type":"string"}}}"#);
        let new = json_schema(r#"{"type":"object","properties":{"b":{"type":"integer"}}}"#);

        let result = checker.check(&new, &[old]).unwrap();
        assert!(result.is_compatible, "NONE compatibility should allow any changes");
    }
}
