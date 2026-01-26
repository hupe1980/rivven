//! Schema inference from JSON data

use crate::schema::types::{Schema, SchemaId, SchemaType, SchemaRegistryResult, SchemaRegistryError};
use serde_json::{Map, Value as JsonValue};
use std::collections::{HashMap, HashSet};

/// Inferred schema with metadata
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// The inferred JSON Schema
    pub schema: JsonValue,
    /// Number of samples used for inference
    pub sample_count: usize,
    /// Fields that were nullable (had null values)
    pub nullable_fields: HashSet<String>,
    /// Fields that were present in all samples (potential required fields)
    pub always_present_fields: HashSet<String>,
}

impl InferredSchema {
    /// Convert to a Schema object
    pub fn to_schema(&self, id: SchemaId) -> Schema {
        Schema::new(
            id,
            SchemaType::Json,
            serde_json::to_string(&self.schema).unwrap_or_default(),
        )
    }
}

/// Schema inference engine
pub struct SchemaInference {
    /// Minimum samples before finalizing schema
    min_samples: usize,
    /// Maximum samples to analyze
    max_samples: usize,
    /// Track field presence across samples
    field_presence: HashMap<String, usize>,
    /// Track field types across samples
    field_types: HashMap<String, HashSet<String>>,
    /// Track nullable fields
    nullable_fields: HashSet<String>,
    /// Number of samples processed
    sample_count: usize,
}

impl SchemaInference {
    /// Create a new schema inference engine
    pub fn new() -> Self {
        Self::with_config(10, 1000)
    }

    /// Create with custom configuration
    pub fn with_config(min_samples: usize, max_samples: usize) -> Self {
        Self {
            min_samples,
            max_samples,
            field_presence: HashMap::new(),
            field_types: HashMap::new(),
            nullable_fields: HashSet::new(),
            sample_count: 0,
        }
    }

    /// Add a sample value for inference
    pub fn add_sample(&mut self, value: &JsonValue) -> SchemaRegistryResult<()> {
        if self.sample_count >= self.max_samples {
            return Ok(());
        }

        self.analyze_value("", value);
        self.sample_count += 1;

        Ok(())
    }

    /// Check if we have enough samples
    pub fn is_ready(&self) -> bool {
        self.sample_count >= self.min_samples
    }

    /// Finalize and return the inferred schema
    pub fn finalize(&self) -> SchemaRegistryResult<InferredSchema> {
        if self.sample_count == 0 {
            return Err(SchemaRegistryError::ParseError(
                "No samples provided for inference".to_string(),
            ));
        }

        let schema = self.build_schema();
        let always_present: HashSet<String> = self
            .field_presence
            .iter()
            .filter(|(_, &count)| count == self.sample_count)
            .map(|(field, _)| field.clone())
            .collect();

        Ok(InferredSchema {
            schema,
            sample_count: self.sample_count,
            nullable_fields: self.nullable_fields.clone(),
            always_present_fields: always_present,
        })
    }

    fn analyze_value(&mut self, path: &str, value: &JsonValue) {
        let type_name = self.json_type_name(value);
        let key = if path.is_empty() {
            "$root".to_string()
        } else {
            path.to_string()
        };

        // Track field presence
        *self.field_presence.entry(key.clone()).or_insert(0) += 1;

        // Track field types
        self.field_types
            .entry(key.clone())
            .or_default()
            .insert(type_name.to_string());

        // Track nullable
        if value.is_null() {
            self.nullable_fields.insert(key);
        }

        // Recurse into objects
        if let Some(obj) = value.as_object() {
            for (name, val) in obj {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{}.{}", path, name)
                };
                self.analyze_value(&child_path, val);
            }
        }

        // Recurse into arrays (analyze first element if present)
        if let Some(arr) = value.as_array() {
            if let Some(first) = arr.first() {
                let child_path = format!("{}[]", path);
                self.analyze_value(&child_path, first);
            }
        }
    }

    fn json_type_name(&self, value: &JsonValue) -> &'static str {
        match value {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    "integer"
                } else {
                    "number"
                }
            }
            JsonValue::String(_) => "string",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }

    fn build_schema(&self) -> JsonValue {
        let mut properties = Map::new();
        let mut required = Vec::new();

        // Group fields by their parent path
        let root_fields: HashSet<_> = self
            .field_types
            .keys()
            .filter(|k| !k.contains('.') && !k.contains("[]") && *k != "$root")
            .cloned()
            .collect();

        for field in &root_fields {
            let types = self.field_types.get(field).unwrap();
            let is_nullable = self.nullable_fields.contains(field);
            let is_always_present = self
                .field_presence
                .get(field)
                .map(|&c| c == self.sample_count)
                .unwrap_or(false);

            let field_schema = self.build_field_schema(field, types, is_nullable);
            properties.insert(field.clone(), field_schema);

            if is_always_present && !is_nullable {
                required.push(JsonValue::String(field.clone()));
            }
        }

        let mut schema = Map::new();
        schema.insert("type".to_string(), JsonValue::String("object".to_string()));
        schema.insert("properties".to_string(), JsonValue::Object(properties));

        if !required.is_empty() {
            schema.insert("required".to_string(), JsonValue::Array(required));
        }

        // Add JSON Schema version
        schema.insert(
            "$schema".to_string(),
            JsonValue::String("http://json-schema.org/draft-07/schema#".to_string()),
        );

        JsonValue::Object(schema)
    }

    fn build_field_schema(
        &self,
        path: &str,
        types: &HashSet<String>,
        is_nullable: bool,
    ) -> JsonValue {
        let mut types_vec: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
        types_vec.retain(|t| *t != "null");

        // Determine the primary type
        let primary_type = if types_vec.len() == 1 {
            types_vec[0]
        } else if types_vec.contains(&"number") && types_vec.contains(&"integer") {
            // Widen to number if we've seen both
            "number"
        } else if types_vec.is_empty() {
            "null"
        } else {
            // Multiple types - use first one
            types_vec[0]
        };

        let mut field_schema = Map::new();

        match primary_type {
            "object" => {
                field_schema.insert("type".to_string(), JsonValue::String("object".to_string()));
                // Add nested properties
                let nested_props = self.build_nested_properties(path);
                if !nested_props.is_empty() {
                    field_schema.insert(
                        "properties".to_string(),
                        JsonValue::Object(nested_props),
                    );
                }
            }
            "array" => {
                field_schema.insert("type".to_string(), JsonValue::String("array".to_string()));
                // Add items schema
                let array_path = format!("{}[]", path);
                if let Some(item_types) = self.field_types.get(&array_path) {
                    let items_nullable = self.nullable_fields.contains(&array_path);
                    let items_schema = self.build_field_schema(&array_path, item_types, items_nullable);
                    field_schema.insert("items".to_string(), items_schema);
                }
            }
            _ => {
                if is_nullable {
                    field_schema.insert(
                        "type".to_string(),
                        JsonValue::Array(vec![
                            JsonValue::String(primary_type.to_string()),
                            JsonValue::String("null".to_string()),
                        ]),
                    );
                } else {
                    field_schema.insert(
                        "type".to_string(),
                        JsonValue::String(primary_type.to_string()),
                    );
                }
            }
        }

        JsonValue::Object(field_schema)
    }

    fn build_nested_properties(&self, parent_path: &str) -> Map<String, JsonValue> {
        let mut properties = Map::new();
        let prefix = format!("{}.", parent_path);

        // Find direct children of this path
        let child_fields: HashSet<_> = self
            .field_types
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .filter_map(|k| {
                let remainder = &k[prefix.len()..];
                // Only direct children (no more dots or array markers)
                if !remainder.contains('.') && !remainder.contains("[]") {
                    Some(remainder.to_string())
                } else {
                    None
                }
            })
            .collect();

        for field in child_fields {
            let full_path = format!("{}.{}", parent_path, field);
            if let Some(types) = self.field_types.get(&full_path) {
                let is_nullable = self.nullable_fields.contains(&full_path);
                let field_schema = self.build_field_schema(&full_path, types, is_nullable);
                properties.insert(field, field_schema);
            }
        }

        properties
    }
}

impl Default for SchemaInference {
    fn default() -> Self {
        Self::new()
    }
}

/// Infer schema from a single JSON value (convenience function)
pub fn infer_schema(value: &JsonValue) -> SchemaRegistryResult<InferredSchema> {
    let mut inference = SchemaInference::with_config(1, 1);
    inference.add_sample(value)?;
    inference.finalize()
}

/// Infer schema from multiple JSON values (convenience function)
pub fn infer_schema_from_samples(values: &[JsonValue]) -> SchemaRegistryResult<InferredSchema> {
    let mut inference = SchemaInference::with_config(1, values.len());
    for value in values {
        inference.add_sample(value)?;
    }
    inference.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_simple_object() {
        let sample = json!({
            "id": 1,
            "name": "Alice",
            "active": true
        });

        let inferred = infer_schema(&sample).unwrap();
        let schema = inferred.schema;

        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["id"]["type"] == "integer");
        assert!(schema["properties"]["name"]["type"] == "string");
        assert!(schema["properties"]["active"]["type"] == "boolean");
    }

    #[test]
    fn test_infer_with_nullable() {
        let samples = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": null}),
        ];

        let inferred = infer_schema_from_samples(&samples).unwrap();

        assert!(inferred.nullable_fields.contains("name"));
        // name should allow null
        let name_type = &inferred.schema["properties"]["name"]["type"];
        assert!(name_type.is_array());
    }

    #[test]
    fn test_infer_nested_object() {
        let sample = json!({
            "user": {
                "id": 1,
                "profile": {
                    "email": "alice@example.com"
                }
            }
        });

        let inferred = infer_schema(&sample).unwrap();
        let schema = inferred.schema;

        assert_eq!(schema["properties"]["user"]["type"], "object");
        assert!(schema["properties"]["user"]["properties"]["profile"]["properties"]["email"]["type"] == "string");
    }

    #[test]
    fn test_infer_array() {
        let sample = json!({
            "items": [1, 2, 3]
        });

        let inferred = infer_schema(&sample).unwrap();
        let schema = inferred.schema;

        assert_eq!(schema["properties"]["items"]["type"], "array");
        assert_eq!(schema["properties"]["items"]["items"]["type"], "integer");
    }
}
