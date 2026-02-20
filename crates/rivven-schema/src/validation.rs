//! Content Validation Engine
//!
//! Executes validation rules against schemas beyond compatibility checking.
//! Supports organizational policies and data quality requirements.
//!
//! # Validation Rule Types
//!
//! | Type | Description | Config |
//! |------|-------------|--------|
//! | `MaxSize` | Maximum schema size | `{"max_bytes": 102400}` |
//! | `NamingConvention` | Name pattern matching | `{"pattern": "^[A-Z]", "field": "name"}` |
//! | `FieldRequired` | Required fields | `{"field": "doc"}` |
//! | `FieldType` | Field type constraints | `{"field": "id", "type": "long"}` |
//! | `Regex` | Content pattern matching | `{"pattern": "...", "field": "..."}` |
//! | `JsonSchema` | JSON Schema validation | `{"schema": {...}}` |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_schema::validation::{ValidationEngine, ValidationEngineConfig};
//! use rivven_schema::types::{ValidationRule, ValidationRuleType, SchemaType};
//!
//! let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
//!
//! // Add rules
//! engine.add_rule(ValidationRule::new(
//!     "max-size",
//!     ValidationRuleType::MaxSize,
//!     r#"{"max_bytes": 102400}"#,
//! ));
//!
//! // Validate a schema
//! let report = engine.validate(SchemaType::Avro, "users-value", schema_str)?;
//! if !report.is_valid() {
//!     println!("Validation errors: {:?}", report.error_messages());
//! }
//! ```

use crate::error::{SchemaError, SchemaResult};
use crate::types::{
    SchemaType, ValidationLevel, ValidationReport, ValidationResult, ValidationRule,
    ValidationRuleType,
};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::debug;

/// Configuration for the validation engine
#[derive(Debug, Clone)]
pub struct ValidationEngineConfig {
    /// Whether to fail fast on first error
    pub fail_fast: bool,
    /// Whether warnings should block registration
    pub warnings_as_errors: bool,
    /// Maximum number of rules to evaluate per schema
    pub max_rules_per_schema: usize,
}

impl Default for ValidationEngineConfig {
    fn default() -> Self {
        Self {
            fail_fast: false,
            warnings_as_errors: false,
            max_rules_per_schema: 100,
        }
    }
}

impl ValidationEngineConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    pub fn with_warnings_as_errors(mut self, warnings_as_errors: bool) -> Self {
        self.warnings_as_errors = warnings_as_errors;
        self
    }
}

/// Validation engine for executing rules against schemas
pub struct ValidationEngine {
    /// Configuration
    config: ValidationEngineConfig,
    /// Global rules (apply to all schemas)
    global_rules: Vec<ValidationRule>,
    /// Per-subject rules
    subject_rules: HashMap<String, Vec<ValidationRule>>,
}

impl ValidationEngine {
    /// Create a new validation engine
    pub fn new(config: ValidationEngineConfig) -> Self {
        Self {
            config,
            global_rules: Vec::new(),
            subject_rules: HashMap::new(),
        }
    }

    /// Add a global rule
    pub fn add_rule(&mut self, rule: ValidationRule) {
        self.global_rules.push(rule);
    }

    /// Add multiple global rules
    pub fn add_rules(&mut self, rules: impl IntoIterator<Item = ValidationRule>) {
        self.global_rules.extend(rules);
    }

    /// Add a rule for a specific subject
    pub fn add_subject_rule(&mut self, subject: &str, rule: ValidationRule) {
        self.subject_rules
            .entry(subject.to_string())
            .or_default()
            .push(rule);
    }

    /// Remove a rule by name
    pub fn remove_rule(&mut self, name: &str) -> bool {
        let before = self.global_rules.len();
        self.global_rules.retain(|r| r.name != name);

        for rules in self.subject_rules.values_mut() {
            rules.retain(|r| r.name != name);
        }

        self.global_rules.len() != before
    }

    /// Get all rules
    pub fn rules(&self) -> &[ValidationRule] {
        &self.global_rules
    }

    /// List all rules (alias for rules(), returns owned Vec)
    pub fn list_rules(&self) -> Vec<ValidationRule> {
        self.global_rules.clone()
    }

    /// Get rules for a subject
    pub fn subject_rules(&self, subject: &str) -> Option<&[ValidationRule]> {
        self.subject_rules.get(subject).map(|v| v.as_slice())
    }

    /// Clear all rules
    pub fn clear(&mut self) {
        self.global_rules.clear();
        self.subject_rules.clear();
    }

    /// Validate a schema against all applicable rules
    pub fn validate(
        &self,
        schema_type: SchemaType,
        subject: &str,
        schema: &str,
    ) -> SchemaResult<ValidationReport> {
        let mut report = ValidationReport::new();
        let mut rules_evaluated = 0;

        // Collect applicable rules
        let applicable_rules: Vec<&ValidationRule> = self
            .global_rules
            .iter()
            .chain(self.subject_rules.get(subject).into_iter().flatten())
            .filter(|r| r.applies(schema_type, subject))
            .take(self.config.max_rules_per_schema)
            .collect();

        debug!(
            "Validating schema for subject {} with {} applicable rules",
            subject,
            applicable_rules.len()
        );

        for rule in applicable_rules {
            let result = self.execute_rule(rule, schema_type, schema)?;

            // Check for fail-fast
            if self.config.fail_fast && !result.passed && result.level == ValidationLevel::Error {
                report.add_result(result);
                return Ok(report);
            }

            report.add_result(result);
            rules_evaluated += 1;
        }

        debug!(
            "Validation complete: {} rules evaluated, {} errors, {} warnings",
            rules_evaluated, report.summary.errors, report.summary.warnings
        );

        Ok(report)
    }

    /// Execute a single validation rule
    fn execute_rule(
        &self,
        rule: &ValidationRule,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        match rule.rule_type {
            ValidationRuleType::MaxSize => self.validate_max_size(rule, schema),
            ValidationRuleType::NamingConvention => {
                self.validate_naming_convention(rule, schema_type, schema)
            }
            ValidationRuleType::FieldRequired => {
                self.validate_field_required(rule, schema_type, schema)
            }
            ValidationRuleType::FieldType => self.validate_field_type(rule, schema_type, schema),
            ValidationRuleType::Regex => self.validate_regex(rule, schema),
            ValidationRuleType::JsonSchema => self.validate_json_schema(rule, schema),
        }
    }

    /// Validate maximum schema size
    fn validate_max_size(
        &self,
        rule: &ValidationRule,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[derive(Deserialize)]
        struct Config {
            max_bytes: usize,
        }

        let config: Config = serde_json::from_str(&rule.config)
            .map_err(|e| SchemaError::Validation(format!("Invalid max_size config: {}", e)))?;

        let size = schema.len();
        if size > config.max_bytes {
            Ok(ValidationResult::fail(
                &rule.name,
                rule.level,
                format!(
                    "Schema size {} bytes exceeds maximum {} bytes",
                    size, config.max_bytes
                ),
            ))
        } else {
            Ok(ValidationResult::pass(&rule.name))
        }
    }

    /// Validate naming convention using regex
    fn validate_naming_convention(
        &self,
        rule: &ValidationRule,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[derive(Deserialize)]
        struct Config {
            pattern: String,
            #[serde(default = "default_name_field")]
            field: String,
        }

        fn default_name_field() -> String {
            "name".to_string()
        }

        let config: Config = serde_json::from_str(&rule.config).map_err(|e| {
            SchemaError::Validation(format!("Invalid naming_convention config: {}", e))
        })?;

        // size-limited regex to prevent ReDoS from adversarial patterns
        let regex = regex::RegexBuilder::new(&config.pattern)
            .size_limit(1_000_000)
            .build()
            .map_err(|e| SchemaError::Validation(format!("Invalid regex pattern: {}", e)))?;

        // Extract name based on schema type
        let name = match schema_type {
            SchemaType::Avro | SchemaType::Json => {
                let parsed: serde_json::Value = serde_json::from_str(schema)
                    .map_err(|e| SchemaError::Validation(format!("Invalid JSON schema: {}", e)))?;
                parsed
                    .get(&config.field)
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            }
            SchemaType::Protobuf => {
                // Extract message name from protobuf (simplified)
                extract_protobuf_name(schema)
            }
        };

        match name {
            Some(n) if regex.is_match(&n) => Ok(ValidationResult::pass(&rule.name)),
            Some(n) => Ok(ValidationResult::fail(
                &rule.name,
                rule.level,
                format!("Name '{}' does not match pattern '{}'", n, config.pattern),
            )),
            None => Ok(ValidationResult::fail(
                &rule.name,
                rule.level,
                format!("Could not extract '{}' field from schema", config.field),
            )),
        }
    }

    /// Validate required field presence
    fn validate_field_required(
        &self,
        rule: &ValidationRule,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[derive(Deserialize)]
        struct Config {
            field: String,
        }

        let config: Config = serde_json::from_str(&rule.config).map_err(|e| {
            SchemaError::Validation(format!("Invalid field_required config: {}", e))
        })?;

        match schema_type {
            SchemaType::Avro | SchemaType::Json => {
                let parsed: serde_json::Value = serde_json::from_str(schema)
                    .map_err(|e| SchemaError::Validation(format!("Invalid JSON schema: {}", e)))?;

                if has_field_recursive(&parsed, &config.field) {
                    Ok(ValidationResult::pass(&rule.name))
                } else {
                    Ok(ValidationResult::fail(
                        &rule.name,
                        rule.level,
                        format!("Required field '{}' not found in schema", config.field),
                    ))
                }
            }
            SchemaType::Protobuf => {
                // use word-boundary regex to avoid partial field name matches
                let field_pattern = format!(r"\b{}\b", regex::escape(&config.field));
                let field_regex = regex::Regex::new(&field_pattern).map_err(|e| {
                    SchemaError::Validation(format!("Invalid field pattern: {}", e))
                })?;
                if field_regex.is_match(schema) {
                    Ok(ValidationResult::pass(&rule.name))
                } else {
                    Ok(ValidationResult::fail(
                        &rule.name,
                        rule.level,
                        format!("Required field '{}' not found in schema", config.field),
                    ))
                }
            }
        }
    }

    /// Validate field type constraints
    fn validate_field_type(
        &self,
        rule: &ValidationRule,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[derive(Deserialize)]
        struct Config {
            field: String,
            #[serde(rename = "type")]
            expected_type: String,
        }

        let config: Config = serde_json::from_str(&rule.config)
            .map_err(|e| SchemaError::Validation(format!("Invalid field_type config: {}", e)))?;

        match schema_type {
            SchemaType::Avro => {
                let parsed: serde_json::Value = serde_json::from_str(schema)
                    .map_err(|e| SchemaError::Validation(format!("Invalid Avro schema: {}", e)))?;

                if let Some(field_type) = find_avro_field_type(&parsed, &config.field) {
                    if field_type == config.expected_type {
                        Ok(ValidationResult::pass(&rule.name))
                    } else {
                        Ok(ValidationResult::fail(
                            &rule.name,
                            rule.level,
                            format!(
                                "Field '{}' has type '{}', expected '{}'",
                                config.field, field_type, config.expected_type
                            ),
                        ))
                    }
                } else {
                    Ok(ValidationResult::fail(
                        &rule.name,
                        rule.level,
                        format!("Field '{}' not found in schema", config.field),
                    ))
                }
            }
            _ => {
                // Skip for non-Avro schemas
                Ok(ValidationResult::pass(&rule.name))
            }
        }
    }

    /// Validate content against regex pattern
    fn validate_regex(
        &self,
        rule: &ValidationRule,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[derive(Deserialize)]
        struct Config {
            pattern: String,
            #[serde(default)]
            must_match: bool,
        }

        let config: Config = serde_json::from_str(&rule.config)
            .map_err(|e| SchemaError::Validation(format!("Invalid regex config: {}", e)))?;

        // size-limited regex to prevent ReDoS from adversarial patterns
        let regex = regex::RegexBuilder::new(&config.pattern)
            .size_limit(1_000_000)
            .build()
            .map_err(|e| SchemaError::Validation(format!("Invalid regex pattern: {}", e)))?;

        let matches = regex.is_match(schema);
        let expected = config.must_match;

        if matches == expected {
            Ok(ValidationResult::pass(&rule.name))
        } else if expected {
            Ok(ValidationResult::fail(
                &rule.name,
                rule.level,
                format!(
                    "Schema does not match required pattern '{}'",
                    config.pattern
                ),
            ))
        } else {
            Ok(ValidationResult::fail(
                &rule.name,
                rule.level,
                format!("Schema matches forbidden pattern '{}'", config.pattern),
            ))
        }
    }

    /// Validate against JSON Schema (meta-validation)
    fn validate_json_schema(
        &self,
        rule: &ValidationRule,
        schema: &str,
    ) -> SchemaResult<ValidationResult> {
        #[cfg(feature = "json-schema")]
        {
            #[derive(Deserialize)]
            struct Config {
                schema: serde_json::Value,
            }

            let config: Config = serde_json::from_str(&rule.config).map_err(|e| {
                SchemaError::Validation(format!("Invalid json_schema config: {}", e))
            })?;

            // Parse the schema being validated
            let instance: serde_json::Value = serde_json::from_str(schema)
                .map_err(|e| SchemaError::Validation(format!("Invalid JSON in schema: {}", e)))?;

            // Compile the validation schema
            let validator = jsonschema::JSONSchema::compile(&config.schema).map_err(|e| {
                SchemaError::Validation(format!("Invalid JSON Schema validator: {}", e))
            })?;

            if validator.is_valid(&instance) {
                Ok(ValidationResult::pass(&rule.name))
            } else {
                let errors: Vec<String> = validator
                    .validate(&instance)
                    .err()
                    .into_iter()
                    .flatten()
                    .map(|e| e.to_string())
                    .take(3)
                    .collect();

                Ok(ValidationResult::fail(
                    &rule.name,
                    rule.level,
                    format!("JSON Schema validation failed: {}", errors.join("; ")),
                ))
            }
        }

        #[cfg(not(feature = "json-schema"))]
        {
            let _ = (rule, schema); // Suppress unused warnings
            tracing::warn!("JSON Schema validation requires the 'json-schema' feature");
            Ok(ValidationResult::fail(
                &rule.name,
                ValidationLevel::Warning,
                "JSON Schema validation skipped: 'json-schema' feature not enabled",
            ))
        }
    }
}

/// Extract protobuf message name (simplified)
fn extract_protobuf_name(schema: &str) -> Option<String> {
    for line in schema.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("message ") {
            let name = trimmed
                .strip_prefix("message ")?
                .split_whitespace()
                .next()?;
            return Some(name.to_string());
        }
    }
    None
}

/// Check if a JSON value has a field recursively
fn has_field_recursive(value: &serde_json::Value, field: &str) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            if map.contains_key(field) {
                return true;
            }
            for v in map.values() {
                if has_field_recursive(v, field) {
                    return true;
                }
            }
            false
        }
        serde_json::Value::Array(arr) => arr.iter().any(|v| has_field_recursive(v, field)),
        _ => false,
    }
}

/// Find the type of a field in an Avro schema
fn find_avro_field_type(schema: &serde_json::Value, field_name: &str) -> Option<String> {
    if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            if field.get("name").and_then(|n| n.as_str()) == Some(field_name) {
                return field.get("type").map(|t| match t {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Object(o) => o
                        .get("type")
                        .and_then(|t| t.as_str())
                        .unwrap_or("complex")
                        .to_string(),
                    serde_json::Value::Array(_) => "union".to_string(),
                    _ => "unknown".to_string(),
                });
            }
        }
    }
    None
}

/// Common validation rules presets
pub mod presets {
    use super::*;

    /// Create a max size rule (default 100KB)
    pub fn max_size(max_bytes: usize) -> ValidationRule {
        ValidationRule::new(
            "max-schema-size",
            ValidationRuleType::MaxSize,
            format!(r#"{{"max_bytes": {}}}"#, max_bytes),
        )
        .with_description(format!("Schema must be smaller than {} bytes", max_bytes))
    }

    /// Create a rule requiring 'doc' field in Avro schemas
    pub fn require_doc() -> ValidationRule {
        ValidationRule::new(
            "require-doc",
            ValidationRuleType::FieldRequired,
            r#"{"field": "doc"}"#,
        )
        .with_description("Schema must have a 'doc' field for documentation")
        .with_schema_types(vec![SchemaType::Avro])
    }

    /// Create a rule requiring 'namespace' field in Avro schemas
    pub fn require_namespace() -> ValidationRule {
        ValidationRule::new(
            "require-namespace",
            ValidationRuleType::FieldRequired,
            r#"{"field": "namespace"}"#,
        )
        .with_description("Avro schema must have a namespace")
        .with_schema_types(vec![SchemaType::Avro])
    }

    /// Create a PascalCase naming rule
    pub fn pascal_case_name() -> ValidationRule {
        ValidationRule::new(
            "pascal-case-name",
            ValidationRuleType::NamingConvention,
            r#"{"pattern": "^[A-Z][a-zA-Z0-9]*$", "field": "name"}"#,
        )
        .with_description("Schema name must be PascalCase")
        .with_level(ValidationLevel::Warning)
    }

    /// Create a rule forbidding certain patterns (e.g., PII markers)
    pub fn forbid_pattern(name: &str, pattern: &str, description: &str) -> ValidationRule {
        ValidationRule::new(
            name,
            ValidationRuleType::Regex,
            format!(r#"{{"pattern": "{}", "must_match": false}}"#, pattern),
        )
        .with_description(description)
    }

    /// Standard production ruleset
    pub fn production_ruleset() -> Vec<ValidationRule> {
        vec![
            max_size(100 * 1024), // 100KB max
            require_doc(),        // Documentation required
            require_namespace(),  // Namespace required for Avro
            pascal_case_name(),   // PascalCase naming
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_engine_creation() {
        let engine = ValidationEngine::new(ValidationEngineConfig::default());
        assert!(engine.rules().is_empty());
    }

    #[test]
    fn test_add_and_remove_rule() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());

        engine.add_rule(presets::max_size(1024));
        assert_eq!(engine.rules().len(), 1);

        engine.remove_rule("max-schema-size");
        assert!(engine.rules().is_empty());
    }

    #[test]
    fn test_max_size_validation() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rule(presets::max_size(100));

        // Small schema passes
        let small_schema = r#"{"type":"string"}"#;
        let report = engine
            .validate(SchemaType::Avro, "test", small_schema)
            .unwrap();
        assert!(report.is_valid());

        // Large schema fails
        let large_schema = "x".repeat(200);
        let report = engine
            .validate(SchemaType::Avro, "test", &large_schema)
            .unwrap();
        assert!(!report.is_valid());
    }

    #[test]
    fn test_field_required_validation() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rule(presets::require_doc());

        // Schema with doc passes
        let with_doc = r#"{"type":"record","name":"User","doc":"A user","fields":[]}"#;
        let report = engine.validate(SchemaType::Avro, "test", with_doc).unwrap();
        assert!(report.is_valid());

        // Schema without doc fails
        let without_doc = r#"{"type":"record","name":"User","fields":[]}"#;
        let report = engine
            .validate(SchemaType::Avro, "test", without_doc)
            .unwrap();
        assert!(!report.is_valid());
    }

    #[test]
    fn test_naming_convention_validation() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rule(
            ValidationRule::new(
                "pascal-case",
                ValidationRuleType::NamingConvention,
                r#"{"pattern": "^[A-Z][a-zA-Z0-9]*$"}"#,
            )
            .with_level(ValidationLevel::Error),
        );

        // PascalCase passes
        let pascal = r#"{"name":"UserEvent"}"#;
        let report = engine.validate(SchemaType::Avro, "test", pascal).unwrap();
        assert!(report.is_valid());

        // camelCase fails
        let camel = r#"{"name":"userEvent"}"#;
        let report = engine.validate(SchemaType::Avro, "test", camel).unwrap();
        assert!(!report.is_valid());
    }

    #[test]
    fn test_regex_validation() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rule(presets::forbid_pattern(
            "no-ssn",
            r"ssn|social.?security",
            "Schema must not contain SSN fields",
        ));

        // Schema without SSN passes
        let clean = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#;
        let report = engine.validate(SchemaType::Avro, "test", clean).unwrap();
        assert!(report.is_valid());

        // Schema with SSN fails
        let with_ssn =
            r#"{"type":"record","name":"User","fields":[{"name":"ssn","type":"string"}]}"#;
        let report = engine.validate(SchemaType::Avro, "test", with_ssn).unwrap();
        assert!(!report.is_valid());
    }

    #[test]
    fn test_field_type_validation() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rule(ValidationRule::new(
            "id-must-be-long",
            ValidationRuleType::FieldType,
            r#"{"field": "id", "type": "long"}"#,
        ));

        // Correct type passes
        let correct = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"long"}]}"#;
        let report = engine.validate(SchemaType::Avro, "test", correct).unwrap();
        assert!(report.is_valid());

        // Wrong type fails
        let wrong = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;
        let report = engine.validate(SchemaType::Avro, "test", wrong).unwrap();
        assert!(!report.is_valid());
    }

    #[test]
    fn test_subject_specific_rules() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());

        // Add rule only for specific subject
        engine.add_subject_rule(
            "users-value",
            ValidationRule::new(
                "users-rule",
                ValidationRuleType::MaxSize,
                r#"{"max_bytes": 50}"#,
            ),
        );

        let schema = r#"{"type":"string"}"#;

        // Different subject doesn't have the rule
        let report = engine
            .validate(SchemaType::Avro, "orders-value", schema)
            .unwrap();
        assert!(report.is_valid());

        // Target subject has the rule
        // (schema is small enough anyway, but rule is evaluated)
        let report = engine
            .validate(SchemaType::Avro, "users-value", schema)
            .unwrap();
        assert!(report.is_valid());
    }

    #[test]
    fn test_fail_fast() {
        let config = ValidationEngineConfig::default().with_fail_fast(true);
        let mut engine = ValidationEngine::new(config);

        engine.add_rule(ValidationRule::new(
            "rule1",
            ValidationRuleType::MaxSize,
            r#"{"max_bytes": 1}"#,
        ));
        engine.add_rule(ValidationRule::new(
            "rule2",
            ValidationRuleType::MaxSize,
            r#"{"max_bytes": 2}"#,
        ));

        let schema = "xxx"; // Will fail both rules
        let report = engine.validate(SchemaType::Avro, "test", schema).unwrap();

        // Should stop after first failure
        assert_eq!(report.results.len(), 1);
    }

    #[test]
    fn test_production_ruleset() {
        let mut engine = ValidationEngine::new(ValidationEngineConfig::default());
        engine.add_rules(presets::production_ruleset());

        // Valid production schema
        let schema = r#"{
            "type": "record",
            "name": "UserCreated",
            "namespace": "com.example.events",
            "doc": "Event emitted when a new user is created",
            "fields": [
                {"name": "userId", "type": "long", "doc": "Unique user ID"}
            ]
        }"#;

        let report = engine
            .validate(SchemaType::Avro, "users-value", schema)
            .unwrap();
        assert!(report.is_valid(), "Errors: {:?}", report.error_messages());
    }

    #[test]
    fn test_protobuf_name_extraction() {
        let proto = r#"
            syntax = "proto3";
            message UserEvent {
                int64 id = 1;
            }
        "#;

        let name = extract_protobuf_name(proto);
        assert_eq!(name, Some("UserEvent".to_string()));
    }
}
