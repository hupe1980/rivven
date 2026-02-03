//! Schema types and data structures
//!
//! This module provides core types for the Schema Registry:
//! - [`SchemaId`], [`Subject`], [`SchemaVersion`] - Core identifiers
//! - [`Schema`], [`SubjectVersion`] - Schema data structures
//! - [`SchemaContext`] - Multi-tenant context/group support
//! - [`VersionState`] - Version lifecycle management
//! - [`ValidationRule`] - Custom content validation rules
//! - [`Mode`], [`CompatibilityLevel`] - Registry configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Re-export SchemaType from rivven-protocol (single source of truth)
pub use rivven_protocol::SchemaType;

/// Unique identifier for a schema (global across all subjects)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(pub u32);

impl SchemaId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for SchemaId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

/// Subject (typically topic-name + "-key" or "-value")
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Subject(pub String);

impl Subject {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Create a key subject for a topic
    pub fn key(topic: &str) -> Self {
        Self(format!("{}-key", topic))
    }

    /// Create a value subject for a topic
    pub fn value(topic: &str) -> Self {
        Self(format!("{}-value", topic))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Subject {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Subject {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// Schema Context (Multi-tenant support)
// ============================================================================

/// Schema context for multi-tenant isolation
///
/// Contexts allow organizing schemas into isolated namespaces, similar to
/// Confluent's Schema Registry contexts. Each context has its own set of
/// subjects and schemas.
///
/// # Examples
///
/// ```rust
/// use rivven_schema::types::SchemaContext;
///
/// // Default context (root)
/// let default_ctx = SchemaContext::default();
///
/// // Tenant-specific context
/// let tenant_ctx = SchemaContext::new("tenant-123");
///
/// // Environment context
/// let prod_ctx = SchemaContext::new("production");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaContext {
    /// Context name (empty string = default/root context)
    name: String,
    /// Context description
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// Whether this context is active
    #[serde(default = "default_true")]
    active: bool,
}

fn default_true() -> bool {
    true
}

impl SchemaContext {
    /// The default (root) context name
    pub const DEFAULT: &'static str = "";

    /// Create a new context with the given name
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            active: true,
        }
    }

    /// Create the default (root) context
    pub fn default_context() -> Self {
        Self::new("")
    }

    /// Get the context name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if this is the default context
    pub fn is_default(&self) -> bool {
        self.name.is_empty()
    }

    /// Set the context description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Get the context description
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Set the active state
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Check if the context is active
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Create a fully qualified subject name with context prefix
    ///
    /// Format: `:.{context}:{subject}` (Confluent compatible)
    pub fn qualify_subject(&self, subject: &Subject) -> String {
        if self.is_default() {
            subject.0.clone()
        } else {
            format!(":.{}:{}", self.name, subject.0)
        }
    }

    /// Parse a qualified subject name into context and subject
    pub fn parse_qualified(qualified: &str) -> (SchemaContext, Subject) {
        if let Some(rest) = qualified.strip_prefix(":.") {
            if let Some(colon_pos) = rest.find(':') {
                let context_name = &rest[..colon_pos];
                let subject_name = &rest[colon_pos + 1..];
                return (SchemaContext::new(context_name), Subject::new(subject_name));
            }
        }
        (SchemaContext::default_context(), Subject::new(qualified))
    }
}

impl Default for SchemaContext {
    fn default() -> Self {
        Self::default_context()
    }
}

impl std::fmt::Display for SchemaContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_default() {
            write!(f, "(default)")
        } else {
            write!(f, "{}", self.name)
        }
    }
}

// ============================================================================
// Version State (Lifecycle management)
// ============================================================================

/// State of a schema version within a subject
///
/// Allows managing the lifecycle of schema versions without deletion.
/// This is important for schema evolution and migration workflows.
///
/// # State Transitions
///
/// ```text
///     ┌─────────┐
///     │ Enabled │ ◄───── Initial state
///     └────┬────┘
///          │ deprecate()
///          ▼
///   ┌──────────────┐
///   │  Deprecated  │ ◄───── Warning state
///   └──────┬───────┘
///          │ disable()
///          ▼
///     ┌──────────┐
///     │ Disabled │ ◄───── Blocked state
///     └──────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum VersionState {
    /// Version is active and can be used (default)
    #[default]
    Enabled,
    /// Version is deprecated - still usable but clients are warned
    Deprecated,
    /// Version is disabled - cannot be used for new registrations
    Disabled,
}

impl VersionState {
    /// Check if the version is usable (enabled or deprecated)
    pub fn is_usable(&self) -> bool {
        matches!(self, VersionState::Enabled | VersionState::Deprecated)
    }

    /// Check if this state requires client warnings
    pub fn requires_warning(&self) -> bool {
        matches!(self, VersionState::Deprecated)
    }

    /// Check if this version is blocked from use
    pub fn is_blocked(&self) -> bool {
        matches!(self, VersionState::Disabled)
    }

    /// Transition to deprecated state
    pub fn deprecate(&self) -> VersionState {
        match self {
            VersionState::Enabled => VersionState::Deprecated,
            other => *other,
        }
    }

    /// Transition to disabled state
    pub fn disable(&self) -> VersionState {
        VersionState::Disabled
    }

    /// Transition back to enabled state
    pub fn enable(&self) -> VersionState {
        VersionState::Enabled
    }
}

impl std::fmt::Display for VersionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionState::Enabled => write!(f, "ENABLED"),
            VersionState::Deprecated => write!(f, "DEPRECATED"),
            VersionState::Disabled => write!(f, "DISABLED"),
        }
    }
}

impl std::str::FromStr for VersionState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ENABLED" => Ok(VersionState::Enabled),
            "DEPRECATED" => Ok(VersionState::Deprecated),
            "DISABLED" => Ok(VersionState::Disabled),
            _ => Err(format!("Invalid version state: {}", s)),
        }
    }
}

// ============================================================================
// Content Validation Rules
// ============================================================================

/// Rule type for content validation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationRuleType {
    /// JSON Schema validation rule
    JsonSchema,
    /// Regular expression pattern
    Regex,
    /// Field presence requirement
    FieldRequired,
    /// Field type constraint
    FieldType,
    /// Custom CEL expression
    Cel,
    /// Maximum schema size in bytes
    MaxSize,
    /// Schema naming convention
    NamingConvention,
}

impl std::str::FromStr for ValidationRuleType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "JSON_SCHEMA" | "JSONSCHEMA" => Ok(ValidationRuleType::JsonSchema),
            "REGEX" => Ok(ValidationRuleType::Regex),
            "FIELD_REQUIRED" | "FIELDREQUIRED" => Ok(ValidationRuleType::FieldRequired),
            "FIELD_TYPE" | "FIELDTYPE" => Ok(ValidationRuleType::FieldType),
            "CEL" => Ok(ValidationRuleType::Cel),
            "MAX_SIZE" | "MAXSIZE" => Ok(ValidationRuleType::MaxSize),
            "NAMING_CONVENTION" | "NAMINGCONVENTION" => Ok(ValidationRuleType::NamingConvention),
            _ => Err(format!("Invalid validation rule type: {}", s)),
        }
    }
}

/// Content validation rule for custom schema validation
///
/// Validation rules extend beyond compatibility checking to enforce
/// organizational policies and data quality requirements.
///
/// # Examples
///
/// ```rust
/// use rivven_schema::types::{ValidationRule, ValidationRuleType, ValidationLevel};
///
/// // Require a "doc" field in all Avro schemas
/// let doc_required = ValidationRule::new(
///     "doc-required",
///     ValidationRuleType::FieldRequired,
///     r#"{"field": "doc"}"#,
/// ).with_level(ValidationLevel::Error);
///
/// // Enforce naming convention
/// let naming_rule = ValidationRule::new(
///     "pascal-case",
///     ValidationRuleType::NamingConvention,
///     r#"{"pattern": "^[A-Z][a-zA-Z0-9]*$"}"#,
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    /// Unique rule name
    pub name: String,
    /// Rule type
    pub rule_type: ValidationRuleType,
    /// Rule configuration (JSON)
    pub config: String,
    /// Validation level
    #[serde(default)]
    pub level: ValidationLevel,
    /// Description of what the rule checks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether the rule is active
    #[serde(default = "default_true")]
    pub active: bool,
    /// Schema types this rule applies to (empty = all)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applies_to: Vec<SchemaType>,
    /// Subjects this rule applies to (regex patterns, empty = all)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subject_patterns: Vec<String>,
}

impl ValidationRule {
    /// Create a new validation rule
    pub fn new(
        name: impl Into<String>,
        rule_type: ValidationRuleType,
        config: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            rule_type,
            config: config.into(),
            level: ValidationLevel::Error,
            description: None,
            active: true,
            applies_to: Vec::new(),
            subject_patterns: Vec::new(),
        }
    }

    /// Set the validation level
    pub fn with_level(mut self, level: ValidationLevel) -> Self {
        self.level = level;
        self
    }

    /// Set the description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set the active state
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Add schema types this rule applies to
    pub fn with_schema_types(mut self, types: Vec<SchemaType>) -> Self {
        self.applies_to = types;
        self
    }

    /// Add subject patterns this rule applies to
    pub fn with_subject_patterns(mut self, patterns: Vec<String>) -> Self {
        self.subject_patterns = patterns;
        self
    }

    /// Alias for with_subject_patterns for convenience
    pub fn for_subjects(self, patterns: Vec<String>) -> Self {
        self.with_subject_patterns(patterns)
    }

    /// Alias for with_schema_types for convenience
    pub fn for_schema_types(self, types: Vec<SchemaType>) -> Self {
        self.with_schema_types(types)
    }

    /// Check if this rule applies to the given schema type and subject
    pub fn applies(&self, schema_type: SchemaType, subject: &str) -> bool {
        if !self.active {
            return false;
        }

        // Check schema type filter
        if !self.applies_to.is_empty() && !self.applies_to.contains(&schema_type) {
            return false;
        }

        // Check subject pattern filter
        if !self.subject_patterns.is_empty() {
            let matches_any = self.subject_patterns.iter().any(|pattern| {
                regex::Regex::new(pattern)
                    .map(|re| re.is_match(subject))
                    .unwrap_or(false)
            });
            if !matches_any {
                return false;
            }
        }

        true
    }

    /// Get the rule name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the rule type
    pub fn rule_type(&self) -> ValidationRuleType {
        self.rule_type.clone()
    }

    /// Get the rule config
    pub fn config(&self) -> &str {
        &self.config
    }

    /// Get the validation level
    pub fn level(&self) -> ValidationLevel {
        self.level
    }

    /// Get the description
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Check if the rule is active
    pub fn is_active(&self) -> bool {
        self.active
    }
}

/// Validation level for rules
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ValidationLevel {
    /// Validation failure is an error (blocks registration)
    #[default]
    Error,
    /// Validation failure is a warning (logged but allowed)
    Warning,
    /// Validation is informational only
    Info,
}

impl std::fmt::Display for ValidationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationLevel::Error => write!(f, "ERROR"),
            ValidationLevel::Warning => write!(f, "WARNING"),
            ValidationLevel::Info => write!(f, "INFO"),
        }
    }
}

impl std::str::FromStr for ValidationLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ERROR" => Ok(ValidationLevel::Error),
            "WARNING" | "WARN" => Ok(ValidationLevel::Warning),
            "INFO" => Ok(ValidationLevel::Info),
            _ => Err(format!("Invalid validation level: {}", s)),
        }
    }
}

/// Result of a validation rule check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Rule name that was checked
    pub rule_name: String,
    /// Whether validation passed
    pub passed: bool,
    /// Validation level
    pub level: ValidationLevel,
    /// Message describing the result
    pub message: String,
    /// Optional details (JSON)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl ValidationResult {
    /// Create a passing result
    pub fn pass(rule_name: impl Into<String>) -> Self {
        Self {
            rule_name: rule_name.into(),
            passed: true,
            level: ValidationLevel::Info,
            message: "Validation passed".to_string(),
            details: None,
        }
    }

    /// Create a failing result
    pub fn fail(
        rule_name: impl Into<String>,
        level: ValidationLevel,
        message: impl Into<String>,
    ) -> Self {
        Self {
            rule_name: rule_name.into(),
            passed: false,
            level,
            message: message.into(),
            details: None,
        }
    }

    /// Add details to the result
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

/// Collection of validation results
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationReport {
    /// All validation results
    pub results: Vec<ValidationResult>,
    /// Summary counts
    pub summary: ValidationSummary,
}

impl ValidationReport {
    /// Create a new empty report
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a result to the report
    pub fn add_result(&mut self, result: ValidationResult) {
        if result.passed {
            self.summary.passed += 1;
        } else {
            match result.level {
                ValidationLevel::Error => self.summary.errors += 1,
                ValidationLevel::Warning => self.summary.warnings += 1,
                ValidationLevel::Info => self.summary.info += 1,
            }
        }
        self.results.push(result);
    }

    /// Check if all validations passed (no errors)
    pub fn is_valid(&self) -> bool {
        self.summary.errors == 0
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        self.summary.warnings > 0
    }

    /// Get all error messages
    pub fn error_messages(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| !r.passed && r.level == ValidationLevel::Error)
            .map(|r| r.message.clone())
            .collect()
    }

    /// Get all warning messages
    pub fn warning_messages(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| !r.passed && r.level == ValidationLevel::Warning)
            .map(|r| r.message.clone())
            .collect()
    }

    /// Get all info messages
    pub fn info_messages(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| !r.passed && r.level == ValidationLevel::Info)
            .map(|r| r.message.clone())
            .collect()
    }
}

/// Summary counts for validation report
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationSummary {
    /// Number of passed validations
    pub passed: usize,
    /// Number of error-level failures
    pub errors: usize,
    /// Number of warning-level failures
    pub warnings: usize,
    /// Number of info-level failures
    pub info: usize,
}

/// Version number within a subject
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaVersion(pub u32);

impl SchemaVersion {
    /// The special "latest" version marker
    pub const LATEST: u32 = u32::MAX;

    pub fn new(version: u32) -> Self {
        Self(version)
    }

    /// Create a version that represents "latest"
    pub fn latest() -> Self {
        Self(Self::LATEST)
    }

    /// Check if this represents the "latest" version
    pub fn is_latest(&self) -> bool {
        self.0 == Self::LATEST
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_latest() {
            write!(f, "latest")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

impl From<u32> for SchemaVersion {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

/// Schema metadata for organization and discovery
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaMetadata {
    /// Human-readable name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Description of the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Labels/tags for categorization (key-value pairs)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    /// Owner/team responsible for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Creation timestamp (RFC 3339)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// Last modified timestamp (RFC 3339)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_at: Option<String>,
    /// Schema context for multi-tenant isolation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

impl SchemaMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Set the context
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

/// A registered schema with its metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Unique schema ID (global)
    pub id: SchemaId,
    /// Schema type/format
    pub schema_type: SchemaType,
    /// The schema definition (JSON string)
    pub schema: String,
    /// MD5 fingerprint for deduplication
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// Schema references (for nested schemas)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    /// Schema metadata (name, description, labels)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SchemaMetadata>,
}

impl Schema {
    pub fn new(id: SchemaId, schema_type: SchemaType, schema: String) -> Self {
        Self {
            id,
            schema_type,
            schema,
            fingerprint: None,
            references: Vec::new(),
            metadata: None,
        }
    }

    pub fn with_fingerprint(mut self, fingerprint: String) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }

    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }

    pub fn with_metadata(mut self, metadata: SchemaMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// Reference to another schema (for composition)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaReference {
    /// Reference name (used in the schema)
    pub name: String,
    /// Subject containing the referenced schema
    pub subject: String,
    /// Version of the referenced schema
    pub version: u32,
}

/// A subject version combines subject, version, and schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectVersion {
    /// Subject name
    pub subject: Subject,
    /// Version number
    pub version: SchemaVersion,
    /// Schema ID
    pub id: SchemaId,
    /// Schema type
    pub schema_type: SchemaType,
    /// The schema definition
    pub schema: String,
    /// Version state (enabled/deprecated/disabled)
    #[serde(default)]
    pub state: VersionState,
}

impl SubjectVersion {
    pub fn new(
        subject: Subject,
        version: SchemaVersion,
        id: SchemaId,
        schema_type: SchemaType,
        schema: String,
    ) -> Self {
        Self {
            subject,
            version,
            id,
            schema_type,
            schema,
            state: VersionState::Enabled,
        }
    }

    /// Create with a specific state
    pub fn with_state(mut self, state: VersionState) -> Self {
        self.state = state;
        self
    }

    /// Check if this version is usable
    pub fn is_usable(&self) -> bool {
        self.state.is_usable()
    }
}

/// Mode for schema registration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Mode {
    /// Read-write mode (default)
    #[default]
    Readwrite,
    /// Read-only mode
    Readonly,
    /// Import mode (for migrations)
    Import,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Readwrite => write!(f, "READWRITE"),
            Mode::Readonly => write!(f, "READONLY"),
            Mode::Import => write!(f, "IMPORT"),
        }
    }
}

/// Compatibility level for schema evolution
///
/// | Level | Description | Can Read |
/// |-------|-------------|----------|
/// | BACKWARD | New schema can read old data | Old → New ✓ |
/// | FORWARD | Old schema can read new data | New → Old ✓ |
/// | FULL | Both directions | Both ✓ |
/// | NONE | No checking | - |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum CompatibilityLevel {
    /// New schema can read data written by old schema (default)
    /// Safe for consumers to upgrade first
    #[default]
    Backward,

    /// New schema can read data written by old schema (transitive)
    BackwardTransitive,

    /// Old schema can read data written by new schema
    /// Safe for producers to upgrade first
    Forward,

    /// Old schema can read data written by new schema (transitive)
    ForwardTransitive,

    /// Both backward and forward compatible
    Full,

    /// Both backward and forward compatible (transitive)
    FullTransitive,

    /// No compatibility checking
    None,
}

impl CompatibilityLevel {
    /// Check if this level requires backward compatibility
    pub fn is_backward(&self) -> bool {
        matches!(
            self,
            Self::Backward | Self::BackwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Check if this level requires forward compatibility
    pub fn is_forward(&self) -> bool {
        matches!(
            self,
            Self::Forward | Self::ForwardTransitive | Self::Full | Self::FullTransitive
        )
    }

    /// Check if this level is transitive (checks all versions)
    pub fn is_transitive(&self) -> bool {
        matches!(
            self,
            Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive
        )
    }
}

impl std::fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
            Self::None => "NONE",
        };
        write!(f, "{}", s)
    }
}

impl std::str::FromStr for CompatibilityLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "BACKWARD" => Ok(Self::Backward),
            "BACKWARD_TRANSITIVE" => Ok(Self::BackwardTransitive),
            "FORWARD" => Ok(Self::Forward),
            "FORWARD_TRANSITIVE" => Ok(Self::ForwardTransitive),
            "FULL" => Ok(Self::Full),
            "FULL_TRANSITIVE" => Ok(Self::FullTransitive),
            "NONE" => Ok(Self::None),
            _ => Err(format!("Unknown compatibility level: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type_parse() {
        assert_eq!("avro".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!("AVRO".parse::<SchemaType>().unwrap(), SchemaType::Avro);
        assert_eq!("json".parse::<SchemaType>().unwrap(), SchemaType::Json);
        assert_eq!(
            "protobuf".parse::<SchemaType>().unwrap(),
            SchemaType::Protobuf
        );
    }

    #[test]
    fn test_subject_naming() {
        let key_subject = Subject::key("users");
        assert_eq!(key_subject.as_str(), "users-key");

        let value_subject = Subject::value("users");
        assert_eq!(value_subject.as_str(), "users-value");
    }

    #[test]
    fn test_schema_id() {
        let id = SchemaId::new(42);
        assert_eq!(id.as_u32(), 42);
        assert_eq!(format!("{}", id), "42");
    }

    #[test]
    fn test_compatibility_level_parse() {
        assert_eq!(
            "backward".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::Backward
        );
        assert_eq!(
            "FULL_TRANSITIVE".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::FullTransitive
        );
        assert_eq!(
            "none".parse::<CompatibilityLevel>().unwrap(),
            CompatibilityLevel::None
        );
    }

    #[test]
    fn test_compatibility_level_methods() {
        assert!(CompatibilityLevel::Backward.is_backward());
        assert!(!CompatibilityLevel::Backward.is_forward());
        assert!(!CompatibilityLevel::Backward.is_transitive());

        assert!(CompatibilityLevel::Full.is_backward());
        assert!(CompatibilityLevel::Full.is_forward());
        assert!(!CompatibilityLevel::Full.is_transitive());

        assert!(CompatibilityLevel::FullTransitive.is_transitive());
    }

    // ========================================================================
    // Schema Context Tests
    // ========================================================================

    #[test]
    fn test_schema_context_default() {
        let ctx = SchemaContext::default();
        assert!(ctx.is_default());
        assert_eq!(ctx.name(), "");
        assert!(ctx.is_active());
    }

    #[test]
    fn test_schema_context_new() {
        let ctx = SchemaContext::new("tenant-123");
        assert!(!ctx.is_default());
        assert_eq!(ctx.name(), "tenant-123");
        assert!(ctx.is_active());
    }

    #[test]
    fn test_schema_context_with_description() {
        let ctx =
            SchemaContext::new("production").with_description("Production environment context");

        assert_eq!(ctx.description(), Some("Production environment context"));
    }

    #[test]
    fn test_schema_context_inactive() {
        let ctx = SchemaContext::new("deprecated").with_active(false);

        assert!(!ctx.is_active());
    }

    #[test]
    fn test_schema_context_qualify_subject() {
        let default_ctx = SchemaContext::default();
        let subject = Subject::new("users-value");

        // Default context doesn't add prefix
        assert_eq!(default_ctx.qualify_subject(&subject), "users-value");

        // Named context adds Confluent-compatible prefix
        let tenant_ctx = SchemaContext::new("tenant-123");
        assert_eq!(
            tenant_ctx.qualify_subject(&subject),
            ":.tenant-123:users-value"
        );
    }

    #[test]
    fn test_schema_context_parse_qualified() {
        // Default context (no prefix)
        let (ctx, subject) = SchemaContext::parse_qualified("users-value");
        assert!(ctx.is_default());
        assert_eq!(subject.as_str(), "users-value");

        // Named context with prefix
        let (ctx, subject) = SchemaContext::parse_qualified(":.tenant-123:users-value");
        assert_eq!(ctx.name(), "tenant-123");
        assert_eq!(subject.as_str(), "users-value");
    }

    #[test]
    fn test_schema_context_display() {
        let default_ctx = SchemaContext::default();
        assert_eq!(format!("{}", default_ctx), "(default)");

        let named_ctx = SchemaContext::new("prod");
        assert_eq!(format!("{}", named_ctx), "prod");
    }

    // ========================================================================
    // Version State Tests
    // ========================================================================

    #[test]
    fn test_version_state_default() {
        let state = VersionState::default();
        assert_eq!(state, VersionState::Enabled);
    }

    #[test]
    fn test_version_state_usable() {
        assert!(VersionState::Enabled.is_usable());
        assert!(VersionState::Deprecated.is_usable());
        assert!(!VersionState::Disabled.is_usable());
    }

    #[test]
    fn test_version_state_requires_warning() {
        assert!(!VersionState::Enabled.requires_warning());
        assert!(VersionState::Deprecated.requires_warning());
        assert!(!VersionState::Disabled.requires_warning());
    }

    #[test]
    fn test_version_state_blocked() {
        assert!(!VersionState::Enabled.is_blocked());
        assert!(!VersionState::Deprecated.is_blocked());
        assert!(VersionState::Disabled.is_blocked());
    }

    #[test]
    fn test_version_state_transitions() {
        let enabled = VersionState::Enabled;

        // Enabled -> Deprecated
        let deprecated = enabled.deprecate();
        assert_eq!(deprecated, VersionState::Deprecated);

        // Deprecated -> Deprecated (no change)
        let still_deprecated = deprecated.deprecate();
        assert_eq!(still_deprecated, VersionState::Deprecated);

        // Deprecated -> Disabled
        let disabled = deprecated.disable();
        assert_eq!(disabled, VersionState::Disabled);

        // Disabled -> Enabled
        let re_enabled = disabled.enable();
        assert_eq!(re_enabled, VersionState::Enabled);
    }

    #[test]
    fn test_version_state_parse() {
        assert_eq!(
            "ENABLED".parse::<VersionState>().unwrap(),
            VersionState::Enabled
        );
        assert_eq!(
            "deprecated".parse::<VersionState>().unwrap(),
            VersionState::Deprecated
        );
        assert_eq!(
            "Disabled".parse::<VersionState>().unwrap(),
            VersionState::Disabled
        );
        assert!("invalid".parse::<VersionState>().is_err());
    }

    #[test]
    fn test_version_state_display() {
        assert_eq!(format!("{}", VersionState::Enabled), "ENABLED");
        assert_eq!(format!("{}", VersionState::Deprecated), "DEPRECATED");
        assert_eq!(format!("{}", VersionState::Disabled), "DISABLED");
    }

    // ========================================================================
    // Validation Rule Tests
    // ========================================================================

    #[test]
    fn test_validation_rule_new() {
        let rule = ValidationRule::new(
            "doc-required",
            ValidationRuleType::FieldRequired,
            r#"{"field": "doc"}"#,
        );

        assert_eq!(rule.name, "doc-required");
        assert_eq!(rule.rule_type, ValidationRuleType::FieldRequired);
        assert!(rule.active);
        assert_eq!(rule.level, ValidationLevel::Error);
    }

    #[test]
    fn test_validation_rule_builder() {
        let rule = ValidationRule::new(
            "naming-convention",
            ValidationRuleType::NamingConvention,
            r#"{"pattern": "^[A-Z]"}"#,
        )
        .with_level(ValidationLevel::Warning)
        .with_description("Enforce PascalCase naming")
        .with_schema_types(vec![SchemaType::Avro])
        .with_subject_patterns(vec![".*-value$".to_string()]);

        assert_eq!(rule.level, ValidationLevel::Warning);
        assert_eq!(
            rule.description,
            Some("Enforce PascalCase naming".to_string())
        );
        assert_eq!(rule.applies_to, vec![SchemaType::Avro]);
        assert_eq!(rule.subject_patterns, vec![".*-value$".to_string()]);
    }

    #[test]
    fn test_validation_rule_applies() {
        let rule = ValidationRule::new("test", ValidationRuleType::JsonSchema, "{}")
            .with_schema_types(vec![SchemaType::Avro])
            .with_subject_patterns(vec!["users-.*".to_string()]);

        // Matches both type and pattern
        assert!(rule.applies(SchemaType::Avro, "users-value"));

        // Wrong type
        assert!(!rule.applies(SchemaType::Json, "users-value"));

        // Wrong pattern
        assert!(!rule.applies(SchemaType::Avro, "orders-value"));
    }

    #[test]
    fn test_validation_rule_inactive() {
        let rule =
            ValidationRule::new("test", ValidationRuleType::MaxSize, "{}").with_active(false);

        assert!(!rule.applies(SchemaType::Avro, "any-subject"));
    }

    #[test]
    fn test_validation_rule_no_filters() {
        let rule = ValidationRule::new("test", ValidationRuleType::Regex, "{}");

        // Without filters, applies to everything
        assert!(rule.applies(SchemaType::Avro, "any-subject"));
        assert!(rule.applies(SchemaType::Json, "other-subject"));
        assert!(rule.applies(SchemaType::Protobuf, "third-subject"));
    }

    // ========================================================================
    // Validation Result Tests
    // ========================================================================

    #[test]
    fn test_validation_result_pass() {
        let result = ValidationResult::pass("test-rule");

        assert!(result.passed);
        assert_eq!(result.rule_name, "test-rule");
    }

    #[test]
    fn test_validation_result_fail() {
        let result = ValidationResult::fail(
            "naming-rule",
            ValidationLevel::Error,
            "Schema name must be PascalCase",
        )
        .with_details(r#"{"name": "invalidName"}"#);

        assert!(!result.passed);
        assert_eq!(result.level, ValidationLevel::Error);
        assert_eq!(result.message, "Schema name must be PascalCase");
        assert!(result.details.is_some());
    }

    // ========================================================================
    // Validation Report Tests
    // ========================================================================

    #[test]
    fn test_validation_report_empty() {
        let report = ValidationReport::new();

        assert!(report.is_valid());
        assert!(!report.has_warnings());
        assert!(report.results.is_empty());
    }

    #[test]
    fn test_validation_report_with_results() {
        let mut report = ValidationReport::new();

        report.add_result(ValidationResult::pass("rule1"));
        report.add_result(ValidationResult::fail(
            "rule2",
            ValidationLevel::Warning,
            "warning msg",
        ));
        report.add_result(ValidationResult::fail(
            "rule3",
            ValidationLevel::Error,
            "error msg",
        ));

        assert!(!report.is_valid()); // Has errors
        assert!(report.has_warnings());
        assert_eq!(report.summary.passed, 1);
        assert_eq!(report.summary.warnings, 1);
        assert_eq!(report.summary.errors, 1);
    }

    #[test]
    fn test_validation_report_error_messages() {
        let mut report = ValidationReport::new();

        report.add_result(ValidationResult::fail(
            "rule1",
            ValidationLevel::Error,
            "Error 1",
        ));
        report.add_result(ValidationResult::fail(
            "rule2",
            ValidationLevel::Warning,
            "Warning 1",
        ));
        report.add_result(ValidationResult::fail(
            "rule3",
            ValidationLevel::Error,
            "Error 2",
        ));

        let errors: Vec<String> = report.error_messages();
        assert_eq!(errors.len(), 2);
        assert!(errors.contains(&"Error 1".to_string()));
        assert!(errors.contains(&"Error 2".to_string()));

        let warnings: Vec<String> = report.warning_messages();
        assert_eq!(warnings.len(), 1);
        assert!(warnings.contains(&"Warning 1".to_string()));
    }

    // ========================================================================
    // Schema Metadata Tests
    // ========================================================================

    #[test]
    fn test_schema_metadata_builder() {
        let metadata = SchemaMetadata::new()
            .with_name("UserEvent")
            .with_description("Schema for user events")
            .with_owner("platform-team")
            .with_label("domain", "users")
            .with_label("version", "v2")
            .with_context("production");

        assert_eq!(metadata.name, Some("UserEvent".to_string()));
        assert_eq!(
            metadata.description,
            Some("Schema for user events".to_string())
        );
        assert_eq!(metadata.owner, Some("platform-team".to_string()));
        assert_eq!(metadata.labels.get("domain"), Some(&"users".to_string()));
        assert_eq!(metadata.labels.get("version"), Some(&"v2".to_string()));
        assert_eq!(metadata.context, Some("production".to_string()));
    }

    // ========================================================================
    // SubjectVersion Tests
    // ========================================================================

    #[test]
    fn test_subject_version_with_state() {
        let sv = SubjectVersion::new(
            Subject::new("users-value"),
            SchemaVersion::new(1),
            SchemaId::new(1),
            SchemaType::Avro,
            r#"{"type":"string"}"#.to_string(),
        )
        .with_state(VersionState::Deprecated);

        assert_eq!(sv.state, VersionState::Deprecated);
        assert!(sv.is_usable()); // Deprecated is still usable
    }

    #[test]
    fn test_subject_version_disabled() {
        let sv = SubjectVersion::new(
            Subject::new("users-value"),
            SchemaVersion::new(1),
            SchemaId::new(1),
            SchemaType::Avro,
            r#"{"type":"string"}"#.to_string(),
        )
        .with_state(VersionState::Disabled);

        assert!(!sv.is_usable()); // Disabled is not usable
    }
}
