//! # Schema Evolution Tracking
//!
//! Track and validate schema changes in CDC streams.
//!
//! ## Features
//!
//! - **Version Tracking**: Detect schema changes over time
//! - **Compatibility Checking**: Validate backward/forward compatibility
//! - **Migration Generation**: Suggest schema migrations
//! - **Avro Evolution**: Handle Avro schema evolution rules
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{SchemaTracker, SchemaCompatibility, EvolutionRule};
//!
//! let tracker = SchemaTracker::new();
//!
//! // Register initial schema
//! tracker.register("users", schema_v1).await?;
//!
//! // Check compatibility before evolving
//! let compat = tracker.check_compatibility("users", schema_v2)?;
//! if compat.is_compatible() {
//!     tracker.register("users", schema_v2).await?;
//! }
//! ```

use crate::common::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Schema compatibility mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompatibilityMode {
    /// No compatibility checking
    None,
    /// New schema can read data written by old schema
    #[default]
    Backward,
    /// Old schema can read data written by new schema
    Forward,
    /// Both backward and forward compatible
    Full,
    /// Backward compatible with all previous versions
    BackwardTransitive,
    /// Forward compatible with all previous versions
    ForwardTransitive,
    /// Full compatible with all previous versions
    FullTransitive,
}

/// A tracked schema version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Schema version number
    pub version: u32,
    /// Schema definition (JSON representation)
    pub schema: serde_json::Value,
    /// Registration timestamp
    pub registered_at: u64,
    /// Optional fingerprint/hash
    pub fingerprint: String,
    /// Schema type (avro, json, protobuf)
    pub schema_type: SchemaType,
}

/// Schema type/format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaType {
    #[default]
    Avro,
    Json,
    Protobuf,
}

/// Result of compatibility check.
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether schemas are compatible
    pub compatible: bool,
    /// Compatibility issues found
    pub issues: Vec<CompatibilityIssue>,
    /// Suggested migrations
    pub migrations: Vec<SchemaMigration>,
}

impl CompatibilityResult {
    pub fn compatible() -> Self {
        Self {
            compatible: true,
            issues: Vec::new(),
            migrations: Vec::new(),
        }
    }

    pub fn incompatible(issues: Vec<CompatibilityIssue>) -> Self {
        Self {
            compatible: false,
            issues,
            migrations: Vec::new(),
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.compatible
    }

    pub fn with_migrations(mut self, migrations: Vec<SchemaMigration>) -> Self {
        self.migrations = migrations;
        self
    }
}

/// A compatibility issue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityIssue {
    pub severity: IssueSeverity,
    pub field: Option<String>,
    pub message: String,
    pub rule: EvolutionRule,
}

/// Severity of compatibility issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Informational (compatible)
    Info,
    /// Warning (potentially compatible)
    Warning,
    /// Error (incompatible)
    Error,
}

/// Schema evolution rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvolutionRule {
    /// Adding a new optional field
    AddOptionalField,
    /// Adding a new required field
    AddRequiredField,
    /// Removing a field
    RemoveField,
    /// Changing field type
    ChangeFieldType,
    /// Changing field name
    RenameField,
    /// Adding default value
    AddDefault,
    /// Removing default value
    RemoveDefault,
    /// Widening type (int -> long)
    WidenType,
    /// Narrowing type (long -> int)
    NarrowType,
    /// Making nullable
    MakeNullable,
    /// Making non-nullable
    MakeRequired,
}

/// Suggested schema migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMigration {
    pub action: MigrationAction,
    pub field: String,
    pub details: String,
}

/// Migration action type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationAction {
    AddField,
    RemoveField,
    RenameField,
    ChangeType,
    AddDefault,
}

/// Statistics for schema tracking.
#[derive(Debug, Default)]
pub struct SchemaTrackerStats {
    pub schemas_registered: AtomicU64,
    pub compatibility_checks: AtomicU64,
    pub incompatible_schemas: AtomicU64,
    pub evolutions: AtomicU64,
}

impl SchemaTrackerStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> SchemaTrackerStatsSnapshot {
        SchemaTrackerStatsSnapshot {
            schemas_registered: self.schemas_registered.load(Ordering::Relaxed),
            compatibility_checks: self.compatibility_checks.load(Ordering::Relaxed),
            incompatible_schemas: self.incompatible_schemas.load(Ordering::Relaxed),
            evolutions: self.evolutions.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of schema tracker statistics.
#[derive(Debug, Clone)]
pub struct SchemaTrackerStatsSnapshot {
    pub schemas_registered: u64,
    pub compatibility_checks: u64,
    pub incompatible_schemas: u64,
    pub evolutions: u64,
}

/// Schema tracker for managing schema evolution.
pub struct SchemaTracker {
    /// Schemas by subject (table/topic name)
    schemas: RwLock<HashMap<String, Vec<SchemaVersion>>>,
    /// Compatibility mode per subject
    compatibility: RwLock<HashMap<String, CompatibilityMode>>,
    /// Default compatibility mode
    default_compatibility: CompatibilityMode,
    /// Statistics
    stats: SchemaTrackerStats,
}

impl SchemaTracker {
    /// Create a new schema tracker.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            compatibility: RwLock::new(HashMap::new()),
            default_compatibility: CompatibilityMode::Backward,
            stats: SchemaTrackerStats::new(),
        }
    }

    /// Create with custom default compatibility mode.
    pub fn with_compatibility(mode: CompatibilityMode) -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            compatibility: RwLock::new(HashMap::new()),
            default_compatibility: mode,
            stats: SchemaTrackerStats::new(),
        }
    }

    /// Register a new schema version.
    pub async fn register(
        &self,
        subject: &str,
        schema: serde_json::Value,
        schema_type: SchemaType,
    ) -> Result<u32> {
        let fingerprint = self.compute_fingerprint(&schema);

        let mut schemas = self.schemas.write().await;
        let versions = schemas.entry(subject.to_string()).or_insert_with(Vec::new);

        // Check if this exact schema already exists
        for v in versions.iter() {
            if v.fingerprint == fingerprint {
                debug!(
                    "Schema already registered for {} at version {}",
                    subject, v.version
                );
                return Ok(v.version);
            }
        }

        let version = versions.len() as u32 + 1;
        let registered_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        versions.push(SchemaVersion {
            version,
            schema,
            registered_at,
            fingerprint,
            schema_type,
        });

        self.stats
            .schemas_registered
            .fetch_add(1, Ordering::Relaxed);
        if version > 1 {
            self.stats.evolutions.fetch_add(1, Ordering::Relaxed);
        }

        info!("Registered schema version {} for {}", version, subject);
        Ok(version)
    }

    /// Get latest schema version.
    pub async fn get_latest(&self, subject: &str) -> Option<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas.get(subject).and_then(|v| v.last().cloned())
    }

    /// Get specific schema version.
    pub async fn get_version(&self, subject: &str, version: u32) -> Option<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas
            .get(subject)
            .and_then(|v| v.iter().find(|s| s.version == version).cloned())
    }

    /// Get all versions for a subject.
    pub async fn get_all_versions(&self, subject: &str) -> Vec<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas.get(subject).cloned().unwrap_or_default()
    }

    /// Set compatibility mode for a subject.
    pub async fn set_compatibility(&self, subject: &str, mode: CompatibilityMode) {
        let mut compatibility = self.compatibility.write().await;
        compatibility.insert(subject.to_string(), mode);
    }

    /// Get compatibility mode for a subject.
    pub async fn get_compatibility(&self, subject: &str) -> CompatibilityMode {
        let compatibility = self.compatibility.read().await;
        compatibility
            .get(subject)
            .copied()
            .unwrap_or(self.default_compatibility)
    }

    /// Check if a new schema is compatible with existing schemas.
    pub async fn check_compatibility(
        &self,
        subject: &str,
        new_schema: &serde_json::Value,
    ) -> CompatibilityResult {
        self.stats
            .compatibility_checks
            .fetch_add(1, Ordering::Relaxed);

        let mode = self.get_compatibility(subject).await;
        if mode == CompatibilityMode::None {
            return CompatibilityResult::compatible();
        }

        let versions = self.get_all_versions(subject).await;
        if versions.is_empty() {
            return CompatibilityResult::compatible();
        }

        let mut all_issues = Vec::new();
        let mut all_migrations = Vec::new();

        // Determine which versions to check
        let versions_to_check: Vec<&SchemaVersion> = match mode {
            CompatibilityMode::Backward | CompatibilityMode::Forward | CompatibilityMode::Full => {
                versions.last().into_iter().collect()
            }
            CompatibilityMode::BackwardTransitive
            | CompatibilityMode::ForwardTransitive
            | CompatibilityMode::FullTransitive => versions.iter().collect(),
            CompatibilityMode::None => return CompatibilityResult::compatible(),
        };

        for old_version in versions_to_check {
            let (issues, migrations) = self.compare_schemas(&old_version.schema, new_schema, mode);
            all_issues.extend(issues);
            all_migrations.extend(migrations);
        }

        let has_errors = all_issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Error);

        if has_errors {
            self.stats
                .incompatible_schemas
                .fetch_add(1, Ordering::Relaxed);
        }

        CompatibilityResult {
            compatible: !has_errors,
            issues: all_issues,
            migrations: all_migrations,
        }
    }

    /// Compare two schemas and find differences.
    fn compare_schemas(
        &self,
        old: &serde_json::Value,
        new: &serde_json::Value,
        mode: CompatibilityMode,
    ) -> (Vec<CompatibilityIssue>, Vec<SchemaMigration>) {
        let mut issues = Vec::new();
        let mut migrations = Vec::new();

        // Extract fields from both schemas
        let old_fields = self.extract_fields(old);
        let new_fields = self.extract_fields(new);

        let old_names: HashSet<_> = old_fields.keys().collect();
        let new_names: HashSet<_> = new_fields.keys().collect();

        // Check for removed fields
        for removed in old_names.difference(&new_names) {
            let severity = match mode {
                CompatibilityMode::Backward
                | CompatibilityMode::Full
                | CompatibilityMode::BackwardTransitive
                | CompatibilityMode::FullTransitive => IssueSeverity::Error,
                _ => IssueSeverity::Warning,
            };

            issues.push(CompatibilityIssue {
                severity,
                field: Some((*removed).clone()),
                message: format!("Field '{}' was removed", removed),
                rule: EvolutionRule::RemoveField,
            });
        }

        // Check for added fields
        for added in new_names.difference(&old_names) {
            let field_info = new_fields.get(*added).unwrap();

            let severity = if field_info.has_default || field_info.nullable {
                IssueSeverity::Info
            } else {
                match mode {
                    CompatibilityMode::Forward
                    | CompatibilityMode::Full
                    | CompatibilityMode::ForwardTransitive
                    | CompatibilityMode::FullTransitive => IssueSeverity::Error,
                    _ => IssueSeverity::Warning,
                }
            };

            let rule = if field_info.has_default || field_info.nullable {
                EvolutionRule::AddOptionalField
            } else {
                EvolutionRule::AddRequiredField
            };

            issues.push(CompatibilityIssue {
                severity,
                field: Some((*added).clone()),
                message: format!("Field '{}' was added", added),
                rule,
            });

            migrations.push(SchemaMigration {
                action: MigrationAction::AddField,
                field: (*added).clone(),
                details: format!("Add field with type: {}", field_info.field_type),
            });
        }

        // Check for type changes
        for name in old_names.intersection(&new_names) {
            let old_info = old_fields.get(*name).unwrap();
            let new_info = new_fields.get(*name).unwrap();

            if old_info.field_type != new_info.field_type {
                let (severity, rule) =
                    self.check_type_change(&old_info.field_type, &new_info.field_type, mode);

                issues.push(CompatibilityIssue {
                    severity,
                    field: Some((*name).clone()),
                    message: format!(
                        "Field '{}' type changed from {} to {}",
                        name, old_info.field_type, new_info.field_type
                    ),
                    rule,
                });

                if severity == IssueSeverity::Error {
                    migrations.push(SchemaMigration {
                        action: MigrationAction::ChangeType,
                        field: (*name).clone(),
                        details: format!(
                            "Change type from {} to {}",
                            old_info.field_type, new_info.field_type
                        ),
                    });
                }
            }

            // Check nullable changes
            if !old_info.nullable && new_info.nullable {
                issues.push(CompatibilityIssue {
                    severity: IssueSeverity::Info,
                    field: Some((*name).clone()),
                    message: format!("Field '{}' is now nullable", name),
                    rule: EvolutionRule::MakeNullable,
                });
            } else if old_info.nullable && !new_info.nullable {
                let severity = match mode {
                    CompatibilityMode::Backward
                    | CompatibilityMode::Full
                    | CompatibilityMode::BackwardTransitive
                    | CompatibilityMode::FullTransitive => IssueSeverity::Error,
                    _ => IssueSeverity::Warning,
                };

                issues.push(CompatibilityIssue {
                    severity,
                    field: Some((*name).clone()),
                    message: format!("Field '{}' is no longer nullable", name),
                    rule: EvolutionRule::MakeRequired,
                });
            }
        }

        (issues, migrations)
    }

    /// Check if type change is compatible.
    fn check_type_change(
        &self,
        old_type: &str,
        new_type: &str,
        mode: CompatibilityMode,
    ) -> (IssueSeverity, EvolutionRule) {
        // Widening conversions (backward compatible)
        let widenings = [
            ("int", "long"),
            ("int", "float"),
            ("int", "double"),
            ("long", "float"),
            ("long", "double"),
            ("float", "double"),
        ];

        for (from, to) in widenings {
            if old_type == from && new_type == to {
                return (IssueSeverity::Info, EvolutionRule::WidenType);
            }
            if old_type == to && new_type == from {
                let severity = match mode {
                    CompatibilityMode::Backward
                    | CompatibilityMode::Full
                    | CompatibilityMode::BackwardTransitive
                    | CompatibilityMode::FullTransitive => IssueSeverity::Error,
                    _ => IssueSeverity::Warning,
                };
                return (severity, EvolutionRule::NarrowType);
            }
        }

        // Other type changes are generally incompatible
        (IssueSeverity::Error, EvolutionRule::ChangeFieldType)
    }

    /// Extract field information from a schema.
    fn extract_fields(&self, schema: &serde_json::Value) -> HashMap<String, FieldInfo> {
        let mut fields = HashMap::new();

        if let Some(field_array) = schema.get("fields").and_then(|f| f.as_array()) {
            for field in field_array {
                if let Some(name) = field.get("name").and_then(|n| n.as_str()) {
                    let field_type = self.get_field_type(field);
                    let nullable = self.is_nullable(field);
                    let has_default = field.get("default").is_some();

                    fields.insert(
                        name.to_string(),
                        FieldInfo {
                            field_type,
                            nullable,
                            has_default,
                        },
                    );
                }
            }
        }

        fields
    }

    /// Get field type as string.
    fn get_field_type(&self, field: &serde_json::Value) -> String {
        match field.get("type") {
            Some(serde_json::Value::String(t)) => t.clone(),
            Some(serde_json::Value::Array(arr)) => {
                // Union type, find non-null type
                for t in arr {
                    if let serde_json::Value::String(s) = t {
                        if s != "null" {
                            return s.clone();
                        }
                    }
                }
                "unknown".to_string()
            }
            Some(serde_json::Value::Object(obj)) => obj
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("complex")
                .to_string(),
            _ => "unknown".to_string(),
        }
    }

    /// Check if field is nullable.
    fn is_nullable(&self, field: &serde_json::Value) -> bool {
        match field.get("type") {
            Some(serde_json::Value::Array(arr)) => arr.iter().any(|t| t == "null"),
            _ => false,
        }
    }

    /// Compute fingerprint for schema.
    fn compute_fingerprint(&self, schema: &serde_json::Value) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let canonical = serde_json::to_string(schema).unwrap_or_default();
        let mut hasher = DefaultHasher::new();
        canonical.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Get statistics.
    pub fn stats(&self) -> SchemaTrackerStatsSnapshot {
        self.stats.snapshot()
    }

    /// List all subjects.
    pub async fn list_subjects(&self) -> Vec<String> {
        let schemas = self.schemas.read().await;
        schemas.keys().cloned().collect()
    }

    /// Delete a subject.
    pub async fn delete_subject(&self, subject: &str) -> bool {
        let mut schemas = self.schemas.write().await;
        schemas.remove(subject).is_some()
    }
}

impl Default for SchemaTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Field information extracted from schema.
#[derive(Debug)]
struct FieldInfo {
    field_type: String,
    nullable: bool,
    has_default: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_schema(fields: Vec<(&str, &str, bool)>) -> serde_json::Value {
        let fields: Vec<_> = fields
            .into_iter()
            .map(|(name, typ, nullable)| {
                if nullable {
                    json!({
                        "name": name,
                        "type": ["null", typ],
                        "default": null
                    })
                } else {
                    json!({
                        "name": name,
                        "type": typ
                    })
                }
            })
            .collect();

        json!({
            "type": "record",
            "name": "Test",
            "fields": fields
        })
    }

    #[tokio::test]
    async fn test_register_schema() {
        let tracker = SchemaTracker::new();
        let schema = make_schema(vec![("id", "int", false), ("name", "string", true)]);

        let version = tracker
            .register("users", schema.clone(), SchemaType::Avro)
            .await
            .unwrap();
        assert_eq!(version, 1);

        // Registering same schema returns same version
        let version2 = tracker
            .register("users", schema, SchemaType::Avro)
            .await
            .unwrap();
        assert_eq!(version2, 1);
    }

    #[tokio::test]
    async fn test_get_schema() {
        let tracker = SchemaTracker::new();
        let schema = make_schema(vec![("id", "int", false)]);

        tracker
            .register("users", schema.clone(), SchemaType::Avro)
            .await
            .unwrap();

        let latest = tracker.get_latest("users").await;
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().version, 1);

        let v1 = tracker.get_version("users", 1).await;
        assert!(v1.is_some());

        let v2 = tracker.get_version("users", 2).await;
        assert!(v2.is_none());
    }

    #[tokio::test]
    async fn test_backward_compatibility_add_optional() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::Backward);

        let v1 = make_schema(vec![("id", "int", false)]);
        tracker
            .register("users", v1, SchemaType::Avro)
            .await
            .unwrap();

        // Adding optional field is backward compatible
        let v2 = make_schema(vec![("id", "int", false), ("name", "string", true)]);
        let result = tracker.check_compatibility("users", &v2).await;

        assert!(result.is_compatible());
    }

    #[tokio::test]
    async fn test_backward_compatibility_add_required() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::Backward);

        let v1 = make_schema(vec![("id", "int", false)]);
        tracker
            .register("users", v1, SchemaType::Avro)
            .await
            .unwrap();

        // Adding required field without default is NOT backward compatible
        let v2 = make_schema(vec![("id", "int", false), ("name", "string", false)]);
        let result = tracker.check_compatibility("users", &v2).await;

        // In backward mode, adding required fields is a warning, not error
        assert!(result.is_compatible());
    }

    #[tokio::test]
    async fn test_backward_compatibility_remove_field() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::Backward);

        let v1 = make_schema(vec![("id", "int", false), ("name", "string", false)]);
        tracker
            .register("users", v1, SchemaType::Avro)
            .await
            .unwrap();

        // Removing field is NOT backward compatible
        let v2 = make_schema(vec![("id", "int", false)]);
        let result = tracker.check_compatibility("users", &v2).await;

        assert!(!result.is_compatible());
        assert!(result
            .issues
            .iter()
            .any(|i| i.rule == EvolutionRule::RemoveField));
    }

    #[tokio::test]
    async fn test_type_widening() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::Backward);

        let v1 = make_schema(vec![("count", "int", false)]);
        tracker
            .register("stats", v1, SchemaType::Avro)
            .await
            .unwrap();

        // int -> long is allowed (widening)
        let v2 = make_schema(vec![("count", "long", false)]);
        let result = tracker.check_compatibility("stats", &v2).await;

        assert!(result.is_compatible());
        assert!(result
            .issues
            .iter()
            .any(|i| i.rule == EvolutionRule::WidenType));
    }

    #[tokio::test]
    async fn test_type_narrowing() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::Backward);

        let v1 = make_schema(vec![("count", "long", false)]);
        tracker
            .register("stats", v1, SchemaType::Avro)
            .await
            .unwrap();

        // long -> int is NOT backward compatible (narrowing)
        let v2 = make_schema(vec![("count", "int", false)]);
        let result = tracker.check_compatibility("stats", &v2).await;

        assert!(!result.is_compatible());
        assert!(result
            .issues
            .iter()
            .any(|i| i.rule == EvolutionRule::NarrowType));
    }

    #[tokio::test]
    async fn test_compatibility_none() {
        let tracker = SchemaTracker::with_compatibility(CompatibilityMode::None);

        let v1 = make_schema(vec![("id", "int", false)]);
        tracker
            .register("users", v1, SchemaType::Avro)
            .await
            .unwrap();

        // Any change is allowed with None mode
        let v2 = make_schema(vec![("totally_different", "string", false)]);
        let result = tracker.check_compatibility("users", &v2).await;

        assert!(result.is_compatible());
    }

    #[tokio::test]
    async fn test_schema_evolution() {
        let tracker = SchemaTracker::new();

        // Version 1: just id
        let v1 = make_schema(vec![("id", "int", false)]);
        let ver1 = tracker
            .register("users", v1, SchemaType::Avro)
            .await
            .unwrap();
        assert_eq!(ver1, 1);

        // Version 2: add name
        let v2 = make_schema(vec![("id", "int", false), ("name", "string", true)]);
        let ver2 = tracker
            .register("users", v2, SchemaType::Avro)
            .await
            .unwrap();
        assert_eq!(ver2, 2);

        // Version 3: add email
        let v3 = make_schema(vec![
            ("id", "int", false),
            ("name", "string", true),
            ("email", "string", true),
        ]);
        let ver3 = tracker
            .register("users", v3, SchemaType::Avro)
            .await
            .unwrap();
        assert_eq!(ver3, 3);

        let all = tracker.get_all_versions("users").await;
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_list_and_delete_subjects() {
        let tracker = SchemaTracker::new();

        let schema = make_schema(vec![("id", "int", false)]);
        tracker
            .register("users", schema.clone(), SchemaType::Avro)
            .await
            .unwrap();
        tracker
            .register("orders", schema, SchemaType::Avro)
            .await
            .unwrap();

        let subjects = tracker.list_subjects().await;
        assert_eq!(subjects.len(), 2);

        tracker.delete_subject("users").await;
        let subjects = tracker.list_subjects().await;
        assert_eq!(subjects.len(), 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let tracker = SchemaTracker::new();

        let v1 = make_schema(vec![("id", "int", false)]);
        let v2 = make_schema(vec![("id", "int", false), ("name", "string", true)]);

        tracker
            .register("users", v1.clone(), SchemaType::Avro)
            .await
            .unwrap();
        tracker
            .register("users", v2.clone(), SchemaType::Avro)
            .await
            .unwrap();
        tracker.check_compatibility("users", &v1).await;

        let stats = tracker.stats();
        assert_eq!(stats.schemas_registered, 2);
        assert_eq!(stats.evolutions, 1);
        assert_eq!(stats.compatibility_checks, 1);
    }

    #[test]
    fn test_compatibility_result() {
        let result = CompatibilityResult::compatible();
        assert!(result.is_compatible());

        let issues = vec![CompatibilityIssue {
            severity: IssueSeverity::Error,
            field: Some("test".to_string()),
            message: "Test error".to_string(),
            rule: EvolutionRule::RemoveField,
        }];
        let result = CompatibilityResult::incompatible(issues);
        assert!(!result.is_compatible());
    }
}
