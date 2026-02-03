//! Schema Registry - main interface
//!
//! Provides a thread-safe, async schema registry with:
//! - Multiple schema formats (Avro, JSON Schema, Protobuf)
//! - Schema evolution with compatibility checking
//! - Content validation rules
//! - Schema contexts for multi-tenant support
//! - Version state management (enabled/deprecated/disabled)
//! - Optional Prometheus metrics

use crate::compatibility::{CompatibilityChecker, CompatibilityLevel, CompatibilityResult};
use crate::config::RegistryConfig;
use crate::error::{SchemaError, SchemaResult};
use crate::fingerprint::SchemaFingerprint;
use crate::storage::{create_storage, Storage};
use crate::types::{
    Schema, SchemaContext, SchemaId, SchemaReference, SchemaType, SchemaVersion, Subject,
    SubjectVersion, ValidationReport, VersionState,
};
use crate::validation::{ValidationEngine, ValidationEngineConfig};
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

#[cfg(feature = "metrics")]
use crate::metrics::{MetricsConfig, RegistryMetrics};

/// Schema Registry - main interface for schema management
///
/// Thread-safe and async-ready schema registry that supports:
/// - Multiple schema formats (Avro, JSON Schema, Protobuf)
/// - Schema evolution with compatibility checking
/// - Content validation rules
/// - Schema contexts for multi-tenant isolation
/// - Version state management
/// - Multiple storage backends
/// - Caching for performance
/// - Optional Prometheus metrics
pub struct SchemaRegistry {
    /// Storage backend
    storage: Storage,
    /// Default compatibility level
    default_compatibility: CompatibilityLevel,
    /// Per-subject compatibility overrides
    subject_compatibility: DashMap<String, CompatibilityLevel>,
    /// Schema normalization enabled
    normalize: bool,
    /// Schema cache by ID
    cache_by_id: DashMap<u32, Schema>,
    /// Schema cache by fingerprint
    cache_by_fingerprint: DashMap<String, u32>,
    /// Validation engine for content rules
    validation_engine: RwLock<ValidationEngine>,
    /// Schema contexts
    contexts: DashMap<String, SchemaContext>,
    /// Per-context subject mapping (context -> subjects)
    context_subjects: DashMap<String, Vec<String>>,
    /// Prometheus metrics (optional)
    #[cfg(feature = "metrics")]
    metrics: Option<std::sync::Arc<RegistryMetrics>>,
}

impl SchemaRegistry {
    /// Create a new schema registry with the given configuration
    pub async fn new(config: RegistryConfig) -> SchemaResult<Self> {
        let storage = create_storage(&config.storage).await?;

        // Initialize default context
        let contexts = DashMap::new();
        contexts.insert(String::new(), SchemaContext::default_context());

        Ok(Self {
            storage,
            default_compatibility: config.compatibility,
            subject_compatibility: DashMap::new(),
            normalize: config.normalize_schemas,
            cache_by_id: DashMap::new(),
            cache_by_fingerprint: DashMap::new(),
            validation_engine: RwLock::new(
                ValidationEngine::new(ValidationEngineConfig::default()),
            ),
            contexts,
            context_subjects: DashMap::new(),
            #[cfg(feature = "metrics")]
            metrics: None,
        })
    }

    /// Create registry with Prometheus metrics enabled
    #[cfg(feature = "metrics")]
    pub async fn with_metrics(
        config: RegistryConfig,
        metrics_config: MetricsConfig,
    ) -> SchemaResult<Self> {
        let mut registry = Self::new(config).await?;
        let metrics = RegistryMetrics::new(metrics_config)
            .map_err(|e| SchemaError::Config(format!("Failed to create metrics: {}", e)))?;
        registry.metrics = Some(std::sync::Arc::new(metrics));
        Ok(registry)
    }

    /// Get the metrics instance
    #[cfg(feature = "metrics")]
    pub fn metrics(&self) -> Option<&std::sync::Arc<RegistryMetrics>> {
        self.metrics.as_ref()
    }

    // ========================================================================
    // Validation Engine
    // ========================================================================

    /// Get a reference to the validation engine for configuration
    pub fn validation_engine(&self) -> &RwLock<ValidationEngine> {
        &self.validation_engine
    }

    /// Add a validation rule
    pub fn add_validation_rule(&self, rule: crate::types::ValidationRule) {
        self.validation_engine.write().add_rule(rule);
    }

    /// Validate a schema without registering it
    pub fn validate_schema(
        &self,
        schema_type: SchemaType,
        subject: &str,
        schema: &str,
    ) -> SchemaResult<ValidationReport> {
        self.validation_engine
            .read()
            .validate(schema_type, subject, schema)
    }

    // ========================================================================
    // Context Management
    // ========================================================================

    /// Create a new schema context
    pub fn create_context(&self, context: SchemaContext) -> SchemaResult<()> {
        let name = context.name().to_string();
        if self.contexts.contains_key(&name) {
            return Err(SchemaError::AlreadyExists(format!("Context '{}'", name)));
        }
        self.contexts.insert(name.clone(), context);
        self.context_subjects.insert(name.clone(), Vec::new());
        info!("Created schema context: {}", name);
        Ok(())
    }

    /// Get a context by name
    pub fn get_context(&self, name: &str) -> Option<SchemaContext> {
        self.contexts.get(name).map(|c| c.clone())
    }

    /// List all contexts
    pub fn list_contexts(&self) -> Vec<SchemaContext> {
        self.contexts.iter().map(|c| c.clone()).collect()
    }

    /// Delete a context (must be empty)
    pub fn delete_context(&self, name: &str) -> SchemaResult<()> {
        if name.is_empty() {
            return Err(SchemaError::InvalidInput(
                "Cannot delete default context".to_string(),
            ));
        }

        // Check if context has subjects
        if let Some(subjects) = self.context_subjects.get(name) {
            if !subjects.is_empty() {
                return Err(SchemaError::InvalidInput(format!(
                    "Context '{}' has {} subjects, delete them first",
                    name,
                    subjects.len()
                )));
            }
        }

        self.contexts.remove(name);
        self.context_subjects.remove(name);
        info!("Deleted schema context: {}", name);
        Ok(())
    }

    // ========================================================================
    // Version State Management
    // ========================================================================

    /// Set the state of a schema version
    pub async fn set_version_state(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
        state: VersionState,
    ) -> SchemaResult<()> {
        let subject = subject.into();

        // Record metric
        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            let old_state = self.storage.get_version_state(&subject, version).await?;
            metrics.record_state_change(
                subject.as_str(),
                &format!("{}", old_state),
                &format!("{}", state),
            );
        }

        self.storage
            .set_version_state(&subject, version, state)
            .await?;

        info!(
            subject = %subject,
            version = version.0,
            state = ?state,
            "Updated version state"
        );

        Ok(())
    }

    /// Get the state of a schema version
    pub async fn get_version_state(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<VersionState> {
        self.storage
            .get_version_state(&subject.into(), version)
            .await
    }

    /// Deprecate a schema version (marks as deprecated but still usable)
    pub async fn deprecate_version(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<()> {
        self.set_version_state(subject, version, VersionState::Deprecated)
            .await
    }

    /// Disable a schema version (blocks usage)
    pub async fn disable_version(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<()> {
        self.set_version_state(subject, version, VersionState::Disabled)
            .await
    }

    /// Enable a schema version
    pub async fn enable_version(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<()> {
        self.set_version_state(subject, version, VersionState::Enabled)
            .await
    }

    // ========================================================================
    // Core Schema Operations
    // ========================================================================

    /// Register a schema under a subject
    ///
    /// Returns the schema ID (either new or existing if schema already registered)
    pub async fn register(
        &self,
        subject: impl Into<Subject>,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<SchemaId> {
        self.register_with_references(subject, schema_type, schema, Vec::new())
            .await
    }

    /// Register a schema with references under a subject
    ///
    /// References allow schemas to reference types defined in other schemas,
    /// enabling complex schema composition (e.g., Avro records referencing
    /// other records, Protobuf imports, JSON Schema $refs).
    ///
    /// Returns the schema ID (either new or existing if schema already registered)
    pub async fn register_with_references(
        &self,
        subject: impl Into<Subject>,
        schema_type: SchemaType,
        schema: &str,
        references: Vec<SchemaReference>,
    ) -> SchemaResult<SchemaId> {
        let subject = subject.into();
        let context = self.get_subject_context(&subject);

        // Record metric for registration attempt
        #[cfg(feature = "metrics")]
        let timer = self.metrics.as_ref().map(|m| m.start_timer("register"));

        // Validate schema references exist
        self.validate_references(&references).await?;

        // Validate and normalize the schema
        validate_schema(schema_type, schema)?;
        let normalized_schema = if self.normalize {
            normalize_schema(schema_type, schema)?
        } else {
            schema.to_string()
        };

        // Run content validation rules
        let validation_report = self.validation_engine.read().validate(
            schema_type,
            subject.as_str(),
            &normalized_schema,
        )?;

        if !validation_report.is_valid() {
            #[cfg(feature = "metrics")]
            if let Some(t) = timer {
                t.error();
            }

            let errors = validation_report.error_messages().join("; ");
            return Err(SchemaError::Validation(errors));
        }

        // Log warnings if any
        if validation_report.has_warnings() {
            warn!(
                subject = %subject,
                warnings = ?validation_report.summary.warnings,
                "Schema registered with validation warnings"
            );
        }

        // Record schema size metric
        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_schema_size(&format!("{:?}", schema_type), normalized_schema.len());
        }

        // Compute fingerprint for deduplication
        let fingerprint = SchemaFingerprint::compute(&normalized_schema);
        let fp_hex = fingerprint.md5_hex();

        // Check if schema already exists (deduplication)
        if let Some(existing) = self.storage.get_schema_by_fingerprint(&fp_hex).await? {
            debug!(
                "Schema already exists with ID {} (fingerprint: {})",
                existing.id, fp_hex
            );

            // Register under this subject if not already
            let versions = self.storage.get_versions(&subject).await?;
            if versions.is_empty() {
                self.storage
                    .register_subject_version(&subject, existing.id)
                    .await?;

                // Track subject in context
                self.add_subject_to_context(&context, &subject);
            }

            #[cfg(feature = "metrics")]
            if let Some(t) = timer {
                t.success();
            }

            return Ok(existing.id);
        }

        // Check compatibility with existing versions
        let compatibility = self.get_subject_compatibility(&subject);
        if compatibility != CompatibilityLevel::None {
            self.check_compatibility_internal(&subject, schema_type, &normalized_schema)
                .await?;

            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_compatibility_check(
                    subject.as_str(),
                    &format!("{}", compatibility),
                    true,
                );
            }
        }

        // Allocate new schema ID
        let id = self.storage.next_schema_id().await?;

        // Create and store schema with references
        let schema_obj = Schema::new(id, schema_type, normalized_schema.clone())
            .with_fingerprint(fp_hex.clone())
            .with_references(references);

        self.storage.store_schema(schema_obj.clone()).await?;

        // Register under subject
        let version = self.storage.register_subject_version(&subject, id).await?;

        // Track subject in context
        self.add_subject_to_context(&context, &subject);

        // Update cache
        self.cache_by_id.insert(id.0, schema_obj);
        self.cache_by_fingerprint.insert(fp_hex, id.0);

        // Record metrics
        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_registration(
                subject.as_str(),
                &format!("{:?}", schema_type),
                context.name(),
            );
            metrics.inc_schemas_count();
            metrics.inc_versions_count();
            if let Some(t) = timer {
                t.success();
            }
        }

        info!(
            "Registered schema {} under subject {} (version {})",
            id, subject, version
        );

        Ok(id)
    }

    /// Get schema by ID
    pub async fn get_by_id(&self, id: SchemaId) -> SchemaResult<Schema> {
        // Check cache first
        if let Some(cached) = self.cache_by_id.get(&id.0) {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_lookup("", "by_id", true);
            }
            return Ok(cached.clone());
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_lookup("", "by_id", false);
        }

        // Fetch from storage
        let schema = self
            .storage
            .get_schema(id)
            .await?
            .ok_or_else(|| SchemaError::NotFound(format!("Schema ID {}", id)))?;

        // Update cache
        self.cache_by_id.insert(id.0, schema.clone());

        Ok(schema)
    }

    /// Get schema by subject and version
    pub async fn get_by_version(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<SubjectVersion> {
        let subject = subject.into();

        let sv = self
            .storage
            .get_subject_version(&subject, version)
            .await?
            .ok_or_else(|| SchemaError::VersionNotFound {
                subject: subject.to_string(),
                version: version.0,
            })?;

        // Check version state
        if sv.state.is_blocked() {
            return Err(SchemaError::VersionDisabled {
                subject: subject.to_string(),
                version: version.0,
            });
        }

        // Warn if deprecated
        if sv.state.requires_warning() {
            warn!(
                subject = %subject,
                version = version.0,
                "Using deprecated schema version"
            );
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_lookup(subject.as_str(), "by_version", false);
        }

        Ok(sv)
    }

    /// Get latest version of a subject
    pub async fn get_latest(&self, subject: impl Into<Subject>) -> SchemaResult<SubjectVersion> {
        let subject = subject.into();
        self.storage
            .get_latest_version(&subject)
            .await?
            .ok_or_else(|| SchemaError::SubjectNotFound(subject.to_string()))
    }

    /// List all versions for a subject
    pub async fn list_versions(&self, subject: impl Into<Subject>) -> SchemaResult<Vec<u32>> {
        self.storage.get_versions(&subject.into()).await
    }

    /// List all subjects
    pub async fn list_subjects(&self) -> SchemaResult<Vec<Subject>> {
        self.storage.list_subjects().await
    }

    /// List subjects in a specific context
    pub fn list_subjects_in_context(&self, context: &str) -> Vec<String> {
        self.context_subjects
            .get(context)
            .map(|s| s.clone())
            .unwrap_or_default()
    }

    /// Check if a schema is compatible with existing versions
    pub async fn check_compatibility(
        &self,
        subject: impl Into<Subject>,
        schema_type: SchemaType,
        schema: &str,
        version: Option<SchemaVersion>,
    ) -> SchemaResult<CompatibilityResult> {
        let subject = subject.into();
        let normalized = if self.normalize {
            normalize_schema(schema_type, schema)?
        } else {
            schema.to_string()
        };

        let compatibility = self.get_subject_compatibility(&subject);
        if compatibility == CompatibilityLevel::None {
            return Ok(CompatibilityResult::compatible());
        }

        let checker = CompatibilityChecker::new(compatibility);

        // Get existing schemas to check against
        let existing_schemas = self.get_existing_schemas(&subject, version).await?;
        let existing_refs: Vec<&str> = existing_schemas.iter().map(|s| s.as_str()).collect();

        let result = checker.check(schema_type, &normalized, &existing_refs)?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.record_compatibility_check(
                subject.as_str(),
                &format!("{}", compatibility),
                result.is_compatible,
            );
        }

        Ok(result)
    }

    /// Delete a subject
    pub async fn delete_subject(
        &self,
        subject: impl Into<Subject>,
        permanent: bool,
    ) -> SchemaResult<Vec<u32>> {
        let subject = subject.into();
        let result = self.storage.delete_subject(&subject, permanent).await?;

        // Remove from context tracking
        for mut ctx in self.context_subjects.iter_mut() {
            ctx.retain(|s| s != subject.as_str());
        }

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.dec_subjects_count();
        }

        Ok(result)
    }

    /// List soft-deleted subjects that can be recovered
    pub async fn list_deleted_subjects(&self) -> SchemaResult<Vec<Subject>> {
        self.storage.list_deleted_subjects().await
    }

    /// Undelete a soft-deleted subject
    ///
    /// Restores a previously soft-deleted subject and all its versions.
    /// Returns the list of restored version numbers.
    pub async fn undelete_subject(&self, subject: impl Into<Subject>) -> SchemaResult<Vec<u32>> {
        let subject = subject.into();
        let result = self.storage.undelete_subject(&subject).await?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = &self.metrics {
            metrics.inc_subjects_count();
        }

        Ok(result)
    }

    /// Delete a specific version
    pub async fn delete_version(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
        permanent: bool,
    ) -> SchemaResult<()> {
        self.storage
            .delete_version(&subject.into(), version, permanent)
            .await
    }

    /// Get compatibility level for a subject
    pub fn get_subject_compatibility(&self, subject: &Subject) -> CompatibilityLevel {
        self.subject_compatibility
            .get(&subject.0)
            .map(|c| *c)
            .unwrap_or(self.default_compatibility)
    }

    /// Set compatibility level for a subject
    pub fn set_subject_compatibility(
        &self,
        subject: impl Into<Subject>,
        level: CompatibilityLevel,
    ) {
        self.subject_compatibility.insert(subject.into().0, level);
    }

    /// Get global default compatibility level
    pub fn get_default_compatibility(&self) -> CompatibilityLevel {
        self.default_compatibility
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get registry statistics
    pub async fn stats(&self) -> RegistryStats {
        let subjects = self.storage.list_subjects().await.unwrap_or_default();
        let schema_count = self.cache_by_id.len();
        let context_count = self.contexts.len();

        RegistryStats {
            schema_count,
            subject_count: subjects.len(),
            context_count,
            cache_size: self.cache_by_id.len() + self.cache_by_fingerprint.len(),
        }
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    // ========================================================================
    // Schema References
    // ========================================================================

    /// Validate that all schema references exist
    ///
    /// Returns Ok(()) if all referenced schemas exist, or an error if any are missing.
    async fn validate_references(&self, references: &[SchemaReference]) -> SchemaResult<()> {
        for ref_schema in references {
            let subject = Subject::new(&ref_schema.subject);
            let version = SchemaVersion::new(ref_schema.version);

            // Check if the referenced subject/version exists
            if self
                .storage
                .get_subject_version(&subject, version)
                .await?
                .is_none()
            {
                return Err(SchemaError::ReferenceNotFound {
                    name: ref_schema.name.clone(),
                    subject: ref_schema.subject.clone(),
                    version: ref_schema.version,
                });
            }

            debug!(
                "Validated reference '{}' -> {}:{}",
                ref_schema.name, ref_schema.subject, ref_schema.version
            );
        }
        Ok(())
    }

    /// Get all schemas that reference a given subject/version
    ///
    /// Returns a list of schema IDs that have references to the specified subject and version.
    pub async fn get_schemas_referencing(
        &self,
        subject: impl Into<Subject>,
        version: SchemaVersion,
    ) -> SchemaResult<Vec<SchemaId>> {
        let subject = subject.into();
        let mut referencing_ids = Vec::new();

        // Get all subjects
        let subjects = self.storage.list_subjects().await?;

        // Check each subject's schemas for references
        for subj in subjects {
            let versions = self.storage.get_versions(&subj).await?;
            for v in versions {
                if let Some(sv) = self
                    .storage
                    .get_subject_version(&subj, SchemaVersion::new(v))
                    .await?
                {
                    // Get the full schema to check references
                    if let Ok(schema) = self.get_by_id(sv.id).await {
                        // Check if any reference points to our target
                        for ref_schema in &schema.references {
                            if ref_schema.subject == subject.as_str()
                                && ref_schema.version == version.0
                            {
                                referencing_ids.push(sv.id);
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(referencing_ids)
    }

    /// Get all references for a schema by ID
    pub async fn get_references(&self, id: SchemaId) -> SchemaResult<Vec<SchemaReference>> {
        let schema = self.get_by_id(id).await?;
        Ok(schema.references)
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    fn get_subject_context(&self, subject: &Subject) -> SchemaContext {
        // Parse context from qualified subject name
        let (ctx, _) = SchemaContext::parse_qualified(subject.as_str());
        ctx
    }

    fn add_subject_to_context(&self, context: &SchemaContext, subject: &Subject) {
        let ctx_name = if context.is_default() {
            String::new()
        } else {
            context.name().to_string()
        };
        // Parse the subject to get the unqualified name
        let (_, unqualified) = SchemaContext::parse_qualified(subject.as_str());
        self.context_subjects
            .entry(ctx_name)
            .or_default()
            .push(unqualified.as_str().to_string());
    }

    // Internal helper to check compatibility
    async fn check_compatibility_internal(
        &self,
        subject: &Subject,
        schema_type: SchemaType,
        schema: &str,
    ) -> SchemaResult<()> {
        let result = self
            .check_compatibility(subject.clone(), schema_type, schema, None)
            .await?;

        if !result.is_compatible {
            #[cfg(feature = "metrics")]
            if let Some(metrics) = &self.metrics {
                metrics.record_error("incompatible_schema", "register");
            }
            return Err(SchemaError::IncompatibleSchema(result.messages.join("; ")));
        }

        Ok(())
    }

    // Get existing schemas for compatibility checking
    async fn get_existing_schemas(
        &self,
        subject: &Subject,
        version: Option<SchemaVersion>,
    ) -> SchemaResult<Vec<String>> {
        let versions = self.storage.get_versions(subject).await?;

        if versions.is_empty() {
            return Ok(Vec::new());
        }

        let versions_to_check = match version {
            Some(v) => vec![v.0],
            None => versions, // Check all versions for transitive
        };

        let mut schemas = Vec::new();
        for v in versions_to_check {
            if let Some(sv) = self
                .storage
                .get_subject_version(subject, SchemaVersion::new(v))
                .await?
            {
                schemas.push(sv.schema);
            }
        }

        Ok(schemas)
    }
}

/// Registry statistics
#[derive(Debug, Clone)]
pub struct RegistryStats {
    pub schema_count: usize,
    pub subject_count: usize,
    pub context_count: usize,
    pub cache_size: usize,
}

/// Validate schema by parsing it with the appropriate parser
fn validate_schema(schema_type: SchemaType, schema: &str) -> SchemaResult<()> {
    match schema_type {
        #[cfg(feature = "avro")]
        SchemaType::Avro => {
            apache_avro::Schema::parse_str(schema)
                .map_err(|e| SchemaError::InvalidSchema(format!("Invalid Avro schema: {}", e)))?;
            Ok(())
        }
        #[cfg(not(feature = "avro"))]
        SchemaType::Avro => {
            // Without avro feature, just validate it's valid JSON
            serde_json::from_str::<serde_json::Value>(schema)
                .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON: {}", e)))?;
            Ok(())
        }
        #[cfg(feature = "json-schema")]
        SchemaType::Json => {
            // Validate it's valid JSON Schema by compiling it
            let value: serde_json::Value = serde_json::from_str(schema)
                .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON: {}", e)))?;
            jsonschema::JSONSchema::compile(&value)
                .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON Schema: {}", e)))?;
            Ok(())
        }
        #[cfg(not(feature = "json-schema"))]
        SchemaType::Json => {
            serde_json::from_str::<serde_json::Value>(schema)
                .map_err(|e| SchemaError::InvalidSchema(format!("Invalid JSON: {}", e)))?;
            Ok(())
        }
        SchemaType::Protobuf => {
            // Protobuf validation would require a proto parser
            // For now just ensure it's not empty
            if schema.trim().is_empty() {
                return Err(SchemaError::InvalidSchema(
                    "Empty protobuf schema".to_string(),
                ));
            }
            Ok(())
        }
    }
}

/// Normalize schema by parsing and re-serializing
fn normalize_schema(schema_type: SchemaType, schema: &str) -> SchemaResult<String> {
    match schema_type {
        SchemaType::Avro | SchemaType::Json => {
            // Parse as JSON and re-serialize
            let value: serde_json::Value = serde_json::from_str(schema)
                .map_err(|e| SchemaError::ParseError(format!("Invalid JSON: {}", e)))?;
            Ok(serde_json::to_string(&value)?)
        }
        SchemaType::Protobuf => {
            // Protobuf normalization would require a proto parser
            Ok(schema.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registry_basic() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Register a schema
        let schema = r#"{"type": "string"}"#;
        let id = registry
            .register("test-subject", SchemaType::Avro, schema)
            .await
            .unwrap();

        // Get by ID
        let retrieved = registry.get_by_id(id).await.unwrap();
        assert_eq!(retrieved.schema_type, SchemaType::Avro);
    }

    #[tokio::test]
    async fn test_registry_deduplication() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        let schema = r#"{"type": "string"}"#;

        // Register same schema twice
        let id1 = registry
            .register("subject1", SchemaType::Avro, schema)
            .await
            .unwrap();
        let id2 = registry
            .register("subject2", SchemaType::Avro, schema)
            .await
            .unwrap();

        // Should be the same ID (deduplicated)
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_registry_versions() {
        let config = RegistryConfig::memory().with_compatibility(CompatibilityLevel::None);
        let registry = SchemaRegistry::new(config).await.unwrap();

        let subject = "test-subject";

        // Register version 1
        registry
            .register(subject, SchemaType::Avro, r#"{"type": "string"}"#)
            .await
            .unwrap();

        // Register version 2 (different schema)
        registry
            .register(subject, SchemaType::Avro, r#"{"type": "int"}"#)
            .await
            .unwrap();

        // Check versions
        let versions = registry.list_versions(subject).await.unwrap();
        assert_eq!(versions.len(), 2);

        // Get latest
        let latest = registry.get_latest(subject).await.unwrap();
        assert_eq!(latest.version.0, 2);
    }

    #[tokio::test]
    async fn test_registry_list_subjects() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        registry
            .register("subject1", SchemaType::Avro, r#"{"type": "string"}"#)
            .await
            .unwrap();
        registry
            .register("subject2", SchemaType::Avro, r#"{"type": "int"}"#)
            .await
            .unwrap();

        let subjects = registry.list_subjects().await.unwrap();
        assert_eq!(subjects.len(), 2);
    }

    #[tokio::test]
    async fn test_registry_invalid_avro_schema() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Invalid Avro schema (not valid JSON)
        let result = registry
            .register("test", SchemaType::Avro, "not valid json")
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid"));

        // Invalid Avro schema (valid JSON but invalid Avro)
        let result = registry
            .register("test", SchemaType::Avro, r#"{"invalid": "avro"}"#)
            .await;
        assert!(result.is_err());
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn test_registry_invalid_json_schema() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Valid JSON but invalid JSON Schema
        let result = registry
            .register("test", SchemaType::Json, r#"{"type": "invalid_type"}"#)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid"));
    }

    #[tokio::test]
    async fn test_registry_contexts() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Default context should exist
        assert!(registry.get_context("").is_some());

        // Create new context
        let ctx = SchemaContext::new("tenant-1").with_description("Test tenant");
        registry.create_context(ctx).unwrap();

        // List contexts
        let contexts = registry.list_contexts();
        assert_eq!(contexts.len(), 2);

        // Delete context
        registry.delete_context("tenant-1").unwrap();
        assert_eq!(registry.list_contexts().len(), 1);
    }

    #[tokio::test]
    async fn test_registry_version_states() {
        let config = RegistryConfig::memory().with_compatibility(CompatibilityLevel::None);
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Register a schema
        registry
            .register("test", SchemaType::Avro, r#"{"type": "string"}"#)
            .await
            .unwrap();

        // Default state is Enabled
        let state = registry
            .get_version_state("test", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(state, VersionState::Enabled);

        // Deprecate
        registry
            .deprecate_version("test", SchemaVersion::new(1))
            .await
            .unwrap();
        let state = registry
            .get_version_state("test", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(state, VersionState::Deprecated);

        // Disable
        registry
            .disable_version("test", SchemaVersion::new(1))
            .await
            .unwrap();
        let state = registry
            .get_version_state("test", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(state, VersionState::Disabled);

        // Getting disabled version should fail
        let result = registry.get_by_version("test", SchemaVersion::new(1)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_registry_validation() {
        use crate::types::{ValidationRule, ValidationRuleType};

        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Add a validation rule - 50 bytes max
        registry.add_validation_rule(ValidationRule::new(
            "max-size",
            ValidationRuleType::MaxSize,
            r#"{"max_bytes": 50}"#,
        ));

        // Small schema should pass (14 bytes)
        let result = registry
            .register("test", SchemaType::Avro, r#"{"type":"int"}"#)
            .await;
        assert!(result.is_ok());

        // Large schema should fail (>50 bytes)
        let large_schema = r#"{"type": "string", "doc": "This is a very long documentation string that exceeds the limit"}"#;
        let result = registry
            .register("test2", SchemaType::Avro, large_schema)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_registry_stats() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        registry
            .register("subject1", SchemaType::Avro, r#"{"type": "string"}"#)
            .await
            .unwrap();

        let stats = registry.stats().await;
        assert_eq!(stats.subject_count, 1);
        assert!(stats.context_count >= 1);
    }

    #[tokio::test]
    async fn test_schema_references_basic() {
        use crate::types::SchemaReference;

        let config = RegistryConfig::memory().with_compatibility(CompatibilityLevel::None);
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Register base schema (Address record)
        let address_schema = r#"{"type": "record", "name": "Address", "fields": [{"name": "street", "type": "string"}, {"name": "city", "type": "string"}]}"#;
        let address_id = registry
            .register("address-value", SchemaType::Avro, address_schema)
            .await
            .unwrap();

        // Register user schema with reference to Address
        // Note: For references to work with Avro, the schema must be valid on its own
        // or we use JSON type which doesn't validate
        let user_schema = r#"{"type": "object", "properties": {"name": {"type": "string"}, "address": {"$ref": "Address"}}}"#;
        let refs = vec![SchemaReference {
            name: "Address".to_string(),
            subject: "address-value".to_string(),
            version: 1,
        }];
        let user_id = registry
            .register_with_references("user-value", SchemaType::Json, user_schema, refs)
            .await
            .unwrap();

        // Verify reference is stored
        let user_schema_obj = registry.get_by_id(user_id).await.unwrap();
        assert_eq!(user_schema_obj.references.len(), 1);
        assert_eq!(user_schema_obj.references[0].name, "Address");
        assert_eq!(user_schema_obj.references[0].subject, "address-value");
        assert_eq!(user_schema_obj.references[0].version, 1);

        // Verify get_schemas_referencing works
        let referencing = registry
            .get_schemas_referencing("address-value", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(referencing.len(), 1);
        assert_eq!(referencing[0], user_id);

        // Address schema should have no references
        let address_schema_obj = registry.get_by_id(address_id).await.unwrap();
        assert!(address_schema_obj.references.is_empty());
    }

    #[tokio::test]
    async fn test_schema_references_not_found() {
        use crate::types::SchemaReference;

        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Try to register schema with reference to non-existent subject
        let user_schema = r#"{"type": "record", "name": "User", "fields": [{"name": "address", "type": "Address"}]}"#;
        let refs = vec![SchemaReference {
            name: "Address".to_string(),
            subject: "non-existent-subject".to_string(),
            version: 1,
        }];

        let result = registry
            .register_with_references("user-value", SchemaType::Avro, user_schema, refs)
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::error::SchemaError::ReferenceNotFound {
                name,
                subject,
                version,
            } => {
                assert_eq!(name, "Address");
                assert_eq!(subject, "non-existent-subject");
                assert_eq!(version, 1);
            }
            err => panic!("Expected ReferenceNotFound, got {:?}", err),
        }
    }

    #[tokio::test]
    async fn test_schema_references_empty() {
        let config = RegistryConfig::memory();
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Register schema without references (empty vec)
        let schema = r#"{"type": "string"}"#;
        let id = registry
            .register_with_references("test", SchemaType::Avro, schema, Vec::new())
            .await
            .unwrap();

        // Should have no references
        let schema_obj = registry.get_by_id(id).await.unwrap();
        assert!(schema_obj.references.is_empty());

        // get_references should return empty vec
        let refs = registry.get_references(id).await.unwrap();
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn test_schema_references_multiple() {
        use crate::types::SchemaReference;

        let config = RegistryConfig::memory().with_compatibility(CompatibilityLevel::None);
        let registry = SchemaRegistry::new(config).await.unwrap();

        // Register two base schemas
        registry
            .register("type-a-value", SchemaType::Avro, r#"{"type": "string"}"#)
            .await
            .unwrap();
        registry
            .register("type-b-value", SchemaType::Avro, r#"{"type": "int"}"#)
            .await
            .unwrap();

        // Register schema with multiple references
        let composite_schema = r#"{"type": "record", "name": "Composite", "fields": []}"#;
        let refs = vec![
            SchemaReference {
                name: "TypeA".to_string(),
                subject: "type-a-value".to_string(),
                version: 1,
            },
            SchemaReference {
                name: "TypeB".to_string(),
                subject: "type-b-value".to_string(),
                version: 1,
            },
        ];

        let id = registry
            .register_with_references("composite-value", SchemaType::Avro, composite_schema, refs)
            .await
            .unwrap();

        // Verify both references stored
        let schema = registry.get_by_id(id).await.unwrap();
        assert_eq!(schema.references.len(), 2);

        // Both base schemas should be referenced
        let ref_a = registry
            .get_schemas_referencing("type-a-value", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(ref_a.len(), 1);

        let ref_b = registry
            .get_schemas_referencing("type-b-value", SchemaVersion::new(1))
            .await
            .unwrap();
        assert_eq!(ref_b.len(), 1);
    }
}
