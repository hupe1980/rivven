//! Prometheus metrics for Schema Registry
//!
//! Provides comprehensive observability metrics for monitoring schema registry
//! operations, performance, and health.
//!
//! # Metrics Overview
//!
//! ## Counters
//! - `schema_registry_schemas_registered_total` - Total schemas registered
//! - `schema_registry_schemas_lookups_total` - Total schema lookups
//! - `schema_registry_compatibility_checks_total` - Compatibility checks performed
//! - `schema_registry_validation_checks_total` - Validation rules executed
//! - `schema_registry_errors_total` - Total errors by type
//!
//! ## Gauges
//! - `schema_registry_schemas_count` - Current number of schemas
//! - `schema_registry_subjects_count` - Current number of subjects
//! - `schema_registry_versions_count` - Current number of versions
//! - `schema_registry_contexts_count` - Current number of contexts
//!
//! ## Histograms
//! - `schema_registry_request_duration_seconds` - Request latency
//! - `schema_registry_schema_size_bytes` - Schema sizes
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_schema::metrics::{RegistryMetrics, MetricsConfig};
//!
//! // Create metrics with custom prefix
//! let config = MetricsConfig::new()
//!     .with_prefix("myapp_schema");
//! let metrics = RegistryMetrics::new(config)?;
//!
//! // Record operations
//! metrics.record_registration("users-value", "AVRO");
//! metrics.record_lookup("users-value", true);
//!
//! // Get metrics for scraping
//! let output = metrics.render()?;
//! ```

use prometheus::{
    CounterVec, Encoder, Gauge, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

/// Metrics configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Prefix for all metric names (default: "schema_registry")
    pub prefix: String,
    /// Include context labels
    pub include_context: bool,
    /// Custom labels to add to all metrics
    pub custom_labels: Vec<(String, String)>,
    /// Histogram buckets for request duration
    pub duration_buckets: Vec<f64>,
    /// Histogram buckets for schema size
    pub size_buckets: Vec<f64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            prefix: "schema_registry".to_string(),
            include_context: true,
            custom_labels: Vec::new(),
            duration_buckets: vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
            size_buckets: vec![
                100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0,
            ],
        }
    }
}

impl MetricsConfig {
    /// Create a new metrics configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the metric prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Enable/disable context labels
    pub fn with_context(mut self, include: bool) -> Self {
        self.include_context = include;
        self
    }

    /// Add custom labels to all metrics
    pub fn with_custom_labels(mut self, labels: Vec<(String, String)>) -> Self {
        self.custom_labels = labels;
        self
    }

    /// Set duration histogram buckets
    pub fn with_duration_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.duration_buckets = buckets;
        self
    }

    /// Set size histogram buckets
    pub fn with_size_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.size_buckets = buckets;
        self
    }
}

/// Prometheus metrics for Schema Registry
pub struct RegistryMetrics {
    /// Prometheus registry
    registry: Registry,
    /// Configuration
    #[allow(dead_code)]
    config: MetricsConfig,

    // Counters
    schemas_registered: CounterVec,
    schemas_lookups: CounterVec,
    compatibility_checks: CounterVec,
    validation_checks: CounterVec,
    errors: CounterVec,
    version_state_changes: CounterVec,

    // Gauges
    schemas_count: Gauge,
    subjects_count: Gauge,
    versions_count: Gauge,
    contexts_count: Gauge,
    deprecated_versions_count: Gauge,
    disabled_versions_count: Gauge,

    // Histograms
    request_duration: HistogramVec,
    schema_size: HistogramVec,
}

impl RegistryMetrics {
    /// Create new registry metrics
    pub fn new(config: MetricsConfig) -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        Self::with_registry(config, registry)
    }

    /// Create metrics with a custom registry
    pub fn with_registry(
        config: MetricsConfig,
        registry: Registry,
    ) -> Result<Self, prometheus::Error> {
        let prefix = &config.prefix;

        // Counters
        let schemas_registered = CounterVec::new(
            Opts::new(
                format!("{}_schemas_registered_total", prefix),
                "Total number of schemas registered",
            ),
            &["subject", "schema_type", "context"],
        )?;

        let schemas_lookups = CounterVec::new(
            Opts::new(
                format!("{}_schemas_lookups_total", prefix),
                "Total number of schema lookups",
            ),
            &["subject", "lookup_type", "cache_hit"],
        )?;

        let compatibility_checks = CounterVec::new(
            Opts::new(
                format!("{}_compatibility_checks_total", prefix),
                "Total number of compatibility checks performed",
            ),
            &["subject", "level", "result"],
        )?;

        let validation_checks = CounterVec::new(
            Opts::new(
                format!("{}_validation_checks_total", prefix),
                "Total number of validation rule checks",
            ),
            &["rule_name", "rule_type", "result"],
        )?;

        let errors = CounterVec::new(
            Opts::new(
                format!("{}_errors_total", prefix),
                "Total number of errors by type",
            ),
            &["error_type", "operation"],
        )?;

        let version_state_changes = CounterVec::new(
            Opts::new(
                format!("{}_version_state_changes_total", prefix),
                "Total number of version state changes",
            ),
            &["subject", "from_state", "to_state"],
        )?;

        // Gauges
        let schemas_count = Gauge::new(
            format!("{}_schemas_count", prefix),
            "Current number of unique schemas",
        )?;

        let subjects_count = Gauge::new(
            format!("{}_subjects_count", prefix),
            "Current number of subjects",
        )?;

        let versions_count = Gauge::new(
            format!("{}_versions_count", prefix),
            "Current total number of schema versions",
        )?;

        let contexts_count = Gauge::new(
            format!("{}_contexts_count", prefix),
            "Current number of schema contexts",
        )?;

        let deprecated_versions_count = Gauge::new(
            format!("{}_deprecated_versions_count", prefix),
            "Current number of deprecated schema versions",
        )?;

        let disabled_versions_count = Gauge::new(
            format!("{}_disabled_versions_count", prefix),
            "Current number of disabled schema versions",
        )?;

        // Histograms
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                format!("{}_request_duration_seconds", prefix),
                "Request duration in seconds",
            )
            .buckets(config.duration_buckets.clone()),
            &["operation", "status"],
        )?;

        let schema_size = HistogramVec::new(
            HistogramOpts::new(
                format!("{}_schema_size_bytes", prefix),
                "Schema size in bytes",
            )
            .buckets(config.size_buckets.clone()),
            &["schema_type"],
        )?;

        // Register all metrics
        registry.register(Box::new(schemas_registered.clone()))?;
        registry.register(Box::new(schemas_lookups.clone()))?;
        registry.register(Box::new(compatibility_checks.clone()))?;
        registry.register(Box::new(validation_checks.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(version_state_changes.clone()))?;
        registry.register(Box::new(schemas_count.clone()))?;
        registry.register(Box::new(subjects_count.clone()))?;
        registry.register(Box::new(versions_count.clone()))?;
        registry.register(Box::new(contexts_count.clone()))?;
        registry.register(Box::new(deprecated_versions_count.clone()))?;
        registry.register(Box::new(disabled_versions_count.clone()))?;
        registry.register(Box::new(request_duration.clone()))?;
        registry.register(Box::new(schema_size.clone()))?;

        Ok(Self {
            registry,
            config,
            schemas_registered,
            schemas_lookups,
            compatibility_checks,
            validation_checks,
            errors,
            version_state_changes,
            schemas_count,
            subjects_count,
            versions_count,
            contexts_count,
            deprecated_versions_count,
            disabled_versions_count,
            request_duration,
            schema_size,
        })
    }

    // ========================================================================
    // Counter operations
    // ========================================================================

    /// Record a schema registration
    pub fn record_registration(&self, subject: &str, schema_type: &str, context: &str) {
        self.schemas_registered
            .with_label_values(&[subject, schema_type, context])
            .inc();
    }

    /// Record a schema lookup
    pub fn record_lookup(&self, subject: &str, lookup_type: &str, cache_hit: bool) {
        self.schemas_lookups
            .with_label_values(&[
                subject,
                lookup_type,
                if cache_hit { "true" } else { "false" },
            ])
            .inc();
    }

    /// Record a compatibility check
    pub fn record_compatibility_check(&self, subject: &str, level: &str, compatible: bool) {
        self.compatibility_checks
            .with_label_values(&[
                subject,
                level,
                if compatible {
                    "compatible"
                } else {
                    "incompatible"
                },
            ])
            .inc();
    }

    /// Record a validation rule check
    pub fn record_validation_check(&self, rule_name: &str, rule_type: &str, passed: bool) {
        self.validation_checks
            .with_label_values(&[
                rule_name,
                rule_type,
                if passed { "passed" } else { "failed" },
            ])
            .inc();
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str, operation: &str) {
        self.errors
            .with_label_values(&[error_type, operation])
            .inc();
    }

    /// Record a version state change
    pub fn record_state_change(&self, subject: &str, from_state: &str, to_state: &str) {
        self.version_state_changes
            .with_label_values(&[subject, from_state, to_state])
            .inc();
    }

    // ========================================================================
    // Gauge operations
    // ========================================================================

    /// Set the current schema count
    pub fn set_schemas_count(&self, count: u64) {
        self.schemas_count.set(count as f64);
    }

    /// Set the current subject count
    pub fn set_subjects_count(&self, count: u64) {
        self.subjects_count.set(count as f64);
    }

    /// Set the current version count
    pub fn set_versions_count(&self, count: u64) {
        self.versions_count.set(count as f64);
    }

    /// Set the current context count
    pub fn set_contexts_count(&self, count: u64) {
        self.contexts_count.set(count as f64);
    }

    /// Set the deprecated versions count
    pub fn set_deprecated_versions_count(&self, count: u64) {
        self.deprecated_versions_count.set(count as f64);
    }

    /// Set the disabled versions count
    pub fn set_disabled_versions_count(&self, count: u64) {
        self.disabled_versions_count.set(count as f64);
    }

    /// Increment schema count
    pub fn inc_schemas_count(&self) {
        self.schemas_count.inc();
    }

    /// Increment subject count
    pub fn inc_subjects_count(&self) {
        self.subjects_count.inc();
    }

    /// Increment version count
    pub fn inc_versions_count(&self) {
        self.versions_count.inc();
    }

    /// Decrement subject count
    pub fn dec_subjects_count(&self) {
        self.subjects_count.dec();
    }

    // ========================================================================
    // Histogram operations
    // ========================================================================

    /// Record request duration
    pub fn record_duration(&self, operation: &str, status: &str, duration_secs: f64) {
        self.request_duration
            .with_label_values(&[operation, status])
            .observe(duration_secs);
    }

    /// Record schema size
    pub fn record_schema_size(&self, schema_type: &str, size_bytes: usize) {
        self.schema_size
            .with_label_values(&[schema_type])
            .observe(size_bytes as f64);
    }

    /// Create a timer for measuring request duration
    pub fn start_timer(&self, operation: &str) -> RequestTimer<'_> {
        RequestTimer {
            metrics: self,
            operation: operation.to_string(),
            start: std::time::Instant::now(),
        }
    }

    // ========================================================================
    // Export
    // ========================================================================

    /// Render metrics in Prometheus text format
    pub fn render(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer).unwrap_or_default())
    }

    /// Get the underlying Prometheus registry
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

/// Timer for measuring request duration
pub struct RequestTimer<'a> {
    metrics: &'a RegistryMetrics,
    operation: String,
    start: std::time::Instant,
}

impl<'a> RequestTimer<'a> {
    /// Complete the timer with success status
    pub fn success(self) {
        self.complete("success");
    }

    /// Complete the timer with error status
    pub fn error(self) {
        self.complete("error");
    }

    /// Complete the timer with custom status
    pub fn complete(self, status: &str) {
        let duration = self.start.elapsed().as_secs_f64();
        self.metrics
            .record_duration(&self.operation, status, duration);
    }
}

impl<'a> Drop for RequestTimer<'a> {
    fn drop(&mut self) {
        // Don't record on drop - require explicit completion
    }
}

/// Shared metrics handle for thread-safe access
pub type SharedMetrics = Arc<RegistryMetrics>;

/// Create shared metrics
pub fn create_shared_metrics(config: MetricsConfig) -> Result<SharedMetrics, prometheus::Error> {
    Ok(Arc::new(RegistryMetrics::new(config)?))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert_eq!(config.prefix, "schema_registry");
        assert!(config.include_context);
        assert!(!config.duration_buckets.is_empty());
    }

    #[test]
    fn test_metrics_config_builder() {
        let config = MetricsConfig::new()
            .with_prefix("myapp")
            .with_context(false)
            .with_duration_buckets(vec![0.1, 0.5, 1.0]);

        assert_eq!(config.prefix, "myapp");
        assert!(!config.include_context);
        assert_eq!(config.duration_buckets, vec![0.1, 0.5, 1.0]);
    }

    #[test]
    fn test_metrics_creation() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config).unwrap();

        // Should be able to record operations
        metrics.record_registration("test-value", "AVRO", "default");
        metrics.record_lookup("test-value", "by_id", true);
        metrics.record_compatibility_check("test-value", "BACKWARD", true);
        metrics.record_validation_check("naming-rule", "NamingConvention", true);
        metrics.record_error("NOT_FOUND", "get_schema");
        metrics.record_state_change("test-value", "ENABLED", "DEPRECATED");
    }

    #[test]
    fn test_metrics_gauges() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config).unwrap();

        metrics.set_schemas_count(100);
        metrics.set_subjects_count(50);
        metrics.set_versions_count(200);
        metrics.set_contexts_count(3);
        metrics.set_deprecated_versions_count(10);
        metrics.set_disabled_versions_count(5);

        metrics.inc_schemas_count();
        metrics.inc_subjects_count();
        metrics.inc_versions_count();
        metrics.dec_subjects_count();
    }

    #[test]
    fn test_metrics_histograms() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config).unwrap();

        metrics.record_duration("register", "success", 0.05);
        metrics.record_schema_size("AVRO", 1500);
    }

    #[test]
    fn test_metrics_timer() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config).unwrap();

        let timer = metrics.start_timer("register");
        std::thread::sleep(std::time::Duration::from_millis(10));
        timer.success();

        let timer = metrics.start_timer("get_schema");
        timer.error();
    }

    #[test]
    fn test_metrics_render() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config).unwrap();

        metrics.record_registration("users-value", "AVRO", "default");
        metrics.set_schemas_count(10);

        let output = metrics.render().unwrap();
        assert!(output.contains("schema_registry_schemas_registered_total"));
        assert!(output.contains("schema_registry_schemas_count"));
    }

    #[test]
    fn test_shared_metrics() {
        let config = MetricsConfig::default();
        let shared = create_shared_metrics(config).unwrap();

        // Can clone and share across threads
        let shared2 = shared.clone();
        shared.record_registration("test", "AVRO", "");
        shared2.set_schemas_count(5);
    }

    #[test]
    fn test_metrics_with_custom_prefix() {
        let config = MetricsConfig::new().with_prefix("myservice_schema_registry");
        let metrics = RegistryMetrics::new(config).unwrap();

        metrics.record_registration("test", "AVRO", "");

        let output = metrics.render().unwrap();
        assert!(output.contains("myservice_schema_registry_schemas_registered_total"));
    }

    #[test]
    fn test_metrics_custom_registry() {
        let custom_registry = Registry::new();
        let config = MetricsConfig::default();

        let metrics = RegistryMetrics::with_registry(config, custom_registry).unwrap();
        metrics.record_registration("test", "AVRO", "");

        assert!(!metrics.render().unwrap().is_empty());
    }
}
