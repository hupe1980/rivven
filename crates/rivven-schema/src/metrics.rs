//! Metrics for Schema Registry
//!
//! Provides comprehensive observability metrics for monitoring schema registry
//! operations, performance, and health using the `metrics` crate facade.
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
//! let metrics = RegistryMetrics::new(config);
//!
//! // Record operations
//! metrics.record_registration("users-value", "AVRO", "default");
//! metrics.record_lookup("users-value", "by_id", true);
//!
//! // Metrics are automatically exported via metrics-exporter-prometheus
//! ```

use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Histogram buckets for request duration (informational - actual buckets set by exporter)
    pub duration_buckets: Vec<f64>,
    /// Histogram buckets for schema size (informational - actual buckets set by exporter)
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

    /// Set duration histogram buckets (informational)
    pub fn with_duration_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.duration_buckets = buckets;
        self
    }

    /// Set size histogram buckets (informational)
    pub fn with_size_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.size_buckets = buckets;
        self
    }
}

/// Metrics for Schema Registry using the `metrics` crate facade
///
/// This implementation uses the lightweight `metrics` crate which provides
/// a facade for recording metrics. The actual export format (Prometheus, etc.)
/// is handled by a separate exporter like `metrics-exporter-prometheus`.
pub struct RegistryMetrics {
    /// Configuration
    config: MetricsConfig,

    // Internal gauge state (metrics crate gauges need explicit value management)
    schemas_count: AtomicU64,
    subjects_count: AtomicU64,
    versions_count: AtomicU64,
    contexts_count: AtomicU64,
    deprecated_versions_count: AtomicU64,
    disabled_versions_count: AtomicU64,
}

impl RegistryMetrics {
    /// Create new registry metrics
    pub fn new(config: MetricsConfig) -> Self {
        let metrics = Self {
            config,
            schemas_count: AtomicU64::new(0),
            subjects_count: AtomicU64::new(0),
            versions_count: AtomicU64::new(0),
            contexts_count: AtomicU64::new(0),
            deprecated_versions_count: AtomicU64::new(0),
            disabled_versions_count: AtomicU64::new(0),
        };

        // Register metric descriptions
        metrics.describe_metrics();

        metrics
    }

    /// Register metric descriptions for documentation
    fn describe_metrics(&self) {
        let prefix = &self.config.prefix;

        // Counters
        describe_counter!(
            format!("{}_schemas_registered_total", prefix),
            "Total number of schemas registered"
        );
        describe_counter!(
            format!("{}_schemas_lookups_total", prefix),
            "Total number of schema lookups"
        );
        describe_counter!(
            format!("{}_compatibility_checks_total", prefix),
            "Total number of compatibility checks performed"
        );
        describe_counter!(
            format!("{}_validation_checks_total", prefix),
            "Total number of validation rule checks"
        );
        describe_counter!(
            format!("{}_errors_total", prefix),
            "Total number of errors by type"
        );
        describe_counter!(
            format!("{}_version_state_changes_total", prefix),
            "Total number of version state changes"
        );

        // Gauges
        describe_gauge!(
            format!("{}_schemas_count", prefix),
            "Current number of unique schemas"
        );
        describe_gauge!(
            format!("{}_subjects_count", prefix),
            "Current number of subjects"
        );
        describe_gauge!(
            format!("{}_versions_count", prefix),
            "Current total number of schema versions"
        );
        describe_gauge!(
            format!("{}_contexts_count", prefix),
            "Current number of schema contexts"
        );
        describe_gauge!(
            format!("{}_deprecated_versions_count", prefix),
            "Current number of deprecated schema versions"
        );
        describe_gauge!(
            format!("{}_disabled_versions_count", prefix),
            "Current number of disabled schema versions"
        );

        // Histograms
        describe_histogram!(
            format!("{}_request_duration_seconds", prefix),
            Unit::Seconds,
            "Request duration in seconds"
        );
        describe_histogram!(
            format!("{}_schema_size_bytes", prefix),
            Unit::Bytes,
            "Schema size in bytes"
        );
    }

    /// Get the metric prefix
    pub fn prefix(&self) -> &str {
        &self.config.prefix
    }

    // ========================================================================
    // Counter operations
    // ========================================================================

    /// Record a schema registration
    pub fn record_registration(&self, subject: &str, schema_type: &str, context: &str) {
        counter!(
            format!("{}_schemas_registered_total", self.config.prefix),
            "subject" => subject.to_string(),
            "schema_type" => schema_type.to_string(),
            "context" => context.to_string()
        )
        .increment(1);
    }

    /// Record a schema lookup
    pub fn record_lookup(&self, subject: &str, lookup_type: &str, cache_hit: bool) {
        counter!(
            format!("{}_schemas_lookups_total", self.config.prefix),
            "subject" => subject.to_string(),
            "lookup_type" => lookup_type.to_string(),
            "cache_hit" => if cache_hit { "true" } else { "false" }.to_string()
        )
        .increment(1);
    }

    /// Record a compatibility check
    pub fn record_compatibility_check(&self, subject: &str, level: &str, compatible: bool) {
        counter!(
            format!("{}_compatibility_checks_total", self.config.prefix),
            "subject" => subject.to_string(),
            "level" => level.to_string(),
            "result" => if compatible { "compatible" } else { "incompatible" }.to_string()
        )
        .increment(1);
    }

    /// Record a validation rule check
    pub fn record_validation_check(&self, rule_name: &str, rule_type: &str, passed: bool) {
        counter!(
            format!("{}_validation_checks_total", self.config.prefix),
            "rule_name" => rule_name.to_string(),
            "rule_type" => rule_type.to_string(),
            "result" => if passed { "passed" } else { "failed" }.to_string()
        )
        .increment(1);
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str, operation: &str) {
        counter!(
            format!("{}_errors_total", self.config.prefix),
            "error_type" => error_type.to_string(),
            "operation" => operation.to_string()
        )
        .increment(1);
    }

    /// Record a version state change
    pub fn record_state_change(&self, subject: &str, from_state: &str, to_state: &str) {
        counter!(
            format!("{}_version_state_changes_total", self.config.prefix),
            "subject" => subject.to_string(),
            "from_state" => from_state.to_string(),
            "to_state" => to_state.to_string()
        )
        .increment(1);
    }

    // ========================================================================
    // Gauge operations
    // ========================================================================

    /// Set the current schema count
    pub fn set_schemas_count(&self, count: u64) {
        self.schemas_count.store(count, Ordering::Relaxed);
        gauge!(format!("{}_schemas_count", self.config.prefix)).set(count as f64);
    }

    /// Set the current subject count
    pub fn set_subjects_count(&self, count: u64) {
        self.subjects_count.store(count, Ordering::Relaxed);
        gauge!(format!("{}_subjects_count", self.config.prefix)).set(count as f64);
    }

    /// Set the current version count
    pub fn set_versions_count(&self, count: u64) {
        self.versions_count.store(count, Ordering::Relaxed);
        gauge!(format!("{}_versions_count", self.config.prefix)).set(count as f64);
    }

    /// Set the current context count
    pub fn set_contexts_count(&self, count: u64) {
        self.contexts_count.store(count, Ordering::Relaxed);
        gauge!(format!("{}_contexts_count", self.config.prefix)).set(count as f64);
    }

    /// Set the deprecated versions count
    pub fn set_deprecated_versions_count(&self, count: u64) {
        self.deprecated_versions_count
            .store(count, Ordering::Relaxed);
        gauge!(format!("{}_deprecated_versions_count", self.config.prefix)).set(count as f64);
    }

    /// Set the disabled versions count
    pub fn set_disabled_versions_count(&self, count: u64) {
        self.disabled_versions_count.store(count, Ordering::Relaxed);
        gauge!(format!("{}_disabled_versions_count", self.config.prefix)).set(count as f64);
    }

    /// Increment schema count
    pub fn inc_schemas_count(&self) {
        let new_val = self.schemas_count.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!(format!("{}_schemas_count", self.config.prefix)).set(new_val as f64);
    }

    /// Increment subject count
    pub fn inc_subjects_count(&self) {
        let new_val = self.subjects_count.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!(format!("{}_subjects_count", self.config.prefix)).set(new_val as f64);
    }

    /// Increment version count
    pub fn inc_versions_count(&self) {
        let new_val = self.versions_count.fetch_add(1, Ordering::Relaxed) + 1;
        gauge!(format!("{}_versions_count", self.config.prefix)).set(new_val as f64);
    }

    /// Decrement subject count
    pub fn dec_subjects_count(&self) {
        // Use CAS loop to prevent underflow wrapping
        loop {
            let current = self.subjects_count.load(Ordering::Relaxed);
            if current == 0 {
                return; // Already at zero, nothing to decrement
            }
            if self
                .subjects_count
                .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                gauge!(format!("{}_subjects_count", self.config.prefix)).set((current - 1) as f64);
                return;
            }
        }
    }

    /// Get current schemas count
    pub fn get_schemas_count(&self) -> u64 {
        self.schemas_count.load(Ordering::Relaxed)
    }

    /// Get current subjects count
    pub fn get_subjects_count(&self) -> u64 {
        self.subjects_count.load(Ordering::Relaxed)
    }

    /// Get current versions count
    pub fn get_versions_count(&self) -> u64 {
        self.versions_count.load(Ordering::Relaxed)
    }

    // ========================================================================
    // Histogram operations
    // ========================================================================

    /// Record request duration
    pub fn record_duration(&self, operation: &str, status: &str, duration_secs: f64) {
        histogram!(
            format!("{}_request_duration_seconds", self.config.prefix),
            "operation" => operation.to_string(),
            "status" => status.to_string()
        )
        .record(duration_secs);
    }

    /// Record schema size
    pub fn record_schema_size(&self, schema_type: &str, size_bytes: usize) {
        histogram!(
            format!("{}_schema_size_bytes", self.config.prefix),
            "schema_type" => schema_type.to_string()
        )
        .record(size_bytes as f64);
    }

    /// Create a timer for measuring request duration
    pub fn start_timer(&self, operation: &str) -> RequestTimer<'_> {
        RequestTimer {
            metrics: self,
            operation: operation.to_string(),
            start: std::time::Instant::now(),
        }
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

impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        // Don't record on drop - require explicit completion
    }
}

/// Shared metrics handle for thread-safe access
pub type SharedMetrics = Arc<RegistryMetrics>;

/// Create shared metrics
pub fn create_shared_metrics(config: MetricsConfig) -> SharedMetrics {
    Arc::new(RegistryMetrics::new(config))
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
        let metrics = RegistryMetrics::new(config);

        // Should be able to record operations without panic
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
        let metrics = RegistryMetrics::new(config);

        metrics.set_schemas_count(100);
        metrics.set_subjects_count(50);
        metrics.set_versions_count(200);
        metrics.set_contexts_count(3);
        metrics.set_deprecated_versions_count(10);
        metrics.set_disabled_versions_count(5);

        assert_eq!(metrics.get_schemas_count(), 100);
        assert_eq!(metrics.get_subjects_count(), 50);
        assert_eq!(metrics.get_versions_count(), 200);

        metrics.inc_schemas_count();
        metrics.inc_subjects_count();
        metrics.inc_versions_count();
        metrics.dec_subjects_count();

        assert_eq!(metrics.get_schemas_count(), 101);
        assert_eq!(metrics.get_subjects_count(), 50); // inc then dec
        assert_eq!(metrics.get_versions_count(), 201);
    }

    #[test]
    fn test_metrics_histograms() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config);

        metrics.record_duration("register", "success", 0.05);
        metrics.record_schema_size("AVRO", 1500);
    }

    #[test]
    fn test_metrics_timer() {
        let config = MetricsConfig::default();
        let metrics = RegistryMetrics::new(config);

        let timer = metrics.start_timer("register");
        std::thread::sleep(std::time::Duration::from_millis(10));
        timer.success();

        let timer = metrics.start_timer("get_schema");
        timer.error();
    }

    #[test]
    fn test_shared_metrics() {
        let config = MetricsConfig::default();
        let shared = create_shared_metrics(config);

        // Can clone and share across threads
        let shared2 = shared.clone();
        shared.record_registration("test", "AVRO", "");
        shared2.set_schemas_count(5);

        assert_eq!(shared.get_schemas_count(), 5);
        assert_eq!(shared2.get_schemas_count(), 5);
    }

    #[test]
    fn test_metrics_with_custom_prefix() {
        let config = MetricsConfig::new().with_prefix("myservice_schema_registry");
        let metrics = RegistryMetrics::new(config);

        assert_eq!(metrics.prefix(), "myservice_schema_registry");
        metrics.record_registration("test", "AVRO", "");
    }
}
