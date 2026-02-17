//! Custom Resource Definitions for Rivven Kubernetes Operator
//!
//! This module defines the `RivvenCluster` CRD that represents a Rivven
//! distributed streaming cluster in Kubernetes.

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use validator::{Validate, ValidationError};

/// Regex for validating Kubernetes resource quantities (e.g., "10Gi", "100Mi")
static QUANTITY_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E)?$").unwrap());

/// Regex for validating Kubernetes names (RFC 1123 subdomain)
static NAME_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$").unwrap());

/// Validate a Kubernetes resource quantity string
fn validate_quantity(value: &str) -> Result<(), ValidationError> {
    if QUANTITY_REGEX.is_match(value) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_quantity")
            .with_message(format!("'{}' is not a valid Kubernetes quantity", value).into()))
    }
}

/// Validate a container image reference
fn validate_image(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Ok(()); // Empty is allowed (uses default)
    }
    if value.len() > 255 {
        return Err(ValidationError::new("image_too_long")
            .with_message("image reference exceeds 255 characters".into()));
    }
    // Basic format check - not overly strict to allow various registries
    if value.contains("..") || value.starts_with('/') || value.starts_with('-') {
        return Err(ValidationError::new("invalid_image")
            .with_message(format!("'{}' is not a valid container image", value).into()));
    }
    Ok(())
}

/// Validate a Kubernetes name (RFC 1123 subdomain)
fn validate_k8s_name(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Ok(()); // Empty is allowed for optional fields
    }
    if value.len() > 63 {
        return Err(
            ValidationError::new("name_too_long").with_message("name exceeds 63 characters".into())
        );
    }
    if !NAME_REGEX.is_match(value) {
        return Err(ValidationError::new("invalid_name").with_message(
            format!("'{}' is not a valid Kubernetes name (RFC 1123)", value).into(),
        ));
    }
    Ok(())
}

/// Validate environment variable name (POSIX)
fn validate_env_vars(vars: &[k8s_openapi::api::core::v1::EnvVar]) -> Result<(), ValidationError> {
    // Limit number of env vars to prevent resource exhaustion
    const MAX_ENV_VARS: usize = 100;
    if vars.len() > MAX_ENV_VARS {
        return Err(ValidationError::new("too_many_env_vars").with_message(
            format!("maximum {} environment variables allowed", MAX_ENV_VARS).into(),
        ));
    }
    for var in vars {
        // Validate env var name format
        if var.name.is_empty() || var.name.len() > 256 {
            return Err(ValidationError::new("invalid_env_name")
                .with_message("environment variable name must be 1-256 characters".into()));
        }
        // Check for dangerous env var names that could override security settings
        let forbidden_names = [
            "LD_PRELOAD",
            "LD_LIBRARY_PATH",
            "DYLD_INSERT_LIBRARIES",
            "DYLD_LIBRARY_PATH",
        ];
        let forbidden_prefixes = ["LD_AUDIT"];
        for name in forbidden_names {
            if var.name == name && var.value.is_some() {
                return Err(ValidationError::new("forbidden_env_var").with_message(
                    format!(
                        "environment variable '{}' is not allowed for security",
                        var.name
                    )
                    .into(),
                ));
            }
        }
        for prefix in forbidden_prefixes {
            if var.name.starts_with(prefix) && var.value.is_some() {
                return Err(ValidationError::new("forbidden_env_var").with_message(
                    format!(
                        "environment variable '{}' is not allowed for security",
                        var.name
                    )
                    .into(),
                ));
            }
        }
    }
    Ok(())
}

/// RivvenCluster custom resource definition
///
/// Represents a Rivven distributed event streaming cluster deployment.
/// The operator watches these resources and reconciles the actual cluster
/// state to match the desired specification.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[kube(
    group = "rivven.hupe1980.github.io",
    version = "v1alpha1",
    kind = "RivvenCluster",
    plural = "rivvenclusters",
    shortname = "rc",
    namespaced,
    status = "RivvenClusterStatus",
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready", "type":"integer", "jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RivvenClusterSpec {
    /// Number of broker replicas (1-100)
    #[serde(default = "default_replicas")]
    #[validate(range(min = 1, max = 100, message = "replicas must be between 1 and 100"))]
    pub replicas: i32,

    /// Rivven version to deploy (must match semver pattern)
    #[serde(default = "default_version")]
    #[validate(length(min = 1, max = 64, message = "version must be 1-64 characters"))]
    pub version: String,

    /// Container image (overrides version-based default)
    /// Must be a valid container image reference
    #[serde(default)]
    #[validate(custom(function = "validate_optional_image"))]
    pub image: Option<String>,

    /// Image pull policy (Always, IfNotPresent, Never)
    #[serde(default = "default_image_pull_policy")]
    #[validate(custom(function = "validate_pull_policy"))]
    pub image_pull_policy: String,

    /// Image pull secrets (max 10 secrets)
    #[serde(default)]
    #[validate(length(max = 10, message = "maximum 10 image pull secrets allowed"))]
    pub image_pull_secrets: Vec<String>,

    /// Storage configuration
    #[serde(default)]
    #[validate(nested)]
    pub storage: StorageSpec,

    /// Resource requirements (CPU, memory)
    #[serde(default)]
    #[schemars(skip)]
    pub resources: Option<ResourceRequirements>,

    /// Broker configuration parameters
    #[serde(default)]
    #[validate(nested)]
    pub config: BrokerConfig,

    /// TLS configuration
    #[serde(default)]
    #[validate(nested)]
    pub tls: TlsSpec,

    /// Metrics configuration
    #[serde(default)]
    #[validate(nested)]
    pub metrics: MetricsSpec,

    /// Pod affinity/anti-affinity rules
    #[serde(default)]
    #[schemars(skip)]
    pub affinity: Option<k8s_openapi::api::core::v1::Affinity>,

    /// Node selector for pod scheduling (max 20 selectors)
    #[serde(default)]
    #[validate(custom(function = "validate_node_selector"))]
    pub node_selector: BTreeMap<String, String>,

    /// Tolerations for pod scheduling
    #[serde(default)]
    #[schemars(skip)]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,

    /// Pod disruption budget configuration
    #[serde(default)]
    #[validate(nested)]
    pub pod_disruption_budget: PdbSpec,

    /// Service account name (must be valid K8s name)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub service_account: Option<String>,

    /// Additional pod annotations (max 50)
    #[serde(default)]
    #[validate(custom(function = "validate_annotations"))]
    pub pod_annotations: BTreeMap<String, String>,

    /// Additional pod labels (max 20)
    #[serde(default)]
    #[validate(custom(function = "validate_labels"))]
    pub pod_labels: BTreeMap<String, String>,

    /// Environment variables (validated for security)
    #[serde(default)]
    #[schemars(skip)]
    #[validate(custom(function = "validate_env_vars"))]
    pub env: Vec<k8s_openapi::api::core::v1::EnvVar>,

    /// Liveness probe configuration
    #[serde(default)]
    #[validate(nested)]
    pub liveness_probe: ProbeSpec,

    /// Readiness probe configuration
    #[serde(default)]
    #[validate(nested)]
    pub readiness_probe: ProbeSpec,

    /// Security context for pods
    #[serde(default)]
    #[schemars(skip)]
    pub security_context: Option<k8s_openapi::api::core::v1::PodSecurityContext>,

    /// Container security context
    #[serde(default)]
    #[schemars(skip)]
    pub container_security_context: Option<k8s_openapi::api::core::v1::SecurityContext>,
}

/// Validate optional image reference
fn validate_optional_image(image: &str) -> Result<(), ValidationError> {
    validate_image(image)
}

/// Validate image pull policy
fn validate_pull_policy(policy: &str) -> Result<(), ValidationError> {
    match policy {
        "Always" | "IfNotPresent" | "Never" => Ok(()),
        _ => Err(ValidationError::new("invalid_pull_policy")
            .with_message("imagePullPolicy must be Always, IfNotPresent, or Never".into())),
    }
}

/// Validate node selector map
fn validate_node_selector(selectors: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if selectors.len() > 20 {
        return Err(ValidationError::new("too_many_selectors")
            .with_message("maximum 20 node selectors allowed".into()));
    }
    for (key, value) in selectors {
        if key.len() > 253 || value.len() > 63 {
            return Err(ValidationError::new("selector_too_long")
                .with_message("selector key max 253 chars, value max 63 chars".into()));
        }
    }
    Ok(())
}

/// Validate optional Kubernetes name (for use with Option<String> fields)
fn validate_optional_k8s_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty() {
        return Ok(()); // Empty is allowed for optional fields
    }
    validate_k8s_name(name)
}

/// Validate annotations map
fn validate_annotations(annotations: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if annotations.len() > 50 {
        return Err(ValidationError::new("too_many_annotations")
            .with_message("maximum 50 annotations allowed".into()));
    }
    for (key, value) in annotations {
        // Annotation keys can be up to 253 chars (with optional prefix)
        if key.len() > 253 {
            return Err(ValidationError::new("annotation_key_too_long")
                .with_message(format!("annotation key '{}' exceeds 253 characters", key).into()));
        }
        // Annotation values can be up to 256KB
        if value.len() > 262144 {
            return Err(ValidationError::new("annotation_value_too_long")
                .with_message(format!("annotation '{}' value exceeds 256KB", key).into()));
        }
    }
    Ok(())
}

/// Validate labels map
fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if labels.len() > 20 {
        return Err(ValidationError::new("too_many_labels")
            .with_message("maximum 20 labels allowed".into()));
    }
    for (key, value) in labels {
        if key.len() > 253 || value.len() > 63 {
            return Err(ValidationError::new("label_too_long")
                .with_message("label key max 253 chars, value max 63 chars".into()));
        }
        // Labels must not override managed labels
        if key.starts_with("app.kubernetes.io/") {
            return Err(ValidationError::new("reserved_label").with_message(
                format!("label '{}' uses reserved prefix app.kubernetes.io/", key).into(),
            ));
        }
    }
    Ok(())
}

/// Storage specification for broker data
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage size (e.g., "100Gi") - must be valid Kubernetes quantity
    #[serde(default = "default_storage_size")]
    #[validate(custom(function = "validate_quantity"))]
    pub size: String,

    /// Storage class name (empty uses default)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub storage_class_name: Option<String>,

    /// Access modes for the PVC
    #[serde(default = "default_access_modes")]
    #[validate(length(min = 1, max = 3, message = "access modes must have 1-3 entries"))]
    #[validate(custom(function = "validate_access_modes"))]
    pub access_modes: Vec<String>,
}

/// Validate PVC access modes
fn validate_access_modes(modes: &[String]) -> Result<(), ValidationError> {
    let valid_modes = [
        "ReadWriteOnce",
        "ReadOnlyMany",
        "ReadWriteMany",
        "ReadWriteOncePod",
    ];
    for mode in modes {
        if !valid_modes.contains(&mode.as_str()) {
            return Err(ValidationError::new("invalid_access_mode")
                .with_message(format!("'{}' is not a valid access mode", mode).into()));
        }
    }
    Ok(())
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            size: default_storage_size(),
            storage_class_name: None,
            access_modes: default_access_modes(),
        }
    }
}

/// Broker configuration parameters
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    /// Default number of partitions for new topics (1-1000)
    #[serde(default = "default_partitions")]
    #[validate(range(min = 1, max = 1000, message = "partitions must be between 1 and 1000"))]
    pub default_partitions: i32,

    /// Default replication factor for new topics (1-10)
    #[serde(default = "default_replication_factor")]
    #[validate(range(
        min = 1,
        max = 10,
        message = "replication factor must be between 1 and 10"
    ))]
    pub default_replication_factor: i32,

    /// Log retention period in hours (1-8760, i.e., 1 hour to 1 year)
    #[serde(default = "default_log_retention_hours")]
    #[validate(range(
        min = 1,
        max = 8760,
        message = "retention hours must be between 1 and 8760"
    ))]
    pub log_retention_hours: i32,

    /// Log segment size in bytes (1MB to 10GB)
    #[serde(default = "default_log_segment_bytes")]
    #[validate(custom(function = "validate_segment_size"))]
    pub log_segment_bytes: i64,

    /// Maximum message size in bytes (1KB to 100MB)
    #[serde(default = "default_max_message_bytes")]
    #[validate(custom(function = "validate_message_size"))]
    pub max_message_bytes: i64,

    /// Enable auto topic creation
    #[serde(default = "default_true")]
    pub auto_create_topics: bool,

    /// Enable compression
    #[serde(default = "default_true")]
    pub compression_enabled: bool,

    /// Compression algorithm (lz4, zstd, none)
    #[serde(default = "default_compression")]
    #[validate(custom(function = "validate_compression_type"))]
    pub compression_type: String,

    /// Raft election timeout in milliseconds (100-60000)
    #[serde(default = "default_election_timeout")]
    #[validate(range(
        min = 100,
        max = 60000,
        message = "election timeout must be between 100ms and 60s"
    ))]
    pub raft_election_timeout_ms: i32,

    /// Raft heartbeat interval in milliseconds (10-10000)
    #[serde(default = "default_heartbeat_interval")]
    #[validate(range(
        min = 10,
        max = 10000,
        message = "heartbeat interval must be between 10ms and 10s"
    ))]
    pub raft_heartbeat_interval_ms: i32,

    /// Additional raw configuration overrides (max 50 entries)
    #[serde(default)]
    #[validate(custom(function = "validate_raw_config"))]
    pub raw: BTreeMap<String, String>,
}

/// Validate compression type
fn validate_compression_type(compression: &str) -> Result<(), ValidationError> {
    match compression {
        "lz4" | "zstd" | "none" | "snappy" | "gzip" => Ok(()),
        _ => Err(ValidationError::new("invalid_compression")
            .with_message("compression must be one of: lz4, zstd, none, snappy, gzip".into())),
    }
}

/// Validate log segment size (1MB to 10GB)
fn validate_segment_size(size: i64) -> Result<(), ValidationError> {
    const MIN_SEGMENT_SIZE: i64 = 1_048_576; // 1MB
    const MAX_SEGMENT_SIZE: i64 = 10_737_418_240; // 10GB
    if !(MIN_SEGMENT_SIZE..=MAX_SEGMENT_SIZE).contains(&size) {
        return Err(ValidationError::new("invalid_segment_size")
            .with_message("segment size must be between 1MB and 10GB".into()));
    }
    Ok(())
}

/// Validate max message size (1KB to 100MB)
fn validate_message_size(size: i64) -> Result<(), ValidationError> {
    const MIN_MESSAGE_SIZE: i64 = 1_024; // 1KB
    const MAX_MESSAGE_SIZE: i64 = 104_857_600; // 100MB
    if !(MIN_MESSAGE_SIZE..=MAX_MESSAGE_SIZE).contains(&size) {
        return Err(ValidationError::new("invalid_message_size")
            .with_message("max message size must be between 1KB and 100MB".into()));
    }
    Ok(())
}

/// Validate raw config map
fn validate_raw_config(config: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if config.len() > 50 {
        return Err(ValidationError::new("too_many_raw_configs")
            .with_message("maximum 50 raw configuration entries allowed".into()));
    }
    for (key, value) in config {
        if key.len() > 128 || value.len() > 4096 {
            return Err(ValidationError::new("raw_config_too_long")
                .with_message("raw config key max 128 chars, value max 4096 chars".into()));
        }
        // Prevent injection of dangerous config keys
        let forbidden_keys = ["command", "args", "image", "securityContext", "volumes"];
        if forbidden_keys.contains(&key.as_str()) {
            return Err(ValidationError::new("forbidden_raw_config")
                .with_message(format!("raw config key '{}' is not allowed", key).into()));
        }
    }
    Ok(())
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            default_partitions: default_partitions(),
            default_replication_factor: default_replication_factor(),
            log_retention_hours: default_log_retention_hours(),
            log_segment_bytes: default_log_segment_bytes(),
            max_message_bytes: default_max_message_bytes(),
            auto_create_topics: default_true(),
            compression_enabled: default_true(),
            compression_type: default_compression(),
            raft_election_timeout_ms: default_election_timeout(),
            raft_heartbeat_interval_ms: default_heartbeat_interval(),
            raw: BTreeMap::new(),
        }
    }
}

/// TLS configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Name of the secret containing TLS certificates (required if enabled)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub cert_secret_name: Option<String>,

    /// Enable mTLS (mutual TLS)
    #[serde(default)]
    pub mtls_enabled: bool,

    /// Name of the secret containing CA certificate for mTLS
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub ca_secret_name: Option<String>,
}

/// Metrics configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    /// Enable Prometheus metrics
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics port (1024-65535, must be unprivileged)
    #[serde(default = "default_metrics_port")]
    #[validate(range(
        min = 1024,
        max = 65535,
        message = "metrics port must be between 1024 and 65535"
    ))]
    pub port: i32,

    /// ServiceMonitor configuration for Prometheus Operator
    #[serde(default)]
    #[validate(nested)]
    pub service_monitor: ServiceMonitorSpec,
}

impl Default for MetricsSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            service_monitor: ServiceMonitorSpec::default(),
        }
    }
}

/// ServiceMonitor configuration for Prometheus Operator integration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMonitorSpec {
    /// Create a ServiceMonitor resource
    #[serde(default)]
    pub enabled: bool,

    /// Namespace for the ServiceMonitor (defaults to cluster namespace)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub namespace: Option<String>,

    /// Scrape interval (must match Prometheus duration format)
    #[serde(default = "default_scrape_interval")]
    #[validate(custom(function = "validate_duration"))]
    pub interval: String,

    /// Additional labels for the ServiceMonitor (max 10)
    #[serde(default)]
    #[validate(custom(function = "validate_service_monitor_labels"))]
    pub labels: BTreeMap<String, String>,
}

/// Validate Prometheus duration format (e.g., "30s", "1m", "5m30s")
fn validate_duration(duration: &str) -> Result<(), ValidationError> {
    static DURATION_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^([0-9]+[smh])+$").unwrap());
    if !DURATION_REGEX.is_match(duration) {
        return Err(ValidationError::new("invalid_duration").with_message(
            format!("'{}' is not a valid duration (e.g., 30s, 1m)", duration).into(),
        ));
    }
    Ok(())
}

/// Validate ServiceMonitor labels
fn validate_service_monitor_labels(
    labels: &BTreeMap<String, String>,
) -> Result<(), ValidationError> {
    if labels.len() > 10 {
        return Err(ValidationError::new("too_many_labels")
            .with_message("maximum 10 ServiceMonitor labels allowed".into()));
    }
    Ok(())
}

/// Pod Disruption Budget configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PdbSpec {
    /// Enable PDB creation
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Minimum available pods (mutually exclusive with maxUnavailable)
    /// Can be an integer or percentage (e.g., "50%")
    #[serde(default)]
    #[validate(custom(function = "validate_optional_int_or_percent"))]
    pub min_available: Option<String>,

    /// Maximum unavailable pods
    /// Can be an integer or percentage (e.g., "25%")
    #[serde(default = "default_max_unavailable")]
    #[validate(custom(function = "validate_optional_int_or_percent"))]
    pub max_unavailable: Option<String>,
}

/// Validate integer or percentage string (for Option<String> fields)
fn validate_optional_int_or_percent(value: &str) -> Result<(), ValidationError> {
    if value.is_empty() {
        return Ok(());
    }
    // Allow integers or percentages
    static INT_OR_PERCENT_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^([0-9]+|[0-9]+%)$").unwrap());
    if !INT_OR_PERCENT_REGEX.is_match(value) {
        return Err(ValidationError::new("invalid_int_or_percent").with_message(
            format!(
                "'{}' must be an integer or percentage (e.g., '1' or '25%')",
                value
            )
            .into(),
        ));
    }
    Ok(())
}

impl Default for PdbSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            min_available: None,
            max_unavailable: Some("1".to_string()),
        }
    }
}

/// Probe configuration for liveness/readiness
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ProbeSpec {
    /// Enable the probe
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Initial delay before first probe (0-3600 seconds)
    #[serde(default = "default_initial_delay")]
    #[validate(range(min = 0, max = 3600, message = "initial delay must be 0-3600 seconds"))]
    pub initial_delay_seconds: i32,

    /// Period between probes (1-300 seconds)
    #[serde(default = "default_period")]
    #[validate(range(min = 1, max = 300, message = "period must be 1-300 seconds"))]
    pub period_seconds: i32,

    /// Timeout for probe (1-60 seconds)
    #[serde(default = "default_timeout")]
    #[validate(range(min = 1, max = 60, message = "timeout must be 1-60 seconds"))]
    pub timeout_seconds: i32,

    /// Success threshold (1-10)
    #[serde(default = "default_one")]
    #[validate(range(min = 1, max = 10, message = "success threshold must be 1-10"))]
    pub success_threshold: i32,

    /// Failure threshold (1-30)
    #[serde(default = "default_three")]
    #[validate(range(min = 1, max = 30, message = "failure threshold must be 1-30"))]
    pub failure_threshold: i32,
}

impl Default for ProbeSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay_seconds: 30,
            period_seconds: 10,
            timeout_seconds: 5,
            success_threshold: 1,
            failure_threshold: 3,
        }
    }
}

/// Status of a RivvenCluster resource
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RivvenClusterStatus {
    /// Current phase of the cluster
    pub phase: ClusterPhase,

    /// Total number of replicas
    pub replicas: i32,

    /// Number of ready replicas
    pub ready_replicas: i32,

    /// Number of updated replicas
    pub updated_replicas: i32,

    /// Current observed generation
    pub observed_generation: i64,

    /// Conditions describing cluster state
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,

    /// Broker endpoints
    #[serde(default)]
    pub broker_endpoints: Vec<String>,

    /// Current leader broker (if known)
    pub leader: Option<String>,

    /// Last time the status was updated
    pub last_updated: Option<String>,

    /// Error message if any
    pub message: Option<String>,
}

/// Phase of the cluster lifecycle
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ClusterPhase {
    /// Cluster is being created
    #[default]
    Pending,
    /// Cluster is being provisioned
    Provisioning,
    /// Cluster is running and healthy
    Running,
    /// Cluster is updating/rolling
    Updating,
    /// Cluster is in degraded state
    Degraded,
    /// Cluster has failed
    Failed,
    /// Cluster is being deleted
    Terminating,
}

/// Condition describing an aspect of cluster state
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    /// Type of condition
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Reason for the condition
    pub reason: Option<String>,

    /// Human-readable message
    pub message: Option<String>,

    /// Last transition time
    pub last_transition_time: Option<String>,
}

// Default value functions
fn default_replicas() -> i32 {
    3
}

fn default_version() -> String {
    "0.0.1".to_string()
}

fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}

fn default_storage_size() -> String {
    "10Gi".to_string()
}

fn default_access_modes() -> Vec<String> {
    vec!["ReadWriteOnce".to_string()]
}

fn default_partitions() -> i32 {
    3
}

fn default_replication_factor() -> i32 {
    2
}

fn default_log_retention_hours() -> i32 {
    168 // 7 days
}

fn default_log_segment_bytes() -> i64 {
    1073741824 // 1 GiB
}

fn default_max_message_bytes() -> i64 {
    1048576 // 1 MiB
}

fn default_compression() -> String {
    "lz4".to_string()
}

fn default_election_timeout() -> i32 {
    1000
}

fn default_heartbeat_interval() -> i32 {
    100
}

fn default_metrics_port() -> i32 {
    9090
}

fn default_scrape_interval() -> String {
    "30s".to_string()
}

fn default_max_unavailable() -> Option<String> {
    Some("1".to_string())
}

fn default_initial_delay() -> i32 {
    30
}

fn default_period() -> i32 {
    10
}

fn default_timeout() -> i32 {
    5
}

fn default_one() -> i32 {
    1
}

fn default_three() -> i32 {
    3
}

fn default_true() -> bool {
    true
}

impl RivvenClusterSpec {
    /// Get the full container image including version
    pub fn get_image(&self) -> String {
        if let Some(ref image) = self.image {
            image.clone()
        } else {
            format!("ghcr.io/hupe1980/rivven:{}", self.version)
        }
    }

    /// Get labels for managed resources
    pub fn get_labels(&self, cluster_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app.kubernetes.io/name".to_string(), "rivven".to_string());
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            cluster_name.to_string(),
        );
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "broker".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/managed-by".to_string(),
            "rivven-operator".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/version".to_string(),
            self.version.clone(),
        );
        labels
    }

    /// Get selector labels for managed resources
    pub fn get_selector_labels(&self, cluster_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app.kubernetes.io/name".to_string(), "rivven".to_string());
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            cluster_name.to_string(),
        );
        labels
    }
}

// ============================================================================
// RivvenConnect CRD - Connector Framework for Rivven
// ============================================================================

/// RivvenConnect custom resource for managing connectors
///
/// This CRD allows declarative management of Rivven Connect pipelines,
/// including source connectors (CDC, HTTP, etc.) and sink connectors
/// (S3, stdout, HTTP webhooks, etc.).
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[kube(
    group = "rivven.hupe1980.github.io",
    version = "v1alpha1",
    kind = "RivvenConnect",
    plural = "rivvenconnects",
    shortname = "rcon",
    namespaced,
    status = "RivvenConnectStatus",
    printcolumn = r#"{"name":"Cluster","type":"string","jsonPath":".spec.clusterRef.name"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Sources","type":"integer","jsonPath":".status.sourcesRunning"}"#,
    printcolumn = r#"{"name":"Sinks","type":"integer","jsonPath":".status.sinksRunning"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RivvenConnectSpec {
    /// Reference to the RivvenCluster this connect instance connects to
    #[validate(nested)]
    pub cluster_ref: ClusterReference,

    /// Number of connect worker replicas (1-10)
    #[serde(default = "default_connect_replicas")]
    #[validate(range(min = 1, max = 10, message = "replicas must be between 1 and 10"))]
    pub replicas: i32,

    /// Connect image version
    #[serde(default = "default_version")]
    pub version: String,

    /// Custom container image (overrides version-based default)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_image"))]
    pub image: Option<String>,

    /// Image pull policy
    #[serde(default = "default_image_pull_policy")]
    #[validate(custom(function = "validate_pull_policy"))]
    pub image_pull_policy: String,

    /// Image pull secrets
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,

    /// Resource requests/limits (following k8s ResourceRequirements schema)
    #[serde(default)]
    pub resources: Option<serde_json::Value>,

    /// Global connect configuration
    #[serde(default)]
    #[validate(nested)]
    pub config: ConnectConfigSpec,

    /// Source connectors (read from external systems, publish to Rivven)
    #[serde(default)]
    #[validate(length(max = 50, message = "maximum 50 source connectors allowed"))]
    pub sources: Vec<SourceConnectorSpec>,

    /// Sink connectors (consume from Rivven, write to external systems)
    #[serde(default)]
    #[validate(length(max = 50, message = "maximum 50 sink connectors allowed"))]
    pub sinks: Vec<SinkConnectorSpec>,

    /// Global settings for all connectors
    #[serde(default)]
    #[validate(nested)]
    pub settings: GlobalConnectSettings,

    /// TLS configuration for broker connection
    #[serde(default)]
    #[validate(nested)]
    pub tls: ConnectTlsSpec,

    /// Pod annotations
    #[serde(default)]
    #[validate(custom(function = "validate_annotations"))]
    pub pod_annotations: BTreeMap<String, String>,

    /// Pod labels (cannot override app.kubernetes.io/* labels)
    #[serde(default)]
    #[validate(custom(function = "validate_labels"))]
    pub pod_labels: BTreeMap<String, String>,

    /// Environment variables for the container
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 environment variables allowed"))]
    pub env: Vec<k8s_openapi::api::core::v1::EnvVar>,

    /// Node selector for pod scheduling
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,

    /// Pod tolerations
    #[serde(default)]
    #[validate(length(max = 20, message = "maximum 20 tolerations allowed"))]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,

    /// Pod affinity rules
    #[serde(default)]
    pub affinity: Option<serde_json::Value>,

    /// Service account name
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub service_account: Option<String>,

    /// Pod security context
    #[serde(default)]
    pub security_context: Option<serde_json::Value>,

    /// Container security context
    #[serde(default)]
    pub container_security_context: Option<serde_json::Value>,
}

/// Reference to a RivvenCluster
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReference {
    /// Name of the RivvenCluster
    #[validate(length(min = 1, max = 63, message = "cluster name must be 1-63 characters"))]
    #[validate(custom(function = "validate_k8s_name"))]
    pub name: String,

    /// Namespace of the RivvenCluster (defaults to same namespace)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub namespace: Option<String>,
}

// ============================================================================
// RivvenTopic CRD - Declarative Topic Management
// ============================================================================

/// RivvenTopic custom resource for declarative topic management
///
/// This CRD allows users to define topics as Kubernetes resources,
/// enabling GitOps workflows for topic lifecycle management.
///
/// # Example
///
/// ```yaml
/// apiVersion: rivven.hupe1980.github.io/v1alpha1
/// kind: RivvenTopic
/// metadata:
///   name: orders-events
///   namespace: production
/// spec:
///   clusterRef:
///     name: my-rivven-cluster
///   partitions: 12
///   replicationFactor: 3
///   config:
///     retentionMs: 604800000   # 7 days
///     cleanupPolicy: delete
///     compressionType: lz4
///   acls:
///     - principal: "user:order-service"
///       operations: ["Read", "Write"]
///     - principal: "user:analytics"
///       operations: ["Read"]
/// ```
// Note: These types are defined for CRD generation and future controller use.
// They are intentionally not yet constructed - the controller will use them.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[kube(
    group = "rivven.hupe1980.github.io",
    version = "v1alpha1",
    kind = "RivvenTopic",
    plural = "rivventopics",
    shortname = "rt",
    namespaced,
    status = "RivvenTopicStatus",
    printcolumn = r#"{"name":"Cluster","type":"string","jsonPath":".spec.clusterRef.name"}"#,
    printcolumn = r#"{"name":"Partitions","type":"integer","jsonPath":".spec.partitions"}"#,
    printcolumn = r#"{"name":"Replication","type":"integer","jsonPath":".spec.replicationFactor"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RivvenTopicSpec {
    /// Reference to the RivvenCluster this topic belongs to
    #[validate(nested)]
    pub cluster_ref: ClusterReference,

    /// Number of partitions for the topic (1-10000)
    /// Cannot be decreased after creation
    #[serde(default = "default_rivven_topic_partitions")]
    #[validate(range(
        min = 1,
        max = 10000,
        message = "partitions must be between 1 and 10000"
    ))]
    pub partitions: i32,

    /// Replication factor for the topic (1-10)
    /// Must not exceed the number of brokers in the cluster
    #[serde(default = "default_rivven_topic_replication")]
    #[validate(range(
        min = 1,
        max = 10,
        message = "replication factor must be between 1 and 10"
    ))]
    pub replication_factor: i32,

    /// Topic configuration parameters
    #[serde(default)]
    #[validate(nested)]
    pub config: TopicConfig,

    /// Access control list entries for the topic
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 ACL entries allowed"))]
    #[validate(custom(function = "validate_topic_acls"))]
    pub acls: Vec<TopicAcl>,

    /// Whether to delete the topic from Rivven when the CRD is deleted
    /// Default: true (topic is deleted when CRD is removed)
    #[serde(default = "default_true")]
    pub delete_on_remove: bool,

    /// Labels to apply to the topic metadata
    #[serde(default)]
    #[validate(custom(function = "validate_labels"))]
    pub topic_labels: BTreeMap<String, String>,
}

fn default_rivven_topic_partitions() -> i32 {
    3
}

fn default_rivven_topic_replication() -> i32 {
    1
}

/// Topic configuration parameters
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfig {
    /// Retention time in milliseconds (1 hour to 10 years)
    /// Default: 604800000 (7 days)
    #[serde(default = "default_topic_retention_ms")]
    #[validate(range(
        min = 3600000,
        max = 315360000000_i64,
        message = "retention must be between 1 hour and 10 years"
    ))]
    pub retention_ms: i64,

    /// Retention size in bytes per partition (-1 for unlimited)
    /// Default: -1 (unlimited)
    #[serde(default = "default_topic_retention_bytes")]
    #[validate(custom(function = "validate_topic_retention_bytes"))]
    pub retention_bytes: i64,

    /// Segment size in bytes (1MB to 10GB)
    #[serde(default = "default_topic_segment_bytes")]
    #[validate(custom(function = "validate_segment_size"))]
    pub segment_bytes: i64,

    /// Cleanup policy: "delete", "compact", or "delete,compact"
    #[serde(default = "default_topic_cleanup_policy")]
    #[validate(custom(function = "validate_topic_cleanup_policy"))]
    pub cleanup_policy: String,

    /// Compression type: "none", "gzip", "snappy", "lz4", "zstd"
    #[serde(default = "default_topic_compression")]
    #[validate(custom(function = "validate_topic_compression"))]
    pub compression_type: String,

    /// Minimum number of in-sync replicas for writes (1-10)
    #[serde(default = "default_topic_min_isr")]
    #[validate(range(min = 1, max = 10, message = "min ISR must be between 1 and 10"))]
    pub min_insync_replicas: i32,

    /// Maximum message size in bytes (1KB to 100MB)
    #[serde(default = "default_max_message_bytes")]
    #[validate(custom(function = "validate_message_size"))]
    pub max_message_bytes: i64,

    /// Whether to enable message timestamps
    #[serde(default = "default_true")]
    pub message_timestamp_enabled: bool,

    /// Timestamp type: "CreateTime" or "LogAppendTime"
    #[serde(default = "default_topic_timestamp_type")]
    #[validate(custom(function = "validate_topic_timestamp_type"))]
    pub message_timestamp_type: String,

    /// Whether to enable idempotent writes
    #[serde(default = "default_true")]
    pub idempotent_writes: bool,

    /// Flush interval in milliseconds (0 for no scheduled flush)
    #[serde(default)]
    #[validate(range(min = 0, max = 86400000, message = "flush interval must be 0-24 hours"))]
    pub flush_interval_ms: i64,

    /// Custom configuration key-value pairs
    #[serde(default)]
    #[validate(custom(function = "validate_topic_custom_config"))]
    pub custom: BTreeMap<String, String>,
}

fn default_topic_retention_ms() -> i64 {
    604800000 // 7 days
}

fn default_topic_retention_bytes() -> i64 {
    -1 // unlimited
}

fn default_topic_segment_bytes() -> i64 {
    1073741824 // 1GB
}

fn default_topic_cleanup_policy() -> String {
    "delete".to_string()
}

fn default_topic_compression() -> String {
    "lz4".to_string()
}

fn default_topic_min_isr() -> i32 {
    1
}

fn default_topic_timestamp_type() -> String {
    "CreateTime".to_string()
}

fn validate_topic_retention_bytes(value: i64) -> Result<(), ValidationError> {
    if value == -1 || (1048576..=10995116277760).contains(&value) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_retention_bytes")
            .with_message("retention_bytes must be -1 (unlimited) or 1MB-10TB".into()))
    }
}

fn validate_topic_cleanup_policy(policy: &str) -> Result<(), ValidationError> {
    match policy {
        "delete" | "compact" | "delete,compact" | "compact,delete" => Ok(()),
        _ => Err(ValidationError::new("invalid_cleanup_policy").with_message(
            "cleanup_policy must be 'delete', 'compact', or 'delete,compact'".into(),
        )),
    }
}

fn validate_topic_compression(compression: &str) -> Result<(), ValidationError> {
    match compression {
        "none" | "gzip" | "snappy" | "lz4" | "zstd" | "producer" => Ok(()),
        _ => Err(ValidationError::new("invalid_compression")
            .with_message("compression must be none, gzip, snappy, lz4, zstd, or producer".into())),
    }
}

fn validate_topic_timestamp_type(ts_type: &str) -> Result<(), ValidationError> {
    match ts_type {
        "CreateTime" | "LogAppendTime" => Ok(()),
        _ => Err(ValidationError::new("invalid_timestamp_type")
            .with_message("timestamp type must be 'CreateTime' or 'LogAppendTime'".into())),
    }
}

fn validate_topic_custom_config(config: &BTreeMap<String, String>) -> Result<(), ValidationError> {
    if config.len() > 50 {
        return Err(ValidationError::new("too_many_custom_configs")
            .with_message("maximum 50 custom config entries allowed".into()));
    }
    for (key, value) in config {
        if key.len() > 128 || value.len() > 4096 {
            return Err(ValidationError::new("config_too_long")
                .with_message("config key max 128 chars, value max 4096 chars".into()));
        }
        // Prevent overriding protected configs
        let protected = [
            "retention.ms",
            "retention.bytes",
            "segment.bytes",
            "cleanup.policy",
        ];
        if protected.contains(&key.as_str()) {
            return Err(ValidationError::new("protected_config").with_message(
                format!(
                    "'{}' must be set via dedicated field, not custom config",
                    key
                )
                .into(),
            ));
        }
    }
    Ok(())
}

/// Topic ACL entry
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TopicAcl {
    /// Principal (e.g., "user:myuser", "group:mygroup", "*")
    #[validate(length(min = 1, max = 256, message = "principal must be 1-256 characters"))]
    #[validate(custom(function = "validate_principal"))]
    pub principal: String,

    /// Allowed operations: Read, Write, Create, Delete, Alter, Describe, All
    #[validate(length(min = 1, max = 7, message = "must specify 1-7 operations"))]
    #[validate(custom(function = "validate_operations"))]
    pub operations: Vec<String>,

    /// Permission type: Allow or Deny (default: Allow)
    #[serde(default = "default_permission_type")]
    #[validate(custom(function = "validate_permission_type"))]
    pub permission_type: String,

    /// Host restriction (default: "*" for any host)
    #[serde(default = "default_acl_host")]
    #[validate(length(max = 256, message = "host must be max 256 characters"))]
    pub host: String,
}

fn default_permission_type() -> String {
    "Allow".to_string()
}

fn default_acl_host() -> String {
    "*".to_string()
}

fn validate_principal(principal: &str) -> Result<(), ValidationError> {
    if principal == "*" {
        return Ok(());
    }
    if let Some((prefix, name)) = principal.split_once(':') {
        if !["user", "group", "User", "Group"].contains(&prefix) {
            return Err(ValidationError::new("invalid_principal_prefix")
                .with_message("principal prefix must be 'user:' or 'group:'".into()));
        }
        if name.is_empty() || name.len() > 128 {
            return Err(ValidationError::new("invalid_principal_name")
                .with_message("principal name must be 1-128 characters".into()));
        }
        Ok(())
    } else {
        Err(ValidationError::new("invalid_principal_format")
            .with_message("principal must be '*' or 'user:name' or 'group:name'".into()))
    }
}

fn validate_operations(ops: &[String]) -> Result<(), ValidationError> {
    let valid_ops = [
        "Read",
        "Write",
        "Create",
        "Delete",
        "Alter",
        "Describe",
        "All",
        "DescribeConfigs",
        "AlterConfigs",
    ];
    for op in ops {
        if !valid_ops.contains(&op.as_str()) {
            return Err(ValidationError::new("invalid_operation").with_message(
                format!("'{}' is not a valid operation. Valid: {:?}", op, valid_ops).into(),
            ));
        }
    }
    Ok(())
}

fn validate_permission_type(perm: &str) -> Result<(), ValidationError> {
    match perm {
        "Allow" | "Deny" => Ok(()),
        _ => Err(ValidationError::new("invalid_permission_type")
            .with_message("permission_type must be 'Allow' or 'Deny'".into())),
    }
}

fn validate_topic_acls(acls: &[TopicAcl]) -> Result<(), ValidationError> {
    // Check for duplicate principal+operation combinations
    let mut seen = std::collections::HashSet::new();
    for acl in acls {
        for op in &acl.operations {
            let key = format!("{}:{}", acl.principal, op);
            if !seen.insert(key.clone()) {
                return Err(ValidationError::new("duplicate_acl")
                    .with_message(format!("duplicate ACL entry for {}", key).into()));
            }
        }
    }
    Ok(())
}

/// Status of the RivvenTopic resource
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RivvenTopicStatus {
    /// Current phase: Pending, Creating, Ready, Updating, Deleting, Failed
    #[serde(default)]
    pub phase: String,

    /// Human-readable message about current state
    #[serde(default)]
    pub message: String,

    /// Actual number of partitions (may differ during scaling)
    #[serde(default)]
    pub current_partitions: i32,

    /// Actual replication factor
    #[serde(default)]
    pub current_replication_factor: i32,

    /// Whether the topic exists in Rivven
    #[serde(default)]
    pub topic_exists: bool,

    /// Observed generation for tracking spec changes
    #[serde(default)]
    pub observed_generation: i64,

    /// Conditions for detailed status tracking
    #[serde(default)]
    pub conditions: Vec<TopicCondition>,

    /// Last time the topic was successfully synced
    #[serde(default)]
    pub last_sync_time: Option<String>,

    /// Partition leader information
    #[serde(default)]
    pub partition_info: Vec<PartitionInfo>,
}

/// Condition for tracking topic status
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopicCondition {
    /// Type of condition: Ready, Synced, ACLsApplied, ConfigApplied
    pub r#type: String,

    /// Status: True, False, Unknown
    pub status: String,

    /// Machine-readable reason
    pub reason: String,

    /// Human-readable message
    pub message: String,

    /// Last transition time
    pub last_transition_time: String,
}

/// Information about a topic partition
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    /// Partition ID
    pub partition: i32,

    /// Leader broker ID
    pub leader: i32,

    /// Replica broker IDs
    pub replicas: Vec<i32>,

    /// In-sync replica broker IDs
    pub isr: Vec<i32>,
}

/// Global connect configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ConnectConfigSpec {
    /// State directory path inside container
    #[serde(default = "default_state_dir")]
    pub state_dir: String,

    /// Log level (trace, debug, info, warn, error)
    #[serde(default = "default_log_level")]
    #[validate(custom(function = "validate_log_level"))]
    pub log_level: String,
}

fn default_state_dir() -> String {
    "/data/connect-state".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn validate_log_level(level: &str) -> Result<(), ValidationError> {
    match level {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(ValidationError::new("invalid_log_level")
            .with_message("log level must be one of: trace, debug, info, warn, error".into())),
    }
}

/// Source connector specification (Kafka Connect style - generic config)
///
/// All connector-specific configuration goes in the generic `config` field.
/// Connector validation happens at runtime, not at CRD schema level.
/// This design scales to 300+ connectors without CRD changes.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SourceConnectorSpec {
    /// Unique name for this source connector
    #[validate(length(min = 1, max = 63, message = "name must be 1-63 characters"))]
    #[validate(custom(function = "validate_k8s_name"))]
    pub name: String,

    /// Connector type (postgres-cdc, mysql-cdc, http, datagen, kafka, mqtt, etc.)
    #[validate(length(min = 1, max = 64, message = "connector type must be 1-64 characters"))]
    #[validate(custom(function = "validate_connector_type"))]
    pub connector: String,

    /// Target topic to publish events to (fallback/default)
    #[validate(length(min = 1, max = 255, message = "topic must be 1-255 characters"))]
    pub topic: String,

    /// Topic routing pattern for CDC connectors
    /// Enables dynamic topic selection based on CDC event metadata.
    /// Supported placeholders: {database}, {schema}, {table}
    /// Example: "cdc.{schema}.{table}" → "cdc.public.users"
    #[serde(default)]
    pub topic_routing: Option<String>,

    /// Whether this source is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Connector-specific configuration (Kafka Connect style)
    /// All connector parameters go here. Validated at runtime by the controller.
    /// Example for postgres-cdc: {"host": "...", "slot": "...", "tables": [...]}
    #[serde(default)]
    pub config: serde_json::Value,

    /// Secret reference for sensitive configuration (passwords, keys, tokens)
    /// The referenced Secret's data will be merged into config at runtime.
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub config_secret_ref: Option<String>,

    /// Topic configuration (partitions, replication)
    #[serde(default)]
    #[validate(nested)]
    pub topic_config: SourceTopicConfigSpec,
}

fn validate_connector_type(connector: &str) -> Result<(), ValidationError> {
    // Allow standard connectors and custom ones (must be alphanumeric with hyphens)
    static CONNECTOR_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$").unwrap());
    if !CONNECTOR_REGEX.is_match(connector) {
        return Err(ValidationError::new("invalid_connector_type")
            .with_message("connector type must be lowercase alphanumeric with hyphens".into()));
    }
    Ok(())
}

/// Table specification for CDC sources
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TableSpec {
    /// Schema/namespace (e.g., "public" for PostgreSQL)
    #[serde(default)]
    pub schema: Option<String>,

    /// Table name
    #[validate(length(min = 1, max = 128, message = "table name must be 1-128 characters"))]
    pub table: String,

    /// Override topic for this specific table
    #[serde(default)]
    pub topic: Option<String>,

    /// Columns to include (empty = all columns)
    #[serde(default)]
    #[validate(length(max = 500, message = "maximum 500 columns per table"))]
    pub columns: Vec<String>,

    /// Columns to exclude
    #[serde(default)]
    #[validate(length(max = 500, message = "maximum 500 excluded columns per table"))]
    pub exclude_columns: Vec<String>,

    /// Column masking rules (column -> mask pattern)
    #[serde(default)]
    pub column_masks: std::collections::BTreeMap<String, String>,
}

/// Source topic configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SourceTopicConfigSpec {
    /// Number of partitions for auto-created topics
    #[serde(default)]
    #[validate(range(min = 1, max = 1000, message = "partitions must be between 1 and 1000"))]
    pub partitions: Option<i32>,

    /// Replication factor for auto-created topics
    #[serde(default)]
    #[validate(range(
        min = 1,
        max = 10,
        message = "replication factor must be between 1 and 10"
    ))]
    pub replication_factor: Option<i32>,

    /// Auto-create topics if they don't exist
    #[serde(default)]
    pub auto_create: Option<bool>,
}

/// Sink connector specification (Kafka Connect style - generic config)
///
/// All connector-specific configuration goes in the generic `config` field.
/// Connector validation happens at runtime, not at CRD schema level.
/// This design scales to 300+ connectors without CRD changes.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SinkConnectorSpec {
    /// Unique name for this sink connector
    #[validate(length(min = 1, max = 63, message = "name must be 1-63 characters"))]
    #[validate(custom(function = "validate_k8s_name"))]
    pub name: String,

    /// Connector type (stdout, s3, http, iceberg, delta, elasticsearch, etc.)
    #[validate(length(min = 1, max = 64, message = "connector type must be 1-64 characters"))]
    #[validate(custom(function = "validate_connector_type"))]
    pub connector: String,

    /// Topics to consume from (supports wildcards like "cdc.*")
    #[validate(length(min = 1, max = 100, message = "must have 1-100 topics"))]
    pub topics: Vec<String>,

    /// Consumer group for offset tracking
    #[validate(length(
        min = 1,
        max = 128,
        message = "consumer group must be 1-128 characters"
    ))]
    pub consumer_group: String,

    /// Whether this sink is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Starting offset (earliest, latest, or timestamp)
    #[serde(default = "default_start_offset")]
    #[validate(custom(function = "validate_start_offset"))]
    pub start_offset: String,

    /// Connector-specific configuration (Kafka Connect style)
    /// All connector parameters go here. Validated at runtime by the controller.
    /// Example for iceberg: {"catalog": {...}, "namespace": "...", "table": "..."}
    #[serde(default)]
    pub config: serde_json::Value,

    /// Secret reference for sensitive configuration (passwords, keys, tokens)
    /// The referenced Secret's data will be merged into config at runtime.
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub config_secret_ref: Option<String>,

    /// Rate limiting configuration
    #[serde(default)]
    #[validate(nested)]
    pub rate_limit: RateLimitSpec,
}

fn default_start_offset() -> String {
    "latest".to_string()
}

fn validate_start_offset(offset: &str) -> Result<(), ValidationError> {
    match offset {
        "earliest" | "latest" => Ok(()),
        s if s.contains('T') && s.contains(':') => Ok(()), // ISO 8601 timestamp
        _ => Err(ValidationError::new("invalid_start_offset").with_message(
            "start offset must be 'earliest', 'latest', or ISO 8601 timestamp".into(),
        )),
    }
}

/// Rate limiting configuration for sinks
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitSpec {
    /// Maximum events per second (0 = unlimited)
    #[serde(default)]
    #[validate(range(
        min = 0,
        max = 1_000_000,
        message = "events per second must be 0-1000000"
    ))]
    pub events_per_second: u64,

    /// Burst capacity (extra events above steady rate)
    #[serde(default)]
    #[validate(range(min = 0, max = 100_000, message = "burst capacity must be 0-100000"))]
    pub burst_capacity: Option<u64>,
}

/// Global settings for all connectors
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct GlobalConnectSettings {
    /// Topic auto-creation settings
    #[serde(default)]
    #[validate(nested)]
    pub topic: TopicSettingsSpec,

    /// Retry configuration
    #[serde(default)]
    #[validate(nested)]
    pub retry: RetryConfigSpec,

    /// Health check configuration
    #[serde(default)]
    #[validate(nested)]
    pub health: HealthConfigSpec,

    /// Metrics configuration
    #[serde(default)]
    #[validate(nested)]
    pub metrics: ConnectMetricsSpec,
}

/// Topic settings for auto-creation
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TopicSettingsSpec {
    /// Enable automatic topic creation
    #[serde(default = "default_true")]
    pub auto_create: bool,

    /// Default partitions for new topics
    #[serde(default = "default_topic_partitions")]
    #[validate(range(min = 1, max = 1000, message = "partitions must be between 1 and 1000"))]
    pub default_partitions: i32,

    /// Default replication factor
    #[serde(default = "default_topic_replication")]
    #[validate(range(
        min = 1,
        max = 10,
        message = "replication factor must be between 1 and 10"
    ))]
    pub default_replication_factor: i32,

    /// Fail if topic doesn't exist and auto_create is false
    #[serde(default = "default_true")]
    pub require_topic_exists: bool,
}

fn default_topic_partitions() -> i32 {
    1
}

fn default_topic_replication() -> i32 {
    1
}

impl Default for TopicSettingsSpec {
    fn default() -> Self {
        Self {
            auto_create: true,
            default_partitions: 1,
            default_replication_factor: 1,
            require_topic_exists: true,
        }
    }
}

/// Retry configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct RetryConfigSpec {
    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 100, message = "max retries must be 0-100"))]
    pub max_retries: i32,

    /// Initial backoff in milliseconds
    #[serde(default = "default_initial_backoff_ms")]
    #[validate(range(min = 10, max = 60000, message = "initial backoff must be 10-60000ms"))]
    pub initial_backoff_ms: i64,

    /// Maximum backoff in milliseconds
    #[serde(default = "default_max_backoff_ms")]
    #[validate(range(
        min = 100,
        max = 3600000,
        message = "max backoff must be 100-3600000ms"
    ))]
    pub max_backoff_ms: i64,

    /// Backoff multiplier
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

fn default_max_retries() -> i32 {
    10
}

fn default_initial_backoff_ms() -> i64 {
    100
}

fn default_max_backoff_ms() -> i64 {
    30000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

impl Default for RetryConfigSpec {
    fn default() -> Self {
        Self {
            max_retries: 10,
            initial_backoff_ms: 100,
            max_backoff_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct HealthConfigSpec {
    /// Enable health check HTTP endpoint
    #[serde(default)]
    pub enabled: bool,

    /// Health check port
    #[serde(default = "default_health_port")]
    #[validate(range(min = 1024, max = 65535, message = "port must be 1024-65535"))]
    pub port: i32,

    /// Health check path
    #[serde(default = "default_health_path")]
    pub path: String,
}

fn default_health_port() -> i32 {
    8080
}

fn default_health_path() -> String {
    "/health".to_string()
}

impl Default for HealthConfigSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 8080,
            path: "/health".to_string(),
        }
    }
}

/// Metrics configuration for connect
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ConnectMetricsSpec {
    /// Enable metrics endpoint
    #[serde(default)]
    pub enabled: bool,

    /// Metrics port
    #[serde(default = "default_connect_metrics_port")]
    #[validate(range(min = 1024, max = 65535, message = "port must be 1024-65535"))]
    pub port: i32,
}

fn default_connect_metrics_port() -> i32 {
    9091
}

impl Default for ConnectMetricsSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            port: 9091,
        }
    }
}

/// TLS configuration for connect broker connection
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ConnectTlsSpec {
    /// Enable TLS for broker connection
    #[serde(default)]
    pub enabled: bool,

    /// Secret containing TLS certificates
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub cert_secret_name: Option<String>,

    /// Enable mTLS (mutual TLS)
    #[serde(default)]
    pub mtls_enabled: bool,

    /// CA secret name for mTLS
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub ca_secret_name: Option<String>,

    /// Skip server certificate verification (DANGEROUS - testing only)
    #[serde(default)]
    pub insecure: bool,
}

fn default_connect_replicas() -> i32 {
    1
}

/// Status of a RivvenConnect resource
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RivvenConnectStatus {
    /// Current phase of the connect instance
    pub phase: ConnectPhase,

    /// Total replicas
    pub replicas: i32,

    /// Ready replicas
    pub ready_replicas: i32,

    /// Number of source connectors running
    pub sources_running: i32,

    /// Number of sink connectors running
    pub sinks_running: i32,

    /// Total number of sources configured
    pub sources_total: i32,

    /// Total number of sinks configured
    pub sinks_total: i32,

    /// Current observed generation
    pub observed_generation: i64,

    /// Conditions describing connect state
    #[serde(default)]
    pub conditions: Vec<ConnectCondition>,

    /// Individual connector statuses
    #[serde(default)]
    pub connector_statuses: Vec<ConnectorStatus>,

    /// Last time the status was updated
    pub last_updated: Option<String>,

    /// Error message if any
    pub message: Option<String>,
}

/// Phase of the connect lifecycle
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ConnectPhase {
    /// Connect is being created
    #[default]
    Pending,
    /// Connect is starting up
    Starting,
    /// Connect is running and healthy
    Running,
    /// Connect is partially healthy (some connectors failed)
    Degraded,
    /// Connect has failed
    Failed,
    /// Connect is being deleted
    Terminating,
}

/// Condition describing an aspect of connect state
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectCondition {
    /// Type of condition (Ready, BrokerConnected, SourcesHealthy, SinksHealthy)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Reason for the condition
    pub reason: Option<String>,

    /// Human-readable message
    pub message: Option<String>,

    /// Last transition time
    pub last_transition_time: Option<String>,
}

/// Status of an individual connector
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectorStatus {
    /// Connector name
    pub name: String,

    /// Connector type (source or sink)
    pub connector_type: String,

    /// Connector kind (postgres-cdc, stdout, etc.)
    pub kind: String,

    /// Current state (running, stopped, failed)
    pub state: String,

    /// Number of events processed
    pub events_processed: i64,

    /// Last error message
    pub last_error: Option<String>,

    /// Last successful operation time
    pub last_success_time: Option<String>,
}

impl RivvenConnectSpec {
    /// Get the full container image including version
    pub fn get_image(&self) -> String {
        if let Some(ref image) = self.image {
            image.clone()
        } else {
            format!("ghcr.io/hupe1980/rivven-connect:{}", self.version)
        }
    }

    /// Get labels for managed resources
    pub fn get_labels(&self, connect_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "rivven-connect".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            connect_name.to_string(),
        );
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "connector".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/managed-by".to_string(),
            "rivven-operator".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/version".to_string(),
            self.version.clone(),
        );
        labels
    }

    /// Get selector labels for managed resources
    pub fn get_selector_labels(&self, connect_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "rivven-connect".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            connect_name.to_string(),
        );
        labels
    }

    /// Get enabled sources count
    pub fn enabled_sources_count(&self) -> usize {
        self.sources.iter().filter(|s| s.enabled).count()
    }

    /// Get enabled sinks count
    pub fn enabled_sinks_count(&self) -> usize {
        self.sinks.iter().filter(|s| s.enabled).count()
    }
}

// ============================================================================
// Advanced CDC Configuration Types (Shared between PostgreSQL and MySQL CDC)
// ============================================================================
// These types map to rivven-cdc features and provide enterprise-grade CDC capabilities.

/// Snapshot configuration for initial data load
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotCdcConfigSpec {
    /// Batch size for SELECT queries (rows per batch)
    #[serde(default = "default_snapshot_batch_size")]
    #[validate(range(min = 100, max = 1000000, message = "batch size must be 100-1000000"))]
    pub batch_size: i32,

    /// Number of tables to snapshot in parallel
    #[serde(default = "default_snapshot_parallel_tables")]
    #[validate(range(min = 1, max = 32, message = "parallel tables must be 1-32"))]
    pub parallel_tables: i32,

    /// Query timeout in seconds
    #[serde(default = "default_snapshot_query_timeout")]
    #[validate(range(min = 10, max = 3600, message = "query timeout must be 10-3600s"))]
    pub query_timeout_secs: i32,

    /// Delay between batches for backpressure (ms)
    #[serde(default)]
    #[validate(range(min = 0, max = 60000, message = "throttle delay must be 0-60000ms"))]
    pub throttle_delay_ms: i32,

    /// Maximum retries per batch on failure
    #[serde(default = "default_snapshot_max_retries")]
    #[validate(range(min = 0, max = 10, message = "max retries must be 0-10"))]
    pub max_retries: i32,

    /// Tables to include in snapshot (empty = all tables)
    #[serde(default)]
    pub include_tables: Vec<String>,

    /// Tables to exclude from snapshot
    #[serde(default)]
    pub exclude_tables: Vec<String>,
}

fn default_snapshot_batch_size() -> i32 {
    10_000
}
fn default_snapshot_parallel_tables() -> i32 {
    4
}
fn default_snapshot_query_timeout() -> i32 {
    300
}
fn default_snapshot_max_retries() -> i32 {
    3
}

/// Incremental snapshot configuration (non-blocking re-snapshots)
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct IncrementalSnapshotSpec {
    /// Enable incremental snapshots
    #[serde(default)]
    pub enabled: bool,

    /// Chunk size (rows per chunk)
    #[serde(default = "default_incremental_chunk_size")]
    #[validate(range(min = 100, max = 100000, message = "chunk size must be 100-100000"))]
    pub chunk_size: i32,

    /// Watermark strategy: insert, update_and_insert
    #[serde(default)]
    #[validate(custom(function = "validate_watermark_strategy"))]
    pub watermark_strategy: String,

    /// Signal table for watermark signals
    #[serde(default)]
    pub watermark_signal_table: Option<String>,

    /// Maximum concurrent chunks
    #[serde(default = "default_incremental_max_chunks")]
    #[validate(range(min = 1, max = 16, message = "max concurrent chunks must be 1-16"))]
    pub max_concurrent_chunks: i32,

    /// Delay between chunks (ms)
    #[serde(default)]
    #[validate(range(min = 0, max = 60000, message = "chunk delay must be 0-60000ms"))]
    pub chunk_delay_ms: i32,
}

fn default_incremental_chunk_size() -> i32 {
    1024
}
fn default_incremental_max_chunks() -> i32 {
    1
}

fn validate_watermark_strategy(strategy: &str) -> Result<(), ValidationError> {
    match strategy {
        "" | "insert" | "update_and_insert" => Ok(()),
        _ => Err(ValidationError::new("invalid_watermark_strategy")
            .with_message("watermark strategy must be: insert or update_and_insert".into())),
    }
}

/// Signal table configuration for ad-hoc snapshots and control
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SignalTableSpec {
    /// Enable signal processing
    #[serde(default)]
    pub enabled: bool,

    /// Fully-qualified signal table name (schema.table)
    #[serde(default)]
    pub data_collection: Option<String>,

    /// Topic for signal messages (alternative to table)
    #[serde(default)]
    pub topic: Option<String>,

    /// Enabled signal channels: source, topic, file, api
    #[serde(default)]
    pub enabled_channels: Vec<String>,

    /// Poll interval for file channel (ms)
    #[serde(default = "default_signal_poll_interval")]
    pub poll_interval_ms: i32,
}

fn default_signal_poll_interval() -> i32 {
    1000
}

/// Heartbeat monitoring configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatCdcSpec {
    /// Enable heartbeat monitoring
    #[serde(default)]
    pub enabled: bool,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_cdc_interval")]
    #[validate(range(min = 1, max = 3600, message = "heartbeat interval must be 1-3600s"))]
    pub interval_secs: i32,

    /// Maximum allowed lag before marking as unhealthy (seconds)
    #[serde(default = "default_heartbeat_max_lag")]
    #[validate(range(min = 10, max = 86400, message = "max lag must be 10-86400s"))]
    pub max_lag_secs: i32,

    /// Emit heartbeat events to a topic
    #[serde(default)]
    pub emit_events: bool,

    /// Topic name for heartbeat events
    #[serde(default)]
    pub topic: Option<String>,

    /// SQL query to execute on each heartbeat (keeps connections alive)
    #[serde(default)]
    pub action_query: Option<String>,
}

fn default_heartbeat_cdc_interval() -> i32 {
    10
}
fn default_heartbeat_max_lag() -> i32 {
    300
}

/// Event deduplication configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct DeduplicationCdcSpec {
    /// Enable deduplication
    #[serde(default)]
    pub enabled: bool,

    /// Bloom filter expected insertions
    #[serde(default = "default_bloom_expected")]
    #[validate(range(
        min = 1000,
        max = 10000000,
        message = "bloom expected must be 1000-10M"
    ))]
    pub bloom_expected_insertions: i64,

    /// Bloom filter false positive rate (0.001-0.1)
    #[serde(default = "default_bloom_fpp")]
    pub bloom_fpp: f64,

    /// LRU cache size
    #[serde(default = "default_lru_size")]
    #[validate(range(min = 1000, max = 1000000, message = "LRU size must be 1000-1M"))]
    pub lru_size: i64,

    /// Deduplication window (seconds)
    #[serde(default = "default_dedup_window")]
    #[validate(range(min = 60, max = 604800, message = "window must be 60-604800s"))]
    pub window_secs: i64,
}

fn default_bloom_expected() -> i64 {
    100_000
}
fn default_bloom_fpp() -> f64 {
    0.01
}
fn default_lru_size() -> i64 {
    10_000
}
fn default_dedup_window() -> i64 {
    3600
}

/// Transaction topic configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TransactionTopicSpec {
    /// Enable transaction topic
    #[serde(default)]
    pub enabled: bool,

    /// Transaction topic name (default: {database}.transaction)
    #[serde(default)]
    pub topic_name: Option<String>,

    /// Include transaction data collections in events
    #[serde(default = "default_true_cdc")]
    pub include_data_collections: bool,

    /// Minimum events in transaction to emit (filter small txns)
    #[serde(default)]
    #[validate(range(min = 0, max = 10000, message = "min events must be 0-10000"))]
    pub min_events_threshold: i32,
}

fn default_true_cdc() -> bool {
    true
}

/// Schema change topic configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaChangeTopicSpec {
    /// Enable schema change topic
    #[serde(default)]
    pub enabled: bool,

    /// Schema change topic name (default: {database}.schema_changes)
    #[serde(default)]
    pub topic_name: Option<String>,

    /// Include table columns in change events
    #[serde(default = "default_true_cdc")]
    pub include_columns: bool,

    /// Schemas to monitor for changes (empty = all)
    #[serde(default)]
    pub schemas: Vec<String>,
}

/// Tombstone configuration for log compaction
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct TombstoneCdcSpec {
    /// Enable tombstone events for deletes
    #[serde(default)]
    pub enabled: bool,

    /// Emit tombstone after delete event
    #[serde(default = "default_true_cdc")]
    pub after_delete: bool,

    /// Tombstone behavior: emit_null, emit_with_key
    #[serde(default)]
    #[validate(custom(function = "validate_tombstone_behavior"))]
    pub behavior: String,
}

fn validate_tombstone_behavior(behavior: &str) -> Result<(), ValidationError> {
    match behavior {
        "" | "emit_null" | "emit_with_key" => Ok(()),
        _ => Err(ValidationError::new("invalid_tombstone_behavior")
            .with_message("tombstone behavior must be: emit_null or emit_with_key".into())),
    }
}

/// Field-level encryption configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct FieldEncryptionSpec {
    /// Enable field encryption
    #[serde(default)]
    pub enabled: bool,

    /// Secret reference containing encryption key
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub key_secret_ref: Option<String>,

    /// Fields to encrypt (table.column format)
    #[serde(default)]
    pub fields: Vec<String>,

    /// Encryption algorithm (default: aes-256-gcm)
    #[serde(default = "default_encryption_algorithm")]
    pub algorithm: String,
}

fn default_encryption_algorithm() -> String {
    "aes-256-gcm".to_string()
}

/// Read-only replica configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ReadOnlyReplicaSpec {
    /// Enable read-only mode (for connecting to replicas)
    #[serde(default)]
    pub enabled: bool,

    /// Replication lag threshold before warnings (ms)
    #[serde(default = "default_lag_threshold")]
    #[validate(range(
        min = 100,
        max = 300000,
        message = "lag threshold must be 100-300000ms"
    ))]
    pub lag_threshold_ms: i64,

    /// Enable deduplication (required for replicas)
    #[serde(default = "default_true_cdc")]
    pub deduplicate: bool,

    /// Watermark source: primary, replica
    #[serde(default)]
    #[validate(custom(function = "validate_watermark_source"))]
    pub watermark_source: String,
}

fn default_lag_threshold() -> i64 {
    5000
}

fn validate_watermark_source(source: &str) -> Result<(), ValidationError> {
    match source {
        "" | "primary" | "replica" => Ok(()),
        _ => Err(ValidationError::new("invalid_watermark_source")
            .with_message("watermark source must be: primary or replica".into())),
    }
}

/// Event router configuration with dead letter queue support
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct EventRouterSpec {
    /// Enable event routing
    #[serde(default)]
    pub enabled: bool,

    /// Default destination when no rules match
    #[serde(default)]
    pub default_destination: Option<String>,

    /// Dead letter queue for unroutable events
    #[serde(default)]
    pub dead_letter_queue: Option<String>,

    /// Drop unroutable events instead of sending to DLQ
    #[serde(default)]
    pub drop_unroutable: bool,

    /// Routing rules (evaluated in priority order)
    #[serde(default)]
    #[validate(nested)]
    pub rules: Vec<RouteRuleSpec>,
}

/// Routing rule specification
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct RouteRuleSpec {
    /// Rule name (for logging/debugging)
    #[validate(length(min = 1, max = 128, message = "rule name must be 1-128 characters"))]
    pub name: String,

    /// Priority (higher = evaluated first)
    #[serde(default)]
    pub priority: i32,

    /// Routing condition type: always, table, table_pattern, schema, operation, field_equals, field_exists
    #[validate(custom(function = "validate_route_condition_type"))]
    pub condition_type: String,

    /// Condition value (table name, pattern, field, etc.)
    #[serde(default)]
    pub condition_value: Option<String>,

    /// Second condition value (for field_equals: expected value)
    #[serde(default)]
    pub condition_value2: Option<String>,

    /// Destination(s) for matching events
    pub destinations: Vec<String>,

    /// Continue evaluating rules after match (fan-out)
    #[serde(default)]
    pub continue_matching: bool,
}

fn validate_route_condition_type(condition: &str) -> Result<(), ValidationError> {
    match condition {
        "always" | "table" | "table_pattern" | "schema" | "operation" | "field_equals"
        | "field_exists" => Ok(()),
        _ => Err(ValidationError::new("invalid_route_condition_type").with_message(
            "condition type must be: always, table, table_pattern, schema, operation, field_equals, or field_exists".into(),
        )),
    }
}

/// Partitioner configuration for Kafka-style partitioning
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct PartitionerSpec {
    /// Enable partitioning
    #[serde(default)]
    pub enabled: bool,

    /// Number of partitions
    #[serde(default = "default_num_partitions")]
    #[validate(range(min = 1, max = 10000, message = "num partitions must be 1-10000"))]
    pub num_partitions: i32,

    /// Partitioning strategy: round_robin, key_hash, table_hash, full_table_hash, sticky
    #[serde(default = "default_partition_strategy")]
    #[validate(custom(function = "validate_partition_strategy"))]
    pub strategy: String,

    /// Key columns for key_hash strategy
    #[serde(default)]
    pub key_columns: Vec<String>,

    /// Fixed partition for sticky strategy
    #[serde(default)]
    pub sticky_partition: Option<i32>,
}

fn default_num_partitions() -> i32 {
    16
}
fn default_partition_strategy() -> String {
    "key_hash".to_string()
}

fn validate_partition_strategy(strategy: &str) -> Result<(), ValidationError> {
    match strategy {
        "round_robin" | "key_hash" | "table_hash" | "full_table_hash" | "sticky" => Ok(()),
        _ => Err(ValidationError::new("invalid_partition_strategy").with_message(
            "partition strategy must be: round_robin, key_hash, table_hash, full_table_hash, or sticky".into(),
        )),
    }
}

/// Single Message Transform (SMT) configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SmtTransformSpec {
    /// Transform type (see SMT documentation for available transforms)
    #[validate(custom(function = "validate_smt_transform_type"))]
    pub transform_type: String,

    /// Transform name (optional, for logging)
    #[serde(default)]
    pub name: Option<String>,

    /// Transform-specific configuration
    #[serde(default)]
    pub config: serde_json::Value,
}

fn validate_smt_transform_type(transform: &str) -> Result<(), ValidationError> {
    match transform {
        "extract_new_record_state" | "value_to_key" | "timestamp_converter"
        | "timezone_converter" | "mask_field" | "filter" | "flatten" | "insert_field"
        | "rename_field" | "replace_field" | "cast" | "regex_router" | "content_router"
        | "header_to_value" | "unwrap" | "set_null" | "compute_field" | "conditional_smt" => Ok(()),
        _ => Err(ValidationError::new("invalid_smt_transform_type").with_message(
            "transform type must be a valid SMT (extract_new_record_state, mask_field, filter, etc.)".into(),
        )),
    }
}

/// Parallel CDC processing configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ParallelCdcSpec {
    /// Enable parallel processing
    #[serde(default)]
    pub enabled: bool,

    /// Maximum concurrent table workers
    #[serde(default = "default_parallel_concurrency")]
    #[validate(range(min = 1, max = 64, message = "concurrency must be 1-64"))]
    pub concurrency: i32,

    /// Buffer size per table
    #[serde(default = "default_per_table_buffer")]
    #[validate(range(
        min = 100,
        max = 100000,
        message = "per table buffer must be 100-100000"
    ))]
    pub per_table_buffer: i32,

    /// Output channel buffer size
    #[serde(default = "default_output_buffer")]
    #[validate(range(min = 1000, max = 1000000, message = "output buffer must be 1000-1M"))]
    pub output_buffer: i32,

    /// Enable work stealing between workers
    #[serde(default = "default_true_cdc")]
    pub work_stealing: bool,

    /// Maximum events per second per table (None = unlimited)
    #[serde(default)]
    pub per_table_rate_limit: Option<i64>,

    /// Shutdown timeout in seconds
    #[serde(default = "default_shutdown_timeout")]
    #[validate(range(min = 1, max = 300, message = "shutdown timeout must be 1-300s"))]
    pub shutdown_timeout_secs: i32,
}

fn default_parallel_concurrency() -> i32 {
    4
}
fn default_per_table_buffer() -> i32 {
    1000
}
fn default_output_buffer() -> i32 {
    10_000
}
fn default_shutdown_timeout() -> i32 {
    30
}

/// Outbox pattern configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct OutboxSpec {
    /// Enable outbox pattern
    #[serde(default)]
    pub enabled: bool,

    /// Outbox table name
    #[serde(default = "default_outbox_table")]
    #[validate(length(min = 1, max = 128, message = "outbox table must be 1-128 characters"))]
    pub table_name: String,

    /// Poll interval in milliseconds
    #[serde(default = "default_outbox_poll_interval")]
    #[validate(range(min = 10, max = 60000, message = "poll interval must be 10-60000ms"))]
    pub poll_interval_ms: i32,

    /// Maximum events per batch
    #[serde(default = "default_outbox_batch_size")]
    #[validate(range(min = 1, max = 10000, message = "batch size must be 1-10000"))]
    pub batch_size: i32,

    /// Maximum delivery retries before DLQ
    #[serde(default = "default_outbox_max_retries")]
    #[validate(range(min = 0, max = 100, message = "max retries must be 0-100"))]
    pub max_retries: i32,

    /// Delivery timeout in seconds
    #[serde(default = "default_outbox_timeout")]
    #[validate(range(min = 1, max = 300, message = "timeout must be 1-300s"))]
    pub delivery_timeout_secs: i32,

    /// Enable ordered delivery per aggregate
    #[serde(default = "default_true_cdc")]
    pub ordered_delivery: bool,

    /// Retention period for delivered events in seconds
    #[serde(default = "default_outbox_retention")]
    #[validate(range(min = 60, max = 604800, message = "retention must be 60-604800s"))]
    pub retention_secs: i64,

    /// Maximum concurrent deliveries
    #[serde(default = "default_outbox_concurrency")]
    #[validate(range(min = 1, max = 100, message = "concurrency must be 1-100"))]
    pub max_concurrency: i32,
}

fn default_outbox_table() -> String {
    "outbox".to_string()
}
fn default_outbox_poll_interval() -> i32 {
    100
}
fn default_outbox_batch_size() -> i32 {
    100
}
fn default_outbox_max_retries() -> i32 {
    3
}
fn default_outbox_timeout() -> i32 {
    30
}
fn default_outbox_retention() -> i64 {
    86400
}
fn default_outbox_concurrency() -> i32 {
    10
}

/// Health monitoring configuration for CDC connectors
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct HealthMonitorSpec {
    /// Enable health monitoring
    #[serde(default)]
    pub enabled: bool,

    /// Interval between health checks in seconds
    #[serde(default = "default_health_check_interval")]
    #[validate(range(min = 1, max = 300, message = "check interval must be 1-300s"))]
    pub check_interval_secs: i32,

    /// Maximum allowed replication lag in milliseconds
    #[serde(default = "default_health_max_lag")]
    #[validate(range(min = 1000, max = 3600000, message = "max lag must be 1000-3600000ms"))]
    pub max_lag_ms: i64,

    /// Number of failed checks before marking unhealthy
    #[serde(default = "default_health_failure_threshold")]
    #[validate(range(min = 1, max = 10, message = "failure threshold must be 1-10"))]
    pub failure_threshold: i32,

    /// Number of successful checks to recover
    #[serde(default = "default_health_success_threshold")]
    #[validate(range(min = 1, max = 10, message = "success threshold must be 1-10"))]
    pub success_threshold: i32,

    /// Timeout for individual health checks in seconds
    #[serde(default = "default_health_check_timeout")]
    #[validate(range(min = 1, max = 60, message = "check timeout must be 1-60s"))]
    pub check_timeout_secs: i32,

    /// Enable automatic recovery on failure
    #[serde(default = "default_true_cdc")]
    pub auto_recovery: bool,

    /// Initial recovery delay in seconds
    #[serde(default = "default_health_recovery_delay")]
    #[validate(range(min = 1, max = 300, message = "recovery delay must be 1-300s"))]
    pub recovery_delay_secs: i32,

    /// Maximum recovery delay in seconds (for exponential backoff)
    #[serde(default = "default_health_max_recovery_delay")]
    #[validate(range(min = 1, max = 3600, message = "max recovery delay must be 1-3600s"))]
    pub max_recovery_delay_secs: i32,
}

fn default_health_check_interval() -> i32 {
    10
}
fn default_health_max_lag() -> i64 {
    30_000
}
fn default_health_failure_threshold() -> i32 {
    3
}
fn default_health_success_threshold() -> i32 {
    2
}
fn default_health_check_timeout() -> i32 {
    5
}
fn default_health_recovery_delay() -> i32 {
    1
}
fn default_health_max_recovery_delay() -> i32 {
    60
}

// ============================================================================
// RivvenSchemaRegistry CRD - Schema Registry for Rivven
// ============================================================================

/// RivvenSchemaRegistry custom resource for managing Schema Registry instances
///
/// This CRD allows declarative management of Rivven Schema Registry deployments,
/// a Confluent-compatible schema registry supporting Avro, JSON Schema, and Protobuf.
///
/// # Example
///
/// ```yaml
/// apiVersion: rivven.hupe1980.github.io/v1alpha1
/// kind: RivvenSchemaRegistry
/// metadata:
///   name: production-registry
///   namespace: rivven
/// spec:
///   clusterRef:
///     name: my-rivven-cluster
///   replicas: 3
///   version: "0.0.1"
///   storage:
///     mode: broker
///     topic: _schemas
///   compatibility:
///     default: BACKWARD
///   schemas:
///     avro: true
///     jsonSchema: true
///     protobuf: true
///   contexts:
///     enabled: true
///   tls:
///     enabled: true
///     certSecretName: schema-registry-tls
/// ```
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[kube(
    group = "rivven.hupe1980.github.io",
    version = "v1alpha1",
    kind = "RivvenSchemaRegistry",
    plural = "rivvenschemaregistries",
    shortname = "rsr",
    namespaced,
    status = "RivvenSchemaRegistryStatus",
    printcolumn = r#"{"name":"Cluster","type":"string","jsonPath":".spec.clusterRef.name"}"#,
    printcolumn = r#"{"name":"Replicas","type":"integer","jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Schemas","type":"integer","jsonPath":".status.schemasRegistered"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct RivvenSchemaRegistrySpec {
    /// Reference to the RivvenCluster this registry connects to
    #[validate(nested)]
    pub cluster_ref: ClusterReference,

    /// Number of registry replicas (1-10)
    #[serde(default = "default_schema_registry_replicas")]
    #[validate(range(min = 1, max = 10, message = "replicas must be between 1 and 10"))]
    pub replicas: i32,

    /// Schema Registry version
    #[serde(default = "default_version")]
    pub version: String,

    /// Custom container image (overrides version-based default)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_image"))]
    pub image: Option<String>,

    /// Image pull policy
    #[serde(default = "default_image_pull_policy")]
    #[validate(custom(function = "validate_pull_policy"))]
    pub image_pull_policy: String,

    /// Image pull secrets
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,

    /// Resource requests/limits
    #[serde(default)]
    pub resources: Option<serde_json::Value>,

    /// HTTP server configuration
    #[serde(default)]
    #[validate(nested)]
    pub server: SchemaRegistryServerSpec,

    /// Storage backend configuration
    #[serde(default)]
    #[validate(nested)]
    pub storage: SchemaRegistryStorageSpec,

    /// Compatibility configuration
    #[serde(default)]
    #[validate(nested)]
    pub compatibility: SchemaCompatibilitySpec,

    /// Schema format support configuration
    #[serde(default)]
    #[validate(nested)]
    pub schemas: SchemaFormatSpec,

    /// Schema contexts (multi-tenant) configuration
    #[serde(default)]
    #[validate(nested)]
    pub contexts: SchemaContextsSpec,

    /// Validation rules configuration
    #[serde(default)]
    #[validate(nested)]
    pub validation: SchemaValidationSpec,

    /// Authentication configuration
    #[serde(default)]
    #[validate(nested)]
    pub auth: SchemaRegistryAuthSpec,

    /// TLS configuration
    #[serde(default)]
    #[validate(nested)]
    pub tls: SchemaRegistryTlsSpec,

    /// Metrics configuration
    #[serde(default)]
    #[validate(nested)]
    pub metrics: SchemaRegistryMetricsSpec,

    /// External registry configuration (for mirroring/sync)
    #[serde(default)]
    #[validate(nested)]
    pub external: ExternalRegistrySpec,

    /// Pod annotations
    #[serde(default)]
    #[validate(custom(function = "validate_annotations"))]
    pub pod_annotations: BTreeMap<String, String>,

    /// Pod labels
    #[serde(default)]
    #[validate(custom(function = "validate_labels"))]
    pub pod_labels: BTreeMap<String, String>,

    /// Environment variables
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 environment variables allowed"))]
    pub env: Vec<k8s_openapi::api::core::v1::EnvVar>,

    /// Node selector for pod scheduling
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,

    /// Pod tolerations
    #[serde(default)]
    #[validate(length(max = 20, message = "maximum 20 tolerations allowed"))]
    pub tolerations: Vec<k8s_openapi::api::core::v1::Toleration>,

    /// Pod affinity rules
    #[serde(default)]
    pub affinity: Option<serde_json::Value>,

    /// Service account name
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub service_account: Option<String>,

    /// Pod security context
    #[serde(default)]
    pub security_context: Option<serde_json::Value>,

    /// Container security context
    #[serde(default)]
    pub container_security_context: Option<serde_json::Value>,

    /// Liveness probe configuration
    #[serde(default)]
    #[validate(nested)]
    pub liveness_probe: ProbeSpec,

    /// Readiness probe configuration
    #[serde(default)]
    #[validate(nested)]
    pub readiness_probe: ProbeSpec,

    /// Pod disruption budget configuration
    #[serde(default)]
    #[validate(nested)]
    pub pod_disruption_budget: PdbSpec,
}

fn default_schema_registry_replicas() -> i32 {
    1
}

/// HTTP server configuration for Schema Registry
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryServerSpec {
    /// HTTP server port (1024-65535)
    #[serde(default = "default_schema_registry_port")]
    #[validate(range(min = 1024, max = 65535, message = "port must be 1024-65535"))]
    pub port: i32,

    /// Bind address (default: 0.0.0.0)
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Request timeout in seconds
    #[serde(default = "default_request_timeout")]
    #[validate(range(min = 1, max = 300, message = "timeout must be 1-300 seconds"))]
    pub timeout_seconds: i32,

    /// Maximum request body size in bytes (default: 10MB)
    #[serde(default = "default_max_request_size")]
    #[validate(range(
        min = 1024,
        max = 104857600,
        message = "max request size must be 1KB-100MB"
    ))]
    pub max_request_size: i64,

    /// Enable CORS (for web clients)
    #[serde(default)]
    pub cors_enabled: bool,

    /// CORS allowed origins (empty = all)
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
}

fn default_schema_registry_port() -> i32 {
    8081
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_request_timeout() -> i32 {
    30
}

fn default_max_request_size() -> i64 {
    10_485_760 // 10MB
}

impl Default for SchemaRegistryServerSpec {
    fn default() -> Self {
        Self {
            port: 8081,
            bind_address: "0.0.0.0".to_string(),
            timeout_seconds: 30,
            max_request_size: 10_485_760,
            cors_enabled: false,
            cors_allowed_origins: vec![],
        }
    }
}

/// Storage configuration for Schema Registry
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryStorageSpec {
    /// Storage mode: memory, broker
    #[serde(default = "default_storage_mode")]
    #[validate(custom(function = "validate_storage_mode"))]
    pub mode: String,

    /// Rivven topic for broker-backed storage (required if mode=broker)
    #[serde(default = "default_schema_topic")]
    #[validate(length(max = 255, message = "topic name max 255 characters"))]
    pub topic: String,

    /// Topic replication factor
    #[serde(default = "default_schema_topic_replication")]
    #[validate(range(min = 1, max = 10, message = "replication factor must be 1-10"))]
    pub replication_factor: i32,

    /// Topic partitions (usually 1 for ordered processing)
    #[serde(default = "default_schema_topic_partitions")]
    #[validate(range(min = 1, max = 100, message = "partitions must be 1-100"))]
    pub partitions: i32,

    /// Enable schema normalization
    #[serde(default = "default_true")]
    pub normalize: bool,
}

fn default_storage_mode() -> String {
    "broker".to_string()
}

fn default_schema_topic() -> String {
    "_schemas".to_string()
}

fn default_schema_topic_replication() -> i32 {
    3
}

fn default_schema_topic_partitions() -> i32 {
    1
}

fn validate_storage_mode(mode: &str) -> Result<(), ValidationError> {
    match mode {
        "memory" | "broker" => Ok(()),
        _ => Err(ValidationError::new("invalid_storage_mode")
            .with_message("storage mode must be 'memory' or 'broker'".into())),
    }
}

impl Default for SchemaRegistryStorageSpec {
    fn default() -> Self {
        Self {
            mode: "broker".to_string(),
            topic: "_schemas".to_string(),
            replication_factor: 3,
            partitions: 1,
            normalize: true,
        }
    }
}

/// Schema compatibility configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaCompatibilitySpec {
    /// Default compatibility level for new subjects
    #[serde(default = "default_compatibility_level")]
    #[validate(custom(function = "validate_compatibility_level"))]
    pub default_level: String,

    /// Allow per-subject compatibility overrides
    #[serde(default = "default_true")]
    pub allow_overrides: bool,

    /// Per-subject compatibility levels
    #[serde(default)]
    #[validate(custom(function = "validate_subject_compatibility_map"))]
    pub subjects: BTreeMap<String, String>,
}

fn default_compatibility_level() -> String {
    "BACKWARD".to_string()
}

fn validate_compatibility_level(level: &str) -> Result<(), ValidationError> {
    let valid_levels = [
        "BACKWARD",
        "BACKWARD_TRANSITIVE",
        "FORWARD",
        "FORWARD_TRANSITIVE",
        "FULL",
        "FULL_TRANSITIVE",
        "NONE",
    ];
    if valid_levels.contains(&level) {
        Ok(())
    } else {
        Err(
            ValidationError::new("invalid_compatibility_level").with_message(
                format!(
                    "'{}' is not valid. Must be one of: {:?}",
                    level, valid_levels
                )
                .into(),
            ),
        )
    }
}

fn validate_subject_compatibility_map(
    subjects: &BTreeMap<String, String>,
) -> Result<(), ValidationError> {
    if subjects.len() > 1000 {
        return Err(ValidationError::new("too_many_subjects")
            .with_message("maximum 1000 per-subject compatibility entries".into()));
    }
    for (subject, level) in subjects {
        if subject.len() > 255 {
            return Err(ValidationError::new("subject_name_too_long")
                .with_message(format!("subject '{}' exceeds 255 characters", subject).into()));
        }
        validate_compatibility_level(level)?;
    }
    Ok(())
}

impl Default for SchemaCompatibilitySpec {
    fn default() -> Self {
        Self {
            default_level: "BACKWARD".to_string(),
            allow_overrides: true,
            subjects: BTreeMap::new(),
        }
    }
}

/// Schema format support configuration
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaFormatSpec {
    /// Enable Avro schema support
    #[serde(default = "default_true")]
    pub avro: bool,

    /// Enable JSON Schema support
    #[serde(default = "default_true")]
    pub json_schema: bool,

    /// Enable Protobuf schema support
    #[serde(default = "default_true")]
    pub protobuf: bool,

    /// Strict schema validation
    #[serde(default = "default_true")]
    pub strict_validation: bool,
}

impl Default for SchemaFormatSpec {
    fn default() -> Self {
        Self {
            avro: true,
            json_schema: true,
            protobuf: true,
            strict_validation: true,
        }
    }
}

/// Schema contexts (multi-tenant) configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaContextsSpec {
    /// Enable schema contexts for multi-tenant isolation
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of contexts (0 = unlimited)
    #[serde(default)]
    #[validate(range(min = 0, max = 10000, message = "max contexts must be 0-10000"))]
    pub max_contexts: i32,

    /// Pre-defined contexts to create on startup
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 pre-defined contexts"))]
    pub predefined: Vec<SchemaContextDefinition>,
}

/// Pre-defined schema context definition
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaContextDefinition {
    /// Context name
    #[validate(length(min = 1, max = 128, message = "context name must be 1-128 characters"))]
    pub name: String,

    /// Context description
    #[serde(default)]
    #[validate(length(max = 512, message = "description max 512 characters"))]
    pub description: Option<String>,

    /// Whether this context is active
    #[serde(default = "default_true")]
    pub active: bool,
}

/// Validation rules configuration for Schema Registry
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaValidationSpec {
    /// Enable content validation rules
    #[serde(default)]
    pub enabled: bool,

    /// Maximum schema size in bytes
    #[serde(default = "default_max_schema_size")]
    #[validate(range(
        min = 1024,
        max = 10485760,
        message = "max schema size must be 1KB-10MB"
    ))]
    pub max_schema_size: i64,

    /// Pre-defined validation rules
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 validation rules"))]
    pub rules: Vec<SchemaValidationRule>,
}

fn default_max_schema_size() -> i64 {
    1_048_576 // 1MB
}

impl Default for SchemaValidationSpec {
    fn default() -> Self {
        Self {
            enabled: false,
            max_schema_size: 1_048_576,
            rules: vec![],
        }
    }
}

/// Validation rule definition
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaValidationRule {
    /// Rule name
    #[validate(length(min = 1, max = 128, message = "rule name must be 1-128 characters"))]
    pub name: String,

    /// Rule type: regex, field_exists, field_type, naming_convention, documentation
    #[validate(custom(function = "validate_rule_type"))]
    pub rule_type: String,

    /// Rule configuration/pattern
    #[validate(length(min = 1, max = 4096, message = "pattern must be 1-4096 characters"))]
    pub pattern: String,

    /// Subjects to apply this rule to (empty = all)
    #[serde(default)]
    pub subjects: Vec<String>,

    /// Schema types to apply this rule to
    #[serde(default)]
    pub schema_types: Vec<String>,

    /// Validation level: error, warning, info
    #[serde(default = "default_validation_level")]
    #[validate(custom(function = "validate_validation_level"))]
    pub level: String,

    /// Rule description
    #[serde(default)]
    #[validate(length(max = 512, message = "description max 512 characters"))]
    pub description: Option<String>,
}

fn validate_rule_type(rule_type: &str) -> Result<(), ValidationError> {
    let valid_types = [
        "regex",
        "field_exists",
        "field_type",
        "naming_convention",
        "documentation",
    ];
    if valid_types.contains(&rule_type) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid_rule_type").with_message(
            format!(
                "'{}' is not valid. Must be one of: {:?}",
                rule_type, valid_types
            )
            .into(),
        ))
    }
}

fn default_validation_level() -> String {
    "error".to_string()
}

fn validate_validation_level(level: &str) -> Result<(), ValidationError> {
    match level {
        "error" | "warning" | "info" => Ok(()),
        _ => Err(ValidationError::new("invalid_validation_level")
            .with_message("validation level must be 'error', 'warning', or 'info'".into())),
    }
}

/// Authentication configuration for Schema Registry
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryAuthSpec {
    /// Enable authentication
    #[serde(default)]
    pub enabled: bool,

    /// Authentication method: basic, jwt, cedar
    #[serde(default)]
    #[validate(custom(function = "validate_auth_method"))]
    pub method: Option<String>,

    /// Secret containing authentication credentials
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub credentials_secret_ref: Option<String>,

    /// JWT/OIDC configuration (if method=jwt)
    #[serde(default)]
    #[validate(nested)]
    pub jwt: JwtAuthSpec,

    /// Basic auth users (for method=basic, max 100)
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 users"))]
    pub users: Vec<SchemaRegistryUser>,
}

fn validate_auth_method(method: &str) -> Result<(), ValidationError> {
    match method {
        "" | "basic" | "jwt" | "cedar" => Ok(()),
        _ => Err(ValidationError::new("invalid_auth_method")
            .with_message("auth method must be 'basic', 'jwt', or 'cedar'".into())),
    }
}

/// JWT/OIDC authentication configuration
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct JwtAuthSpec {
    /// OIDC issuer URL
    #[serde(default)]
    pub issuer_url: Option<String>,

    /// JWKS URL (for key validation)
    #[serde(default)]
    pub jwks_url: Option<String>,

    /// Required audience claim
    #[serde(default)]
    pub audience: Option<String>,

    /// Claim to use for username
    #[serde(default = "default_username_claim")]
    pub username_claim: String,

    /// Claim to use for roles
    #[serde(default = "default_roles_claim")]
    pub roles_claim: String,
}

fn default_username_claim() -> String {
    "sub".to_string()
}

fn default_roles_claim() -> String {
    "roles".to_string()
}

/// Schema Registry user definition (for basic auth)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryUser {
    /// Username
    #[validate(length(min = 1, max = 128, message = "username must be 1-128 characters"))]
    pub username: String,

    /// Secret key containing password
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub password_secret_key: Option<String>,

    /// User role: admin, writer, reader
    #[serde(default = "default_user_role")]
    #[validate(custom(function = "validate_user_role"))]
    pub role: String,

    /// Allowed subjects (empty = all, for reader/writer roles)
    #[serde(default)]
    pub allowed_subjects: Vec<String>,
}

fn default_user_role() -> String {
    "reader".to_string()
}

fn validate_user_role(role: &str) -> Result<(), ValidationError> {
    match role {
        "admin" | "writer" | "reader" => Ok(()),
        _ => Err(ValidationError::new("invalid_user_role")
            .with_message("role must be 'admin', 'writer', or 'reader'".into())),
    }
}

/// TLS configuration for Schema Registry
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryTlsSpec {
    /// Enable TLS for HTTP server
    #[serde(default)]
    pub enabled: bool,

    /// Secret containing TLS certificates
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub cert_secret_name: Option<String>,

    /// Enable mTLS (mutual TLS)
    #[serde(default)]
    pub mtls_enabled: bool,

    /// CA secret name for mTLS
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub ca_secret_name: Option<String>,

    /// Enable TLS for broker connection
    #[serde(default)]
    pub broker_tls_enabled: bool,

    /// Secret for broker TLS certificates
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub broker_cert_secret_name: Option<String>,
}

/// Metrics configuration for Schema Registry
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryMetricsSpec {
    /// Enable Prometheus metrics endpoint
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Metrics endpoint port
    #[serde(default = "default_schema_metrics_port")]
    #[validate(range(min = 1024, max = 65535, message = "port must be 1024-65535"))]
    pub port: i32,

    /// Metrics endpoint path
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Create ServiceMonitor for Prometheus Operator
    #[serde(default)]
    pub service_monitor_enabled: bool,

    /// ServiceMonitor scrape interval
    #[serde(default = "default_scrape_interval")]
    #[validate(custom(function = "validate_duration"))]
    pub scrape_interval: String,
}

fn default_schema_metrics_port() -> i32 {
    9090
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

impl Default for SchemaRegistryMetricsSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            path: "/metrics".to_string(),
            service_monitor_enabled: false,
            scrape_interval: "30s".to_string(),
        }
    }
}

/// External registry configuration for mirroring/sync
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct ExternalRegistrySpec {
    /// Enable external registry integration
    #[serde(default)]
    pub enabled: bool,

    /// External registry type: confluent, glue
    #[serde(default)]
    #[validate(custom(function = "validate_external_registry_type"))]
    pub registry_type: Option<String>,

    /// Confluent Schema Registry URL
    #[serde(default)]
    pub confluent_url: Option<String>,

    /// AWS Glue registry ARN
    #[serde(default)]
    pub glue_registry_arn: Option<String>,

    /// AWS region for Glue
    #[serde(default)]
    pub aws_region: Option<String>,

    /// Sync mode: mirror (read from external), push (write to external), bidirectional
    #[serde(default)]
    #[validate(custom(function = "validate_sync_mode"))]
    pub sync_mode: Option<String>,

    /// Subjects to sync (empty = all)
    #[serde(default)]
    pub sync_subjects: Vec<String>,

    /// Sync interval in seconds
    #[serde(default = "default_sync_interval")]
    #[validate(range(
        min = 10,
        max = 86400,
        message = "sync interval must be 10-86400 seconds"
    ))]
    pub sync_interval_seconds: i32,

    /// Credentials secret reference
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub credentials_secret_ref: Option<String>,
}

fn validate_external_registry_type(reg_type: &str) -> Result<(), ValidationError> {
    match reg_type {
        "" | "confluent" | "glue" => Ok(()),
        _ => Err(ValidationError::new("invalid_external_registry_type")
            .with_message("registry type must be 'confluent' or 'glue'".into())),
    }
}

fn validate_sync_mode(mode: &str) -> Result<(), ValidationError> {
    match mode {
        "" | "mirror" | "push" | "bidirectional" => Ok(()),
        _ => Err(ValidationError::new("invalid_sync_mode")
            .with_message("sync mode must be 'mirror', 'push', or 'bidirectional'".into())),
    }
}

fn default_sync_interval() -> i32 {
    300 // 5 minutes
}

/// Status of a RivvenSchemaRegistry resource
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RivvenSchemaRegistryStatus {
    /// Current phase of the registry
    #[serde(default)]
    pub phase: SchemaRegistryPhase,

    /// Total number of replicas
    pub replicas: i32,

    /// Number of ready replicas
    pub ready_replicas: i32,

    /// Number of schemas registered
    pub schemas_registered: i32,

    /// Number of subjects
    pub subjects_count: i32,

    /// Number of active contexts (if contexts enabled)
    pub contexts_count: i32,

    /// Current observed generation
    pub observed_generation: i64,

    /// Conditions describing registry state
    #[serde(default)]
    pub conditions: Vec<SchemaRegistryCondition>,

    /// Registry endpoints (URLs)
    #[serde(default)]
    pub endpoints: Vec<String>,

    /// Storage backend status
    pub storage_status: Option<String>,

    /// External registry sync status
    pub external_sync_status: Option<String>,

    /// Last successful external sync time
    pub last_sync_time: Option<String>,

    /// Last time the status was updated
    pub last_updated: Option<String>,

    /// Error message if any
    pub message: Option<String>,
}

/// Phase of the Schema Registry lifecycle
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum SchemaRegistryPhase {
    /// Registry is being created
    #[default]
    Pending,
    /// Registry is being provisioned
    Provisioning,
    /// Registry is running and healthy
    Running,
    /// Registry is updating
    Updating,
    /// Registry is in degraded state
    Degraded,
    /// Registry has failed
    Failed,
    /// Registry is being deleted
    Terminating,
}

/// Condition describing an aspect of Schema Registry state
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRegistryCondition {
    /// Type of condition (Ready, BrokerConnected, StorageHealthy, ExternalSyncHealthy)
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status of the condition (True, False, Unknown)
    pub status: String,

    /// Reason for the condition
    pub reason: Option<String>,

    /// Human-readable message
    pub message: Option<String>,

    /// Last transition time
    pub last_transition_time: Option<String>,
}

impl RivvenSchemaRegistrySpec {
    /// Get the full container image including version
    pub fn get_image(&self) -> String {
        if let Some(ref image) = self.image {
            image.clone()
        } else {
            format!("ghcr.io/hupe1980/rivven-schema:{}", self.version)
        }
    }

    /// Get labels for managed resources
    pub fn get_labels(&self, registry_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "rivven-schema-registry".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            registry_name.to_string(),
        );
        labels.insert(
            "app.kubernetes.io/component".to_string(),
            "schema-registry".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/managed-by".to_string(),
            "rivven-operator".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/version".to_string(),
            self.version.clone(),
        );
        labels
    }

    /// Get selector labels for managed resources
    pub fn get_selector_labels(&self, registry_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert(
            "app.kubernetes.io/name".to_string(),
            "rivven-schema-registry".to_string(),
        );
        labels.insert(
            "app.kubernetes.io/instance".to_string(),
            registry_name.to_string(),
        );
        labels
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_spec() {
        let spec = RivvenClusterSpec {
            replicas: 3,
            version: "0.0.1".to_string(),
            image: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            storage: StorageSpec::default(),
            resources: None,
            config: BrokerConfig::default(),
            tls: TlsSpec::default(),
            metrics: MetricsSpec::default(),
            affinity: None,
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            pod_disruption_budget: PdbSpec::default(),
            service_account: None,
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            security_context: None,
            container_security_context: None,
        };

        assert_eq!(spec.replicas, 3);
        assert_eq!(spec.get_image(), "ghcr.io/hupe1980/rivven:0.0.1");
    }

    #[test]
    fn test_get_labels() {
        let spec = RivvenClusterSpec {
            replicas: 3,
            version: "0.0.1".to_string(),
            image: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            storage: StorageSpec::default(),
            resources: None,
            config: BrokerConfig::default(),
            tls: TlsSpec::default(),
            metrics: MetricsSpec::default(),
            affinity: None,
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            pod_disruption_budget: PdbSpec::default(),
            service_account: None,
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            security_context: None,
            container_security_context: None,
        };

        let labels = spec.get_labels("my-cluster");
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"rivven".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_custom_image() {
        let spec = RivvenClusterSpec {
            replicas: 1,
            version: "0.0.1".to_string(),
            image: Some("my-registry/rivven:custom".to_string()),
            image_pull_policy: "Always".to_string(),
            image_pull_secrets: vec![],
            storage: StorageSpec::default(),
            resources: None,
            config: BrokerConfig::default(),
            tls: TlsSpec::default(),
            metrics: MetricsSpec::default(),
            affinity: None,
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            pod_disruption_budget: PdbSpec::default(),
            service_account: None,
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            security_context: None,
            container_security_context: None,
        };

        assert_eq!(spec.get_image(), "my-registry/rivven:custom");
    }

    #[test]
    fn test_cluster_phase_default() {
        let phase = ClusterPhase::default();
        assert_eq!(phase, ClusterPhase::Pending);
    }

    #[test]
    fn test_storage_spec_default() {
        let storage = StorageSpec::default();
        assert_eq!(storage.size, "10Gi");
        assert!(storage.storage_class_name.is_none());
    }

    #[test]
    fn test_broker_config_defaults() {
        let config = BrokerConfig::default();
        assert_eq!(config.default_partitions, 3);
        assert_eq!(config.default_replication_factor, 2);
        assert!(config.auto_create_topics);
    }

    #[test]
    fn test_probe_spec_defaults() {
        let probe = ProbeSpec::default();
        assert!(probe.enabled);
        assert_eq!(probe.initial_delay_seconds, 30);
        assert_eq!(probe.period_seconds, 10);
    }

    #[test]
    fn test_validate_quantity_valid() {
        assert!(validate_quantity("10Gi").is_ok());
        assert!(validate_quantity("100Mi").is_ok());
        assert!(validate_quantity("1Ti").is_ok());
        assert!(validate_quantity("500").is_ok());
        assert!(validate_quantity("1.5Gi").is_ok());
    }

    #[test]
    fn test_validate_quantity_invalid() {
        assert!(validate_quantity("10GB").is_err()); // Wrong suffix
        assert!(validate_quantity("abc").is_err()); // Not a number
        assert!(validate_quantity("-10Gi").is_err()); // Negative
        assert!(validate_quantity("").is_err()); // Empty
    }

    #[test]
    fn test_validate_k8s_name_valid() {
        assert!(validate_k8s_name("my-cluster").is_ok());
        assert!(validate_k8s_name("cluster123").is_ok());
        assert!(validate_k8s_name("a").is_ok());
    }

    #[test]
    fn test_validate_k8s_name_invalid() {
        assert!(validate_k8s_name("My-Cluster").is_err()); // Uppercase
        assert!(validate_k8s_name("-cluster").is_err()); // Starts with dash
        assert!(validate_k8s_name("cluster-").is_err()); // Ends with dash
        assert!(validate_k8s_name("cluster_name").is_err()); // Underscore
    }

    #[test]
    fn test_validate_compression_type() {
        assert!(validate_compression_type("lz4").is_ok());
        assert!(validate_compression_type("zstd").is_ok());
        assert!(validate_compression_type("none").is_ok());
        assert!(validate_compression_type("invalid").is_err());
    }

    #[test]
    fn test_validate_segment_size() {
        assert!(validate_segment_size(1_048_576).is_ok()); // 1MB - minimum
        assert!(validate_segment_size(10_737_418_240).is_ok()); // 10GB - maximum
        assert!(validate_segment_size(1_073_741_824).is_ok()); // 1GB - valid
        assert!(validate_segment_size(1_000).is_err()); // Too small
        assert!(validate_segment_size(20_000_000_000).is_err()); // Too large
    }

    #[test]
    fn test_validate_message_size() {
        assert!(validate_message_size(1_024).is_ok()); // 1KB - minimum
        assert!(validate_message_size(104_857_600).is_ok()); // 100MB - maximum
        assert!(validate_message_size(1_048_576).is_ok()); // 1MB - valid
        assert!(validate_message_size(100).is_err()); // Too small
        assert!(validate_message_size(200_000_000).is_err()); // Too large
    }

    #[test]
    fn test_validate_pull_policy() {
        assert!(validate_pull_policy("Always").is_ok());
        assert!(validate_pull_policy("IfNotPresent").is_ok());
        assert!(validate_pull_policy("Never").is_ok());
        assert!(validate_pull_policy("always").is_err()); // Wrong case
        assert!(validate_pull_policy("Invalid").is_err());
    }

    #[test]
    fn test_validate_duration() {
        assert!(validate_duration("30s").is_ok());
        assert!(validate_duration("1m").is_ok());
        assert!(validate_duration("5m30s").is_ok());
        assert!(validate_duration("1h").is_ok());
        assert!(validate_duration("invalid").is_err());
        assert!(validate_duration("30").is_err()); // Missing unit
    }

    #[test]
    fn test_validate_access_modes() {
        assert!(validate_access_modes(&["ReadWriteOnce".to_string()]).is_ok());
        assert!(
            validate_access_modes(&["ReadWriteOnce".to_string(), "ReadOnlyMany".to_string()])
                .is_ok()
        );
        assert!(validate_access_modes(&["Invalid".to_string()]).is_err());
    }

    // RivvenConnect CRD tests
    #[test]
    fn test_connect_spec_defaults() {
        let spec = RivvenConnectSpec {
            cluster_ref: ClusterReference {
                name: "my-cluster".to_string(),
                namespace: None,
            },
            replicas: 1,
            version: "0.0.1".to_string(),
            image: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            resources: None,
            config: ConnectConfigSpec::default(),
            sources: vec![],
            sinks: vec![],
            settings: GlobalConnectSettings::default(),
            tls: ConnectTlsSpec::default(),
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            affinity: None,
            service_account: None,
            security_context: None,
            container_security_context: None,
        };
        assert_eq!(spec.replicas, 1);
    }

    #[test]
    fn test_connect_phase_default() {
        let phase = ConnectPhase::default();
        assert_eq!(phase, ConnectPhase::Pending);
    }

    #[test]
    fn test_validate_connector_type() {
        assert!(validate_connector_type("postgres-cdc").is_ok());
        assert!(validate_connector_type("mysql-cdc").is_ok());
        assert!(validate_connector_type("http").is_ok());
        assert!(validate_connector_type("stdout").is_ok());
        assert!(validate_connector_type("s3").is_ok());
        assert!(validate_connector_type("datagen").is_ok());
        assert!(validate_connector_type("custom-connector").is_ok());
    }

    #[test]
    fn test_validate_start_offset() {
        assert!(validate_start_offset("earliest").is_ok());
        assert!(validate_start_offset("latest").is_ok());
        assert!(validate_start_offset("2024-01-01T00:00:00Z").is_ok());
        assert!(validate_start_offset("invalid").is_err());
    }

    #[test]
    fn test_validate_image_valid() {
        assert!(validate_image("nginx").is_ok());
        assert!(validate_image("nginx:latest").is_ok());
        assert!(validate_image("ghcr.io/hupe1980/rivven:0.0.1").is_ok());
        assert!(validate_image("my-registry.io:5000/image:tag").is_ok());
        assert!(validate_image("localhost:5000/myimage").is_ok());
        assert!(validate_image("").is_ok()); // Empty allowed, uses default
    }

    #[test]
    fn test_validate_image_invalid() {
        assert!(validate_image("/absolute/path").is_err()); // Starts with /
        assert!(validate_image("-invalid").is_err()); // Starts with -
        assert!(validate_image("image..path").is_err()); // Contains ..
                                                         // Very long image name
        let long_name = "a".repeat(300);
        assert!(validate_image(&long_name).is_err());
    }

    #[test]
    fn test_validate_node_selector() {
        let mut selectors = BTreeMap::new();
        selectors.insert("node-type".to_string(), "compute".to_string());
        assert!(validate_node_selector(&selectors).is_ok());

        // Too many selectors
        let mut many = BTreeMap::new();
        for i in 0..25 {
            many.insert(format!("key-{}", i), "value".to_string());
        }
        assert!(validate_node_selector(&many).is_err());
    }

    #[test]
    fn test_validate_annotations() {
        let mut annotations = BTreeMap::new();
        annotations.insert("prometheus.io/scrape".to_string(), "true".to_string());
        assert!(validate_annotations(&annotations).is_ok());

        // Too many annotations
        let mut many = BTreeMap::new();
        for i in 0..55 {
            many.insert(format!("annotation-{}", i), "value".to_string());
        }
        assert!(validate_annotations(&many).is_err());
    }

    #[test]
    fn test_validate_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("team".to_string(), "platform".to_string());
        assert!(validate_labels(&labels).is_ok());

        // Reserved prefix
        let mut reserved = BTreeMap::new();
        reserved.insert("app.kubernetes.io/custom".to_string(), "value".to_string());
        assert!(validate_labels(&reserved).is_err());
    }

    #[test]
    fn test_validate_raw_config() {
        let mut config = BTreeMap::new();
        config.insert("custom.setting".to_string(), "value".to_string());
        assert!(validate_raw_config(&config).is_ok());

        // Forbidden key
        let mut forbidden = BTreeMap::new();
        forbidden.insert("command".to_string(), "/bin/sh".to_string());
        assert!(validate_raw_config(&forbidden).is_err());

        // Too many entries
        let mut many = BTreeMap::new();
        for i in 0..55 {
            many.insert(format!("config-{}", i), "value".to_string());
        }
        assert!(validate_raw_config(&many).is_err());
    }

    #[test]
    fn test_validate_int_or_percent() {
        assert!(validate_optional_int_or_percent("1").is_ok());
        assert!(validate_optional_int_or_percent("25%").is_ok());
        assert!(validate_optional_int_or_percent("100%").is_ok());
        assert!(validate_optional_int_or_percent("").is_ok()); // Empty allowed
        assert!(validate_optional_int_or_percent("abc").is_err());
        assert!(validate_optional_int_or_percent("25%%").is_err());
    }

    #[test]
    fn test_tls_spec_default() {
        let tls = TlsSpec::default();
        assert!(!tls.enabled);
        assert!(tls.cert_secret_name.is_none());
        assert!(!tls.mtls_enabled);
    }

    #[test]
    fn test_metrics_spec_default() {
        let metrics = MetricsSpec::default();
        assert!(metrics.enabled);
        assert_eq!(metrics.port, 9090);
    }

    #[test]
    fn test_pdb_spec_default() {
        let pdb = PdbSpec::default();
        assert!(pdb.enabled);
        assert!(pdb.min_available.is_none());
        assert_eq!(pdb.max_unavailable, Some("1".to_string()));
    }

    #[test]
    fn test_service_monitor_labels() {
        let mut labels = BTreeMap::new();
        labels.insert("release".to_string(), "prometheus".to_string());
        assert!(validate_service_monitor_labels(&labels).is_ok());

        // Too many labels
        let mut many = BTreeMap::new();
        for i in 0..15 {
            many.insert(format!("label-{}", i), "value".to_string());
        }
        assert!(validate_service_monitor_labels(&many).is_err());
    }

    #[test]
    fn test_cluster_condition_time_format() {
        let condition = ClusterCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: Some("AllReplicasReady".to_string()),
            message: Some("All replicas are ready".to_string()),
        };
        assert!(condition.last_transition_time.unwrap().contains('T'));
    }

    // ========================================================================
    // RivvenSchemaRegistry CRD Tests
    // ========================================================================

    #[test]
    fn test_schema_registry_phase_default() {
        let phase = SchemaRegistryPhase::default();
        assert_eq!(phase, SchemaRegistryPhase::Pending);
    }

    #[test]
    fn test_schema_registry_server_spec_default() {
        let spec = SchemaRegistryServerSpec::default();
        assert_eq!(spec.port, 8081);
        assert_eq!(spec.bind_address, "0.0.0.0");
        assert_eq!(spec.timeout_seconds, 30);
        assert_eq!(spec.max_request_size, 10_485_760);
        assert!(!spec.cors_enabled);
    }

    #[test]
    fn test_schema_registry_storage_spec_default() {
        let spec = SchemaRegistryStorageSpec::default();
        assert_eq!(spec.mode, "broker");
        assert_eq!(spec.topic, "_schemas");
        assert_eq!(spec.replication_factor, 3);
        assert_eq!(spec.partitions, 1);
        assert!(spec.normalize);
    }

    #[test]
    fn test_validate_storage_mode() {
        assert!(validate_storage_mode("memory").is_ok());
        assert!(validate_storage_mode("broker").is_ok());
        assert!(validate_storage_mode("invalid").is_err());
    }

    #[test]
    fn test_schema_compatibility_spec_default() {
        let spec = SchemaCompatibilitySpec::default();
        assert_eq!(spec.default_level, "BACKWARD");
        assert!(spec.allow_overrides);
        assert!(spec.subjects.is_empty());
    }

    #[test]
    fn test_validate_compatibility_level() {
        assert!(validate_compatibility_level("BACKWARD").is_ok());
        assert!(validate_compatibility_level("BACKWARD_TRANSITIVE").is_ok());
        assert!(validate_compatibility_level("FORWARD").is_ok());
        assert!(validate_compatibility_level("FORWARD_TRANSITIVE").is_ok());
        assert!(validate_compatibility_level("FULL").is_ok());
        assert!(validate_compatibility_level("FULL_TRANSITIVE").is_ok());
        assert!(validate_compatibility_level("NONE").is_ok());
        assert!(validate_compatibility_level("invalid").is_err());
        assert!(validate_compatibility_level("backward").is_err()); // Case sensitive
    }

    #[test]
    fn test_schema_format_spec_default() {
        let spec = SchemaFormatSpec::default();
        assert!(spec.avro);
        assert!(spec.json_schema);
        assert!(spec.protobuf);
        assert!(spec.strict_validation);
    }

    #[test]
    fn test_schema_contexts_spec_default() {
        let spec = SchemaContextsSpec::default();
        assert!(!spec.enabled);
        assert_eq!(spec.max_contexts, 0);
        assert!(spec.predefined.is_empty());
    }

    #[test]
    fn test_schema_validation_spec_default() {
        let spec = SchemaValidationSpec::default();
        assert!(!spec.enabled);
        assert_eq!(spec.max_schema_size, 1_048_576);
        assert!(spec.rules.is_empty());
    }

    #[test]
    fn test_validate_rule_type() {
        assert!(validate_rule_type("regex").is_ok());
        assert!(validate_rule_type("field_exists").is_ok());
        assert!(validate_rule_type("field_type").is_ok());
        assert!(validate_rule_type("naming_convention").is_ok());
        assert!(validate_rule_type("documentation").is_ok());
        assert!(validate_rule_type("invalid").is_err());
    }

    #[test]
    fn test_validate_validation_level() {
        assert!(validate_validation_level("error").is_ok());
        assert!(validate_validation_level("warning").is_ok());
        assert!(validate_validation_level("info").is_ok());
        assert!(validate_validation_level("invalid").is_err());
    }

    #[test]
    fn test_schema_registry_auth_spec_default() {
        let spec = SchemaRegistryAuthSpec::default();
        assert!(!spec.enabled);
        assert!(spec.method.is_none());
        assert!(spec.credentials_secret_ref.is_none());
    }

    #[test]
    fn test_validate_auth_method() {
        assert!(validate_auth_method("basic").is_ok());
        assert!(validate_auth_method("jwt").is_ok());
        assert!(validate_auth_method("cedar").is_ok());
        assert!(validate_auth_method("").is_ok());
        assert!(validate_auth_method("invalid").is_err());
    }

    #[test]
    fn test_jwt_auth_spec_default() {
        let spec = JwtAuthSpec::default();
        assert!(spec.issuer_url.is_none());
        assert!(spec.jwks_url.is_none());
        // Note: Default::default() gives empty strings; serde defaults apply during deserialization
        assert!(spec.username_claim.is_empty());
        assert!(spec.roles_claim.is_empty());
    }

    #[test]
    fn test_validate_user_role() {
        assert!(validate_user_role("admin").is_ok());
        assert!(validate_user_role("writer").is_ok());
        assert!(validate_user_role("reader").is_ok());
        assert!(validate_user_role("invalid").is_err());
    }

    #[test]
    fn test_schema_registry_tls_spec_default() {
        let spec = SchemaRegistryTlsSpec::default();
        assert!(!spec.enabled);
        assert!(spec.cert_secret_name.is_none());
        assert!(!spec.mtls_enabled);
        assert!(!spec.broker_tls_enabled);
    }

    #[test]
    fn test_schema_registry_metrics_spec_default() {
        let spec = SchemaRegistryMetricsSpec::default();
        assert!(spec.enabled);
        assert_eq!(spec.port, 9090);
        assert_eq!(spec.path, "/metrics");
        assert!(!spec.service_monitor_enabled);
        assert_eq!(spec.scrape_interval, "30s");
    }

    #[test]
    fn test_external_registry_spec_default() {
        let spec = ExternalRegistrySpec::default();
        assert!(!spec.enabled);
        assert!(spec.registry_type.is_none());
        assert!(spec.confluent_url.is_none());
        assert!(spec.glue_registry_arn.is_none());
        // Note: Default::default() gives 0; serde default (300) applies during deserialization
        assert_eq!(spec.sync_interval_seconds, 0);
    }

    #[test]
    fn test_validate_external_registry_type() {
        assert!(validate_external_registry_type("confluent").is_ok());
        assert!(validate_external_registry_type("glue").is_ok());
        assert!(validate_external_registry_type("").is_ok());
        assert!(validate_external_registry_type("invalid").is_err());
    }

    #[test]
    fn test_validate_sync_mode() {
        assert!(validate_sync_mode("mirror").is_ok());
        assert!(validate_sync_mode("push").is_ok());
        assert!(validate_sync_mode("bidirectional").is_ok());
        assert!(validate_sync_mode("").is_ok());
        assert!(validate_sync_mode("invalid").is_err());
    }

    #[test]
    fn test_schema_registry_spec_get_image_default() {
        let spec = RivvenSchemaRegistrySpec {
            cluster_ref: ClusterReference {
                name: "test".to_string(),
                namespace: None,
            },
            replicas: 1,
            version: "0.0.1".to_string(),
            image: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            resources: None,
            server: SchemaRegistryServerSpec::default(),
            storage: SchemaRegistryStorageSpec::default(),
            compatibility: SchemaCompatibilitySpec::default(),
            schemas: SchemaFormatSpec::default(),
            contexts: SchemaContextsSpec::default(),
            validation: SchemaValidationSpec::default(),
            auth: SchemaRegistryAuthSpec::default(),
            tls: SchemaRegistryTlsSpec::default(),
            metrics: SchemaRegistryMetricsSpec::default(),
            external: ExternalRegistrySpec::default(),
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            affinity: None,
            service_account: None,
            security_context: None,
            container_security_context: None,
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            pod_disruption_budget: PdbSpec::default(),
        };
        assert_eq!(spec.get_image(), "ghcr.io/hupe1980/rivven-schema:0.0.1");
    }

    #[test]
    fn test_schema_registry_spec_get_image_custom() {
        let spec = RivvenSchemaRegistrySpec {
            cluster_ref: ClusterReference {
                name: "test".to_string(),
                namespace: None,
            },
            replicas: 1,
            version: "0.0.1".to_string(),
            image: Some("custom/schema-registry:latest".to_string()),
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            resources: None,
            server: SchemaRegistryServerSpec::default(),
            storage: SchemaRegistryStorageSpec::default(),
            compatibility: SchemaCompatibilitySpec::default(),
            schemas: SchemaFormatSpec::default(),
            contexts: SchemaContextsSpec::default(),
            validation: SchemaValidationSpec::default(),
            auth: SchemaRegistryAuthSpec::default(),
            tls: SchemaRegistryTlsSpec::default(),
            metrics: SchemaRegistryMetricsSpec::default(),
            external: ExternalRegistrySpec::default(),
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            affinity: None,
            service_account: None,
            security_context: None,
            container_security_context: None,
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            pod_disruption_budget: PdbSpec::default(),
        };
        assert_eq!(spec.get_image(), "custom/schema-registry:latest");
    }

    #[test]
    fn test_schema_registry_spec_get_labels() {
        let spec = RivvenSchemaRegistrySpec {
            cluster_ref: ClusterReference {
                name: "test".to_string(),
                namespace: None,
            },
            replicas: 1,
            version: "0.0.1".to_string(),
            image: None,
            image_pull_policy: "IfNotPresent".to_string(),
            image_pull_secrets: vec![],
            resources: None,
            server: SchemaRegistryServerSpec::default(),
            storage: SchemaRegistryStorageSpec::default(),
            compatibility: SchemaCompatibilitySpec::default(),
            schemas: SchemaFormatSpec::default(),
            contexts: SchemaContextsSpec::default(),
            validation: SchemaValidationSpec::default(),
            auth: SchemaRegistryAuthSpec::default(),
            tls: SchemaRegistryTlsSpec::default(),
            metrics: SchemaRegistryMetricsSpec::default(),
            external: ExternalRegistrySpec::default(),
            pod_annotations: BTreeMap::new(),
            pod_labels: BTreeMap::new(),
            env: vec![],
            node_selector: BTreeMap::new(),
            tolerations: vec![],
            affinity: None,
            service_account: None,
            security_context: None,
            container_security_context: None,
            liveness_probe: ProbeSpec::default(),
            readiness_probe: ProbeSpec::default(),
            pod_disruption_budget: PdbSpec::default(),
        };

        let labels = spec.get_labels("my-registry");
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"rivven-schema-registry".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"my-registry".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"schema-registry".to_string())
        );
    }

    #[test]
    fn test_schema_registry_condition_time_format() {
        let condition = SchemaRegistryCondition {
            condition_type: "Ready".to_string(),
            status: "True".to_string(),
            last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
            reason: Some("AllReplicasReady".to_string()),
            message: Some("All replicas are ready".to_string()),
        };
        assert!(condition.last_transition_time.unwrap().contains('T'));
    }

    #[test]
    fn test_validate_subject_compatibility_map() {
        let mut subjects = BTreeMap::new();
        subjects.insert("orders-value".to_string(), "BACKWARD".to_string());
        subjects.insert("users-value".to_string(), "FULL".to_string());
        assert!(validate_subject_compatibility_map(&subjects).is_ok());

        // Invalid compatibility level
        let mut invalid = BTreeMap::new();
        invalid.insert("test".to_string(), "invalid".to_string());
        assert!(validate_subject_compatibility_map(&invalid).is_err());

        // Subject name too long
        let mut long_name = BTreeMap::new();
        long_name.insert("a".repeat(300), "BACKWARD".to_string());
        assert!(validate_subject_compatibility_map(&long_name).is_err());
    }

    // ========================================================================
    // Advanced CDC Configuration Tests
    // ========================================================================

    #[test]
    fn test_snapshot_cdc_config_spec_serde_defaults() {
        // Test that serde deserialization applies default functions
        let json = r#"{}"#;
        let config: SnapshotCdcConfigSpec = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.parallel_tables, 4);
        assert_eq!(config.query_timeout_secs, 300);
        assert_eq!(config.throttle_delay_ms, 0);
        assert_eq!(config.max_retries, 3);
        assert!(config.include_tables.is_empty());
        assert!(config.exclude_tables.is_empty());
    }

    #[test]
    fn test_snapshot_cdc_config_spec_validation() {
        let json = r#"{"batchSize": 50}"#;
        let config: SnapshotCdcConfigSpec = serde_json::from_str(json).unwrap();
        assert!(config.validate().is_err());

        let json = r#"{"batchSize": 5000, "parallelTables": 50}"#;
        let config: SnapshotCdcConfigSpec = serde_json::from_str(json).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_incremental_snapshot_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: IncrementalSnapshotSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.chunk_size, 1024);
        assert!(config.watermark_strategy.is_empty());
        assert_eq!(config.max_concurrent_chunks, 1);
    }

    #[test]
    fn test_validate_watermark_strategy() {
        assert!(validate_watermark_strategy("insert").is_ok());
        assert!(validate_watermark_strategy("update_and_insert").is_ok());
        assert!(validate_watermark_strategy("").is_ok());
        assert!(validate_watermark_strategy("invalid").is_err());
    }

    #[test]
    fn test_heartbeat_cdc_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: HeartbeatCdcSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.interval_secs, 10);
        assert_eq!(config.max_lag_secs, 300);
        assert!(!config.emit_events);
    }

    #[test]
    fn test_deduplication_cdc_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: DeduplicationCdcSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.bloom_expected_insertions, 100_000);
        assert_eq!(config.bloom_fpp, 0.01);
        assert_eq!(config.lru_size, 10_000);
        assert_eq!(config.window_secs, 3600);
    }

    #[test]
    fn test_transaction_topic_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: TransactionTopicSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.topic_name.is_none());
        assert!(config.include_data_collections);
        assert_eq!(config.min_events_threshold, 0);
    }

    #[test]
    fn test_schema_change_topic_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: SchemaChangeTopicSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.topic_name.is_none());
        assert!(config.include_columns);
        assert!(config.schemas.is_empty());
    }

    #[test]
    fn test_tombstone_cdc_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: TombstoneCdcSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.after_delete);
        assert!(config.behavior.is_empty());
    }

    #[test]
    fn test_validate_tombstone_behavior() {
        assert!(validate_tombstone_behavior("emit_null").is_ok());
        assert!(validate_tombstone_behavior("emit_with_key").is_ok());
        assert!(validate_tombstone_behavior("").is_ok());
        assert!(validate_tombstone_behavior("invalid").is_err());
    }

    #[test]
    fn test_field_encryption_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: FieldEncryptionSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.key_secret_ref.is_none());
        assert!(config.fields.is_empty());
        assert_eq!(config.algorithm, "aes-256-gcm");
    }

    #[test]
    fn test_read_only_replica_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: ReadOnlyReplicaSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.lag_threshold_ms, 5000);
        assert!(config.deduplicate);
        assert!(config.watermark_source.is_empty());
    }

    #[test]
    fn test_validate_watermark_source() {
        assert!(validate_watermark_source("primary").is_ok());
        assert!(validate_watermark_source("replica").is_ok());
        assert!(validate_watermark_source("").is_ok());
        assert!(validate_watermark_source("invalid").is_err());
    }

    #[test]
    fn test_event_router_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: EventRouterSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.default_destination.is_none());
        assert!(config.dead_letter_queue.is_none());
        assert!(!config.drop_unroutable);
        assert!(config.rules.is_empty());
    }

    #[test]
    fn test_validate_route_condition_type() {
        assert!(validate_route_condition_type("always").is_ok());
        assert!(validate_route_condition_type("table").is_ok());
        assert!(validate_route_condition_type("table_pattern").is_ok());
        assert!(validate_route_condition_type("schema").is_ok());
        assert!(validate_route_condition_type("operation").is_ok());
        assert!(validate_route_condition_type("field_equals").is_ok());
        assert!(validate_route_condition_type("field_exists").is_ok());
        assert!(validate_route_condition_type("invalid").is_err());
    }

    #[test]
    fn test_partitioner_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: PartitionerSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.num_partitions, 16);
        assert_eq!(config.strategy, "key_hash");
        assert!(config.key_columns.is_empty());
    }

    #[test]
    fn test_validate_partition_strategy() {
        assert!(validate_partition_strategy("round_robin").is_ok());
        assert!(validate_partition_strategy("key_hash").is_ok());
        assert!(validate_partition_strategy("table_hash").is_ok());
        assert!(validate_partition_strategy("full_table_hash").is_ok());
        assert!(validate_partition_strategy("sticky").is_ok());
        assert!(validate_partition_strategy("invalid").is_err());
    }

    #[test]
    fn test_validate_smt_transform_type() {
        assert!(validate_smt_transform_type("extract_new_record_state").is_ok());
        assert!(validate_smt_transform_type("mask_field").is_ok());
        assert!(validate_smt_transform_type("filter").is_ok());
        assert!(validate_smt_transform_type("flatten").is_ok());
        assert!(validate_smt_transform_type("cast").is_ok());
        assert!(validate_smt_transform_type("regex_router").is_ok());
        assert!(validate_smt_transform_type("content_router").is_ok());
        assert!(validate_smt_transform_type("invalid").is_err());
    }

    #[test]
    fn test_parallel_cdc_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: ParallelCdcSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.per_table_buffer, 1000);
        assert_eq!(config.output_buffer, 10_000);
        assert!(config.work_stealing);
        assert!(config.per_table_rate_limit.is_none());
        assert_eq!(config.shutdown_timeout_secs, 30);
    }

    #[test]
    fn test_outbox_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: OutboxSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.table_name, "outbox");
        assert_eq!(config.poll_interval_ms, 100);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.delivery_timeout_secs, 30);
        assert!(config.ordered_delivery);
        assert_eq!(config.retention_secs, 86400);
        assert_eq!(config.max_concurrency, 10);
    }

    #[test]
    fn test_health_monitor_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: HealthMonitorSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert_eq!(config.check_interval_secs, 10);
        assert_eq!(config.max_lag_ms, 30_000);
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.success_threshold, 2);
        assert_eq!(config.check_timeout_secs, 5);
        assert!(config.auto_recovery);
        assert_eq!(config.recovery_delay_secs, 1);
        assert_eq!(config.max_recovery_delay_secs, 60);
    }

    #[test]
    fn test_signal_table_spec_serde_defaults() {
        let json = r#"{}"#;
        let config: SignalTableSpec = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.data_collection.is_none());
        assert!(config.topic.is_none());
        assert!(config.enabled_channels.is_empty());
        assert_eq!(config.poll_interval_ms, 1000);
    }
}
