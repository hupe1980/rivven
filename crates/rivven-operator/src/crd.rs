//! Custom Resource Definitions for Rivven Kubernetes Operator
//!
//! This module defines the `RivvenCluster` CRD that represents a Rivven
//! distributed streaming cluster in Kubernetes.

use k8s_openapi::api::core::v1::ResourceRequirements;
use kube::CustomResource;
use once_cell::sync::Lazy;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use validator::{Validate, ValidationError};

/// Regex for validating Kubernetes resource quantities (e.g., "10Gi", "100Mi")
static QUANTITY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[0-9]+(\.[0-9]+)?(Ki|Mi|Gi|Ti|Pi|Ei|k|M|G|T|P|E)?$").unwrap()
});

/// Regex for validating Kubernetes names (RFC 1123 subdomain)
static NAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$").unwrap());

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
        return Err(ValidationError::new("name_too_long")
            .with_message("name exceeds 63 characters".into()));
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
        return Err(ValidationError::new("too_many_env_vars")
            .with_message(format!("maximum {} environment variables allowed", MAX_ENV_VARS).into()));
    }
    for var in vars {
        // Validate env var name format
        if var.name.is_empty() || var.name.len() > 256 {
            return Err(ValidationError::new("invalid_env_name")
                .with_message("environment variable name must be 1-256 characters".into()));
        }
        // Check for dangerous env var names that could override security settings
        let forbidden_prefixes = ["LD_", "DYLD_", "PATH=", "HOME=", "USER="];
        for prefix in forbidden_prefixes {
            if var.name.starts_with(prefix) && var.value.is_some() {
                return Err(ValidationError::new("forbidden_env_var").with_message(
                    format!("environment variable '{}' is not allowed for security", var.name)
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
    group = "rivven.io",
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
    let valid_modes = ["ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany", "ReadWriteOncePod"];
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
    #[validate(range(min = 1, max = 10, message = "replication factor must be between 1 and 10"))]
    pub default_replication_factor: i32,

    /// Log retention period in hours (1-8760, i.e., 1 hour to 1 year)
    #[serde(default = "default_log_retention_hours")]
    #[validate(range(min = 1, max = 8760, message = "retention hours must be between 1 and 8760"))]
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
    static DURATION_REGEX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^([0-9]+[smh])+$").unwrap());
    if !DURATION_REGEX.is_match(duration) {
        return Err(ValidationError::new("invalid_duration")
            .with_message(format!("'{}' is not a valid duration (e.g., 30s, 1m)", duration).into()));
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
    static INT_OR_PERCENT_REGEX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^([0-9]+|[0-9]+%)$").unwrap());
    if !INT_OR_PERCENT_REGEX.is_match(value) {
        return Err(ValidationError::new("invalid_int_or_percent").with_message(
            format!("'{}' must be an integer or percentage (e.g., '1' or '25%')", value).into(),
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
    "0.1.0".to_string()
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
// Note: The RivvenConnect CRD is defined here but the controller is not yet
// implemented. The #[allow(dead_code)] attributes are temporary until the
// connect_controller module is added.

/// RivvenConnect custom resource for managing connectors
///
/// This CRD allows declarative management of Rivven Connect pipelines,
/// including source connectors (CDC, HTTP, etc.) and sink connectors
/// (S3, stdout, HTTP webhooks, etc.).
#[allow(dead_code)]
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[kube(
    group = "rivven.io",
    version = "v1alpha1",
    kind = "RivvenConnect",
    plural = "rivvenconnects",
    shortname = "rc",
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
#[allow(dead_code)]
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

/// Global connect configuration
#[allow(dead_code)]
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

#[allow(dead_code)]
fn default_state_dir() -> String {
    "/data/connect-state".to_string()
}

#[allow(dead_code)]
fn default_log_level() -> String {
    "info".to_string()
}

#[allow(dead_code)]
fn validate_log_level(level: &str) -> Result<(), ValidationError> {
    match level {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(()),
        _ => Err(ValidationError::new("invalid_log_level")
            .with_message("log level must be one of: trace, debug, info, warn, error".into())),
    }
}

/// Source connector specification
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SourceConnectorSpec {
    /// Unique name for this source connector
    #[validate(length(min = 1, max = 63, message = "name must be 1-63 characters"))]
    #[validate(custom(function = "validate_k8s_name"))]
    pub name: String,

    /// Connector type (postgres-cdc, mysql-cdc, http, datagen, etc.)
    #[validate(length(min = 1, max = 64, message = "connector type must be 1-64 characters"))]
    #[validate(custom(function = "validate_connector_type"))]
    pub connector: String,

    /// Target topic to publish events to
    #[validate(length(min = 1, max = 255, message = "topic must be 1-255 characters"))]
    pub topic: String,

    /// Topic routing pattern (supports {schema}, {table}, {database} placeholders)
    #[serde(default)]
    pub topic_routing: Option<String>,

    /// Whether this source is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Connector-specific configuration (stored in ConfigMap or Secret)
    #[serde(default)]
    pub config: serde_json::Value,

    /// Secret reference for sensitive configuration (passwords, keys)
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub config_secret_ref: Option<String>,

    /// Tables/streams to capture
    #[serde(default)]
    #[validate(length(max = 100, message = "maximum 100 tables per source"))]
    pub tables: Vec<TableSpec>,

    /// Topic configuration (partitions, replication)
    #[serde(default)]
    #[validate(nested)]
    pub topic_config: SourceTopicConfigSpec,
}

#[allow(dead_code)]
fn validate_connector_type(connector: &str) -> Result<(), ValidationError> {
    // Allow standard connectors and custom ones (must be alphanumeric with hyphens)
    static CONNECTOR_REGEX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$").unwrap());
    if !CONNECTOR_REGEX.is_match(connector) {
        return Err(ValidationError::new("invalid_connector_type")
            .with_message("connector type must be lowercase alphanumeric with hyphens".into()));
    }
    Ok(())
}

/// Table specification for CDC sources
#[allow(dead_code)]
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
}

/// Source topic configuration
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SourceTopicConfigSpec {
    /// Number of partitions for auto-created topics
    #[serde(default)]
    #[validate(range(min = 1, max = 1000, message = "partitions must be between 1 and 1000"))]
    pub partitions: Option<i32>,

    /// Replication factor for auto-created topics
    #[serde(default)]
    #[validate(range(min = 1, max = 10, message = "replication factor must be between 1 and 10"))]
    pub replication_factor: Option<i32>,

    /// Auto-create topics if they don't exist
    #[serde(default)]
    pub auto_create: Option<bool>,
}

/// Sink connector specification
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct SinkConnectorSpec {
    /// Unique name for this sink connector
    #[validate(length(min = 1, max = 63, message = "name must be 1-63 characters"))]
    #[validate(custom(function = "validate_k8s_name"))]
    pub name: String,

    /// Connector type (stdout, s3, http, elasticsearch, etc.)
    #[validate(length(min = 1, max = 64, message = "connector type must be 1-64 characters"))]
    #[validate(custom(function = "validate_connector_type"))]
    pub connector: String,

    /// Topics to consume from (supports wildcards like "cdc.*")
    #[validate(length(min = 1, max = 100, message = "must have 1-100 topics"))]
    pub topics: Vec<String>,

    /// Consumer group for offset tracking
    #[validate(length(min = 1, max = 128, message = "consumer group must be 1-128 characters"))]
    pub consumer_group: String,

    /// Whether this sink is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Starting offset (earliest, latest, or timestamp)
    #[serde(default = "default_start_offset")]
    #[validate(custom(function = "validate_start_offset"))]
    pub start_offset: String,

    /// Connector-specific configuration
    #[serde(default)]
    pub config: serde_json::Value,

    /// Secret reference for sensitive configuration
    #[serde(default)]
    #[validate(custom(function = "validate_optional_k8s_name"))]
    pub config_secret_ref: Option<String>,

    /// Rate limiting configuration
    #[serde(default)]
    #[validate(nested)]
    pub rate_limit: RateLimitSpec,
}

#[allow(dead_code)]
fn default_start_offset() -> String {
    "latest".to_string()
}

#[allow(dead_code)]
fn validate_start_offset(offset: &str) -> Result<(), ValidationError> {
    match offset {
        "earliest" | "latest" => Ok(()),
        s if s.contains('T') && s.contains(':') => Ok(()), // ISO 8601 timestamp
        _ => Err(ValidationError::new("invalid_start_offset")
            .with_message("start offset must be 'earliest', 'latest', or ISO 8601 timestamp".into())),
    }
}

/// Rate limiting configuration for sinks
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "camelCase")]
pub struct RateLimitSpec {
    /// Maximum events per second (0 = unlimited)
    #[serde(default)]
    #[validate(range(min = 0, max = 1_000_000, message = "events per second must be 0-1000000"))]
    pub events_per_second: u64,

    /// Burst capacity (extra events above steady rate)
    #[serde(default)]
    #[validate(range(min = 0, max = 100_000, message = "burst capacity must be 0-100000"))]
    pub burst_capacity: Option<u64>,
}

/// Global settings for all connectors
#[allow(dead_code)]
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
#[allow(dead_code)]
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
    #[validate(range(min = 1, max = 10, message = "replication factor must be between 1 and 10"))]
    pub default_replication_factor: i32,

    /// Fail if topic doesn't exist and auto_create is false
    #[serde(default = "default_true")]
    pub require_topic_exists: bool,
}

#[allow(dead_code)]
fn default_topic_partitions() -> i32 {
    1
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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
    #[validate(range(min = 100, max = 3600000, message = "max backoff must be 100-3600000ms"))]
    pub max_backoff_ms: i64,

    /// Backoff multiplier
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

#[allow(dead_code)]
fn default_max_retries() -> i32 {
    10
}

#[allow(dead_code)]
fn default_initial_backoff_ms() -> i64 {
    100
}

#[allow(dead_code)]
fn default_max_backoff_ms() -> i64 {
    30000
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
fn default_health_port() -> i32 {
    8080
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
fn default_connect_replicas() -> i32 {
    1
}

/// Status of a RivvenConnect resource
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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
        labels.insert("app.kubernetes.io/name".to_string(), "rivven-connect".to_string());
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
        labels.insert("app.kubernetes.io/name".to_string(), "rivven-connect".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_spec() {
        let spec = RivvenClusterSpec {
            replicas: 3,
            version: "0.1.0".to_string(),
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
        assert_eq!(spec.get_image(), "ghcr.io/hupe1980/rivven:0.1.0");
    }

    #[test]
    fn test_get_labels() {
        let spec = RivvenClusterSpec {
            replicas: 3,
            version: "0.1.0".to_string(),
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
        assert_eq!(labels.get("app.kubernetes.io/name"), Some(&"rivven".to_string()));
        assert_eq!(labels.get("app.kubernetes.io/instance"), Some(&"my-cluster".to_string()));
    }

    #[test]
    fn test_custom_image() {
        let spec = RivvenClusterSpec {
            replicas: 1,
            version: "0.1.0".to_string(),
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
        assert!(validate_access_modes(&[
            "ReadWriteOnce".to_string(),
            "ReadOnlyMany".to_string()
        ])
        .is_ok());
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
            version: "0.1.0".to_string(),
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
}
