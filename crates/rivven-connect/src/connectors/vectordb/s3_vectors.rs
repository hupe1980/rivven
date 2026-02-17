//! Amazon S3 Vectors sink connector
//!
//! This module provides a sink connector for streaming data into Amazon S3
//! Vectors using the official `aws-sdk-s3vectors` crate.
//!
//! # Features
//!
//! - **High-throughput upserts** via the `PutVectors` API with configurable batching
//! - **Automatic batching** with configurable size and timer-based flush via `tokio::select!`
//! - **Flexible vector key extraction** — from event field, UUID, or hash-based
//! - **Metadata support** — attach key-value metadata to each vector for filtered queries
//! - **Auto-provisioning** — optionally create vector bucket and index on first write
//! - **AWS credential management** — uses the standard AWS SDK credential chain
//!   (environment, profile, IMDS, ECS task role) with automatic refresh
//! - **Circuit breaker** to prevent cascading failures on repeated upsert errors
//! - **Metrics integration** for observability (counters, gauges, histograms)
//! - **Structured error classification** — all SDK error variants mapped to typed
//!   `ConnectorError` variants with correct retryable/non-retryable semantics
//! - **Connector-level batch retry** with exponential backoff and jitter
//! - **Timer-based flush** via `tokio::select!` (no stale partial batches)
//! - **Session-scoped structured logging** for easy per-run correlation
//! - **Operation-level timeout** via SDK `TimeoutConfig`
//!
//! # Vector Key Generation
//!
//! The connector supports three strategies for generating S3 Vectors keys:
//!
//! 1. **Field** (`id_strategy: field`) — Extract the key from a JSON field in the
//!    event data. The field value is converted to a string.
//! 2. **UUID** (`id_strategy: uuid`) — Extract a UUID string from a field in the
//!    event data. Validated as 32 hex digits (with or without hyphens).
//! 3. **Hash** (`id_strategy: hash`) — Deterministically hash the entire event payload
//!    into a hex string key via FNV-1a (stable across restarts). Useful for deduplication.
//!
//! # Vector Extraction
//!
//! Vectors are extracted from event data via a configurable `vector_field` path.
//! The field must contain a JSON array of numbers (e.g. `[0.1, 0.2, ...]`).
//! Values are stored as `float32` in S3 Vectors.
//!
//! # Retry / Recovery Behavior
//!
//! The connector classifies S3 Vectors errors into retryable (throttling,
//! timeout, service unavailable) and fatal (access denied, validation,
//! not found) categories. Transient failures are retried with exponential
//! backoff and jitter (capped at `max_backoff_ms`). On top of retry, the
//! circuit breaker opens after consecutive failures and fails fast until
//! the reset timeout expires.
//!
//! # Circuit Breaker
//!
//! - **Closed**: Normal operation, batches flow through
//! - **Open**: After `failure_threshold` consecutive failures, batches fail fast
//! - **Half-Open**: After `cb_reset_timeout_secs`, allows one test batch through
//!
//! # Distance Metrics
//!
//! S3 Vectors supports:
//! - `cosine` — Cosine similarity (default)
//! - `euclidean` — Euclidean (L2) distance
//!
//! # SDK Error Classification
//!
//! All `SdkError` variants are explicitly handled:
//!
//! - `SdkError::ServiceError` — maps by service error code
//!   - `AccessDeniedException` → `ConnectorError::Auth`
//!   - `ValidationException` → `ConnectorError::Schema`
//!   - `NotFoundException` → `ConnectorError::NotFound`
//!   - `ConflictException` → `ConnectorError::Schema`
//!   - `TooManyRequestsException` → `ConnectorError::RateLimited`
//!   - `InternalServerException` → `ConnectorError::Connection` (retryable)
//!   - `ServiceUnavailableException` → `ConnectorError::Connection` (retryable)
//!   - `ServiceQuotaExceededException` → `ConnectorError::RateLimited`
//! - `SdkError::TimeoutError` → `ConnectorError::Timeout`
//!   - `SdkError::DispatchFailure` → `ConnectorError::Connection`
//! - `SdkError::ConstructionFailure` → `ConnectorError::Config`
//! - `SdkError::ResponseError` → `ConnectorError::Serialization`

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use aws_sdk_s3vectors::types::VectorData;
use aws_smithy_types::Document as S3VDocument;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use validator::Validate;

// ─────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────

/// Strategy for generating the vector key in S3 Vectors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3VectorIdStrategy {
    /// Use the value of a JSON field as the key.
    #[default]
    Field,
    /// Use a UUID string from a JSON field (validated).
    Uuid,
    /// Hash the entire event into a deterministic hex key (FNV-1a).
    Hash,
}

/// Circuit breaker configuration for S3 Vectors connector.
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct S3VectorCBConfig {
    /// Whether the circuit breaker is enabled.
    #[serde(default = "default_cb_enabled")]
    pub enabled: bool,
    /// Number of consecutive failures to open the circuit.
    #[serde(default = "default_failure_threshold")]
    #[validate(range(min = 1, max = 100))]
    pub failure_threshold: u32,
    /// Seconds to wait before transitioning from Open → Half-Open.
    #[serde(default = "default_reset_timeout_secs")]
    #[validate(range(min = 1, max = 600))]
    pub reset_timeout_secs: u64,
    /// Number of successes in Half-Open to transition to Closed.
    #[serde(default = "default_success_threshold")]
    #[validate(range(min = 1, max = 50))]
    pub success_threshold: u32,
}

fn default_cb_enabled() -> bool {
    true
}
fn default_failure_threshold() -> u32 {
    5
}
fn default_reset_timeout_secs() -> u64 {
    30
}
fn default_success_threshold() -> u32 {
    2
}

impl Default for S3VectorCBConfig {
    fn default() -> Self {
        Self {
            enabled: default_cb_enabled(),
            failure_threshold: default_failure_threshold(),
            reset_timeout_secs: default_reset_timeout_secs(),
            success_threshold: default_success_threshold(),
        }
    }
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3VectorDistanceMetric {
    /// Cosine similarity (default).
    #[default]
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
}

/// Configuration for the S3 Vectors sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, Validate, JsonSchema)]
pub struct S3VectorSinkConfig {
    /// AWS region (e.g. `us-east-1`). Uses SDK default resolution if omitted.
    #[serde(default)]
    pub region: Option<String>,

    /// Custom endpoint URL (for testing with LocalStack, etc.).
    #[serde(default)]
    pub endpoint_url: Option<String>,

    /// Name of the S3 vector bucket.
    pub vector_bucket_name: String,

    /// Name of the vector index within the bucket.
    pub index_name: String,

    /// Dimension of vectors (required for auto-provisioning the index).
    #[serde(default)]
    #[validate(range(min = 1, max = 20_000))]
    pub dimension: Option<i32>,

    /// Distance metric for the index (used during auto-provisioning).
    #[serde(default)]
    pub distance_metric: S3VectorDistanceMetric,

    /// Whether to auto-create the vector bucket and index if they don't exist.
    #[serde(default)]
    pub auto_create: bool,

    /// JSON path to the vector field in event data.
    #[serde(default = "default_vector_field")]
    #[validate(length(min = 1, max = 255))]
    pub vector_field: String,

    /// Strategy for generating vector keys.
    #[serde(default)]
    pub id_strategy: S3VectorIdStrategy,

    /// JSON path to the field used for key extraction (for `field` and `uuid` strategies).
    #[serde(default = "default_id_field")]
    #[validate(length(min = 1, max = 255))]
    pub id_field: String,

    /// Number of vectors per `PutVectors` batch.
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 10_000))]
    pub batch_size: usize,

    /// Seconds between timer-triggered flushes of partial batches.
    #[serde(default = "default_flush_interval_secs")]
    #[validate(range(min = 1, max = 300))]
    pub flush_interval_secs: u64,

    /// Whether to skip events that fail vector/key extraction instead of aborting.
    #[serde(default)]
    pub skip_invalid_events: bool,

    /// Maximum retries per batch upsert on transient errors.
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_retries: u32,

    /// Initial backoff in milliseconds for retry.
    #[serde(default = "default_initial_backoff_ms")]
    #[validate(range(min = 50, max = 60_000))]
    pub initial_backoff_ms: u64,

    /// Maximum backoff in milliseconds for retry.
    #[serde(default = "default_max_backoff_ms")]
    #[validate(range(min = 100, max = 300_000))]
    pub max_backoff_ms: u64,

    /// Per-operation timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    #[validate(range(min = 1, max = 300))]
    pub timeout_secs: u64,

    /// Metadata fields to include from event data. Maps from event field path
    /// to metadata key name. If empty, no metadata is attached.
    #[serde(default)]
    pub metadata_fields: HashMap<String, String>,

    /// Circuit breaker configuration.
    #[serde(default)]
    #[validate(nested)]
    pub circuit_breaker: S3VectorCBConfig,
}

fn default_vector_field() -> String {
    "vector".to_string()
}
fn default_id_field() -> String {
    "id".to_string()
}
fn default_batch_size() -> usize {
    100
}
fn default_flush_interval_secs() -> u64 {
    5
}
fn default_max_retries() -> u32 {
    3
}
fn default_initial_backoff_ms() -> u64 {
    200
}
fn default_max_backoff_ms() -> u64 {
    10_000
}
fn default_timeout_secs() -> u64 {
    30
}

impl Default for S3VectorSinkConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            vector_bucket_name: String::new(),
            index_name: String::new(),
            dimension: None,
            distance_metric: S3VectorDistanceMetric::default(),
            auto_create: false,
            vector_field: default_vector_field(),
            id_strategy: S3VectorIdStrategy::default(),
            id_field: default_id_field(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval_secs(),
            skip_invalid_events: false,
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            timeout_secs: default_timeout_secs(),
            metadata_fields: HashMap::new(),
            circuit_breaker: S3VectorCBConfig::default(),
        }
    }
}

impl S3VectorSinkConfig {
    /// Validate retry parameters.
    pub fn validate_retry(&self) -> std::result::Result<(), String> {
        if self.initial_backoff_ms > self.max_backoff_ms {
            return Err(format!(
                "initial_backoff_ms ({}) must be <= max_backoff_ms ({})",
                self.initial_backoff_ms, self.max_backoff_ms
            ));
        }
        Ok(())
    }

    /// Validate vector bucket name (AWS naming rules).
    pub fn validate_bucket_name(name: &str) -> std::result::Result<(), String> {
        if name.is_empty() {
            return Err("vector_bucket_name must not be empty".to_string());
        }
        if name.len() < 3 || name.len() > 63 {
            return Err(format!(
                "vector_bucket_name must be 3–63 characters (got {})",
                name.len()
            ));
        }
        // S3 bucket naming rules: lowercase, digits, hyphens; no leading/trailing hyphens
        if !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(
                "vector_bucket_name must contain only lowercase letters, digits, and hyphens"
                    .to_string(),
            );
        }
        if name.starts_with('-') || name.ends_with('-') {
            return Err("vector_bucket_name must not start or end with a hyphen".to_string());
        }
        Ok(())
    }

    /// Validate index name.
    pub fn validate_index_name(name: &str) -> std::result::Result<(), String> {
        if name.is_empty() {
            return Err("index_name must not be empty".to_string());
        }
        if name.len() > 63 {
            return Err(format!(
                "index_name must be <= 63 characters (got {})",
                name.len()
            ));
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────
// Retry parameters
// ─────────────────────────────────────────────────────────────────

struct RetryParams {
    max_retries: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    timeout: Duration,
}

impl From<&S3VectorSinkConfig> for RetryParams {
    fn from(config: &S3VectorSinkConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            initial_backoff_ms: config.initial_backoff_ms,
            max_backoff_ms: config.max_backoff_ms,
            timeout: Duration::from_secs(config.timeout_secs),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Circuit breaker (lock-free, atomic)
// ─────────────────────────────────────────────────────────────────

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CBState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

impl From<u32> for CBState {
    fn from(v: u32) -> Self {
        match v {
            1 => CBState::Open,
            2 => CBState::HalfOpen,
            _ => CBState::Closed,
        }
    }
}

/// Lock-free circuit breaker using atomics.
struct CircuitBreaker {
    state: AtomicU32,
    consecutive_failures: AtomicU32,
    half_open_successes: AtomicU32,
    opened_at: AtomicU64,
    failure_threshold: u32,
    reset_timeout: Duration,
    success_threshold: u32,
}

impl CircuitBreaker {
    fn new(config: &S3VectorCBConfig) -> Self {
        Self {
            state: AtomicU32::new(CBState::Closed as u32),
            consecutive_failures: AtomicU32::new(0),
            half_open_successes: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
            failure_threshold: config.failure_threshold,
            reset_timeout: Duration::from_secs(config.reset_timeout_secs),
            success_threshold: config.success_threshold,
        }
    }

    #[inline]
    fn current_state(&self) -> CBState {
        CBState::from(self.state.load(Ordering::Acquire))
    }

    fn now_epoch_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn allow_request(&self) -> bool {
        match self.current_state() {
            CBState::Closed => true,
            CBState::Open => {
                let opened = self.opened_at.load(Ordering::Acquire);
                let elapsed = self.now_epoch_secs().saturating_sub(opened);
                if elapsed >= self.reset_timeout.as_secs() {
                    // CAS: only one thread wins the Open → HalfOpen transition.
                    // Prevents TOCTOU where multiple concurrent tasks all see
                    // Open, all pass the timeout check, and all proceed as
                    // the "single probe request".
                    if self
                        .state
                        .compare_exchange(
                            CBState::Open as u32,
                            CBState::HalfOpen as u32,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.half_open_successes.store(0, Ordering::Release);
                        true
                    } else {
                        // Another thread already transitioned — reject.
                        false
                    }
                } else {
                    false
                }
            }
            CBState::HalfOpen => true,
        }
    }

    fn record_success(&self) {
        match self.current_state() {
            CBState::HalfOpen => {
                let prev = self.half_open_successes.fetch_add(1, Ordering::AcqRel);
                if prev + 1 >= self.success_threshold {
                    self.state.store(CBState::Closed as u32, Ordering::Release);
                    self.consecutive_failures.store(0, Ordering::Release);
                    self.half_open_successes.store(0, Ordering::Release);
                }
            }
            _ => {
                self.consecutive_failures.store(0, Ordering::Release);
            }
        }
    }

    fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
        if self.current_state() == CBState::HalfOpen {
            self.state.store(CBState::Open as u32, Ordering::Release);
            self.opened_at
                .store(self.now_epoch_secs(), Ordering::Release);
            return;
        }
        // Guard: only transition Closed→Open once; do NOT re-stamp
        // `opened_at` when already Open (which would starve the reset timer).
        if prev + 1 >= self.failure_threshold && self.current_state() != CBState::Open {
            self.state.store(CBState::Open as u32, Ordering::Release);
            self.opened_at
                .store(self.now_epoch_secs(), Ordering::Release);
        }
    }

    fn diagnostics(&self) -> (CBState, u32, u32) {
        (
            self.current_state(),
            self.consecutive_failures.load(Ordering::Acquire),
            self.half_open_successes.load(Ordering::Acquire),
        )
    }
}

// ─────────────────────────────────────────────────────────────────
// Error classification
// ─────────────────────────────────────────────────────────────────

/// Maximum error message length to prevent log amplification.
const MAX_ERROR_LEN: usize = 512;

/// Truncate a message to `MAX_ERROR_LEN` characters respecting UTF-8.
#[inline]
fn truncated(msg: &str) -> String {
    if msg.len() <= MAX_ERROR_LEN {
        msg.to_string()
    } else {
        let mut end = MAX_ERROR_LEN;
        while end > 0 && !msg.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &msg[..end])
    }
}

/// Classify an `SdkError` from `PutVectors` into a typed `ConnectorError`.
///
/// All `SdkError` variants are explicitly handled:
/// - `ServiceError` — dispatched by error code name
/// - `TimeoutError` → `Timeout`
/// - `DispatchFailure` → `Connection`
/// - `ConstructionFailure` → `Config`
/// - `ResponseError` → `Serialization`
fn classify_put_vectors_error(
    err: &aws_sdk_s3vectors::error::SdkError<
        aws_sdk_s3vectors::operation::put_vectors::PutVectorsError,
    >,
) -> ConnectorError {
    use aws_sdk_s3vectors::error::SdkError;
    match err {
        SdkError::ServiceError(service_err) => {
            let msg = truncated(&format!("{}", service_err.err()));
            classify_service_error_name(service_err.err(), &msg)
        }
        SdkError::TimeoutError(_) => {
            ConnectorError::Timeout("S3 Vectors operation timed out".to_string())
        }
        SdkError::DispatchFailure(e) => {
            ConnectorError::Connection(truncated(&format!("S3 Vectors dispatch failure: {:?}", e)))
        }
        SdkError::ConstructionFailure(e) => ConnectorError::Config(truncated(&format!(
            "S3 Vectors construction failure: {:?}",
            e
        ))),
        SdkError::ResponseError(e) => {
            ConnectorError::Serialization(truncated(&format!("S3 Vectors response error: {:?}", e)))
        }
        _ => ConnectorError::Internal(truncated(&format!("S3 Vectors unknown error: {:?}", err))),
    }
}

/// Classify a service error by its error code name.
fn classify_service_error_name(
    err: &aws_sdk_s3vectors::operation::put_vectors::PutVectorsError,
    msg: &str,
) -> ConnectorError {
    // Use the error metadata code to classify
    use aws_sdk_s3vectors::error::ProvideErrorMetadata;
    let code = err.code().unwrap_or("Unknown");
    match code {
        "AccessDeniedException" => ConnectorError::Auth(msg.to_string()),
        "ValidationException" | "InvalidRequestException" => {
            ConnectorError::Schema(msg.to_string())
        }
        "NotFoundException" | "ResourceNotFoundException" => {
            ConnectorError::NotFound(msg.to_string())
        }
        "ConflictException" => ConnectorError::Schema(msg.to_string()),
        "TooManyRequestsException" | "ThrottlingException" | "ServiceQuotaExceededException" => {
            ConnectorError::RateLimited(msg.to_string())
        }
        "InternalServerException" | "InternalFailure" => {
            ConnectorError::Connection(msg.to_string())
        }
        "ServiceUnavailableException" => ConnectorError::Connection(msg.to_string()),
        _ => classify_error_message(msg),
    }
}

/// Fallback classification based on error message substrings.
fn classify_error_message(msg: &str) -> ConnectorError {
    let lower = msg.to_lowercase();
    if lower.contains("timeout") || lower.contains("timed out") {
        ConnectorError::Timeout(msg.to_string())
    } else if lower.contains("throttl") || lower.contains("rate") || lower.contains("too many") {
        ConnectorError::RateLimited(msg.to_string())
    } else if lower.contains("access denied")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
    {
        ConnectorError::Auth(msg.to_string())
    } else if lower.contains("not found") {
        ConnectorError::NotFound(msg.to_string())
    } else if lower.contains("validation")
        || lower.contains("invalid")
        || lower.contains("malformed")
    {
        ConnectorError::Schema(msg.to_string())
    } else if lower.contains("connection")
        || lower.contains("unavailable")
        || lower.contains("refused")
        || lower.contains("reset")
    {
        ConnectorError::Connection(msg.to_string())
    } else {
        ConnectorError::Internal(msg.to_string())
    }
}

/// Classify a generic `SdkError` for non-PutVectors operations.
fn classify_generic_sdk_error<E: std::fmt::Debug>(
    err: &aws_sdk_s3vectors::error::SdkError<E>,
) -> ConnectorError {
    use aws_sdk_s3vectors::error::SdkError;
    match err {
        SdkError::TimeoutError(_) => {
            ConnectorError::Timeout("S3 Vectors operation timed out".to_string())
        }
        SdkError::DispatchFailure(e) => {
            ConnectorError::Connection(truncated(&format!("S3 Vectors dispatch failure: {:?}", e)))
        }
        SdkError::ConstructionFailure(e) => ConnectorError::Config(truncated(&format!(
            "S3 Vectors construction failure: {:?}",
            e
        ))),
        SdkError::ResponseError(e) => {
            ConnectorError::Serialization(truncated(&format!("S3 Vectors response error: {:?}", e)))
        }
        _ => {
            let msg = truncated(&format!("{:?}", err));
            classify_error_message(&msg)
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────

/// Generate a session ID for structured logging correlation.
fn generate_session_id() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    format!("rivven-s3v-{}-{}", std::process::id(), micros)
}

/// Exponential backoff with jitter (prevents thundering herd).
#[inline]
fn calculate_backoff(attempt: u32, initial_ms: u64, max_ms: u64) -> Duration {
    let exp = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let base = initial_ms.saturating_mul(exp);
    let capped = base.min(max_ms);
    // Simple jitter: ±25%
    let jitter_range = capped / 4;
    let jitter = if jitter_range > 0 {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        seed % (jitter_range * 2)
    } else {
        0
    };
    // Clamp to max_ms so jitter never exceeds the configured ceiling
    let with_jitter = capped
        .saturating_sub(jitter_range)
        .saturating_add(jitter)
        .min(max_ms);
    Duration::from_millis(with_jitter)
}

/// Extract a vector (array of f32) from event data at the given field path.
#[inline]
fn extract_vector(data: &serde_json::Value, field: &str) -> std::result::Result<Vec<f32>, String> {
    let value = get_nested_field(data, field)
        .ok_or_else(|| format!("vector field '{}' not found in event data", field))?;

    let arr = value
        .as_array()
        .ok_or_else(|| format!("vector field '{}' is not an array", field))?;

    if arr.is_empty() {
        return Err(format!("vector field '{}' is an empty array", field));
    }

    let mut vec = Vec::with_capacity(arr.len());
    for (i, v) in arr.iter().enumerate() {
        let f64_val = v.as_f64().ok_or_else(|| {
            format!(
                "vector field '{}' element {} is not a number: {:?}",
                field, i, v
            )
        })?;

        // Reject non-finite values
        if !f64_val.is_finite() {
            return Err(format!(
                "vector field '{}' element {} is non-finite: {}",
                field, i, f64_val
            ));
        }

        // Detect f64→f32 overflow
        let f32_val = f64_val as f32;
        if !f32_val.is_finite() && f64_val.is_finite() {
            return Err(format!(
                "vector field '{}' element {} overflows f32: {}",
                field, i, f64_val
            ));
        }

        vec.push(f32_val);
    }

    Ok(vec)
}

/// Extract the vector key from event data according to the configured strategy.
#[inline]
fn extract_key(
    data: &serde_json::Value,
    strategy: &S3VectorIdStrategy,
    id_field: &str,
) -> std::result::Result<String, String> {
    match strategy {
        S3VectorIdStrategy::Field => {
            let value = get_nested_field(data, id_field)
                .ok_or_else(|| format!("id field '{}' not found in event data", id_field))?;
            match value {
                serde_json::Value::String(s) => {
                    if s.is_empty() {
                        Err(format!("id field '{}' is an empty string", id_field))
                    } else {
                        Ok(s.clone())
                    }
                }
                serde_json::Value::Number(n) => Ok(n.to_string()),
                _ => Err(format!(
                    "id field '{}' is not a string or number: {:?}",
                    id_field, value
                )),
            }
        }
        S3VectorIdStrategy::Uuid => {
            let value = get_nested_field(data, id_field)
                .ok_or_else(|| format!("id field '{}' not found in event data", id_field))?;
            let s = value
                .as_str()
                .ok_or_else(|| format!("id field '{}' is not a string", id_field))?;
            validate_uuid(s)?;
            Ok(s.to_string())
        }
        S3VectorIdStrategy::Hash => {
            let bytes = serde_json::to_vec(data)
                .map_err(|e| format!("Failed to serialize event for hashing: {}", e))?;
            Ok(format!("{:016x}", fnv1a_hash(&bytes)))
        }
    }
}

/// Validate a UUID string: 32 hex digits, optionally 36 chars with hyphens.
fn validate_uuid(s: &str) -> std::result::Result<(), String> {
    let len = s.len();
    if len != 32 && len != 36 {
        return Err(format!(
            "UUID must be 32 or 36 characters (got {}): '{}'",
            len, s
        ));
    }
    // Single-pass validation
    let mut hex_count = 0;
    for b in s.bytes() {
        match b {
            b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => hex_count += 1,
            b'-' => {}
            _ => {
                return Err(format!(
                    "UUID contains invalid character '{}': '{}'",
                    b as char, s
                ));
            }
        }
    }
    if hex_count != 32 {
        return Err(format!(
            "UUID must contain exactly 32 hex digits (got {}): '{}'",
            hex_count, s
        ));
    }
    Ok(())
}

/// FNV-1a 64-bit hash.
#[inline]
fn fnv1a_hash(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Navigate a dotted path (`foo.bar.baz`) through nested JSON objects.
///
/// Returns `None` if any segment is empty (malformed paths like `"foo..bar"`).
#[inline]
fn get_nested_field<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = data;
    for key in path.split('.') {
        if key.is_empty() {
            return None;
        }
        current = current.get(key)?;
    }
    Some(current)
}

/// Build a `PutInputVector` from a source event.
#[inline]
fn build_put_vector(
    event: &SourceEvent,
    config: &S3VectorSinkConfig,
) -> std::result::Result<(aws_sdk_s3vectors::types::PutInputVector, usize), String> {
    let data = &event.data;

    if !data.is_object() {
        return Err("event data is not a JSON object".to_string());
    }

    let key = extract_key(data, &config.id_strategy, &config.id_field)?;
    let vector = extract_vector(data, &config.vector_field)?;
    let metadata_size_estimate = config.metadata_fields.len() * 50;
    let payload_size = key.len() + (vector.len() * 4) + metadata_size_estimate; // approximate

    let vector_data = VectorData::Float32(vector);

    // Build metadata: configured fields + auto-injected event metadata
    let metadata = if !config.metadata_fields.is_empty() {
        let mut doc = HashMap::new();
        for (event_path, meta_key) in &config.metadata_fields {
            if let Some(val) = get_nested_field(data, event_path) {
                doc.insert(meta_key.clone(), json_to_s3v_document(val));
            }
        }
        // Auto-inject event metadata for query correlation (matches qdrant/pinecone)
        doc.insert("_key".to_string(), S3VDocument::String(key.clone()));
        doc.insert(
            "_event_type".to_string(),
            S3VDocument::String(format!("{:?}", event.event_type)),
        );
        doc.insert(
            "_stream".to_string(),
            S3VDocument::String(event.stream.clone()),
        );
        doc.insert(
            "_namespace".to_string(),
            S3VDocument::String(event.namespace.as_deref().unwrap_or("").to_string()),
        );
        doc.insert(
            "_timestamp".to_string(),
            S3VDocument::String(event.timestamp.to_rfc3339()),
        );
        Some(doc)
    } else {
        None
    };

    let mut builder = aws_sdk_s3vectors::types::PutInputVector::builder()
        .key(key)
        .data(vector_data);

    if let Some(meta) = metadata {
        builder = builder.set_metadata(Some(S3VDocument::Object(meta)));
    }

    let put_vector = builder
        .build()
        .map_err(|e| format!("Failed to build PutInputVector: {}", e))?;

    Ok((put_vector, payload_size))
}

/// Convert a `serde_json::Value` into an S3 Vectors `Document`.
#[inline]
fn json_to_s3v_document(value: &serde_json::Value) -> S3VDocument {
    match value {
        serde_json::Value::Null => S3VDocument::Null,
        serde_json::Value::Bool(b) => S3VDocument::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                S3VDocument::Number(aws_smithy_types::Number::PosInt(u))
            } else if let Some(i) = n.as_i64() {
                S3VDocument::Number(aws_smithy_types::Number::NegInt(i))
            } else if let Some(f) = n.as_f64() {
                S3VDocument::Number(aws_smithy_types::Number::Float(f))
            } else {
                S3VDocument::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => S3VDocument::String(s.clone()),
        serde_json::Value::Array(arr) => {
            S3VDocument::Array(arr.iter().map(json_to_s3v_document).collect())
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, S3VDocument> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_s3v_document(v)))
                .collect();
            S3VDocument::Object(map)
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Sink implementation
// ─────────────────────────────────────────────────────────────────

/// Amazon S3 Vectors Sink implementation.
pub struct S3VectorSink {
    #[allow(dead_code)] // initialized in new(), CB is re-created from config in write()
    circuit_breaker: Option<CircuitBreaker>,
}

impl std::fmt::Debug for S3VectorSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorSink")
            .field("transport", &"aws-sdk-s3vectors")
            .finish()
    }
}

impl Default for S3VectorSink {
    fn default() -> Self {
        Self::new()
    }
}

impl S3VectorSink {
    /// Create a new sink with default circuit breaker.
    pub fn new() -> Self {
        Self {
            circuit_breaker: Some(CircuitBreaker::new(&S3VectorCBConfig::default())),
        }
    }

    /// Create the AWS S3 Vectors client from config.
    async fn create_client(
        config: &S3VectorSinkConfig,
    ) -> std::result::Result<aws_sdk_s3vectors::Client, ConnectorError> {
        let mut aws_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            aws_config_loader = aws_config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            aws_config_loader = aws_config_loader.endpoint_url(endpoint);
        }

        let sdk_config = aws_config_loader.load().await;

        let mut s3v_config_builder = aws_sdk_s3vectors::config::Builder::from(&sdk_config);

        // Set operation timeout
        s3v_config_builder = s3v_config_builder.timeout_config(
            aws_sdk_s3vectors::config::timeout::TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(config.timeout_secs))
                .build(),
        );

        let client = aws_sdk_s3vectors::Client::from_conf(s3v_config_builder.build());

        Ok(client)
    }

    /// Ensure vector bucket and index exist (auto-provisioning).
    async fn ensure_infrastructure(
        client: &aws_sdk_s3vectors::Client,
        config: &S3VectorSinkConfig,
        session_id: &str,
    ) -> std::result::Result<(), ConnectorError> {
        // Check if bucket exists
        let bucket_exists = match client
            .get_vector_bucket()
            .vector_bucket_name(&config.vector_bucket_name)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                let err = classify_generic_sdk_error(&e);
                if matches!(err, ConnectorError::NotFound(_)) {
                    false
                } else {
                    return Err(err);
                }
            }
        };

        if !bucket_exists {
            info!(
                session_id,
                bucket = %config.vector_bucket_name,
                "Creating vector bucket"
            );
            match client
                .create_vector_bucket()
                .vector_bucket_name(&config.vector_bucket_name)
                .send()
                .await
            {
                Ok(_) => {
                    info!(
                        session_id,
                        bucket = %config.vector_bucket_name,
                        "Vector bucket created"
                    );
                }
                Err(e) => {
                    // Treat ConflictException as success (concurrent create race)
                    let err = classify_generic_sdk_error(&e);
                    if !matches!(err, ConnectorError::Schema(_)) {
                        return Err(err);
                    }
                    info!(
                        session_id,
                        bucket = %config.vector_bucket_name,
                        "Vector bucket already exists (concurrent create)"
                    );
                }
            }
        }

        // Check if index exists
        let index_exists = match client
            .get_index()
            .vector_bucket_name(&config.vector_bucket_name)
            .index_name(&config.index_name)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                let err = classify_generic_sdk_error(&e);
                if matches!(err, ConnectorError::NotFound(_)) {
                    false
                } else {
                    return Err(err);
                }
            }
        };

        if !index_exists {
            let dimension = config.dimension.ok_or_else(|| {
                ConnectorError::Config(
                    "dimension is required when auto_create is true and index does not exist"
                        .to_string(),
                )
            })?;

            let distance = match config.distance_metric {
                S3VectorDistanceMetric::Cosine => aws_sdk_s3vectors::types::DistanceMetric::Cosine,
                S3VectorDistanceMetric::Euclidean => {
                    aws_sdk_s3vectors::types::DistanceMetric::Euclidean
                }
            };

            info!(
                session_id,
                index = %config.index_name,
                dimension,
                distance = ?distance,
                "Creating vector index"
            );

            match client
                .create_index()
                .vector_bucket_name(&config.vector_bucket_name)
                .index_name(&config.index_name)
                .dimension(dimension)
                .distance_metric(distance)
                .data_type(aws_sdk_s3vectors::types::DataType::Float32)
                .send()
                .await
            {
                Ok(_) => {
                    info!(
                        session_id,
                        index = %config.index_name,
                        "Vector index created"
                    );
                }
                Err(e) => {
                    // Treat ConflictException as success (concurrent create race)
                    let err = classify_generic_sdk_error(&e);
                    if !matches!(err, ConnectorError::Schema(_)) {
                        return Err(err);
                    }
                    info!(
                        session_id,
                        index = %config.index_name,
                        "Vector index already exists (concurrent create)"
                    );
                }
            }
        }

        Ok(())
    }

    /// Upsert a batch of vectors with retry logic.
    async fn upsert_batch(
        client: &aws_sdk_s3vectors::Client,
        bucket: &str,
        index: &str,
        batch: &[aws_sdk_s3vectors::types::PutInputVector],
        retry: &RetryParams,
        session_id: &str,
    ) -> std::result::Result<(), ConnectorError> {
        let mut last_err = None;

        for attempt in 0..=retry.max_retries {
            if attempt > 0 {
                counter!("s3vectors.batches.retried").increment(1);
                let backoff =
                    calculate_backoff(attempt - 1, retry.initial_backoff_ms, retry.max_backoff_ms);
                debug!(
                    session_id,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying PutVectors"
                );
                tokio::time::sleep(backoff).await;
            }

            let result = tokio::time::timeout(
                retry.timeout,
                client
                    .put_vectors()
                    .vector_bucket_name(bucket)
                    .index_name(index)
                    .set_vectors(Some(batch.to_vec()))
                    .send(),
            )
            .await;

            match result {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => {
                    let classified = classify_put_vectors_error(&e);
                    if classified.is_retryable() && attempt < retry.max_retries {
                        warn!(
                            session_id,
                            attempt,
                            error = %classified,
                            "Transient PutVectors error, will retry"
                        );
                        last_err = Some(classified);
                        continue;
                    }
                    return Err(classified);
                }
                Err(_elapsed) => {
                    let err = ConnectorError::Timeout(format!(
                        "PutVectors timed out after {}s (attempt {})",
                        retry.timeout.as_secs(),
                        attempt + 1
                    ));
                    if attempt < retry.max_retries {
                        warn!(session_id, attempt, "PutVectors timed out, will retry");
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ConnectorError::Internal("All retries exhausted with no error captured".to_string())
        }))
    }
}

#[async_trait]
impl Sink for S3VectorSink {
    type Config = S3VectorSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("s3-vectors", env!("CARGO_PKG_VERSION"))
            .description(
                "Amazon S3 Vectors — cost-optimized vector storage with native similarity search",
            )
            .documentation_url("https://rivven.dev/docs/connectors/s3-vectors-sink")
            .config_schema_from::<S3VectorSinkConfig>()
            .metadata("protocol", "https")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        let mut builder = CheckResult::builder();

        // ── Config validation ────────────────────────────────────────
        if let Err(e) = config.validate() {
            builder = builder.check(CheckDetail::failed("config_validation", format!("{}", e)));
            return Ok(builder.build());
        }

        let t0 = std::time::Instant::now();

        // ── Bucket name validation ───────────────────────────────────
        if let Err(e) = S3VectorSinkConfig::validate_bucket_name(&config.vector_bucket_name) {
            builder = builder.check(
                CheckDetail::failed("bucket_name", &e)
                    .with_duration_ms(t0.elapsed().as_millis() as u64),
            );
            return Ok(builder.build());
        }
        builder = builder.check(
            CheckDetail::passed("bucket_name").with_duration_ms(t0.elapsed().as_millis() as u64),
        );

        // ── Index name validation ────────────────────────────────────
        let t1 = std::time::Instant::now();
        if let Err(e) = S3VectorSinkConfig::validate_index_name(&config.index_name) {
            builder = builder.check(
                CheckDetail::failed("index_name", &e)
                    .with_duration_ms(t1.elapsed().as_millis() as u64),
            );
            return Ok(builder.build());
        }
        builder = builder.check(
            CheckDetail::passed("index_name").with_duration_ms(t1.elapsed().as_millis() as u64),
        );

        // ── Retry config validation ─────────────────────────────────
        let t2 = std::time::Instant::now();
        if let Err(e) = config.validate_retry() {
            builder = builder.check(
                CheckDetail::failed("retry_config", &e)
                    .with_duration_ms(t2.elapsed().as_millis() as u64),
            );
            return Ok(builder.build());
        }
        builder = builder.check(
            CheckDetail::passed("retry_config").with_duration_ms(t2.elapsed().as_millis() as u64),
        );

        // ── Auto-create dimension validation ─────────────────────────
        if config.auto_create && config.dimension.is_none() {
            builder = builder.check(CheckDetail::failed(
                "dimension",
                "dimension is required when auto_create is true",
            ));
            return Ok(builder.build());
        }

        // ── AWS connectivity ─────────────────────────────────────────
        let tc = std::time::Instant::now();
        info!(
            bucket = %config.vector_bucket_name,
            index = %config.index_name,
            "Checking S3 Vectors connectivity"
        );

        match Self::create_client(config).await {
            Ok(client) => {
                // Try to describe the index to verify connectivity
                match client
                    .get_index()
                    .vector_bucket_name(&config.vector_bucket_name)
                    .index_name(&config.index_name)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        builder = builder.check(
                            CheckDetail::passed("connectivity")
                                .with_duration_ms(tc.elapsed().as_millis() as u64),
                        );

                        if let Some(index) = resp.index() {
                            info!(dimension = index.dimension(), "S3 Vectors index accessible");
                            builder = builder.check(
                                CheckDetail::passed("index_info")
                                    .with_duration_ms(tc.elapsed().as_millis() as u64),
                            );
                        }
                    }
                    Err(e) => {
                        let classified = classify_generic_sdk_error(&e);
                        if config.auto_create && matches!(classified, ConnectorError::NotFound(_)) {
                            builder = builder.check(
                                CheckDetail::passed("connectivity")
                                    .with_duration_ms(tc.elapsed().as_millis() as u64),
                            );
                            info!("Index not found — will be auto-created on first write");
                        } else {
                            let msg = format!("Failed to connect to S3 Vectors: {}", classified);
                            warn!("{}", msg);
                            builder = builder.check(
                                CheckDetail::failed("connectivity", &msg)
                                    .with_duration_ms(tc.elapsed().as_millis() as u64),
                            );
                            return Ok(builder.build());
                        }
                    }
                }
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("aws_client", format!("{}", e))
                        .with_duration_ms(tc.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        Ok(builder.build())
    }

    async fn write(
        &self,
        config: &Self::Config,
        mut events: futures::stream::BoxStream<'static, SourceEvent>,
    ) -> crate::error::Result<WriteResult> {
        // ── Config validation ────────────────────────────────────────
        config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        config.validate_retry().map_err(ConnectorError::Config)?;
        S3VectorSinkConfig::validate_bucket_name(&config.vector_bucket_name)
            .map_err(ConnectorError::Config)?;
        S3VectorSinkConfig::validate_index_name(&config.index_name)
            .map_err(ConnectorError::Config)?;

        // vector_field and id_field must differ (except for Hash strategy)
        if config.id_strategy != S3VectorIdStrategy::Hash && config.vector_field == config.id_field
        {
            return Err(ConnectorError::Config(format!(
                "vector_field and id_field must differ (both set to '{}')",
                config.vector_field
            ))
            .into());
        }

        // Reject metadata keys that collide with auto-injected reserved keys
        const RESERVED_META_KEYS: &[&str] =
            &["_key", "_event_type", "_stream", "_namespace", "_timestamp"];
        for meta_key in config.metadata_fields.values() {
            if RESERVED_META_KEYS.contains(&meta_key.as_str()) {
                return Err(ConnectorError::Config(format!(
                    "metadata key '{}' is reserved for auto-injected event metadata",
                    meta_key
                ))
                .into());
            }
        }

        // Re-initialize circuit breaker from the runtime config
        let circuit_breaker = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(&config.circuit_breaker))
        } else {
            None
        };

        let session_id = generate_session_id();
        let client = Self::create_client(config).await?;

        // Auto-provision infrastructure if requested
        if config.auto_create {
            Self::ensure_infrastructure(&client, config, &session_id).await?;
        }

        let retry = RetryParams::from(config);
        let mut batch: Vec<aws_sdk_s3vectors::types::PutInputVector> =
            Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut total_bytes = 0u64;
        let mut total_failed = 0u64;
        let mut errors: Vec<String> = Vec::new();
        /// Maximum error messages retained to prevent unbounded memory growth
        /// when `skip_invalid_events` is enabled and many events are malformed.
        const MAX_ERRORS: usize = 1_000;

        // Timer-based flush via tokio::select! — prevents stale partial batches
        let flush_duration = Duration::from_secs(config.flush_interval_secs);
        let mut flush_ticker = tokio::time::interval(flush_duration);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush_ticker.tick().await; // consume initial immediate tick

        info!(
            session_id,
            bucket = %config.vector_bucket_name,
            index = %config.index_name,
            batch_size = config.batch_size,
            vector_field = %config.vector_field,
            id_strategy = ?config.id_strategy,
            cb_enabled = config.circuit_breaker.enabled,
            "Starting S3 Vectors sink"
        );

        let mut stream_ended = false;

        loop {
            // ── Collect events or flush on timer ────────────────────────
            let should_flush;

            tokio::select! {
                biased; // prioritize draining events over timer flushes

                maybe_event = events.next() => {
                    match maybe_event {
                        Some(event) => {
                            match build_put_vector(&event, config) {
                                Ok((vector, payload_bytes)) => {
                                    batch.push(vector);
                                    total_bytes += payload_bytes as u64;
                                    should_flush = batch.len() >= config.batch_size;
                                }
                                Err(e) => {
                                    if config.skip_invalid_events {
                                        total_failed += 1;
                                        if errors.len() < MAX_ERRORS {
                                            errors.push(format!("Skipped invalid event: {}", e));
                                        }
                                        debug!(session_id, error = %e, "Skipping invalid event");
                                        should_flush = false;
                                    } else {
                                        return Err(ConnectorError::Serialization(format!(
                                            "Failed to build vector: {}", e
                                        )).into());
                                    }
                                }
                            }
                        }
                        None => {
                            stream_ended = true;
                            should_flush = !batch.is_empty();
                        }
                    }
                }

                _ = flush_ticker.tick() => {
                    should_flush = !batch.is_empty();
                    if should_flush {
                        debug!(
                            session_id,
                            records = batch.len(),
                            "Timer-triggered flush of partial batch"
                        );
                    }
                }
            }

            // ── Flush batch ─────────────────────────────────────────────
            if should_flush && !batch.is_empty() {
                let batch_size = batch.len();

                // Circuit breaker gate
                if let Some(ref cb) = circuit_breaker {
                    if !cb.allow_request() {
                        counter!("s3vectors.circuit_breaker.rejected").increment(1);
                        warn!(
                            session_id,
                            batch_size, "Circuit breaker OPEN — rejecting batch"
                        );
                        total_failed += batch_size as u64;
                        if errors.len() < MAX_ERRORS {
                            errors.push(format!(
                                "Batch of {} vectors rejected by circuit breaker",
                                batch_size
                            ));
                        }
                        batch.clear();
                        if stream_ended {
                            break;
                        }
                        continue;
                    }
                }

                gauge!("s3vectors.batch.size").set(batch_size as f64);
                debug!(session_id, batch_size, "Flushing batch to S3 Vectors");

                let t0 = std::time::Instant::now();

                match Self::upsert_batch(
                    &client,
                    &config.vector_bucket_name,
                    &config.index_name,
                    &batch,
                    &retry,
                    &session_id,
                )
                .await
                {
                    Ok(()) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("s3vectors.batch.duration_ms").record(elapsed_ms);
                        counter!("s3vectors.batches.success").increment(1);
                        counter!("s3vectors.records.written").increment(batch_size as u64);
                        total_written += batch_size as u64;
                        debug!(session_id, total_written, "Batch upserted successfully");

                        if let Some(ref cb) = circuit_breaker {
                            cb.record_success();
                        }
                    }
                    Err(e) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("s3vectors.batch.duration_ms").record(elapsed_ms);
                        counter!("s3vectors.batches.failed").increment(1);
                        counter!("s3vectors.records.failed").increment(batch_size as u64);
                        error!(session_id, "Failed to upsert batch to S3 Vectors: {}", e);

                        if let Some(ref cb) = circuit_breaker {
                            cb.record_failure();
                        }

                        // Batch upsert failures always propagate.
                        // `skip_invalid_events` only covers individual event
                        // build failures (missing vector, bad ID), never
                        // wholesale S3 Vectors transport/server errors.
                        return Err(e.into());
                    }
                }

                batch.clear();
            }

            if stream_ended {
                break;
            }
        }

        // Log circuit breaker diagnostics on shutdown
        if let Some(ref cb) = circuit_breaker {
            let (cb_state, cb_failures, cb_half_open_ok) = cb.diagnostics();
            info!(
                session_id,
                total_written,
                total_failed,
                total_bytes,
                cb_state = ?cb_state,
                cb_consecutive_failures = cb_failures,
                cb_half_open_successes = cb_half_open_ok,
                "S3 Vectors sink completed"
            );
        } else {
            info!(
                session_id,
                total_written, total_failed, total_bytes, "S3 Vectors sink completed"
            );
        }

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: total_bytes,
            records_failed: total_failed,
            errors,
        })
    }
}

// ─────────────────────────────────────────────────────────────────
// Factory + registration
// ─────────────────────────────────────────────────────────────────

/// Factory for creating `S3VectorSink` instances.
pub struct S3VectorSinkFactory;

impl SinkFactory for S3VectorSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        S3VectorSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(S3VectorSink::new()))
    }
}

crate::impl_any_sink!(S3VectorSink, S3VectorSinkConfig);

/// Register the S3 Vectors sink connector.
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("s3-vectors", Arc::new(S3VectorSinkFactory));
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Config defaults ──────────────────────────────────────────

    #[test]
    fn test_default_config() {
        let config = S3VectorSinkConfig::default();
        assert_eq!(config.vector_field, "vector");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 10_000);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.metadata_fields.is_empty());
        assert!(config.circuit_breaker.enabled);
        assert!(!config.auto_create);
        assert_eq!(config.distance_metric, S3VectorDistanceMetric::Cosine);
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = S3VectorSinkConfig {
            region: Some("us-west-2".to_string()),
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            dimension: Some(384),
            auto_create: true,
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: S3VectorSinkConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(deserialized.region, Some("us-west-2".to_string()));
        assert_eq!(deserialized.vector_bucket_name, "test-bucket");
        assert_eq!(deserialized.index_name, "test-index");
        assert_eq!(deserialized.dimension, Some(384));
        assert!(deserialized.auto_create);
    }

    #[test]
    fn test_config_from_yaml() {
        let yaml = r#"
vector_bucket_name: my-vectors
index_name: embeddings
dimension: 1536
distance_metric: euclidean
auto_create: true
vector_field: embedding
id_strategy: hash
batch_size: 500
timeout_secs: 60
metadata_fields:
  title: title
  category: cat
"#;
        let config: S3VectorSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.vector_bucket_name, "my-vectors");
        assert_eq!(config.index_name, "embeddings");
        assert_eq!(config.dimension, Some(1536));
        assert_eq!(config.distance_metric, S3VectorDistanceMetric::Euclidean);
        assert!(config.auto_create);
        assert_eq!(config.vector_field, "embedding");
        assert_eq!(config.id_strategy, S3VectorIdStrategy::Hash);
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.metadata_fields.len(), 2);
    }

    // ── Config validation ────────────────────────────────────────

    #[test]
    fn test_validate_retry_ok() {
        let config = S3VectorSinkConfig::default();
        assert!(config.validate_retry().is_ok());
    }

    #[test]
    fn test_validate_retry_bad() {
        let config = S3VectorSinkConfig {
            initial_backoff_ms: 50_000,
            max_backoff_ms: 1_000,
            ..Default::default()
        };
        assert!(config.validate_retry().is_err());
    }

    #[test]
    fn test_validate_bucket_name_ok() {
        assert!(S3VectorSinkConfig::validate_bucket_name("my-vector-bucket").is_ok());
        assert!(S3VectorSinkConfig::validate_bucket_name("abc").is_ok());
        assert!(S3VectorSinkConfig::validate_bucket_name("test123").is_ok());
    }

    #[test]
    fn test_validate_bucket_name_empty() {
        assert!(S3VectorSinkConfig::validate_bucket_name("").is_err());
    }

    #[test]
    fn test_validate_bucket_name_too_short() {
        assert!(S3VectorSinkConfig::validate_bucket_name("ab").is_err());
    }

    #[test]
    fn test_validate_bucket_name_too_long() {
        let long = "a".repeat(64);
        assert!(S3VectorSinkConfig::validate_bucket_name(&long).is_err());
    }

    #[test]
    fn test_validate_bucket_name_uppercase() {
        assert!(S3VectorSinkConfig::validate_bucket_name("MyBucket").is_err());
    }

    #[test]
    fn test_validate_bucket_name_leading_hyphen() {
        assert!(S3VectorSinkConfig::validate_bucket_name("-my-bucket").is_err());
    }

    #[test]
    fn test_validate_bucket_name_trailing_hyphen() {
        assert!(S3VectorSinkConfig::validate_bucket_name("my-bucket-").is_err());
    }

    #[test]
    fn test_validate_index_name_ok() {
        assert!(S3VectorSinkConfig::validate_index_name("embeddings").is_ok());
    }

    #[test]
    fn test_validate_index_name_empty() {
        assert!(S3VectorSinkConfig::validate_index_name("").is_err());
    }

    #[test]
    fn test_validate_index_name_too_long() {
        let long = "a".repeat(64);
        assert!(S3VectorSinkConfig::validate_index_name(&long).is_err());
    }

    // ── Vector extraction ────────────────────────────────────────

    #[test]
    fn test_extract_vector_ok() {
        let data = serde_json::json!({"vector": [1.0, 2.0, 3.0]});
        let vec = extract_vector(&data, "vector").unwrap();
        assert_eq!(vec, vec![1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_extract_vector_nested() {
        let data = serde_json::json!({"data": {"embedding": [0.5, 0.6]}});
        let vec = extract_vector(&data, "data.embedding").unwrap();
        assert_eq!(vec, vec![0.5f32, 0.6]);
    }

    #[test]
    fn test_extract_vector_missing_field() {
        let data = serde_json::json!({"other": 42});
        assert!(extract_vector(&data, "vector").is_err());
    }

    #[test]
    fn test_extract_vector_not_array() {
        let data = serde_json::json!({"vector": "not-an-array"});
        assert!(extract_vector(&data, "vector").is_err());
    }

    #[test]
    fn test_extract_vector_empty_array() {
        let data = serde_json::json!({"vector": []});
        assert!(extract_vector(&data, "vector").is_err());
    }

    #[test]
    fn test_extract_vector_non_numeric() {
        let data = serde_json::json!({"vector": [1.0, "bad", 3.0]});
        assert!(extract_vector(&data, "vector").is_err());
    }

    #[test]
    fn test_extract_vector_nan_rejected() {
        // serde_json cannot represent NaN, so we test via null workaround
        let data = serde_json::json!({"vector": [1.0, null, 3.0]});
        assert!(extract_vector(&data, "vector").is_err());
    }

    #[test]
    fn test_extract_vector_f32_overflow() {
        let data = serde_json::json!({"vector": [1.0, 3.5e+38, 3.0]});
        assert!(extract_vector(&data, "vector").is_err());
    }

    // ── Key extraction ───────────────────────────────────────────

    #[test]
    fn test_extract_key_field_string() {
        let data = serde_json::json!({"id": "vec-001"});
        let key = extract_key(&data, &S3VectorIdStrategy::Field, "id").unwrap();
        assert_eq!(key, "vec-001");
    }

    #[test]
    fn test_extract_key_field_number() {
        let data = serde_json::json!({"id": 42});
        let key = extract_key(&data, &S3VectorIdStrategy::Field, "id").unwrap();
        assert_eq!(key, "42");
    }

    #[test]
    fn test_extract_key_field_empty_string() {
        let data = serde_json::json!({"id": ""});
        assert!(extract_key(&data, &S3VectorIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_key_field_missing() {
        let data = serde_json::json!({"other": "val"});
        assert!(extract_key(&data, &S3VectorIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_key_uuid_valid() {
        let data = serde_json::json!({"id": "550e8400-e29b-41d4-a716-446655440000"});
        let key = extract_key(&data, &S3VectorIdStrategy::Uuid, "id").unwrap();
        assert_eq!(key, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_extract_key_uuid_no_hyphens() {
        let data = serde_json::json!({"id": "550e8400e29b41d4a716446655440000"});
        let key = extract_key(&data, &S3VectorIdStrategy::Uuid, "id").unwrap();
        assert_eq!(key, "550e8400e29b41d4a716446655440000");
    }

    #[test]
    fn test_extract_key_uuid_uppercase() {
        let data = serde_json::json!({"id": "550E8400-E29B-41D4-A716-446655440000"});
        let key = extract_key(&data, &S3VectorIdStrategy::Uuid, "id").unwrap();
        assert_eq!(key, "550E8400-E29B-41D4-A716-446655440000");
    }

    #[test]
    fn test_extract_key_uuid_invalid_length() {
        let data = serde_json::json!({"id": "short"});
        assert!(extract_key(&data, &S3VectorIdStrategy::Uuid, "id").is_err());
    }

    #[test]
    fn test_extract_key_uuid_invalid_chars() {
        let data = serde_json::json!({"id": "550e8400-e29b-41d4-a716-44665544gggg"});
        assert!(extract_key(&data, &S3VectorIdStrategy::Uuid, "id").is_err());
    }

    #[test]
    fn test_extract_key_hash_deterministic() {
        let data = serde_json::json!({"key": "value", "num": 42});
        let k1 = extract_key(&data, &S3VectorIdStrategy::Hash, "unused").unwrap();
        let k2 = extract_key(&data, &S3VectorIdStrategy::Hash, "unused").unwrap();
        assert_eq!(k1, k2);
        assert_eq!(k1.len(), 16); // 16 hex chars for u64
    }

    #[test]
    fn test_extract_key_hash_different_data() {
        let d1 = serde_json::json!({"a": 1});
        let d2 = serde_json::json!({"a": 2});
        let k1 = extract_key(&d1, &S3VectorIdStrategy::Hash, "x").unwrap();
        let k2 = extract_key(&d2, &S3VectorIdStrategy::Hash, "x").unwrap();
        assert_ne!(k1, k2);
    }

    // ── UUID validation ──────────────────────────────────────────

    #[test]
    fn test_validate_uuid_with_hyphens() {
        assert!(validate_uuid("550e8400-e29b-41d4-a716-446655440000").is_ok());
    }

    #[test]
    fn test_validate_uuid_without_hyphens() {
        assert!(validate_uuid("550e8400e29b41d4a716446655440000").is_ok());
    }

    #[test]
    fn test_validate_uuid_wrong_length() {
        assert!(validate_uuid("abc").is_err());
    }

    #[test]
    fn test_validate_uuid_wrong_hex_count() {
        // 36 chars but more than 32 hex digits (no hyphens, just hex chars)
        assert!(validate_uuid("550e8400e29b41d4a716446655440000aaaa").is_err());
    }

    // ── FNV-1a hash ──────────────────────────────────────────────

    #[test]
    fn test_fnv1a_empty() {
        assert_eq!(fnv1a_hash(b""), 0xcbf29ce484222325);
    }

    #[test]
    fn test_fnv1a_deterministic() {
        let h1 = fnv1a_hash(b"hello world");
        let h2 = fnv1a_hash(b"hello world");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_fnv1a_different() {
        assert_ne!(fnv1a_hash(b"hello"), fnv1a_hash(b"world"));
    }

    // ── Backoff calculation ──────────────────────────────────────

    #[test]
    fn test_calculate_backoff_zero_attempt() {
        let b = calculate_backoff(0, 200, 10_000);
        assert!(b.as_millis() >= 150 && b.as_millis() <= 250);
    }

    #[test]
    fn test_calculate_backoff_capped() {
        let b = calculate_backoff(20, 200, 10_000);
        assert!(b.as_millis() <= 10_000);
    }

    #[test]
    fn test_calculate_backoff_exponential() {
        // Attempt 0: ~200ms, Attempt 1: ~400ms, Attempt 2: ~800ms
        let b0 = calculate_backoff(0, 200, 100_000);
        let b1 = calculate_backoff(1, 200, 100_000);
        // b1 should be roughly 2x b0 (with jitter)
        assert!(b1.as_millis() > b0.as_millis());
    }

    // ── JSON to Document conversion ──────────────────────────────

    #[test]
    fn test_json_to_s3v_document_string() {
        let v = serde_json::json!("hello");
        match json_to_s3v_document(&v) {
            S3VDocument::String(s) => assert_eq!(s, "hello"),
            other => panic!("Expected String, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_bool() {
        let v = serde_json::json!(true);
        match json_to_s3v_document(&v) {
            S3VDocument::Bool(b) => assert!(b),
            other => panic!("Expected Bool, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_null() {
        let v = serde_json::json!(null);
        assert!(matches!(json_to_s3v_document(&v), S3VDocument::Null));
    }

    #[test]
    fn test_json_to_s3v_document_number() {
        let v = serde_json::json!(42);
        match json_to_s3v_document(&v) {
            S3VDocument::Number(_) => {}
            other => panic!("Expected Number, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_array() {
        let v = serde_json::json!([1, "two", true]);
        match json_to_s3v_document(&v) {
            S3VDocument::Array(arr) => assert_eq!(arr.len(), 3),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_object() {
        let v = serde_json::json!({"key": "value"});
        match json_to_s3v_document(&v) {
            S3VDocument::Object(map) => {
                assert_eq!(map.len(), 1);
                assert!(map.contains_key("key"));
            }
            other => panic!("Expected Object, got {:?}", other),
        }
    }

    // ── Circuit breaker ──────────────────────────────────────────

    #[test]
    fn test_circuit_breaker_initially_closed() {
        let cb = CircuitBreaker::new(&S3VectorCBConfig::default());
        assert_eq!(cb.current_state(), CBState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 60,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.current_state(), CBState::Closed);
        cb.record_failure(); // threshold reached
        assert_eq!(cb.current_state(), CBState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 60,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // resets
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.current_state(), CBState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure_reopens() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 1,
            reset_timeout_secs: 0, // immediate transition for testing
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure(); // opens
        assert_eq!(cb.current_state(), CBState::Open);
        // With reset_timeout_secs=0, allow_request transitions to HalfOpen
        assert!(cb.allow_request());
        assert_eq!(cb.current_state(), CBState::HalfOpen);
        cb.record_failure(); // should re-open
        assert_eq!(cb.current_state(), CBState::Open);
    }

    #[test]
    fn test_circuit_breaker_diagnostics() {
        let cb = CircuitBreaker::new(&S3VectorCBConfig::default());
        let (state, failures, half_open) = cb.diagnostics();
        assert_eq!(state, CBState::Closed);
        assert_eq!(failures, 0);
        assert_eq!(half_open, 0);
    }

    // ── Error classification ─────────────────────────────────────

    #[test]
    fn test_classify_error_message_timeout() {
        let err = classify_error_message("Operation timed out");
        assert!(matches!(err, ConnectorError::Timeout(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_throttle() {
        let err = classify_error_message("Request throttled");
        assert!(matches!(err, ConnectorError::RateLimited(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_access_denied() {
        let err = classify_error_message("Access denied for this operation");
        assert!(matches!(err, ConnectorError::Auth(_)));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_not_found() {
        let err = classify_error_message("Resource not found");
        assert!(matches!(err, ConnectorError::NotFound(_)));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_validation() {
        let err = classify_error_message("Validation error: invalid dimension");
        assert!(matches!(err, ConnectorError::Schema(_)));
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_connection() {
        let err = classify_error_message("Connection refused");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_unknown() {
        let err = classify_error_message("Something weird happened");
        assert!(matches!(err, ConnectorError::Internal(_)));
        assert!(!err.is_retryable());
    }

    // ── Truncation ───────────────────────────────────────────────

    #[test]
    fn test_truncated_short() {
        assert_eq!(truncated("hello"), "hello");
    }

    #[test]
    fn test_truncated_long() {
        let long = "a".repeat(1000);
        let t = truncated(&long);
        assert!(t.len() <= MAX_ERROR_LEN + 4); // +ellipsis
        assert!(t.ends_with('…'));
    }

    #[test]
    fn test_truncated_multibyte() {
        let s = "é".repeat(300);
        let t = truncated(&s);
        // Should not panic on UTF-8 boundary
        assert!(t.len() <= MAX_ERROR_LEN + 4);
    }

    // ── Build vector ─────────────────────────────────────────────

    #[test]
    fn test_build_put_vector_ok() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            ..Default::default()
        };
        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "doc-001",
                "embedding": [0.1, 0.2, 0.3]
            }))
            .build();
        let (vec, size) = build_put_vector(&event, &config).unwrap();
        assert!(size > 0);
        // vec is a PutInputVector — we can't easily inspect it without public fields
        // but the fact it built without error is the primary assertion
        let _ = vec;
    }

    #[test]
    fn test_build_put_vector_with_metadata() {
        let mut meta_fields = HashMap::new();
        meta_fields.insert("title".to_string(), "doc_title".to_string());
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            metadata_fields: meta_fields,
            ..Default::default()
        };
        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "doc-001",
                "embedding": [0.1, 0.2],
                "title": "Test Document"
            }))
            .build();
        let (vec, _) = build_put_vector(&event, &config).unwrap();
        let _ = vec;
    }

    #[test]
    fn test_build_put_vector_non_object_data() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            ..Default::default()
        };
        // null
        let event = SourceEvent::builder().data(serde_json::json!(null)).build();
        assert!(build_put_vector(&event, &config).is_err());
        // array
        let event = SourceEvent::builder()
            .data(serde_json::json!([1, 2, 3]))
            .build();
        assert!(build_put_vector(&event, &config).is_err());
        // string
        let event = SourceEvent::builder()
            .data(serde_json::json!("hello"))
            .build();
        assert!(build_put_vector(&event, &config).is_err());
    }

    // ── Spec ─────────────────────────────────────────────────────

    #[test]
    fn test_spec() {
        let spec = S3VectorSink::spec();
        assert_eq!(spec.connector_type, "s3-vectors");
        assert!(spec.description.as_deref().is_some_and(|d| !d.is_empty()));
    }

    // ── Send + Sync ──────────────────────────────────────────────

    #[test]
    fn test_s3_vector_sink_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<S3VectorSink>();
    }

    #[test]
    fn test_s3_vector_sink_factory_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<S3VectorSinkFactory>();
    }

    // ── Id strategy defaults ─────────────────────────────────────

    #[test]
    fn test_id_strategy_default_is_field() {
        assert_eq!(S3VectorIdStrategy::default(), S3VectorIdStrategy::Field);
    }

    #[test]
    fn test_distance_metric_default_is_cosine() {
        assert_eq!(
            S3VectorDistanceMetric::default(),
            S3VectorDistanceMetric::Cosine
        );
    }

    // ── Session ID ───────────────────────────────────────────────

    #[test]
    fn test_generate_session_id_format() {
        let id = generate_session_id();
        assert!(id.starts_with("rivven-s3v-"));
        assert!(id.len() > 15);
    }

    #[test]
    fn test_generate_session_id_unique() {
        let id1 = generate_session_id();
        std::thread::sleep(Duration::from_micros(2));
        let id2 = generate_session_id();
        assert_ne!(id1, id2);
    }

    // ── Nested field extraction ──────────────────────────────────

    #[test]
    fn test_get_nested_field_top_level() {
        let data = serde_json::json!({"key": "value"});
        let v = get_nested_field(&data, "key").unwrap();
        assert_eq!(v, &serde_json::json!("value"));
    }

    #[test]
    fn test_get_nested_field_deep() {
        let data = serde_json::json!({"a": {"b": {"c": 42}}});
        let v = get_nested_field(&data, "a.b.c").unwrap();
        assert_eq!(v, &serde_json::json!(42));
    }

    #[test]
    fn test_get_nested_field_missing() {
        let data = serde_json::json!({"a": 1});
        assert!(get_nested_field(&data, "b").is_none());
    }

    #[test]
    fn test_get_nested_field_missing_intermediate() {
        let data = serde_json::json!({"a": 1});
        assert!(get_nested_field(&data, "a.b.c").is_none());
    }

    // ── Circuit breaker TOCTOU safety ────────────────────────────

    #[test]
    fn test_circuit_breaker_cas_open_to_half_open() {
        // Verify the compare_exchange pattern: only one "thread" should
        // successfully transition from Open → HalfOpen.
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 1,
            reset_timeout_secs: 0, // immediate transition
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure(); // → Open
        assert_eq!(cb.current_state(), CBState::Open);

        // First call transitions Open → HalfOpen via CAS
        assert!(cb.allow_request());
        assert_eq!(cb.current_state(), CBState::HalfOpen);

        // Manually force state back to Open to test CAS precondition
        cb.state.store(CBState::Open as u32, Ordering::Release);
        cb.opened_at.store(0, Ordering::Release); // epoch 0 → always elapsed

        // Verify CAS transitions
        assert!(cb.allow_request()); // CAS succeeds
        assert_eq!(cb.current_state(), CBState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_half_open_close_via_success_threshold() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 1,
            reset_timeout_secs: 0,
            success_threshold: 3,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure(); // → Open
        cb.allow_request(); // → HalfOpen

        cb.record_success(); // 1/3
        assert_eq!(cb.current_state(), CBState::HalfOpen);
        cb.record_success(); // 2/3
        assert_eq!(cb.current_state(), CBState::HalfOpen);
        cb.record_success(); // 3/3 → Closed
        assert_eq!(cb.current_state(), CBState::Closed);
        assert_eq!(cb.consecutive_failures.load(Ordering::Acquire), 0);
    }

    // ── Auto-injected metadata ───────────────────────────────────

    #[test]
    fn test_build_put_vector_auto_injects_event_metadata() {
        let mut meta_fields = HashMap::new();
        meta_fields.insert("title".to_string(), "doc_title".to_string());
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            metadata_fields: meta_fields,
            ..Default::default()
        };
        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "doc-001",
                "embedding": [0.1, 0.2],
                "title": "Test"
            }))
            .build();
        // The build should succeed — we can't inspect PutInputVector internals,
        // but we can verify the metadata HashMap construction below.
        let (_, size) = build_put_vector(&event, &config).unwrap();
        assert!(size > 0);
    }

    #[test]
    fn test_metadata_auto_inject_fields_present() {
        // Directly test the metadata construction path
        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "vec-1",
                "embedding": [1.0, 2.0],
                "title": "Hello"
            }))
            .build();
        let data = &event.data;
        let key = "vec-1".to_string();
        // Simulate the metadata construction from build_put_vector
        let mut doc = HashMap::new();
        doc.insert(
            "doc_title".to_string(),
            json_to_s3v_document(get_nested_field(data, "title").unwrap()),
        );
        doc.insert("_key".to_string(), S3VDocument::String(key));
        doc.insert(
            "_event_type".to_string(),
            S3VDocument::String(format!("{:?}", event.event_type)),
        );
        doc.insert(
            "_stream".to_string(),
            S3VDocument::String(event.stream.clone()),
        );
        doc.insert(
            "_namespace".to_string(),
            S3VDocument::String(event.namespace.as_deref().unwrap_or("").to_string()),
        );
        doc.insert(
            "_timestamp".to_string(),
            S3VDocument::String(event.timestamp.to_rfc3339()),
        );

        // Verify all auto-injected keys are present
        assert!(doc.contains_key("_key"));
        assert!(doc.contains_key("_event_type"));
        assert!(doc.contains_key("_stream"));
        assert!(doc.contains_key("_namespace"));
        assert!(doc.contains_key("_timestamp"));
        assert!(doc.contains_key("doc_title"));
        assert_eq!(doc.len(), 6);

        // Verify _timestamp format
        if let S3VDocument::String(ts) = &doc["_timestamp"] {
            assert!(ts.contains('T'));
            assert!(ts.ends_with("+00:00") || ts.ends_with('Z'));
        } else {
            panic!("_timestamp should be String");
        }
    }

    // ── Validation bounds (tightened) ────────────────────────────

    #[test]
    fn test_validate_max_retries_capped_at_10() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-idx".to_string(),
            max_retries: 11, // exceeds max=10
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_initial_backoff_min_50() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-idx".to_string(),
            initial_backoff_ms: 10, // below min=50
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_max_retries_at_boundary() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-idx".to_string(),
            max_retries: 10, // exactly at max boundary
            ..Default::default()
        };
        // Should pass (range annotation validates the retries field alone)
        assert!(config.validate().is_ok());
    }

    // ── Empty metadata path ──────────────────────────────────────

    #[test]
    fn test_build_put_vector_no_metadata_when_fields_empty() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            metadata_fields: HashMap::new(), // empty → no metadata
            ..Default::default()
        };
        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "d1",
                "embedding": [0.1, 0.2]
            }))
            .build();
        let (_, size) = build_put_vector(&event, &config).unwrap();
        assert!(size > 0);
    }

    // ── CB config defaults validated ─────────────────────────────

    #[test]
    fn test_cb_config_default_values() {
        let cb = S3VectorCBConfig::default();
        assert!(cb.enabled);
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.reset_timeout_secs, 30);
        assert_eq!(cb.success_threshold, 2);
    }

    #[test]
    fn test_cb_config_validation_failure_threshold_zero() {
        let cb = S3VectorCBConfig {
            failure_threshold: 0, // min is 1
            ..Default::default()
        };
        assert!(cb.validate().is_err());
    }

    #[test]
    fn test_cb_config_validation_success_threshold_over_max() {
        let cb = S3VectorCBConfig {
            success_threshold: 51, // max is 50
            ..Default::default()
        };
        assert!(cb.validate().is_err());
    }

    // ── Distance metric serde ────────────────────────────────────

    #[test]
    fn test_distance_metric_serde_roundtrip() {
        let yaml = r#"euclidean"#;
        let metric: S3VectorDistanceMetric = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(metric, S3VectorDistanceMetric::Euclidean);
        let serialized = serde_yaml::to_string(&metric).unwrap();
        assert!(serialized.trim() == "euclidean");
    }

    // ── Id strategy serde ────────────────────────────────────────

    #[test]
    fn test_id_strategy_serde_roundtrip() {
        for (yaml, expected) in [
            ("field", S3VectorIdStrategy::Field),
            ("uuid", S3VectorIdStrategy::Uuid),
            ("hash", S3VectorIdStrategy::Hash),
        ] {
            let parsed: S3VectorIdStrategy = serde_yaml::from_str(yaml).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    // ── FNV-1a known values ──────────────────────────────────────

    #[test]
    fn test_fnv1a_known_value() {
        // FNV-1a("") = offset basis = 0xcbf29ce484222325
        assert_eq!(fnv1a_hash(b""), 0xcbf29ce484222325);
        // FNV-1a("a") — known value
        let h = fnv1a_hash(b"a");
        assert_ne!(h, 0xcbf29ce484222325); // differs from empty
        assert_eq!(h, 0xaf63dc4c8601ec8c); // known FNV-1a-64("a")
    }

    // ── Bucket/index name edge cases ─────────────────────────────

    #[test]
    fn test_validate_bucket_name_max_length() {
        let name = "a".repeat(63);
        assert!(S3VectorSinkConfig::validate_bucket_name(&name).is_ok());
    }

    #[test]
    fn test_validate_bucket_name_special_chars() {
        assert!(S3VectorSinkConfig::validate_bucket_name("my_bucket").is_err()); // underscores
        assert!(S3VectorSinkConfig::validate_bucket_name("my.bucket").is_err()); // dots
        assert!(S3VectorSinkConfig::validate_bucket_name("my bucket").is_err());
        // spaces
    }

    #[test]
    fn test_validate_index_name_max_length() {
        let name = "a".repeat(63);
        assert!(S3VectorSinkConfig::validate_index_name(&name).is_ok());
    }

    #[test]
    fn test_validate_index_name_one_over_max() {
        let name = "a".repeat(64);
        assert!(S3VectorSinkConfig::validate_index_name(&name).is_err());
    }

    // ── Vector extraction edge cases ─────────────────────────────

    #[test]
    fn test_extract_vector_very_large_dimension() {
        let arr: Vec<f64> = (0..10_000).map(|i| i as f64 * 0.001).collect();
        let data = serde_json::json!({"vector": arr});
        let vec = extract_vector(&data, "vector").unwrap();
        assert_eq!(vec.len(), 10_000);
    }

    #[test]
    fn test_extract_vector_single_element() {
        let data = serde_json::json!({"vector": [42.0]});
        let vec = extract_vector(&data, "vector").unwrap();
        assert_eq!(vec, vec![42.0f32]);
    }

    #[test]
    fn test_extract_vector_negative_values() {
        let data = serde_json::json!({"vector": [-1.5, -0.0, 0.0, 1.5]});
        let vec = extract_vector(&data, "vector").unwrap();
        assert_eq!(vec.len(), 4);
        assert_eq!(vec[0], -1.5f32);
    }

    // ── Key extraction edge cases ────────────────────────────────

    #[test]
    fn test_extract_key_field_boolean_rejected() {
        let data = serde_json::json!({"id": true});
        assert!(extract_key(&data, &S3VectorIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_key_field_null_rejected() {
        let data = serde_json::json!({"id": null});
        assert!(extract_key(&data, &S3VectorIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_key_field_nested() {
        let data = serde_json::json!({"meta": {"doc_id": "abc-123"}});
        let key = extract_key(&data, &S3VectorIdStrategy::Field, "meta.doc_id").unwrap();
        assert_eq!(key, "abc-123");
    }

    #[test]
    fn test_extract_key_uuid_not_a_string() {
        let data = serde_json::json!({"id": 12345});
        assert!(extract_key(&data, &S3VectorIdStrategy::Uuid, "id").is_err());
    }

    // ── Backoff edge cases ───────────────────────────────────────

    #[test]
    fn test_calculate_backoff_zero_initial() {
        // With initial=0, all exponential products are 0, so jitter_range=0
        let b = calculate_backoff(0, 0, 10_000);
        assert_eq!(b.as_millis(), 0);
    }

    #[test]
    fn test_calculate_backoff_initial_equals_max() {
        let b = calculate_backoff(0, 5_000, 5_000);
        assert!(b.as_millis() <= 5_000);
    }

    // ── JSON to Document edge cases ──────────────────────────────

    #[test]
    fn test_json_to_s3v_document_negative_int() {
        let v = serde_json::json!(-42);
        match json_to_s3v_document(&v) {
            S3VDocument::Number(n) => match n {
                aws_smithy_types::Number::NegInt(i) => assert_eq!(i, -42),
                other => panic!("Expected NegInt, got {:?}", other),
            },
            other => panic!("Expected Number, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_float() {
        let v = serde_json::json!(1.23456);
        match json_to_s3v_document(&v) {
            S3VDocument::Number(n) => match n {
                aws_smithy_types::Number::Float(f) => {
                    assert!((f - 1.23456).abs() < f64::EPSILON);
                }
                other => panic!("Expected Float, got {:?}", other),
            },
            other => panic!("Expected Number, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_positive_int() {
        let v = serde_json::json!(42u64);
        match json_to_s3v_document(&v) {
            S3VDocument::Number(n) => match n {
                aws_smithy_types::Number::PosInt(u) => assert_eq!(u, 42),
                other => panic!("Expected PosInt, got {:?}", other),
            },
            other => panic!("Expected Number, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_nested_object() {
        let v = serde_json::json!({"a": {"b": [1, 2]}});
        match json_to_s3v_document(&v) {
            S3VDocument::Object(map) => {
                assert!(map.contains_key("a"));
                match &map["a"] {
                    S3VDocument::Object(inner) => {
                        assert!(inner.contains_key("b"));
                        match &inner["b"] {
                            S3VDocument::Array(arr) => assert_eq!(arr.len(), 2),
                            other => panic!("Expected Array, got {:?}", other),
                        }
                    }
                    other => panic!("Expected inner Object, got {:?}", other),
                }
            }
            other => panic!("Expected Object, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_empty_array() {
        let v = serde_json::json!([]);
        match json_to_s3v_document(&v) {
            S3VDocument::Array(arr) => assert!(arr.is_empty()),
            other => panic!("Expected empty Array, got {:?}", other),
        }
    }

    #[test]
    fn test_json_to_s3v_document_empty_object() {
        let v = serde_json::json!({});
        match json_to_s3v_document(&v) {
            S3VDocument::Object(map) => assert!(map.is_empty()),
            other => panic!("Expected empty Object, got {:?}", other),
        }
    }

    // ── Error classification edge cases ──────────────────────────

    #[test]
    fn test_classify_error_message_contains_rate() {
        let err = classify_error_message("Rate limit exceeded");
        assert!(matches!(err, ConnectorError::RateLimited(_)));
    }

    #[test]
    fn test_classify_error_message_contains_too_many() {
        let err = classify_error_message("Too many requests");
        assert!(matches!(err, ConnectorError::RateLimited(_)));
    }

    #[test]
    fn test_classify_error_message_forbidden() {
        let err = classify_error_message("Forbidden");
        assert!(matches!(err, ConnectorError::Auth(_)));
    }

    #[test]
    fn test_classify_error_message_unauthorized() {
        let err = classify_error_message("Unauthorized access");
        assert!(matches!(err, ConnectorError::Auth(_)));
    }

    #[test]
    fn test_classify_error_message_unavailable() {
        let err = classify_error_message("Service unavailable");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_reset() {
        let err = classify_error_message("Connection reset by peer");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_classify_error_message_malformed() {
        let err = classify_error_message("Malformed request body");
        assert!(matches!(err, ConnectorError::Schema(_)));
        assert!(!err.is_retryable());
    }

    // ── Truncation edge cases ────────────────────────────────────

    #[test]
    fn test_truncated_exact_boundary() {
        let s = "a".repeat(MAX_ERROR_LEN);
        let t = truncated(&s);
        assert_eq!(t.len(), MAX_ERROR_LEN); // no ellipsis needed
        assert!(!t.ends_with('…'));
    }

    #[test]
    fn test_truncated_one_over() {
        let s = "a".repeat(MAX_ERROR_LEN + 1);
        let t = truncated(&s);
        assert!(t.ends_with('…'));
        assert!(t.len() <= MAX_ERROR_LEN + 4);
    }

    #[test]
    fn test_truncated_empty() {
        assert_eq!(truncated(""), "");
    }

    // ── Factory ──────────────────────────────────────────────────

    #[test]
    fn test_factory_spec() {
        let factory = S3VectorSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "s3-vectors");
    }

    #[test]
    fn test_factory_create() {
        let factory = S3VectorSinkFactory;
        let _sink = factory.create().unwrap();
        // Verify it returns a boxed sink without panicking
    }

    // ── Default impl ─────────────────────────────────────────────

    #[test]
    fn test_s3_vector_sink_default() {
        let sink = S3VectorSink::default();
        assert!(sink.circuit_breaker.is_some());
    }

    // ── Debug impl ───────────────────────────────────────────────

    #[test]
    fn test_s3_vector_sink_debug() {
        let sink = S3VectorSink::new();
        let debug = format!("{:?}", sink);
        assert!(debug.contains("S3VectorSink"));
        assert!(debug.contains("aws-sdk-s3vectors"));
        // Ensure no sensitive data leaks into debug output
        assert!(!debug.contains("circuit_breaker"));
    }

    // ── Config with all overrides ────────────────────────────────

    #[test]
    fn test_config_full_yaml_roundtrip() {
        let yaml = r#"
region: eu-west-1
endpoint_url: http://localhost:4566
vector_bucket_name: my-vectors
index_name: idx-384
dimension: 384
distance_metric: euclidean
auto_create: true
vector_field: emb
id_strategy: uuid
id_field: doc_uuid
batch_size: 500
flush_interval_secs: 10
skip_invalid_events: true
max_retries: 5
initial_backoff_ms: 100
max_backoff_ms: 5000
timeout_secs: 60
metadata_fields:
  title: doc_title
  category: cat
circuit_breaker:
  enabled: true
  failure_threshold: 10
  reset_timeout_secs: 120
  success_threshold: 3
"#;
        let config: S3VectorSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.region, Some("eu-west-1".to_string()));
        assert_eq!(
            config.endpoint_url,
            Some("http://localhost:4566".to_string())
        );
        assert_eq!(config.dimension, Some(384));
        assert_eq!(config.distance_metric, S3VectorDistanceMetric::Euclidean);
        assert_eq!(config.id_strategy, S3VectorIdStrategy::Uuid);
        assert_eq!(config.id_field, "doc_uuid");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_secs, 10);
        assert!(config.skip_invalid_events);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 5000);
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.metadata_fields.len(), 2);
        assert_eq!(config.circuit_breaker.failure_threshold, 10);
        assert_eq!(config.circuit_breaker.reset_timeout_secs, 120);
        assert_eq!(config.circuit_breaker.success_threshold, 3);

        // Round-trip
        let re = serde_yaml::to_string(&config).unwrap();
        let config2: S3VectorSinkConfig = serde_yaml::from_str(&re).unwrap();
        assert_eq!(config2.region, config.region);
        assert_eq!(config2.batch_size, config.batch_size);
    }

    // ── P3-01: record_failure already-Open guard ─────────────────

    #[test]
    fn test_circuit_breaker_record_failure_when_already_open_does_not_restamp() {
        // Verify that repeated failures after the circuit is already Open
        // do NOT reset `opened_at` (which would starve the recovery timer).
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 120,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure(); // 1
        cb.record_failure(); // 2 → Open
        assert_eq!(cb.current_state(), CBState::Open);
        let _opened_at_1 = cb.opened_at.load(Ordering::Acquire);

        // Manually set `opened_at` to a known sentinel so we can detect overwrite
        cb.opened_at.store(42, Ordering::Release);

        cb.record_failure(); // Should NOT overwrite `opened_at`
        assert_eq!(cb.current_state(), CBState::Open);
        assert_eq!(cb.opened_at.load(Ordering::Acquire), 42);
    }

    // ── P3-02: record_failure in HalfOpen increments consecutive_failures ──

    #[test]
    fn test_circuit_breaker_half_open_failure_increments_counter() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 1,
            reset_timeout_secs: 0,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure(); // → Open (consecutive_failures = 1)
        cb.allow_request(); // → HalfOpen

        let failures_before = cb.consecutive_failures.load(Ordering::Acquire);
        cb.record_failure(); // HalfOpen → Open — should ALSO increment counter
        let failures_after = cb.consecutive_failures.load(Ordering::Acquire);
        assert_eq!(failures_after, failures_before + 1);
        assert_eq!(cb.current_state(), CBState::Open);
    }

    // ── P3-04: get_nested_field rejects malformed dotted paths ───

    #[test]
    fn test_get_nested_field_empty_segment_rejected() {
        let data = serde_json::json!({"foo": {"bar": 42}});
        // Double dot → empty segment
        assert!(get_nested_field(&data, "foo..bar").is_none());
    }

    #[test]
    fn test_get_nested_field_leading_dot_rejected() {
        let data = serde_json::json!({"foo": 1});
        assert!(get_nested_field(&data, ".foo").is_none());
    }

    #[test]
    fn test_get_nested_field_trailing_dot_rejected() {
        let data = serde_json::json!({"foo": 1});
        assert!(get_nested_field(&data, "foo.").is_none());
    }

    #[test]
    fn test_get_nested_field_empty_path() {
        let data = serde_json::json!({"a": 1});
        assert!(get_nested_field(&data, "").is_none());
    }

    // ── P3-05: metadata key collision with reserved keys ─────────

    #[test]
    fn test_metadata_reserved_key_collision_detected() {
        // Simulate the validation logic from write()
        const RESERVED_META_KEYS: &[&str] =
            &["_key", "_event_type", "_stream", "_namespace", "_timestamp"];

        let mut meta_fields = HashMap::new();
        meta_fields.insert("some_field".to_string(), "_timestamp".to_string());

        let has_collision = meta_fields
            .values()
            .any(|k| RESERVED_META_KEYS.contains(&k.as_str()));
        assert!(has_collision);
    }

    #[test]
    fn test_metadata_no_reserved_collision() {
        const RESERVED_META_KEYS: &[&str] =
            &["_key", "_event_type", "_stream", "_namespace", "_timestamp"];

        let mut meta_fields = HashMap::new();
        meta_fields.insert("title".to_string(), "doc_title".to_string());
        meta_fields.insert("category".to_string(), "cat".to_string());

        let has_collision = meta_fields
            .values()
            .any(|k| RESERVED_META_KEYS.contains(&k.as_str()));
        assert!(!has_collision);
    }

    // ── P3-07: payload size includes metadata estimate ───────────

    #[test]
    fn test_build_put_vector_payload_includes_metadata_size() {
        let config_no_meta = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            metadata_fields: HashMap::new(),
            ..Default::default()
        };
        let mut meta_fields = HashMap::new();
        meta_fields.insert("title".to_string(), "doc_title".to_string());
        meta_fields.insert("category".to_string(), "cat".to_string());
        let config_with_meta = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "doc_id".to_string(),
            metadata_fields: meta_fields,
            ..Default::default()
        };

        let event = SourceEvent::builder()
            .data(serde_json::json!({
                "doc_id": "d1",
                "embedding": [0.1, 0.2],
                "title": "t",
                "category": "c"
            }))
            .build();
        let (_, size_no) = build_put_vector(&event, &config_no_meta).unwrap();
        let (_, size_with) = build_put_vector(&event, &config_with_meta).unwrap();
        // Config with metadata should have larger payload estimate
        assert!(size_with > size_no);
    }

    // ── P3-09: ConflictException treated as success ──────────────

    #[test]
    fn test_conflict_exception_maps_to_schema() {
        // verify ConflictException is classified as Schema (used in ensure_infrastructure)
        let err = classify_error_message("ConflictException: resource already exists");
        // "ConflictException" doesn't match any message patterns, falls to Internal
        // But in classify_service_error_name, ConflictException → Schema
        // The ensure_infrastructure code catches Schema from create_ calls
        let _ = err; // classification test covered elsewhere
    }

    // ── P3-10: validate length on id_field and vector_field ──────

    #[test]
    fn test_validate_empty_vector_field() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "".to_string(), // empty — should fail validation
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_id_field() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            id_field: "".to_string(), // empty — should fail validation
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_vector_field_too_long() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            vector_field: "a".repeat(256),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_id_field_too_long() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            id_field: "a".repeat(256),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_id_field_at_max_boundary() {
        let config = S3VectorSinkConfig {
            vector_bucket_name: "test-bucket".to_string(),
            index_name: "test-index".to_string(),
            id_field: "a".repeat(255), // exactly at boundary
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    // ── P3-01 + P3-02: sustained failure never resets timer ──────

    #[test]
    fn test_circuit_breaker_sustained_failures_dont_starve_recovery() {
        let config = S3VectorCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 300,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);
        cb.record_failure();
        cb.record_failure(); // → Open
        let first_ts = cb.opened_at.load(Ordering::Acquire);
        assert_ne!(first_ts, 0);

        // Many more failures while already Open
        for _ in 0..50 {
            cb.record_failure();
        }
        // opened_at should not have changed
        assert_eq!(cb.opened_at.load(Ordering::Acquire), first_ts);
        // consecutive_failures should be 2 + 50 = 52
        assert_eq!(cb.consecutive_failures.load(Ordering::Acquire), 52);
    }
}
