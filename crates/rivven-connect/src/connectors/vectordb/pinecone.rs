//! Pinecone vector database sink connector
//!
//! This module provides a sink connector for streaming data into Pinecone
//! using gRPC via `tonic` (rustls — no OpenSSL).
//!
//! # Features
//!
//! - **High-throughput upserts** via gRPC with configurable batching
//! - **Automatic batching** with configurable size and timer-based flush via `tokio::select!`
//! - **Flexible vector ID extraction** — from event field, UUID, or hash-based
//! - **Namespace support** for logical partitioning within an index
//! - **Sparse vector support** for hybrid search use cases
//! - **API key authentication** with `SensitiveString` masking (zeroize-on-drop)
//! - **Circuit breaker** to prevent cascading failures on repeated upsert errors
//! - **Metrics integration** for observability (counters, gauges, histograms)
//! - **Structured error classification** into typed `ConnectorError` variants
//! - **Connector-level batch retry** with exponential backoff and jitter
//! - **Timer-based flush** via `tokio::select!` (no stale partial batches)
//! - **Session-scoped structured logging** for easy per-run correlation
//!
//! # Vector ID Generation
//!
//! The connector supports three strategies for generating Pinecone vector IDs:
//!
//! 1. **Field** (`id_strategy: field`) — Extract the ID from a JSON field in the event data.
//!    The field value is converted to a string.
//! 2. **UUID** (`id_strategy: uuid`) — Extract a UUID string from a field in the event data.
//!    Validated as 32 hex digits (with or without hyphens).
//! 3. **Hash** (`id_strategy: hash`) — Deterministically hash the entire event payload
//!    into a hex string point ID via FNV-1a (stable across restarts). Useful for deduplication.
//!
//! # Vector Extraction
//!
//! Dense vectors are extracted from event data via a configurable `vector_field` path.
//! The field must contain a JSON array of numbers (e.g. `[0.1, 0.2, ...]`).
//! Optionally, sparse vectors can be extracted via `sparse_vector_indices_field`
//! and `sparse_vector_values_field` for hybrid search.
//!
//! # Retry / Recovery Behavior
//!
//! The connector classifies Pinecone errors into retryable (connection,
//! timeout, rate-limit, internal server) and fatal (auth, schema, config)
//! categories. Transient failures are retried with exponential backoff
//! and jitter (capped at `max_backoff_ms`). On top of retry, the circuit
//! breaker opens after consecutive failures and fails fast until the reset
//! timeout expires.
//!
//! # Circuit Breaker
//!
//! - **Closed**: Normal operation, batches flow through
//! - **Open**: After `failure_threshold` consecutive failures, batches fail fast
//! - **Half-Open**: After `cb_reset_timeout_secs`, allows one test batch through.
//!   A single failure in Half-Open immediately re-opens the circuit (canonical CB spec).
//!
//! # Metrics
//!
//! | Metric | Type | Description |
//! |--------|------|-------------|
//! | `pinecone.batches.success` | Counter | Successfully upserted batches |
//! | `pinecone.batches.failed` | Counter | Failed batch upserts |
//! | `pinecone.batches.retried` | Counter | Batch retry attempts |
//! | `pinecone.records.written` | Counter | Total vectors written |
//! | `pinecone.records.failed` | Counter | Total vectors failed |
//! | `pinecone.circuit_breaker.rejected` | Counter | Batches rejected by open CB |
//! | `pinecone.batch.size` | Gauge | Current batch size before flush |
//! | `pinecone.batch.duration_ms` | Histogram | End-to-end batch upsert latency |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_connect::connectors::vectordb::PineconeSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("pinecone", Arc::new(PineconeSinkFactory));
//! ```

use super::pinecone_client::{
    json_to_prost_value, string_value, Namespace, PineconeClient, PineconeClientConfig,
    PineconeError, SparseValues, Vector,
};
use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

// ─────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────

/// Strategy for generating Pinecone vector IDs from event data.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum PineconeIdStrategy {
    /// Extract a string ID from a named field in the event data.
    /// Numbers are converted to their string representation.
    #[default]
    Field,
    /// Extract a UUID string from a named field in the event data.
    /// Validated as 32 hex digits (with or without hyphens).
    Uuid,
    /// Deterministically hash the full event payload into a hex string via FNV-1a.
    /// Stable across process restarts — suitable for content-based deduplication.
    Hash,
}

/// Circuit breaker configuration for the Pinecone sink.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct PineconeCBConfig {
    /// Enable the circuit breaker.
    #[serde(default = "default_cb_enabled")]
    pub enabled: bool,

    /// Number of consecutive failures before the circuit opens.
    #[serde(default = "default_cb_failure_threshold")]
    #[validate(range(min = 1, max = 100))]
    pub failure_threshold: u32,

    /// Seconds to wait before transitioning from open to half-open.
    #[serde(default = "default_cb_reset_timeout_secs")]
    #[validate(range(min = 1, max = 3600))]
    pub reset_timeout_secs: u64,

    /// Successful batches required in half-open state to close the circuit.
    #[serde(default = "default_cb_success_threshold")]
    #[validate(range(min = 1, max = 10))]
    pub success_threshold: u32,
}

impl Default for PineconeCBConfig {
    fn default() -> Self {
        Self {
            enabled: default_cb_enabled(),
            failure_threshold: default_cb_failure_threshold(),
            reset_timeout_secs: default_cb_reset_timeout_secs(),
            success_threshold: default_cb_success_threshold(),
        }
    }
}

fn default_cb_enabled() -> bool {
    true
}
fn default_cb_failure_threshold() -> u32 {
    5
}
fn default_cb_reset_timeout_secs() -> u64 {
    30
}
fn default_cb_success_threshold() -> u32 {
    2
}

/// Retry parameters for batch upsert operations.
#[derive(Debug, Clone, Copy)]
struct RetryParams {
    max_retries: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    timeout: Duration,
}

impl From<&PineconeSinkConfig> for RetryParams {
    fn from(config: &PineconeSinkConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            initial_backoff_ms: config.initial_backoff_ms,
            max_backoff_ms: config.max_backoff_ms,
            timeout: Duration::from_secs(config.timeout_secs),
        }
    }
}

/// Configuration for the Pinecone sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct PineconeSinkConfig {
    /// Pinecone API key (required).
    /// Obtain from the Pinecone console at <https://app.pinecone.io>.
    pub api_key: SensitiveString,

    /// Index host URL (required).
    /// The host endpoint of the specific Pinecone index to write to.
    /// Found via the Pinecone console or `describe_index` API.
    /// Example: `"https://my-index-abc123.svc.us-east1-gcp.pinecone.io"`
    #[validate(length(min = 1, max = 2048))]
    pub index_host: String,

    /// Optional control plane host override (unused by the REST client).
    /// Retained for configuration backward compatibility.
    #[serde(default)]
    pub control_plane_host: Option<String>,

    /// Optional namespace for logical partitioning within the index.
    /// If unset, vectors are written to the default namespace.
    #[serde(default)]
    pub namespace: Option<String>,

    /// JSON field path in event data containing the dense vector (array of floats).
    /// For example, `"embedding"` extracts `event.data["embedding"]`.
    #[serde(default = "default_vector_field")]
    #[validate(length(min = 1, max = 255))]
    pub vector_field: String,

    /// Optional JSON field path for sparse vector indices (array of u32).
    /// Both `sparse_vector_indices_field` and `sparse_vector_values_field`
    /// must be set together for sparse vector support.
    #[serde(default)]
    pub sparse_vector_indices_field: Option<String>,

    /// Optional JSON field path for sparse vector values (array of f32).
    #[serde(default)]
    pub sparse_vector_values_field: Option<String>,

    /// Strategy for generating Pinecone vector IDs.
    #[serde(default)]
    pub id_strategy: PineconeIdStrategy,

    /// JSON field path for vector ID extraction (used with `field` and `uuid` strategies).
    /// Default: `"id"`
    #[serde(default = "default_id_field")]
    #[validate(length(min = 1, max = 255))]
    pub id_field: String,

    /// Number of vectors to batch before upserting
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100_000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Whether to skip individual events that fail vector/ID extraction.
    /// When false (default), a single bad event aborts the entire batch.
    #[serde(default)]
    pub skip_invalid_events: bool,

    /// Maximum number of retries for transient upsert failures
    #[serde(default = "default_max_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_retries: u32,

    /// Initial backoff between retries (milliseconds)
    #[serde(default = "default_initial_backoff_ms")]
    #[validate(range(min = 50, max = 60_000))]
    pub initial_backoff_ms: u64,

    /// Maximum backoff between retries (milliseconds)
    #[serde(default = "default_max_backoff_ms")]
    #[validate(range(min = 100, max = 300_000))]
    pub max_backoff_ms: u64,

    /// Timeout in seconds for each Pinecone data-plane API call.
    /// If an upsert does not complete within this duration, the call
    /// is aborted and classified as a timeout error (retryable).
    #[serde(default = "default_timeout_secs")]
    #[validate(range(min = 1, max = 300))]
    pub timeout_secs: u64,

    /// Circuit breaker configuration
    #[serde(default)]
    #[validate(nested)]
    pub circuit_breaker: PineconeCBConfig,
}

fn default_vector_field() -> String {
    "embedding".to_string()
}

fn default_id_field() -> String {
    "id".to_string()
}

fn default_batch_size() -> usize {
    1_000
}

fn default_flush_interval() -> u64 {
    5
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_backoff_ms() -> u64 {
    200
}

fn default_max_backoff_ms() -> u64 {
    5000
}

fn default_timeout_secs() -> u64 {
    30
}

/// Regex pattern for valid Pinecone index host URLs.
/// Must be a valid HTTPS URL or localhost for testing.
static INDEX_HOST_PATTERN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"^https?://[A-Za-z0-9._\-]+(:[0-9]+)?(/.*)?$").unwrap()
});

impl PineconeSinkConfig {
    /// Validate that an index host URL is well-formed.
    pub fn validate_index_host(host: &str) -> std::result::Result<(), String> {
        if !INDEX_HOST_PATTERN.is_match(host) {
            return Err(format!(
                "Invalid index host '{}': must be a valid HTTP(S) URL",
                host
            ));
        }
        Ok(())
    }

    /// Validate retry configuration consistency.
    pub fn validate_retry(&self) -> std::result::Result<(), String> {
        if self.initial_backoff_ms > self.max_backoff_ms {
            return Err(format!(
                "initial_backoff_ms ({}) must be <= max_backoff_ms ({})",
                self.initial_backoff_ms, self.max_backoff_ms
            ));
        }
        Ok(())
    }

    /// Validate sparse vector field configuration consistency.
    pub fn validate_sparse_fields(&self) -> std::result::Result<(), String> {
        let has_indices = self.sparse_vector_indices_field.is_some();
        let has_values = self.sparse_vector_values_field.is_some();
        if has_indices != has_values {
            return Err(
                "sparse_vector_indices_field and sparse_vector_values_field must both be set or both be unset"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl Default for PineconeSinkConfig {
    fn default() -> Self {
        Self {
            api_key: SensitiveString::new(""),
            index_host: String::new(),
            control_plane_host: None,
            namespace: None,
            vector_field: default_vector_field(),
            sparse_vector_indices_field: None,
            sparse_vector_values_field: None,
            id_strategy: PineconeIdStrategy::default(),
            id_field: default_id_field(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            skip_invalid_events: false,
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            timeout_secs: default_timeout_secs(),
            circuit_breaker: PineconeCBConfig::default(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Vector construction helpers
// ─────────────────────────────────────────────────────────────────

/// Extract a dense vector (list of f32) from event data at the given field path.
#[inline]
fn extract_vector(data: &serde_json::Value, field: &str) -> std::result::Result<Vec<f32>, String> {
    let arr = data
        .get(field)
        .ok_or_else(|| format!("Missing vector field '{}'", field))?
        .as_array()
        .ok_or_else(|| format!("Vector field '{}' is not an array", field))?;

    if arr.is_empty() {
        return Err(format!("Vector field '{}' is an empty array", field));
    }

    arr.iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_f64()
                .ok_or_else(|| format!("Vector element [{}] is not a number", i))
                .and_then(|f| {
                    let f32_val = f as f32;
                    if f32_val.is_infinite() && f.is_finite() {
                        Err(format!("Vector element [{}] overflows f32: {}", i, f))
                    } else {
                        Ok(f32_val)
                    }
                })
        })
        .collect()
}

/// Extract sparse vector components from event data.
#[inline]
fn extract_sparse_vector(
    data: &serde_json::Value,
    indices_field: &str,
    values_field: &str,
) -> std::result::Result<SparseValues, String> {
    let indices = data
        .get(indices_field)
        .ok_or_else(|| format!("Missing sparse indices field '{}'", indices_field))?
        .as_array()
        .ok_or_else(|| format!("Sparse indices field '{}' is not an array", indices_field))?
        .iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| format!("Sparse index [{}] is not a valid u32", i))
        })
        .collect::<std::result::Result<Vec<u32>, String>>()?;

    let values = data
        .get(values_field)
        .ok_or_else(|| format!("Missing sparse values field '{}'", values_field))?
        .as_array()
        .ok_or_else(|| format!("Sparse values field '{}' is not an array", values_field))?
        .iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_f64()
                .ok_or_else(|| format!("Sparse value [{}] is not a number", i))
                .and_then(|f| {
                    let f32_val = f as f32;
                    if f32_val.is_infinite() && f.is_finite() {
                        Err(format!("Sparse value [{}] overflows f32: {}", i, f))
                    } else {
                        Ok(f32_val)
                    }
                })
        })
        .collect::<std::result::Result<Vec<f32>, String>>()?;

    if indices.len() != values.len() {
        return Err(format!(
            "Sparse vector indices ({}) and values ({}) must have the same length",
            indices.len(),
            values.len()
        ));
    }

    Ok(SparseValues { indices, values })
}

/// Extract a vector ID from event data according to the chosen strategy.
/// Returns a `String` since Pinecone uses string-typed vector IDs.
#[inline]
fn extract_vector_id(
    data: &serde_json::Value,
    strategy: &PineconeIdStrategy,
    id_field: &str,
) -> std::result::Result<String, String> {
    match strategy {
        PineconeIdStrategy::Field => {
            let val = data
                .get(id_field)
                .ok_or_else(|| format!("Missing ID field '{}'", id_field))?;
            if let Some(s) = val.as_str() {
                if s.is_empty() {
                    return Err(format!("ID field '{}' is an empty string", id_field));
                }
                Ok(s.to_string())
            } else if let Some(n) = val.as_u64() {
                Ok(n.to_string())
            } else if let Some(n) = val.as_i64() {
                Ok(n.to_string())
            } else if let Some(f) = val.as_f64() {
                if !f.is_finite() {
                    return Err(format!(
                        "ID field '{}' has non-finite value: {}",
                        id_field, f
                    ));
                }
                Ok(format!("{}", f))
            } else {
                Err(format!(
                    "ID field '{}' must be a string or number, got: {}",
                    id_field, val
                ))
            }
        }
        PineconeIdStrategy::Uuid => {
            let val = data
                .get(id_field)
                .ok_or_else(|| format!("Missing UUID field '{}'", id_field))?;
            let uuid_str = val
                .as_str()
                .ok_or_else(|| format!("UUID field '{}' is not a string", id_field))?;
            // Accept both hyphenated (36 chars) and non-hyphenated (32 chars)
            // hex UUID formats.  Reject anything else client-side rather than
            // relying on the Pinecone server to surface a cryptic gRPC error.
            let len = uuid_str.len();
            if len != 32 && len != 36 {
                return Err(format!(
                    "Invalid UUID in field '{}': expected 32 or 36 chars, got {} chars",
                    id_field, len
                ));
            }
            // Single-pass validation using bytes (UUID is always ASCII)
            let mut hex_count = 0u32;
            let all_valid = uuid_str.bytes().all(|b| {
                if b == b'-' {
                    true
                } else if b.is_ascii_hexdigit() {
                    hex_count += 1;
                    true
                } else {
                    false
                }
            });
            if hex_count != 32 || !all_valid {
                return Err(format!(
                    "Invalid UUID in field '{}': expected 32 hex digits, got '{}'",
                    id_field, uuid_str
                ));
            }
            Ok(uuid_str.to_string())
        }
        PineconeIdStrategy::Hash => {
            // Deterministic FNV-1a hash on JSON bytes — stable across
            // process restarts (unlike `DefaultHasher` which uses random
            // SipHash keys per process).
            let json_bytes = serde_json::to_vec(data)
                .map_err(|e| format!("Failed to serialize event for hashing: {}", e))?;
            let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
            for &b in &json_bytes {
                h ^= b as u64;
                h = h.wrapping_mul(0x0100_0000_01b3); // FNV-1a prime
            }
            Ok(format!("{:016x}", h))
        }
    }
}

/// Build a Pinecone `Vector` from a source event.
///
/// Returns `(vector, payload_bytes)` where `payload_bytes` is the approximate
/// size of the serialized payload (for throughput metrics).
#[inline]
fn build_vector(
    event: &SourceEvent,
    config: &PineconeSinkConfig,
) -> std::result::Result<(Vector, usize), String> {
    let values = extract_vector(&event.data, &config.vector_field)?;
    let id = extract_vector_id(&event.data, &config.id_strategy, &config.id_field)?;

    // Validate Pinecone ID length limit (max 512 bytes)
    if id.len() > 512 {
        return Err(format!("Vector ID exceeds 512 bytes: {} bytes", id.len()));
    }

    // Extract sparse vector if configured
    let sparse_values = match (
        &config.sparse_vector_indices_field,
        &config.sparse_vector_values_field,
    ) {
        (Some(indices_field), Some(values_field)) => Some(extract_sparse_vector(
            &event.data,
            indices_field,
            values_field,
        )?),
        _ => None,
    };

    // Build metadata: all event data fields except vectors
    let mut metadata_fields = BTreeMap::new();
    if let serde_json::Value::Object(map) = &event.data {
        for (k, v) in map {
            // Exclude vector fields from metadata
            if k == &config.vector_field {
                continue;
            }
            if let Some(ref sf) = config.sparse_vector_indices_field {
                if k == sf {
                    continue;
                }
            }
            if let Some(ref sf) = config.sparse_vector_values_field {
                if k == sf {
                    continue;
                }
            }
            metadata_fields.insert(k.clone(), json_to_prost_value(v));
        }
    }
    // Add event metadata
    metadata_fields.insert(
        "_event_type".to_string(),
        string_value(event.event_type.to_string()),
    );
    metadata_fields.insert("_stream".to_string(), string_value(event.stream.clone()));
    if let Some(ref ns) = event.namespace {
        metadata_fields.insert("_namespace".to_string(), string_value(ns.clone()));
    } else {
        metadata_fields.insert("_namespace".to_string(), string_value(String::new()));
    }
    metadata_fields.insert(
        "_timestamp".to_string(),
        string_value(event.timestamp.to_rfc3339()),
    );

    // Estimate size without serialization — avoids allocation on the hot path.
    let payload_bytes = metadata_fields.len() * 50 + values.len() * 4;

    let vector = Vector {
        id,
        values,
        sparse_values,
        metadata: Some(prost_types::Struct {
            fields: metadata_fields,
        }),
    };

    Ok((vector, payload_bytes))
}

// ─────────────────────────────────────────────────────────────────
// Circuit Breaker (lock-free, per-sink)
// ─────────────────────────────────────────────────────────────────

/// Circuit breaker state machine (lock-free).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum CBState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

impl CBState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::Open,
            2 => Self::HalfOpen,
            _ => Self::Closed,
        }
    }
}

/// Lock-free circuit breaker using atomics — no `Mutex` on the hot path.
struct CircuitBreaker {
    state: AtomicU32,
    consecutive_failures: AtomicU32,
    opened_at: AtomicU64,
    half_open_successes: AtomicU32,
    failure_threshold: u32,
    reset_timeout: Duration,
    success_threshold: u32,
}

impl CircuitBreaker {
    fn new(config: &PineconeCBConfig) -> Self {
        Self {
            state: AtomicU32::new(CBState::Closed as u32),
            consecutive_failures: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
            half_open_successes: AtomicU32::new(0),
            failure_threshold: config.failure_threshold,
            reset_timeout: Duration::from_secs(config.reset_timeout_secs),
            success_threshold: config.success_threshold,
        }
    }

    #[inline]
    fn state(&self) -> CBState {
        CBState::from_u32(self.state.load(Ordering::Acquire))
    }

    fn now_epoch_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Returns `true` if the request should proceed.
    fn allow_request(&self) -> bool {
        match self.state() {
            CBState::Closed => true,
            CBState::Open => {
                let opened = self.opened_at.load(Ordering::Acquire);
                if Self::now_epoch_secs().saturating_sub(opened) >= self.reset_timeout.as_secs() {
                    // Atomic CAS: only one thread transitions Open → HalfOpen
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
                    }
                    true
                } else {
                    false
                }
            }
            CBState::HalfOpen => true,
        }
    }

    fn record_success(&self) {
        match self.state() {
            CBState::Closed => {
                self.consecutive_failures.store(0, Ordering::Release);
            }
            CBState::HalfOpen => {
                let prev = self.half_open_successes.fetch_add(1, Ordering::AcqRel);
                if prev + 1 >= self.success_threshold {
                    self.state.store(CBState::Closed as u32, Ordering::Release);
                    self.consecutive_failures.store(0, Ordering::Release);
                    info!("Circuit breaker closed (Pinecone recovered)");
                }
            }
            CBState::Open => {}
        }
    }

    fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);

        // In HalfOpen state, any single failure immediately re-opens the
        // circuit.  This matches the canonical CB spec: the probe request
        // failed, so we're not recovering yet.
        if self.state() == CBState::HalfOpen {
            self.state.store(CBState::Open as u32, Ordering::Release);
            self.opened_at
                .store(Self::now_epoch_secs(), Ordering::Release);
            warn!(
                threshold = self.failure_threshold,
                "Circuit breaker re-OPENED from HalfOpen (Pinecone)"
            );
            return;
        }

        // Use >= to handle concurrent fetch_add races where two threads
        // increment past the threshold simultaneously.
        if prev + 1 >= self.failure_threshold && self.state() != CBState::Open {
            self.state.store(CBState::Open as u32, Ordering::Release);
            self.opened_at
                .store(Self::now_epoch_secs(), Ordering::Release);
            warn!(
                threshold = self.failure_threshold,
                "Circuit breaker OPEN (Pinecone)"
            );
        }
    }

    /// Diagnostic snapshot for shutdown logging.
    fn diagnostics(&self) -> (CBState, u32, u32) {
        let state = self.state();
        let failures = self.consecutive_failures.load(Ordering::Acquire);
        let half_open_ok = self.half_open_successes.load(Ordering::Acquire);
        (state, failures, half_open_ok)
    }
}

// ─────────────────────────────────────────────────────────────────
// Error classification
// ─────────────────────────────────────────────────────────────────

/// Classify a `PineconeError` into a typed `ConnectorError`.
///
/// Uses the error variant and gRPC status code for precise classification.
/// All 16 gRPC status codes are explicitly mapped; unknown codes fall through
/// to string-based message heuristics.
fn classify_pinecone_error(err: &PineconeError) -> ConnectorError {
    let raw = err.to_string();
    // Truncate to prevent leaking sensitive SDK internals in logs/responses
    let msg = if raw.len() > 512 {
        let mut end = 512;
        while end > 0 && !raw.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &raw[..end])
    } else {
        raw
    };

    match err {
        // ── Timeout (retryable) ─────────────────────────────────────
        PineconeError::Timeout(_) => ConnectorError::Timeout(msg),

        // ── Connection (retryable) ──────────────────────────────────
        PineconeError::Connection(_) => ConnectorError::Connection(msg),

        // ── Serialization (non-retryable) ───────────────────────────
        PineconeError::Serialization(_) => ConnectorError::Serialization(msg),

        // ── API errors — classify by gRPC status code ──────────────
        // All 16 gRPC codes are explicitly mapped for defence-in-depth.
        PineconeError::Api { code, .. } => match code.as_str() {
            // Auth (non-retryable)
            "Unauthenticated" | "PermissionDenied" => ConnectorError::Auth(msg),
            // Not found (non-retryable)
            "NotFound" => ConnectorError::NotFound(msg),
            // Schema / client error (non-retryable)
            "InvalidArgument" | "FailedPrecondition" | "AlreadyExists" | "OutOfRange" => {
                ConnectorError::Schema(msg)
            }
            // Rate limiting (retryable)
            "ResourceExhausted" => ConnectorError::RateLimited(msg),
            // Timeout (retryable)
            "DeadlineExceeded" => ConnectorError::Timeout(msg),
            // Transient server errors (retryable)
            "Cancelled" | "Unavailable" | "Internal" | "Unknown" => ConnectorError::Connection(msg),
            // Fatal server errors (non-retryable)
            "Unimplemented" | "DataLoss" => ConnectorError::Fatal(msg),
            _ => classify_error_message(&msg),
        },
    }
}

/// Classify an error message string into a `ConnectorError`.
///
/// Fallback for API error responses whose HTTP status code does not match
/// a known category.  Scans the lowercased message for well-known patterns
/// (rate-limit, timeout, connection, auth, schema, etc.).
fn classify_error_message(msg: &str) -> ConnectorError {
    let lower = msg.to_lowercase();

    if lower.contains("resource exhausted")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
    {
        ConnectorError::RateLimited(msg.to_string())
    } else if lower.contains("deadline exceeded")
        || lower.contains("timeout")
        || lower.contains("timed out")
    {
        ConnectorError::Timeout(msg.to_string())
    } else if lower.contains("unavailable")
        || lower.contains("connection")
        || lower.contains("broken pipe")
        || lower.contains("reset by peer")
        || lower.contains("connect error")
        || lower.contains("transport error")
        || lower.contains("dns error")
    {
        ConnectorError::Connection(msg.to_string())
    } else if lower.contains("unauthenticated")
        || lower.contains("permission denied")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
    {
        ConnectorError::Auth(msg.to_string())
    } else if lower.contains("not found") || lower.contains("doesn't exist") {
        ConnectorError::NotFound(msg.to_string())
    } else if lower.contains("invalid_argument")
        || lower.contains("invalid argument")
        || lower.contains("invalid request")
        || lower.contains("invalid vector")
        || lower.contains("invalid dimension")
        || lower.contains("dimension mismatch")
        || lower.contains("wrong vector size")
    {
        ConnectorError::Schema(msg.to_string())
    } else {
        ConnectorError::Fatal(msg.to_string())
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
    format!("rivven-pc-{}-{}", std::process::id(), micros)
}

/// Exponential backoff with jitter (prevents thundering herd).
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

// ─────────────────────────────────────────────────────────────────
// Sink implementation
// ─────────────────────────────────────────────────────────────────

/// Pinecone vector database Sink implementation
pub struct PineconeSink {
    circuit_breaker: Option<CircuitBreaker>,
}

impl std::fmt::Debug for PineconeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PineconeSink")
            .field("circuit_breaker_enabled", &self.circuit_breaker.is_some())
            .finish()
    }
}

impl Default for PineconeSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PineconeSink {
    /// Create a new Pinecone sink instance with default config (circuit breaker enabled).
    pub fn new() -> Self {
        Self::with_config(&PineconeSinkConfig::default())
    }

    /// Create a sink pre-configured with the given config's circuit breaker.
    pub fn with_config(config: &PineconeSinkConfig) -> Self {
        let cb = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(&config.circuit_breaker))
        } else {
            None
        };
        Self {
            circuit_breaker: cb,
        }
    }

    /// Build a configured `PineconeClient` from the sink config.
    fn create_client(config: &PineconeSinkConfig) -> crate::error::Result<PineconeClient> {
        let pinecone_config = PineconeClientConfig {
            api_key: config.api_key.expose_secret().to_string(),
        };
        Ok(
            PineconeClient::new(&pinecone_config, &config.index_host).map_err(|e| {
                ConnectorError::Connection(format!("Failed to build Pinecone client: {}", e))
            })?,
        )
    }

    /// Upsert a batch of vectors into Pinecone with retry logic.
    async fn upsert_batch(
        client: &PineconeClient,
        batch: &[Vector],
        namespace: &Namespace,
        retry: &RetryParams,
        session_id: &str,
    ) -> std::result::Result<(), ConnectorError> {
        let mut attempt = 0u32;

        loop {
            let upsert_result =
                match tokio::time::timeout(retry.timeout, client.upsert(batch, namespace)).await {
                    Ok(result) => result,
                    Err(_elapsed) => {
                        // Synthesize a timeout error for unified handling below
                        Err(PineconeError::Timeout(format!(
                            "Upsert timed out after {}s",
                            retry.timeout.as_secs()
                        )))
                    }
                };

            match upsert_result {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    let classified = classify_pinecone_error(&e);

                    if !classified.is_retryable() {
                        // Non-retryable: surface the precise error class
                        return Err(classified);
                    }

                    if attempt > retry.max_retries {
                        return Err(ConnectorError::Fatal(format!(
                            "Upsert retries exhausted after {} attempt(s): {}",
                            attempt, classified
                        )));
                    }

                    counter!("pinecone.batches.retried").increment(1);
                    let backoff = calculate_backoff(
                        attempt - 1,
                        retry.initial_backoff_ms,
                        retry.max_backoff_ms,
                    );
                    warn!(
                        session_id,
                        attempt,
                        max_retries = retry.max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error_class = %classified,
                        "Retryable Pinecone upsert error: {}", classified
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

#[async_trait]
impl Sink for PineconeSink {
    type Config = PineconeSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("pinecone", env!("CARGO_PKG_VERSION"))
            .description("Pinecone sink — high-throughput vector upserts via gRPC (rustls)")
            .documentation_url("https://rivven.dev/docs/connectors/pinecone-sink")
            .config_schema_from::<PineconeSinkConfig>()
            .metadata("protocol", "grpc")
            .metadata("encoding", "protobuf")
            .metadata("auth", "api-key")
            .metadata("circuit_breaker", "true")
            .metadata("metrics", "true")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        let mut builder = CheckResult::builder();

        // ── API key validation ──────────────────────────────────
        if config.api_key.expose_secret().is_empty() {
            builder = builder.check(CheckDetail::failed("api_key", "api_key must not be empty"));
            return Ok(builder.build());
        }

        // ── Full config validation (validator crate annotations) ────
        if let Err(e) = config.validate() {
            builder = builder.check(CheckDetail::failed("config_validation", format!("{}", e)));
            return Ok(builder.build());
        }

        // ── Config validation ──────────────────────────────────────
        let t0 = std::time::Instant::now();

        if let Err(e) = config.validate_retry() {
            builder = builder.check(
                CheckDetail::failed("retry_config", &e)
                    .with_duration_ms(t0.elapsed().as_millis() as u64),
            );
            return Ok(builder.build());
        }
        builder = builder.check(
            CheckDetail::passed("retry_config").with_duration_ms(t0.elapsed().as_millis() as u64),
        );

        // ── Sparse vector config validation ─────────────────────────
        let t_sparse = std::time::Instant::now();
        match config.validate_sparse_fields() {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("sparse_config")
                        .with_duration_ms(t_sparse.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("sparse_config", &e)
                        .with_duration_ms(t_sparse.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        // ── Index host validation ───────────────────────────────────
        let t1 = std::time::Instant::now();
        match Self::Config::validate_index_host(&config.index_host) {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("index_host")
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("index_host", &e)
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        // Security: warn about unencrypted HTTP connections
        if config.index_host.starts_with("http://") {
            warn!(
                index_host = %config.index_host,
                "index_host uses unencrypted HTTP — API key transmitted in cleartext"
            );
        }

        info!(
            index_host = %config.index_host,
            namespace = ?config.namespace,
            "Checking Pinecone connectivity"
        );

        let client = Self::create_client(config)?;

        // ── Index connectivity + stats ──────────────────────────────
        let t2 = std::time::Instant::now();
        match client.describe_index_stats().await {
            Ok(stats) => {
                builder = builder.check(
                    CheckDetail::passed("connectivity")
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );

                info!(
                    dimension = stats.dimension,
                    total_vectors = stats.total_vector_count,
                    "Pinecone index stats retrieved"
                );
                builder = builder.check(
                    CheckDetail::passed("index_stats")
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                let msg = format!("Failed to connect to Pinecone index: {}", e);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("connectivity", &msg)
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
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
        // Validate config before starting
        if config.api_key.expose_secret().is_empty() {
            return Err(ConnectorError::Config("api_key must not be empty".into()).into());
        }
        // Full validator crate validation — enforces all #[validate] annotations
        // (range bounds on batch_size, flush_interval_secs, timeout_secs, nested CB config, etc.)
        config
            .validate()
            .map_err(|e| ConnectorError::Config(format!("Invalid config: {}", e)))?;
        config.validate_retry().map_err(ConnectorError::Config)?;
        PineconeSinkConfig::validate_index_host(&config.index_host)
            .map_err(ConnectorError::Config)?;
        config
            .validate_sparse_fields()
            .map_err(ConnectorError::Config)?;

        // P3-09: vector_field and id_field must differ (except for Hash strategy
        // which doesn't use id_field).
        if config.id_strategy != PineconeIdStrategy::Hash && config.vector_field == config.id_field
        {
            return Err(ConnectorError::Config(format!(
                "vector_field and id_field must differ (both set to '{}')",
                config.vector_field
            ))
            .into());
        }

        // P3-01: Re-initialize circuit breaker from the runtime config.
        // The factory creates sinks with PineconeSinkConfig::default(); the
        // user's config may specify different CB settings or disable it.
        let circuit_breaker = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(&config.circuit_breaker))
        } else {
            None
        };

        let session_id = generate_session_id();
        let client = Self::create_client(config)?;

        let retry = RetryParams::from(config);
        let namespace: Namespace = match &config.namespace {
            Some(ns) if !ns.is_empty() => ns.as_str().into(),
            _ => Namespace::default(),
        };

        let mut batch: Vec<Vector> = Vec::with_capacity(config.batch_size);
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
            index_host = %config.index_host,
            namespace = ?config.namespace,
            batch_size = config.batch_size,
            vector_field = %config.vector_field,
            id_strategy = ?config.id_strategy,
            cb_enabled = config.circuit_breaker.enabled,
            "Starting Pinecone sink"
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
                            match build_vector(&event, config) {
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
                        counter!("pinecone.circuit_breaker.rejected").increment(1);
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

                gauge!("pinecone.batch.size").set(batch_size as f64);
                debug!(session_id, batch_size, "Flushing batch to Pinecone");

                let t0 = std::time::Instant::now();

                match Self::upsert_batch(&client, &batch, &namespace, &retry, &session_id).await {
                    Ok(()) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("pinecone.batch.duration_ms").record(elapsed_ms);
                        counter!("pinecone.batches.success").increment(1);
                        counter!("pinecone.records.written").increment(batch_size as u64);
                        total_written += batch_size as u64;
                        debug!(session_id, total_written, "Batch upserted successfully");

                        if let Some(ref cb) = circuit_breaker {
                            cb.record_success();
                        }
                    }
                    Err(e) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("pinecone.batch.duration_ms").record(elapsed_ms);
                        counter!("pinecone.batches.failed").increment(1);
                        counter!("pinecone.records.failed").increment(batch_size as u64);
                        error!(session_id, "Failed to upsert batch to Pinecone: {}", e);

                        if let Some(ref cb) = circuit_breaker {
                            cb.record_failure();
                        }

                        // Batch upsert failures always propagate.
                        // `skip_invalid_events` only covers individual event
                        // build failures (missing vector, bad ID), never
                        // wholesale Pinecone transport/server errors.
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
                "Pinecone sink completed"
            );
        } else {
            info!(
                session_id,
                total_written, total_failed, total_bytes, "Pinecone sink completed"
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
// Factory & registration
// ─────────────────────────────────────────────────────────────────

/// Factory for creating Pinecone sink instances
pub struct PineconeSinkFactory;

impl SinkFactory for PineconeSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        PineconeSink::spec()
    }

    fn create(&self) -> Result<Box<dyn AnySink>> {
        Ok(Box::new(PineconeSink::new()))
    }
}

// Implement AnySink for PineconeSink
crate::impl_any_sink!(PineconeSink, PineconeSinkConfig);

/// Register the Pinecone sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("pinecone", Arc::new(PineconeSinkFactory));
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = PineconeSink::spec();
        assert_eq!(spec.connector_type, "pinecone");
        assert!(spec.config_schema.is_some());
        let meta = &spec.metadata;
        assert_eq!(
            meta.get("circuit_breaker").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(meta.get("metrics").map(|s| s.as_str()), Some("true"));
        assert_eq!(meta.get("protocol").map(|s| s.as_str()), Some("grpc"));
        assert_eq!(meta.get("encoding").map(|s| s.as_str()), Some("protobuf"));
        assert_eq!(meta.get("auth").map(|s| s.as_str()), Some("api-key"));
    }

    #[test]
    fn test_default_config() {
        let config = PineconeSinkConfig::default();
        assert_eq!(config.vector_field, "embedding");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.batch_size, 1_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert!(!config.skip_invalid_events);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.namespace.is_none());
        assert!(config.control_plane_host.is_none());
        assert!(config.sparse_vector_indices_field.is_none());
        assert!(config.sparse_vector_values_field.is_none());
        assert_eq!(config.id_strategy, PineconeIdStrategy::Field);
        // Circuit breaker defaults
        assert!(config.circuit_breaker.enabled);
        assert_eq!(config.circuit_breaker.failure_threshold, 5);
        assert_eq!(config.circuit_breaker.reset_timeout_secs, 30);
        assert_eq!(config.circuit_breaker.success_threshold, 2);
    }

    #[test]
    fn test_factory() {
        let factory = PineconeSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "pinecone");
        let _sink = factory.create().unwrap();
    }

    #[test]
    fn test_new_has_circuit_breaker() {
        let sink = PineconeSink::new();
        assert!(
            sink.circuit_breaker.is_some(),
            "new() must create a circuit breaker by default"
        );
    }

    #[test]
    fn test_config_validation() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test-api-key"),
            index_host: "https://my-index.svc.pinecone.io".to_string(),
            batch_size: 500,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.validate_retry().is_ok());
        assert!(config.validate_sparse_fields().is_ok());

        // Invalid: batch_size = 0
        let mut bad_batch = config.clone();
        bad_batch.batch_size = 0;
        assert!(bad_batch.validate().is_err());

        // Invalid: batch_size too large
        let mut huge_batch = config.clone();
        huge_batch.batch_size = 200_000;
        assert!(huge_batch.validate().is_err());

        // Invalid: initial_backoff > max_backoff
        let mut bad_backoff = config.clone();
        bad_backoff.initial_backoff_ms = 10_000;
        bad_backoff.max_backoff_ms = 1_000;
        assert!(bad_backoff.validate_retry().is_err());
    }

    #[test]
    fn test_sparse_field_validation() {
        // Both set — valid
        let config = PineconeSinkConfig {
            sparse_vector_indices_field: Some("idx".to_string()),
            sparse_vector_values_field: Some("vals".to_string()),
            ..Default::default()
        };
        assert!(config.validate_sparse_fields().is_ok());

        // Neither set — valid
        let config2 = PineconeSinkConfig::default();
        assert!(config2.validate_sparse_fields().is_ok());

        // Only indices — invalid
        let bad1 = PineconeSinkConfig {
            sparse_vector_indices_field: Some("idx".to_string()),
            sparse_vector_values_field: None,
            ..Default::default()
        };
        assert!(bad1.validate_sparse_fields().is_err());

        // Only values — invalid
        let bad2 = PineconeSinkConfig {
            sparse_vector_indices_field: None,
            sparse_vector_values_field: Some("vals".to_string()),
            ..Default::default()
        };
        assert!(bad2.validate_sparse_fields().is_err());
    }

    #[test]
    fn test_config_serde_defaults() {
        let yaml = r#"
api_key: "test-key"
index_host: "https://my-index.svc.pinecone.io"
"#;
        let config: PineconeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.index_host, "https://my-index.svc.pinecone.io");
        assert_eq!(config.vector_field, "embedding");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.batch_size, 1_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.id_strategy, PineconeIdStrategy::Field);
        assert!(config.namespace.is_none());
    }

    #[test]
    fn test_config_with_all_fields() {
        let yaml = r#"
api_key: "my-secret-key"
index_host: "https://my-index-abc123.svc.us-east1-gcp.pinecone.io"
control_plane_host: "https://api.pinecone.io"
namespace: "production"
vector_field: "vector"
sparse_vector_indices_field: "sparse_idx"
sparse_vector_values_field: "sparse_vals"
id_strategy: uuid
id_field: "doc_id"
batch_size: 5000
flush_interval_secs: 10
skip_invalid_events: true
max_retries: 5
initial_backoff_ms: 100
max_backoff_ms: 10000
circuit_breaker:
  enabled: false
  failure_threshold: 10
  reset_timeout_secs: 60
  success_threshold: 3
"#;
        let config: PineconeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.index_host,
            "https://my-index-abc123.svc.us-east1-gcp.pinecone.io"
        );
        assert!(config.api_key.expose_secret().contains("my-secret-key"));
        assert_eq!(
            config.control_plane_host.as_deref(),
            Some("https://api.pinecone.io")
        );
        assert_eq!(config.namespace.as_deref(), Some("production"));
        assert_eq!(config.vector_field, "vector");
        assert_eq!(
            config.sparse_vector_indices_field.as_deref(),
            Some("sparse_idx")
        );
        assert_eq!(
            config.sparse_vector_values_field.as_deref(),
            Some("sparse_vals")
        );
        assert_eq!(config.id_strategy, PineconeIdStrategy::Uuid);
        assert_eq!(config.id_field, "doc_id");
        assert_eq!(config.batch_size, 5_000);
        assert_eq!(config.flush_interval_secs, 10);
        assert!(config.skip_invalid_events);
        assert!(!config.circuit_breaker.enabled);
        assert_eq!(config.circuit_breaker.failure_threshold, 10);
    }

    #[test]
    fn test_id_strategy_serde() {
        let field: PineconeIdStrategy = serde_yaml::from_str("\"field\"").unwrap();
        assert_eq!(field, PineconeIdStrategy::Field);

        let uuid: PineconeIdStrategy = serde_yaml::from_str("\"uuid\"").unwrap();
        assert_eq!(uuid, PineconeIdStrategy::Uuid);

        let hash: PineconeIdStrategy = serde_yaml::from_str("\"hash\"").unwrap();
        assert_eq!(hash, PineconeIdStrategy::Hash);
    }

    #[test]
    fn test_index_host_validation() {
        // Valid hosts
        assert!(
            PineconeSinkConfig::validate_index_host("https://my-index.svc.pinecone.io").is_ok()
        );
        assert!(PineconeSinkConfig::validate_index_host(
            "https://my-index-abc123.svc.us-east1-gcp.pinecone.io"
        )
        .is_ok());
        assert!(PineconeSinkConfig::validate_index_host("http://localhost:5081").is_ok());
        assert!(PineconeSinkConfig::validate_index_host("https://10.0.0.1:443").is_ok());

        // Invalid hosts
        assert!(PineconeSinkConfig::validate_index_host("").is_err());
        assert!(PineconeSinkConfig::validate_index_host("not-a-url").is_err());
        assert!(PineconeSinkConfig::validate_index_host("ftp://wrong-scheme").is_err());
        assert!(PineconeSinkConfig::validate_index_host("Robert'; DROP TABLE--").is_err());
    }

    #[test]
    fn test_extract_vector() {
        let data = serde_json::json!({"embedding": [0.1, 0.2, 0.3], "id": "abc"});
        let v = extract_vector(&data, "embedding").unwrap();
        assert_eq!(v, vec![0.1f32, 0.2, 0.3]);

        // Missing field
        assert!(extract_vector(&data, "missing").is_err());

        // Not an array
        let bad = serde_json::json!({"embedding": "not_array"});
        assert!(extract_vector(&bad, "embedding").is_err());

        // Non-numeric element
        let bad_elem = serde_json::json!({"embedding": [0.1, "two", 0.3]});
        assert!(extract_vector(&bad_elem, "embedding").is_err());
    }

    #[test]
    fn test_extract_vector_edge_cases() {
        // Empty array — rejected by extract_vector (P4-09)
        let empty = serde_json::json!({"v": []});
        assert!(extract_vector(&empty, "v").is_err());

        // Single element
        let single = serde_json::json!({"v": [1.5]});
        let v = extract_vector(&single, "v").unwrap();
        assert_eq!(v, vec![1.5f32]);

        // High dimensionality (OpenAI ada-002 = 1536)
        let dims: Vec<f64> = (0..1536).map(|i| i as f64 / 1536.0).collect();
        let high_dim = serde_json::json!({"v": dims});
        let v = extract_vector(&high_dim, "v").unwrap();
        assert_eq!(v.len(), 1536);
    }

    #[test]
    fn test_extract_sparse_vector() {
        let data = serde_json::json!({
            "idx": [0, 3, 7],
            "vals": [0.5, 0.8, 0.1]
        });
        let sv = extract_sparse_vector(&data, "idx", "vals").unwrap();
        assert_eq!(sv.indices, vec![0u32, 3, 7]);
        assert_eq!(sv.values, vec![0.5f32, 0.8, 0.1]);

        // Missing indices
        assert!(extract_sparse_vector(&data, "missing", "vals").is_err());

        // Missing values
        assert!(extract_sparse_vector(&data, "idx", "missing").is_err());

        // Length mismatch
        let bad = serde_json::json!({
            "idx": [0, 3],
            "vals": [0.5, 0.8, 0.1]
        });
        assert!(extract_sparse_vector(&bad, "idx", "vals").is_err());
    }

    #[test]
    fn test_extract_vector_id_field_strategy() {
        // String ID
        let data = serde_json::json!({"id": "doc-42"});
        let id = extract_vector_id(&data, &PineconeIdStrategy::Field, "id").unwrap();
        assert_eq!(id, "doc-42");

        // Numeric ID → string
        let data_num = serde_json::json!({"id": 42});
        let id = extract_vector_id(&data_num, &PineconeIdStrategy::Field, "id").unwrap();
        assert_eq!(id, "42");

        // Negative number → string
        let data_neg = serde_json::json!({"id": -7});
        let id = extract_vector_id(&data_neg, &PineconeIdStrategy::Field, "id").unwrap();
        assert_eq!(id, "-7");

        // Missing field
        assert!(extract_vector_id(&data, &PineconeIdStrategy::Field, "missing").is_err());

        // Boolean — should fail
        let bad = serde_json::json!({"id": true});
        assert!(extract_vector_id(&bad, &PineconeIdStrategy::Field, "id").is_err());

        // Null — should fail
        let null_val = serde_json::json!({"id": null});
        assert!(extract_vector_id(&null_val, &PineconeIdStrategy::Field, "id").is_err());

        // Empty string — should fail
        let empty_str = serde_json::json!({"id": ""});
        assert!(extract_vector_id(&empty_str, &PineconeIdStrategy::Field, "id").is_err());

        // Array — should fail
        let arr = serde_json::json!({"id": [1, 2]});
        assert!(extract_vector_id(&arr, &PineconeIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_vector_id_uuid_strategy() {
        // Valid hyphenated UUID
        let data = serde_json::json!({"doc_id": "550e8400-e29b-41d4-a716-446655440000"});
        let id = extract_vector_id(&data, &PineconeIdStrategy::Uuid, "doc_id").unwrap();
        assert_eq!(id, "550e8400-e29b-41d4-a716-446655440000");

        // Valid non-hyphenated UUID
        let no_hyphens = serde_json::json!({"doc_id": "550e8400e29b41d4a716446655440000"});
        let id = extract_vector_id(&no_hyphens, &PineconeIdStrategy::Uuid, "doc_id").unwrap();
        assert_eq!(id, "550e8400e29b41d4a716446655440000");

        // Not a string
        let bad = serde_json::json!({"doc_id": 123});
        assert!(extract_vector_id(&bad, &PineconeIdStrategy::Uuid, "doc_id").is_err());

        // Too short
        let short = serde_json::json!({"doc_id": "abc"});
        assert!(extract_vector_id(&short, &PineconeIdStrategy::Uuid, "doc_id").is_err());

        // Invalid hex characters
        let bad_hex = serde_json::json!({"doc_id": "ZZZZZZZZ-e29b-41d4-a716-446655440000"});
        assert!(extract_vector_id(&bad_hex, &PineconeIdStrategy::Uuid, "doc_id").is_err());

        // Right length but not hex
        let almost = serde_json::json!({"doc_id": "550g8400-e29b-41d4-a716-446655440000"});
        assert!(extract_vector_id(&almost, &PineconeIdStrategy::Uuid, "doc_id").is_err());
    }

    #[test]
    fn test_extract_vector_id_hash_strategy() {
        let data1 = serde_json::json!({"text": "hello", "embedding": [0.1, 0.2]});
        let id1 = extract_vector_id(&data1, &PineconeIdStrategy::Hash, "").unwrap();

        // Must be 16 hex chars
        assert_eq!(id1.len(), 16, "Hash ID should be 16 hex chars");
        assert!(
            id1.chars().all(|c| c.is_ascii_hexdigit()),
            "Hash ID must be hex: {}",
            id1
        );

        // Same data → same hash
        let id1_again = extract_vector_id(&data1, &PineconeIdStrategy::Hash, "").unwrap();
        assert_eq!(id1, id1_again, "FNV-1a hash must be deterministic");

        // Different data → different hash
        let data2 = serde_json::json!({"text": "world", "embedding": [0.3, 0.4]});
        let id2 = extract_vector_id(&data2, &PineconeIdStrategy::Hash, "").unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_hash_strategy_deterministic_across_calls() {
        let data = serde_json::json!({"text": "hello world", "score": 0.95});

        let id1 = extract_vector_id(&data, &PineconeIdStrategy::Hash, "").unwrap();
        let id2 = extract_vector_id(&data, &PineconeIdStrategy::Hash, "").unwrap();
        let id3 = extract_vector_id(&data, &PineconeIdStrategy::Hash, "").unwrap();

        assert_eq!(id1, id2, "FNV-1a hash must be deterministic");
        assert_eq!(id2, id3, "FNV-1a hash must be deterministic");
    }

    #[test]
    fn test_json_to_prost_value() {
        use prost_types::value::Kind;

        // String
        let v = json_to_prost_value(&serde_json::json!("hello"));
        assert_eq!(v.kind, Some(Kind::StringValue("hello".into())));

        // Number
        let v = json_to_prost_value(&serde_json::json!(42.5));
        assert_eq!(v.kind, Some(Kind::NumberValue(42.5)));

        // Boolean
        let v = json_to_prost_value(&serde_json::json!(true));
        assert_eq!(v.kind, Some(Kind::BoolValue(true)));

        // Null
        let v = json_to_prost_value(&serde_json::json!(null));
        assert_eq!(v.kind, Some(Kind::NullValue(0)));

        // Array → JSON string
        let v = json_to_prost_value(&serde_json::json!([1, 2, 3]));
        assert_eq!(v.kind, Some(Kind::StringValue("[1,2,3]".into())));

        // Object → JSON string
        let v = json_to_prost_value(&serde_json::json!({"a": 1}));
        match &v.kind {
            Some(Kind::StringValue(s)) => assert!(s.contains("\"a\"")),
            other => panic!("expected StringValue, got {:?}", other),
        }
    }

    #[test]
    fn test_build_vector() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: Some("public".to_string()),
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": "vec-1",
                "embedding": [0.1, 0.2, 0.3],
                "title": "Test document"
            }),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let (vector, bytes) = build_vector(&event, &config).unwrap();
        assert_eq!(vector.id, "vec-1");
        assert_eq!(vector.values, vec![0.1f32, 0.2, 0.3]);
        assert!(vector.sparse_values.is_none());
        assert!(bytes > 0);

        // Metadata should NOT contain the vector field
        let meta = vector.metadata.as_ref().unwrap();
        assert!(!meta.fields.contains_key("embedding"));
        // But should contain other fields
        assert!(meta.fields.contains_key("title"));
        assert!(meta.fields.contains_key("_event_type"));
        assert!(meta.fields.contains_key("_stream"));
        assert!(meta.fields.contains_key("_namespace"));
        assert!(meta.fields.contains_key("_timestamp"));
    }

    #[test]
    fn test_build_vector_with_sparse() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": "vec-2",
                "embedding": [0.1, 0.2],
                "sparse_idx": [0, 5, 10],
                "sparse_vals": [0.9, 0.5, 0.1]
            }),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            sparse_vector_indices_field: Some("sparse_idx".to_string()),
            sparse_vector_values_field: Some("sparse_vals".to_string()),
            ..Default::default()
        };

        let (vector, _) = build_vector(&event, &config).unwrap();
        assert_eq!(vector.id, "vec-2");
        let sv = vector.sparse_values.unwrap();
        assert_eq!(sv.indices, vec![0u32, 5, 10]);
        assert_eq!(sv.values, vec![0.9f32, 0.5, 0.1]);

        // Sparse fields should be excluded from metadata
        let meta = vector.metadata.as_ref().unwrap();
        assert!(!meta.fields.contains_key("sparse_idx"));
        assert!(!meta.fields.contains_key("sparse_vals"));
    }

    #[test]
    fn test_build_vector_missing_vector_field() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({"id": "no-vec", "title": "no vector"}),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let result = build_vector(&event, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing vector field"));
    }

    #[test]
    fn test_build_vector_id_too_long() {
        use chrono::Utc;

        let long_id = "x".repeat(513);
        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": long_id,
                "embedding": [0.1, 0.2]
            }),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let result = build_vector(&event, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("512 bytes"));
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("secret-key"),
            index_host: "https://my-index.svc.pinecone.io".to_string(),
            control_plane_host: Some("https://api.pinecone.io".to_string()),
            namespace: Some("prod".to_string()),
            vector_field: "vector".to_string(),
            sparse_vector_indices_field: Some("sp_idx".to_string()),
            sparse_vector_values_field: Some("sp_vals".to_string()),
            id_strategy: PineconeIdStrategy::Uuid,
            id_field: "doc_id".to_string(),
            batch_size: 2000,
            flush_interval_secs: 10,
            skip_invalid_events: true,
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            timeout_secs: 30,
            circuit_breaker: PineconeCBConfig {
                enabled: false,
                failure_threshold: 10,
                reset_timeout_secs: 60,
                success_threshold: 3,
            },
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: PineconeSinkConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.index_host, config.index_host);
        assert_eq!(deserialized.control_plane_host, config.control_plane_host);
        assert_eq!(deserialized.namespace, config.namespace);
        assert_eq!(deserialized.vector_field, config.vector_field);
        assert_eq!(
            deserialized.sparse_vector_indices_field,
            config.sparse_vector_indices_field
        );
        assert_eq!(
            deserialized.sparse_vector_values_field,
            config.sparse_vector_values_field
        );
        assert_eq!(deserialized.id_strategy, config.id_strategy);
        assert_eq!(deserialized.id_field, config.id_field);
        assert_eq!(deserialized.batch_size, config.batch_size);
        assert_eq!(deserialized.flush_interval_secs, config.flush_interval_secs);
        assert_eq!(deserialized.skip_invalid_events, config.skip_invalid_events);
        assert_eq!(deserialized.max_retries, config.max_retries);
        assert!(!deserialized.circuit_breaker.enabled);
        assert_eq!(deserialized.circuit_breaker.failure_threshold, 10);
    }

    #[test]
    fn test_calculate_backoff() {
        // Attempt 0: close to initial_ms
        let d0 = calculate_backoff(0, 200, 5000);
        assert!(d0.as_millis() >= 100, "backoff too small: {:?}", d0);
        assert!(d0.as_millis() <= 250, "backoff too large: {:?}", d0);

        // Higher attempt → larger backoff (capped to max_backoff_ms)
        let d5 = calculate_backoff(5, 200, 5000);
        assert!(d5.as_millis() >= 3000, "backoff too small: {:?}", d5);
        assert!(
            d5.as_millis() <= 5000,
            "backoff must not exceed max_ms: {:?}",
            d5
        );

        // Very high attempt → capped to max
        let d10 = calculate_backoff(10, 200, 5000);
        assert!(d10.as_millis() <= 5000);
    }

    #[test]
    fn test_backoff_overflow_safety() {
        let d64 = calculate_backoff(64, 200, 5000);
        assert!(
            d64.as_millis() >= 3000,
            "attempt=64 should cap to max, got {:?}",
            d64
        );
        assert!(d64.as_millis() <= 5000);

        let d100 = calculate_backoff(100, 200, 5000);
        assert!(
            d100.as_millis() >= 3000,
            "attempt=100 should cap to max, got {:?}",
            d100
        );
        assert!(d100.as_millis() <= 5000);
    }

    #[test]
    fn test_generate_session_id() {
        let id1 = generate_session_id();
        assert!(id1.starts_with("rivven-pc-"));
        let parts: Vec<&str> = id1.split('-').collect();
        assert!(parts.len() >= 4, "session_id format: {:?}", parts);

        let id2 = generate_session_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_circuit_breaker_allows_when_closed() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 1,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CBState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 60,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CBState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CBState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 0,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CBState::Open);

        assert!(cb.allow_request());
        assert_eq!(cb.state(), CBState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CBState::Closed);
    }

    #[test]
    fn test_circuit_breaker_record_failure_exact_threshold() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 60,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);

        cb.record_failure(); // 1
        assert_eq!(cb.state(), CBState::Closed);

        cb.record_failure(); // 2 == threshold → opens
        assert_eq!(cb.state(), CBState::Open);

        // Additional failures beyond threshold — no re-trigger
        cb.record_failure(); // 3
        assert_eq!(cb.state(), CBState::Open);
    }

    #[test]
    fn test_circuit_breaker_diagnostics() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 60,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);

        let (state, failures, half_open_ok) = cb.diagnostics();
        assert_eq!(state, CBState::Closed);
        assert_eq!(failures, 0);
        assert_eq!(half_open_ok, 0);

        cb.record_failure();
        cb.record_failure();
        cb.record_failure();

        let (state, failures, _) = cb.diagnostics();
        assert_eq!(state, CBState::Open);
        assert_eq!(failures, 3);
    }

    #[test]
    fn test_cb_config_defaults() {
        let cfg = PineconeCBConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.failure_threshold, 5);
        assert_eq!(cfg.reset_timeout_secs, 30);
        assert_eq!(cfg.success_threshold, 2);
    }

    #[test]
    fn test_cb_config_validation() {
        let valid = PineconeCBConfig::default();
        assert!(valid.validate().is_ok());

        // failure_threshold = 0
        let bad = PineconeCBConfig {
            failure_threshold: 0,
            ..Default::default()
        };
        assert!(bad.validate().is_err());

        // failure_threshold > 100
        let too_high = PineconeCBConfig {
            failure_threshold: 101,
            ..Default::default()
        };
        assert!(too_high.validate().is_err());

        // reset_timeout_secs > 3600
        let bad_timeout = PineconeCBConfig {
            reset_timeout_secs: 7200,
            ..Default::default()
        };
        assert!(bad_timeout.validate().is_err());

        // success_threshold = 0
        let zero_success = PineconeCBConfig {
            success_threshold: 0,
            ..Default::default()
        };
        assert!(zero_success.validate().is_err());

        // success_threshold > 10
        let bad_success = PineconeCBConfig {
            success_threshold: 11,
            ..Default::default()
        };
        assert!(bad_success.validate().is_err());
    }

    #[test]
    fn test_nested_cb_config_validation() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            circuit_breaker: PineconeCBConfig {
                failure_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_with_config_constructor() {
        let config = PineconeSinkConfig::default();
        let sink = PineconeSink::with_config(&config);
        assert!(sink.circuit_breaker.is_some());

        let mut config_no_cb = PineconeSinkConfig::default();
        config_no_cb.circuit_breaker.enabled = false;
        let sink2 = PineconeSink::with_config(&config_no_cb);
        assert!(sink2.circuit_breaker.is_none());
    }

    #[test]
    fn test_config_retry_bounds() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            max_retries: 10,
            initial_backoff_ms: 50,
            max_backoff_ms: 300_000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.validate_retry().is_ok());

        // max_retries = 0 (no retries)
        let no_retry = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            max_retries: 0,
            ..Default::default()
        };
        assert!(no_retry.validate().is_ok());
    }

    #[test]
    fn test_error_variant_retryability() {
        // Retryable error classes
        assert!(ConnectorError::RateLimited("rate limited".into()).is_retryable());
        assert!(ConnectorError::Timeout("deadline exceeded".into()).is_retryable());
        assert!(ConnectorError::Connection("connection refused".into()).is_retryable());
        // Non-retryable error classes
        assert!(!ConnectorError::Auth("unauthenticated".into()).is_retryable());
        assert!(!ConnectorError::NotFound("index not found".into()).is_retryable());
        assert!(!ConnectorError::Schema("dimension mismatch".into()).is_retryable());
        assert!(!ConnectorError::Fatal("internal error".into()).is_retryable());
    }

    #[test]
    fn test_classify_error_message_patterns() {
        // Connection patterns
        assert!(matches!(
            classify_error_message("unavailable: connection refused"),
            ConnectorError::Connection(_)
        ));
        assert!(matches!(
            classify_error_message("transport error: hyper error"),
            ConnectorError::Connection(_)
        ));
        assert!(matches!(
            classify_error_message("dns error: no record found"),
            ConnectorError::Connection(_)
        ));

        // Auth patterns
        assert!(matches!(
            classify_error_message("unauthenticated: bad api key"),
            ConnectorError::Auth(_)
        ));
        assert!(matches!(
            classify_error_message("permission denied"),
            ConnectorError::Auth(_)
        ));

        // Schema patterns
        assert!(matches!(
            classify_error_message("invalid_argument: vector dimension mismatch"),
            ConnectorError::Schema(_)
        ));

        // Rate limiting
        assert!(matches!(
            classify_error_message("resource exhausted: too many requests"),
            ConnectorError::RateLimited(_)
        ));

        // Timeout
        assert!(matches!(
            classify_error_message("deadline exceeded"),
            ConnectorError::Timeout(_)
        ));

        // Not found
        assert!(matches!(
            classify_error_message("not found: index doesn't exist"),
            ConnectorError::NotFound(_)
        ));

        // Unknown → Fatal
        assert!(matches!(
            classify_error_message("some random error"),
            ConnectorError::Fatal(_)
        ));
    }

    #[test]
    fn test_retry_params_from_config() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            max_retries: 7,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            ..Default::default()
        };

        let retry = RetryParams::from(&config);
        assert_eq!(retry.max_retries, 7);
        assert_eq!(retry.initial_backoff_ms, 100);
        assert_eq!(retry.max_backoff_ms, 10_000);
        assert_eq!(retry.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_cb_half_open_multiple_successes() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 0,
            success_threshold: 3,
        };
        let cb = CircuitBreaker::new(&config);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CBState::Open);

        // Transition to half-open
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CBState::HalfOpen);

        // First two successes — still half-open
        cb.record_success();
        assert_eq!(cb.state(), CBState::HalfOpen);
        cb.record_success();
        assert_eq!(cb.state(), CBState::HalfOpen);

        // Third success — circuit closes
        cb.record_success();
        assert_eq!(cb.state(), CBState::Closed);
    }

    #[test]
    fn test_cb_half_open_failure_reopens() {
        let config = PineconeCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 0,
            success_threshold: 3,
        };
        let cb = CircuitBreaker::new(&config);

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CBState::Open);

        // Transition to half-open
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CBState::HalfOpen);

        // A single failure in HalfOpen must immediately re-open
        cb.record_failure();
        assert_eq!(
            cb.state(),
            CBState::Open,
            "HalfOpen must re-open on any failure"
        );
    }

    #[test]
    fn test_build_vector_delete_event() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Delete,
            stream: "orders".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": "order-99",
                "embedding": [0.5, 0.6]
            }),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let (vector, bytes) = build_vector(&event, &config).unwrap();
        assert_eq!(vector.id, "order-99");
        assert!(bytes > 0);
        let meta = vector.metadata.as_ref().unwrap();
        assert!(meta.fields.contains_key("_event_type"));
    }

    #[test]
    fn test_build_vector_large_payload() {
        use chrono::Utc;

        // Simulate a high-field-count event (50 fields + vector)
        let mut data = serde_json::Map::new();
        data.insert("id".to_string(), serde_json::json!("stress-1"));
        data.insert(
            "embedding".to_string(),
            serde_json::json!(vec![0.1f32; 384]),
        );
        for i in 0..50 {
            data.insert(
                format!("field_{}", i),
                serde_json::Value::String(format!("value_{}", i)),
            );
        }

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "stress".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::Value::Object(data),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let (vector, bytes) = build_vector(&event, &config).unwrap();
        assert_eq!(vector.id, "stress-1");
        // Should have all fields minus the vector field, plus 3-4 metadata fields
        let meta = vector.metadata.as_ref().unwrap();
        let field_count = meta.fields.len();
        assert!(
            field_count >= 53,
            "Expected >= 53 metadata fields (50 data + id + 3 meta), got {}",
            field_count
        );
        // Byte estimate should be positive and proportional
        assert!(bytes > 384 * 4, "bytes should include vector cost");
    }

    #[test]
    fn test_fnv1a_stability() {
        // Verify FNV-1a produces stable hashes for the same input
        let empty = serde_json::json!({});
        let id1 = extract_vector_id(&empty, &PineconeIdStrategy::Hash, "").unwrap();
        let id2 = extract_vector_id(&empty, &PineconeIdStrategy::Hash, "").unwrap();
        assert_eq!(id1, id2, "FNV-1a must be stable");

        // Different inputs produce different hashes
        let a = serde_json::json!({"x": 1});
        let b = serde_json::json!({"x": 2});
        let ha = extract_vector_id(&a, &PineconeIdStrategy::Hash, "").unwrap();
        let hb = extract_vector_id(&b, &PineconeIdStrategy::Hash, "").unwrap();
        assert_ne!(ha, hb);
    }

    // ── Pass-2 audit hardening tests ────────────────────────────────

    #[test]
    fn test_non_finite_float_id_rejected() {
        // NaN
        let nan = serde_json::json!({"id": f64::NAN});
        assert!(extract_vector_id(&nan, &PineconeIdStrategy::Field, "id").is_err());

        // Infinity
        let inf = serde_json::json!({"id": f64::INFINITY});
        assert!(extract_vector_id(&inf, &PineconeIdStrategy::Field, "id").is_err());

        // Negative infinity
        let neg_inf = serde_json::json!({"id": f64::NEG_INFINITY});
        assert!(extract_vector_id(&neg_inf, &PineconeIdStrategy::Field, "id").is_err());

        // Normal float is fine
        let ok = serde_json::json!({"id": 7.5});
        assert!(extract_vector_id(&ok, &PineconeIdStrategy::Field, "id").is_ok());
    }

    #[test]
    fn test_sparse_index_u32_overflow_rejected() {
        // u64 value > u32::MAX should be rejected
        let data = serde_json::json!({
            "idx": [0, 4294967296u64], // 2^32
            "vals": [0.5, 0.8]
        });
        assert!(extract_sparse_vector(&data, "idx", "vals").is_err());

        // u32::MAX should be accepted
        let ok = serde_json::json!({
            "idx": [0, 4294967295u64], // u32::MAX
            "vals": [0.5, 0.8]
        });
        assert!(extract_sparse_vector(&ok, "idx", "vals").is_ok());
    }

    #[test]
    fn test_build_vector_empty_vector_rejected() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": "vec-1",
                "embedding": []
            }),
            metadata: EventMetadata::default(),
        };

        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };

        let result = build_vector(&event, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty array"));
    }

    #[test]
    fn test_timeout_secs_config() {
        let yaml = r#"
api_key: "test-key"
index_host: "https://test.pinecone.io"
timeout_secs: 60
"#;
        let config: PineconeSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.timeout_secs, 60);

        // Default
        let yaml_default = r#"
api_key: "test-key"
index_host: "https://test.pinecone.io"
"#;
        let config2: PineconeSinkConfig = serde_yaml::from_str(yaml_default).unwrap();
        assert_eq!(config2.timeout_secs, 30);
    }

    #[test]
    fn test_pinecone_sink_debug() {
        let sink = PineconeSink::new();
        let debug_str = format!("{:?}", sink);
        assert!(debug_str.contains("PineconeSink"));
        assert!(debug_str.contains("circuit_breaker_enabled"));
    }

    #[test]
    fn test_id_strategy_hash_derive() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(PineconeIdStrategy::Field);
        set.insert(PineconeIdStrategy::Uuid);
        set.insert(PineconeIdStrategy::Hash);
        assert_eq!(set.len(), 3);
    }

    // ── Pass-3 audit hardening tests ────────────────────────────────

    #[test]
    fn test_extract_vector_f32_overflow() {
        // f64 value > f32::MAX should now be rejected (P3-04)
        let data = serde_json::json!({"v": [1e39]});
        let result = extract_vector(&data, "v");
        assert!(result.is_err(), "f64 > f32::MAX should be rejected");
        assert!(result.unwrap_err().contains("overflows f32"));

        // Negative overflow
        let neg = serde_json::json!({"v": [-1e39]});
        assert!(extract_vector(&neg, "v").is_err());

        // Normal large-but-representable f32 value should be fine
        let ok = serde_json::json!({"v": [3.4e38]});
        assert!(extract_vector(&ok, "v").is_ok());
    }

    #[test]
    fn test_extract_sparse_vector_f32_overflow() {
        // Sparse values with f64 > f32::MAX should be rejected (P3-04)
        let data = serde_json::json!({
            "idx": [0],
            "vals": [1e39]
        });
        let result = extract_sparse_vector(&data, "idx", "vals");
        assert!(result.is_err(), "Sparse f64 > f32::MAX should be rejected");
        assert!(result.unwrap_err().contains("overflows f32"));
    }

    #[test]
    fn test_backoff_never_exceeds_max() {
        // P3-03: jitter-adjusted backoff must never exceed max_backoff_ms
        let max_ms = 5000u64;
        for attempt in 0..20 {
            let d = calculate_backoff(attempt, 200, max_ms);
            assert!(
                d.as_millis() <= max_ms as u128,
                "attempt={} backoff={}ms exceeds max_ms={}",
                attempt,
                d.as_millis(),
                max_ms
            );
        }
    }

    #[test]
    fn test_vector_field_equals_id_field_detected() {
        use chrono::Utc;

        // When vector_field == id_field, build_vector produces a confusing
        // error. P3-09 validates this in write()/check(), but we can also
        // verify the runtime behavior here.
        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "test".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "embedding": [0.1, 0.2],
                "other": "x"
            }),
            metadata: EventMetadata::default(),
        };
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            vector_field: "embedding".to_string(),
            id_field: "embedding".to_string(),
            ..Default::default()
        };
        // build_vector still fails (array not a string/number), but the error
        // message should guide users — the write() validation catches this first.
        let result = build_vector(&event, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_namespace_metadata_always_present() {
        use chrono::Utc;

        // P3-07: _namespace should always be in metadata, even when None
        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": "vec-1",
                "embedding": [0.1, 0.2]
            }),
            metadata: EventMetadata::default(),
        };
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            ..Default::default()
        };
        let (vector, _) = build_vector(&event, &config).unwrap();
        let meta = vector.metadata.as_ref().unwrap();
        assert!(
            meta.fields.contains_key("_namespace"),
            "_namespace must always be present in metadata"
        );
    }

    #[test]
    fn test_uuid_length_validation() {
        // P3-10: UUID must be exactly 32 or 36 chars
        // 32 hex (no hyphens)
        let ok_32 = serde_json::json!({"id": "550e8400e29b41d4a716446655440000"});
        assert!(extract_vector_id(&ok_32, &PineconeIdStrategy::Uuid, "id").is_ok());

        // 36 chars (with hyphens)
        let ok_36 = serde_json::json!({"id": "550e8400-e29b-41d4-a716-446655440000"});
        assert!(extract_vector_id(&ok_36, &PineconeIdStrategy::Uuid, "id").is_ok());

        // Arbitrary hyphens (33 chars: 32 hex + 1 hyphen) must be rejected
        let bad_33 = serde_json::json!({"id": "550e8400e29b41d4-a716446655440000"});
        assert!(
            extract_vector_id(&bad_33, &PineconeIdStrategy::Uuid, "id").is_err(),
            "33-char UUID with misplaced hyphen should be rejected"
        );

        // Too many hyphens (40 chars)
        let too_many = serde_json::json!({"id": "5-5-0-e-8-4-0-0-e-2-9-b-4-1-d-4-a-7-1-6-4-4-6-6-5-5-4-4-0-0-0-0"});
        assert!(
            extract_vector_id(&too_many, &PineconeIdStrategy::Uuid, "id").is_err(),
            "UUID with arbitrary hyphen placement should be rejected"
        );
    }

    #[test]
    fn test_batch_size_one_config_valid() {
        let config = PineconeSinkConfig {
            api_key: SensitiveString::new("test"),
            index_host: "https://test.pinecone.io".to_string(),
            batch_size: 1,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cb_config_from_write_not_factory() {
        // P3-01: Verify that PineconeSink::with_config uses user's CB config
        let mut config = PineconeSinkConfig::default();
        config.circuit_breaker.enabled = false;
        let sink = PineconeSink::with_config(&config);
        assert!(
            sink.circuit_breaker.is_none(),
            "CB should be disabled when config says enabled=false"
        );

        let mut config2 = PineconeSinkConfig::default();
        config2.circuit_breaker.failure_threshold = 99;
        let sink2 = PineconeSink::with_config(&config2);
        let cb = sink2.circuit_breaker.as_ref().unwrap();
        assert_eq!(cb.failure_threshold, 99);
    }

    // ── Pass-4 audit hardening tests ────────────────────────────────

    #[test]
    fn test_uuid_uppercase_hex_accepted() {
        // P4-06: is_ascii_hexdigit accepts uppercase; confirm with a test
        let upper = serde_json::json!({"id": "550E8400-E29B-41D4-A716-446655440000"});
        let id = extract_vector_id(&upper, &PineconeIdStrategy::Uuid, "id").unwrap();
        assert_eq!(id, "550E8400-E29B-41D4-A716-446655440000");

        // Mixed case
        let mixed = serde_json::json!({"id": "550e8400-E29B-41d4-A716-446655440000"});
        assert!(extract_vector_id(&mixed, &PineconeIdStrategy::Uuid, "id").is_ok());

        // Uppercase non-hyphenated
        let upper_no_hyphens = serde_json::json!({"id": "550E8400E29B41D4A716446655440000"});
        assert!(extract_vector_id(&upper_no_hyphens, &PineconeIdStrategy::Uuid, "id").is_ok());
    }

    #[test]
    fn test_build_vector_non_object_data() {
        // P4-05: Non-object event.data (array, null, string, number) should fail
        use chrono::Utc;

        for data in [
            serde_json::json!(null),
            serde_json::json!([0.1, 0.2]),
            serde_json::json!("string"),
            serde_json::json!(42),
        ] {
            let event = SourceEvent {
                event_type: SourceEventType::Insert,
                stream: "test".to_string(),
                namespace: None,
                timestamp: Utc::now(),
                data,
                metadata: EventMetadata::default(),
            };
            let config = PineconeSinkConfig {
                api_key: SensitiveString::new("test"),
                index_host: "https://test.pinecone.io".to_string(),
                ..Default::default()
            };
            assert!(
                build_vector(&event, &config).is_err(),
                "Non-object event data should fail"
            );
        }
    }

    #[test]
    fn test_classify_error_message_invalid_narrowed() {
        // P4-02: "invalid" substring no longer matches broadly.
        // "invalidated" should NOT be classified as Schema.
        assert!(
            matches!(
                classify_error_message("internal: cached state invalidated"),
                ConnectorError::Fatal(_)
            ),
            "\"invalidated\" should fall through to Fatal, not Schema"
        );

        // "invalid_argument" still matches Schema
        assert!(matches!(
            classify_error_message("invalid_argument: vector dimension mismatch"),
            ConnectorError::Schema(_)
        ));

        // "invalid argument" (space) matches Schema
        assert!(matches!(
            classify_error_message("invalid argument: bad vector"),
            ConnectorError::Schema(_)
        ));

        // "invalid vector" matches Schema
        assert!(matches!(
            classify_error_message("invalid vector: wrong size"),
            ConnectorError::Schema(_)
        ));
    }

    #[test]
    fn test_extract_vector_empty_array_rejected_early() {
        // P4-09: extract_vector itself rejects empty arrays
        let empty = serde_json::json!({"v": []});
        let result = extract_vector(&empty, "v");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("empty array"));
    }

    // ── Pass-5: gRPC client migration tests ──────────────────────

    #[test]
    fn test_classify_pinecone_error_by_grpc_code() {
        // Unauthenticated → Auth
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "Unauthenticated".into(),
                message: "Invalid API key".into()
            }),
            ConnectorError::Auth(_)
        ));
        // PermissionDenied → Auth
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "PermissionDenied".into(),
                message: "Forbidden".into()
            }),
            ConnectorError::Auth(_)
        ));
        // NotFound → NotFound
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "NotFound".into(),
                message: "Index not found".into()
            }),
            ConnectorError::NotFound(_)
        ));
        // InvalidArgument → Schema
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "InvalidArgument".into(),
                message: "Bad Request".into()
            }),
            ConnectorError::Schema(_)
        ));
        // FailedPrecondition → Schema
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "FailedPrecondition".into(),
                message: "Unprocessable".into()
            }),
            ConnectorError::Schema(_)
        ));
        // ResourceExhausted → RateLimited
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "ResourceExhausted".into(),
                message: "Too Many Requests".into()
            }),
            ConnectorError::RateLimited(_)
        ));
        // DeadlineExceeded → Timeout
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "DeadlineExceeded".into(),
                message: "Request Timeout".into()
            }),
            ConnectorError::Timeout(_)
        ));
        // Unavailable → Connection (retryable)
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "Unavailable".into(),
                message: "Service Unavailable".into()
            }),
            ConnectorError::Connection(_)
        ));
        // Internal → Connection (retryable)
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "Internal".into(),
                message: "Internal Server Error".into()
            }),
            ConnectorError::Connection(_)
        ));
        // Unknown → Connection (retryable)
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Api {
                code: "Unknown".into(),
                message: "Unknown Error".into()
            }),
            ConnectorError::Connection(_)
        ));
        // Timeout variant → Timeout
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Timeout("deadline".into())),
            ConnectorError::Timeout(_)
        ));
        // Connection variant → Connection
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Connection("refused".into())),
            ConnectorError::Connection(_)
        ));
        // Serialization variant → Serialization
        assert!(matches!(
            classify_pinecone_error(&PineconeError::Serialization("bad proto".into())),
            ConnectorError::Serialization(_)
        ));
    }

    #[test]
    fn test_classify_pinecone_error_truncates_long_message() {
        let long_msg = "x".repeat(1024);
        let err = PineconeError::Api {
            code: "Internal".into(),
            message: long_msg,
        };
        let classified = classify_pinecone_error(&err);
        let msg = classified.to_string();
        // Message should be truncated to ~512 chars + "..."
        assert!(
            msg.len() < 600,
            "Expected truncated message, got len={}",
            msg.len()
        );
        assert!(msg.contains("..."));
    }

    // ── Pass-6: comprehensive gRPC code coverage tests ─────────

    #[test]
    fn test_classify_grpc_already_exists() {
        let err = PineconeError::Api {
            code: "AlreadyExists".into(),
            message: "Vector ID already exists".into(),
        };
        assert!(
            matches!(classify_pinecone_error(&err), ConnectorError::Schema(_)),
            "AlreadyExists should map to Schema"
        );
    }

    #[test]
    fn test_classify_grpc_out_of_range() {
        let err = PineconeError::Api {
            code: "OutOfRange".into(),
            message: "Dimension out of range".into(),
        };
        assert!(
            matches!(classify_pinecone_error(&err), ConnectorError::Schema(_)),
            "OutOfRange should map to Schema"
        );
    }

    #[test]
    fn test_classify_grpc_cancelled() {
        let err = PineconeError::Api {
            code: "Cancelled".into(),
            message: "Request cancelled".into(),
        };
        assert!(
            matches!(classify_pinecone_error(&err), ConnectorError::Connection(_)),
            "Cancelled should map to Connection (retryable)"
        );
        // Verify retryable
        assert!(classify_pinecone_error(&err).is_retryable());
    }

    #[test]
    fn test_classify_grpc_unimplemented() {
        let err = PineconeError::Api {
            code: "Unimplemented".into(),
            message: "Method not supported".into(),
        };
        assert!(
            matches!(classify_pinecone_error(&err), ConnectorError::Fatal(_)),
            "Unimplemented should map to Fatal (non-retryable)"
        );
        assert!(!classify_pinecone_error(&err).is_retryable());
    }

    #[test]
    fn test_classify_grpc_data_loss() {
        let err = PineconeError::Api {
            code: "DataLoss".into(),
            message: "Unrecoverable data loss".into(),
        };
        assert!(
            matches!(classify_pinecone_error(&err), ConnectorError::Fatal(_)),
            "DataLoss should map to Fatal (non-retryable)"
        );
        assert!(!classify_pinecone_error(&err).is_retryable());
    }
}
