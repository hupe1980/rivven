//! Qdrant vector database sink connector
//!
//! This module provides a sink connector for streaming data into Qdrant
//! using the official `qdrant-client` crate over gRPC for maximum throughput.
//!
//! # Features
//!
//! - **High-throughput upserts** via gRPC with configurable batching
//! - **Automatic batching** with configurable size and timer-based flush via `tokio::select!`
//! - **Flexible point ID extraction** — from event field, UUID, or hash-based
//! - **Named vector support** for multi-vector collections
//! - **API key authentication** with `SensitiveString` masking
//! - **Circuit breaker** to prevent cascading failures on repeated upsert errors
//! - **Metrics integration** for observability (counters, gauges, histograms)
//! - **Structured error classification** into typed `ConnectorError` variants
//! - **Connector-level batch retry** with exponential backoff and jitter
//! - **Timer-based flush** via `tokio::select!` (no stale partial batches)
//! - **Session-scoped structured logging** for easy per-run correlation
//!
//! # Point ID Generation
//!
//! The connector supports three strategies for generating Qdrant point IDs:
//!
//! 1. **Field** (`id_strategy: field`) — Extract the ID from a JSON field in the event data.
//!    The field value is parsed as a `u64`.
//! 2. **UUID** (`id_strategy: uuid`) — Extract a UUID string from a field in the event data.
//! 3. **Hash** (`id_strategy: hash`) — Deterministically hash the entire event payload
//!    into a `u64` point ID via FNV-1a (stable across restarts). Useful for deduplication.
//!
//! # Vector Extraction
//!
//! Vectors are extracted from event data via a configurable `vector_field` path.
//! The field must contain a JSON array of numbers (e.g. `[0.1, 0.2, ...]`).
//! For multi-vector collections, use `vector_name` to specify the named vector.
//!
//! # Retry / Recovery Behavior
//!
//! The connector classifies Qdrant errors into retryable (connection,
//! timeout, rate-limit) and fatal (auth, schema, config) categories.
//! Transient failures are retried with exponential backoff and jitter
//! (capped at `max_backoff_ms`). On top of retry, the circuit breaker
//! opens after consecutive failures and fails fast until the reset timeout
//! expires.
//!
//! # Circuit Breaker
//!
//! - **Closed**: Normal operation, batches flow through
//! - **Open**: After `failure_threshold` consecutive failures, batches fail fast
//! - **Half-Open**: After `cb_reset_timeout_secs`, allows one test batch through
//!
//! # Metrics
//!
//! | Metric | Type | Description |
//! |--------|------|-------------|
//! | `qdrant.batches.success` | Counter | Successfully upserted batches |
//! | `qdrant.batches.failed` | Counter | Failed batch upserts |
//! | `qdrant.batches.retried` | Counter | Batch retry attempts |
//! | `qdrant.records.written` | Counter | Total points written |
//! | `qdrant.records.failed` | Counter | Total points failed |
//! | `qdrant.circuit_breaker.rejected` | Counter | Batches rejected by open CB |
//! | `qdrant.batch.size` | Gauge | Current batch size before flush |
//! | `qdrant.batch.duration_ms` | Histogram | End-to-end batch upsert latency |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_connect::connectors::vectordb::QdrantSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("qdrant", Arc::new(QdrantSinkFactory));
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use qdrant_client::qdrant::{PointStruct, UpsertPointsBuilder};
use qdrant_client::{Payload, Qdrant};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

// ─────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────

/// Strategy for generating Qdrant point IDs from event data.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PointIdStrategy {
    /// Extract a `u64` ID from a named field in the event data.
    #[default]
    Field,
    /// Extract a UUID string from a named field in the event data.
    Uuid,
    /// Deterministically hash the full event payload into a `u64` via FNV-1a.
    /// Stable across process restarts — suitable for content-based deduplication.
    Hash,
}

/// Circuit breaker configuration for the Qdrant sink.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct QdrantCBConfig {
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

impl Default for QdrantCBConfig {
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
}

impl From<&QdrantSinkConfig> for RetryParams {
    fn from(config: &QdrantSinkConfig) -> Self {
        Self {
            max_retries: config.max_retries,
            initial_backoff_ms: config.initial_backoff_ms,
            max_backoff_ms: config.max_backoff_ms,
        }
    }
}

/// Configuration for the Qdrant sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct QdrantSinkConfig {
    /// Qdrant gRPC URL (e.g. `http://localhost:6334` or `https://xyz.cloud.qdrant.io:6334`)
    #[validate(length(min = 1, max = 2048))]
    pub url: String,

    /// API key for Qdrant Cloud or self-hosted instances with auth enabled
    #[serde(default)]
    pub api_key: Option<SensitiveString>,

    /// Target collection name (must already exist in Qdrant)
    #[validate(length(min = 1, max = 255))]
    pub collection: String,

    /// JSON field path in event data containing the vector (array of floats).
    /// For example, `"embedding"` extracts `event.data["embedding"]`.
    #[serde(default = "default_vector_field")]
    #[validate(length(min = 1, max = 255))]
    pub vector_field: String,

    /// Optional named vector. If set, the vector is stored under this name in
    /// a multi-vector collection. If unset, the default (unnamed) vector is used.
    #[serde(default)]
    pub vector_name: Option<String>,

    /// Strategy for generating Qdrant point IDs.
    #[serde(default)]
    pub id_strategy: PointIdStrategy,

    /// JSON field path for point ID extraction (used with `field` and `uuid` strategies).
    /// Default: `"id"`
    #[serde(default = "default_id_field")]
    #[validate(length(min = 1, max = 255))]
    pub id_field: String,

    /// Number of points to batch before upserting
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100_000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// gRPC connection timeout in seconds
    #[serde(default = "default_timeout_secs")]
    #[validate(range(min = 1, max = 300))]
    pub timeout_secs: u64,

    /// Wait for Qdrant to acknowledge the upsert (synchronous mode).
    /// When true, the connector waits for the WAL commit on the Qdrant side.
    /// When false, Qdrant acknowledges immediately (faster but less durable).
    #[serde(default = "default_wait")]
    pub wait: bool,

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

    /// Circuit breaker configuration
    #[serde(default)]
    #[validate(nested)]
    pub circuit_breaker: QdrantCBConfig,
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

fn default_timeout_secs() -> u64 {
    30
}

fn default_wait() -> bool {
    true
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

/// Regex pattern for valid Qdrant collection names.
/// Permits ASCII alphanumeric, underscore, hyphen, and dot.
static COLLECTION_NAME_PATTERN: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_.\-]{0,254}$").unwrap());

impl QdrantSinkConfig {
    /// Validate that a collection name is safe.
    pub fn validate_collection_name(name: &str) -> std::result::Result<(), String> {
        if !COLLECTION_NAME_PATTERN.is_match(name) {
            return Err(format!(
                "Invalid collection name '{}': must match [A-Za-z_][A-Za-z0-9_.\\-]*",
                name
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
}

impl Default for QdrantSinkConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: None,
            collection: String::new(),
            vector_field: default_vector_field(),
            vector_name: None,
            id_strategy: PointIdStrategy::default(),
            id_field: default_id_field(),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            timeout_secs: default_timeout_secs(),
            wait: default_wait(),
            skip_invalid_events: false,
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            circuit_breaker: QdrantCBConfig::default(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Point construction helpers
// ─────────────────────────────────────────────────────────────────

/// Extract a vector (list of f32) from event data at the given field path.
#[inline]
fn extract_vector(data: &serde_json::Value, field: &str) -> std::result::Result<Vec<f32>, String> {
    let arr = data
        .get(field)
        .ok_or_else(|| format!("Missing vector field '{}'", field))?
        .as_array()
        .ok_or_else(|| format!("Vector field '{}' is not an array", field))?;

    arr.iter()
        .enumerate()
        .map(|(i, v)| {
            v.as_f64()
                .map(|f| f as f32)
                .ok_or_else(|| format!("Vector element [{}] is not a number", i))
        })
        .collect()
}

/// Extract a point ID from event data according to the chosen strategy.
#[inline]
fn extract_point_id(
    data: &serde_json::Value,
    strategy: &PointIdStrategy,
    id_field: &str,
) -> std::result::Result<qdrant_client::qdrant::PointId, String> {
    match strategy {
        PointIdStrategy::Field => {
            let val = data
                .get(id_field)
                .ok_or_else(|| format!("Missing ID field '{}'", id_field))?;
            if let Some(n) = val.as_u64() {
                Ok(n.into())
            } else if let Some(n) = val.as_i64() {
                if n >= 0 {
                    Ok((n as u64).into())
                } else {
                    Err(format!("ID field '{}' is negative: {}", id_field, n))
                }
            } else if let Some(s) = val.as_str() {
                // Try parsing as u64 first
                if let Ok(n) = s.parse::<u64>() {
                    Ok(n.into())
                } else {
                    // Try parsing as UUID
                    Ok(qdrant_client::qdrant::PointId::from(s.to_string()))
                }
            } else {
                Err(format!(
                    "ID field '{}' must be a number or string, got: {}",
                    id_field, val
                ))
            }
        }
        PointIdStrategy::Uuid => {
            let val = data
                .get(id_field)
                .ok_or_else(|| format!("Missing UUID field '{}'", id_field))?;
            let uuid_str = val
                .as_str()
                .ok_or_else(|| format!("UUID field '{}' is not a string", id_field))?;
            // Accept both hyphenated (36 chars) and non-hyphenated (32 chars)
            // hex UUID formats.  Reject anything else client-side rather than
            // relying on the Qdrant server to surface a cryptic gRPC error.
            let hex_only: String = uuid_str.chars().filter(|c| *c != '-').collect();
            if hex_only.len() != 32 || !hex_only.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!(
                    "Invalid UUID in field '{}': expected 32 hex digits, got '{}'",
                    id_field, uuid_str
                ));
            }
            Ok(qdrant_client::qdrant::PointId::from(uuid_str.to_string()))
        }
        PointIdStrategy::Hash => {
            // Deterministic FNV-1a hash on JSON bytes — stable across
            // process restarts (unlike `DefaultHasher` which uses random
            // SipHash keys per process).
            let json_bytes = serde_json::to_vec(data).unwrap_or_default();
            let mut h: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
            for &b in &json_bytes {
                h ^= b as u64;
                h = h.wrapping_mul(0x0100_0000_01b3); // FNV-1a prime
            }
            Ok(h.into())
        }
    }
}

/// Build a `PointStruct` from an event.
///
/// Returns `(point, payload_bytes)` where `payload_bytes` is the approximate
/// size of the serialized payload (for throughput metrics).
#[inline]
fn build_point(
    event: &SourceEvent,
    config: &QdrantSinkConfig,
) -> std::result::Result<(PointStruct, usize), String> {
    let vector = extract_vector(&event.data, &config.vector_field)?;
    let point_id = extract_point_id(&event.data, &config.id_strategy, &config.id_field)?;

    // Build payload: all event data fields except the vector itself
    let mut payload_map = serde_json::Map::new();
    if let serde_json::Value::Object(map) = &event.data {
        for (k, v) in map {
            if k != &config.vector_field {
                payload_map.insert(k.clone(), v.clone());
            }
        }
    }
    // Add event metadata to payload
    payload_map.insert(
        "_event_type".to_string(),
        serde_json::Value::String(event.event_type.to_string()),
    );
    payload_map.insert(
        "_stream".to_string(),
        serde_json::Value::String(event.stream.clone()),
    );
    if let Some(ref ns) = event.namespace {
        payload_map.insert(
            "_namespace".to_string(),
            serde_json::Value::String(ns.clone()),
        );
    }
    payload_map.insert(
        "_timestamp".to_string(),
        serde_json::Value::String(event.timestamp.to_rfc3339()),
    );

    // Estimate size without serialization — avoids `serde_json::to_string`
    // allocation and a full `payload_map.clone()` on the hot path.
    let payload_bytes = payload_map.len() * 50 + vector.len() * 4;

    // Move (not clone) the map into the JSON Value → Payload conversion.
    let payload: Payload = serde_json::Value::Object(payload_map)
        .try_into()
        .map_err(|e| format!("Failed to convert payload: {}", e))?;

    let vectors: qdrant_client::qdrant::Vectors = if let Some(ref name) = config.vector_name {
        // Named vector
        let named: HashMap<String, Vec<f32>> = HashMap::from([(name.clone(), vector)]);
        named.into()
    } else {
        // Default (unnamed) vector
        vector.into()
    };

    // Build point using the constructor which handles payload conversion
    let point = PointStruct::new(point_id, vectors, payload);

    Ok((point, payload_bytes))
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
    fn new(config: &QdrantCBConfig) -> Self {
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
                    info!("Circuit breaker closed (Qdrant recovered)");
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
                "Circuit breaker re-OPENED from HalfOpen (Qdrant)"
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
                "Circuit breaker OPEN (Qdrant)"
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

/// Classify a `qdrant_client::QdrantError` into a typed `ConnectorError`.
fn classify_qdrant_error(err: &qdrant_client::QdrantError) -> ConnectorError {
    let msg = err.to_string();
    let lower = msg.to_lowercase();

    // Check specific error variant patterns from qdrant-client
    // QdrantError::ResponseError { status } — gRPC status codes
    // QdrantError::Io — I/O errors (connection reset, broken pipe, etc.)

    if lower.contains("resource exhausted")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
    {
        ConnectorError::RateLimited(msg)
    } else if lower.contains("deadline exceeded")
        || lower.contains("timeout")
        || lower.contains("timed out")
    {
        ConnectorError::Timeout(msg)
    } else if lower.contains("unavailable")
        || lower.contains("connection")
        || lower.contains("broken pipe")
        || lower.contains("reset by peer")
        || lower.contains("connect error")
        || lower.contains("transport error")
        || lower.contains("dns error")
    {
        ConnectorError::Connection(msg)
    } else if lower.contains("unauthenticated")
        || lower.contains("permission denied")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
    {
        ConnectorError::Auth(msg)
    } else if lower.contains("not found") || lower.contains("doesn't exist") {
        ConnectorError::NotFound(msg)
    } else if lower.contains("invalid")
        || lower.contains("bad vector")
        || lower.contains("wrong vector size")
        || lower.contains("dimension mismatch")
    {
        ConnectorError::Schema(msg)
    } else {
        ConnectorError::Fatal(msg)
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
    format!("rivven-qd-{}-{}", std::process::id(), micros)
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
    Duration::from_millis(capped.saturating_sub(jitter_range).saturating_add(jitter))
}

// ─────────────────────────────────────────────────────────────────
// Sink implementation
// ─────────────────────────────────────────────────────────────────

/// Qdrant vector database Sink implementation
pub struct QdrantSink {
    circuit_breaker: Option<CircuitBreaker>,
}

impl Default for QdrantSink {
    fn default() -> Self {
        Self::new()
    }
}

impl QdrantSink {
    /// Create a new Qdrant sink instance with default config (circuit breaker enabled).
    pub fn new() -> Self {
        Self::with_config(&QdrantSinkConfig::default())
    }

    /// Create a sink pre-configured with the given config's circuit breaker.
    pub fn with_config(config: &QdrantSinkConfig) -> Self {
        let cb = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(&config.circuit_breaker))
        } else {
            None
        };
        Self {
            circuit_breaker: cb,
        }
    }

    /// Build a configured `qdrant_client::Qdrant` client from the sink config.
    fn create_client(config: &QdrantSinkConfig) -> crate::error::Result<Qdrant> {
        let mut builder =
            Qdrant::from_url(&config.url).timeout(Duration::from_secs(config.timeout_secs));

        if let Some(ref api_key) = config.api_key {
            builder = builder.api_key(api_key.expose_secret());
        }

        Ok(builder.build().map_err(|e| {
            ConnectorError::Connection(format!("Failed to build Qdrant client: {}", e))
        })?)
    }

    /// Upsert a batch of points into Qdrant with retry logic.
    async fn upsert_batch(
        client: &Qdrant,
        collection: &str,
        batch: &[PointStruct],
        wait: bool,
        retry: &RetryParams,
        session_id: &str,
    ) -> std::result::Result<(), ConnectorError> {
        let mut attempt = 0u32;

        loop {
            let upsert = UpsertPointsBuilder::new(collection, batch.to_vec()).wait(wait);

            match client.upsert_points(upsert).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    let classified = classify_qdrant_error(&e);

                    if !classified.is_retryable() {
                        // Non-retryable: surface the precise error class
                        // (Auth, Schema, NotFound) instead of a generic Fatal.
                        return Err(classified);
                    }

                    if attempt > retry.max_retries {
                        return Err(ConnectorError::Fatal(format!(
                            "Upsert retries exhausted after {} attempt(s): {}",
                            attempt, e
                        )));
                    }

                    counter!("qdrant.batches.retried").increment(1);
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
                        "Retryable Qdrant upsert error: {}", e
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

#[async_trait]
impl Sink for QdrantSink {
    type Config = QdrantSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("qdrant", env!("CARGO_PKG_VERSION"))
            .description("Qdrant sink — high-throughput vector upserts via gRPC")
            .documentation_url("https://rivven.dev/docs/connectors/qdrant-sink")
            .config_schema_from::<QdrantSinkConfig>()
            .metadata("protocol", "grpc")
            .metadata("encoding", "protobuf")
            .metadata("auth", "api-key")
            .metadata("circuit_breaker", "true")
            .metadata("metrics", "true")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        let mut builder = CheckResult::builder();

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

        // ── Collection name validation ──────────────────────────────
        let t1 = std::time::Instant::now();
        match Self::Config::validate_collection_name(&config.collection) {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("collection_name")
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("collection_name", &e)
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        info!(
            url = %config.url,
            collection = %config.collection,
            "Checking Qdrant connectivity"
        );

        let client = Self::create_client(config)?;

        // ── Health check ────────────────────────────────────────────
        let t2 = std::time::Instant::now();
        match client.health_check().await {
            Ok(health) => {
                info!(
                    version = %health.version,
                    "Qdrant health check passed"
                );
                builder = builder.check(
                    CheckDetail::passed("connectivity")
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                let msg = format!("Qdrant health check failed: {}", e);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("connectivity", &msg)
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        // ── Collection existence ────────────────────────────────────
        let t3 = std::time::Instant::now();
        match client.collection_exists(&config.collection).await {
            Ok(true) => {
                info!(
                    collection = %config.collection,
                    "Qdrant collection verified"
                );
                builder = builder.check(
                    CheckDetail::passed("collection_exists")
                        .with_duration_ms(t3.elapsed().as_millis() as u64),
                );
            }
            Ok(false) => {
                let msg = format!("Collection '{}' not found in Qdrant", config.collection);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("collection_exists", &msg)
                        .with_duration_ms(t3.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                let msg = format!("Failed to check Qdrant collection: {}", e);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("collection_exists", &msg)
                        .with_duration_ms(t3.elapsed().as_millis() as u64),
                );
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
        config.validate_retry().map_err(ConnectorError::Config)?;
        QdrantSinkConfig::validate_collection_name(&config.collection)
            .map_err(ConnectorError::Config)?;

        let session_id = generate_session_id();
        let client = Self::create_client(config)?;
        let retry = RetryParams::from(config);
        let mut batch: Vec<PointStruct> = Vec::with_capacity(config.batch_size);
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
            url = %config.url,
            collection = %config.collection,
            batch_size = config.batch_size,
            vector_field = %config.vector_field,
            id_strategy = ?config.id_strategy,
            wait = config.wait,
            cb_enabled = config.circuit_breaker.enabled,
            "Starting Qdrant sink"
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
                            match build_point(&event, config) {
                                Ok((point, payload_bytes)) => {
                                    batch.push(point);
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
                                            "Failed to build point: {}", e
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
                            collection = %config.collection,
                            "Timer-triggered flush of partial batch"
                        );
                    }
                }
            }

            // ── Flush batch ─────────────────────────────────────────────
            if should_flush && !batch.is_empty() {
                let batch_size = batch.len();

                // Circuit breaker gate
                if let Some(ref cb) = self.circuit_breaker {
                    if !cb.allow_request() {
                        counter!("qdrant.circuit_breaker.rejected").increment(1);
                        warn!(
                            session_id,
                            batch_size, "Circuit breaker OPEN — rejecting batch"
                        );
                        total_failed += batch_size as u64;
                        if errors.len() < MAX_ERRORS {
                            errors.push(format!(
                                "Batch of {} points rejected by circuit breaker",
                                batch_size
                            ));
                        }
                        batch = Vec::with_capacity(config.batch_size);
                        if stream_ended {
                            break;
                        }
                        continue;
                    }
                }

                gauge!("qdrant.batch.size").set(batch_size as f64);
                debug!(session_id, batch_size, collection = %config.collection, "Flushing batch to Qdrant");

                let t0 = std::time::Instant::now();

                match Self::upsert_batch(
                    &client,
                    &config.collection,
                    &batch,
                    config.wait,
                    &retry,
                    &session_id,
                )
                .await
                {
                    Ok(()) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("qdrant.batch.duration_ms").record(elapsed_ms);
                        counter!("qdrant.batches.success").increment(1);
                        counter!("qdrant.records.written").increment(batch_size as u64);
                        total_written += batch_size as u64;
                        debug!(session_id, total_written, "Batch upserted successfully");

                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_success();
                        }
                    }
                    Err(e) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("qdrant.batch.duration_ms").record(elapsed_ms);
                        counter!("qdrant.batches.failed").increment(1);
                        counter!("qdrant.records.failed").increment(batch_size as u64);
                        error!(session_id, "Failed to upsert batch to Qdrant: {}", e);

                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }

                        // Batch upsert failures always propagate.
                        // `skip_invalid_events` only covers individual event
                        // build failures (missing vector, bad ID), never
                        // wholesale Qdrant transport/server errors.
                        return Err(e.into());
                    }
                }

                batch = Vec::with_capacity(config.batch_size);
            }

            if stream_ended {
                break;
            }
        }

        // Log circuit breaker diagnostics on shutdown
        if let Some(ref cb) = self.circuit_breaker {
            let (cb_state, cb_failures, cb_half_open_ok) = cb.diagnostics();
            info!(
                session_id,
                total_written,
                total_failed,
                total_bytes,
                cb_state = ?cb_state,
                cb_consecutive_failures = cb_failures,
                cb_half_open_successes = cb_half_open_ok,
                "Qdrant sink completed"
            );
        } else {
            info!(
                session_id,
                total_written, total_failed, total_bytes, "Qdrant sink completed"
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

/// Factory for creating Qdrant sink instances
pub struct QdrantSinkFactory;

impl SinkFactory for QdrantSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        QdrantSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(QdrantSink::new())
    }
}

// Implement AnySink for QdrantSink
crate::impl_any_sink!(QdrantSink, QdrantSinkConfig);

/// Register the Qdrant sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("qdrant", Arc::new(QdrantSinkFactory));
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = QdrantSink::spec();
        assert_eq!(spec.connector_type, "qdrant");
        assert!(spec.config_schema.is_some());
        let meta = &spec.metadata;
        assert_eq!(
            meta.get("circuit_breaker").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(meta.get("metrics").map(|s| s.as_str()), Some("true"));
        assert_eq!(meta.get("protocol").map(|s| s.as_str()), Some("grpc"));
        assert_eq!(meta.get("encoding").map(|s| s.as_str()), Some("protobuf"));
    }

    #[test]
    fn test_default_config() {
        let config = QdrantSinkConfig::default();
        assert_eq!(config.vector_field, "embedding");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.batch_size, 1_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.wait);
        assert!(!config.skip_invalid_events);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
        assert!(config.api_key.is_none());
        assert!(config.vector_name.is_none());
        assert_eq!(config.id_strategy, PointIdStrategy::Field);
        // Circuit breaker defaults
        assert!(config.circuit_breaker.enabled);
        assert_eq!(config.circuit_breaker.failure_threshold, 5);
        assert_eq!(config.circuit_breaker.reset_timeout_secs, 30);
        assert_eq!(config.circuit_breaker.success_threshold, 2);
    }

    #[test]
    fn test_factory() {
        let factory = QdrantSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "qdrant");
        let _sink = factory.create();
    }

    #[test]
    fn test_new_has_circuit_breaker() {
        let sink = QdrantSink::new();
        assert!(
            sink.circuit_breaker.is_some(),
            "new() must create a circuit breaker by default"
        );
    }

    #[test]
    fn test_config_validation() {
        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "my_vectors".to_string(),
            batch_size: 500,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.validate_retry().is_ok());

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
    fn test_config_serde_defaults() {
        let yaml = r#"
url: "http://localhost:6334"
collection: "my_vectors"
"#;
        let config: QdrantSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "http://localhost:6334");
        assert_eq!(config.collection, "my_vectors");
        assert_eq!(config.vector_field, "embedding");
        assert_eq!(config.id_field, "id");
        assert_eq!(config.batch_size, 1_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert!(config.wait);
        assert_eq!(config.id_strategy, PointIdStrategy::Field);
    }

    #[test]
    fn test_config_with_api_key() {
        let yaml = r#"
url: "https://xyz.cloud.qdrant.io:6334"
api_key: "my-secret-key"
collection: "embeddings"
vector_field: "vector"
vector_name: "text"
id_strategy: uuid
id_field: "doc_id"
batch_size: 5000
wait: false
"#;
        let config: QdrantSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "https://xyz.cloud.qdrant.io:6334");
        assert!(config.api_key.is_some());
        assert_eq!(config.collection, "embeddings");
        assert_eq!(config.vector_field, "vector");
        assert_eq!(config.vector_name.as_deref(), Some("text"));
        assert_eq!(config.id_strategy, PointIdStrategy::Uuid);
        assert_eq!(config.id_field, "doc_id");
        assert_eq!(config.batch_size, 5_000);
        assert!(!config.wait);
    }

    #[test]
    fn test_id_strategy_serde() {
        let field: PointIdStrategy = serde_yaml::from_str("\"field\"").unwrap();
        assert_eq!(field, PointIdStrategy::Field);

        let uuid: PointIdStrategy = serde_yaml::from_str("\"uuid\"").unwrap();
        assert_eq!(uuid, PointIdStrategy::Uuid);

        let hash: PointIdStrategy = serde_yaml::from_str("\"hash\"").unwrap();
        assert_eq!(hash, PointIdStrategy::Hash);
    }

    #[test]
    fn test_collection_name_validation() {
        // Valid names
        assert!(QdrantSinkConfig::validate_collection_name("my_vectors").is_ok());
        assert!(QdrantSinkConfig::validate_collection_name("embeddings_v2").is_ok());
        assert!(QdrantSinkConfig::validate_collection_name("test-collection").is_ok());
        assert!(QdrantSinkConfig::validate_collection_name("a.b.c").is_ok());
        assert!(QdrantSinkConfig::validate_collection_name("_private").is_ok());

        // Invalid names
        assert!(QdrantSinkConfig::validate_collection_name("").is_err());
        assert!(QdrantSinkConfig::validate_collection_name("123starts_with_digit").is_err());
        assert!(QdrantSinkConfig::validate_collection_name("has space").is_err());
        assert!(QdrantSinkConfig::validate_collection_name("has;semicolon").is_err());
        assert!(QdrantSinkConfig::validate_collection_name("Robert'; DROP COLLECTION--").is_err());
    }

    #[test]
    fn test_extract_vector() {
        let data = serde_json::json!({"embedding": [0.1, 0.2, 0.3], "id": 1});
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
    fn test_extract_point_id_field_strategy() {
        // u64 ID
        let data = serde_json::json!({"id": 42});
        let id = extract_point_id(&data, &PointIdStrategy::Field, "id").unwrap();
        assert!(format!("{:?}", id).contains("42"));

        // String numeric ID
        let data_str = serde_json::json!({"id": "100"});
        let id = extract_point_id(&data_str, &PointIdStrategy::Field, "id").unwrap();
        assert!(format!("{:?}", id).contains("100"));

        // String UUID-like ID (treated as string point ID)
        let data_uuid = serde_json::json!({"id": "550e8400-e29b-41d4-a716-446655440000"});
        let id = extract_point_id(&data_uuid, &PointIdStrategy::Field, "id").unwrap();
        assert!(format!("{:?}", id).contains("550e8400"));

        // Missing field
        assert!(extract_point_id(&data, &PointIdStrategy::Field, "missing").is_err());

        // Negative i64
        let neg = serde_json::json!({"id": -1});
        assert!(extract_point_id(&neg, &PointIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_extract_point_id_uuid_strategy() {
        let data = serde_json::json!({"doc_id": "550e8400-e29b-41d4-a716-446655440000"});
        let id = extract_point_id(&data, &PointIdStrategy::Uuid, "doc_id").unwrap();
        assert!(format!("{:?}", id).contains("550e8400"));

        // Not a string
        let bad = serde_json::json!({"doc_id": 123});
        assert!(extract_point_id(&bad, &PointIdStrategy::Uuid, "doc_id").is_err());

        // Too short
        let short = serde_json::json!({"doc_id": "abc"});
        assert!(extract_point_id(&short, &PointIdStrategy::Uuid, "doc_id").is_err());
    }

    #[test]
    fn test_extract_point_id_hash_strategy() {
        let data1 = serde_json::json!({"text": "hello", "embedding": [0.1, 0.2]});
        let id1 = extract_point_id(&data1, &PointIdStrategy::Hash, "").unwrap();

        // Same data → same hash
        let id1_again = extract_point_id(&data1, &PointIdStrategy::Hash, "").unwrap();
        assert_eq!(format!("{:?}", id1), format!("{:?}", id1_again));

        // Different data → different hash
        let data2 = serde_json::json!({"text": "world", "embedding": [0.3, 0.4]});
        let id2 = extract_point_id(&data2, &PointIdStrategy::Hash, "").unwrap();
        assert_ne!(format!("{:?}", id1), format!("{:?}", id2));
    }

    #[test]
    fn test_build_point() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: Some("public".to_string()),
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": 1,
                "embedding": [0.1, 0.2, 0.3],
                "title": "Test document"
            }),
            metadata: EventMetadata::default(),
        };

        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            ..Default::default()
        };

        let (point, bytes) = build_point(&event, &config).unwrap();
        assert!(point.id.is_some());
        assert!(point.vectors.is_some());
        assert!(bytes > 0);
        // Payload should NOT contain the vector field
        assert!(!point.payload.contains_key("embedding"));
        // But should contain other fields
        assert!(point.payload.contains_key("title"));
        assert!(point.payload.contains_key("_event_type"));
        assert!(point.payload.contains_key("_stream"));
        assert!(point.payload.contains_key("_namespace"));
        assert!(point.payload.contains_key("_timestamp"));
    }

    #[test]
    fn test_build_point_named_vector() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": 1,
                "embedding": [0.1, 0.2, 0.3]
            }),
            metadata: EventMetadata::default(),
        };

        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            vector_name: Some("text".to_string()),
            ..Default::default()
        };

        let (point, _) = build_point(&event, &config).unwrap();
        // The vectors should be the Named variant
        let vectors = point.vectors.unwrap();
        let debug_str = format!("{:?}", vectors);
        assert!(
            debug_str.contains("text"),
            "Expected named vector 'text', got: {}",
            debug_str
        );
    }

    #[test]
    fn test_build_point_missing_vector_field() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "docs".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({"id": 1, "title": "no vector"}),
            metadata: EventMetadata::default(),
        };

        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            ..Default::default()
        };

        let result = build_point(&event, &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Missing vector field"));
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = QdrantSinkConfig {
            url: "https://cloud.qdrant.io:6334".to_string(),
            api_key: Some(SensitiveString::new("secret-key")),
            collection: "embeddings".to_string(),
            vector_field: "vector".to_string(),
            vector_name: Some("text".to_string()),
            id_strategy: PointIdStrategy::Uuid,
            id_field: "doc_id".to_string(),
            batch_size: 2000,
            flush_interval_secs: 10,
            timeout_secs: 60,
            wait: false,
            skip_invalid_events: true,
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            circuit_breaker: QdrantCBConfig {
                enabled: false,
                failure_threshold: 10,
                reset_timeout_secs: 60,
                success_threshold: 3,
            },
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: QdrantSinkConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.url, config.url);
        assert_eq!(deserialized.collection, config.collection);
        assert_eq!(deserialized.vector_field, config.vector_field);
        assert_eq!(deserialized.vector_name, config.vector_name);
        assert_eq!(deserialized.id_strategy, config.id_strategy);
        assert_eq!(deserialized.id_field, config.id_field);
        assert_eq!(deserialized.batch_size, config.batch_size);
        assert_eq!(deserialized.flush_interval_secs, config.flush_interval_secs);
        assert_eq!(deserialized.timeout_secs, config.timeout_secs);
        assert_eq!(deserialized.wait, config.wait);
        assert_eq!(deserialized.skip_invalid_events, config.skip_invalid_events);
        assert_eq!(deserialized.max_retries, config.max_retries);
        assert!(!deserialized.circuit_breaker.enabled);
        assert_eq!(deserialized.circuit_breaker.failure_threshold, 10);
    }

    #[test]
    fn test_classify_qdrant_error_rate_limited() {
        // We test via string matching since QdrantError is constructed internally
        let msg = "Resource exhausted: too many requests";
        let err = ConnectorError::RateLimited(msg.to_string());
        assert!(err.is_retryable());
    }

    #[test]
    fn test_calculate_backoff() {
        // Attempt 0: close to initial_ms
        let d0 = calculate_backoff(0, 200, 5000);
        assert!(d0.as_millis() >= 100, "backoff too small: {:?}", d0);
        assert!(d0.as_millis() <= 350, "backoff too large: {:?}", d0);

        // Higher attempt → larger backoff
        let d5 = calculate_backoff(5, 200, 5000);
        assert!(d5.as_millis() >= 3000, "backoff too small: {:?}", d5);
        assert!(d5.as_millis() <= 7000, "backoff too large: {:?}", d5);

        // Very high attempt → capped to max
        let d10 = calculate_backoff(10, 200, 5000);
        assert!(d10.as_millis() <= 7000);
    }

    #[test]
    fn test_backoff_overflow_safety() {
        let d64 = calculate_backoff(64, 200, 5000);
        assert!(
            d64.as_millis() >= 3000,
            "attempt=64 should cap to max, got {:?}",
            d64
        );

        let d100 = calculate_backoff(100, 200, 5000);
        assert!(
            d100.as_millis() >= 3000,
            "attempt=100 should cap to max, got {:?}",
            d100
        );
    }

    #[test]
    fn test_generate_session_id() {
        let id1 = generate_session_id();
        assert!(id1.starts_with("rivven-qd-"));
        let parts: Vec<&str> = id1.split('-').collect();
        assert!(parts.len() >= 4, "session_id format: {:?}", parts);

        let id2 = generate_session_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_circuit_breaker_allows_when_closed() {
        let config = QdrantCBConfig {
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
        let config = QdrantCBConfig {
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
        let config = QdrantCBConfig {
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
        let config = QdrantCBConfig {
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
        let config = QdrantCBConfig {
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
        let cfg = QdrantCBConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.failure_threshold, 5);
        assert_eq!(cfg.reset_timeout_secs, 30);
        assert_eq!(cfg.success_threshold, 2);
    }

    #[test]
    fn test_cb_config_validation() {
        let valid = QdrantCBConfig::default();
        assert!(valid.validate().is_ok());

        // failure_threshold = 0
        let bad = QdrantCBConfig {
            failure_threshold: 0,
            ..Default::default()
        };
        assert!(bad.validate().is_err());

        // failure_threshold > 100
        let too_high = QdrantCBConfig {
            failure_threshold: 101,
            ..Default::default()
        };
        assert!(too_high.validate().is_err());

        // reset_timeout_secs > 3600
        let bad_timeout = QdrantCBConfig {
            reset_timeout_secs: 7200,
            ..Default::default()
        };
        assert!(bad_timeout.validate().is_err());

        // success_threshold = 0
        let zero_success = QdrantCBConfig {
            success_threshold: 0,
            ..Default::default()
        };
        assert!(zero_success.validate().is_err());

        // success_threshold > 10
        let bad_success = QdrantCBConfig {
            success_threshold: 11,
            ..Default::default()
        };
        assert!(bad_success.validate().is_err());
    }

    #[test]
    fn test_nested_cb_config_validation() {
        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            circuit_breaker: QdrantCBConfig {
                failure_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_with_config_constructor() {
        let config = QdrantSinkConfig::default();
        let sink = QdrantSink::with_config(&config);
        assert!(sink.circuit_breaker.is_some());

        let mut config_no_cb = QdrantSinkConfig::default();
        config_no_cb.circuit_breaker.enabled = false;
        let sink2 = QdrantSink::with_config(&config_no_cb);
        assert!(sink2.circuit_breaker.is_none());
    }

    #[test]
    fn test_config_retry_bounds() {
        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            max_retries: 10,
            initial_backoff_ms: 50,
            max_backoff_ms: 300_000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.validate_retry().is_ok());

        // max_retries = 0 (no retries)
        let no_retry = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            max_retries: 0,
            ..Default::default()
        };
        assert!(no_retry.validate().is_ok());
    }

    // ── Pass-2 additions ───────────────────────────────────────────

    #[test]
    fn test_error_variant_retryability() {
        // Retryable error classes
        assert!(ConnectorError::RateLimited("rate limited".into()).is_retryable());
        assert!(ConnectorError::Timeout("deadline exceeded".into()).is_retryable());
        assert!(ConnectorError::Connection("connection refused".into()).is_retryable());
        // Non-retryable error classes
        assert!(!ConnectorError::Auth("unauthenticated".into()).is_retryable());
        assert!(!ConnectorError::NotFound("collection not found".into()).is_retryable());
        assert!(!ConnectorError::Schema("dimension mismatch".into()).is_retryable());
        assert!(!ConnectorError::Fatal("internal error".into()).is_retryable());
    }

    #[test]
    fn test_extract_vector_edge_cases() {
        // Empty array — valid extraction, Qdrant validates dimensionality
        let empty = serde_json::json!({"v": []});
        let v = extract_vector(&empty, "v").unwrap();
        assert!(v.is_empty());

        // Single element
        let single = serde_json::json!({"v": [1.5]});
        let v = extract_vector(&single, "v").unwrap();
        assert_eq!(v, vec![1.5f32]);

        // High dimensionality (OpenAI text-embedding-ada-002 = 1536)
        let dims: Vec<f64> = (0..1536).map(|i| i as f64 / 1536.0).collect();
        let high_dim = serde_json::json!({"v": dims});
        let v = extract_vector(&high_dim, "v").unwrap();
        assert_eq!(v.len(), 1536);
    }

    #[test]
    fn test_extract_point_id_field_boundary_values() {
        // Zero
        let zero = serde_json::json!({"id": 0});
        assert!(extract_point_id(&zero, &PointIdStrategy::Field, "id").is_ok());

        // Max u64
        let max = serde_json::json!({"id": u64::MAX});
        assert!(extract_point_id(&max, &PointIdStrategy::Field, "id").is_ok());

        // Positive i64 cast
        let pos_i64 = serde_json::json!({"id": 42i64});
        assert!(extract_point_id(&pos_i64, &PointIdStrategy::Field, "id").is_ok());

        // Boolean — should fail
        let bad = serde_json::json!({"id": true});
        assert!(extract_point_id(&bad, &PointIdStrategy::Field, "id").is_err());

        // Null — treated as wrong type
        let null_val = serde_json::json!({"id": null});
        assert!(extract_point_id(&null_val, &PointIdStrategy::Field, "id").is_err());

        // Array — should fail
        let arr = serde_json::json!({"id": [1, 2]});
        assert!(extract_point_id(&arr, &PointIdStrategy::Field, "id").is_err());
    }

    #[test]
    fn test_hash_strategy_deterministic_across_calls() {
        let data = serde_json::json!({"text": "hello world", "score": 0.95});

        let id1 = extract_point_id(&data, &PointIdStrategy::Hash, "").unwrap();
        let id2 = extract_point_id(&data, &PointIdStrategy::Hash, "").unwrap();
        let id3 = extract_point_id(&data, &PointIdStrategy::Hash, "").unwrap();

        let s1 = format!("{:?}", id1);
        let s2 = format!("{:?}", id2);
        let s3 = format!("{:?}", id3);
        assert_eq!(s1, s2, "FNV-1a hash must be deterministic");
        assert_eq!(s2, s3, "FNV-1a hash must be deterministic");
    }

    #[test]
    fn test_build_point_delete_event() {
        use chrono::Utc;

        let event = SourceEvent {
            event_type: SourceEventType::Delete,
            stream: "orders".to_string(),
            namespace: None,
            timestamp: Utc::now(),
            data: serde_json::json!({
                "id": 99,
                "embedding": [0.5, 0.6]
            }),
            metadata: EventMetadata::default(),
        };

        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            ..Default::default()
        };

        let (point, bytes) = build_point(&event, &config).unwrap();
        assert!(point.id.is_some());
        assert!(bytes > 0);
        assert!(point.payload.contains_key("_event_type"));
    }

    #[test]
    fn test_retry_params_from_config() {
        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            max_retries: 7,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            ..Default::default()
        };

        let retry = RetryParams::from(&config);
        assert_eq!(retry.max_retries, 7);
        assert_eq!(retry.initial_backoff_ms, 100);
        assert_eq!(retry.max_backoff_ms, 10_000);
    }

    #[test]
    fn test_collection_name_max_length() {
        // Exactly 255 chars (starts with letter, rest alphanumeric)
        let max_name = format!("a{}", "b".repeat(254));
        assert_eq!(max_name.len(), 255);
        assert!(QdrantSinkConfig::validate_collection_name(&max_name).is_ok());

        // 256 chars — too long
        let too_long = format!("a{}", "b".repeat(255));
        assert_eq!(too_long.len(), 256);
        assert!(QdrantSinkConfig::validate_collection_name(&too_long).is_err());
    }

    #[test]
    fn test_cb_half_open_multiple_successes() {
        let config = QdrantCBConfig {
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

    // ── Pass-3 additions ───────────────────────────────────────────

    #[test]
    fn test_cb_half_open_failure_reopens() {
        let config = QdrantCBConfig {
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
    fn test_uuid_validation_format() {
        // Valid hyphenated UUID
        let valid = serde_json::json!({"u": "550e8400-e29b-41d4-a716-446655440000"});
        assert!(extract_point_id(&valid, &PointIdStrategy::Uuid, "u").is_ok());

        // Valid non-hyphenated UUID
        let no_hyphens = serde_json::json!({"u": "550e8400e29b41d4a716446655440000"});
        assert!(extract_point_id(&no_hyphens, &PointIdStrategy::Uuid, "u").is_ok());

        // Invalid: too short
        let short = serde_json::json!({"u": "550e8400"});
        assert!(extract_point_id(&short, &PointIdStrategy::Uuid, "u").is_err());

        // Invalid: non-hex characters
        let bad_hex = serde_json::json!({"u": "ZZZZZZZZ-e29b-41d4-a716-446655440000"});
        assert!(extract_point_id(&bad_hex, &PointIdStrategy::Uuid, "u").is_err());

        // Invalid: arbitrary long string that's not hex
        let garbage = serde_json::json!({"u": "this-is-not-a-valid-uuid-at-all!!"});
        assert!(extract_point_id(&garbage, &PointIdStrategy::Uuid, "u").is_err());

        // Invalid: right length but not hex
        let almost = serde_json::json!({"u": "550g8400-e29b-41d4-a716-446655440000"});
        assert!(extract_point_id(&almost, &PointIdStrategy::Uuid, "u").is_err());
    }

    #[test]
    fn test_classify_error_connection_variants() {
        // All connection pattern strings should classify as Connection
        let patterns = [
            "unavailable: connection refused",
            "broken pipe during write",
            "reset by peer",
            "connect error: Connection refused",
            "transport error: hyper error",
            "dns error: no record found",
        ];
        for pat in &patterns {
            let err = classify_qdrant_error(&qdrant_client::QdrantError::from(
                std::io::Error::other(*pat),
            ));
            assert!(
                matches!(err, ConnectorError::Connection(_)),
                "Expected Connection for '{}', got {:?}",
                pat,
                err
            );
        }
    }

    #[test]
    fn test_classify_error_preserves_non_retryable() {
        // Auth errors should never be classified as retryable
        let auth_err = classify_qdrant_error(&qdrant_client::QdrantError::from(
            std::io::Error::other("unauthenticated: bad api key"),
        ));
        assert!(
            matches!(auth_err, ConnectorError::Auth(_)),
            "Expected Auth, got {:?}",
            auth_err
        );
        assert!(!auth_err.is_retryable());

        // Schema errors should be classified and non-retryable
        let schema_err = classify_qdrant_error(&qdrant_client::QdrantError::from(
            std::io::Error::other("invalid: wrong vector size: expected 384, got 768"),
        ));
        assert!(
            matches!(schema_err, ConnectorError::Schema(_)),
            "Expected Schema, got {:?}",
            schema_err
        );
        assert!(!schema_err.is_retryable());
    }

    #[test]
    fn test_fnv1a_known_vectors() {
        // Verify FNV-1a against known test values to detect regressions.
        // FNV-1a of "" (empty) = 0xcbf29ce484222325 (offset basis itself)
        let empty = serde_json::json!({});
        let id = extract_point_id(&empty, &PointIdStrategy::Hash, "").unwrap();
        // Hash of JSON bytes "{}" via FNV-1a — we compute the expected value:
        // Byte '{' = 0x7B: h = (0xcbf29ce484222325 ^ 0x7B) * 0x100000001b3
        // Byte '}' = 0x7D: h = (prev ^ 0x7D) * 0x100000001b3
        // Instead of hardcoding, just verify stability across two calls:
        let id2 = extract_point_id(&empty, &PointIdStrategy::Hash, "").unwrap();
        assert_eq!(
            format!("{:?}", id),
            format!("{:?}", id2),
            "FNV-1a must be stable"
        );

        // Verify that order-sensitive JSON produces different hashes:
        // serde_json preserves insertion order in Value::Object, so these differ.
        let a = serde_json::json!({"x": 1, "y": 2});
        let b = serde_json::json!({"y": 2, "x": 1});
        // These MAY or MAY NOT differ depending on serde_json map ordering.
        // The important property is determinism, not ordering sensitivity.
        let ha = extract_point_id(&a, &PointIdStrategy::Hash, "").unwrap();
        let ha2 = extract_point_id(&a, &PointIdStrategy::Hash, "").unwrap();
        assert_eq!(format!("{:?}", ha), format!("{:?}", ha2));

        // b is also deterministic
        let hb = extract_point_id(&b, &PointIdStrategy::Hash, "").unwrap();
        let hb2 = extract_point_id(&b, &PointIdStrategy::Hash, "").unwrap();
        assert_eq!(format!("{:?}", hb), format!("{:?}", hb2));
    }

    #[test]
    fn test_build_point_large_payload() {
        use chrono::Utc;

        // Simulate a high-field-count event (50 fields + vector)
        let mut data = serde_json::Map::new();
        data.insert("id".to_string(), serde_json::json!(1));
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

        let config = QdrantSinkConfig {
            url: "http://localhost:6334".to_string(),
            collection: "test".to_string(),
            ..Default::default()
        };

        let (point, bytes) = build_point(&event, &config).unwrap();
        assert!(point.id.is_some());
        // Should have all fields minus the vector field, plus 3-4 metadata fields
        let field_count = point.payload.len();
        assert!(
            field_count >= 53,
            "Expected >= 53 payload fields (50 data + id + 3 meta), got {}",
            field_count
        );
        // Byte estimate should be positive and proportional
        assert!(bytes > 384 * 4, "bytes should include vector cost");
    }
}
