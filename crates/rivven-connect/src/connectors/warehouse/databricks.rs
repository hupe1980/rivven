//! Databricks sink connector
//!
//! This module provides a sink connector for streaming data into Databricks Delta
//! tables using the Zerobus Ingest SDK — a high-performance, async-first gRPC client
//! that handles authentication, retries, stream recovery, and acknowledgment tracking
//! automatically.
//!
//! # Features
//!
//! - **High-throughput streaming** via Databricks Zerobus service
//! - **OAuth 2.0 authentication** with Unity Catalog
//! - **Automatic recovery** with configurable retries on transient failures
//! - **Batch ingestion** for optimal throughput (all-or-nothing semantics)
//! - **JSON serialization** (no protobuf schema generation required)
//! - **Offset tracking** with server-side acknowledgment
//! - **Circuit breaker** to prevent cascading failures on repeated batch errors
//! - **Metrics integration** for observability (counters, gauges, histograms)
//! - **Ack callbacks** for real-time server acknowledgment tracking
//! - **Byte-level throughput** tracking for accurate bandwidth measurement
//! - **Connector-level batch retry** with exponential backoff
//! - **Timer-based flush** via `tokio::select!` (no stale partial batches)
//! - **Graceful shutdown** with flush and unacked record recovery
//!
//! # Authentication
//!
//! The connector authenticates via OAuth 2.0 client credentials against Unity Catalog.
//! You need a **service principal** with:
//!
//! - `SELECT` — read table schema
//! - `MODIFY` — write data to the table
//! - `USE CATALOG` / `USE SCHEMA` — access the catalog and schema
//!
//! # Retry / Recovery Behavior
//!
//! The Zerobus SDK has built-in stream recovery with configurable retries and backoff.
//! On top of that, the connector adds:
//!
//! - **Circuit breaker**: Opens after consecutive failures, fails fast until reset
//! - **Error classification**: Maps Zerobus errors to retryable vs. fatal categories
//! - **Connector-level batch retry**: Exponential backoff for retryable ingest errors
//! - **Batch-level abort**: Non-retryable errors stop ingestion immediately
//!
//! # Circuit Breaker
//!
//! The connector implements a circuit breaker pattern to prevent cascading failures:
//! - **Closed**: Normal operation, batches flow through
//! - **Open**: After consecutive failures, batches fail fast without SDK calls
//! - **Half-Open**: After reset timeout, allows one test batch through
//!
//! # Metrics
//!
//! The connector records the following metrics (via the `metrics` crate):
//!
//! | Metric | Type | Description |
//! |--------|------|-------------|
//! | `databricks.batches.success` | Counter | Successfully ingested batches |
//! | `databricks.batches.failed` | Counter | Failed batch ingestions |
//! | `databricks.batches.retried` | Counter | Batch retry attempts |
//! | `databricks.records.written` | Counter | Total records written |
//! | `databricks.records.failed` | Counter | Total records failed |
//! | `databricks.records.serialization_errors` | Counter | JSON serialization failures |
//! | `databricks.acks.success` | Counter | Server-side offset acknowledgments |
//! | `databricks.acks.error` | Counter | Server-side ack errors |
//! | `databricks.circuit_breaker.rejected` | Counter | Batches rejected by circuit breaker |
//! | `databricks.stream.close_errors` | Counter | Errors during stream close |
//! | `databricks.batch.size` | Gauge | Current batch size (records) |
//! | `databricks.unacked.records` | Gauge | Unacknowledged records on close |
//! | `databricks.batch.duration_ms` | Histogram | End-to-end batch ingest latency |
//! | `databricks.ack.duration_ms` | Histogram | Server acknowledgment latency |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_connect::connectors::warehouse::DatabricksSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("databricks", Arc::new(DatabricksSinkFactory));
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use chrono::Utc;
use databricks_zerobus_ingest_sdk::{
    databricks::zerobus::RecordType, AckCallback, JsonString, OffsetId,
    StreamConfigurationOptions, TableProperties, ZerobusSdk, ZerobusError,
};
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for the Databricks sink connector.
///
/// Connects to a Databricks workspace via the Zerobus Ingest SDK and streams
/// JSON-encoded events into a Delta table.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct DatabricksSinkConfig {
    /// Zerobus API endpoint
    ///
    /// Region-specific endpoint in the form:
    /// `<shard-id>.zerobus.<region>.cloud.databricks.com` (AWS) or
    /// `<shard-id>.zerobus.<region>.azuredatabricks.net` (Azure).
    /// The `https://` scheme is optional.
    #[validate(length(min = 1, max = 512))]
    pub endpoint: String,

    /// Unity Catalog URL
    ///
    /// Workspace URL used for OAuth token retrieval:
    /// `https://<workspace>.cloud.databricks.com` (AWS) or
    /// `https://<workspace>.azuredatabricks.net` (Azure).
    #[validate(length(min = 1, max = 512))]
    pub unity_catalog_url: String,

    /// Fully qualified table name in Unity Catalog
    ///
    /// Format: `catalog.schema.table`
    #[validate(length(min = 5, max = 512))]
    pub table_name: String,

    /// OAuth 2.0 client ID (service principal)
    #[validate(length(min = 1, max = 255))]
    pub client_id: String,

    /// OAuth 2.0 client secret (service principal)
    pub client_secret: SensitiveString,

    /// Number of events to batch before flushing to Databricks.
    ///
    /// The batch is sent with all-or-nothing semantics via `ingest_records_offset`.
    /// Larger batches improve throughput; smaller batches reduce latency.
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 100_000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch.
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Maximum number of unacknowledged records in flight.
    ///
    /// Tune based on available memory and desired throughput.
    #[serde(default = "default_max_inflight")]
    #[validate(range(min = 1, max = 10_000_000))]
    pub max_inflight_requests: usize,

    /// Enable automatic stream recovery on transient failures.
    #[serde(default = "default_recovery")]
    pub recovery: bool,

    /// Maximum number of recovery retry attempts.
    #[serde(default = "default_recovery_retries")]
    #[validate(range(min = 0, max = 100))]
    pub recovery_retries: u32,

    /// Timeout for each recovery attempt in milliseconds.
    #[serde(default = "default_recovery_timeout_ms")]
    #[validate(range(min = 1000, max = 300_000))]
    pub recovery_timeout_ms: u64,

    /// Backoff between recovery retry attempts in milliseconds.
    ///
    /// The SDK waits this duration before retrying after a recovery failure.
    #[serde(default = "default_recovery_backoff_ms")]
    #[validate(range(min = 100, max = 60_000))]
    pub recovery_backoff_ms: u64,

    /// Timeout for server acknowledgement before triggering recovery, in milliseconds.
    ///
    /// If no acknowledgement is received within this time (and there are pending
    /// records), the stream is considered failed and recovery is triggered.
    #[serde(default = "default_server_ack_timeout_ms")]
    #[validate(range(min = 5_000, max = 600_000))]
    pub server_ack_timeout_ms: u64,

    /// Timeout for flush operations in milliseconds.
    #[serde(default = "default_flush_timeout_ms")]
    #[validate(range(min = 1000, max = 600_000))]
    pub flush_timeout_ms: u64,

    /// Whether to wait for server acknowledgment of each batch.
    ///
    /// When `true`, the connector calls `wait_for_offset` after each batch
    /// to guarantee durability before proceeding. When `false`, batches are
    /// fire-and-forget with a final `flush()` on shutdown.
    #[serde(default = "default_wait_for_ack")]
    pub wait_for_ack: bool,

    /// Circuit breaker configuration for protecting against cascading failures.
    #[serde(default)]
    #[validate(nested)]
    pub circuit_breaker: CircuitBreakerConfig,

    /// Maximum connector-level retries per batch.
    ///
    /// These retries are on top of the SDK's built-in stream recovery. Set to 0
    /// to disable connector-level retries (relying solely on SDK recovery).
    #[serde(default = "default_max_batch_retries")]
    #[validate(range(min = 0, max = 10))]
    pub max_batch_retries: u32,

    /// Initial backoff between batch retries in milliseconds.
    #[serde(default = "default_initial_backoff_ms")]
    #[validate(range(min = 10, max = 30_000))]
    pub initial_backoff_ms: u64,

    /// Maximum backoff between batch retries in milliseconds.
    #[serde(default = "default_max_backoff_ms")]
    #[validate(range(min = 100, max = 120_000))]
    pub max_backoff_ms: u64,
}

// ── Circuit breaker configuration ───────────────────────────────────────────

/// Circuit breaker configuration for the Databricks sink.
///
/// Prevents cascading failures by failing fast after consecutive batch errors.
/// When the circuit is open, batches are dropped immediately without contacting
/// the Zerobus service.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct CircuitBreakerConfig {
    /// Whether circuit breaker is enabled.
    #[serde(default = "default_circuit_breaker_enabled")]
    pub enabled: bool,

    /// Number of consecutive batch failures before circuit opens.
    #[serde(default = "default_failure_threshold")]
    #[validate(range(min = 1, max = 100))]
    pub failure_threshold: u32,

    /// Time in seconds to wait before transitioning from open to half-open.
    #[serde(default = "default_reset_timeout_secs")]
    #[validate(range(min = 1, max = 3600))]
    pub reset_timeout_secs: u64,

    /// Number of successful batches in half-open state to close circuit.
    #[serde(default = "default_success_threshold")]
    #[validate(range(min = 1, max = 10))]
    pub success_threshold: u32,
}

fn default_circuit_breaker_enabled() -> bool {
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

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: default_circuit_breaker_enabled(),
            failure_threshold: default_failure_threshold(),
            reset_timeout_secs: default_reset_timeout_secs(),
            success_threshold: default_success_threshold(),
        }
    }
}

// ── Validation helpers ──────────────────────────────────────────────────────

/// Regex for validating fully-qualified table names (`catalog.schema.table`).
static TABLE_NAME_RE: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z_]\w*\.[a-zA-Z_]\w*\.[a-zA-Z_]\w*$").unwrap());

/// Regex for loose endpoint format validation.
///
/// Matches patterns like `<shard>.zerobus.<region>.cloud.databricks.com` (AWS)
/// or `<shard>.zerobus.<region>.azuredatabricks.net` (Azure), with optional scheme.
static ENDPOINT_RE: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?i)^(https?://)?[\w\-]+\.zerobus\.[\w\-]+\.(cloud\.databricks\.com|azuredatabricks\.net)"
        ).unwrap()
    });

// ── Default value functions ─────────────────────────────────────────────────

fn default_batch_size() -> usize {
    500
}

fn default_flush_interval() -> u64 {
    5
}

fn default_max_inflight() -> usize {
    100_000
}

fn default_recovery() -> bool {
    true
}

fn default_recovery_retries() -> u32 {
    4
}

fn default_recovery_timeout_ms() -> u64 {
    15_000
}

fn default_recovery_backoff_ms() -> u64 {
    2_000
}

fn default_server_ack_timeout_ms() -> u64 {
    60_000
}

fn default_flush_timeout_ms() -> u64 {
    300_000
}

fn default_wait_for_ack() -> bool {
    true
}

fn default_max_batch_retries() -> u32 {
    2
}

fn default_initial_backoff_ms() -> u64 {
    200
}

fn default_max_backoff_ms() -> u64 {
    5_000
}

impl Default for DatabricksSinkConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            unity_catalog_url: String::new(),
            table_name: String::new(),
            client_id: String::new(),
            client_secret: SensitiveString::new(""),
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            max_inflight_requests: default_max_inflight(),
            recovery: default_recovery(),
            recovery_retries: default_recovery_retries(),
            recovery_timeout_ms: default_recovery_timeout_ms(),
            recovery_backoff_ms: default_recovery_backoff_ms(),
            server_ack_timeout_ms: default_server_ack_timeout_ms(),
            flush_timeout_ms: default_flush_timeout_ms(),
            wait_for_ack: default_wait_for_ack(),
            circuit_breaker: CircuitBreakerConfig::default(),
            max_batch_retries: default_max_batch_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
        }
    }
}

impl DatabricksSinkConfig {
    /// Validate the table name format (`catalog.schema.table`).
    pub fn validate_table_name(&self) -> std::result::Result<(), String> {
        if !TABLE_NAME_RE.is_match(&self.table_name) {
            return Err(format!(
                "Invalid table name '{}': expected format 'catalog.schema.table'",
                self.table_name
            ));
        }
        Ok(())
    }

    /// Validate the endpoint format.
    ///
    /// Checks that the endpoint follows the expected Databricks Zerobus pattern.
    /// Non-matching endpoints produce a warning (not a hard failure) since
    /// preview/custom endpoints may differ.
    pub fn validate_endpoint(&self) -> std::result::Result<(), String> {
        let ep = self.endpoint.trim();
        if ep.is_empty() {
            return Err("Endpoint must not be empty".to_string());
        }
        if !ENDPOINT_RE.is_match(ep) {
            return Err(format!(
                "Endpoint '{}' does not match expected format: \
                 '<shard>.zerobus.<region>.cloud.databricks.com' or \
                 '<shard>.zerobus.<region>.azuredatabricks.net'",
                ep
            ));
        }
        Ok(())
    }

    /// Build `StreamConfigurationOptions` from this config.
    fn stream_options(&self) -> StreamConfigurationOptions {
        StreamConfigurationOptions {
            max_inflight_requests: self.max_inflight_requests,
            recovery: self.recovery,
            recovery_timeout_ms: self.recovery_timeout_ms,
            recovery_backoff_ms: self.recovery_backoff_ms,
            recovery_retries: self.recovery_retries,
            server_lack_of_ack_timeout_ms: self.server_ack_timeout_ms,
            flush_timeout_ms: self.flush_timeout_ms,
            record_type: RecordType::Json,
            ..Default::default()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Row model
// ─────────────────────────────────────────────────────────────────────────────

/// Row structure serialized as JSON and sent to Databricks via Zerobus.
///
/// Each source event is converted into this shape. The `data` field carries the
/// original event payload, and `metadata` carries connector metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabricksRow {
    event_type: String,
    stream: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    timestamp: String,
    data: serde_json::Value,
    metadata: serde_json::Value,
    ingested_at: String,
}

impl DatabricksRow {
    fn from_event(event: &SourceEvent) -> Self {
        Self {
            event_type: event.event_type.to_string(),
            stream: event.stream.clone(),
            namespace: event.namespace.clone(),
            timestamp: event.timestamp.to_rfc3339(),
            data: event.data.clone(),
            metadata: serde_json::to_value(&event.metadata).unwrap_or_default(),
            ingested_at: Utc::now().to_rfc3339(),
        }
    }

    /// Serialize to a JSON string suitable for `JsonString` ingestion.
    fn to_json_string(&self) -> std::result::Result<String, ConnectorError> {
        serde_json::to_string(self)
            .map_err(|e| ConnectorError::Serialization(format!("Failed to serialize row: {}", e)))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Circuit breaker
// ─────────────────────────────────────────────────────────────────────────────

/// Circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Lock-free circuit breaker for protecting against cascading batch failures.
///
/// Uses atomic operations for state tracking, avoiding contention on the hot path.
struct CircuitBreaker {
    config: CircuitBreakerConfig,
    /// Current consecutive failure count.
    failure_count: AtomicU32,
    /// Current consecutive success count (for half-open state).
    success_count: AtomicU32,
    /// Timestamp when circuit opened (microseconds since epoch), 0 = never opened.
    opened_at: AtomicU64,
}

impl CircuitBreaker {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
        }
    }

    fn state(&self) -> CircuitState {
        if !self.config.enabled {
            return CircuitState::Closed;
        }

        let failures = self.failure_count.load(Ordering::Relaxed);
        if failures < self.config.failure_threshold {
            return CircuitState::Closed;
        }

        let opened_at = self.opened_at.load(Ordering::Relaxed);
        if opened_at == 0 {
            return CircuitState::Closed;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        let reset_timeout_micros = self.config.reset_timeout_secs * 1_000_000;
        if now > opened_at + reset_timeout_micros {
            CircuitState::HalfOpen
        } else {
            CircuitState::Open
        }
    }

    fn allow_request(&self) -> bool {
        match self.state() {
            CircuitState::Closed | CircuitState::HalfOpen => true,
            CircuitState::Open => false,
        }
    }

    fn record_success(&self) {
        match self.state() {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::Relaxed);
            }
            CircuitState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;
                if successes >= self.config.success_threshold {
                    self.failure_count.store(0, Ordering::Relaxed);
                    self.success_count.store(0, Ordering::Relaxed);
                    self.opened_at.store(0, Ordering::Relaxed);
                    debug!(
                        "Databricks circuit breaker closed after {} successful batches",
                        successes
                    );
                }
            }
            CircuitState::Open => {}
        }
    }

    fn record_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;

        if failures == self.config.failure_threshold {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            self.opened_at.store(now, Ordering::Relaxed);
            warn!(
                "Databricks circuit breaker opened after {} consecutive failures",
                failures
            );
        }

        // Reset success count on any failure in half-open state
        if self.state() == CircuitState::HalfOpen {
            self.success_count.store(0, Ordering::Relaxed);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Metrics ack callback
// ─────────────────────────────────────────────────────────────────────────────

/// SDK-level ack callback that records metrics for every server acknowledgment.
///
/// This runs inside the SDK's internal task, so it must be lightweight (no I/O).
struct MetricsAckCallback;

impl AckCallback for MetricsAckCallback {
    fn on_ack(&self, offset_id: OffsetId) {
        counter!("databricks.acks.success").increment(1);
        debug!(offset = offset_id, "Offset acknowledged by server");
    }

    fn on_error(&self, offset_id: OffsetId, error_message: &str) {
        counter!("databricks.acks.error").increment(1);
        error!(offset = offset_id, error = error_message, "Server ack error");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Generate a unique stream identifier for structured tracing.
fn generate_stream_id() -> String {
    format!(
        "rivven-dbx-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros())
            .unwrap_or(0)
    )
}

/// Classify a `ZerobusError` into a typed `ConnectorError`.
///
/// Inspects both `is_retryable()` and the error message to provide fine-grained
/// error classification for monitoring, alerting, and retry decisions.
fn classify_zerobus_error(e: &ZerobusError) -> ConnectorError {
    let msg = e.to_string();
    if e.is_retryable() {
        return ConnectorError::Transient(format!("Retryable Zerobus error: {}", msg));
    }
    // Heuristic classification based on error message
    let lower = msg.to_lowercase();
    if lower.contains("auth")
        || lower.contains("credential")
        || lower.contains("token")
        || lower.contains("oauth")
        || lower.contains("401")
        || lower.contains("403")
    {
        ConnectorError::Auth(format!("Databricks authentication error: {}", msg))
    } else if lower.contains("not found")
        || lower.contains("404")
        || lower.contains("does not exist")
    {
        ConnectorError::NotFound(format!("Databricks resource not found: {}", msg))
    } else if lower.contains("timeout") || lower.contains("timed out") {
        ConnectorError::Timeout(format!("Databricks timeout: {}", msg))
    } else if lower.contains("rate") || lower.contains("throttl") || lower.contains("429") {
        ConnectorError::RateLimited(format!("Databricks rate limited: {}", msg))
    } else {
        ConnectorError::Fatal(format!("Non-retryable Zerobus error: {}", msg))
    }
}

/// Calculate exponential backoff duration with a capped maximum.
///
/// Uses `initial_ms * 2^(attempt-1)` with a ceiling of `max_ms`.
fn calculate_backoff(attempt: u32, initial_ms: u64, max_ms: u64) -> Duration {
    let backoff_ms = initial_ms.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
    Duration::from_millis(backoff_ms.min(max_ms))
}

// ─────────────────────────────────────────────────────────────────────────────
// Sink implementation
// ─────────────────────────────────────────────────────────────────────────────

/// Databricks Sink — streams events into a Databricks Delta table via Zerobus.
///
/// Holds a circuit breaker for batch-level failure protection. The circuit breaker
/// state persists across write calls — consecutive failures across multiple runs
/// will trip the circuit.
pub struct DatabricksSink {
    /// Circuit breaker for protecting against cascading batch failures.
    circuit_breaker: Option<CircuitBreaker>,
}

impl DatabricksSink {
    /// Create a new Databricks sink with default configuration.
    pub fn new() -> Self {
        Self {
            circuit_breaker: Some(CircuitBreaker::new(CircuitBreakerConfig::default())),
        }
    }

    /// Create a new Databricks sink with the given config.
    pub fn with_config(config: &DatabricksSinkConfig) -> Self {
        let circuit_breaker = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(config.circuit_breaker.clone()))
        } else {
            None
        };
        Self { circuit_breaker }
    }

    /// Build a `ZerobusSdk` from config.
    fn build_sdk(
        config: &DatabricksSinkConfig,
    ) -> std::result::Result<ZerobusSdk, ConnectorError> {
        ZerobusSdk::builder()
            .endpoint(&config.endpoint)
            .unity_catalog_url(&config.unity_catalog_url)
            .build()
            .map_err(|e| {
                ConnectorError::Connection(format!("Failed to build Zerobus SDK: {}", e))
            })
    }
}

impl Default for DatabricksSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Sink for DatabricksSink {
    type Config = DatabricksSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("databricks", env!("CARGO_PKG_VERSION"))
            .description(
                "Databricks Delta table sink — high-throughput streaming ingestion via Zerobus",
            )
            .documentation_url("https://rivven.dev/docs/connectors/databricks-sink")
            .config_schema_from::<DatabricksSinkConfig>()
            .metadata("protocol", "grpc")
            .metadata("auth", "oauth2")
            .metadata("format", "json")
            .metadata("sdk", "databricks-zerobus-ingest-sdk")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        info!(
            "Checking Databricks connectivity for table '{}'",
            config.table_name
        );

        let mut builder = CheckResult::builder();

        // 1. Validate table name format
        match config.validate_table_name() {
            Ok(()) => {
                builder = builder.check_passed("table_name_format");
            }
            Err(msg) => {
                return Ok(builder.check_failed("table_name_format", msg).build());
            }
        }

        // 2. Validate endpoint format (warning only — custom endpoints allowed)
        match config.validate_endpoint() {
            Ok(()) => {
                builder = builder.check_passed("endpoint_format");
            }
            Err(msg) => {
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::passed("endpoint_format")
                        .with_message(format!("Warning: {}", msg)),
                );
            }
        }

        // 3. Build SDK (validates endpoint/TLS handshake)
        let sdk = match Self::build_sdk(config) {
            Ok(sdk) => {
                builder = builder.check_passed("sdk_initialization");
                sdk
            }
            Err(e) => {
                return Ok(builder
                    .check_failed("sdk_initialization", e.to_string())
                    .build());
            }
        };

        // 4. Create stream (validates OAuth + table + endpoint connectivity)
        let table_props = TableProperties {
            table_name: config.table_name.clone(),
            descriptor_proto: None,
        };

        let options = StreamConfigurationOptions {
            max_inflight_requests: 1,
            recovery: false,
            record_type: RecordType::Json,
            ..Default::default()
        };

        let start = std::time::Instant::now();
        match sdk
            .create_stream(
                table_props,
                config.client_id.clone(),
                config.client_secret.expose_secret().to_string(),
                Some(options),
            )
            .await
        {
            Ok(mut stream) => {
                let elapsed = start.elapsed();
                builder = builder.check(
                    CheckDetail::passed("stream_creation")
                        .with_duration_ms(elapsed.as_millis() as u64),
                );
                if let Err(e) = stream.close().await {
                    warn!("Failed to close check stream (non-fatal): {}", e);
                }
                info!(
                    "Successfully connected to Databricks table '{}' ({}ms)",
                    config.table_name,
                    elapsed.as_millis()
                );
            }
            Err(e) => {
                let elapsed = start.elapsed();
                let err_str = e.to_string();
                builder = builder.check(
                    CheckDetail::failed("stream_creation", &err_str)
                        .with_duration_ms(elapsed.as_millis() as u64),
                );
                warn!(
                    "Stream creation check failed for '{}': {}",
                    config.table_name, err_str
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
        // Validate
        config
            .validate_table_name()
            .map_err(|msg| ConnectorError::Config(msg))?;

        // Build SDK and create stream with metrics ack callback
        let sdk = Self::build_sdk(config)?;

        let table_props = TableProperties {
            table_name: config.table_name.clone(),
            descriptor_proto: None,
        };

        let mut options = config.stream_options();
        options.ack_callback = Some(Arc::new(MetricsAckCallback));

        let stream_id = generate_stream_id();

        let mut stream = sdk
            .create_stream(
                table_props,
                config.client_id.clone(),
                config.client_secret.expose_secret().to_string(),
                Some(options),
            )
            .await
            .map_err(|e| {
                let classified = classify_zerobus_error(&e);
                error!(stream_id = %stream_id, "Failed to create Zerobus stream: {}", e);
                classified
            })?;

        let mut batch: Vec<JsonString> = Vec::with_capacity(config.batch_size);
        let mut batch_bytes: usize = 0;
        let mut total_written = 0u64;
        let mut total_bytes = 0u64;
        let mut total_failed = 0u64;
        let mut errors: Vec<String> = Vec::new();

        // Timer-based flush via tokio::select! — prevents stale partial batches
        // when no events arrive for extended periods.
        let flush_duration = Duration::from_secs(config.flush_interval_secs);
        let mut flush_ticker = tokio::time::interval(flush_duration);
        flush_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        flush_ticker.tick().await; // consume initial immediate tick

        info!(
            stream_id = %stream_id,
            table = %config.table_name,
            batch_size = config.batch_size,
            max_inflight = config.max_inflight_requests,
            max_batch_retries = config.max_batch_retries,
            "Starting Databricks sink"
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
                            let row = DatabricksRow::from_event(&event);
                            match row.to_json_string() {
                                Ok(json) => {
                                    batch_bytes += json.len();
                                    batch.push(JsonString(json));
                                }
                                Err(e) => {
                                    total_failed += 1;
                                    counter!("databricks.records.serialization_errors").increment(1);
                                    errors.push(format!("Serialization error: {}", e));
                                }
                            }
                            should_flush = batch.len() >= config.batch_size;
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
                            stream_id = %stream_id,
                            records = batch.len(),
                            "Timer-triggered flush of partial batch"
                        );
                    }
                }
            }

            // ── Flush batch ─────────────────────────────────────────────
            if should_flush && !batch.is_empty() {
                // Check circuit breaker before contacting Zerobus
                if let Some(ref cb) = self.circuit_breaker {
                    if !cb.allow_request() {
                        counter!("databricks.circuit_breaker.rejected").increment(1);
                        let batch_len = batch.len();
                        total_failed += batch_len as u64;
                        counter!("databricks.records.failed").increment(batch_len as u64);
                        errors.push(format!(
                            "Circuit breaker open — dropped batch of {} records",
                            batch_len
                        ));
                        warn!(
                            stream_id = %stream_id,
                            records = batch_len,
                            "Circuit breaker open, dropping batch"
                        );
                        batch.clear();
                        batch_bytes = 0;
                        if stream_ended {
                            break;
                        }
                        continue;
                    }
                }

                let batch_len = batch.len();
                let bytes_in_batch = batch_bytes;
                gauge!("databricks.batch.size").set(batch_len as f64);

                debug!(
                    stream_id = %stream_id,
                    records = batch_len,
                    bytes = bytes_in_batch,
                    "Ingesting batch to Databricks"
                );

                let batch_start = std::time::Instant::now();
                let mut records =
                    std::mem::replace(&mut batch, Vec::with_capacity(config.batch_size));
                batch_bytes = 0;

                // ── Ingest with connector-level retry ───────────────────
                let max_retries = config.max_batch_retries;
                let mut last_err: Option<ZerobusError> = None;
                let mut succeeded = false;
                let mut result_offset: Option<OffsetId> = None;

                for attempt in 0..=max_retries {
                    let to_send = if attempt < max_retries {
                        // Clone for potential retry — only on non-final attempts
                        records.iter().map(|r| JsonString(r.0.clone())).collect()
                    } else {
                        // Move on final attempt to avoid unnecessary allocation
                        std::mem::take(&mut records)
                    };

                    match stream.ingest_records_offset(to_send).await {
                        Ok(offset) => {
                            result_offset = offset;
                            succeeded = true;
                            if attempt > 0 {
                                debug!(
                                    stream_id = %stream_id,
                                    attempt = attempt + 1,
                                    "Batch succeeded on retry"
                                );
                            }
                            break;
                        }
                        Err(e) => {
                            if e.is_retryable() && attempt < max_retries {
                                counter!("databricks.batches.retried").increment(1);
                                let backoff = calculate_backoff(
                                    attempt + 1,
                                    config.initial_backoff_ms,
                                    config.max_backoff_ms,
                                );
                                warn!(
                                    stream_id = %stream_id,
                                    attempt = attempt + 1,
                                    max_retries = max_retries,
                                    backoff_ms = backoff.as_millis() as u64,
                                    error = %e,
                                    "Retryable batch error, backing off"
                                );
                                tokio::time::sleep(backoff).await;
                                last_err = Some(e);
                            } else {
                                last_err = Some(e);
                                break;
                            }
                        }
                    }
                }

                // ── Handle ingest outcome ───────────────────────────────
                if succeeded {
                    if let Some(offset) = result_offset {
                        if config.wait_for_ack {
                            match stream.wait_for_offset(offset).await {
                                Ok(()) => {
                                    let elapsed = batch_start.elapsed();
                                    histogram!("databricks.batch.duration_ms")
                                        .record(elapsed.as_millis() as f64);
                                    histogram!("databricks.ack.duration_ms")
                                        .record(elapsed.as_millis() as f64);
                                    counter!("databricks.batches.success").increment(1);
                                    counter!("databricks.records.written")
                                        .increment(batch_len as u64);
                                    total_written += batch_len as u64;
                                    total_bytes += bytes_in_batch as u64;
                                    if let Some(ref cb) = self.circuit_breaker {
                                        cb.record_success();
                                    }
                                    debug!(
                                        stream_id = %stream_id,
                                        offset = offset,
                                        records = batch_len,
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "Batch acknowledged"
                                    );
                                }
                                Err(e) => {
                                    let elapsed = batch_start.elapsed();
                                    histogram!("databricks.batch.duration_ms")
                                        .record(elapsed.as_millis() as f64);
                                    counter!("databricks.batches.failed").increment(1);
                                    counter!("databricks.records.failed")
                                        .increment(batch_len as u64);
                                    total_failed += batch_len as u64;
                                    if let Some(ref cb) = self.circuit_breaker {
                                        cb.record_failure();
                                    }
                                    let classified = classify_zerobus_error(&e);
                                    let msg = format!(
                                        "Ack failed for batch of {} at offset {}: {} [{}]",
                                        batch_len, offset, e, classified
                                    );
                                    error!(stream_id = %stream_id, "{}", msg);
                                    errors.push(msg);
                                }
                            }
                        } else {
                            // Fire-and-forget mode
                            let elapsed = batch_start.elapsed();
                            histogram!("databricks.batch.duration_ms")
                                .record(elapsed.as_millis() as f64);
                            counter!("databricks.batches.success").increment(1);
                            counter!("databricks.records.written")
                                .increment(batch_len as u64);
                            total_written += batch_len as u64;
                            total_bytes += bytes_in_batch as u64;
                            if let Some(ref cb) = self.circuit_breaker {
                                cb.record_success();
                            }
                        }
                    } else {
                        debug!(stream_id = %stream_id, "Empty batch response (no offset)");
                    }
                } else if let Some(e) = last_err {
                    // All attempts exhausted or non-retryable error
                    let elapsed = batch_start.elapsed();
                    histogram!("databricks.batch.duration_ms")
                        .record(elapsed.as_millis() as f64);
                    counter!("databricks.batches.failed").increment(1);
                    counter!("databricks.records.failed").increment(batch_len as u64);
                    total_failed += batch_len as u64;
                    if let Some(ref cb) = self.circuit_breaker {
                        cb.record_failure();
                    }
                    let classified = classify_zerobus_error(&e);
                    let msg = format!(
                        "Failed to ingest batch of {} records (after {} attempt(s)): {} [{}]",
                        batch_len,
                        max_retries + 1,
                        e,
                        classified
                    );
                    error!(stream_id = %stream_id, "{}", msg);
                    errors.push(msg);

                    // Non-retryable errors abort immediately
                    if !e.is_retryable() {
                        error!(
                            stream_id = %stream_id,
                            "Non-retryable error — aborting ingestion"
                        );
                        break;
                    }
                }
            }

            if stream_ended {
                break;
            }
        }

        // ── Graceful close ──────────────────────────────────────────────
        // SDK flushes internally and waits for outstanding acks.
        match stream.close().await {
            Ok(()) => {
                info!(stream_id = %stream_id, "Databricks stream closed gracefully");
            }
            Err(e) => {
                counter!("databricks.stream.close_errors").increment(1);
                warn!(stream_id = %stream_id, "Stream close encountered errors: {}", e);

                // Retrieve unacknowledged records for diagnostics
                match stream.get_unacked_records().await {
                    Ok(unacked) => {
                        let count = unacked.count();
                        if count > 0 {
                            gauge!("databricks.unacked.records").set(count as f64);
                            warn!(
                                stream_id = %stream_id,
                                unacked = count,
                                "Records unacknowledged on close"
                            );
                            total_failed += count as u64;
                            total_written = total_written.saturating_sub(count as u64);
                            errors.push(format!("{} records unacknowledged on close", count));
                        }
                    }
                    Err(e2) => {
                        warn!(
                            stream_id = %stream_id,
                            "Failed to retrieve unacked records: {}", e2
                        );
                    }
                }
            }
        }

        info!(
            stream_id = %stream_id,
            written = total_written,
            bytes = total_bytes,
            failed = total_failed,
            "Databricks sink completed"
        );

        Ok(WriteResult {
            records_written: total_written,
            bytes_written: total_bytes,
            records_failed: total_failed,
            errors,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Factory for creating Databricks sink instances.
pub struct DatabricksSinkFactory;

impl SinkFactory for DatabricksSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        DatabricksSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(DatabricksSink::new())
    }
}

// Implement AnySink for DatabricksSink
crate::impl_any_sink!(DatabricksSink, DatabricksSinkConfig);

/// Register the Databricks sink with the given registry.
pub fn register(registry: &mut crate::SinkRegistry) {
    registry.register("databricks", Arc::new(DatabricksSinkFactory));
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = DatabricksSink::spec();
        assert_eq!(spec.connector_type, "databricks");
        assert!(spec.config_schema.is_some());
        assert_eq!(
            spec.metadata.get("protocol"),
            Some(&"grpc".to_string())
        );
        assert_eq!(
            spec.metadata.get("auth"),
            Some(&"oauth2".to_string())
        );
    }

    #[test]
    fn test_default_config() {
        let config = DatabricksSinkConfig::default();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.max_inflight_requests, 100_000);
        assert!(config.recovery);
        assert_eq!(config.recovery_retries, 4);
        assert_eq!(config.recovery_timeout_ms, 15_000);
        assert_eq!(config.recovery_backoff_ms, 2_000);
        assert_eq!(config.server_ack_timeout_ms, 60_000);
        assert_eq!(config.flush_timeout_ms, 300_000);
        assert!(config.wait_for_ack);
        assert!(config.circuit_breaker.enabled);
        // Retry defaults
        assert_eq!(config.max_batch_retries, 2);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5_000);
    }

    #[test]
    fn test_factory() {
        let factory = DatabricksSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "databricks");
        let _sink = factory.create();
    }

    #[test]
    fn test_config_validation() {
        let config = DatabricksSinkConfig {
            endpoint: "my-shard.zerobus.us-east-1.cloud.databricks.com".to_string(),
            unity_catalog_url: "https://my-workspace.cloud.databricks.com".to_string(),
            table_name: "catalog.schema.my_table".to_string(),
            client_id: "test-client-id".to_string(),
            client_secret: SensitiveString::new("test-secret"),
            batch_size: 1000,
            ..Default::default()
        };

        // Valid config
        assert!(config.validate().is_ok());
        assert!(config.validate_table_name().is_ok());

        // Invalid batch_size (zero)
        let mut invalid = config.clone();
        invalid.batch_size = 0;
        assert!(invalid.validate().is_err());

        // Invalid batch_size (too large)
        let mut invalid = config.clone();
        invalid.batch_size = 200_000;
        assert!(invalid.validate().is_err());

        // Invalid max_inflight
        let mut invalid = config.clone();
        invalid.max_inflight_requests = 0;
        assert!(invalid.validate().is_err());

        // Invalid recovery_backoff_ms (too small)
        let mut invalid = config.clone();
        invalid.recovery_backoff_ms = 10;
        assert!(invalid.validate().is_err());

        // Invalid server_ack_timeout_ms (too small)
        let mut invalid = config.clone();
        invalid.server_ack_timeout_ms = 100;
        assert!(invalid.validate().is_err());

        // Invalid max_batch_retries (too large)
        let mut invalid = config.clone();
        invalid.max_batch_retries = 20;
        assert!(invalid.validate().is_err());

        // Invalid initial_backoff_ms (too small)
        let mut invalid = config.clone();
        invalid.initial_backoff_ms = 1;
        assert!(invalid.validate().is_err());

        // Invalid max_backoff_ms (too large)
        let mut invalid = config.clone();
        invalid.max_backoff_ms = 200_000;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_table_name_validation() {
        let mut config = DatabricksSinkConfig::default();

        // Valid names
        config.table_name = "catalog.schema.table".to_string();
        assert!(config.validate_table_name().is_ok());

        config.table_name = "my_catalog.my_schema.my_table".to_string();
        assert!(config.validate_table_name().is_ok());

        config.table_name = "_catalog._schema._table".to_string();
        assert!(config.validate_table_name().is_ok());

        // Invalid names
        config.table_name = "just_a_table".to_string();
        assert!(config.validate_table_name().is_err());

        config.table_name = "schema.table".to_string();
        assert!(config.validate_table_name().is_err());

        config.table_name = "".to_string();
        assert!(config.validate_table_name().is_err());

        config.table_name = "catalog.schema.table.extra".to_string();
        assert!(config.validate_table_name().is_err());

        config.table_name = "123.456.789".to_string();
        assert!(config.validate_table_name().is_err());
    }

    #[test]
    fn test_endpoint_validation() {
        let mut config = DatabricksSinkConfig::default();

        // Valid AWS endpoint
        config.endpoint = "my-shard.zerobus.us-east-1.cloud.databricks.com".to_string();
        assert!(config.validate_endpoint().is_ok());

        // Valid AWS endpoint with scheme
        config.endpoint = "https://my-shard.zerobus.us-west-2.cloud.databricks.com".to_string();
        assert!(config.validate_endpoint().is_ok());

        // Valid Azure endpoint
        config.endpoint =
            "my-shard.zerobus.westeurope.azuredatabricks.net".to_string();
        assert!(config.validate_endpoint().is_ok());

        // Valid Azure endpoint with scheme
        config.endpoint =
            "https://my-shard.zerobus.eastus.azuredatabricks.net".to_string();
        assert!(config.validate_endpoint().is_ok());

        // Invalid: empty
        config.endpoint = "".to_string();
        assert!(config.validate_endpoint().is_err());

        // Invalid: random URL
        config.endpoint = "https://example.com".to_string();
        assert!(config.validate_endpoint().is_err());

        // Invalid: missing zerobus subdomain
        config.endpoint = "my-shard.us-east-1.cloud.databricks.com".to_string();
        assert!(config.validate_endpoint().is_err());
    }

    #[test]
    fn test_stream_options() {
        let config = DatabricksSinkConfig {
            max_inflight_requests: 50_000,
            recovery: true,
            recovery_retries: 5,
            recovery_timeout_ms: 20_000,
            recovery_backoff_ms: 3_000,
            server_ack_timeout_ms: 90_000,
            flush_timeout_ms: 600_000,
            ..Default::default()
        };

        let opts = config.stream_options();
        assert_eq!(opts.max_inflight_requests, 50_000);
        assert!(opts.recovery);
        assert_eq!(opts.recovery_retries, 5);
        assert_eq!(opts.recovery_timeout_ms, 20_000);
        assert_eq!(opts.recovery_backoff_ms, 3_000);
        assert_eq!(opts.server_lack_of_ack_timeout_ms, 90_000);
        assert_eq!(opts.flush_timeout_ms, 600_000);
    }

    #[test]
    fn test_row_serialization() {
        let row = DatabricksRow {
            event_type: "insert".to_string(),
            stream: "users".to_string(),
            namespace: Some("public".to_string()),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            data: serde_json::json!({"id": 1, "name": "test"}),
            metadata: serde_json::json!({}),
            ingested_at: "2024-01-01T00:00:01Z".to_string(),
        };

        let json_str = row.to_json_string().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["event_type"], "insert");
        assert_eq!(parsed["stream"], "users");
        assert_eq!(parsed["namespace"], "public");
        assert_eq!(parsed["data"]["id"], 1);

        // Verify bytes are non-zero
        assert!(json_str.len() > 50);
    }

    #[test]
    fn test_row_serialization_null_namespace() {
        let row = DatabricksRow {
            event_type: "delete".to_string(),
            stream: "orders".to_string(),
            namespace: None,
            timestamp: "2024-06-15T12:00:00Z".to_string(),
            data: serde_json::json!({"order_id": 42}),
            metadata: serde_json::json!({"source": "cdc"}),
            ingested_at: "2024-06-15T12:00:01Z".to_string(),
        };

        let json_str = row.to_json_string().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        // namespace should be absent (skip_serializing_if)
        assert!(parsed.get("namespace").is_none());
        assert_eq!(parsed["event_type"], "delete");
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = DatabricksSinkConfig {
            endpoint: "shard.zerobus.us-east-1.cloud.databricks.com".to_string(),
            unity_catalog_url: "https://workspace.cloud.databricks.com".to_string(),
            table_name: "catalog.schema.events".to_string(),
            client_id: "cid".to_string(),
            client_secret: SensitiveString::new("secret"),
            batch_size: 1000,
            flush_interval_secs: 10,
            max_inflight_requests: 50_000,
            recovery: true,
            recovery_retries: 3,
            recovery_timeout_ms: 10_000,
            recovery_backoff_ms: 5_000,
            server_ack_timeout_ms: 120_000,
            flush_timeout_ms: 120_000,
            wait_for_ack: false,
            circuit_breaker: CircuitBreakerConfig {
                enabled: true,
                failure_threshold: 10,
                reset_timeout_secs: 60,
                success_threshold: 3,
            },
            max_batch_retries: 3,
            initial_backoff_ms: 300,
            max_backoff_ms: 8_000,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: DatabricksSinkConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.endpoint, config.endpoint);
        assert_eq!(deserialized.table_name, config.table_name);
        assert_eq!(deserialized.batch_size, 1000);
        assert_eq!(deserialized.max_inflight_requests, 50_000);
        assert_eq!(deserialized.recovery_backoff_ms, 5_000);
        assert_eq!(deserialized.server_ack_timeout_ms, 120_000);
        assert!(!deserialized.wait_for_ack);
        assert!(deserialized.circuit_breaker.enabled);
        assert_eq!(deserialized.circuit_breaker.failure_threshold, 10);
        assert_eq!(deserialized.max_batch_retries, config.max_batch_retries);
        assert_eq!(deserialized.initial_backoff_ms, config.initial_backoff_ms);
        assert_eq!(deserialized.max_backoff_ms, config.max_backoff_ms);
    }

    #[test]
    fn test_config_defaults_applied_on_deserialize() {
        // Minimal config — all optional fields should get defaults
        let json = r#"{
            "endpoint": "shard.zerobus.region.cloud.databricks.com",
            "unity_catalog_url": "https://workspace.cloud.databricks.com",
            "table_name": "cat.sch.tbl",
            "client_id": "id",
            "client_secret": "secret"
        }"#;

        let config: DatabricksSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.max_inflight_requests, 100_000);
        assert!(config.recovery);
        assert_eq!(config.recovery_retries, 4);
        assert_eq!(config.recovery_backoff_ms, 2_000);
        assert_eq!(config.server_ack_timeout_ms, 60_000);
        assert!(config.wait_for_ack);
        // Circuit breaker defaults
        assert!(config.circuit_breaker.enabled);
        assert_eq!(config.circuit_breaker.failure_threshold, 5);
        assert_eq!(config.circuit_breaker.reset_timeout_secs, 30);
        assert_eq!(config.circuit_breaker.success_threshold, 2);
        // Retry defaults
        assert_eq!(config.max_batch_retries, 2);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5_000);
    }

    #[test]
    fn test_sdk_builder_requires_endpoint() {
        // Building without calling .endpoint() should fail
        let result = ZerobusSdk::builder().build();
        assert!(result.is_err(), "SDK builder should require endpoint");
    }

    #[test]
    fn test_circuit_breaker_config_defaults() {
        let cb = CircuitBreakerConfig::default();
        assert!(cb.enabled);
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.reset_timeout_secs, 30);
        assert_eq!(cb.success_threshold, 2);
    }

    #[test]
    fn test_circuit_breaker_states() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 1,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(config);

        // Initially closed
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());

        // Record failures up to threshold
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());

        // Third failure opens the circuit
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_disabled() {
        let config = CircuitBreakerConfig {
            enabled: false,
            failure_threshold: 1,
            reset_timeout_secs: 1,
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(config);

        // Always closed when disabled
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 3,
            reset_timeout_secs: 60,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::new(config);

        // Record some failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        // Success should reset failure count
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);

        // Now need 3 more failures to open
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_breaker_config_serde() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 10,
            reset_timeout_secs: 120,
            success_threshold: 5,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CircuitBreakerConfig = serde_json::from_str(&json).unwrap();
        assert!(deserialized.enabled);
        assert_eq!(deserialized.failure_threshold, 10);
        assert_eq!(deserialized.reset_timeout_secs, 120);
        assert_eq!(deserialized.success_threshold, 5);
    }

    #[test]
    fn test_generate_stream_id() {
        let id = generate_stream_id();
        assert!(id.starts_with("rivven-dbx-"));
        assert!(id.len() > 15);

        // Two IDs should be distinct
        let id2 = generate_stream_id();
        assert_ne!(id, id2);
    }

    #[test]
    fn test_with_config_constructor() {
        // Circuit breaker enabled
        let config = DatabricksSinkConfig::default();
        let sink = DatabricksSink::with_config(&config);
        assert!(sink.circuit_breaker.is_some());

        // Circuit breaker disabled
        let mut config = DatabricksSinkConfig::default();
        config.circuit_breaker.enabled = false;
        let sink = DatabricksSink::with_config(&config);
        assert!(sink.circuit_breaker.is_none());
    }

    #[test]
    fn test_calculate_backoff() {
        // attempt 1: 200ms
        assert_eq!(calculate_backoff(1, 200, 5_000), Duration::from_millis(200));
        // attempt 2: 400ms
        assert_eq!(calculate_backoff(2, 200, 5_000), Duration::from_millis(400));
        // attempt 3: 800ms
        assert_eq!(calculate_backoff(3, 200, 5_000), Duration::from_millis(800));
        // attempt 4: 1600ms
        assert_eq!(
            calculate_backoff(4, 200, 5_000),
            Duration::from_millis(1_600)
        );
        // attempt 5: 3200ms
        assert_eq!(
            calculate_backoff(5, 200, 5_000),
            Duration::from_millis(3_200)
        );
        // attempt 6: capped at 5000ms
        assert_eq!(
            calculate_backoff(6, 200, 5_000),
            Duration::from_millis(5_000)
        );
        // attempt 0: initial_ms * 2^0 = initial_ms (no sub-underflow)
        assert_eq!(calculate_backoff(0, 200, 5_000), Duration::from_millis(200));
    }

    #[test]
    fn test_retry_config_defaults_on_deserialize() {
        // Minimal JSON — retry fields should get defaults
        let json = r#"{
            "endpoint": "shard.zerobus.region.cloud.databricks.com",
            "unity_catalog_url": "https://ws.cloud.databricks.com",
            "table_name": "cat.sch.tbl",
            "client_id": "id",
            "client_secret": "sec"
        }"#;
        let config: DatabricksSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_batch_retries, 2);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5_000);
    }

    #[test]
    fn test_retry_config_custom_values() {
        let json = r#"{
            "endpoint": "shard.zerobus.region.cloud.databricks.com",
            "unity_catalog_url": "https://ws.cloud.databricks.com",
            "table_name": "cat.sch.tbl",
            "client_id": "id",
            "client_secret": "sec",
            "max_batch_retries": 5,
            "initial_backoff_ms": 500,
            "max_backoff_ms": 10000
        }"#;
        let config: DatabricksSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_batch_retries, 5);
        assert_eq!(config.initial_backoff_ms, 500);
        assert_eq!(config.max_backoff_ms, 10_000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_retry_disabled() {
        let json = r#"{
            "endpoint": "shard.zerobus.region.cloud.databricks.com",
            "unity_catalog_url": "https://ws.cloud.databricks.com",
            "table_name": "cat.sch.tbl",
            "client_id": "id",
            "client_secret": "sec",
            "max_batch_retries": 0
        }"#;
        let config: DatabricksSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_batch_retries, 0);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_calculate_backoff_overflow_safety() {
        // Very large attempt number — should not panic
        let result = calculate_backoff(100, 200, 5_000);
        assert_eq!(result, Duration::from_millis(5_000));

        // Very large initial_ms
        let result = calculate_backoff(3, u64::MAX / 2, u64::MAX);
        assert!(result.as_millis() > 0);
    }
}
