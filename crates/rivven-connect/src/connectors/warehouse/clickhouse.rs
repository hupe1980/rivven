//! ClickHouse sink connector
//!
//! This module provides a sink connector for streaming data into ClickHouse
//! using the official `clickhouse-rs` client over the HTTP interface with
//! native `RowBinary` encoding for maximum throughput.
//!
//! # Features
//!
//! - **High-throughput inserts** via ClickHouse native HTTP API with `RowBinary` encoding
//! - **Automatic batching** with configurable size and timer-based flush via `tokio::select!`
//! - **Multiple authentication methods** (password, JWT token)
//! - **TLS support** via rustls (no OpenSSL dependency)
//! - **LZ4 compression** on the wire (ClickHouse native, enabled by default)
//! - **Schema validation** against ClickHouse table schema (optional)
//! - **Circuit breaker** to prevent cascading failures on repeated insert errors
//! - **Metrics integration** for observability (counters, gauges, histograms)
//! - **Structured error classification** into typed `ConnectorError` variants
//! - **Connector-level batch retry** with exponential backoff and jitter
//! - **Timer-based flush** via `tokio::select!` (no stale partial batches)
//! - **Session-scoped structured logging** for easy per-run correlation
//!
//! # Authentication
//!
//! 1. **Username/Password** — Via `username` / `password` config (default)
//! 2. **Access Token** — Via `access_token` config (e.g. ClickHouse Cloud JWT)
//!
//! # Retry / Recovery Behavior
//!
//! The connector classifies ClickHouse errors into retryable (connection,
//! timeout, rate-limit) and fatal (auth, schema, permission) categories.
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
//! The connector records the following metrics (via the `metrics` crate):
//!
//! | Metric | Type | Description |
//! |--------|------|-------------|
//! | `clickhouse.batches.success` | Counter | Successfully inserted batches |
//! | `clickhouse.batches.failed` | Counter | Failed batch inserts |
//! | `clickhouse.batches.retried` | Counter | Batch retry attempts |
//! | `clickhouse.records.written` | Counter | Total records written |
//! | `clickhouse.records.failed` | Counter | Total records failed |
//! | `clickhouse.circuit_breaker.rejected` | Counter | Batches rejected by open circuit breaker |
//! | `clickhouse.batch.size` | Gauge | Current batch size before flush |
//! | `clickhouse.batch.duration_ms` | Histogram | End-to-end batch insert latency |
//!
//! # Performance
//!
//! The connector is designed for maximum insert throughput:
//!
//! - Uses `RowBinary` format (zero JSON overhead on the wire)
//! - LZ4 compression reduces network bandwidth
//! - Batch inserts amortize HTTP round-trip cost
//! - Lock-free circuit breaker with atomic state transitions
//! - Async pipeline — event collection and flush proceed concurrently
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::registry::SinkRegistry;
//! use rivven_connect::connectors::warehouse::ClickHouseSinkFactory;
//! use std::sync::Arc;
//!
//! let mut sinks = SinkRegistry::new();
//! sinks.register("clickhouse", Arc::new(ClickHouseSinkFactory));
//! ```

use crate::connectors::{AnySink, SinkFactory};
use crate::error::ConnectorError;
use crate::prelude::*;
use async_trait::async_trait;
use chrono::Utc;
use clickhouse::Client;
use futures::StreamExt;
use metrics::{counter, gauge, histogram};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use validator::Validate;

use crate::types::SensitiveString;

// ─────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────

/// Compression algorithm for ClickHouse inserts.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ClickHouseCompression {
    /// No compression
    None,
    /// LZ4 compression (ClickHouse native, best throughput — default)
    #[default]
    Lz4,
}

/// Circuit breaker configuration for the ClickHouse sink.
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct ClickHouseCBConfig {
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

impl Default for ClickHouseCBConfig {
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

/// Configuration for the ClickHouse sink
#[derive(Debug, Clone, Deserialize, Serialize, Validate, JsonSchema)]
pub struct ClickHouseSinkConfig {
    /// ClickHouse HTTP(S) URL (e.g. `http://localhost:8123` or `https://abc.clickhouse.cloud:8443`)
    #[validate(length(min = 1, max = 2048), url)]
    pub url: String,

    /// Database name
    #[serde(default = "default_database")]
    #[validate(length(min = 1, max = 255))]
    pub database: String,

    /// Target table name (must already exist in ClickHouse)
    #[validate(length(min = 1, max = 255))]
    pub table: String,

    /// Username for authentication
    #[serde(default = "default_username")]
    #[validate(length(min = 1, max = 255))]
    pub username: String,

    /// Password for authentication
    #[serde(default)]
    pub password: Option<SensitiveString>,

    /// Access token for ClickHouse Cloud or JWT-based auth
    /// (mutually exclusive with password)
    #[serde(default)]
    pub access_token: Option<SensitiveString>,

    /// Number of rows to batch before inserting
    #[serde(default = "default_batch_size")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub batch_size: usize,

    /// Maximum time in seconds before flushing a partial batch
    #[serde(default = "default_flush_interval")]
    #[validate(range(min = 1, max = 3600))]
    pub flush_interval_secs: u64,

    /// Compression algorithm for inserts
    #[serde(default)]
    pub compression: ClickHouseCompression,

    /// Enable schema validation against ClickHouse table schema.
    /// Adds ~5-10% overhead but gives clearer error messages on schema mismatch.
    #[serde(default = "default_validation")]
    pub validation: bool,

    /// Whether to skip individual rows that fail serialization.
    /// When false (default), a single bad row aborts the entire batch.
    #[serde(default)]
    pub skip_invalid_rows: bool,

    /// Maximum number of retries for transient insert failures
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
    pub circuit_breaker: ClickHouseCBConfig,
}

fn default_database() -> String {
    "default".to_string()
}

fn default_username() -> String {
    "default".to_string()
}

fn default_batch_size() -> usize {
    10_000
}

fn default_flush_interval() -> u64 {
    5
}

fn default_validation() -> bool {
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

/// Regex pattern for valid ClickHouse identifiers (database / table names).
/// Permits ASCII alphanumeric, underscore, and dot (for qualified names).
static IDENTIFIER_PATTERN: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[A-Za-z_][A-Za-z0-9_.]{0,254}$").unwrap());

impl ClickHouseSinkConfig {
    /// Validate that a database name is safe for inclusion in queries.
    pub fn validate_database_name(name: &str) -> std::result::Result<(), String> {
        if !IDENTIFIER_PATTERN.is_match(name) {
            return Err(format!(
                "Invalid database name '{}': must match [A-Za-z_][A-Za-z0-9_.]*",
                name
            ));
        }
        Ok(())
    }

    /// Validate that a table name is safe for inclusion in queries.
    pub fn validate_table_name(name: &str) -> std::result::Result<(), String> {
        if !IDENTIFIER_PATTERN.is_match(name) {
            return Err(format!(
                "Invalid table name '{}': must match [A-Za-z_][A-Za-z0-9_.]*",
                name
            ));
        }
        Ok(())
    }

    /// Validate that exactly one credential source is configured.
    pub fn validate_auth(&self) -> std::result::Result<(), String> {
        match (&self.password, &self.access_token) {
            (Some(_), Some(_)) => {
                Err("Only one of 'password' or 'access_token' may be set".to_string())
            }
            _ => Ok(()),
        }
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

impl Default for ClickHouseSinkConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            database: default_database(),
            table: String::new(),
            username: default_username(),
            password: None,
            access_token: None,
            batch_size: default_batch_size(),
            flush_interval_secs: default_flush_interval(),
            compression: ClickHouseCompression::default(),
            validation: default_validation(),
            skip_invalid_rows: false,
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            circuit_breaker: ClickHouseCBConfig::default(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// Row type — maps SourceEvent → ClickHouse row (RowBinary encoding)
// ─────────────────────────────────────────────────────────────────

/// ClickHouse row payload serialized via `RowBinary`.
///
/// This is the wire format the `clickhouse-rs` client encodes into
/// ClickHouse's native binary protocol for maximum throughput (no
/// JSON parsing overhead on the server side).
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
struct ClickHouseRow {
    event_type: String,
    stream: String,
    namespace: String,
    timestamp: String,
    data: String,
    metadata: String,
    _ingested_at: String,
}

impl ClickHouseRow {
    /// Convert a `SourceEvent` into a `ClickHouseRow` and return the
    /// approximate payload size in bytes (used for throughput metrics).
    #[inline]
    fn from_event(event: &SourceEvent) -> (Self, usize) {
        let data_json = serde_json::to_string(&event.data).unwrap_or_default();
        let metadata_json = serde_json::to_string(&event.metadata).unwrap_or_default();
        let payload_bytes = data_json.len() + metadata_json.len();
        let row = Self {
            event_type: event.event_type.to_string(),
            stream: event.stream.clone(),
            namespace: event.namespace.clone().unwrap_or_default(),
            timestamp: event.timestamp.to_rfc3339(),
            data: data_json,
            metadata: metadata_json,
            _ingested_at: Utc::now().to_rfc3339(),
        };
        (row, payload_bytes)
    }
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
    fn new(config: &ClickHouseCBConfig) -> Self {
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
                    info!("Circuit breaker closed (ClickHouse recovered)");
                }
            }
            CBState::Open => {}
        }
    }

    fn record_failure(&self) {
        let prev = self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == self.failure_threshold && self.state() != CBState::Open {
            self.state.store(CBState::Open as u32, Ordering::Release);
            self.opened_at
                .store(Self::now_epoch_secs(), Ordering::Release);
            warn!(
                threshold = self.failure_threshold,
                "Circuit breaker OPEN (ClickHouse)"
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

/// Classify a `clickhouse::error::Error` into a typed `ConnectorError`.
fn classify_clickhouse_error(err: &clickhouse::error::Error) -> ConnectorError {
    let msg = err.to_string();
    let lower = msg.to_lowercase();

    if lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("timeout_exceeded")
    {
        ConnectorError::Timeout(msg)
    } else if lower.contains("too many requests") || lower.contains("rate") {
        ConnectorError::RateLimited(msg)
    } else if lower.contains("authentication")
        || lower.contains("access denied")
        || lower.contains("wrong password")
    {
        ConnectorError::Auth(msg)
    } else if lower.contains("not found") || lower.contains("unknown table") {
        ConnectorError::NotFound(msg)
    } else if lower.contains("no such column")
        || lower.contains("type mismatch")
        || lower.contains("expected column")
        || lower.contains("expected '")
    {
        ConnectorError::Schema(msg)
    } else if lower.contains("connection")
        || lower.contains("network")
        || lower.contains("broken pipe")
        || lower.contains("reset by peer")
        || lower.contains("service_unavailable")
        || lower.contains("network_error")
    {
        ConnectorError::Connection(msg)
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
    format!("rivven-ch-{}-{}", std::process::id(), micros)
}

/// Exponential backoff with jitter (prevents thundering herd).
fn calculate_backoff(attempt: u32, initial_ms: u64, max_ms: u64) -> Duration {
    let exp = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let base = initial_ms.saturating_mul(exp);
    let capped = base.min(max_ms);
    // Simple jitter: ±25%
    let jitter_range = capped / 4;
    let jitter = if jitter_range > 0 {
        // Use a lightweight deterministic source; not cryptographic.
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

/// ClickHouse Sink implementation
pub struct ClickHouseSink {
    circuit_breaker: Option<CircuitBreaker>,
}

impl Default for ClickHouseSink {
    fn default() -> Self {
        Self::new()
    }
}

impl ClickHouseSink {
    /// Create a new ClickHouse sink instance with default config (circuit breaker enabled).
    pub fn new() -> Self {
        Self::with_config(&ClickHouseSinkConfig::default())
    }

    /// Create a sink pre-configured with the given config's circuit breaker.
    pub fn with_config(config: &ClickHouseSinkConfig) -> Self {
        let cb = if config.circuit_breaker.enabled {
            Some(CircuitBreaker::new(&config.circuit_breaker))
        } else {
            None
        };
        Self {
            circuit_breaker: cb,
        }
    }

    /// Build a configured `clickhouse::Client` from the sink config.
    fn create_client(config: &ClickHouseSinkConfig) -> crate::error::Result<Client> {
        let mut client = Client::default()
            .with_url(&config.url)
            .with_database(&config.database)
            .with_user(&config.username);

        // Authentication: password or access_token
        if let Some(ref password) = config.password {
            client = client.with_password(password.expose_secret());
        }
        if let Some(ref token) = config.access_token {
            client = client.with_access_token(token.expose_secret());
        }

        // Compression
        match config.compression {
            ClickHouseCompression::Lz4 => {
                client = client.with_compression(clickhouse::Compression::Lz4);
            }
            ClickHouseCompression::None => {
                client = client.with_compression(clickhouse::Compression::None);
            }
        }

        // Validation toggle
        if !config.validation {
            client = client.with_validation(false);
        }

        Ok(client)
    }

    /// Insert a batch of rows into ClickHouse with retry logic.
    async fn insert_batch(
        client: &Client,
        table: &str,
        batch: &[ClickHouseRow],
        max_retries: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        session_id: &str,
    ) -> std::result::Result<(), ConnectorError> {
        let mut attempt = 0u32;

        loop {
            let mut insert = client.insert::<ClickHouseRow>(table).await.map_err(|e| {
                ConnectorError::Connection(format!("Failed to begin insert: {}", e))
            })?;

            for row in batch {
                insert.write(row).await.map_err(|e| {
                    ConnectorError::Serialization(format!("Failed to write row: {}", e))
                })?;
            }

            match insert.end().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    let classified = classify_clickhouse_error(&e);

                    if !classified.is_retryable() || attempt > max_retries {
                        return Err(ConnectorError::Fatal(format!(
                            "Insert failed after {} attempt(s): {}",
                            attempt, e
                        )));
                    }

                    counter!("clickhouse.batches.retried").increment(1);
                    let backoff =
                        calculate_backoff(attempt - 1, initial_backoff_ms, max_backoff_ms);
                    warn!(
                        session_id,
                        attempt,
                        max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error_class = %classified,
                        "Retryable ClickHouse insert error: {}", e
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }
}

#[async_trait]
impl Sink for ClickHouseSink {
    type Config = ClickHouseSinkConfig;

    fn spec() -> ConnectorSpec {
        ConnectorSpec::new("clickhouse", env!("CARGO_PKG_VERSION"))
            .description("ClickHouse sink — high-throughput inserts via native RowBinary over HTTP")
            .documentation_url("https://rivven.dev/docs/connectors/clickhouse-sink")
            .config_schema_from::<ClickHouseSinkConfig>()
            .metadata("protocol", "http")
            .metadata("encoding", "RowBinary")
            .metadata("compression", "lz4")
            .metadata("auth", "password,access-token")
            .metadata("circuit_breaker", "true")
            .metadata("metrics", "true")
    }

    async fn check(&self, config: &Self::Config) -> crate::error::Result<CheckResult> {
        let mut builder = CheckResult::builder();

        // ── Config validation ──────────────────────────────────────
        let t0 = std::time::Instant::now();

        match config.validate_auth() {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("auth_config")
                        .with_duration_ms(t0.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("auth_config", &e)
                        .with_duration_ms(t0.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        if let Err(e) = config.validate_retry() {
            builder = builder.check(CheckDetail::failed("retry_config", &e));
            return Ok(builder.build());
        }

        // ── Database name validation ────────────────────────────────
        let t1 = std::time::Instant::now();
        match Self::Config::validate_database_name(&config.database) {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("database_name")
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("database_name", &e)
                        .with_duration_ms(t1.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        // ── Table name validation ───────────────────────────────────
        let t2 = std::time::Instant::now();
        match Self::Config::validate_table_name(&config.table) {
            Ok(()) => {
                builder = builder.check(
                    CheckDetail::passed("table_name")
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                builder = builder.check(
                    CheckDetail::failed("table_name", &e)
                        .with_duration_ms(t2.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        info!(
            url = %config.url,
            database = %config.database,
            table = %config.table,
            "Checking ClickHouse connectivity"
        );

        let client = Self::create_client(config)?;

        // ── Connectivity check ──────────────────────────────────────
        let t3 = std::time::Instant::now();
        match client.query("SELECT 1").fetch_one::<u8>().await {
            Ok(_) => {
                builder = builder.check(
                    CheckDetail::passed("connectivity")
                        .with_duration_ms(t3.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                let msg = format!("ClickHouse connectivity check failed: {}", e);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("connectivity", &msg)
                        .with_duration_ms(t3.elapsed().as_millis() as u64),
                );
                return Ok(builder.build());
            }
        }

        // ── Table existence (parameterized query) ───────────────────
        let t4 = std::time::Instant::now();
        let table_query = "SELECT count() FROM system.tables WHERE database = ? AND name = ?";
        match client
            .query(table_query)
            .bind(&config.database)
            .bind(&config.table)
            .fetch_one::<u64>()
            .await
        {
            Ok(count) if count > 0 => {
                info!(
                    database = %config.database,
                    table = %config.table,
                    "ClickHouse table verified"
                );
                builder = builder.check(
                    CheckDetail::passed("table_exists")
                        .with_duration_ms(t4.elapsed().as_millis() as u64),
                );
            }
            Ok(_) => {
                let msg = format!(
                    "Table '{}.{}' not found in ClickHouse",
                    config.database, config.table
                );
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("table_exists", &msg)
                        .with_duration_ms(t4.elapsed().as_millis() as u64),
                );
            }
            Err(e) => {
                let msg = format!("Failed to query ClickHouse metadata: {}", e);
                warn!("{}", msg);
                builder = builder.check(
                    CheckDetail::failed("table_exists", &msg)
                        .with_duration_ms(t4.elapsed().as_millis() as u64),
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
        config.validate_auth().map_err(ConnectorError::Config)?;
        config.validate_retry().map_err(ConnectorError::Config)?;
        ClickHouseSinkConfig::validate_database_name(&config.database)
            .map_err(ConnectorError::Config)?;
        ClickHouseSinkConfig::validate_table_name(&config.table).map_err(ConnectorError::Config)?;

        let session_id = generate_session_id();
        let client = Self::create_client(config)?;
        let mut batch: Vec<ClickHouseRow> = Vec::with_capacity(config.batch_size);
        let mut total_written = 0u64;
        let mut total_bytes = 0u64;
        let mut total_failed = 0u64;
        let mut errors: Vec<String> = Vec::new();

        // Timer-based flush via tokio::select! — prevents stale partial batches
        // when no events arrive for extended periods (I-11 pattern).
        let flush_duration = Duration::from_secs(config.flush_interval_secs);
        let mut flush_ticker = tokio::time::interval(flush_duration);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush_ticker.tick().await; // consume initial immediate tick

        info!(
            session_id,
            url = %config.url,
            database = %config.database,
            table = %config.table,
            batch_size = config.batch_size,
            compression = ?config.compression,
            cb_enabled = config.circuit_breaker.enabled,
            "Starting ClickHouse sink"
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
                            let (row, payload_bytes) = ClickHouseRow::from_event(&event);
                            batch.push(row);
                            total_bytes += payload_bytes as u64;
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
                            session_id,
                            records = batch.len(),
                            table = %config.table,
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
                        counter!("clickhouse.circuit_breaker.rejected").increment(1);
                        warn!(
                            session_id,
                            batch_size, "Circuit breaker OPEN — rejecting batch"
                        );
                        // Skip this batch but don't abort the entire sink.
                        total_failed += batch_size as u64;
                        errors.push(format!(
                            "Batch of {} rows rejected by circuit breaker",
                            batch_size
                        ));
                        batch = Vec::with_capacity(config.batch_size);
                        if stream_ended {
                            break;
                        }
                        continue;
                    }
                }

                gauge!("clickhouse.batch.size").set(batch_size as f64);
                debug!(session_id, batch_size, table = %config.table, "Flushing batch to ClickHouse");

                let t0 = std::time::Instant::now();

                match Self::insert_batch(
                    &client,
                    &config.table,
                    &batch,
                    config.max_retries,
                    config.initial_backoff_ms,
                    config.max_backoff_ms,
                    &session_id,
                )
                .await
                {
                    Ok(()) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("clickhouse.batch.duration_ms").record(elapsed_ms);
                        counter!("clickhouse.batches.success").increment(1);
                        counter!("clickhouse.records.written").increment(batch_size as u64);
                        total_written += batch_size as u64;
                        debug!(session_id, total_written, "Batch inserted successfully");

                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_success();
                        }
                    }
                    Err(e) => {
                        let elapsed_ms = t0.elapsed().as_millis() as f64;
                        histogram!("clickhouse.batch.duration_ms").record(elapsed_ms);
                        counter!("clickhouse.batches.failed").increment(1);
                        counter!("clickhouse.records.failed").increment(batch_size as u64);
                        error!(session_id, "Failed to insert batch to ClickHouse: {}", e);

                        if let Some(ref cb) = self.circuit_breaker {
                            cb.record_failure();
                        }

                        if config.skip_invalid_rows {
                            total_failed += batch_size as u64;
                            errors.push(format!("Batch of {} rows failed: {}", batch_size, e));
                        } else {
                            return Err(e.into());
                        }
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
                "ClickHouse sink completed"
            );
        } else {
            info!(
                session_id,
                total_written, total_failed, total_bytes, "ClickHouse sink completed"
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

/// Factory for creating ClickHouse sink instances
pub struct ClickHouseSinkFactory;

impl SinkFactory for ClickHouseSinkFactory {
    fn spec(&self) -> ConnectorSpec {
        ClickHouseSink::spec()
    }

    fn create(&self) -> Box<dyn AnySink> {
        Box::new(ClickHouseSink::new())
    }
}

// Implement AnySink for ClickHouseSink
crate::impl_any_sink!(ClickHouseSink, ClickHouseSinkConfig);

/// Register the ClickHouse sink with the given registry
pub fn register(registry: &mut crate::SinkRegistry) {
    use std::sync::Arc;
    registry.register("clickhouse", Arc::new(ClickHouseSinkFactory));
}

// ─────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = ClickHouseSink::spec();
        assert_eq!(spec.connector_type, "clickhouse");
        assert!(spec.config_schema.is_some());
        // Verify enriched metadata
        let meta = &spec.metadata;
        assert_eq!(
            meta.get("circuit_breaker").map(|s| s.as_str()),
            Some("true")
        );
        assert_eq!(meta.get("metrics").map(|s| s.as_str()), Some("true"));
        assert_eq!(meta.get("encoding").map(|s| s.as_str()), Some("RowBinary"));
    }

    #[test]
    fn test_default_config() {
        let config = ClickHouseSinkConfig::default();
        assert_eq!(config.database, "default");
        assert_eq!(config.username, "default");
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.compression, ClickHouseCompression::Lz4);
        assert!(config.validation);
        assert!(!config.skip_invalid_rows);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
        assert!(config.password.is_none());
        assert!(config.access_token.is_none());
        // Circuit breaker defaults
        assert!(config.circuit_breaker.enabled);
        assert_eq!(config.circuit_breaker.failure_threshold, 5);
        assert_eq!(config.circuit_breaker.reset_timeout_secs, 30);
        assert_eq!(config.circuit_breaker.success_threshold, 2);
    }

    #[test]
    fn test_factory() {
        let factory = ClickHouseSinkFactory;
        let spec = factory.spec();
        assert_eq!(spec.connector_type, "clickhouse");
        // Factory create() → new() → with_config(&default) → CB is always active
        let _sink = factory.create();
    }

    #[test]
    fn test_new_has_circuit_breaker() {
        // Critical: new() must always have a circuit breaker (production path)
        let sink = ClickHouseSink::new();
        assert!(
            sink.circuit_breaker.is_some(),
            "new() must create a circuit breaker by default"
        );
    }

    #[test]
    fn test_config_validation() {
        let config = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            database: "mydb".to_string(),
            table: "events".to_string(),
            username: "default".to_string(),
            password: Some(SensitiveString::new("secret")),
            batch_size: 5000,
            ..Default::default()
        };

        // Valid config
        assert!(config.validate().is_ok());
        assert!(config.validate_auth().is_ok());
        assert!(config.validate_retry().is_ok());

        // Invalid: both password and access_token
        let mut dual_auth = config.clone();
        dual_auth.access_token = Some(SensitiveString::new("token"));
        assert!(dual_auth.validate_auth().is_err());

        // Invalid: batch_size = 0
        let mut bad_batch = config.clone();
        bad_batch.batch_size = 0;
        assert!(bad_batch.validate().is_err());

        // Invalid: batch_size too large
        let mut huge_batch = config.clone();
        huge_batch.batch_size = 2_000_000;
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
url: "http://localhost:8123"
table: "events"
"#;
        let config: ClickHouseSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "http://localhost:8123");
        assert_eq!(config.table, "events");
        assert_eq!(config.database, "default");
        assert_eq!(config.username, "default");
        assert_eq!(config.batch_size, 10_000);
        assert_eq!(config.flush_interval_secs, 5);
        assert_eq!(config.compression, ClickHouseCompression::Lz4);
        assert!(config.validation);
    }

    #[test]
    fn test_config_with_access_token() {
        let yaml = r#"
url: "https://abc123.clickhouse.cloud:8443"
database: "production"
table: "events"
username: "default"
access_token: "eyJhbGciOi..."
batch_size: 50000
compression: lz4
validation: false
"#;
        let config: ClickHouseSinkConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "https://abc123.clickhouse.cloud:8443");
        assert_eq!(config.database, "production");
        assert!(config.access_token.is_some());
        assert!(config.password.is_none());
        assert_eq!(config.batch_size, 50_000);
        assert!(!config.validation);
        assert!(config.validate_auth().is_ok());
    }

    #[test]
    fn test_compression_serde() {
        let yaml_lz4 = "\"lz4\"";
        let c: ClickHouseCompression = serde_yaml::from_str(yaml_lz4).unwrap();
        assert_eq!(c, ClickHouseCompression::Lz4);

        let yaml_none = "\"none\"";
        let c: ClickHouseCompression = serde_yaml::from_str(yaml_none).unwrap();
        assert_eq!(c, ClickHouseCompression::None);
    }

    #[test]
    fn test_row_serialization() {
        let row = ClickHouseRow {
            event_type: "insert".to_string(),
            stream: "orders".to_string(),
            namespace: "public".to_string(),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            data: r#"{"id":1,"amount":99.99}"#.to_string(),
            metadata: r#"{}"#.to_string(),
            _ingested_at: "2026-01-01T00:00:01Z".to_string(),
        };

        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["event_type"], "insert");
        assert_eq!(json["stream"], "orders");
        assert_eq!(json["namespace"], "public");
        assert_eq!(json["timestamp"], "2026-01-01T00:00:00Z");
    }

    #[test]
    fn test_url_validation() {
        // Valid URLs
        let valid = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };
        assert!(valid.validate().is_ok());

        let valid_https = ClickHouseSinkConfig {
            url: "https://abc.clickhouse.cloud:8443".to_string(),
            table: "events".to_string(),
            ..Default::default()
        };
        assert!(valid_https.validate().is_ok());

        // Invalid: empty URL
        let empty_url = ClickHouseSinkConfig {
            url: String::new(),
            table: "events".to_string(),
            ..Default::default()
        };
        assert!(empty_url.validate().is_err());
    }

    #[test]
    fn test_config_retry_bounds() {
        let config = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            table: "events".to_string(),
            max_retries: 10,
            initial_backoff_ms: 50,
            max_backoff_ms: 300_000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
        assert!(config.validate_retry().is_ok());

        // Boundary: max_retries = 0 (no retries)
        let no_retry = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            table: "events".to_string(),
            max_retries: 0,
            ..Default::default()
        };
        assert!(no_retry.validate().is_ok());
    }

    // ── New tests ──────────────────────────────────────────────────

    #[test]
    fn test_classify_clickhouse_error() {
        use clickhouse::error::Error;

        let timeout_err = Error::Custom("Request timeout exceeded".to_string());
        let classified = classify_clickhouse_error(&timeout_err);
        assert!(
            matches!(classified, ConnectorError::Timeout(_)),
            "Expected Timeout, got {:?}",
            classified
        );
        assert!(classified.is_retryable());

        let conn_err = Error::Custom("connection refused".to_string());
        let classified = classify_clickhouse_error(&conn_err);
        assert!(matches!(classified, ConnectorError::Connection(_)));
        assert!(classified.is_retryable());

        let rate_err = Error::Custom("Too Many Requests".to_string());
        let classified = classify_clickhouse_error(&rate_err);
        assert!(matches!(classified, ConnectorError::RateLimited(_)));
        assert!(classified.is_retryable());

        let auth_err = Error::Custom("authentication failed: wrong password".to_string());
        let classified = classify_clickhouse_error(&auth_err);
        assert!(matches!(classified, ConnectorError::Auth(_)));
        assert!(!classified.is_retryable());

        let schema_err = Error::Custom("No such column 'foo' in table 'bar'".to_string());
        let classified = classify_clickhouse_error(&schema_err);
        assert!(matches!(classified, ConnectorError::Schema(_)));
        assert!(!classified.is_retryable());

        let fatal_err = Error::Custom("some unknown error".to_string());
        let classified = classify_clickhouse_error(&fatal_err);
        assert!(matches!(classified, ConnectorError::Fatal(_)));
        assert!(!classified.is_retryable());
    }

    #[test]
    fn test_calculate_backoff() {
        // Attempt 0: should be close to initial_ms
        let d0 = calculate_backoff(0, 200, 5000);
        // With ±25% jitter: 150..250 ms
        assert!(d0.as_millis() >= 100, "backoff too small: {:?}", d0);
        assert!(d0.as_millis() <= 350, "backoff too large: {:?}", d0);

        // Higher attempt should produce larger backoff
        let d5 = calculate_backoff(5, 200, 5000);
        // 200 * 2^5 = 6400 → capped to 5000, ±25%: 3750..6250
        assert!(d5.as_millis() >= 3000, "backoff too small: {:?}", d5);
        assert!(d5.as_millis() <= 7000, "backoff too large: {:?}", d5);

        // Very high attempt should be capped to max
        let d10 = calculate_backoff(10, 200, 5000);
        assert!(d10.as_millis() <= 7000);
    }

    #[test]
    fn test_generate_session_id() {
        let id1 = generate_session_id();
        assert!(id1.starts_with("rivven-ch-"));
        let parts: Vec<&str> = id1.split('-').collect();
        assert!(parts.len() >= 4, "session_id format: {:?}", parts);

        // Two sequential calls should produce different IDs
        let id2 = generate_session_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_circuit_breaker_allows_when_closed() {
        let config = ClickHouseCBConfig {
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
        let config = ClickHouseCBConfig {
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
        assert!(!cb.allow_request()); // reset_timeout_secs = 60 so still open
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let config = ClickHouseCBConfig {
            enabled: true,
            failure_threshold: 2,
            reset_timeout_secs: 0, // immediate half-open
            success_threshold: 1,
        };
        let cb = CircuitBreaker::new(&config);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CBState::Open);

        // With reset_timeout_secs = 0, allow_request should transition to HalfOpen
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CBState::HalfOpen);

        cb.record_success();
        assert_eq!(cb.state(), CBState::Closed);
    }

    #[test]
    fn test_circuit_breaker_cb_config_defaults() {
        let cfg = ClickHouseCBConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.failure_threshold, 5);
        assert_eq!(cfg.reset_timeout_secs, 30);
        assert_eq!(cfg.success_threshold, 2);
    }

    #[test]
    fn test_with_config_constructor() {
        let config = ClickHouseSinkConfig::default();
        let sink = ClickHouseSink::with_config(&config);
        // CB is enabled by default
        assert!(sink.circuit_breaker.is_some());

        let mut config_no_cb = ClickHouseSinkConfig::default();
        config_no_cb.circuit_breaker.enabled = false;
        let sink2 = ClickHouseSink::with_config(&config_no_cb);
        assert!(sink2.circuit_breaker.is_none());
    }

    #[test]
    fn test_table_name_validation() {
        // Valid names
        assert!(ClickHouseSinkConfig::validate_table_name("events").is_ok());
        assert!(ClickHouseSinkConfig::validate_table_name("my_table_123").is_ok());
        assert!(ClickHouseSinkConfig::validate_table_name("db.table").is_ok());
        assert!(ClickHouseSinkConfig::validate_table_name("_private").is_ok());

        // Invalid names
        assert!(ClickHouseSinkConfig::validate_table_name("").is_err());
        assert!(ClickHouseSinkConfig::validate_table_name("123starts_with_digit").is_err());
        assert!(ClickHouseSinkConfig::validate_table_name("has space").is_err());
        assert!(ClickHouseSinkConfig::validate_table_name("has;semicolon").is_err());
        assert!(ClickHouseSinkConfig::validate_table_name("Robert'; DROP TABLE--").is_err());
    }

    #[test]
    fn test_database_name_validation() {
        assert!(ClickHouseSinkConfig::validate_database_name("default").is_ok());
        assert!(ClickHouseSinkConfig::validate_database_name("my_db").is_ok());
        assert!(ClickHouseSinkConfig::validate_database_name("").is_err());
        assert!(ClickHouseSinkConfig::validate_database_name("Robert'; DROP DATABASE--").is_err());
    }

    #[test]
    fn test_config_serde_roundtrip() {
        let config = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            database: "testdb".to_string(),
            table: "events".to_string(),
            username: "admin".to_string(),
            password: Some(SensitiveString::new("secret")),
            access_token: None,
            batch_size: 5000,
            flush_interval_secs: 10,
            compression: ClickHouseCompression::None,
            validation: false,
            skip_invalid_rows: true,
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 10_000,
            circuit_breaker: ClickHouseCBConfig {
                enabled: false,
                failure_threshold: 10,
                reset_timeout_secs: 60,
                success_threshold: 3,
            },
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: ClickHouseSinkConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.url, config.url);
        assert_eq!(deserialized.database, config.database);
        assert_eq!(deserialized.table, config.table);
        assert_eq!(deserialized.batch_size, config.batch_size);
        assert_eq!(deserialized.flush_interval_secs, config.flush_interval_secs);
        assert_eq!(deserialized.compression, config.compression);
        assert_eq!(deserialized.validation, config.validation);
        assert_eq!(deserialized.skip_invalid_rows, config.skip_invalid_rows);
        assert_eq!(deserialized.max_retries, config.max_retries);
        assert!(!deserialized.circuit_breaker.enabled);
        assert_eq!(deserialized.circuit_breaker.failure_threshold, 10);
        assert_eq!(deserialized.circuit_breaker.reset_timeout_secs, 60);
        assert_eq!(deserialized.circuit_breaker.success_threshold, 3);
    }

    #[test]
    fn test_row_from_event() {
        use chrono::Utc;
        use serde_json::json;

        let event = SourceEvent {
            event_type: SourceEventType::Insert,
            stream: "orders".to_string(),
            namespace: Some("public".to_string()),
            timestamp: Utc::now(),
            data: json!({"id": 1, "amount": 99.99}),
            metadata: EventMetadata::default(),
        };

        let (row, payload_bytes) = ClickHouseRow::from_event(&event);
        assert_eq!(row.event_type, "insert");
        assert_eq!(row.stream, "orders");
        assert_eq!(row.namespace, "public");
        assert!(payload_bytes > 0);
        assert!(row.data.contains("amount"));
    }

    #[test]
    fn test_cb_config_validation() {
        // Valid defaults
        let valid = ClickHouseCBConfig::default();
        assert!(valid.validate().is_ok());

        // Invalid: failure_threshold = 0
        let bad = ClickHouseCBConfig {
            failure_threshold: 0,
            ..Default::default()
        };
        assert!(bad.validate().is_err());

        // Invalid: failure_threshold > 100
        let too_high = ClickHouseCBConfig {
            failure_threshold: 101,
            ..Default::default()
        };
        assert!(too_high.validate().is_err());

        // Invalid: reset_timeout_secs > 3600
        let bad_timeout = ClickHouseCBConfig {
            reset_timeout_secs: 7200,
            ..Default::default()
        };
        assert!(bad_timeout.validate().is_err());

        // Invalid: success_threshold > 10
        let bad_success = ClickHouseCBConfig {
            success_threshold: 11,
            ..Default::default()
        };
        assert!(bad_success.validate().is_err());

        // Invalid: success_threshold = 0
        let zero_success = ClickHouseCBConfig {
            success_threshold: 0,
            ..Default::default()
        };
        assert!(zero_success.validate().is_err());
    }

    #[test]
    fn test_nested_cb_config_validation() {
        // Parent config with invalid CB config should fail validate()
        let config = ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            table: "events".to_string(),
            circuit_breaker: ClickHouseCBConfig {
                failure_threshold: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_backoff_overflow_safety() {
        // At attempt >= 64, wrapping_shl would reset to initial_ms.
        // With checked_shl, it should saturate to max_ms.
        let d64 = calculate_backoff(64, 200, 5000);
        assert!(
            d64.as_millis() >= 3000,
            "attempt=64 should be capped to max, got {:?}",
            d64
        );

        let d100 = calculate_backoff(100, 200, 5000);
        assert!(
            d100.as_millis() >= 3000,
            "attempt=100 should be capped to max, got {:?}",
            d100
        );
    }

    #[test]
    fn test_error_classification_expected_not_overbroad() {
        use clickhouse::error::Error;

        // "expected column" → Schema (correct)
        let schema_err = Error::Custom("expected column 'id' but got 'name'".to_string());
        let classified = classify_clickhouse_error(&schema_err);
        assert!(
            matches!(classified, ConnectorError::Schema(_)),
            "Expected Schema, got {:?}",
            classified
        );

        // "expected '" → Schema (correct, ClickHouse syntax errors)
        let syntax_err = Error::Custom("expected ';' at end of query".to_string());
        let classified = classify_clickhouse_error(&syntax_err);
        assert!(
            matches!(classified, ConnectorError::Schema(_)),
            "Expected Schema, got {:?}",
            classified
        );

        // "expected" alone (without column/quote) → should NOT be Schema
        let ambiguous_err = Error::Custom("the server expected more data".to_string());
        let classified = classify_clickhouse_error(&ambiguous_err);
        assert!(
            !matches!(classified, ConnectorError::Schema(_)),
            "Bare 'expected' should not be classified as Schema, got {:?}",
            classified
        );
    }

    #[test]
    fn test_circuit_breaker_record_failure_exact_threshold() {
        // Ensure exactly one Open transition (== not >=)
        let config = ClickHouseCBConfig {
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

        // Additional failures beyond threshold should NOT re-trigger state change
        cb.record_failure(); // 3 > threshold, == is false, no duplicate transition
        assert_eq!(cb.state(), CBState::Open);
    }

    #[test]
    fn test_circuit_breaker_diagnostics() {
        let config = ClickHouseCBConfig {
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
}
