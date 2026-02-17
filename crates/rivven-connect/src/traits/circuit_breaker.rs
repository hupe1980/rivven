//! Circuit breaker pattern for resilient connector operations.
//!
//! Prevents cascading failures when external services are unavailable by
//! automatically short-circuiting requests after repeated failures and
//! probing recovery with a half-open window.
//!
//! # States
//!
//! - **Closed** — Normal operation, requests pass through.
//! - **Open** — Circuit is tripped, requests fail immediately.
//! - **Half-Open** — Testing if service has recovered; limited probe requests.
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::prelude::*;
//!
//! let breaker = CircuitBreaker::new()
//!     .with_failure_threshold(5)
//!     .with_reset_timeout(Duration::from_secs(30));
//!
//! if breaker.allow_request() {
//!     match do_work().await {
//!         Ok(_) => breaker.record_success(),
//!         Err(_) => breaker.record_failure(),
//!     }
//! }
//! ```

use parking_lot::RwLock;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// CircuitState
// ---------------------------------------------------------------------------

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    /// Normal operation — requests pass through.
    Closed = 0,
    /// Circuit is open — requests fail immediately.
    Open = 1,
    /// Testing if service has recovered.
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Closed,
            1 => Self::Open,
            2 => Self::HalfOpen,
            _ => Self::Closed,
        }
    }
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "closed"),
            Self::Open => write!(f, "open"),
            Self::HalfOpen => write!(f, "half-open"),
        }
    }
}

// ---------------------------------------------------------------------------
// CircuitBreakerConfig
// ---------------------------------------------------------------------------

/// Serde-friendly circuit breaker configuration.
///
/// Connectors embed this directly in their config structs and the SDK
/// constructs a [`CircuitBreaker`] from it via [`CircuitBreaker::from_config`].
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct CircuitBreakerConfig {
    /// Whether the circuit breaker is enabled (default `true`).
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Number of consecutive failures before opening the circuit (default 5).
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    /// Seconds to wait before transitioning Open → Half-Open (default 30).
    #[serde(default = "default_reset_timeout_secs")]
    pub reset_timeout_secs: u64,

    /// Successful probes in half-open before closing the circuit (default 2).
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,
}

fn default_enabled() -> bool {
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
            enabled: default_enabled(),
            failure_threshold: default_failure_threshold(),
            reset_timeout_secs: default_reset_timeout_secs(),
            success_threshold: default_success_threshold(),
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics snapshot
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of circuit breaker state for metrics / logging.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerDiagnostics {
    pub state: CircuitState,
    pub failure_count: u64,
    pub success_count: u64,
}

// ---------------------------------------------------------------------------
// CircuitBreaker
// ---------------------------------------------------------------------------

/// Circuit breaker for protecting against cascading failures.
///
/// Lock-free hot path: state, failure_count, success_count use atomics with
/// `Acquire`/`Release` ordering. The `last_failure` and `last_state_change`
/// timestamps use a parking_lot `RwLock` that is held for O(1) only and
/// never across `.await`.
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u64,
    reset_timeout: Duration,
    success_threshold: u64,
    state: AtomicU8,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    half_open_count: AtomicU64,
    last_failure: RwLock<Option<Instant>>,
    last_state_change: RwLock<Instant>,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

impl CircuitBreaker {
    /// Create a circuit breaker with default thresholds.
    pub fn new() -> Self {
        Self::from_config(&CircuitBreakerConfig::default())
    }

    /// Create a circuit breaker from a serde-friendly config.
    ///
    /// This is the preferred constructor for connector code:
    /// ```rust,ignore
    /// let cb = CircuitBreaker::from_config(&config.circuit_breaker);
    /// ```
    pub fn from_config(config: &CircuitBreakerConfig) -> Self {
        Self {
            failure_threshold: config.failure_threshold as u64,
            reset_timeout: Duration::from_secs(config.reset_timeout_secs),
            success_threshold: config.success_threshold as u64,
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            half_open_count: AtomicU64::new(0),
            last_failure: RwLock::new(None),
            last_state_change: RwLock::new(Instant::now()),
        }
    }

    // -- Builder methods (chainable, for test / SDK usage) -------------------

    /// Set the failure threshold.
    pub fn with_failure_threshold(mut self, threshold: u64) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set the reset timeout.
    pub fn with_reset_timeout(mut self, timeout: Duration) -> Self {
        self.reset_timeout = timeout;
        self
    }

    /// Set the success threshold for closing from half-open.
    pub fn with_success_threshold(mut self, threshold: u64) -> Self {
        self.success_threshold = threshold;
        self
    }

    // -- Queries -------------------------------------------------------------

    /// Current state.
    pub fn state(&self) -> CircuitState {
        self.state.load(Ordering::Acquire).into()
    }

    /// Current failure count.
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::Acquire)
    }

    /// Point-in-time diagnostics snapshot for metrics / logging.
    pub fn diagnostics(&self) -> CircuitBreakerDiagnostics {
        CircuitBreakerDiagnostics {
            state: self.state(),
            failure_count: self.failure_count.load(Ordering::Acquire),
            success_count: self.success_count.load(Ordering::Acquire),
        }
    }

    // -- Gate ----------------------------------------------------------------

    /// Check if a request is allowed through the circuit.
    ///
    /// * **Closed** → always `true`.
    /// * **Open** → `true` only if the reset timeout has elapsed (transitions
    ///   to Half-Open via CAS; losers of the race still get `true`).
    /// * **Half-Open** → `true` while probe count < `success_threshold`.
    pub fn allow_request(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let elapsed = self.last_state_change.read().elapsed();
                if elapsed >= self.reset_timeout {
                    self.transition_to(CircuitState::HalfOpen);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                let count = self.half_open_count.load(Ordering::Acquire);
                count < self.success_threshold
            }
        }
    }

    // -- Async wrapper -------------------------------------------------------

    /// Execute a fallible async function through the circuit breaker.
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.allow_request() {
            return Err(CircuitBreakerError::Open);
        }

        if self.state() == CircuitState::HalfOpen {
            self.half_open_count.fetch_add(1, Ordering::AcqRel);
        }

        match f().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(CircuitBreakerError::Inner(e))
            }
        }
    }

    // -- Recording -----------------------------------------------------------

    /// Record a successful operation.
    pub fn record_success(&self) {
        match self.state() {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::AcqRel) + 1;
                if count >= self.success_threshold {
                    self.transition_to(CircuitState::Closed);
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed operation.
    pub fn record_failure(&self) {
        *self.last_failure.write() = Some(Instant::now());

        match self.state() {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if count >= self.failure_threshold {
                    self.transition_to(CircuitState::Open);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately re-opens the circuit.
                self.transition_to(CircuitState::Open);
            }
            CircuitState::Open => {}
        }
    }

    // -- Manual controls -----------------------------------------------------

    /// Manually close the circuit.
    pub fn reset(&self) {
        self.transition_to(CircuitState::Closed);
    }

    /// Manually open the circuit.
    pub fn trip(&self) {
        self.transition_to(CircuitState::Open);
    }

    // -- Internal ------------------------------------------------------------

    fn transition_to(&self, new_state: CircuitState) {
        let old = self.state.swap(new_state as u8, Ordering::AcqRel);
        if old != new_state as u8 {
            *self.last_state_change.write() = Instant::now();
            // Reset counters on every state transition.
            self.failure_count.store(0, Ordering::Release);
            self.success_count.store(0, Ordering::Release);
            self.half_open_count.store(0, Ordering::Release);
        }
    }
}

// ---------------------------------------------------------------------------
// CircuitBreakerError
// ---------------------------------------------------------------------------

/// Error returned by [`CircuitBreaker::call`].
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open — request was rejected.
    Open,
    /// The underlying operation failed.
    Inner(E),
}

impl<E: fmt::Display> fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "circuit breaker is open"),
            Self::Inner(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Open => None,
            Self::Inner(e) => Some(e),
        }
    }
}

// ---------------------------------------------------------------------------
// SharedCircuitBreaker
// ---------------------------------------------------------------------------

/// A cloneable circuit breaker backed by `Arc<CircuitBreaker>`.
#[derive(Debug, Clone)]
pub struct SharedCircuitBreaker {
    inner: Arc<CircuitBreaker>,
}

impl SharedCircuitBreaker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CircuitBreaker::new()),
        }
    }

    pub fn with_config(config: &CircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(CircuitBreaker::from_config(config)),
        }
    }

    pub fn state(&self) -> CircuitState {
        self.inner.state()
    }

    pub fn allow_request(&self) -> bool {
        self.inner.allow_request()
    }

    pub fn is_allowed(&self) -> bool {
        self.inner.allow_request()
    }

    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.inner.call(f).await
    }

    pub fn record_success(&self) {
        self.inner.record_success();
    }

    pub fn record_failure(&self) {
        self.inner.record_failure();
    }

    pub fn reset(&self) {
        self.inner.reset();
    }

    pub fn trip(&self) {
        self.inner.trip();
    }

    pub fn diagnostics(&self) -> CircuitBreakerDiagnostics {
        self.inner.diagnostics()
    }
}

impl Default for SharedCircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let breaker = CircuitBreaker::new();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request());
    }

    #[test]
    fn test_opens_after_failures() {
        let breaker = CircuitBreaker::new().with_failure_threshold(3);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request());
    }

    #[test]
    fn test_reset_on_success() {
        let breaker = CircuitBreaker::new().with_failure_threshold(3);

        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.failure_count(), 2);

        breaker.record_success();
        assert_eq!(breaker.failure_count(), 0);
    }

    #[test]
    fn test_manual_reset() {
        let breaker = CircuitBreaker::new().with_failure_threshold(1);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.allow_request());
    }

    #[test]
    fn test_manual_trip() {
        let breaker = CircuitBreaker::new();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.trip();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request());
    }

    #[tokio::test]
    async fn test_call_success() {
        let breaker = CircuitBreaker::new();

        let result: Result<i32, CircuitBreakerError<&str>> =
            breaker.call(|| async { Ok::<_, &str>(42) }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_call_failure() {
        let breaker = CircuitBreaker::new();

        let result: Result<i32, CircuitBreakerError<&str>> =
            breaker.call(|| async { Err::<i32, _>("error") }).await;

        assert!(matches!(result, Err(CircuitBreakerError::Inner("error"))));
    }

    #[tokio::test]
    async fn test_rejects_when_open() {
        let breaker = CircuitBreaker::new().with_failure_threshold(1);

        breaker.record_failure();

        let result: Result<i32, CircuitBreakerError<&str>> =
            breaker.call(|| async { Ok::<_, &str>(42) }).await;

        assert!(matches!(result, Err(CircuitBreakerError::Open)));
    }

    #[test]
    fn test_shared_circuit_breaker() {
        let breaker = SharedCircuitBreaker::new();
        let breaker2 = breaker.clone();

        breaker.record_failure();
        assert_eq!(breaker2.inner.failure_count(), 1);
    }

    #[test]
    fn test_circuit_state_from_u8() {
        assert_eq!(CircuitState::from(0), CircuitState::Closed);
        assert_eq!(CircuitState::from(1), CircuitState::Open);
        assert_eq!(CircuitState::from(2), CircuitState::HalfOpen);
        assert_eq!(CircuitState::from(255), CircuitState::Closed);
    }

    #[test]
    fn test_from_config() {
        let config = CircuitBreakerConfig {
            enabled: true,
            failure_threshold: 10,
            reset_timeout_secs: 60,
            success_threshold: 3,
        };
        let breaker = CircuitBreaker::from_config(&config);
        assert_eq!(breaker.failure_threshold, 10);
        assert_eq!(breaker.reset_timeout, Duration::from_secs(60));
        assert_eq!(breaker.success_threshold, 3);
    }

    #[test]
    fn test_diagnostics() {
        let breaker = CircuitBreaker::new().with_failure_threshold(3);
        breaker.record_failure();
        breaker.record_failure();

        let diag = breaker.diagnostics();
        assert_eq!(diag.state, CircuitState::Closed);
        assert_eq!(diag.failure_count, 2);
        assert_eq!(diag.success_count, 0);
    }

    #[test]
    fn test_serde_config_roundtrip() {
        let config = CircuitBreakerConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deser: CircuitBreakerConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.failure_threshold, config.failure_threshold);
        assert_eq!(deser.reset_timeout_secs, config.reset_timeout_secs);
        assert_eq!(deser.success_threshold, config.success_threshold);
        assert_eq!(deser.enabled, config.enabled);
    }

    #[test]
    fn test_half_open_closes_after_successes() {
        let breaker = CircuitBreaker::new()
            .with_failure_threshold(1)
            .with_reset_timeout(Duration::ZERO)
            .with_success_threshold(2);

        // Trip to Open
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Reset timeout = 0, so allow_request transitions to HalfOpen
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let breaker = CircuitBreaker::new()
            .with_failure_threshold(1)
            .with_reset_timeout(Duration::ZERO);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Transition to HalfOpen
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Any failure in HalfOpen re-opens immediately
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_state_display() {
        assert_eq!(CircuitState::Closed.to_string(), "closed");
        assert_eq!(CircuitState::Open.to_string(), "open");
        assert_eq!(CircuitState::HalfOpen.to_string(), "half-open");
    }
}
