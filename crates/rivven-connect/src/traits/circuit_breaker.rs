//! Circuit breaker pattern for resilient connections
//!
//! This module provides a circuit breaker implementation to prevent
//! cascading failures when external services are unavailable.
//!
//! # States
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Circuit is tripped, requests fail immediately
//! - **Half-Open**: Testing if service has recovered
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::prelude::*;
//!
//! let breaker = CircuitBreaker::new()
//!     .with_failure_threshold(5)
//!     .with_reset_timeout(Duration::from_secs(30))
//!     .with_half_open_requests(3);
//!
//! async fn make_request(breaker: &CircuitBreaker) -> Result<Response> {
//!     breaker.call(|| async {
//!         // Make external request
//!         client.get("https://api.example.com/data").await
//!     }).await
//! }
//! ```

use parking_lot::RwLock;
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CircuitState {
    /// Normal operation - requests pass through
    Closed = 0,
    /// Circuit is open - requests fail immediately
    Open = 1,
    /// Testing if service has recovered
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Configuration for circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u64,
    /// Time to wait before trying again (half-open)
    pub reset_timeout: Duration,
    /// Number of successful requests in half-open before closing
    pub success_threshold: u64,
    /// Number of requests allowed in half-open state
    pub half_open_requests: u64,
    /// Time window for counting failures
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold: 3,
            half_open_requests: 3,
            failure_window: Duration::from_secs(60),
        }
    }
}

/// Circuit breaker for protecting against cascading failures
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
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
    /// Create a new circuit breaker with default configuration
    pub fn new() -> Self {
        Self {
            config: CircuitBreakerConfig::default(),
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            half_open_count: AtomicU64::new(0),
            last_failure: RwLock::new(None),
            last_state_change: RwLock::new(Instant::now()),
        }
    }

    /// Create a circuit breaker with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            half_open_count: AtomicU64::new(0),
            last_failure: RwLock::new(None),
            last_state_change: RwLock::new(Instant::now()),
        }
    }

    /// Set the failure threshold
    pub fn with_failure_threshold(mut self, threshold: u64) -> Self {
        self.config.failure_threshold = threshold;
        self
    }

    /// Set the reset timeout
    pub fn with_reset_timeout(mut self, timeout: Duration) -> Self {
        self.config.reset_timeout = timeout;
        self
    }

    /// Set the success threshold for closing from half-open
    pub fn with_success_threshold(mut self, threshold: u64) -> Self {
        self.config.success_threshold = threshold;
        self
    }

    /// Set the number of requests allowed in half-open state
    pub fn with_half_open_requests(mut self, requests: u64) -> Self {
        self.config.half_open_requests = requests;
        self
    }

    /// Set the failure counting window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.config.failure_window = window;
        self
    }

    /// Get the current state
    pub fn state(&self) -> CircuitState {
        self.state.load(Ordering::SeqCst).into()
    }

    /// Get the current failure count
    pub fn failure_count(&self) -> u64 {
        self.failure_count.load(Ordering::SeqCst)
    }

    /// Check if a request is allowed
    pub fn is_allowed(&self) -> bool {
        match self.state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if reset timeout has passed
                let last_change = *self.last_state_change.read();
                if last_change.elapsed() >= self.config.reset_timeout {
                    // Transition to half-open
                    self.transition_to(CircuitState::HalfOpen);
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests
                let count = self.half_open_count.load(Ordering::SeqCst);
                count < self.config.half_open_requests
            }
        }
    }

    /// Execute a function through the circuit breaker
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.is_allowed() {
            return Err(CircuitBreakerError::Open);
        }

        // Track half-open requests
        if self.state() == CircuitState::HalfOpen {
            self.half_open_count.fetch_add(1, Ordering::SeqCst);
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

    /// Record a successful operation
    pub fn record_success(&self) {
        match self.state() {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.config.success_threshold {
                    // Transition to closed
                    self.transition_to(CircuitState::Closed);
                }
            }
            CircuitState::Open => {
                // Should not happen
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        *self.last_failure.write() = Some(Instant::now());

        match self.state() {
            CircuitState::Closed => {
                // Check if failure is within the window
                let should_count = self
                    .last_failure
                    .read()
                    .map(|t| t.elapsed() < self.config.failure_window)
                    .unwrap_or(true);

                if should_count {
                    let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                    if count >= self.config.failure_threshold {
                        self.transition_to(CircuitState::Open);
                    }
                } else {
                    // Reset and start counting again
                    self.failure_count.store(1, Ordering::SeqCst);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                self.transition_to(CircuitState::Open);
            }
            CircuitState::Open => {
                // Already open
            }
        }
    }

    /// Manually reset the circuit breaker
    pub fn reset(&self) {
        self.transition_to(CircuitState::Closed);
    }

    /// Manually open the circuit breaker
    pub fn trip(&self) {
        self.transition_to(CircuitState::Open);
    }

    fn transition_to(&self, new_state: CircuitState) {
        let old_state = self.state.swap(new_state as u8, Ordering::SeqCst);

        if old_state != new_state as u8 {
            *self.last_state_change.write() = Instant::now();

            // Reset counters on state change
            match new_state {
                CircuitState::Closed => {
                    self.failure_count.store(0, Ordering::SeqCst);
                    self.success_count.store(0, Ordering::SeqCst);
                    self.half_open_count.store(0, Ordering::SeqCst);
                }
                CircuitState::Open => {
                    self.success_count.store(0, Ordering::SeqCst);
                    self.half_open_count.store(0, Ordering::SeqCst);
                }
                CircuitState::HalfOpen => {
                    self.success_count.store(0, Ordering::SeqCst);
                    self.half_open_count.store(0, Ordering::SeqCst);
                }
            }
        }
    }
}

/// Error returned by circuit breaker
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, request was rejected
    Open,
    /// The underlying operation failed
    Inner(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::Open => write!(f, "circuit breaker is open"),
            CircuitBreakerError::Inner(e) => write!(f, "{}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::Open => None,
            CircuitBreakerError::Inner(e) => Some(e),
        }
    }
}

/// A shared circuit breaker that can be cloned
#[derive(Debug, Clone)]
pub struct SharedCircuitBreaker {
    inner: Arc<CircuitBreaker>,
}

impl SharedCircuitBreaker {
    /// Create a new shared circuit breaker
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CircuitBreaker::new()),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(CircuitBreaker::with_config(config)),
        }
    }

    /// Get the current state
    pub fn state(&self) -> CircuitState {
        self.inner.state()
    }

    /// Check if a request is allowed
    pub fn is_allowed(&self) -> bool {
        self.inner.is_allowed()
    }

    /// Execute a function through the circuit breaker
    pub async fn call<F, Fut, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.inner.call(f).await
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        self.inner.record_success();
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        self.inner.record_failure();
    }

    /// Reset the circuit breaker
    pub fn reset(&self) {
        self.inner.reset();
    }

    /// Trip the circuit breaker
    pub fn trip(&self) {
        self.inner.trip();
    }
}

impl Default for SharedCircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_initial_state() {
        let breaker = CircuitBreaker::new();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let breaker = CircuitBreaker::new().with_failure_threshold(3);

        // Record failures
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_reset_on_success() {
        let breaker = CircuitBreaker::new().with_failure_threshold(3);

        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.failure_count(), 2);

        breaker.record_success();
        assert_eq!(breaker.failure_count(), 0);
    }

    #[test]
    fn test_circuit_breaker_manual_reset() {
        let breaker = CircuitBreaker::new().with_failure_threshold(1);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert!(breaker.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_manual_trip() {
        let breaker = CircuitBreaker::new();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.trip();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.is_allowed());
    }

    #[tokio::test]
    async fn test_circuit_breaker_call_success() {
        let breaker = CircuitBreaker::new();

        let result: Result<i32, CircuitBreakerError<&str>> =
            breaker.call(|| async { Ok::<_, &str>(42) }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_circuit_breaker_call_failure() {
        let breaker = CircuitBreaker::new();

        let result: Result<i32, CircuitBreakerError<&str>> =
            breaker.call(|| async { Err::<i32, _>("error") }).await;

        assert!(matches!(result, Err(CircuitBreakerError::Inner("error"))));
    }

    #[tokio::test]
    async fn test_circuit_breaker_rejects_when_open() {
        let breaker = CircuitBreaker::new().with_failure_threshold(1);

        // Trip the breaker
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
        assert_eq!(CircuitState::from(255), CircuitState::Closed); // Unknown defaults to Closed
    }
}
