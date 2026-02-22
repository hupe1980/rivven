//! Resilience primitives for CDC operations
//!
//! Production-grade fault tolerance:
//! - Token bucket rate limiter
//! - Circuit breaker pattern
//! - Backoff strategies
//! - Retry configuration
//! - Operational guardrails

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, info, warn};

// ============================================================================
// Retry Configuration
// ============================================================================

/// Error types that can be automatically retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetriableErrorType {
    /// Connection was lost unexpectedly
    ConnectionLost,
    /// Connection was refused by the server
    ConnectionRefused,
    /// Operation timed out
    Timeout,
    /// Database deadlock detected
    DeadlockDetected,
    /// Replication slot is in use by another process
    ReplicationSlotInUse,
    /// Temporary server failure (5xx errors)
    TemporaryFailure,
    /// Server is overloaded
    ServerOverloaded,
    /// Network unreachable
    NetworkUnreachable,
    /// DNS resolution failed
    DnsResolutionFailed,
    /// TLS handshake failed
    TlsHandshakeFailed,
}

impl RetriableErrorType {
    /// Get all default retriable error types.
    pub fn defaults() -> HashSet<Self> {
        [
            Self::ConnectionLost,
            Self::ConnectionRefused,
            Self::Timeout,
            Self::DeadlockDetected,
            Self::ReplicationSlotInUse,
            Self::TemporaryFailure,
            Self::ServerOverloaded,
        ]
        .into_iter()
        .collect()
    }
}

/// Configuration for retry behavior.
///
/// # Example
///
/// ```rust
/// use rivven_cdc::common::{RetryConfig, RetriableErrorType};
/// use std::time::Duration;
///
/// let config = RetryConfig::builder()
///     .max_retries(10)
///     .retry_delay(Duration::from_secs(1))
///     .max_delay(Duration::from_secs(60))
///     .jitter(0.25)
///     .build();
///
/// assert_eq!(config.max_retries(), 10);
/// assert!(config.is_retriable(&RetriableErrorType::ConnectionLost));
/// ```
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum retry attempts.
    /// - `-1` = infinite retries (use with caution)
    /// - `0` = retries disabled
    /// - `n` = retry up to n times
    max_retries: i32,
    /// Base delay between retries (before exponential backoff).
    retry_delay: Duration,
    /// Maximum delay cap (prevents excessive backoff).
    max_delay: Duration,
    /// Jitter factor (0.0 - 1.0) to randomize delays.
    /// Helps prevent thundering herd problems.
    jitter: f64,
    /// Error types considered retriable.
    retriable_errors: HashSet<RetriableErrorType>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            retry_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: 0.25,
            retriable_errors: RetriableErrorType::defaults(),
        }
    }
}

impl RetryConfig {
    /// Create a builder for RetryConfig.
    pub fn builder() -> RetryConfigBuilder {
        RetryConfigBuilder::default()
    }

    /// Create a disabled retry config (no retries).
    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create an infinite retry config.
    pub fn infinite() -> Self {
        Self {
            max_retries: -1,
            ..Default::default()
        }
    }

    /// Get maximum retries (-1 = infinite, 0 = disabled).
    pub fn max_retries(&self) -> i32 {
        self.max_retries
    }

    /// Get base retry delay.
    pub fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    /// Get maximum delay cap.
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    /// Get jitter factor.
    pub fn jitter(&self) -> f64 {
        self.jitter
    }

    /// Check if retries are enabled.
    pub fn is_enabled(&self) -> bool {
        self.max_retries != 0
    }

    /// Check if infinite retries are configured.
    pub fn is_infinite(&self) -> bool {
        self.max_retries == -1
    }

    /// Check if an error type is retriable.
    pub fn is_retriable(&self, error_type: &RetriableErrorType) -> bool {
        self.retriable_errors.contains(error_type)
    }

    /// Check if we should retry given the current attempt.
    pub fn should_retry(&self, attempt: u32) -> bool {
        if self.max_retries == -1 {
            true
        } else if self.max_retries == 0 {
            false
        } else {
            attempt < self.max_retries as u32
        }
    }

    /// Calculate delay for a given attempt (with exponential backoff and jitter).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base = self
            .retry_delay
            .saturating_mul(2u32.saturating_pow(attempt));
        let capped = base.min(self.max_delay);

        // Apply jitter
        if self.jitter > 0.0 {
            let jitter_range = capped.as_secs_f64() * self.jitter;
            // Simple deterministic jitter based on attempt (for reproducibility)
            let jitter_offset = (attempt as f64 * 0.618033988749895) % 1.0; // Golden ratio
            let jitter_amount = jitter_range * (jitter_offset * 2.0 - 1.0);
            let adjusted = capped.as_secs_f64() + jitter_amount;
            Duration::from_secs_f64(adjusted.max(0.0))
        } else {
            capped
        }
    }
}

/// Builder for RetryConfig.
#[derive(Debug, Clone, Default)]
pub struct RetryConfigBuilder {
    max_retries: Option<i32>,
    retry_delay: Option<Duration>,
    max_delay: Option<Duration>,
    jitter: Option<f64>,
    retriable_errors: Option<HashSet<RetriableErrorType>>,
}

impl RetryConfigBuilder {
    /// Set maximum retry attempts.
    pub fn max_retries(mut self, value: i32) -> Self {
        self.max_retries = Some(value);
        self
    }

    /// Set base retry delay.
    pub fn retry_delay(mut self, value: Duration) -> Self {
        self.retry_delay = Some(value);
        self
    }

    /// Set maximum delay cap.
    pub fn max_delay(mut self, value: Duration) -> Self {
        self.max_delay = Some(value);
        self
    }

    /// Set jitter factor (0.0 - 1.0).
    pub fn jitter(mut self, value: f64) -> Self {
        self.jitter = Some(value.clamp(0.0, 1.0));
        self
    }

    /// Set retriable error types.
    pub fn retriable_errors(mut self, errors: HashSet<RetriableErrorType>) -> Self {
        self.retriable_errors = Some(errors);
        self
    }

    /// Add a retriable error type.
    pub fn add_retriable_error(mut self, error: RetriableErrorType) -> Self {
        self.retriable_errors
            .get_or_insert_with(RetriableErrorType::defaults)
            .insert(error);
        self
    }

    /// Build the RetryConfig.
    pub fn build(self) -> RetryConfig {
        let defaults = RetryConfig::default();
        RetryConfig {
            max_retries: self.max_retries.unwrap_or(defaults.max_retries),
            retry_delay: self.retry_delay.unwrap_or(defaults.retry_delay),
            max_delay: self.max_delay.unwrap_or(defaults.max_delay),
            jitter: self.jitter.unwrap_or(defaults.jitter),
            retriable_errors: self.retriable_errors.unwrap_or(defaults.retriable_errors),
        }
    }
}

// ============================================================================
// Guardrails Configuration
// ============================================================================

/// Action to take when a guardrail limit is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailAction {
    /// Log a warning and continue operation.
    #[default]
    Warn,
    /// Skip the excess items and continue.
    Skip,
    /// Stop the connector with an error.
    Fail,
}

/// Operational guardrails to prevent runaway connectors.
///
/// These limits help protect against:
/// - Memory exhaustion from large transactions
/// - Resource exhaustion from too many tables
/// - Hanging snapshots
/// - Queue overflow
///
/// # Example
///
/// ```rust
/// use rivven_cdc::common::{GuardrailsConfig, GuardrailAction};
/// use std::time::Duration;
///
/// let config = GuardrailsConfig::builder()
///     .max_tables(100)
///     .max_tables_action(GuardrailAction::Fail)
///     .max_transaction_size(50_000)
///     .max_queue_depth(10_000)
///     .build();
///
/// assert_eq!(config.max_tables(), 100);
/// assert!(!config.check_tables(50).is_exceeded());
/// assert!(config.check_tables(150).is_exceeded());
/// ```
#[derive(Debug, Clone)]
pub struct GuardrailsConfig {
    /// Maximum number of tables to capture.
    max_tables: usize,
    /// Action when max_tables is exceeded.
    max_tables_action: GuardrailAction,
    /// Maximum events per transaction before flush.
    max_transaction_size: usize,
    /// Maximum memory for transaction buffer (bytes).
    max_transaction_memory: usize,
    /// Maximum queue depth before backpressure.
    max_queue_depth: usize,
    /// Maximum snapshot duration before timeout.
    max_snapshot_duration: Duration,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            max_tables: 10_000,
            max_tables_action: GuardrailAction::Warn,
            max_transaction_size: 100_000,
            max_transaction_memory: 256 * 1024 * 1024, // 256 MB
            max_queue_depth: 10_000,
            max_snapshot_duration: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }
}

impl GuardrailsConfig {
    /// Create a builder for GuardrailsConfig.
    pub fn builder() -> GuardrailsConfigBuilder {
        GuardrailsConfigBuilder::default()
    }

    /// Create a permissive config (high limits).
    pub fn permissive() -> Self {
        Self {
            max_tables: usize::MAX,
            max_tables_action: GuardrailAction::Warn,
            max_transaction_size: usize::MAX,
            max_transaction_memory: usize::MAX,
            max_queue_depth: usize::MAX,
            max_snapshot_duration: Duration::from_secs(u64::MAX),
        }
    }

    /// Create a strict config (low limits).
    pub fn strict() -> Self {
        Self {
            max_tables: 100,
            max_tables_action: GuardrailAction::Fail,
            max_transaction_size: 10_000,
            max_transaction_memory: 64 * 1024 * 1024, // 64 MB
            max_queue_depth: 1_000,
            max_snapshot_duration: Duration::from_secs(60 * 60), // 1 hour
        }
    }

    /// Get max tables limit.
    pub fn max_tables(&self) -> usize {
        self.max_tables
    }

    /// Get action for max_tables exceeded.
    pub fn max_tables_action(&self) -> GuardrailAction {
        self.max_tables_action
    }

    /// Get max transaction size.
    pub fn max_transaction_size(&self) -> usize {
        self.max_transaction_size
    }

    /// Get max transaction memory.
    pub fn max_transaction_memory(&self) -> usize {
        self.max_transaction_memory
    }

    /// Get max queue depth.
    pub fn max_queue_depth(&self) -> usize {
        self.max_queue_depth
    }

    /// Get max snapshot duration.
    pub fn max_snapshot_duration(&self) -> Duration {
        self.max_snapshot_duration
    }

    /// Check if table count exceeds limit.
    pub fn check_tables(&self, count: usize) -> GuardrailCheck {
        GuardrailCheck {
            limit: self.max_tables,
            current: count,
            action: self.max_tables_action,
        }
    }

    /// Check if transaction size exceeds limit.
    pub fn check_transaction_size(&self, count: usize) -> GuardrailCheck {
        GuardrailCheck {
            limit: self.max_transaction_size,
            current: count,
            action: GuardrailAction::Warn,
        }
    }

    /// Check if transaction memory exceeds limit.
    pub fn check_transaction_memory(&self, bytes: usize) -> GuardrailCheck {
        GuardrailCheck {
            limit: self.max_transaction_memory,
            current: bytes,
            action: GuardrailAction::Warn,
        }
    }

    /// Check if queue depth exceeds limit.
    pub fn check_queue_depth(&self, depth: usize) -> GuardrailCheck {
        GuardrailCheck {
            limit: self.max_queue_depth,
            current: depth,
            action: GuardrailAction::Warn,
        }
    }

    /// Check if snapshot duration exceeds limit.
    pub fn check_snapshot_duration(&self, duration: Duration) -> bool {
        duration > self.max_snapshot_duration
    }
}

/// Result of a guardrail check.
#[derive(Debug, Clone, Copy)]
pub struct GuardrailCheck {
    /// The configured limit.
    pub limit: usize,
    /// The current value.
    pub current: usize,
    /// The action to take if exceeded.
    pub action: GuardrailAction,
}

impl GuardrailCheck {
    /// Check if the limit is exceeded.
    pub fn is_exceeded(&self) -> bool {
        self.current > self.limit
    }

    /// Get the excess amount (0 if not exceeded).
    pub fn excess(&self) -> usize {
        self.current.saturating_sub(self.limit)
    }

    /// Execute the configured action.
    pub fn execute(&self) -> Result<()> {
        if !self.is_exceeded() {
            return Ok(());
        }

        match self.action {
            GuardrailAction::Warn => {
                warn!(
                    "Guardrail exceeded: {} > {} (limit)",
                    self.current, self.limit
                );
                Ok(())
            }
            GuardrailAction::Skip => {
                info!(
                    "Guardrail exceeded: {} > {} (skipping excess)",
                    self.current, self.limit
                );
                Ok(())
            }
            GuardrailAction::Fail => Err(anyhow!(
                "Guardrail exceeded: {} > {} (failing)",
                self.current,
                self.limit
            )),
        }
    }
}

/// Builder for GuardrailsConfig.
#[derive(Debug, Clone, Default)]
pub struct GuardrailsConfigBuilder {
    max_tables: Option<usize>,
    max_tables_action: Option<GuardrailAction>,
    max_transaction_size: Option<usize>,
    max_transaction_memory: Option<usize>,
    max_queue_depth: Option<usize>,
    max_snapshot_duration: Option<Duration>,
}

impl GuardrailsConfigBuilder {
    /// Set max tables limit.
    pub fn max_tables(mut self, value: usize) -> Self {
        self.max_tables = Some(value);
        self
    }

    /// Set action when max_tables is exceeded.
    pub fn max_tables_action(mut self, action: GuardrailAction) -> Self {
        self.max_tables_action = Some(action);
        self
    }

    /// Set max transaction size.
    pub fn max_transaction_size(mut self, value: usize) -> Self {
        self.max_transaction_size = Some(value);
        self
    }

    /// Set max transaction memory.
    pub fn max_transaction_memory(mut self, value: usize) -> Self {
        self.max_transaction_memory = Some(value);
        self
    }

    /// Set max queue depth.
    pub fn max_queue_depth(mut self, value: usize) -> Self {
        self.max_queue_depth = Some(value);
        self
    }

    /// Set max snapshot duration.
    pub fn max_snapshot_duration(mut self, value: Duration) -> Self {
        self.max_snapshot_duration = Some(value);
        self
    }

    /// Build the GuardrailsConfig.
    pub fn build(self) -> GuardrailsConfig {
        let defaults = GuardrailsConfig::default();
        GuardrailsConfig {
            max_tables: self.max_tables.unwrap_or(defaults.max_tables),
            max_tables_action: self.max_tables_action.unwrap_or(defaults.max_tables_action),
            max_transaction_size: self
                .max_transaction_size
                .unwrap_or(defaults.max_transaction_size),
            max_transaction_memory: self
                .max_transaction_memory
                .unwrap_or(defaults.max_transaction_memory),
            max_queue_depth: self.max_queue_depth.unwrap_or(defaults.max_queue_depth),
            max_snapshot_duration: self
                .max_snapshot_duration
                .unwrap_or(defaults.max_snapshot_duration),
        }
    }
}

// ============================================================================
// Token Bucket Rate Limiter
// ============================================================================

/// Token bucket rate limiter for CDC event processing
///
/// Prevents resource exhaustion by limiting events/sec per connection.
///
/// # Concurrency Note
///
/// The `last_refill` field uses `tokio::sync::RwLock` (not `std::sync::Mutex`),
/// and the token counter uses `AtomicU32`, so this type is safe to use in async
/// contexts without risking blocking the executor. The `RwLock` is only held
/// briefly during refill calculations with no `.await` inside the write-locked
/// critical section (the write guard is dropped before any async work).
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::common::RateLimiter;
///
/// # tokio_test::block_on(async {
/// let limiter = RateLimiter::new(100, 1000); // 100 burst, 1000/sec refill
///
/// if limiter.try_acquire().await.is_ok() {
///     // Process event
/// }
/// # });
/// ```
pub struct RateLimiter {
    /// Maximum tokens (burst capacity)
    capacity: u32,
    /// Current token count
    tokens: AtomicU32,
    /// Refill rate (tokens per second)
    refill_rate: u32,
    /// Last refill timestamp
    last_refill: RwLock<Instant>,
}

impl RateLimiter {
    /// Create a new rate limiter
    ///
    /// # Arguments
    /// * `capacity` - Maximum burst size (e.g., 1000 events)
    /// * `refill_rate` - Events per second (e.g., 10000 events/sec)
    pub fn new(capacity: u32, refill_rate: u32) -> Self {
        Self {
            capacity,
            tokens: AtomicU32::new(capacity),
            refill_rate,
            last_refill: RwLock::new(Instant::now()),
        }
    }

    /// Acquire a token (non-blocking)
    ///
    /// Returns `Ok(())` if token acquired, `Err` if rate limit exceeded.
    pub async fn try_acquire(&self) -> Result<()> {
        self.refill().await;

        loop {
            let current = self.tokens.load(Ordering::SeqCst);
            if current == 0 {
                return Err(anyhow!("Rate limit exceeded"));
            }

            if self
                .tokens
                .compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Acquire a token (blocking with timeout)
    pub async fn acquire_timeout(&self, timeout: Duration) -> Result<()> {
        let start = Instant::now();

        loop {
            if self.try_acquire().await.is_ok() {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(anyhow!("Rate limiter timeout after {:?}", timeout));
            }

            // Wait a bit before retrying
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Refill tokens based on elapsed time
    async fn refill(&self) {
        let mut last_refill = self.last_refill.write().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        // Calculate tokens to add
        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u32;

        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::SeqCst);
            let new_tokens = (current + tokens_to_add).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::SeqCst);
            *last_refill = now;
        }
    }

    /// Get current token count (for monitoring)
    pub fn available_tokens(&self) -> u32 {
        self.tokens.load(Ordering::SeqCst)
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests allowed
    Closed,
    /// Too many failures - requests rejected
    Open,
    /// Testing if service recovered
    HalfOpen,
}

/// Circuit breaker for fault tolerance
///
/// Prevents cascading failures by stopping requests to failing services.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cdc::common::CircuitBreaker;
/// use std::time::Duration;
///
/// # tokio_test::block_on(async {
/// let cb = CircuitBreaker::new(5, Duration::from_secs(30), 2);
///
/// if cb.allow_request().await {
///     // Make request
///     // On success:
///     cb.record_success().await;
///     // On failure:
///     // cb.record_failure().await;
/// }
/// # });
/// ```
pub struct CircuitBreaker {
    /// Failure threshold before opening circuit
    failure_threshold: u32,
    /// Current failure count
    failure_count: AtomicU32,
    /// Current circuit state (0=Closed, 1=Open, 2=HalfOpen)
    state: AtomicU32,
    /// Time when circuit was opened
    open_time: RwLock<Option<Instant>>,
    /// How long to wait before trying half-open
    timeout: Duration,
    /// Success count in half-open state
    half_open_success_count: AtomicU32,
    /// Required successes to close circuit
    success_threshold: u32,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    ///
    /// # Arguments
    /// * `failure_threshold` - Number of failures before opening circuit
    /// * `timeout` - How long to wait in Open state before trying HalfOpen
    /// * `success_threshold` - Number of successes in HalfOpen to close circuit
    pub fn new(failure_threshold: u32, timeout: Duration, success_threshold: u32) -> Self {
        Self {
            failure_threshold,
            failure_count: AtomicU32::new(0),
            state: AtomicU32::new(0), // Closed
            open_time: RwLock::new(None),
            timeout,
            half_open_success_count: AtomicU32::new(0),
            success_threshold,
        }
    }

    /// Check if request is allowed
    pub async fn allow_request(&self) -> bool {
        let state = self.get_state();

        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout elapsed
                let open_time = self.open_time.read().await;
                if let Some(t) = *open_time {
                    if t.elapsed() > self.timeout {
                        drop(open_time); // Release read lock
                        self.transition_to_half_open().await;
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful request
    pub async fn record_success(&self) {
        let state = self.get_state();

        match state {
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let successes = self.half_open_success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.success_threshold {
                    self.transition_to_closed().await;
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed request
    pub async fn record_failure(&self) {
        let state = self.get_state();

        match state {
            CircuitState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.failure_threshold {
                    self.transition_to_open().await;
                }
            }
            CircuitState::HalfOpen => {
                self.transition_to_open().await;
            }
            CircuitState::Open => {}
        }
    }

    /// Get current state
    pub fn get_state(&self) -> CircuitState {
        match self.state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }

    /// Get failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }

    async fn transition_to_open(&self) {
        warn!("Circuit breaker transitioning to OPEN state");
        self.state.store(1, Ordering::SeqCst);
        let mut open_time = self.open_time.write().await;
        *open_time = Some(Instant::now());
    }

    async fn transition_to_half_open(&self) {
        debug!("Circuit breaker transitioning to HALF_OPEN state");
        self.state.store(2, Ordering::SeqCst);
        self.half_open_success_count.store(0, Ordering::SeqCst);
    }

    async fn transition_to_closed(&self) {
        debug!("Circuit breaker transitioning to CLOSED state");
        self.state.store(0, Ordering::SeqCst);
        self.failure_count.store(0, Ordering::SeqCst);
        self.half_open_success_count.store(0, Ordering::SeqCst);
    }
}

/// Exponential backoff with jitter
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    attempt: u32,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempt: 0,
        }
    }

    /// Get the next backoff duration
    pub fn next_backoff(&mut self) -> Duration {
        let backoff = self.base.saturating_mul(2u32.saturating_pow(self.attempt));
        self.attempt += 1;
        backoff.min(self.max)
    }

    /// Reset the backoff
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Get current attempt number
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // RetryConfig Tests
    // ========================================================================

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries(), 10);
        assert_eq!(config.retry_delay(), Duration::from_secs(1));
        assert_eq!(config.max_delay(), Duration::from_secs(60));
        assert!((config.jitter() - 0.25).abs() < f64::EPSILON);
        assert!(config.is_enabled());
        assert!(!config.is_infinite());
    }

    #[test]
    fn test_retry_config_disabled() {
        let config = RetryConfig::disabled();
        assert_eq!(config.max_retries(), 0);
        assert!(!config.is_enabled());
        assert!(!config.is_infinite());
        assert!(!config.should_retry(0));
    }

    #[test]
    fn test_retry_config_infinite() {
        let config = RetryConfig::infinite();
        assert_eq!(config.max_retries(), -1);
        assert!(config.is_enabled());
        assert!(config.is_infinite());
        assert!(config.should_retry(1000));
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::builder()
            .max_retries(5)
            .retry_delay(Duration::from_millis(500))
            .max_delay(Duration::from_secs(30))
            .jitter(0.1)
            .build();

        assert_eq!(config.max_retries(), 5);
        assert_eq!(config.retry_delay(), Duration::from_millis(500));
        assert_eq!(config.max_delay(), Duration::from_secs(30));
        assert!((config.jitter() - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_retry_config_should_retry() {
        let config = RetryConfig::builder().max_retries(3).build();

        assert!(config.should_retry(0));
        assert!(config.should_retry(1));
        assert!(config.should_retry(2));
        assert!(!config.should_retry(3));
        assert!(!config.should_retry(4));
    }

    #[test]
    fn test_retry_config_is_retriable() {
        let config = RetryConfig::default();

        assert!(config.is_retriable(&RetriableErrorType::ConnectionLost));
        assert!(config.is_retriable(&RetriableErrorType::Timeout));
        assert!(config.is_retriable(&RetriableErrorType::DeadlockDetected));
        // Not in defaults
        assert!(!config.is_retriable(&RetriableErrorType::NetworkUnreachable));
    }

    #[test]
    fn test_retry_config_delay_for_attempt() {
        let config = RetryConfig::builder()
            .retry_delay(Duration::from_millis(100))
            .max_delay(Duration::from_secs(5))
            .jitter(0.0) // Disable jitter for predictable testing
            .build();

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));

        // Should cap at max_delay
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(5));
    }

    #[test]
    fn test_retriable_error_type_defaults() {
        let defaults = RetriableErrorType::defaults();

        assert!(defaults.contains(&RetriableErrorType::ConnectionLost));
        assert!(defaults.contains(&RetriableErrorType::Timeout));
        assert!(defaults.contains(&RetriableErrorType::DeadlockDetected));
        assert!(!defaults.contains(&RetriableErrorType::NetworkUnreachable));
    }

    // ========================================================================
    // GuardrailsConfig Tests
    // ========================================================================

    #[test]
    fn test_guardrails_config_default() {
        let config = GuardrailsConfig::default();

        assert_eq!(config.max_tables(), 10_000);
        assert_eq!(config.max_tables_action(), GuardrailAction::Warn);
        assert_eq!(config.max_transaction_size(), 100_000);
        assert_eq!(config.max_transaction_memory(), 256 * 1024 * 1024);
        assert_eq!(config.max_queue_depth(), 10_000);
        assert_eq!(
            config.max_snapshot_duration(),
            Duration::from_secs(24 * 60 * 60)
        );
    }

    #[test]
    fn test_guardrails_config_permissive() {
        let config = GuardrailsConfig::permissive();

        assert_eq!(config.max_tables(), usize::MAX);
        assert!(!config.check_tables(1_000_000).is_exceeded());
    }

    #[test]
    fn test_guardrails_config_strict() {
        let config = GuardrailsConfig::strict();

        assert_eq!(config.max_tables(), 100);
        assert_eq!(config.max_tables_action(), GuardrailAction::Fail);
        assert!(config.check_tables(150).is_exceeded());
    }

    #[test]
    fn test_guardrails_config_builder() {
        let config = GuardrailsConfig::builder()
            .max_tables(500)
            .max_tables_action(GuardrailAction::Skip)
            .max_transaction_size(50_000)
            .build();

        assert_eq!(config.max_tables(), 500);
        assert_eq!(config.max_tables_action(), GuardrailAction::Skip);
        assert_eq!(config.max_transaction_size(), 50_000);
    }

    #[test]
    fn test_guardrail_check_not_exceeded() {
        let config = GuardrailsConfig::builder().max_tables(100).build();

        let check = config.check_tables(50);
        assert!(!check.is_exceeded());
        assert_eq!(check.excess(), 0);
        assert!(check.execute().is_ok());
    }

    #[test]
    fn test_guardrail_check_exceeded_warn() {
        let config = GuardrailsConfig::builder()
            .max_tables(100)
            .max_tables_action(GuardrailAction::Warn)
            .build();

        let check = config.check_tables(150);
        assert!(check.is_exceeded());
        assert_eq!(check.excess(), 50);
        assert!(check.execute().is_ok()); // Warn doesn't fail
    }

    #[test]
    fn test_guardrail_check_exceeded_fail() {
        let config = GuardrailsConfig::builder()
            .max_tables(100)
            .max_tables_action(GuardrailAction::Fail)
            .build();

        let check = config.check_tables(150);
        assert!(check.is_exceeded());
        assert!(check.execute().is_err()); // Fail returns error
    }

    #[test]
    fn test_guardrails_check_transaction_size() {
        let config = GuardrailsConfig::builder()
            .max_transaction_size(1000)
            .build();

        assert!(!config.check_transaction_size(500).is_exceeded());
        assert!(config.check_transaction_size(1500).is_exceeded());
    }

    #[test]
    fn test_guardrails_check_queue_depth() {
        let config = GuardrailsConfig::builder().max_queue_depth(100).build();

        assert!(!config.check_queue_depth(50).is_exceeded());
        assert!(config.check_queue_depth(150).is_exceeded());
    }

    #[test]
    fn test_guardrails_check_snapshot_duration() {
        let config = GuardrailsConfig::builder()
            .max_snapshot_duration(Duration::from_secs(60))
            .build();

        assert!(!config.check_snapshot_duration(Duration::from_secs(30)));
        assert!(config.check_snapshot_duration(Duration::from_secs(90)));
    }

    // ========================================================================
    // Rate Limiter Tests
    // ========================================================================

    #[tokio::test]
    async fn test_rate_limiter_burst() {
        let limiter = RateLimiter::new(10, 100);

        // Should allow 10 immediate requests
        for _ in 0..10 {
            assert!(limiter.try_acquire().await.is_ok());
        }

        // 11th should fail
        assert!(limiter.try_acquire().await.is_err());
    }

    #[tokio::test]
    async fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(10, 10); // 10 tokens/sec

        // Exhaust tokens
        for _ in 0..10 {
            limiter.try_acquire().await.ok();
        }

        // Wait for refill
        sleep(Duration::from_millis(1100)).await;

        // Should have ~10 tokens available
        assert!(limiter.try_acquire().await.is_ok());
    }

    // ========================================================================
    // Circuit Breaker Tests
    // ========================================================================

    #[tokio::test]
    async fn test_circuit_breaker_open() {
        let cb = CircuitBreaker::new(3, Duration::from_secs(1), 2);

        // Initial state: closed
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.allow_request().await);

        // Record failures
        for _ in 0..3 {
            cb.record_failure().await;
        }

        // Should be open now
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(!cb.allow_request().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(100), 2);

        // Open the circuit
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Should allow request (half-open)
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Record successes
        cb.record_success().await;
        cb.record_success().await;

        // Should be closed now
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    // ========================================================================
    // Exponential Backoff Tests
    // ========================================================================

    #[test]
    fn test_exponential_backoff() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10));

        assert_eq!(backoff.next_backoff(), Duration::from_millis(100));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(200));
        assert_eq!(backoff.next_backoff(), Duration::from_millis(400));

        backoff.reset();
        assert_eq!(backoff.next_backoff(), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff_max() {
        let mut backoff = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(5));

        for _ in 0..10 {
            let d = backoff.next_backoff();
            assert!(d <= Duration::from_secs(5));
        }
    }
}
