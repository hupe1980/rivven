//! Resilience primitives for CDC operations
//!
//! Production-grade fault tolerance:
//! - Token bucket rate limiter
//! - Circuit breaker pattern
//! - Backoff strategies

use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Token bucket rate limiter for CDC event processing
///
/// Prevents resource exhaustion by limiting events/sec per connection.
///
/// # Example
///
/// ```rust
/// use rivven_cdc::RateLimiter;
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
/// ```rust
/// use rivven_cdc::CircuitBreaker;
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
