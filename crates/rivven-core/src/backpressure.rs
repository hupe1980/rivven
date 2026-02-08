//! Backpressure and Flow Control
//!
//! Production-grade flow control mechanisms for high-throughput streaming:
//! - **Token Bucket**: Rate limiting with burst capacity
//! - **Leaky Bucket**: Smooth rate limiting
//! - **Credit-Based Flow Control**: Receiver-driven backpressure
//! - **Adaptive Rate Limiter**: Self-tuning based on latency
//! - **Circuit Breaker**: Fail-fast on downstream issues
//! - **Windowed Rate Tracker**: Sliding window request tracking
//!
//! Based on industry best practices from:
//! - TCP congestion control (AIMD, BBR concepts)
//! - Reactive Streams backpressure
//! - Netflix Hystrix circuit breaker patterns

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Notify, Semaphore};

/// Token bucket rate limiter
/// Allows bursts up to bucket capacity, then limits to refill rate
#[derive(Debug)]
pub struct TokenBucket {
    /// Current number of tokens
    tokens: AtomicU64,
    /// Maximum tokens (burst capacity)
    capacity: u64,
    /// Tokens added per second (stored as f64 bits for atomic updates)
    refill_rate_bits: AtomicU64,
    /// Creation time for time reference
    created: Instant,
    /// Last refill timestamp (nanoseconds since creation)
    last_refill: AtomicU64,
    /// Statistics
    stats: TokenBucketStats,
}

impl TokenBucket {
    /// Create a new token bucket
    pub fn new(capacity: u64, refill_rate: f64) -> Self {
        let now = Instant::now();
        Self {
            tokens: AtomicU64::new(capacity),
            capacity,
            refill_rate_bits: AtomicU64::new(refill_rate.to_bits()),
            created: now,
            last_refill: AtomicU64::new(0),
            stats: TokenBucketStats::new(),
        }
    }

    /// Try to acquire tokens without blocking
    pub fn try_acquire(&self, count: u64) -> bool {
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < count {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            if self
                .tokens
                .compare_exchange_weak(
                    current,
                    current - count,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.stats.acquired.fetch_add(count, Ordering::Relaxed);
                return true;
            }
            // Hint to the CPU that we're in a spin loop
            std::hint::spin_loop();
        }
    }

    /// Acquire tokens, blocking if necessary
    pub async fn acquire(&self, count: u64) {
        while !self.try_acquire(count) {
            // Calculate wait time
            let tokens_needed = count.saturating_sub(self.tokens.load(Ordering::Relaxed));
            let rate = self.refill_rate();
            let wait_secs = tokens_needed as f64 / rate;
            let wait_duration = Duration::from_secs_f64(wait_secs.max(0.001));

            tokio::time::sleep(wait_duration).await;
        }
    }

    /// Get the current refill rate (tokens per second)
    fn refill_rate(&self) -> f64 {
        f64::from_bits(self.refill_rate_bits.load(Ordering::Relaxed))
    }

    /// Update the refill rate atomically (for adaptive rate limiting)
    pub fn update_rate(&self, new_rate: f64) {
        self.refill_rate_bits
            .store(new_rate.to_bits(), Ordering::Relaxed);
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let now = self.created.elapsed().as_nanos() as u64;
        let last = self.last_refill.load(Ordering::Acquire);

        if now <= last {
            return;
        }

        let rate = self.refill_rate();
        let elapsed_secs = (now - last) as f64 / 1_000_000_000.0;
        let new_tokens = (elapsed_secs * rate) as u64;

        if new_tokens == 0 {
            return;
        }

        if self
            .last_refill
            .compare_exchange(last, now, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            loop {
                let current = self.tokens.load(Ordering::Acquire);
                let new_total = (current + new_tokens).min(self.capacity);

                if self
                    .tokens
                    .compare_exchange_weak(current, new_total, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    /// Get current token count
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> TokenBucketStatsSnapshot {
        TokenBucketStatsSnapshot {
            acquired: self.stats.acquired.load(Ordering::Relaxed),
            rejected: self.stats.rejected.load(Ordering::Relaxed),
            available: self.available(),
            capacity: self.capacity,
        }
    }
}

#[derive(Debug)]
struct TokenBucketStats {
    acquired: AtomicU64,
    rejected: AtomicU64,
}

impl TokenBucketStats {
    fn new() -> Self {
        Self {
            acquired: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TokenBucketStatsSnapshot {
    pub acquired: u64,
    pub rejected: u64,
    pub available: u64,
    pub capacity: u64,
}

/// Credit-based flow control for receiver-driven backpressure
/// Receivers grant credits to senders, senders can only send up to credit limit
#[derive(Debug)]
pub struct CreditFlowControl {
    /// Available credits
    credits: AtomicU64,
    /// Maximum credits
    max_credits: u64,
    /// Notify when credits become available
    credit_notify: Notify,
    /// Statistics
    stats: CreditStats,
}

impl CreditFlowControl {
    /// Create new credit flow control
    pub fn new(initial_credits: u64, max_credits: u64) -> Self {
        Self {
            credits: AtomicU64::new(initial_credits),
            max_credits,
            credit_notify: Notify::new(),
            stats: CreditStats::new(),
        }
    }

    /// Try to consume credits without blocking
    pub fn try_consume(&self, count: u64) -> bool {
        loop {
            let current = self.credits.load(Ordering::Acquire);
            if current < count {
                self.stats.blocked.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            if self
                .credits
                .compare_exchange_weak(
                    current,
                    current - count,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.stats.consumed.fetch_add(count, Ordering::Relaxed);
                return true;
            }
        }
    }

    /// Consume credits, blocking if necessary
    pub async fn consume(&self, count: u64) {
        while !self.try_consume(count) {
            self.credit_notify.notified().await;
        }
    }

    /// Grant credits (called by receiver)
    pub fn grant(&self, count: u64) {
        loop {
            let current = self.credits.load(Ordering::Acquire);
            let new_total = (current + count).min(self.max_credits);

            if self
                .credits
                .compare_exchange_weak(current, new_total, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.stats.granted.fetch_add(count, Ordering::Relaxed);
                self.credit_notify.notify_waiters();
                break;
            }
        }
    }

    /// Get available credits
    pub fn available(&self) -> u64 {
        self.credits.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> CreditStatsSnapshot {
        CreditStatsSnapshot {
            consumed: self.stats.consumed.load(Ordering::Relaxed),
            granted: self.stats.granted.load(Ordering::Relaxed),
            blocked: self.stats.blocked.load(Ordering::Relaxed),
            available: self.available(),
        }
    }
}

#[derive(Debug)]
struct CreditStats {
    consumed: AtomicU64,
    granted: AtomicU64,
    blocked: AtomicU64,
}

impl CreditStats {
    fn new() -> Self {
        Self {
            consumed: AtomicU64::new(0),
            granted: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreditStatsSnapshot {
    pub consumed: u64,
    pub granted: u64,
    pub blocked: u64,
    pub available: u64,
}

/// Adaptive rate limiter that adjusts based on latency feedback
/// Uses AIMD (Additive Increase Multiplicative Decrease) algorithm
#[derive(Debug)]
pub struct AdaptiveRateLimiter {
    /// Current rate limit (requests per second)
    rate: AtomicU64,
    /// Minimum rate
    min_rate: u64,
    /// Maximum rate
    max_rate: u64,
    /// Target latency (microseconds)
    target_latency_us: u64,
    /// Additive increase factor
    additive_increase: u64,
    /// Multiplicative decrease factor (0.5 = halve on overload)
    multiplicative_decrease: f64,
    /// Inner token bucket for actual rate limiting
    bucket: TokenBucket,
    /// Latency samples for adaptation
    latency_samples: RwLock<LatencySamples>,
    /// Statistics
    stats: AdaptiveStats,
}

#[derive(Debug)]
struct LatencySamples {
    samples: Vec<u64>,
    index: usize,
    filled: bool,
}

impl LatencySamples {
    fn new(size: usize) -> Self {
        Self {
            samples: vec![0; size],
            index: 0,
            filled: false,
        }
    }

    fn add(&mut self, latency_us: u64) {
        self.samples[self.index] = latency_us;
        self.index = (self.index + 1) % self.samples.len();
        if self.index == 0 {
            self.filled = true;
        }
    }

    fn percentile(&self, p: f64) -> u64 {
        let count = if self.filled {
            self.samples.len()
        } else {
            self.index
        };
        if count == 0 {
            return 0;
        }

        let mut sorted: Vec<u64> = self.samples[..count].to_vec();
        sorted.sort_unstable();

        let idx = ((count as f64 * p) as usize).min(count - 1);
        sorted[idx]
    }
}

impl AdaptiveRateLimiter {
    /// Create new adaptive rate limiter
    pub fn new(config: AdaptiveRateLimiterConfig) -> Self {
        let bucket = TokenBucket::new(config.initial_rate, config.initial_rate as f64);

        Self {
            rate: AtomicU64::new(config.initial_rate),
            min_rate: config.min_rate,
            max_rate: config.max_rate,
            target_latency_us: config.target_latency_us,
            additive_increase: config.additive_increase,
            multiplicative_decrease: config.multiplicative_decrease,
            bucket,
            latency_samples: RwLock::new(LatencySamples::new(100)),
            stats: AdaptiveStats::new(),
        }
    }

    /// Try to acquire permission
    pub fn try_acquire(&self) -> bool {
        self.bucket.try_acquire(1)
    }

    /// Acquire permission, blocking if necessary
    pub async fn acquire(&self) {
        self.bucket.acquire(1).await
    }

    /// Record latency feedback for adaptation
    pub fn record_latency(&self, latency: Duration) {
        let latency_us = latency.as_micros() as u64;

        {
            let mut samples = self.latency_samples.write();
            samples.add(latency_us);
        }

        // Adapt rate based on p99 latency
        let p99 = {
            let samples = self.latency_samples.read();
            samples.percentile(0.99)
        };

        let current_rate = self.rate.load(Ordering::Relaxed);

        if p99 > self.target_latency_us {
            // Decrease rate (multiplicative)
            let new_rate =
                ((current_rate as f64 * self.multiplicative_decrease) as u64).max(self.min_rate);
            self.rate.store(new_rate, Ordering::Relaxed);
            self.bucket.update_rate(new_rate as f64);
            self.stats.decreases.fetch_add(1, Ordering::Relaxed);
        } else if p99 < self.target_latency_us / 2 {
            // Increase rate (additive)
            let new_rate = (current_rate + self.additive_increase).min(self.max_rate);
            self.rate.store(new_rate, Ordering::Relaxed);
            self.bucket.update_rate(new_rate as f64);
            self.stats.increases.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get current rate limit
    pub fn current_rate(&self) -> u64 {
        self.rate.load(Ordering::Relaxed)
    }

    /// Get statistics
    pub fn stats(&self) -> AdaptiveStatsSnapshot {
        let samples = self.latency_samples.read();
        AdaptiveStatsSnapshot {
            current_rate: self.current_rate(),
            increases: self.stats.increases.load(Ordering::Relaxed),
            decreases: self.stats.decreases.load(Ordering::Relaxed),
            p50_latency_us: samples.percentile(0.5),
            p99_latency_us: samples.percentile(0.99),
        }
    }
}

/// Configuration for adaptive rate limiter
#[derive(Debug, Clone)]
pub struct AdaptiveRateLimiterConfig {
    pub initial_rate: u64,
    pub min_rate: u64,
    pub max_rate: u64,
    pub target_latency_us: u64,
    pub additive_increase: u64,
    pub multiplicative_decrease: f64,
}

impl Default for AdaptiveRateLimiterConfig {
    fn default() -> Self {
        Self {
            initial_rate: 10000,
            min_rate: 100,
            max_rate: 1000000,
            target_latency_us: 10000, // 10ms
            additive_increase: 100,
            multiplicative_decrease: 0.5,
        }
    }
}

#[derive(Debug)]
struct AdaptiveStats {
    increases: AtomicU64,
    decreases: AtomicU64,
}

impl AdaptiveStats {
    fn new() -> Self {
        Self {
            increases: AtomicU64::new(0),
            decreases: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveStatsSnapshot {
    pub current_rate: u64,
    pub increases: u64,
    pub decreases: u64,
    pub p50_latency_us: u64,
    pub p99_latency_us: u64,
}

/// Circuit breaker for fail-fast on downstream issues
#[derive(Debug)]
pub struct CircuitBreaker {
    /// Current state
    state: RwLock<CircuitState>,
    /// Configuration
    config: CircuitBreakerConfig,
    /// Statistics
    stats: CircuitBreakerStats,
    /// Start of current failure window
    window_start: parking_lot::Mutex<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation
    Closed,
    /// Failing, rejecting all requests
    Open { opened_at: Instant },
    /// Testing if downstream recovered
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,
    /// Time window for counting failures
    pub failure_window: Duration,
    /// Time to wait before trying half-open
    pub recovery_timeout: Duration,
    /// Number of successes in half-open to close
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            failure_window: Duration::from_secs(60),
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 3,
        }
    }
}

impl CircuitBreaker {
    /// Create new circuit breaker
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: RwLock::new(CircuitState::Closed),
            config,
            stats: CircuitBreakerStats::new(),
            window_start: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Check if request is allowed
    pub fn allow(&self) -> bool {
        let state = *self.state.read();

        match state {
            CircuitState::Closed => {
                self.stats.allowed.fetch_add(1, Ordering::Relaxed);
                true
            }
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() > self.config.recovery_timeout {
                    // Transition to half-open
                    *self.state.write() = CircuitState::HalfOpen;
                    self.stats.allowed.fetch_add(1, Ordering::Relaxed);
                    true
                } else {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in half-open
                self.stats.allowed.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let mut state = self.state.write();
        self.stats.successes.fetch_add(1, Ordering::Relaxed);

        match *state {
            CircuitState::HalfOpen => {
                let successes = self
                    .stats
                    .half_open_successes
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                if successes >= self.config.success_threshold as u64 {
                    *state = CircuitState::Closed;
                    self.stats.half_open_successes.store(0, Ordering::Relaxed);
                    self.stats.failures_in_window.store(0, Ordering::Relaxed);
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.stats.failures_in_window.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let mut state = self.state.write();
        self.stats.failures.fetch_add(1, Ordering::Relaxed);

        match *state {
            CircuitState::Closed => {
                // Check if failure window has expired; if so, reset counter
                let mut ws = self.window_start.lock();
                if ws.elapsed() > self.config.failure_window {
                    self.stats.failures_in_window.store(1, Ordering::Relaxed);
                    *ws = Instant::now();
                } else {
                    let failures = self
                        .stats
                        .failures_in_window
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    if failures >= self.config.failure_threshold as u64 {
                        *state = CircuitState::Open {
                            opened_at: Instant::now(),
                        };
                        self.stats.opens.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open reopens circuit
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
                self.stats.half_open_successes.store(0, Ordering::Relaxed);
                self.stats.opens.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Get current state
    pub fn state(&self) -> CircuitState {
        *self.state.read()
    }

    /// Get statistics
    pub fn stats(&self) -> CircuitBreakerStatsSnapshot {
        CircuitBreakerStatsSnapshot {
            state: self.state(),
            allowed: self.stats.allowed.load(Ordering::Relaxed),
            rejected: self.stats.rejected.load(Ordering::Relaxed),
            successes: self.stats.successes.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            opens: self.stats.opens.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct CircuitBreakerStats {
    allowed: AtomicU64,
    rejected: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    failures_in_window: AtomicU64,
    half_open_successes: AtomicU64,
    opens: AtomicU64,
}

impl CircuitBreakerStats {
    fn new() -> Self {
        Self {
            allowed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            failures_in_window: AtomicU64::new(0),
            half_open_successes: AtomicU64::new(0),
            opens: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerStatsSnapshot {
    pub state: CircuitState,
    pub allowed: u64,
    pub rejected: u64,
    pub successes: u64,
    pub failures: u64,
    pub opens: u64,
}

/// Bounded channel with backpressure
/// Provides async send that blocks when buffer is full
pub struct BackpressureChannel<T> {
    /// Inner sender
    tx: mpsc::Sender<T>,
    /// Inner receiver
    rx: Mutex<mpsc::Receiver<T>>,
    /// Semaphore for backpressure
    permits: Arc<Semaphore>,
    /// Capacity
    capacity: usize,
    /// Statistics
    stats: ChannelStats,
}

impl<T> BackpressureChannel<T> {
    /// Create a new backpressure channel
    pub fn new(capacity: usize) -> Self {
        // Use semaphore to track capacity, but start at 0 (no items in channel)
        let (tx, rx) = mpsc::channel(capacity);

        Self {
            tx,
            rx: Mutex::new(rx),
            permits: Arc::new(Semaphore::new(0)), // Start at 0, add permits when items are sent
            capacity,
            stats: ChannelStats::new(),
        }
    }

    /// Send a value, blocking if buffer is full
    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        self.stats.sent.fetch_add(1, Ordering::Relaxed);
        let result = self.tx.send(value).await;
        if result.is_ok() {
            // Add permit to signal item is in channel
            self.permits.add_permits(1);
        }
        result
    }

    /// Try to send without blocking
    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        let result = self.tx.try_send(value);
        match &result {
            Ok(()) => {
                self.stats.sent.fetch_add(1, Ordering::Relaxed);
                self.permits.add_permits(1);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.stats.blocked.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }

    /// Receive a value
    pub async fn recv(&self) -> Option<T> {
        // Acquire permit (blocks until item available)
        let permit = self.permits.acquire().await.ok()?;
        permit.forget(); // Don't release, we're consuming the item

        let mut rx = self.rx.lock().await;
        let result = rx.recv().await;

        if result.is_some() {
            self.stats.received.fetch_add(1, Ordering::Relaxed);
        }

        result
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.permits.available_permits()
    }

    /// Check if channel is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get statistics
    pub fn stats(&self) -> ChannelStatsSnapshot {
        ChannelStatsSnapshot {
            sent: self.stats.sent.load(Ordering::Relaxed),
            received: self.stats.received.load(Ordering::Relaxed),
            blocked: self.stats.blocked.load(Ordering::Relaxed),
            current_len: self.len(),
            capacity: self.capacity,
        }
    }
}

struct ChannelStats {
    sent: AtomicU64,
    received: AtomicU64,
    blocked: AtomicU64,
}

impl ChannelStats {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChannelStatsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub blocked: u64,
    pub current_len: usize,
    pub capacity: usize,
}

/// Windowed rate tracker for monitoring request rates
pub struct WindowedRateTracker {
    /// Window buckets
    buckets: RwLock<Vec<AtomicU64>>,
    /// Bucket duration
    bucket_duration: Duration,
    /// Number of buckets
    num_buckets: usize,
    /// Current bucket index
    current_bucket: AtomicUsize,
    /// Last bucket rotation time
    last_rotation: RwLock<Instant>,
}

impl WindowedRateTracker {
    /// Create a new windowed rate tracker
    pub fn new(window_duration: Duration, num_buckets: usize) -> Self {
        let buckets: Vec<AtomicU64> = (0..num_buckets).map(|_| AtomicU64::new(0)).collect();

        Self {
            buckets: RwLock::new(buckets),
            bucket_duration: window_duration / num_buckets as u32,
            num_buckets,
            current_bucket: AtomicUsize::new(0),
            last_rotation: RwLock::new(Instant::now()),
        }
    }

    /// Record a request
    pub fn record(&self, count: u64) {
        self.maybe_rotate();

        let buckets = self.buckets.read();
        let idx = self.current_bucket.load(Ordering::Relaxed);
        buckets[idx].fetch_add(count, Ordering::Relaxed);
    }

    /// Get rate over the window
    pub fn rate(&self) -> f64 {
        self.maybe_rotate();

        let buckets = self.buckets.read();
        let total: u64 = buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum();

        let window_secs = self.bucket_duration.as_secs_f64() * self.num_buckets as f64;
        total as f64 / window_secs
    }

    /// Get total count in window
    pub fn total(&self) -> u64 {
        self.maybe_rotate();

        let buckets = self.buckets.read();
        buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    /// Maybe rotate to next bucket
    fn maybe_rotate(&self) {
        let now = Instant::now();
        let elapsed = {
            let last = self.last_rotation.read();
            now.duration_since(*last)
        };

        if elapsed < self.bucket_duration {
            return;
        }

        let buckets_to_rotate =
            (elapsed.as_secs_f64() / self.bucket_duration.as_secs_f64()) as usize;
        if buckets_to_rotate == 0 {
            return;
        }

        // Update last rotation
        {
            let mut last = self.last_rotation.write();
            *last = now;
        }

        let buckets = self.buckets.read();

        // Rotate buckets
        for _ in 0..buckets_to_rotate.min(self.num_buckets) {
            let next = (self.current_bucket.load(Ordering::Relaxed) + 1) % self.num_buckets;
            buckets[next].store(0, Ordering::Relaxed);
            self.current_bucket.store(next, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 10.0);

        // Should be able to acquire up to capacity
        assert!(bucket.try_acquire(5));
        assert!(bucket.try_acquire(5));

        // Should fail when empty
        assert!(!bucket.try_acquire(1));

        let stats = bucket.stats();
        assert_eq!(stats.acquired, 10);
        assert_eq!(stats.rejected, 1);
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        // Use larger capacity so refill doesn't cap out
        let bucket = TokenBucket::new(1000, 100.0); // 1000 capacity, 100 tokens/sec

        // Drain bucket
        bucket.try_acquire(1000);
        assert!(!bucket.try_acquire(1));

        // Wait for refill (50ms should give us ~5 tokens)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should have some tokens now (at least 4 with 100/sec rate over 50ms)
        let available = bucket.available();
        assert!(
            available >= 4,
            "Expected at least 4 tokens, got {}",
            available
        );
    }

    #[test]
    fn test_credit_flow_control() {
        let flow = CreditFlowControl::new(10, 100);

        // Consume credits
        assert!(flow.try_consume(5));
        assert!(flow.try_consume(5));
        assert!(!flow.try_consume(1));

        assert_eq!(flow.available(), 0);

        // Grant credits
        flow.grant(5);
        assert_eq!(flow.available(), 5);
        assert!(flow.try_consume(5));
    }

    #[test]
    fn test_adaptive_rate_limiter() {
        let config = AdaptiveRateLimiterConfig {
            initial_rate: 1000,
            min_rate: 100,
            max_rate: 10000,
            target_latency_us: 10000,
            additive_increase: 100,
            multiplicative_decrease: 0.5,
        };

        let limiter = AdaptiveRateLimiter::new(config);

        // Initial rate
        assert_eq!(limiter.current_rate(), 1000);

        // Record high latency -> rate should decrease
        for _ in 0..100 {
            limiter.record_latency(Duration::from_millis(20));
        }
        assert!(limiter.current_rate() < 1000);
    }

    #[test]
    fn test_circuit_breaker_closed() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        // Normal operation
        assert!(breaker.allow());
        breaker.record_success();

        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        // Trigger failures
        for _ in 0..3 {
            assert!(breaker.allow());
            breaker.record_failure();
        }

        // Circuit should be open
        match breaker.state() {
            CircuitState::Open { .. } => {}
            _ => panic!("Expected open state"),
        }

        // New requests should be rejected
        assert!(!breaker.allow());
    }

    #[tokio::test]
    async fn test_backpressure_channel() {
        let channel = BackpressureChannel::new(3);

        // Fill channel
        channel.send(1).await.unwrap();
        channel.send(2).await.unwrap();
        channel.send(3).await.unwrap();

        assert_eq!(channel.len(), 3);

        // Try send should fail
        assert!(channel.try_send(4).is_err());

        // Receive should work
        assert_eq!(channel.recv().await, Some(1));
        assert_eq!(channel.len(), 2);

        // Now we can send again
        channel.send(4).await.unwrap();
    }

    #[test]
    fn test_windowed_rate_tracker() {
        let tracker = WindowedRateTracker::new(Duration::from_secs(1), 10);

        // Record some requests
        tracker.record(100);
        tracker.record(100);

        assert_eq!(tracker.total(), 200);
        assert!(tracker.rate() > 0.0);
    }
}
