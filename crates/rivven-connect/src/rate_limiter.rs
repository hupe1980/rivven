//! Rate limiting for connectors
//!
//! Implements configurable throughput limiting for sinks to prevent
//! overwhelming downstream systems. Uses the token bucket algorithm
//! for smooth rate limiting with burst tolerance.
//!
//! Features:
//! - Configurable events per second limit
//! - Burst capacity for handling traffic spikes
//! - Async-friendly non-blocking design
//! - Metrics integration for observability

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// Configuration for sink rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum events per second (0 = unlimited)
    pub events_per_second: u64,
    /// Burst capacity (extra events allowed above steady rate)
    /// Default: 10% of events_per_second or minimum 10
    pub burst_capacity: u64,
}

impl RateLimitConfig {
    /// Create a new rate limit config with default burst capacity
    pub fn new(events_per_second: u64) -> Self {
        let burst = if events_per_second == 0 {
            0
        } else {
            (events_per_second / 10).max(10)
        };
        Self {
            events_per_second,
            burst_capacity: burst,
        }
    }

    /// Create an unlimited rate limiter (no throttling)
    pub fn unlimited() -> Self {
        Self {
            events_per_second: 0,
            burst_capacity: 0,
        }
    }

    /// Create a rate limit config with custom burst capacity
    pub fn with_burst(events_per_second: u64, burst_capacity: u64) -> Self {
        Self {
            events_per_second,
            burst_capacity,
        }
    }

    /// Check if rate limiting is enabled
    pub fn is_enabled(&self) -> bool {
        self.events_per_second > 0
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Token bucket rate limiter for controlling event throughput
///
/// The token bucket algorithm allows for burst traffic while maintaining
/// a long-term average rate. Tokens are added at a constant rate and
/// consumed when events are processed.
pub struct TokenBucketRateLimiter {
    /// Current number of available tokens
    tokens: AtomicU64,
    /// Maximum token capacity (events_per_second + burst_capacity)
    capacity: u64,
    /// Token refill rate (tokens per second)
    refill_rate: u64,
    /// Last time tokens were refilled
    last_refill: RwLock<Instant>,
    /// Configuration
    config: RateLimitConfig,
    /// Total events rate limited
    events_throttled: AtomicU64,
    /// Total time spent waiting (nanoseconds)
    total_wait_ns: AtomicU64,
}

impl TokenBucketRateLimiter {
    /// Create a new token bucket rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        let capacity = if config.is_enabled() {
            config.events_per_second + config.burst_capacity
        } else {
            u64::MAX // Effectively unlimited
        };

        Self {
            tokens: AtomicU64::new(capacity),
            capacity,
            refill_rate: config.events_per_second,
            last_refill: RwLock::new(Instant::now()),
            config,
            events_throttled: AtomicU64::new(0),
            total_wait_ns: AtomicU64::new(0),
        }
    }

    /// Acquire tokens for the given number of events
    ///
    /// Returns immediately if tokens are available, otherwise waits
    /// until enough tokens are available.
    ///
    /// # Arguments
    /// * `count` - Number of events to process
    ///
    /// # Returns
    /// The duration waited (zero if no wait was needed)
    pub async fn acquire(&self, count: u64) -> Duration {
        // Skip rate limiting if disabled
        if !self.config.is_enabled() {
            return Duration::ZERO;
        }

        let start = Instant::now();
        let mut total_wait = Duration::ZERO;

        loop {
            // Refill tokens based on elapsed time
            self.refill().await;

            // Try to acquire tokens
            let current = self.tokens.load(Ordering::Acquire);
            if current >= count {
                // Attempt atomic compare-and-swap
                if self
                    .tokens
                    .compare_exchange(
                        current,
                        current - count,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // Success - we got our tokens
                    if !total_wait.is_zero() {
                        self.events_throttled.fetch_add(count, Ordering::Relaxed);
                        self.total_wait_ns
                            .fetch_add(total_wait.as_nanos() as u64, Ordering::Relaxed);
                        debug!(
                            "Rate limiter: waited {:?} to acquire {} tokens",
                            total_wait, count
                        );
                    }
                    return total_wait;
                }
                // CAS failed, retry
                continue;
            }

            // Not enough tokens - calculate wait time
            let tokens_needed = count.saturating_sub(current);
            let wait_duration = if self.refill_rate > 0 {
                // Time to generate needed tokens = tokens / rate
                let wait_secs = tokens_needed as f64 / self.refill_rate as f64;
                Duration::from_secs_f64(wait_secs.min(1.0)) // Cap at 1 second max wait
            } else {
                Duration::from_millis(10)
            };

            // Wait for tokens to become available
            tokio::time::sleep(wait_duration).await;
            total_wait = start.elapsed();
        }
    }

    /// Try to acquire tokens without waiting
    ///
    /// Returns true if tokens were acquired, false if rate limit would be exceeded.
    #[allow(dead_code)]
    pub async fn try_acquire(&self, count: u64) -> bool {
        // Skip rate limiting if disabled
        if !self.config.is_enabled() {
            return true;
        }

        // Refill tokens based on elapsed time
        self.refill().await;

        // Try to acquire tokens
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current < count {
                return false;
            }

            if self
                .tokens
                .compare_exchange(
                    current,
                    current - count,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
            // CAS failed, retry
        }
    }

    /// Refill tokens based on elapsed time
    async fn refill(&self) {
        if self.refill_rate == 0 {
            return;
        }

        let mut last = self.last_refill.write().await;
        let elapsed = last.elapsed();

        // Only refill if at least 1ms has passed
        if elapsed.as_millis() < 1 {
            return;
        }

        // Calculate tokens to add
        let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;

        if tokens_to_add > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = current.saturating_add(tokens_to_add).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::Release);
            *last = Instant::now();
        }
    }

    /// Get current available tokens
    #[allow(dead_code)]
    pub fn available_tokens(&self) -> u64 {
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get statistics about rate limiting
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            events_throttled: self.events_throttled.load(Ordering::Relaxed),
            total_wait_ms: self.total_wait_ns.load(Ordering::Relaxed) / 1_000_000,
            current_tokens: self.tokens.load(Ordering::Relaxed),
            capacity: self.capacity,
            rate_limit: self.config.events_per_second,
            enabled: self.config.is_enabled(),
        }
    }

    /// Reset the rate limiter (useful for testing)
    #[allow(dead_code)]
    pub async fn reset(&self) {
        self.tokens.store(self.capacity, Ordering::Release);
        *self.last_refill.write().await = Instant::now();
        self.events_throttled.store(0, Ordering::Relaxed);
        self.total_wait_ns.store(0, Ordering::Relaxed);
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    /// Number of events that were throttled (had to wait)
    pub events_throttled: u64,
    /// Total time spent waiting in milliseconds
    pub total_wait_ms: u64,
    /// Current available tokens
    pub current_tokens: u64,
    /// Maximum token capacity
    pub capacity: u64,
    /// Configured rate limit (events per second)
    pub rate_limit: u64,
    /// Whether rate limiting is enabled
    pub enabled: bool,
}

impl std::fmt::Display for RateLimiterStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.enabled {
            write!(
                f,
                "RateLimiter({}eps, {} throttled, {}ms waited, {}/{} tokens)",
                self.rate_limit,
                self.events_throttled,
                self.total_wait_ms,
                self.current_tokens,
                self.capacity
            )
        } else {
            write!(f, "RateLimiter(unlimited)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_unlimited_rate_limiter() {
        let config = RateLimitConfig::unlimited();
        let limiter = TokenBucketRateLimiter::new(config);

        // Should never block
        for _ in 0..1000 {
            let wait = limiter.acquire(1).await;
            assert_eq!(wait, Duration::ZERO);
        }

        let stats = limiter.stats();
        assert!(!stats.enabled);
        assert_eq!(stats.events_throttled, 0);
    }

    #[tokio::test]
    async fn test_rate_limiter_config() {
        let config = RateLimitConfig::new(1000);
        assert_eq!(config.events_per_second, 1000);
        assert_eq!(config.burst_capacity, 100); // 10% of 1000

        let config = RateLimitConfig::new(50);
        assert_eq!(config.events_per_second, 50);
        assert_eq!(config.burst_capacity, 10); // minimum 10

        let config = RateLimitConfig::with_burst(1000, 500);
        assert_eq!(config.events_per_second, 1000);
        assert_eq!(config.burst_capacity, 500);
    }

    #[tokio::test]
    async fn test_try_acquire_success() {
        let config = RateLimitConfig::with_burst(100, 50); // 150 total capacity
        let limiter = TokenBucketRateLimiter::new(config);

        // Should succeed within burst capacity
        assert!(limiter.try_acquire(100).await);
        assert_eq!(limiter.available_tokens(), 50);

        assert!(limiter.try_acquire(50).await);
        assert_eq!(limiter.available_tokens(), 0);
    }

    #[tokio::test]
    async fn test_try_acquire_failure() {
        let config = RateLimitConfig::with_burst(10, 5); // 15 total capacity
        let limiter = TokenBucketRateLimiter::new(config);

        // Drain all tokens
        assert!(limiter.try_acquire(15).await);

        // Should fail - no tokens available
        assert!(!limiter.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_token_refill() {
        let config = RateLimitConfig::with_burst(1000, 0); // 1000 tokens/sec, no burst
        let limiter = TokenBucketRateLimiter::new(config);

        // Drain all tokens
        assert!(limiter.try_acquire(1000).await);
        assert_eq!(limiter.available_tokens(), 0);

        // Wait for refill (100ms should give ~100 tokens)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Trigger refill and check
        let acquired = limiter.try_acquire(50).await;
        assert!(acquired, "Should have refilled enough tokens");
    }

    #[tokio::test]
    async fn test_acquire_waits_for_tokens() {
        let config = RateLimitConfig::with_burst(100, 10); // 110 total
        let limiter = TokenBucketRateLimiter::new(config);

        // Drain most tokens
        assert!(limiter.try_acquire(100).await);

        // Request more than available - should wait
        let start = Instant::now();
        let wait = limiter.acquire(20).await;
        let elapsed = start.elapsed();

        // Should have waited for tokens to refill
        assert!(elapsed >= Duration::from_millis(50)); // At least some wait
        assert!(wait > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_stats() {
        let config = RateLimitConfig::new(1000);
        let limiter = TokenBucketRateLimiter::new(config);

        let stats = limiter.stats();
        assert!(stats.enabled);
        assert_eq!(stats.rate_limit, 1000);
        assert_eq!(stats.capacity, 1100); // 1000 + 10% burst
        assert_eq!(stats.events_throttled, 0);
    }

    #[tokio::test]
    async fn test_reset() {
        let config = RateLimitConfig::with_burst(100, 0);
        let limiter = TokenBucketRateLimiter::new(config);

        // Drain all tokens
        assert!(limiter.try_acquire(100).await);
        assert_eq!(limiter.available_tokens(), 0);

        // Reset
        limiter.reset().await;
        assert_eq!(limiter.available_tokens(), 100);
    }

    #[tokio::test]
    async fn test_concurrent_acquire() {
        use std::sync::Arc;

        let config = RateLimitConfig::with_burst(1000, 100); // 1100 total
        let limiter = Arc::new(TokenBucketRateLimiter::new(config));

        // Spawn multiple tasks trying to acquire tokens
        let mut handles = Vec::new();
        for _ in 0..10 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    l.acquire(1).await;
                }
            }));
        }

        // Wait for all to complete
        for h in handles {
            h.await.unwrap();
        }

        // All tasks completed - rate limiter handled concurrent access
        let stats = limiter.stats();
        assert!(stats.enabled);
    }
}
