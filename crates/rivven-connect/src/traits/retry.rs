//! Retry utilities for connector operations
//!
//! Provides configurable retry logic with exponential backoff for connector
//! implementations.

use crate::error::{ConnectorError, ConnectorResult};
use std::future::Future;
use std::time::Duration;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (not including the initial attempt)
    pub max_retries: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (e.g., 2.0 doubles delay each retry)
    pub backoff_multiplier: f64,
    /// Optional jitter factor (0.0 to 1.0) to add randomness to delays
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl RetryConfig {
    /// Create a new retry config
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a retry config with no retries (fail immediately)
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a retry config with fixed delay (no exponential backoff)
    pub fn fixed_delay(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay: delay,
            max_delay: delay,
            backoff_multiplier: 1.0,
            jitter_factor: 0.0,
        }
    }

    /// Set max retries (builder pattern)
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set initial delay (builder pattern)
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set max delay (builder pattern)
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set backoff multiplier (builder pattern)
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Set jitter factor (builder pattern)
    pub fn with_jitter(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate delay for a specific attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // cap attempt to prevent i32 overflow and degenerate backoff
        let capped_attempt = attempt.min(30);
        let base_delay = self.initial_delay.as_millis() as f64
            * self.backoff_multiplier.powi(capped_attempt as i32 - 1);

        let capped_delay = base_delay.min(self.max_delay.as_millis() as f64);

        // Add jitter
        let jitter = if self.jitter_factor > 0.0 {
            let jitter_range = capped_delay * self.jitter_factor;
            // Simple deterministic jitter based on attempt number
            let jitter_value = (attempt as f64 * 0.618033988749895) % 1.0;
            jitter_range * (jitter_value - 0.5) * 2.0
        } else {
            0.0
        };

        Duration::from_millis((capped_delay + jitter).max(0.0) as u64)
    }
}

/// Result of a retry operation
#[derive(Debug)]
pub struct RetryResult<T> {
    /// The successful result, if any
    pub result: Option<T>,
    /// Number of attempts made
    pub attempts: u32,
    /// Total time spent retrying (including delays)
    pub total_duration: Duration,
    /// Last error encountered (if failed)
    pub last_error: Option<ConnectorError>,
}

impl<T> RetryResult<T> {
    /// Check if the operation succeeded
    pub fn is_success(&self) -> bool {
        self.result.is_some()
    }

    /// Get the result or the last error
    pub fn into_result(self) -> ConnectorResult<T> {
        match self.result {
            Some(value) => Ok(value),
            None => Err(self.last_error.unwrap_or_else(|| {
                ConnectorError::Internal("retry exhausted with no error".to_string())
            })),
        }
    }
}

/// Execute an async operation with retry logic
///
/// # Example
///
/// ```rust,ignore
/// use rivven_connect::traits::retry::{RetryConfig, retry};
///
/// let config = RetryConfig::default().with_max_retries(3);
///
/// let result = retry(&config, || async {
///     make_http_request().await
/// }).await;
/// ```
pub async fn retry<T, E, F, Fut>(config: &RetryConfig, mut operation: F) -> RetryResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Into<ConnectorError>,
{
    let start = std::time::Instant::now();
    let mut attempts = 0;

    loop {
        attempts += 1;

        match operation().await {
            Ok(value) => {
                return RetryResult {
                    result: Some(value),
                    attempts,
                    total_duration: start.elapsed(),
                    last_error: None,
                };
            }
            Err(e) => {
                let error: ConnectorError = e.into();

                // Check if we should retry
                let should_retry = error.is_retryable() && attempts <= config.max_retries;

                if should_retry {
                    let delay = config.delay_for_attempt(attempts);
                    tokio::time::sleep(delay).await;
                    // Continue to next iteration
                } else {
                    return RetryResult {
                        result: None,
                        attempts,
                        total_duration: start.elapsed(),
                        last_error: Some(error),
                    };
                }
            }
        }
    }
}

/// Execute an async operation with retry, returning the result directly
///
/// This is a convenience wrapper around `retry` that returns a `ConnectorResult`.
pub async fn retry_result<T, E, F, Fut>(config: &RetryConfig, operation: F) -> ConnectorResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Into<ConnectorError>,
{
    retry(config, operation).await.into_result()
}

/// A retry guard that tracks attempts and calculates delays
#[derive(Debug)]
pub struct RetryGuard {
    config: RetryConfig,
    attempt: u32,
    start_time: std::time::Instant,
}

impl RetryGuard {
    /// Create a new retry guard
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            attempt: 0,
            start_time: std::time::Instant::now(),
        }
    }

    /// Check if another attempt should be made
    pub fn should_retry(&self) -> bool {
        self.attempt <= self.config.max_retries
    }

    /// Record an attempt and get the delay before the next attempt
    pub fn record_attempt(&mut self) -> Option<Duration> {
        self.attempt += 1;

        if self.attempt <= self.config.max_retries {
            Some(self.config.delay_for_attempt(self.attempt))
        } else {
            None
        }
    }

    /// Get current attempt number (1-indexed)
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Get elapsed time since guard creation
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Reset the guard for reuse
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.start_time = std::time::Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay, Duration::from_millis(100));
    }

    #[test]
    fn test_retry_config_no_retry() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_retry_config_fixed_delay() {
        let config = RetryConfig::fixed_delay(5, Duration::from_secs(1));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.backoff_multiplier, 1.0);
    }

    #[test]
    fn test_delay_calculation_exponential() {
        let config = RetryConfig::new()
            .with_initial_delay(Duration::from_millis(100))
            .with_backoff_multiplier(2.0)
            .with_jitter(0.0);

        assert_eq!(config.delay_for_attempt(0), Duration::ZERO);
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(400));
    }

    #[test]
    fn test_delay_capped_at_max() {
        let config = RetryConfig::new()
            .with_initial_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5))
            .with_backoff_multiplier(10.0)
            .with_jitter(0.0);

        // 10^3 = 1000 seconds would exceed max
        let delay = config.delay_for_attempt(4);
        assert!(delay <= Duration::from_secs(5));
    }

    #[test]
    fn test_retry_guard() {
        let config = RetryConfig::new().with_max_retries(3);
        let mut guard = RetryGuard::new(config);

        assert!(guard.should_retry());
        assert_eq!(guard.attempt(), 0);

        let delay1 = guard.record_attempt();
        assert!(delay1.is_some());
        assert_eq!(guard.attempt(), 1);

        guard.record_attempt();
        guard.record_attempt();

        let delay4 = guard.record_attempt();
        assert!(delay4.is_none()); // Exceeded max retries
    }

    #[tokio::test]
    async fn test_retry_success() {
        let config = RetryConfig::new().with_max_retries(3);

        let result = retry(&config, || async { Ok::<_, ConnectorError>(42) }).await;

        assert!(result.is_success());
        assert_eq!(result.result, Some(42));
        assert_eq!(result.attempts, 1);
    }

    #[tokio::test]
    async fn test_retry_with_transient_failures() {
        let config = RetryConfig::new()
            .with_max_retries(3)
            .with_initial_delay(Duration::from_millis(1));

        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = retry(&config, || {
            let counter = counter_clone.clone();
            async move {
                let attempt = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if attempt < 2 {
                    Err(ConnectorError::Transient("temporary failure".to_string()))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert!(result.is_success());
        assert_eq!(result.result, Some(42));
        assert_eq!(result.attempts, 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let config = RetryConfig::new()
            .with_max_retries(2)
            .with_initial_delay(Duration::from_millis(1));

        let result = retry(&config, || async {
            Err::<i32, _>(ConnectorError::Transient("always fails".to_string()))
        })
        .await;

        assert!(!result.is_success());
        assert!(result.last_error.is_some());
        assert_eq!(result.attempts, 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_retry_non_retryable_error() {
        let config = RetryConfig::new().with_max_retries(5);

        let result = retry(&config, || async {
            Err::<i32, _>(ConnectorError::Fatal("permanent failure".to_string()))
        })
        .await;

        assert!(!result.is_success());
        assert_eq!(result.attempts, 1); // Doesn't retry on fatal errors
    }
}
