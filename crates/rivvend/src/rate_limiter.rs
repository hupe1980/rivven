//! Rate limiting and DoS protection for the Rivven server
//!
//! Implements:
//! - Per-IP connection limits
//! - Global connection limits  
//! - Per-IP request rate limiting (token bucket)
//! - Request size limits
//! - Connection tracking and cleanup

use rivven_core::metrics::CoreMetrics;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum connections per IP address
    pub max_connections_per_ip: u32,
    /// Maximum total connections across all IPs
    pub max_total_connections: u32,
    /// Requests per second limit per IP (0 = unlimited)
    pub rate_limit_per_ip: u32,
    /// Maximum request size in bytes
    pub max_request_size: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_connections_per_ip: 100,
            max_total_connections: 10_000,
            rate_limit_per_ip: 10_000,
            max_request_size: 10 * 1024 * 1024, // 10 MB
            idle_timeout: Duration::from_secs(120),
        }
    }
}

/// Token bucket for per-IP rate limiting
///
/// Uses `AtomicU64` storing microseconds since process start for lock-free refill timing.
struct TokenBucket {
    tokens: AtomicU64,
    /// Microseconds since `EPOCH` of the last refill (lock-free)
    last_refill_us: AtomicU64,
    capacity: u64,
    refill_rate: u64, // tokens per second
}

/// Process-wide epoch for lock-free Instant→u64 conversion.
/// Using `LazyLock` ensures a single initialization.
static RATE_LIMITER_EPOCH: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

fn instant_to_us(t: Instant) -> u64 {
    t.duration_since(*RATE_LIMITER_EPOCH).as_micros() as u64
}

fn now_us() -> u64 {
    instant_to_us(Instant::now())
}

impl TokenBucket {
    fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity),
            last_refill_us: AtomicU64::new(now_us()),
            capacity,
            refill_rate,
        }
    }

    /// Try to acquire a token (lock-free).
    ///
    /// Refills tokens based on elapsed time since the last refill using
    /// CAS on the `last_refill_us` atomic. Only one thread wins the refill
    /// race; others proceed directly to token consumption.
    fn try_acquire(&self) -> bool {
        // Attempt refill via CAS on last_refill_us
        let now = now_us();
        let last = self.last_refill_us.load(Ordering::Acquire);
        let elapsed_us = now.saturating_sub(last);

        if elapsed_us > 0 {
            let refill_amount = (elapsed_us as f64 / 1_000_000.0 * self.refill_rate as f64) as u64;
            if refill_amount > 0 {
                // CAS: only one thread wins the refill
                if self
                    .last_refill_us
                    .compare_exchange_weak(last, now, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // We won the refill race — add tokens atomically.
                    // Must use fetch_update (CAS loop) to avoid overwriting
                    // concurrent token consumption between our load and store.
                    let cap = self.capacity;
                    let _ =
                        self.tokens
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                                Some((current + refill_amount).min(cap))
                            });
                }
            }
        }

        // Try to consume a token (CAS loop)
        loop {
            let current = self.tokens.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }
            if self
                .tokens
                .compare_exchange_weak(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
    }
}

/// Per-IP connection and rate state
struct IpState {
    /// Number of active connections from this IP
    connections: AtomicU32,
    /// Token bucket for rate limiting
    rate_bucket: TokenBucket,
    /// Last activity time (microseconds since RATE_LIMITER_EPOCH, lock-free)
    last_activity_us: AtomicU64,
}

impl IpState {
    fn new(rate_limit: u32) -> Self {
        let capacity = rate_limit as u64;
        Self {
            connections: AtomicU32::new(0),
            rate_bucket: TokenBucket::new(capacity, capacity), // refill fully per second
            last_activity_us: AtomicU64::new(now_us()),
        }
    }

    fn decrement_connections(&self) {
        let _ = self
            .connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
    }

    fn get_connections(&self) -> u32 {
        self.connections.load(Ordering::Relaxed)
    }

    /// Lock-free rate check — no nested locks required.
    fn try_rate_limit(&self) -> bool {
        let result = self.rate_bucket.try_acquire();
        if result {
            self.last_activity_us.store(now_us(), Ordering::Release);
        }
        result
    }

    fn is_stale(&self, timeout: Duration) -> bool {
        let last = self.last_activity_us.load(Ordering::Acquire);
        let elapsed_us = now_us().saturating_sub(last);
        elapsed_us > timeout.as_micros() as u64 && self.connections.load(Ordering::Relaxed) == 0
    }
}

/// Rate limiter result for connection attempts
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionResult {
    /// Connection allowed
    Allowed,
    /// Rejected: too many connections from this IP
    TooManyConnectionsFromIp,
    /// Rejected: global connection limit reached
    TooManyTotalConnections,
}

/// Rate limiter result for requests
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestResult {
    /// Request allowed
    Allowed,
    /// Rate limited
    RateLimited,
    /// Request too large
    RequestTooLarge,
}

/// Connection guard that decrements connection count on drop
pub struct ConnectionGuard {
    /// Total connections counter
    total_connections: Arc<AtomicU32>,
    /// Per-IP state
    ip_state: Arc<IpState>,
    /// Client IP (for debug/logging only)
    ip: IpAddr,
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard")
            .field("ip", &self.ip)
            .finish()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Decrement counters — saturating to avoid underflow on double-drop
        let _ = self
            .total_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
        self.ip_state.decrement_connections();
        debug!("Connection released from {}", self.ip);
    }
}

/// Maximum number of tracked IPs before forced eviction of stale entries.
/// Prevents unbounded HashMap growth from many unique source IPs.
const MAX_TRACKED_IPS: usize = 100_000;

/// Thread-safe rate limiter managing per-IP and global limits
pub struct RateLimiter {
    config: RateLimitConfig,
    /// Per-IP state
    ip_states: RwLock<HashMap<IpAddr, Arc<IpState>>>,
    /// Total connection count (Arc to share with guards)
    total_connections: Arc<AtomicU32>,
}

impl RateLimiter {
    /// Create a new rate limiter with the given configuration
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            ip_states: RwLock::new(HashMap::new()),
            total_connections: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Check if a new connection from the given IP should be allowed
    /// Returns a guard that will release the connection on drop
    pub async fn try_connection(&self, ip: IpAddr) -> Result<ConnectionGuard, ConnectionResult> {
        // Atomically try to increment global counter if under limit
        loop {
            let total = self.total_connections.load(Ordering::Acquire);
            if total >= self.config.max_total_connections {
                warn!("Global connection limit reached: {}", total);
                CoreMetrics::increment_rate_limit_rejections();
                return Err(ConnectionResult::TooManyTotalConnections);
            }

            // Try to atomically increment the counter
            if self
                .total_connections
                .compare_exchange(total, total + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            // Another thread modified the counter, retry
        }

        // Get or create IP state
        let ip_state = {
            let states = self.ip_states.read().await;
            if let Some(state) = states.get(&ip) {
                state.clone()
            } else {
                drop(states);
                let mut states = self.ip_states.write().await;
                // Double-check after acquiring write lock
                if let Some(state) = states.get(&ip) {
                    state.clone()
                } else {
                    // Evict stale entries when the map grows too large
                    if states.len() >= MAX_TRACKED_IPS {
                        self.evict_stale_entries_locked(&mut states);
                    }
                    let state = Arc::new(IpState::new(self.config.rate_limit_per_ip));
                    states.insert(ip, state.clone());
                    state
                }
            }
        };

        // Atomically try to increment per-IP counter if under limit
        loop {
            let ip_connections = ip_state.connections.load(Ordering::Acquire);
            if ip_connections >= self.config.max_connections_per_ip {
                // Rollback global counter since we're rejecting
                self.total_connections.fetch_sub(1, Ordering::Relaxed);
                warn!(
                    "Connection limit for {} reached: {} >= {}",
                    ip, ip_connections, self.config.max_connections_per_ip
                );
                CoreMetrics::increment_rate_limit_rejections();
                return Err(ConnectionResult::TooManyConnectionsFromIp);
            }

            // Try to atomically increment the IP counter
            if ip_state
                .connections
                .compare_exchange(
                    ip_connections,
                    ip_connections + 1,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
            // Another thread modified the counter, retry
        }

        debug!(
            "Connection accepted from {}: {} connections (total: {})",
            ip,
            ip_state.get_connections(),
            self.total_connections.load(Ordering::Relaxed)
        );

        Ok(ConnectionGuard {
            total_connections: self.total_connections.clone(),
            ip_state,
            ip,
        })
    }

    /// Check if a request should be allowed (rate limiting)
    ///
    /// If an IP has no `IpState` entry (e.g., requests forwarded via
    /// a reverse proxy using X-Forwarded-For that never went through
    /// `try_connection`), we now create the state on demand instead of
    /// unconditionally allowing the request.
    pub async fn check_request(&self, ip: &IpAddr, request_size: usize) -> RequestResult {
        // Check request size
        if request_size > self.config.max_request_size {
            warn!(
                "Request from {} too large: {} > {}",
                ip, request_size, self.config.max_request_size
            );
            CoreMetrics::increment_rate_limit_rejections();
            return RequestResult::RequestTooLarge;
        }

        // Skip rate limiting if disabled
        if self.config.rate_limit_per_ip == 0 {
            return RequestResult::Allowed;
        }

        // Try read lock first (fast path)
        {
            let states = self.ip_states.read().await;
            if let Some(state) = states.get(ip) {
                if !state.try_rate_limit() {
                    debug!("Rate limited request from {}", ip);
                    CoreMetrics::increment_rate_limit_rejections();
                    return RequestResult::RateLimited;
                }
                return RequestResult::Allowed;
            }
        }

        // No entry — create on demand under write lock (double-check pattern)
        {
            let mut states = self.ip_states.write().await;
            let state = states
                .entry(*ip)
                .or_insert_with(|| Arc::new(IpState::new(self.config.rate_limit_per_ip)));
            if !state.try_rate_limit() {
                debug!("Rate limited request from {}", ip);
                CoreMetrics::increment_rate_limit_rejections();
                return RequestResult::RateLimited;
            }
        }

        RequestResult::Allowed
    }

    /// Get current statistics
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            total_connections: self.total_connections.load(Ordering::Relaxed),
            max_total_connections: self.config.max_total_connections,
            max_per_ip: self.config.max_connections_per_ip,
        }
    }

    /// Get the configured idle timeout (used as read timeout for connections)
    pub fn idle_timeout(&self) -> Duration {
        self.config.idle_timeout
    }

    /// Cleanup stale IP entries (should be called periodically)
    pub async fn cleanup_stale(&self) {
        let mut states = self.ip_states.write().await;
        self.evict_stale_entries_locked(&mut states);
    }

    /// Evict stale entries from the map while holding the write lock.
    fn evict_stale_entries_locked(&self, states: &mut HashMap<IpAddr, Arc<IpState>>) {
        let stale_timeout = self.config.idle_timeout * 2; // Give extra time

        let before = states.len();
        states.retain(|ip, state| {
            let stale = state.is_stale(stale_timeout);
            if stale {
                debug!("Cleaned up stale rate limit state for {}", ip);
            }
            !stale
        });

        let evicted = before - states.len();
        if evicted > 0 {
            debug!(
                "Evicted {} stale IP entries ({} remaining)",
                evicted,
                states.len()
            );
        }
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub total_connections: u32,
    pub max_total_connections: u32,
    pub max_per_ip: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[tokio::test]
    async fn test_connection_limit_per_ip() {
        let config = RateLimitConfig {
            max_connections_per_ip: 2,
            max_total_connections: 100,
            rate_limit_per_ip: 0, // disable rate limiting
            ..Default::default()
        };
        let limiter = Arc::new(RateLimiter::new(config));
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        // First two connections should succeed
        let guard1 = limiter.try_connection(ip).await;
        assert!(guard1.is_ok());

        let guard2 = limiter.try_connection(ip).await;
        assert!(guard2.is_ok());

        // Third connection should fail
        let result = limiter.try_connection(ip).await;
        assert_eq!(
            result.unwrap_err(),
            ConnectionResult::TooManyConnectionsFromIp
        );

        // Drop one guard, should allow another connection
        drop(guard1);

        let guard3 = limiter.try_connection(ip).await;
        assert!(guard3.is_ok());
    }

    #[tokio::test]
    async fn test_global_connection_limit() {
        let config = RateLimitConfig {
            max_connections_per_ip: 100,
            max_total_connections: 2,
            rate_limit_per_ip: 0,
            ..Default::default()
        };
        let limiter = Arc::new(RateLimiter::new(config));

        let ip1 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2));
        let ip3 = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 3));

        let _guard1 = limiter.try_connection(ip1).await.unwrap();
        let _guard2 = limiter.try_connection(ip2).await.unwrap();

        // Third connection should fail even from different IP
        let result = limiter.try_connection(ip3).await;
        assert_eq!(
            result.unwrap_err(),
            ConnectionResult::TooManyTotalConnections
        );
    }

    #[tokio::test]
    async fn test_request_size_limit() {
        let config = RateLimitConfig {
            max_request_size: 1000,
            rate_limit_per_ip: 0,
            ..Default::default()
        };
        let limiter = RateLimiter::new(config);
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        // Small request should pass
        let result = limiter.check_request(&ip, 500).await;
        assert_eq!(result, RequestResult::Allowed);

        // Large request should fail
        let result = limiter.check_request(&ip, 2000).await;
        assert_eq!(result, RequestResult::RequestTooLarge);
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let config = RateLimitConfig {
            max_connections_per_ip: 100,
            max_total_connections: 100,
            rate_limit_per_ip: 5, // 5 requests per second
            max_request_size: 10_000,
            idle_timeout: Duration::from_secs(60),
        };
        let limiter = Arc::new(RateLimiter::new(config));
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        // Create connection first to populate IP state
        let _guard = limiter.try_connection(ip).await.unwrap();

        // First 5 requests should pass
        for _ in 0..5 {
            let result = limiter.check_request(&ip, 100).await;
            assert_eq!(result, RequestResult::Allowed);
        }

        // 6th request should be rate limited
        let result = limiter.check_request(&ip, 100).await;
        assert_eq!(result, RequestResult::RateLimited);
    }

    #[tokio::test]
    async fn test_connection_guard_releases_on_drop() {
        let config = RateLimitConfig {
            max_connections_per_ip: 1,
            max_total_connections: 100,
            rate_limit_per_ip: 0,
            ..Default::default()
        };
        let limiter = Arc::new(RateLimiter::new(config));
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

        // First connection succeeds
        {
            let _guard = limiter.try_connection(ip).await.unwrap();
            // Connection is active here
            assert_eq!(limiter.stats().total_connections, 1);
        }
        // Guard dropped here

        // After drop, connection should be released
        // Note: In async context, we may need a small delay
        assert_eq!(limiter.stats().total_connections, 0);

        // New connection should succeed
        let _guard = limiter.try_connection(ip).await.unwrap();
        assert_eq!(limiter.stats().total_connections, 1);
    }
}
