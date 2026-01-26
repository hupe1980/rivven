//! Rate limiting and DoS protection for the Rivven server
//!
//! Implements:
//! - Per-IP connection limits
//! - Global connection limits  
//! - Per-IP request rate limiting (token bucket)
//! - Request size limits
//! - Connection tracking and cleanup

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use rivven_core::metrics::CoreMetrics;

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
struct TokenBucket {
    tokens: AtomicU64,
    last_refill: RwLock<Instant>,
    capacity: u64,
    refill_rate: u64, // tokens per second
}

impl TokenBucket {
    fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity),
            last_refill: RwLock::new(Instant::now()),
            capacity,
            refill_rate,
        }
    }

    async fn try_acquire(&self) -> bool {
        // Refill tokens based on elapsed time
        let mut last = self.last_refill.write().await;
        let elapsed = last.elapsed();
        let refill_amount = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
        
        if refill_amount > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_tokens = (current + refill_amount).min(self.capacity);
            self.tokens.store(new_tokens, Ordering::Relaxed);
            *last = Instant::now();
        }
        drop(last);

        // Try to consume a token
        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current == 0 {
                return false;
            }
            if self.tokens.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ).is_ok() {
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
    /// Last activity time for cleanup
    last_activity: RwLock<Instant>,
}

impl IpState {
    fn new(rate_limit: u32) -> Self {
        let capacity = rate_limit as u64;
        Self {
            connections: AtomicU32::new(0),
            rate_bucket: TokenBucket::new(capacity, capacity), // refill fully per second
            last_activity: RwLock::new(Instant::now()),
        }
    }

    fn increment_connections(&self) -> u32 {
        self.connections.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn decrement_connections(&self) {
        self.connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn get_connections(&self) -> u32 {
        self.connections.load(Ordering::Relaxed)
    }

    async fn try_rate_limit(&self) -> bool {
        let result = self.rate_bucket.try_acquire().await;
        if result {
            *self.last_activity.write().await = Instant::now();
        }
        result
    }

    async fn is_stale(&self, timeout: Duration) -> bool {
        let last = self.last_activity.read().await;
        last.elapsed() > timeout && self.connections.load(Ordering::Relaxed) == 0
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
        // Decrement counters - all atomic, no blocking
        self.total_connections.fetch_sub(1, Ordering::Relaxed);
        self.ip_state.decrement_connections();
        debug!("Connection released from {}", self.ip);
    }
}

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
    pub async fn try_connection(
        &self,
        ip: IpAddr,
    ) -> Result<ConnectionGuard, ConnectionResult> {
        // Check global limit first
        let total = self.total_connections.load(Ordering::Relaxed);
        if total >= self.config.max_total_connections {
            warn!("Global connection limit reached: {}", total);
            CoreMetrics::increment_rate_limit_rejections();
            return Err(ConnectionResult::TooManyTotalConnections);
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
                    let state = Arc::new(IpState::new(self.config.rate_limit_per_ip));
                    states.insert(ip, state.clone());
                    state
                }
            }
        };

        // Check per-IP limit
        let ip_connections = ip_state.get_connections();
        if ip_connections >= self.config.max_connections_per_ip {
            warn!(
                "Connection limit for {} reached: {} >= {}",
                ip, ip_connections, self.config.max_connections_per_ip
            );
            CoreMetrics::increment_rate_limit_rejections();
            return Err(ConnectionResult::TooManyConnectionsFromIp);
        }

        // Increment counters
        ip_state.increment_connections();
        self.total_connections.fetch_add(1, Ordering::Relaxed);

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

        // Get IP state
        let states = self.ip_states.read().await;
        if let Some(state) = states.get(ip) {
            if !state.try_rate_limit().await {
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
        let stale_timeout = self.config.idle_timeout * 2; // Give extra time
        
        let mut to_remove = Vec::new();
        for (ip, state) in states.iter() {
            if state.is_stale(stale_timeout).await {
                to_remove.push(*ip);
            }
        }

        for ip in to_remove {
            states.remove(&ip);
            debug!("Cleaned up stale rate limit state for {}", ip);
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
        assert_eq!(result.unwrap_err(), ConnectionResult::TooManyConnectionsFromIp);

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
        assert_eq!(result.unwrap_err(), ConnectionResult::TooManyTotalConnections);
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
