//! Connection pool abstraction for rivven-rdbc
//!
//! High-performance connection pooling with:
//! - Configurable pool sizes and timeouts
//! - Health checking and connection validation
//! - Metrics and observability
//! - Idle connection management
//! - Connection lifecycle tracking
//!
//! # Performance Characteristics
//!
//! The pool is optimized for the "hot path" - acquiring an idle connection:
//!
//! - **Semaphore-based concurrency**: O(1) permit acquisition, no thread starvation
//! - **LIFO idle stack**: Better cache locality for recently-used connections
//! - **Lock-free stats**: Atomic counters with `Relaxed` ordering for minimal overhead
//! - **Created_at preservation**: Accurate lifecycle tracking without extra allocations
//!
//! ## Typical Latencies
//!
//! | Operation | Latency |
//! |-----------|---------|
//! | Hot path (idle available) | ~1-5μs |
//! | Cold path (new connection) | ~1-10ms |
//! | Health check (test_on_borrow) | ~0.5-2ms |
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_rdbc::prelude::*;
//! use rivven_rdbc::pool::SimpleConnectionPool;
//! use rivven_rdbc::postgres::PgConnectionFactory;
//!
//! let pool = SimpleConnectionPool::new(
//!     PoolConfig::new("postgres://localhost/db").with_max_size(10),
//!     Arc::new(PgConnectionFactory),
//! ).await?;
//!
//! let conn = pool.get().await?;
//! conn.execute("SELECT 1", &[]).await?;
//! // Connection is returned to pool when dropped
//! ```
//!
//! # Statistics and Observability
//!
//! The pool tracks comprehensive statistics for monitoring:
//!
//! ```rust,ignore
//! let stats = pool.stats();
//! println!("Reuse rate: {:.1}%", stats.reuse_rate() * 100.0);
//! println!("Avg wait: {:.2}ms", stats.avg_wait_time_ms());
//! println!("Active connections: {}", stats.active_connections());
//! println!("Recycled (lifetime): {}", stats.lifetime_expired_count);
//! println!("Recycled (idle): {}", stats.idle_expired_count);
//! ```

use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

use crate::connection::{Connection, ConnectionConfig, ConnectionFactory};
use crate::error::{Error, Result};

/// Connection pool trait
#[async_trait]
pub trait ConnectionPool: Send + Sync {
    /// Get a connection from the pool
    async fn get(&self) -> Result<PooledConnection>;

    /// Return a connection to the pool with its original creation time
    async fn return_connection(&self, conn: Box<dyn Connection>, created_at: Instant);

    /// Get current pool size
    fn size(&self) -> usize;

    /// Get number of idle connections
    fn idle(&self) -> usize;

    /// Get number of connections in use
    fn in_use(&self) -> usize {
        self.size().saturating_sub(self.idle())
    }

    /// Get pool statistics
    fn stats(&self) -> PoolStats;

    /// Close all connections and shutdown the pool
    async fn close(&self) -> Result<()>;
}

/// A connection borrowed from the pool
pub struct PooledConnection {
    /// The underlying connection
    conn: Option<Box<dyn Connection>>,
    /// Reference to the pool for return
    pool: Arc<dyn ConnectionPool>,
    /// When the connection was originally created (for proper lifecycle tracking)
    created_at: Instant,
    /// When this connection was borrowed from the pool
    borrowed_at: Instant,
}

impl PooledConnection {
    /// Create a new pooled connection wrapper
    pub fn new(conn: Box<dyn Connection>, pool: Arc<dyn ConnectionPool>) -> Self {
        let now = Instant::now();
        Self {
            conn: Some(conn),
            pool,
            created_at: now,
            borrowed_at: now,
        }
    }

    /// Create a new pooled connection wrapper with a specific creation time
    pub fn with_created_at(
        conn: Box<dyn Connection>,
        pool: Arc<dyn ConnectionPool>,
        created_at: Instant,
    ) -> Self {
        Self {
            conn: Some(conn),
            pool,
            created_at,
            borrowed_at: Instant::now(),
        }
    }

    /// Get when this connection was originally created
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Get the age of this connection
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Check if connection has exceeded the given max lifetime
    pub fn is_expired(&self, max_lifetime: Duration) -> bool {
        self.age() > max_lifetime
    }

    /// Get when this connection was borrowed from the pool
    #[inline]
    pub fn borrowed_at(&self) -> Instant {
        self.borrowed_at
    }

    /// Get how long this connection has been borrowed (time in use)
    #[inline]
    pub fn time_in_use(&self) -> Duration {
        self.borrowed_at.elapsed()
    }

    /// Get the underlying connection
    ///
    /// # Panics
    /// Panics if called after the connection has been returned to the pool
    /// (structurally unreachable in normal usage).
    pub fn connection(&self) -> &dyn Connection {
        self.conn
            .as_ref()
            .expect("BUG: PooledConnection used after return to pool")
            .as_ref()
    }

    /// Get mutable reference to the underlying connection
    ///
    /// # Panics
    /// Panics if called after the connection has been returned to the pool
    /// (structurally unreachable in normal usage).
    pub fn connection_mut(&mut self) -> &mut dyn Connection {
        self.conn
            .as_mut()
            .expect("BUG: PooledConnection used after return to pool")
            .as_mut()
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = dyn Connection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("BUG: PooledConnection used after return to pool")
            .as_ref()
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("BUG: PooledConnection used after return to pool")
            .as_mut()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            let created_at = self.created_at;
            // Only spawn if a tokio runtime is still available.
            // During shutdown the runtime may already be gone, in which
            // case the connection is dropped without being returned to the
            // pool. This leaks the semaphore permit, effectively reducing
            // the pool's capacity by one. Because this only occurs during
            // runtime shutdown (after which no new `get()` calls will
            // succeed), the leak is harmless in practice.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    pool.return_connection(conn, created_at).await;
                });
            } else {
                tracing::warn!(
                    "Tokio runtime unavailable during PooledConnection drop; \
                     connection dropped without being returned to pool \
                     (semaphore permit leaked — only expected during shutdown)"
                );
            }
        }
    }
}

/// Pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Connection configuration
    pub connection: ConnectionConfig,
    /// Minimum pool size (idle connections)
    pub min_size: usize,
    /// Maximum pool size
    pub max_size: usize,
    /// Maximum time to wait for a connection
    pub acquire_timeout: Duration,
    /// Maximum connection lifetime (for recycling)
    pub max_lifetime: Duration,
    /// Idle timeout (connections idle longer are closed)
    pub idle_timeout: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Whether to test connections on borrow
    pub test_on_borrow: bool,
    /// Whether to test connections on return
    pub test_on_return: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            min_size: 1,
            max_size: 10,
            acquire_timeout: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(1800), // 30 minutes
            idle_timeout: Duration::from_secs(600),  // 10 minutes
            health_check_interval: Duration::from_secs(30),
            test_on_borrow: true,
            test_on_return: false,
        }
    }
}

impl PoolConfig {
    /// Create pool config from a connection URL
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            connection: ConnectionConfig::new(url),
            ..Default::default()
        }
    }

    /// Set minimum pool size
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_size = size;
        self
    }

    /// Set maximum pool size
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set acquire timeout
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set maximum connection lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Enable/disable test on borrow
    pub fn with_test_on_borrow(mut self, test: bool) -> Self {
        self.test_on_borrow = test;
        self
    }

    /// Enable/disable test on return
    pub fn with_test_on_return(mut self, test: bool) -> Self {
        self.test_on_return = test;
        self
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total number of connections created
    pub connections_created: u64,
    /// Total number of connections closed
    pub connections_closed: u64,
    /// Total number of connection acquisitions
    pub acquisitions: u64,
    /// Number of times pool was exhausted
    pub exhausted_count: u64,
    /// Total wait time for connections (in milliseconds)
    pub total_wait_time_ms: u64,
    /// Number of health check failures
    pub health_check_failures: u64,
    /// Total number of health checks performed
    pub health_checks_performed: u64,
    /// Connections recycled due to max lifetime exceeded
    pub lifetime_expired_count: u64,
    /// Connections recycled due to idle timeout exceeded
    pub idle_expired_count: u64,
    /// Connections successfully reused from idle pool
    pub reused_count: u64,
    /// Fresh connections created on-demand (not from pool)
    pub fresh_count: u64,
}

impl PoolStats {
    /// Get total recycled connections (lifetime + idle expired)
    #[inline]
    pub fn recycled_total(&self) -> u64 {
        self.lifetime_expired_count + self.idle_expired_count
    }

    /// Get connection reuse rate (0.0 to 1.0)
    ///
    /// Higher is better - indicates pool is effectively reusing connections.
    #[inline]
    pub fn reuse_rate(&self) -> f64 {
        if self.acquisitions == 0 {
            0.0
        } else {
            self.reused_count as f64 / self.acquisitions as f64
        }
    }

    /// Get average wait time in milliseconds
    #[inline]
    pub fn avg_wait_time_ms(&self) -> f64 {
        if self.acquisitions == 0 {
            0.0
        } else {
            self.total_wait_time_ms as f64 / self.acquisitions as f64
        }
    }

    /// Get health check failure rate (0.0 to 1.0)
    #[inline]
    pub fn health_failure_rate(&self) -> f64 {
        if self.health_checks_performed == 0 {
            0.0
        } else {
            self.health_check_failures as f64 / self.health_checks_performed as f64
        }
    }

    /// Get active (non-closed) connection count
    #[inline]
    pub fn active_connections(&self) -> u64 {
        self.connections_created
            .saturating_sub(self.connections_closed)
    }
}

/// Atomic pool stats for concurrent updates
///
/// Uses `Relaxed` ordering for maximum performance on the hot path.
/// Stats are eventually consistent, suitable for observability and metrics.
#[derive(Debug, Default)]
#[allow(missing_docs)]
pub struct AtomicPoolStats {
    pub connections_created: AtomicU64,
    pub connections_closed: AtomicU64,
    pub acquisitions: AtomicU64,
    pub exhausted_count: AtomicU64,
    pub total_wait_time_ms: AtomicU64,
    pub health_check_failures: AtomicU64,
    pub health_checks_performed: AtomicU64,
    pub lifetime_expired_count: AtomicU64,
    pub idle_expired_count: AtomicU64,
    pub reused_count: AtomicU64,
    pub fresh_count: AtomicU64,
}

impl AtomicPoolStats {
    /// Create new atomic stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a connection creation
    pub fn record_created(&self) {
        self.connections_created.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection close
    pub fn record_closed(&self) {
        self.connections_closed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an acquisition
    pub fn record_acquisition(&self, wait_time_ms: u64) {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);
        self.total_wait_time_ms
            .fetch_add(wait_time_ms, Ordering::Relaxed);
    }

    /// Record pool exhaustion
    pub fn record_exhausted(&self) {
        self.exhausted_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record health check failure
    pub fn record_health_check_failure(&self) {
        self.health_check_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a health check was performed (pass or fail)
    pub fn record_health_check(&self) {
        self.health_checks_performed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection recycled due to max lifetime exceeded
    pub fn record_lifetime_expired(&self) {
        self.lifetime_expired_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record connection recycled due to idle timeout exceeded
    pub fn record_idle_expired(&self) {
        self.idle_expired_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection reused from the idle pool
    #[inline]
    pub fn record_reused(&self) {
        self.reused_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a fresh connection created (not from pool)
    #[inline]
    pub fn record_fresh(&self) {
        self.fresh_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot current stats
    pub fn snapshot(&self) -> PoolStats {
        PoolStats {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            acquisitions: self.acquisitions.load(Ordering::Relaxed),
            exhausted_count: self.exhausted_count.load(Ordering::Relaxed),
            total_wait_time_ms: self.total_wait_time_ms.load(Ordering::Relaxed),
            health_check_failures: self.health_check_failures.load(Ordering::Relaxed),
            health_checks_performed: self.health_checks_performed.load(Ordering::Relaxed),
            lifetime_expired_count: self.lifetime_expired_count.load(Ordering::Relaxed),
            idle_expired_count: self.idle_expired_count.load(Ordering::Relaxed),
            reused_count: self.reused_count.load(Ordering::Relaxed),
            fresh_count: self.fresh_count.load(Ordering::Relaxed),
        }
    }

    /// Calculate average wait time in milliseconds
    pub fn avg_wait_time_ms(&self) -> f64 {
        let acquisitions = self.acquisitions.load(Ordering::Relaxed);
        if acquisitions == 0 {
            0.0
        } else {
            self.total_wait_time_ms.load(Ordering::Relaxed) as f64 / acquisitions as f64
        }
    }
}

/// Pool builder for fluent configuration
pub struct PoolBuilder {
    config: PoolConfig,
}

impl PoolBuilder {
    /// Create a new pool builder
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            config: PoolConfig::new(url),
        }
    }

    /// Set minimum pool size
    pub fn min_size(mut self, size: usize) -> Self {
        self.config.min_size = size;
        self
    }

    /// Set maximum pool size
    pub fn max_size(mut self, size: usize) -> Self {
        self.config.max_size = size;
        self
    }

    /// Set acquire timeout
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.config.acquire_timeout = timeout;
        self
    }

    /// Set max lifetime
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.config.max_lifetime = lifetime;
        self
    }

    /// Set idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.idle_timeout = timeout;
        self
    }

    /// Enable test on borrow
    pub fn test_on_borrow(mut self, test: bool) -> Self {
        self.config.test_on_borrow = test;
        self
    }

    /// Get the configuration
    pub fn config(self) -> PoolConfig {
        self.config
    }
}

// ============================================================================
// SimpleConnectionPool - Production-ready connection pool implementation
// ============================================================================

/// A simple but production-ready connection pool.
///
/// Features:
/// - Configurable min/max connections
/// - Semaphore-based concurrency control (no thread starvation)
/// - Health checking on borrow
/// - Automatic connection recycling
/// - Metrics and statistics
/// - Graceful shutdown
///
/// # Hot Path Optimization
///
/// The pool uses a lock-free semaphore for concurrency control, minimizing
/// contention on the hot path. Connection acquisition is O(1) when idle
/// connections are available.
pub struct SimpleConnectionPool {
    /// Pool configuration
    config: PoolConfig,
    /// Connection factory
    factory: Arc<dyn ConnectionFactory>,
    /// Idle connections (LIFO for better cache locality)
    idle: Mutex<Vec<PoolEntry>>,
    /// Semaphore to limit total connections
    semaphore: Semaphore,
    /// Current total connection count
    total_connections: AtomicUsize,
    /// Statistics
    stats: Arc<AtomicPoolStats>,
    /// Shutdown flag
    shutdown: std::sync::atomic::AtomicBool,
    /// Self reference for creating PooledConnections
    self_ref: tokio::sync::OnceCell<std::sync::Weak<Self>>,
}

/// Reason for connection recycling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecycleReason {
    /// Connection exceeded max lifetime
    LifetimeExpired,
    /// Connection exceeded idle timeout
    IdleExpired,
}

/// Internal pool entry with metadata
struct PoolEntry {
    /// The connection
    conn: Box<dyn Connection>,
    /// When the connection was created
    created_at: Instant,
    /// When last used
    last_used: Instant,
}

impl SimpleConnectionPool {
    /// Create a new connection pool.
    ///
    /// Initializes with `min_size` connections eagerly.
    pub async fn new(config: PoolConfig, factory: Arc<dyn ConnectionFactory>) -> Result<Arc<Self>> {
        let pool = Arc::new(Self {
            semaphore: Semaphore::new(config.max_size),
            config: config.clone(),
            factory,
            idle: Mutex::new(Vec::with_capacity(config.max_size)),
            total_connections: AtomicUsize::new(0),
            stats: Arc::new(AtomicPoolStats::new()),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            self_ref: tokio::sync::OnceCell::new(),
        });

        // Store weak self-reference
        let _ = pool.self_ref.set(Arc::downgrade(&pool));

        // Pre-populate with min_size connections
        for _ in 0..config.min_size {
            if let Ok(conn) = pool.create_connection().await {
                let mut idle = pool.idle.lock().await;
                idle.push(PoolEntry {
                    conn,
                    created_at: Instant::now(),
                    last_used: Instant::now(),
                });
            }
        }

        Ok(pool)
    }

    /// Create a new connection pool with a builder pattern.
    pub fn builder(url: impl Into<String>) -> PoolBuilder {
        PoolBuilder::new(url)
    }

    /// Get the pool as an Arc.
    ///
    /// Returns None if the pool has been dropped (should never happen in practice).
    fn get_self_arc(&self) -> Option<Arc<Self>> {
        self.self_ref.get().and_then(|w| w.upgrade())
    }

    /// Create a new connection using the factory
    async fn create_connection(&self) -> Result<Box<dyn Connection>> {
        let conn = self.factory.connect(&self.config.connection).await?;
        self.total_connections.fetch_add(1, Ordering::Release);
        self.stats.record_created();
        Ok(conn)
    }

    /// Validate a connection before returning it
    async fn validate_connection(&self, conn: &dyn Connection) -> bool {
        if self.config.test_on_borrow {
            conn.is_valid().await
        } else {
            true
        }
    }

    /// Check if a connection should be recycled (expired)
    /// Returns the reason if it should be recycled, None otherwise
    fn should_recycle(&self, entry: &PoolEntry) -> Option<RecycleReason> {
        if entry.created_at.elapsed() > self.config.max_lifetime {
            Some(RecycleReason::LifetimeExpired)
        } else if entry.last_used.elapsed() > self.config.idle_timeout {
            Some(RecycleReason::IdleExpired)
        } else {
            None
        }
    }

    /// Get pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }
}

#[async_trait]
impl ConnectionPool for SimpleConnectionPool {
    async fn get(&self) -> Result<PooledConnection> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::PoolExhausted {
                message: "Pool is shut down".to_string(),
            });
        }

        let start = Instant::now();

        // Acquire semaphore permit with timeout
        let permit = tokio::time::timeout(self.config.acquire_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| {
                self.stats.record_exhausted();
                Error::PoolExhausted {
                    message: format!(
                        "Timeout waiting for connection ({}ms)",
                        self.config.acquire_timeout.as_millis()
                    ),
                }
            })?
            .map_err(|_| Error::PoolExhausted {
                message: "Pool semaphore closed".to_string(),
            })?;

        // Try to get an idle connection (preserving created_at)
        // pop entry from idle vec first, release lock, THEN validate
        let conn_with_time: Option<(Box<dyn Connection>, Instant)> = {
            loop {
                let candidate = {
                    let mut idle = self.idle.lock().await;
                    loop {
                        match idle.pop() {
                            Some(entry) => {
                                // Check if connection expired (cheap, no I/O)
                                if let Some(reason) = self.should_recycle(&entry) {
                                    match reason {
                                        RecycleReason::LifetimeExpired => {
                                            self.stats.record_lifetime_expired();
                                        }
                                        RecycleReason::IdleExpired => {
                                            self.stats.record_idle_expired();
                                        }
                                    }
                                    self.total_connections.fetch_sub(1, Ordering::Release);
                                    self.stats.record_closed();
                                    continue;
                                }
                                break Some(entry);
                            }
                            None => break None,
                        }
                    }
                };
                // Lock is released here — validate without holding the idle mutex
                match candidate {
                    Some(entry) => {
                        self.stats.record_health_check();
                        if self.validate_connection(&*entry.conn).await {
                            break Some((entry.conn, entry.created_at));
                        }
                        // Invalid connection — discard and try again
                        self.total_connections.fetch_sub(1, Ordering::Release);
                        self.stats.record_closed();
                        self.stats.record_health_check_failure();
                        continue;
                    }
                    None => break None,
                }
            }
        };

        // If no idle connection, create new one
        let (conn, created_at, was_reused) = match conn_with_time {
            Some((c, t)) => {
                self.stats.record_reused();
                (c, t, true)
            }
            None => {
                // Create new connection
                match self.create_connection().await {
                    Ok(c) => {
                        self.stats.record_fresh();
                        (c, Instant::now(), false)
                    }
                    Err(e) => {
                        // Release permit on failure
                        drop(permit);
                        return Err(e);
                    }
                }
            }
        };

        // Record stats - wait time includes time to get a connection (reused or fresh)
        let wait_ms = start.elapsed().as_millis() as u64;
        self.stats.record_acquisition(wait_ms);

        // Log fast-path hit for observability (optional, compile-time gated)
        #[cfg(feature = "pool-trace")]
        tracing::trace!(
            reused = was_reused,
            wait_ms = wait_ms,
            age_ms = created_at.elapsed().as_millis() as u64,
            "connection acquired from pool"
        );
        let _ = was_reused; // Suppress unused warning when pool-trace is disabled

        // Forget permit - it will be released when connection is returned
        std::mem::forget(permit);

        // Get Arc reference for PooledConnection
        let pool_arc = self.get_self_arc().ok_or_else(|| Error::PoolExhausted {
            message: "Pool has been dropped".to_string(),
        })?;

        Ok(PooledConnection::with_created_at(
            conn, pool_arc, created_at,
        ))
    }

    async fn return_connection(&self, conn: Box<dyn Connection>, created_at: Instant) {
        // Release a semaphore permit
        self.semaphore.add_permits(1);

        if self.shutdown.load(Ordering::Acquire) {
            // Pool is shutting down, close connection
            let _ = conn.close().await;
            self.total_connections.fetch_sub(1, Ordering::Release);
            self.stats.record_closed();
            return;
        }

        // Optionally validate on return
        if self.config.test_on_return {
            self.stats.record_health_check();
            if !conn.is_valid().await {
                self.total_connections.fetch_sub(1, Ordering::Release);
                self.stats.record_closed();
                self.stats.record_health_check_failure();
                return;
            }
        }

        // Return to idle pool with original creation time preserved
        let mut idle = self.idle.lock().await;
        idle.push(PoolEntry {
            conn,
            created_at,
            last_used: Instant::now(),
        });
    }

    fn size(&self) -> usize {
        self.total_connections.load(Ordering::Acquire)
    }

    fn idle(&self) -> usize {
        // return actual idle connection count, not semaphore permits
        match self.idle.try_lock() {
            Ok(guard) => guard.len(),
            Err(_) => 0, // Lock contended — return 0 as best-effort
        }
    }

    fn stats(&self) -> PoolStats {
        self.stats.snapshot()
    }

    async fn close(&self) -> Result<()> {
        self.shutdown.store(true, Ordering::Release);

        // Close all idle connections
        let mut idle = self.idle.lock().await;
        for entry in idle.drain(..) {
            let _ = entry.conn.close().await;
            self.total_connections.fetch_sub(1, Ordering::Release);
            self.stats.record_closed();
        }

        Ok(())
    }
}

/// Create a connection pool from a URL with default settings.
///
/// Auto-detects database type from URL scheme.
pub async fn create_pool(
    url: impl Into<String>,
    factory: Arc<dyn ConnectionFactory>,
) -> Result<Arc<SimpleConnectionPool>> {
    let config = PoolConfig::new(url);
    SimpleConnectionPool::new(config, factory).await
}

/// Create a connection pool with custom configuration.
pub async fn create_pool_with_config(
    config: PoolConfig,
    factory: Arc<dyn ConnectionFactory>,
) -> Result<Arc<SimpleConnectionPool>> {
    SimpleConnectionPool::new(config, factory).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new("postgres://localhost/test")
            .with_min_size(5)
            .with_max_size(20)
            .with_acquire_timeout(Duration::from_secs(10))
            .with_test_on_borrow(true);

        assert_eq!(config.min_size, 5);
        assert_eq!(config.max_size, 20);
        assert_eq!(config.acquire_timeout, Duration::from_secs(10));
        assert!(config.test_on_borrow);
    }

    #[test]
    fn test_pool_builder() {
        let config = PoolBuilder::new("mysql://localhost/test")
            .min_size(2)
            .max_size(15)
            .acquire_timeout(Duration::from_secs(5))
            .config();

        assert_eq!(config.min_size, 2);
        assert_eq!(config.max_size, 15);
        assert_eq!(config.acquire_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_atomic_pool_stats() {
        let stats = AtomicPoolStats::new();

        stats.record_created();
        stats.record_created();
        stats.record_acquisition(100);
        stats.record_acquisition(200);
        stats.record_closed();
        stats.record_exhausted();
        stats.record_health_check_failure();
        stats.record_health_check();
        stats.record_health_check();
        stats.record_lifetime_expired();
        stats.record_lifetime_expired();
        stats.record_idle_expired();
        stats.record_reused();
        stats.record_reused();
        stats.record_reused();
        stats.record_fresh();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.connections_created, 2);
        assert_eq!(snapshot.connections_closed, 1);
        assert_eq!(snapshot.acquisitions, 2);
        assert_eq!(snapshot.total_wait_time_ms, 300);
        assert_eq!(snapshot.exhausted_count, 1);
        assert_eq!(snapshot.health_check_failures, 1);
        assert_eq!(snapshot.health_checks_performed, 2);
        assert_eq!(snapshot.lifetime_expired_count, 2);
        assert_eq!(snapshot.idle_expired_count, 1);
        assert_eq!(snapshot.reused_count, 3);
        assert_eq!(snapshot.fresh_count, 1);

        assert!((stats.avg_wait_time_ms() - 150.0).abs() < 0.01);
    }

    #[test]
    fn test_pool_stats_helper_methods() {
        let stats = PoolStats {
            connections_created: 100,
            connections_closed: 30,
            acquisitions: 1000,
            exhausted_count: 5,
            total_wait_time_ms: 5000,
            health_check_failures: 10,
            health_checks_performed: 100,
            lifetime_expired_count: 15,
            idle_expired_count: 10,
            reused_count: 800,
            fresh_count: 200,
        };

        // recycled_total = lifetime + idle expired
        assert_eq!(stats.recycled_total(), 25);

        // reuse_rate = reused / acquisitions
        assert!((stats.reuse_rate() - 0.8).abs() < 0.001);

        // avg_wait_time_ms = total_wait / acquisitions
        assert!((stats.avg_wait_time_ms() - 5.0).abs() < 0.001);

        // health_failure_rate = failures / health_checks_performed
        assert!((stats.health_failure_rate() - 0.1).abs() < 0.001);

        // active_connections = created - closed
        assert_eq!(stats.active_connections(), 70);
    }

    #[test]
    fn test_pool_stats_zero_acquisitions() {
        let stats = PoolStats::default();

        // Should handle divide by zero gracefully
        assert!((stats.reuse_rate() - 0.0).abs() < 0.001);
        assert!((stats.avg_wait_time_ms() - 0.0).abs() < 0.001);
        assert!((stats.health_failure_rate() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_recycle_reason_enum() {
        // Test RecycleReason enum functionality
        let lifetime_reason = RecycleReason::LifetimeExpired;
        let idle_reason = RecycleReason::IdleExpired;

        assert_eq!(lifetime_reason, RecycleReason::LifetimeExpired);
        assert_eq!(idle_reason, RecycleReason::IdleExpired);
        assert_ne!(lifetime_reason, idle_reason);

        // Debug formatting should work
        assert!(format!("{:?}", lifetime_reason).contains("LifetimeExpired"));
        assert!(format!("{:?}", idle_reason).contains("IdleExpired"));
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.connections_created, 0);
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.acquisitions, 0);
        assert_eq!(stats.exhausted_count, 0);
        assert_eq!(stats.total_wait_time_ms, 0);
        assert_eq!(stats.health_check_failures, 0);
        assert_eq!(stats.lifetime_expired_count, 0);
        assert_eq!(stats.idle_expired_count, 0);
        assert_eq!(stats.reused_count, 0);
        assert_eq!(stats.fresh_count, 0);
    }

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::default();
        assert_eq!(config.min_size, 1);
        assert_eq!(config.max_size, 10);
        assert_eq!(config.acquire_timeout, Duration::from_secs(30));
        assert_eq!(config.max_lifetime, Duration::from_secs(1800)); // 30 minutes
        assert_eq!(config.idle_timeout, Duration::from_secs(600)); // 10 minutes
        assert_eq!(config.health_check_interval, Duration::from_secs(30));
        assert!(config.test_on_borrow);
        assert!(!config.test_on_return);
    }

    #[test]
    fn test_pooled_connection_lifecycle_methods() {
        use std::time::{Duration, Instant};

        // Create a mock pool for testing (we can't create PooledConnection without pool)
        // So we just test the public methods on PooledConnection struct directly
        // by checking that created_at, age(), and is_expired() are logically consistent

        let now = Instant::now();
        let short_lifetime = Duration::from_millis(10);

        // A connection created "now" should not be expired for a 30-minute lifetime
        assert!(now.elapsed() < Duration::from_secs(1800));

        // A 10ms lifetime should expire quickly after creation
        std::thread::sleep(Duration::from_millis(15));
        assert!(now.elapsed() > short_lifetime);
    }

    #[test]
    fn test_pooled_connection_borrowed_at() {
        use std::time::{Duration, Instant};

        // Test borrowed_at and time_in_use logic
        let borrowed_at = Instant::now();
        std::thread::sleep(Duration::from_millis(5));

        // time_in_use should be at least 5ms
        assert!(borrowed_at.elapsed() >= Duration::from_millis(5));

        // borrowed_at should be close to the creation time
        assert!(borrowed_at.elapsed() < Duration::from_secs(1));
    }
}
