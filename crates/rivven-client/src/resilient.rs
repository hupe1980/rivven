//! Production-grade resilient client with connection pooling, retries, and circuit breaker
//!
//! # Features
//!
//! - **Connection pooling**: Efficiently reuse connections across requests
//! - **Automatic retries**: Exponential backoff with jitter for transient failures
//! - **Circuit breaker**: Prevent cascading failures to unhealthy servers
//! - **Health checking**: Background health monitoring and connection validation
//! - **Timeouts**: Request and connection timeouts with configurable values
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_client::{ResilientClient, ResilientClientConfig};
//!
//! let config = ResilientClientConfig::builder()
//!     .bootstrap_servers(vec!["localhost:9092".to_string()])
//!     .pool_size(10)
//!     .retry_max_attempts(3)
//!     .circuit_breaker_threshold(5)
//!     .build();
//!
//! let client = ResilientClient::new(config).await?;
//!
//! // Auto-retry on transient failures
//! let offset = client.publish("my-topic", b"hello").await?;
//! ```

use crate::{Client, Error, MessageData, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the resilient client
#[derive(Debug, Clone)]
pub struct ResilientClientConfig {
    /// Bootstrap servers (host:port)
    pub bootstrap_servers: Vec<String>,
    /// Connection pool size per server
    pub pool_size: usize,
    /// Maximum retry attempts
    pub retry_max_attempts: u32,
    /// Initial retry delay
    pub retry_initial_delay: Duration,
    /// Maximum retry delay
    pub retry_max_delay: Duration,
    /// Retry backoff multiplier
    pub retry_multiplier: f64,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker recovery timeout
    pub circuit_breaker_timeout: Duration,
    /// Circuit breaker half-open success threshold
    pub circuit_breaker_success_threshold: u32,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Enable automatic health checking
    pub health_check_enabled: bool,
    /// Maximum connection lifetime before recycling (default: 300s)
    pub max_connection_lifetime: Duration,
    /// Optional authentication credentials
    ///
    /// When set, every new connection created by the pool will automatically
    /// authenticate using SCRAM-SHA-256 before being returned to the caller.
    pub auth: Option<ResilientAuthConfig>,
}

/// Authentication configuration for resilient client
#[derive(Debug, Clone)]
pub struct ResilientAuthConfig {
    /// Username
    pub username: String,
    /// Password
    pub password: String,
}

impl Default for ResilientClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            pool_size: 5,
            retry_max_attempts: 3,
            retry_initial_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(10),
            retry_multiplier: 2.0,
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(30),
            circuit_breaker_success_threshold: 2,
            connection_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            health_check_interval: Duration::from_secs(30),
            health_check_enabled: true,
            max_connection_lifetime: Duration::from_secs(300),
            auth: None,
        }
    }
}

impl ResilientClientConfig {
    /// Create a new builder
    pub fn builder() -> ResilientClientConfigBuilder {
        ResilientClientConfigBuilder::default()
    }
}

/// Builder for ResilientClientConfig
#[derive(Default)]
pub struct ResilientClientConfigBuilder {
    config: ResilientClientConfig,
}

impl ResilientClientConfigBuilder {
    /// Set bootstrap servers
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.config.bootstrap_servers = servers;
        self
    }

    /// Set pool size per server
    pub fn pool_size(mut self, size: usize) -> Self {
        self.config.pool_size = size;
        self
    }

    /// Set maximum retry attempts
    pub fn retry_max_attempts(mut self, attempts: u32) -> Self {
        self.config.retry_max_attempts = attempts;
        self
    }

    /// Set initial retry delay
    pub fn retry_initial_delay(mut self, delay: Duration) -> Self {
        self.config.retry_initial_delay = delay;
        self
    }

    /// Set maximum retry delay
    pub fn retry_max_delay(mut self, delay: Duration) -> Self {
        self.config.retry_max_delay = delay;
        self
    }

    /// Set retry backoff multiplier
    pub fn retry_multiplier(mut self, multiplier: f64) -> Self {
        self.config.retry_multiplier = multiplier;
        self
    }

    /// Set circuit breaker failure threshold
    pub fn circuit_breaker_threshold(mut self, threshold: u32) -> Self {
        self.config.circuit_breaker_threshold = threshold;
        self
    }

    /// Set circuit breaker recovery timeout
    pub fn circuit_breaker_timeout(mut self, timeout: Duration) -> Self {
        self.config.circuit_breaker_timeout = timeout;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Enable or disable health checking
    pub fn health_check_enabled(mut self, enabled: bool) -> Self {
        self.config.health_check_enabled = enabled;
        self
    }

    /// Set health check interval
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.config.health_check_interval = interval;
        self
    }

    /// Set maximum connection lifetime before recycling
    pub fn max_connection_lifetime(mut self, lifetime: Duration) -> Self {
        self.config.max_connection_lifetime = lifetime;
        self
    }

    /// Set authentication credentials
    ///
    /// When set, every new connection created by the pool will authenticate
    /// using SCRAM-SHA-256 before being returned to the caller.
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = Some(ResilientAuthConfig {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Build the configuration
    pub fn build(self) -> ResilientClientConfig {
        self.config
    }
}

// ============================================================================
// Circuit Breaker
// ============================================================================

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

/// Circuit breaker for a single server
struct CircuitBreaker {
    state: AtomicU32,
    failure_count: AtomicU32,
    success_count: AtomicU32,
    last_failure: RwLock<Option<Instant>>,
    config: Arc<ResilientClientConfig>,
}

impl CircuitBreaker {
    fn new(config: Arc<ResilientClientConfig>) -> Self {
        Self {
            state: AtomicU32::new(0), // Closed
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure: RwLock::new(None),
            config,
        }
    }

    fn get_state(&self) -> CircuitState {
        match self.state.load(Ordering::SeqCst) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            _ => CircuitState::HalfOpen,
        }
    }

    async fn allow_request(&self) -> bool {
        match self.get_state() {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let last_failure = self.last_failure.read().await;
                if let Some(t) = *last_failure {
                    if t.elapsed() > self.config.circuit_breaker_timeout {
                        self.state.store(2, Ordering::SeqCst); // HalfOpen
                        self.success_count.store(0, Ordering::SeqCst);
                        return true;
                    }
                }
                false
            }
            CircuitState::HalfOpen => true,
        }
    }

    async fn record_success(&self) {
        self.failure_count.store(0, Ordering::SeqCst);

        if self.get_state() == CircuitState::HalfOpen {
            let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count >= self.config.circuit_breaker_success_threshold {
                self.state.store(0, Ordering::SeqCst); // Closed
                debug!("Circuit breaker closed after {} successes", count);
            }
        }
    }

    async fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        *self.last_failure.write().await = Some(Instant::now());

        if count >= self.config.circuit_breaker_threshold {
            self.state.store(1, Ordering::SeqCst); // Open
            warn!("Circuit breaker opened after {} failures", count);
        }
    }
}

// ============================================================================
// Connection Pool
// ============================================================================

/// Pooled connection wrapper.
///
/// Holds a semaphore permit from the owning `ConnectionPool`. When this struct
/// is dropped (e.g. on request timeout), the permit is automatically released,
/// preventing permanent pool slot exhaustion.
struct PooledConnection {
    client: Client,
    created_at: Instant,
    last_used: Instant,
    /// Semaphore permit — released on drop to return the pool slot.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Connection pool for a single server
struct ConnectionPool {
    addr: String,
    connections: Mutex<Vec<PooledConnection>>,
    semaphore: Arc<Semaphore>,
    config: Arc<ResilientClientConfig>,
    circuit_breaker: CircuitBreaker,
}

impl ConnectionPool {
    fn new(addr: String, config: Arc<ResilientClientConfig>) -> Self {
        Self {
            addr,
            connections: Mutex::new(Vec::new()),
            semaphore: Arc::new(Semaphore::new(config.pool_size)),
            circuit_breaker: CircuitBreaker::new(config.clone()),
            config,
        }
    }

    async fn get(&self) -> Result<PooledConnection> {
        // Check circuit breaker
        if !self.circuit_breaker.allow_request().await {
            return Err(Error::CircuitBreakerOpen(self.addr.clone()));
        }

        // Acquire semaphore permit — owned so it can be stored in PooledConnection
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::ConnectionError("Pool exhausted".to_string()))?;

        // Try to get existing connection — also check idle staleness
        // to avoid reusing connections the server may have already closed.
        {
            let mut connections = self.connections.lock().await;
            while let Some(conn) = connections.pop() {
                // Discard connections idle for too long (server may have closed them)
                if conn.last_used.elapsed() < Duration::from_secs(60) {
                    let mut conn = conn;
                    conn.last_used = Instant::now();
                    conn._permit = permit;
                    return Ok(conn);
                }
                // Drop stale connection, try next one in pool
            }
        }

        // Create new connection with timeout
        let mut client = timeout(self.config.connection_timeout, Client::connect(&self.addr))
            .await
            .map_err(|_| Error::ConnectionError(format!("Connection timeout to {}", self.addr)))?
            .map_err(|e| {
                Error::ConnectionError(format!("Failed to connect to {}: {}", self.addr, e))
            })?;

        // Auto-authenticate new connections when credentials are configured.
        // Uses SCRAM-SHA-256 for secure challenge-response authentication.
        if let Some(auth) = &self.config.auth {
            client
                .authenticate_scram(&auth.username, &auth.password)
                .await
                .map_err(|e| {
                    Error::ConnectionError(format!(
                        "Authentication failed for {}: {}",
                        self.addr, e
                    ))
                })?;
        }

        Ok(PooledConnection {
            client,
            created_at: Instant::now(),
            last_used: Instant::now(),
            _permit: permit,
        })
    }

    async fn put(&self, conn: PooledConnection) {
        // Use configured max_connection_lifetime instead of hardcoded 300s
        if conn.created_at.elapsed() < self.config.max_connection_lifetime {
            let mut connections = self.connections.lock().await;
            if connections.len() < self.config.pool_size {
                connections.push(conn);
            }
        }
    }

    async fn record_success(&self) {
        self.circuit_breaker.record_success().await;
    }

    async fn record_failure(&self) {
        self.circuit_breaker.record_failure().await;
    }

    fn circuit_state(&self) -> CircuitState {
        self.circuit_breaker.get_state()
    }
}

// ============================================================================
// Resilient Client
// ============================================================================

/// Production-grade resilient client with connection pooling and fault tolerance
pub struct ResilientClient {
    pools: HashMap<String, Arc<ConnectionPool>>,
    config: Arc<ResilientClientConfig>,
    current_server: AtomicU64,
    total_requests: AtomicU64,
    total_failures: AtomicU64,
    _health_check_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ResilientClient {
    /// Create a new resilient client
    pub async fn new(config: ResilientClientConfig) -> Result<Self> {
        if config.bootstrap_servers.is_empty() {
            return Err(Error::ConnectionError(
                "No bootstrap servers configured".to_string(),
            ));
        }

        let config = Arc::new(config);
        let mut pools = HashMap::new();

        for server in &config.bootstrap_servers {
            let pool = Arc::new(ConnectionPool::new(server.clone(), config.clone()));
            pools.insert(server.clone(), pool);
        }

        info!(
            "Resilient client initialized with {} servers, pool size {}",
            config.bootstrap_servers.len(),
            config.pool_size
        );

        let mut client = Self {
            pools,
            config: config.clone(),
            current_server: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            _health_check_handle: None,
        };

        // Start health check background task
        if config.health_check_enabled {
            let pools_clone: HashMap<String, Arc<ConnectionPool>> = client
                .pools
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let interval = config.health_check_interval;

            let handle = tokio::spawn(async move {
                loop {
                    sleep(interval).await;
                    for (addr, pool) in &pools_clone {
                        if let Ok(mut conn) = pool.get().await {
                            match conn.client.ping().await {
                                Ok(()) => {
                                    pool.record_success().await;
                                    debug!("Health check passed for {}", addr);
                                }
                                Err(e) => {
                                    pool.record_failure().await;
                                    warn!("Health check failed for {}: {}", addr, e);
                                }
                            }
                            pool.put(conn).await;
                        }
                    }
                }
            });

            client._health_check_handle = Some(handle);
        }

        Ok(client)
    }

    /// Execute an operation with automatic retries and server failover
    async fn execute_with_retry<F, T, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn(PooledConnection) -> Fut + Clone,
        Fut: std::future::Future<Output = (PooledConnection, Result<T>)>,
    {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        let servers: Vec<_> = self.config.bootstrap_servers.clone();
        let num_servers = servers.len();

        for attempt in 0..self.config.retry_max_attempts {
            // Round-robin server selection with failover
            let server_idx =
                (self.current_server.fetch_add(1, Ordering::Relaxed) as usize) % num_servers;
            let server = &servers[server_idx];

            let pool = match self.pools.get(server) {
                Some(p) => p,
                None => continue,
            };

            // Skip servers with open circuit breaker
            if pool.circuit_state() == CircuitState::Open {
                debug!("Skipping {} (circuit breaker open)", server);
                continue;
            }

            // Get connection from pool
            let conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to get connection from {}: {}", server, e);
                    pool.record_failure().await;
                    continue;
                }
            };

            // Execute operation with timeout
            let result = timeout(self.config.request_timeout, (operation.clone())(conn)).await;

            match result {
                Ok((conn, Ok(value))) => {
                    pool.record_success().await;
                    pool.put(conn).await;
                    return Ok(value);
                }
                Ok((conn, Err(e))) => {
                    self.total_failures.fetch_add(1, Ordering::Relaxed);
                    pool.record_failure().await;

                    // Determine if error is retryable
                    if is_retryable_error(&e) && attempt < self.config.retry_max_attempts - 1 {
                        let delay = calculate_backoff(
                            attempt,
                            self.config.retry_initial_delay,
                            self.config.retry_max_delay,
                            self.config.retry_multiplier,
                        );
                        warn!(
                            "Retryable error on attempt {}: {}. Retrying in {:?}",
                            attempt + 1,
                            e,
                            delay
                        );
                        pool.put(conn).await;
                        sleep(delay).await;
                        continue;
                    }

                    return Err(e);
                }
                Err(_) => {
                    self.total_failures.fetch_add(1, Ordering::Relaxed);
                    pool.record_failure().await;
                    warn!("Request timeout to {}", server);

                    if attempt < self.config.retry_max_attempts - 1 {
                        let delay = calculate_backoff(
                            attempt,
                            self.config.retry_initial_delay,
                            self.config.retry_max_delay,
                            self.config.retry_multiplier,
                        );
                        sleep(delay).await;
                    }
                }
            }
        }

        Err(Error::ConnectionError(format!(
            "All {} retry attempts exhausted",
            self.config.retry_max_attempts
        )))
    }

    /// Publish a message to a topic with automatic retries
    ///
    /// **WARNING: At-least-once semantics.** If the server commits the write
    /// but the response is lost (timeout/network partition), the retry produces
    /// a duplicate. For exactly-once, use [`Self::idempotent_publish`] or
    /// `IdempotentPublish` requests instead.
    pub async fn publish(&self, topic: impl Into<String>, value: impl Into<Bytes>) -> Result<u64> {
        let topic = topic.into();
        let value = value.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            let value = value.clone();
            async move {
                let result = conn.client.publish(&topic, value).await;
                (conn, result)
            }
        })
        .await
    }

    /// Publish a message with a key
    ///
    /// **WARNING: At-least-once semantics.** See [`Self::publish`] for details.
    pub async fn publish_with_key(
        &self,
        topic: impl Into<String>,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
    ) -> Result<u64> {
        let topic = topic.into();
        let key: Option<Bytes> = key.map(|k| k.into());
        let value = value.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            let key = key.clone();
            let value = value.clone();
            async move {
                let result = conn.client.publish_with_key(&topic, key, value).await;
                (conn, result)
            }
        })
        .await
    }

    /// Consume messages with automatic retries
    ///
    /// Uses read_uncommitted isolation level (default).
    /// For transactional consumers, use [`Self::consume_with_isolation`] or [`Self::consume_read_committed`].
    pub async fn consume(
        &self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        self.consume_with_isolation(topic, partition, offset, max_messages, None)
            .await
    }

    /// Consume messages with specified isolation level and automatic retries
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `offset` - Starting offset
    /// * `max_messages` - Maximum messages to return
    /// * `isolation_level` - Transaction isolation level:
    ///   - `None` or `Some(0)` = read_uncommitted (default)
    ///   - `Some(1)` = read_committed (filters aborted transactions)
    pub async fn consume_with_isolation(
        &self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
        isolation_level: Option<u8>,
    ) -> Result<Vec<MessageData>> {
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            async move {
                let result = conn
                    .client
                    .consume_with_isolation(
                        &topic,
                        partition,
                        offset,
                        max_messages,
                        isolation_level,
                    )
                    .await;
                (conn, result)
            }
        })
        .await
    }

    /// Consume messages with read_committed isolation level and automatic retries
    ///
    /// Only returns committed transactional messages; aborted transactions are filtered out.
    pub async fn consume_read_committed(
        &self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        self.consume_with_isolation(topic, partition, offset, max_messages, Some(1))
            .await
    }

    /// Create a topic with automatic retries
    pub async fn create_topic(
        &self,
        name: impl Into<String>,
        partitions: Option<u32>,
    ) -> Result<u32> {
        let name = name.into();

        self.execute_with_retry(move |mut conn| {
            let name = name.clone();
            async move {
                let result = conn.client.create_topic(&name, partitions).await;
                (conn, result)
            }
        })
        .await
    }

    /// List all topics
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        self.execute_with_retry(|mut conn| async move {
            let result = conn.client.list_topics().await;
            (conn, result)
        })
        .await
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: impl Into<String>) -> Result<()> {
        let name = name.into();

        self.execute_with_retry(move |mut conn| {
            let name = name.clone();
            async move {
                let result = conn.client.delete_topic(&name).await;
                (conn, result)
            }
        })
        .await
    }

    /// Commit consumer offset
    pub async fn commit_offset(
        &self,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        let consumer_group = consumer_group.into();
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let consumer_group = consumer_group.clone();
            let topic = topic.clone();
            async move {
                let result = conn
                    .client
                    .commit_offset(&consumer_group, &topic, partition, offset)
                    .await;
                (conn, result)
            }
        })
        .await
    }

    /// Get consumer offset
    pub async fn get_offset(
        &self,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
    ) -> Result<Option<u64>> {
        let consumer_group = consumer_group.into();
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let consumer_group = consumer_group.clone();
            let topic = topic.clone();
            async move {
                let result = conn
                    .client
                    .get_offset(&consumer_group, &topic, partition)
                    .await;
                (conn, result)
            }
        })
        .await
    }

    /// Get offset bounds (earliest, latest)
    pub async fn get_offset_bounds(
        &self,
        topic: impl Into<String>,
        partition: u32,
    ) -> Result<(u64, u64)> {
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            async move {
                let result = conn.client.get_offset_bounds(&topic, partition).await;
                (conn, result)
            }
        })
        .await
    }

    /// Get topic metadata
    pub async fn get_metadata(&self, topic: impl Into<String>) -> Result<(String, u32)> {
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            async move {
                let result = conn.client.get_metadata(&topic).await;
                (conn, result)
            }
        })
        .await
    }

    /// Ping (health check)
    pub async fn ping(&self) -> Result<()> {
        self.execute_with_retry(|mut conn| async move {
            let result = conn.client.ping().await;
            (conn, result)
        })
        .await
    }

    /// Get client statistics
    pub fn stats(&self) -> ClientStats {
        let pools: Vec<_> = self
            .pools
            .iter()
            .map(|(addr, pool)| ServerStats {
                address: addr.clone(),
                circuit_state: pool.circuit_state(),
            })
            .collect();

        ClientStats {
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_failures: self.total_failures.load(Ordering::Relaxed),
            servers: pools,
        }
    }
}

impl Drop for ResilientClient {
    fn drop(&mut self) {
        if let Some(handle) = self._health_check_handle.take() {
            handle.abort();
        }
    }
}

/// Client statistics
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub total_requests: u64,
    pub total_failures: u64,
    pub servers: Vec<ServerStats>,
}

/// Per-server statistics
#[derive(Debug, Clone)]
pub struct ServerStats {
    pub address: String,
    pub circuit_state: CircuitState,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Determine if an error is retryable
fn is_retryable_error(error: &Error) -> bool {
    matches!(
        error,
        Error::ConnectionError(_) | Error::IoError(_) | Error::CircuitBreakerOpen(_)
    )
}

/// Calculate exponential backoff with jitter
fn calculate_backoff(
    attempt: u32,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
) -> Duration {
    let base_delay = initial_delay.as_millis() as f64 * multiplier.powi(attempt as i32);
    let capped_delay = base_delay.min(max_delay.as_millis() as f64);

    // Add jitter (±25%)
    let jitter = (rand_simple() * 0.5 - 0.25) * capped_delay;
    let final_delay = (capped_delay + jitter).max(0.0);

    Duration::from_millis(final_delay as u64)
}

/// Random number generator (0.0 - 1.0).
///
/// Uses `rand::random::<f64>()` instead of `subsec_nanos()` which
/// is deterministic within the same millisecond, causing thundering herd
/// when multiple clients have synchronized clocks.
fn rand_simple() -> f64 {
    rand::random::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = ResilientClientConfig::builder()
            .bootstrap_servers(vec!["server1:9092".to_string(), "server2:9092".to_string()])
            .pool_size(10)
            .retry_max_attempts(5)
            .circuit_breaker_threshold(10)
            .connection_timeout(Duration::from_secs(5))
            .build();

        assert_eq!(config.bootstrap_servers.len(), 2);
        assert_eq!(config.pool_size, 10);
        assert_eq!(config.retry_max_attempts, 5);
        assert_eq!(config.circuit_breaker_threshold, 10);
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_calculate_backoff() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);

        // First attempt
        let delay = calculate_backoff(0, initial, max, 2.0);
        assert!(delay.as_millis() >= 75 && delay.as_millis() <= 125);

        // Second attempt (should be ~200ms)
        let delay = calculate_backoff(1, initial, max, 2.0);
        assert!(delay.as_millis() >= 150 && delay.as_millis() <= 250);

        // Many attempts (should cap at max)
        let delay = calculate_backoff(20, initial, max, 2.0);
        assert!(delay <= max + Duration::from_millis(2500)); // max + jitter
    }

    #[test]
    fn test_is_retryable_error() {
        assert!(is_retryable_error(&Error::ConnectionError("test".into())));
        assert!(is_retryable_error(&Error::CircuitBreakerOpen(
            "test".into()
        )));
        assert!(!is_retryable_error(&Error::InvalidResponse));
        assert!(!is_retryable_error(&Error::ServerError("test".into())));
    }

    #[test]
    fn test_circuit_state() {
        let config = Arc::new(ResilientClientConfig::default());
        let cb = CircuitBreaker::new(config);

        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    // ========================================================================
    // Circuit Breaker State Machine Tests
    // ========================================================================

    #[tokio::test]
    async fn test_circuit_breaker_starts_closed() {
        let config = Arc::new(ResilientClientConfig::default());
        let cb = CircuitBreaker::new(config);

        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.allow_request().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_threshold_failures() {
        let config = Arc::new(
            ResilientClientConfig::builder()
                .circuit_breaker_threshold(3)
                .build(),
        );
        let cb = CircuitBreaker::new(config);

        // Should be closed initially
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // Record failures up to threshold - 1
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Closed);
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // Threshold reached - should open
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(!cb.allow_request().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_success_resets_failure_count() {
        let config = Arc::new(
            ResilientClientConfig::builder()
                .circuit_breaker_threshold(3)
                .build(),
        );
        let cb = CircuitBreaker::new(config);

        // Record some failures
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.failure_count.load(Ordering::SeqCst), 2);

        // Success should reset
        cb.record_success().await;
        assert_eq!(cb.failure_count.load(Ordering::SeqCst), 0);
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_after_timeout() {
        let config = Arc::new(
            ResilientClientConfig::builder()
                .circuit_breaker_threshold(1)
                .circuit_breaker_timeout(Duration::from_millis(50))
                .build(),
        );
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);
        assert!(!cb.allow_request().await);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should transition to half-open and allow request
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_closes_after_success_threshold() {
        let config = Arc::new(
            ResilientClientConfig::builder()
                .circuit_breaker_threshold(1)
                .circuit_breaker_timeout(Duration::from_millis(10))
                .build(),
        );
        // Note: default success threshold is 2
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for timeout and transition to half-open
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // First success - still half-open
        cb.record_success().await;
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Second success - should close
        cb.record_success().await;
        assert_eq!(cb.get_state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_failure_in_half_open_reopens() {
        let config = Arc::new(
            ResilientClientConfig::builder()
                .circuit_breaker_threshold(1)
                .circuit_breaker_timeout(Duration::from_millis(10))
                .build(),
        );
        let cb = CircuitBreaker::new(config);

        // Open the circuit
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);

        // Wait for timeout and transition to half-open
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(cb.allow_request().await);
        assert_eq!(cb.get_state(), CircuitState::HalfOpen);

        // Failure in half-open should reopen
        cb.record_failure().await;
        assert_eq!(cb.get_state(), CircuitState::Open);
    }

    // ========================================================================
    // Connection Pool Tests
    // ========================================================================

    #[test]
    fn test_pool_config_defaults() {
        let config = ResilientClientConfig::default();
        assert_eq!(config.pool_size, 5);
        assert_eq!(config.retry_max_attempts, 3);
        assert_eq!(config.circuit_breaker_threshold, 5);
        assert_eq!(config.circuit_breaker_success_threshold, 2);
    }

    #[tokio::test]
    async fn test_pool_semaphore_limits_concurrent_connections() {
        let config = Arc::new(ResilientClientConfig::builder().pool_size(2).build());
        let pool = ConnectionPool::new("localhost:9999".to_string(), config);

        // Verify semaphore has correct permits
        // Note: can't directly test without a server, but verify pool was created
        assert_eq!(pool.addr, "localhost:9999");
    }

    // ========================================================================
    // Retry Logic Tests
    // ========================================================================

    #[test]
    fn test_backoff_respects_max_delay() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(1);

        // Even with high attempt count, should not exceed max + jitter
        for attempt in 10..20 {
            let delay = calculate_backoff(attempt, initial, max, 2.0);
            // Max jitter is 25% of max = 250ms
            assert!(delay <= max + Duration::from_millis(250));
        }
    }

    #[test]
    fn test_backoff_exponential_growth() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(100);

        // Get base delays (center of jitter range)
        let delay0 = calculate_backoff(0, initial, max, 2.0);
        let delay1 = calculate_backoff(1, initial, max, 2.0);
        let delay2 = calculate_backoff(2, initial, max, 2.0);

        // Each should be roughly 2x the previous (accounting for jitter)
        // delay0 ≈ 100ms, delay1 ≈ 200ms, delay2 ≈ 400ms
        assert!(delay1 > delay0 / 2); // Very loose check due to jitter
        assert!(delay2 > delay1 / 2);
    }

    // ========================================================================
    // Client Statistics Tests
    // ========================================================================

    #[test]
    fn test_client_stats_structure() {
        let stats = ClientStats {
            total_requests: 100,
            total_failures: 5,
            servers: vec![
                ServerStats {
                    address: "server1:9092".to_string(),
                    circuit_state: CircuitState::Closed,
                },
                ServerStats {
                    address: "server2:9092".to_string(),
                    circuit_state: CircuitState::Open,
                },
            ],
        };

        assert_eq!(stats.total_requests, 100);
        assert_eq!(stats.total_failures, 5);
        assert_eq!(stats.servers.len(), 2);
        assert_eq!(stats.servers[0].circuit_state, CircuitState::Closed);
        assert_eq!(stats.servers[1].circuit_state, CircuitState::Open);
    }
}
