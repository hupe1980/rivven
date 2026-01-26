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

/// Pooled connection wrapper
struct PooledConnection {
    client: Client,
    created_at: Instant,
    last_used: Instant,
}

/// Connection pool for a single server
struct ConnectionPool {
    addr: String,
    connections: Mutex<Vec<PooledConnection>>,
    semaphore: Semaphore,
    config: Arc<ResilientClientConfig>,
    circuit_breaker: CircuitBreaker,
}

impl ConnectionPool {
    fn new(addr: String, config: Arc<ResilientClientConfig>) -> Self {
        Self {
            addr,
            connections: Mutex::new(Vec::new()),
            semaphore: Semaphore::new(config.pool_size),
            circuit_breaker: CircuitBreaker::new(config.clone()),
            config,
        }
    }

    async fn get(&self) -> Result<PooledConnection> {
        // Check circuit breaker
        if !self.circuit_breaker.allow_request().await {
            return Err(Error::CircuitBreakerOpen(self.addr.clone()));
        }

        // Acquire semaphore permit
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| Error::ConnectionError("Pool exhausted".to_string()))?;

        // Try to get existing connection
        {
            let mut connections = self.connections.lock().await;
            if let Some(mut conn) = connections.pop() {
                conn.last_used = Instant::now();
                return Ok(conn);
            }
        }

        // Create new connection with timeout
        let client = timeout(self.config.connection_timeout, Client::connect(&self.addr))
            .await
            .map_err(|_| Error::ConnectionError(format!("Connection timeout to {}", self.addr)))?
            .map_err(|e| {
                Error::ConnectionError(format!("Failed to connect to {}: {}", self.addr, e))
            })?;

        Ok(PooledConnection {
            client,
            created_at: Instant::now(),
            last_used: Instant::now(),
        })
    }

    async fn put(&self, conn: PooledConnection) {
        // Only return if connection is healthy
        if conn.created_at.elapsed() < Duration::from_secs(300) {
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
    pub async fn consume(
        &self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        let topic = topic.into();

        self.execute_with_retry(move |mut conn| {
            let topic = topic.clone();
            async move {
                let result = conn
                    .client
                    .consume(&topic, partition, offset, max_messages)
                    .await;
                (conn, result)
            }
        })
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

/// Simple random number generator (0.0 - 1.0)
fn rand_simple() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
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
}
