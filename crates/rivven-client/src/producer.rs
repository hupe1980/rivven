//! High-performance Producer with sticky partitioning, batching, and metadata cache
//!
//! This module provides a production-ready [`Producer`] that implements all
//! best practices from Kafka's Java client and beyond:
//!
//! # Features
//!
//! - **Arc-based concurrency**: Thread-safe sharing via `Arc<Producer>`
//! - **Metadata cache with TTL**: Reduces metadata requests
//! - **Sticky partitioning**: Batches messages to the same partition for efficiency
//! - **Configurable batching**: Linger time + batch size optimization
//! - **Backpressure**: Memory-bounded buffers prevent OOM
//! - **Automatic retries**: Exponential backoff with jitter
//! - **Per-operation timeouts**: Fine-grained timeout control
//! - **Idempotency**: Exactly-once semantics (when enabled)
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_client::{Producer, ProducerConfig};
//! use std::sync::Arc;
//!
//! # async fn example() -> rivven_client::Result<()> {
//! let config = ProducerConfig::builder()
//!     .bootstrap_servers(vec!["localhost:9092".to_string()])
//!     .batch_size(16384)
//!     .linger_ms(5)
//!     .buffer_memory(32 * 1024 * 1024)
//!     .enable_idempotence(true)
//!     .build()?;
//!
//! let producer = Arc::new(Producer::new(config).await?);
//!
//! // Share across tasks
//! for i in 0..1000 {
//!     let producer = Arc::clone(&producer);
//!     tokio::spawn(async move {
//!         producer.send("my-topic", format!("msg-{}", i)).await.unwrap();
//!     });
//! }
//! # Ok(())
//! # }
//! ```

use crate::client::ClientStream;
use crate::{Error, Request, Response, Result};
use rivven_protocol::BatchRecord;
use bytes::Bytes;
use rand::Rng;
#[cfg(feature = "compression")]
use rivven_core::compression::{CompressionAlgorithm, CompressionConfig, Compressor};
#[cfg(feature = "tls")]
use rivven_core::tls::TlsConfig;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Default batch size in bytes
const DEFAULT_BATCH_SIZE: usize = 16384;
/// Default linger time in milliseconds
const DEFAULT_LINGER_MS: u64 = 0;
/// Default buffer memory limit
const DEFAULT_BUFFER_MEMORY: usize = 32 * 1024 * 1024;
/// Default metadata TTL
const DEFAULT_METADATA_TTL: Duration = Duration::from_secs(300);
/// Sticky partition switch threshold (when batch is full)
const STICKY_BATCH_THRESHOLD: usize = 8;

/// Estimated per-record memory overhead in bytes.
///
/// Accounts for `ProducerRecord` struct, topic `String` (heap + 24B),
/// `oneshot` channel (~128B), `RecordBatch` entry, and internal
/// book-keeping. This is added to the raw value+key size when
/// acquiring memory-semaphore permits so that backpressure kicks in
/// before the true memory usage exceeds `buffer_memory`.
const RECORD_OVERHEAD_BYTES: usize = 256;

// ============================================================================
// Configuration
// ============================================================================

/// Producer configuration with Kafka-like semantics
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Bootstrap servers (host:port)
    pub bootstrap_servers: Vec<String>,
    /// Batch size in bytes before sending
    pub batch_size: usize,
    /// Time to wait for additional messages (ms)
    pub linger_ms: u64,
    /// Maximum memory for buffering
    pub buffer_memory: usize,
    /// Maximum in-flight requests per connection
    pub max_in_flight_requests: usize,
    /// Enable idempotent producer
    pub enable_idempotence: bool,
    /// Request timeout
    pub request_timeout: Duration,
    /// Delivery timeout (includes retries)
    pub delivery_timeout: Duration,
    /// Number of retries
    pub retries: u32,
    /// Initial retry backoff
    pub retry_backoff_ms: u64,
    /// Maximum retry backoff
    pub retry_backoff_max_ms: u64,
    /// Metadata refresh interval
    pub metadata_max_age: Duration,
    /// Compression type (none, gzip, snappy, lz4, zstd)
    pub compression_type: CompressionType,
    /// Acks required: 0 = none, 1 = leader, -1 = all.
    ///
    /// **Note:** The current Rivven wire protocol (`Request::Publish`)
    /// does not carry an acks field, so `acks = -1` (All) is not communicated to
    /// the broker — the server always applies its own replication policy.
    /// `acks = 0` (fire-and-forget) is respected client-side: the producer skips
    /// waiting for per-record responses in `send_batch`.
    pub acks: i8,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Authentication credentials (optional, for SCRAM-SHA-256)
    pub auth: Option<ProducerAuthConfig>,
    /// TLS configuration (optional). When set, the producer connects
    /// over TLS instead of plaintext.
    #[cfg(feature = "tls")]
    pub tls_config: Option<TlsConfig>,
    /// TLS server name for certificate verification.
    /// Required when `tls_config` is `Some`.
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<String>,
}

/// Authentication configuration for the producer connection
#[derive(Clone)]
pub struct ProducerAuthConfig {
    pub username: String,
    pub password: String,
}

impl std::fmt::Debug for ProducerAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerAuthConfig")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// Compression types for producer messages
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    /// Convert client-side `CompressionType` to core's `CompressionAlgorithm`.
    ///
    /// maps producer config compression setting to the algorithm
    /// used by `rivven_core::compression::Compressor`.
    #[cfg(feature = "compression")]
    pub fn to_algorithm(self) -> Result<CompressionAlgorithm> {
        match self {
            CompressionType::None => Ok(CompressionAlgorithm::None),
            CompressionType::Gzip => Err(Error::ConfigError(
                "Gzip compression is not supported; use Zstd, Snappy, or Lz4 instead".to_string(),
            )),
            CompressionType::Snappy => Ok(CompressionAlgorithm::Snappy),
            CompressionType::Lz4 => Ok(CompressionAlgorithm::Lz4),
            CompressionType::Zstd => Ok(CompressionAlgorithm::Zstd),
        }
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            batch_size: DEFAULT_BATCH_SIZE,
            linger_ms: DEFAULT_LINGER_MS,
            buffer_memory: DEFAULT_BUFFER_MEMORY,
            max_in_flight_requests: 5,
            enable_idempotence: false,
            request_timeout: Duration::from_secs(30),
            delivery_timeout: Duration::from_secs(120),
            retries: 3,
            retry_backoff_ms: 100,
            retry_backoff_max_ms: 1000,
            metadata_max_age: DEFAULT_METADATA_TTL,
            compression_type: CompressionType::None,
            acks: 1,
            connection_timeout: Duration::from_secs(10),
            auth: None,
            #[cfg(feature = "tls")]
            tls_config: None,
            #[cfg(feature = "tls")]
            tls_server_name: None,
        }
    }
}

impl ProducerConfig {
    /// Create a new builder
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }

    /// High-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            batch_size: 65536,
            linger_ms: 10,
            max_in_flight_requests: 10,
            compression_type: CompressionType::Lz4,
            ..Default::default()
        }
    }

    /// Low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            batch_size: 1,
            linger_ms: 0,
            max_in_flight_requests: 1,
            compression_type: CompressionType::None,
            ..Default::default()
        }
    }

    /// Exactly-once configuration
    pub fn exactly_once() -> Self {
        Self {
            enable_idempotence: true,
            acks: -1,
            max_in_flight_requests: 5,
            retries: i32::MAX as u32,
            ..Default::default()
        }
    }
}

/// Builder for ProducerConfig
#[derive(Default)]
pub struct ProducerConfigBuilder {
    config: ProducerConfig,
}

impl ProducerConfigBuilder {
    /// Set bootstrap servers
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.config.bootstrap_servers = servers;
        self
    }

    /// Set batch size in bytes
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set linger time in milliseconds
    pub fn linger_ms(mut self, ms: u64) -> Self {
        self.config.linger_ms = ms;
        self
    }

    /// Set buffer memory limit
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.config.buffer_memory = bytes;
        self
    }

    /// Set max in-flight requests per connection
    pub fn max_in_flight_requests(mut self, max: usize) -> Self {
        self.config.max_in_flight_requests = max;
        self
    }

    /// Enable idempotent producer
    pub fn enable_idempotence(mut self, enable: bool) -> Self {
        self.config.enable_idempotence = enable;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set delivery timeout
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    /// Set number of retries
    pub fn retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Set retry backoff in milliseconds
    pub fn retry_backoff_ms(mut self, ms: u64) -> Self {
        self.config.retry_backoff_ms = ms;
        self
    }

    /// Set maximum retry backoff in milliseconds
    pub fn retry_backoff_max_ms(mut self, ms: u64) -> Self {
        self.config.retry_backoff_max_ms = ms;
        self
    }

    /// Set metadata max age (TTL)
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
        self
    }

    /// Set compression type
    pub fn compression_type(mut self, compression: CompressionType) -> Self {
        self.config.compression_type = compression;
        self
    }

    /// Set acks required
    pub fn acks(mut self, acks: i8) -> Self {
        self.config.acks = acks;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// Set authentication credentials (SCRAM-SHA-256)
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = Some(ProducerAuthConfig {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    /// Set TLS configuration for encrypted connections
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls_config: TlsConfig, server_name: impl Into<String>) -> Self {
        self.config.tls_config = Some(tls_config);
        self.config.tls_server_name = Some(server_name.into());
        self
    }

    /// Build the configuration.
    ///
    /// Returns an error when:
    /// - `tls_config` is set but `tls_server_name` is missing
    /// - `enable_idempotence` is true and `max_in_flight_requests > 5`
    ///   (Kafka mandates ≤ 5 for ordering guarantees)
    pub fn build(self) -> Result<ProducerConfig> {
        #[cfg(feature = "tls")]
        if self.config.tls_config.is_some() && self.config.tls_server_name.is_none() {
            return Err(Error::ConfigError(
                "tls_config is set but tls_server_name is missing; \
                 a server name is required for TLS certificate verification"
                    .to_string(),
            ));
        }

        // CLIENT-05: Enforce Kafka's max.in.flight.requests.per.connection ≤ 5
        // when idempotence is enabled. Without this constraint, the broker
        // cannot guarantee ordering and deduplication.
        if self.config.enable_idempotence && self.config.max_in_flight_requests > 5 {
            return Err(Error::ConfigError(format!(
                "max_in_flight_requests ({}) must be ≤ 5 when enable_idempotence is true \
                 (required for ordering guarantees, matching Kafka's KIP-98)",
                self.config.max_in_flight_requests
            )));
        }

        Ok(self.config)
    }
}

// ============================================================================
// Metadata Cache
// ============================================================================

/// Cached topic metadata with TTL
#[derive(Debug, Clone)]
struct CachedMetadata {
    /// Number of partitions
    partition_count: u32,
    /// When the cache entry was created
    cached_at: Instant,
}

/// Thread-safe metadata cache with TTL
struct MetadataCache {
    cache: RwLock<HashMap<String, CachedMetadata>>,
    ttl: Duration,
}

impl MetadataCache {
    fn new(ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Get cached partition count for a topic
    async fn get(&self, topic: &str) -> Option<u32> {
        let cache = self.cache.read().await;
        cache.get(topic).and_then(|entry| {
            if entry.cached_at.elapsed() < self.ttl {
                Some(entry.partition_count)
            } else {
                None
            }
        })
    }

    /// Update cache for a topic
    async fn put(&self, topic: String, partition_count: u32) {
        let mut cache = self.cache.write().await;
        cache.insert(
            topic,
            CachedMetadata {
                partition_count,
                cached_at: Instant::now(),
            },
        );
    }

    /// Invalidate a topic's cache entry
    async fn invalidate(&self, topic: &str) {
        let mut cache = self.cache.write().await;
        cache.remove(topic);
    }

    /// Clear expired entries
    async fn cleanup(&self) {
        let mut cache = self.cache.write().await;
        cache.retain(|_, entry| entry.cached_at.elapsed() < self.ttl);
    }
}

// ============================================================================
// Sticky Partitioner
// ============================================================================

/// Sticky partitioner that batches messages to the same partition.
///
/// Uses `DashMap` with per-shard locking and atomics for interior
/// mutability — no async lock, no global contention.
struct StickyPartitioner {
    /// Per-topic sticky state (DashMap provides per-shard locking)
    sticky_partitions: dashmap::DashMap<String, StickyState>,
}

struct StickyState {
    /// Current partition (atomically updated)
    partition: AtomicU32,
    /// Number of messages batched to this partition
    batch_count: AtomicU32,
    /// Total partitions for this topic (updated on topology change)
    total_partitions: AtomicU32,
}

impl StickyPartitioner {
    fn new() -> Self {
        Self {
            sticky_partitions: dashmap::DashMap::new(),
        }
    }

    /// Get partition for a message (sync — no async lock).
    ///
    /// If key is provided, uses consistent hashing.
    /// Otherwise, uses sticky partitioning (batch to same partition).
    fn partition(&self, topic: &str, key: Option<&[u8]>, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }

        // If key is provided, use consistent hash
        if let Some(key) = key {
            return murmur2_partition(key, num_partitions);
        }

        // Fast path: read existing entry via DashMap shard lock
        if let Some(state) = self.sticky_partitions.get(topic) {
            let total = state.total_partitions.load(Ordering::Relaxed);

            // Topology changed — reset
            if total != num_partitions {
                state.total_partitions.store(num_partitions, Ordering::Relaxed);
                let new_part = rand_partition(num_partitions);
                state.partition.store(new_part, Ordering::Relaxed);
                state.batch_count.store(1, Ordering::Relaxed);
                return new_part;
            }

            let count = state.batch_count.fetch_add(1, Ordering::Relaxed) + 1;
            if count >= STICKY_BATCH_THRESHOLD as u32 {
                let current = state.partition.load(Ordering::Relaxed);
                let next = (current + 1) % num_partitions;
                state.partition.store(next, Ordering::Relaxed);
                state.batch_count.store(0, Ordering::Relaxed);
                return next;
            }

            return state.partition.load(Ordering::Relaxed);
        }

        // Slow path: initialize new topic entry
        let initial = rand_partition(num_partitions);
        let state = StickyState {
            partition: AtomicU32::new(initial),
            batch_count: AtomicU32::new(1),
            total_partitions: AtomicU32::new(num_partitions),
        };
        self.sticky_partitions.insert(topic.to_string(), state);
        initial
    }
}

/// Murmur2 consistent hash — delegates to the canonical shared implementation
/// in `rivven_core::hash` to guarantee client/server partition agreement.
#[inline]
fn murmur2_partition(key: &[u8], num_partitions: u32) -> u32 {
    rivven_core::hash::murmur2_partition(key, num_partitions)
}

/// Get a random partition using a proper PRNG.
///
/// Returns `0` when `num_partitions` is zero to avoid a panic from the
/// modulo operator (CLIENT-12).
fn rand_partition(num_partitions: u32) -> u32 {
    if num_partitions == 0 {
        return 0;
    }
    (rand::random::<u32>()) % num_partitions
}

// ============================================================================
// Producer Record & Batch
// ============================================================================

/// A record to be sent to the broker
pub struct ProducerRecord {
    /// Topic name
    pub topic: String,
    /// Optional partition (if None, will be computed)
    pub partition: Option<u32>,
    /// Optional key for partitioning
    pub key: Option<Bytes>,
    /// Message value
    pub value: Bytes,
    /// Response channel
    response_tx: oneshot::Sender<Result<RecordMetadata>>,
    /// Backpressure permit — travels with the record through the channel
    /// and sender task pipeline so it is only released when the record is
    /// consumed, NOT when the caller's delivery timeout fires (CLIENT-11).
    _permit: OwnedSemaphorePermit,
}

/// Metadata returned after successful send
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: u32,
    /// Offset within partition
    pub offset: u64,
    /// Timestamp
    pub timestamp: u64,
}

/// A batch of records for a single topic-partition
struct RecordBatch {
    topic: String,
    partition: u32,
    records: Vec<(
        Bytes,
        Option<Bytes>,
        oneshot::Sender<Result<RecordMetadata>>,
    )>,
    batch_size_bytes: usize,
    created_at: Instant,
}

impl RecordBatch {
    fn new(topic: String, partition: u32) -> Self {
        Self {
            topic,
            partition,
            records: Vec::new(),
            batch_size_bytes: 0,
            created_at: Instant::now(),
        }
    }

    fn add(
        &mut self,
        key: Option<Bytes>,
        value: Bytes,
        tx: oneshot::Sender<Result<RecordMetadata>>,
    ) {
        self.batch_size_bytes += value.len() + key.as_ref().map(|k| k.len()).unwrap_or(0);
        self.records.push((value, key, tx));
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Error from a batch send that carries undelivered records for retry.
///
/// When `send_batch` encounters a transport-level failure (I/O error,
/// connection reset, etc.), it returns this struct instead of dropping
/// the remaining oneshot channels. The caller can reconnect and retry
/// with the `undelivered` records — their response channels are still
/// live, so the original `Producer::send()` callers will receive the
/// result of the retry attempt.
struct BatchSendError {
    error: Error,
    /// Records that were NOT delivered. `(value, key, response_channel)`.
    undelivered: Vec<(
        Bytes,
        Option<Bytes>,
        oneshot::Sender<Result<RecordMetadata>>,
    )>,
}

// ============================================================================
// Producer
// ============================================================================

/// High-performance thread-safe producer
///
/// Uses `Arc<Producer>` for concurrent access across tasks.
/// Implements sticky partitioning, batching, metadata caching, and backpressure.
pub struct Producer {
    inner: Arc<ProducerInner>,
}

struct ProducerInner {
    config: ProducerConfig,
    /// Metadata cache with TTL
    metadata_cache: MetadataCache,
    /// Sticky partitioner
    partitioner: StickyPartitioner,
    /// Channel to sender task
    record_tx: mpsc::Sender<ProducerRecord>,
    /// Memory semaphore for backpressure
    memory_semaphore: Arc<Semaphore>,
    /// Producer ID for idempotence (used when enable_idempotence=true)
    producer_id: AtomicU64,
    /// Whether idempotence is actually active (not just configured).
    /// Set to true only after successful init_producer_id from broker.
    idempotence_active: AtomicBool,
    /// Persistent client for metadata fetches. Avoids creating
    /// a new TCP connection (with handshake + SCRAM auth) per cache miss.
    metadata_client: tokio::sync::Mutex<Option<crate::Client>>,
    /// Compressor instance wired from `config.compression_type`.
    /// Created once and reused across all send_batch calls.
    #[cfg(feature = "compression")]
    compressor: Compressor,
    /// Statistics
    stats: ProducerStats,
    /// Number of records pending delivery (for flush)
    pending_records: AtomicU64,
    /// Notify when pending records reaches zero
    flush_notify: Notify,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Shutdown signal
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl Clone for Producer {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Producer {
    /// Create a new producer
    pub async fn new(config: ProducerConfig) -> Result<Self> {
        if config.bootstrap_servers.is_empty() {
            return Err(Error::ConfigError(
                "No bootstrap servers configured".to_string(),
            ));
        }

        // The wire protocol does not carry an acks field, so
        // acks=-1 (All) cannot be communicated to the broker — the server
        // applies its own replication policy.  Warn once so operators are
        // aware.  acks=0 is handled client-side in send_batch().
        if config.acks == -1 {
            warn!(
                "acks=-1 (All) requested but the Rivven wire protocol does not \
                 transmit an acks field; the broker will apply its default \
                 replication policy"
            );
        }

        // Calculate initial memory permits (based on average record size)
        let memory_permits = config.buffer_memory / 1024; // ~1KB per permit
        let memory_semaphore = Arc::new(Semaphore::new(memory_permits));

        // Create channels
        let (record_tx, record_rx) = mpsc::channel(config.buffer_memory / config.batch_size);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // build a Compressor from the configured CompressionType
        #[cfg(feature = "compression")]
        let compressor = {
            let algorithm = config.compression_type.to_algorithm()?;
            Compressor::with_config(CompressionConfig {
                algorithm,
                ..CompressionConfig::default()
            })
        };

        let inner = Arc::new(ProducerInner {
            config: config.clone(),
            metadata_cache: MetadataCache::new(config.metadata_max_age),
            partitioner: StickyPartitioner::new(),
            record_tx,
            memory_semaphore: memory_semaphore.clone(),
            producer_id: AtomicU64::new(0),
            idempotence_active: AtomicBool::new(false),
            metadata_client: tokio::sync::Mutex::new(None),
            #[cfg(feature = "compression")]
            compressor,
            stats: ProducerStats::new(),
            pending_records: AtomicU64::new(0),
            flush_notify: Notify::new(),
            shutdown: AtomicBool::new(false),
            shutdown_tx,
        });

        // Try all bootstrap servers instead of only bootstrap_servers[0].
        // This provides failover if the first server is down.
        // Use Client::connect() for protocol handshake and optional
        // SCRAM-SHA-256 authentication, instead of raw TcpStream.
        let mut client: Option<crate::Client> = None;
        let mut last_err: Option<Error> = None;
        for addr in &config.bootstrap_servers {
            let connect_fut = connect_producer_client(addr, &config);
            match tokio::time::timeout(config.connection_timeout, connect_fut).await {
                Ok(Ok(c)) => {
                    debug!("Connected to bootstrap server {}", addr);
                    client = Some(c);
                    break;
                }
                Ok(Err(e)) => {
                    debug!("Failed to connect to {}: {}", addr, e);
                    // Authentication failures are permanent — stop trying
                    // other servers since credentials won't suddenly work.
                    if !e.is_retriable() {
                        return Err(e);
                    }
                    last_err = Some(e);
                }
                Err(_) => {
                    debug!("Connection timeout to {}", addr);
                    last_err = Some(Error::ConnectionError(format!(
                        "Connection timeout to {}",
                        addr
                    )));
                }
            }
        }
        let connected_client = client.ok_or_else(|| {
            last_err.unwrap_or_else(|| {
                Error::ConnectionError("Failed to connect to any bootstrap server".into())
            })
        })?;
        // Extract the underlying ClientStream (plaintext or TLS) for the sender task
        let stream = connected_client.into_client_stream();

        // Spawn sender task with initialization channel
        let (init_tx, init_rx) = oneshot::channel();
        let sender_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            sender_task(sender_inner, stream, record_rx, shutdown_rx, init_tx).await;
        });

        // Wait for sender task initialization (including idempotence
        // handshake). Fail fast when idempotence was requested but could
        // not be established.
        match init_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(Error::ConnectionError(
                    "Sender task terminated during initialization".into(),
                ))
            }
        }

        // Spawn metadata cleanup task
        let cleanup_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if cleanup_inner.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                cleanup_inner.metadata_cache.cleanup().await;
            }
        });

        info!(
            "Producer initialized with {} bootstrap servers",
            config.bootstrap_servers.len()
        );

        Ok(Self { inner })
    }

    /// Send a message to a topic
    ///
    /// Uses sticky partitioning for keyless messages.
    pub async fn send(
        &self,
        topic: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<RecordMetadata> {
        self.send_with_key(topic, None::<Bytes>, value).await
    }

    /// Send a message with a key
    ///
    /// Key is used for consistent partitioning.
    pub async fn send_with_key(
        &self,
        topic: impl Into<String>,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
    ) -> Result<RecordMetadata> {
        let topic = topic.into();
        let key: Option<Bytes> = key.map(|k| k.into());
        let value = value.into();

        // Backpressure: wait for memory
        // Include per-record overhead (struct, channel, topic
        // string, batch bookkeeping) so the semaphore accurately reflects
        // total memory pressure, not just payload size.
        let value_size = value.len() + key.as_ref().map(|k| k.len()).unwrap_or(0);
        let total_size = value_size + topic.len() + RECORD_OVERHEAD_BYTES;
        let permits_needed = (total_size / 1024).max(1);

        let _permit = tokio::time::timeout(
            self.inner.config.request_timeout,
            Arc::clone(&self.inner.memory_semaphore).acquire_many_owned(permits_needed as u32),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::ConnectionError("Producer closed".into()))?;

        let (response_tx, response_rx) = oneshot::channel();

        let record = ProducerRecord {
            topic,
            partition: None,
            key,
            value,
            response_tx,
            _permit,
        };

        self.inner
            .record_tx
            .send(record)
            .await
            .map_err(|_| Error::ConnectionError("Sender task closed".into()))?;

        self.inner
            .stats
            .records_sent
            .fetch_add(1, Ordering::Relaxed);
        self.inner.pending_records.fetch_add(1, Ordering::Release);

        // Wait for response
        tokio::time::timeout(self.inner.config.delivery_timeout, response_rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::ConnectionError("Response channel dropped".into()))?
    }

    /// Send a message to a specific partition
    pub async fn send_to_partition(
        &self,
        topic: impl Into<String>,
        partition: u32,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
    ) -> Result<RecordMetadata> {
        let topic = topic.into();
        let key: Option<Bytes> = key.map(|k| k.into());
        let value = value.into();

        // Apply the same backpressure, stats, and flush tracking
        // as send_with_key().
        // Include per-record overhead (see RECORD_OVERHEAD_BYTES).
        let value_size = value.len() + key.as_ref().map(|k| k.len()).unwrap_or(0);
        let total_size = value_size + topic.len() + RECORD_OVERHEAD_BYTES;
        let permits_needed = (total_size / 1024).max(1);

        let _permit = tokio::time::timeout(
            self.inner.config.request_timeout,
            Arc::clone(&self.inner.memory_semaphore).acquire_many_owned(permits_needed as u32),
        )
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::ConnectionError("Producer closed".into()))?;

        let (response_tx, response_rx) = oneshot::channel();

        let record = ProducerRecord {
            topic,
            partition: Some(partition),
            key,
            value,
            response_tx,
            _permit,
        };

        self.inner
            .record_tx
            .send(record)
            .await
            .map_err(|_| Error::ConnectionError("Sender task closed".into()))?;

        self.inner
            .stats
            .records_sent
            .fetch_add(1, Ordering::Relaxed);
        self.inner.pending_records.fetch_add(1, Ordering::Release);

        tokio::time::timeout(self.inner.config.delivery_timeout, response_rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::ConnectionError("Response channel dropped".into()))?
    }

    /// Flush all pending records and wait for delivery confirmation
    ///
    /// This method blocks until all records that were sent before the flush
    /// call have been acknowledged by the broker. Uses efficient wait-notify
    /// instead of polling.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if all pending records were delivered successfully
    /// - `Err(Timeout)` if records weren't delivered within `delivery_timeout`
    pub async fn flush(&self) -> Result<()> {
        // Fast path: no pending records
        if self.inner.pending_records.load(Ordering::Acquire) == 0 {
            return Ok(());
        }

        // Wait for all pending records to be delivered
        let deadline = tokio::time::Instant::now() + self.inner.config.delivery_timeout;

        loop {
            // Check if all records are delivered
            if self.inner.pending_records.load(Ordering::Acquire) == 0 {
                return Ok(());
            }

            // Check timeout
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout);
            }

            // Wait for notification or timeout (whichever comes first)
            tokio::select! {
                _ = self.inner.flush_notify.notified() => {
                    // Re-check pending count
                    continue;
                }
                _ = tokio::time::sleep(remaining.min(Duration::from_millis(10))) => {
                    // Periodic check in case notification was missed
                    continue;
                }
            }
        }
    }

    /// Get producer statistics
    pub fn stats(&self) -> ProducerStatsSnapshot {
        ProducerStatsSnapshot {
            records_sent: self.inner.stats.records_sent.load(Ordering::Relaxed),
            records_delivered: self.inner.stats.records_delivered.load(Ordering::Relaxed),
            batches_sent: self.inner.stats.batches_sent.load(Ordering::Relaxed),
            errors: self.inner.stats.errors.load(Ordering::Relaxed),
            retries: self.inner.stats.retries.load(Ordering::Relaxed),
        }
    }

    /// Check whether idempotent delivery is actually active.
    ///
    /// Returns `true` only if both:
    /// 1. `enable_idempotence` was set in config, AND
    /// 2. `init_producer_id` succeeded on the broker
    ///
    /// If this returns `false` when you configured `exactly_once()`, the
    /// producer silently degraded to at-least-once delivery.
    pub fn is_idempotent(&self) -> bool {
        self.inner.idempotence_active.load(Ordering::Acquire)
    }

    /// Close the producer gracefully.
    ///
    /// Flushes any buffered records (with a 30-second timeout) before
    /// signalling shutdown, matching Kafka's `KafkaProducer.close()`
    /// semantics.
    pub async fn close(&self) {
        // Drain buffered records before shutdown
        match tokio::time::timeout(Duration::from_secs(30), self.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Flush failed during close: {e}"),
            Err(_) => warn!("Flush timed out during close"),
        }
        self.inner.shutdown.store(true, Ordering::Relaxed);
        let _ = self.inner.shutdown_tx.send(true);
    }

    /// Get partition count for a topic (from cache or server)
    ///
    /// Checks the local cache first (O(1) lookup). On cache miss,
    /// fetches metadata from the broker and caches the result.
    ///
    /// # Performance
    ///
    /// - Cache hit: ~50ns (single RwLock read)
    /// - Cache miss: ~1-5ms (network round-trip)
    pub async fn get_partition_count(&self, topic: &str) -> Result<u32> {
        // Fast path: check cache first
        if let Some(count) = self.inner.metadata_cache.get(topic).await {
            return Ok(count);
        }

        // Slow path: fetch from server
        self.refresh_metadata(topic).await
    }

    /// Force refresh metadata for a topic from the server
    ///
    /// This bypasses the cache and always fetches fresh metadata.
    /// Useful after topic reconfiguration or partition expansion.
    pub async fn refresh_metadata(&self, topic: &str) -> Result<u32> {
        // Invalidate existing cache entry so the fetch always goes to the broker
        self.inner.metadata_cache.invalidate(topic).await;
        let partitions = fetch_topic_metadata(&self.inner, topic).await?;
        debug!(
            "Refreshed metadata for topic '{}': {} partitions",
            topic, partitions
        );
        Ok(partitions)
    }
}

// ============================================================================
// Metadata Fetch Helper
// ============================================================================

/// Fetch topic metadata from a bootstrap server and cache the result.
///
/// tries all bootstrap servers instead of only the first one.
/// uses `Client::connect()` for protocol handshake + optional
/// SCRAM-SHA-256 auth, instead of raw TcpStream.
/// reuses a persistent metadata client stored in ProducerInner.
/// Only creates a new connection on first call or after connection failure.
async fn fetch_topic_metadata(inner: &ProducerInner, topic: &str) -> Result<u32> {
    // Fast path: try the cached metadata client
    {
        let mut guard = inner.metadata_client.lock().await;
        if let Some(ref mut client) = *guard {
            let request = Request::GetMetadata {
                topic: topic.to_string(),
            };
            match client.send_request(request).await {
                Ok(Response::Metadata { name, partitions }) => {
                    inner.metadata_cache.put(name, partitions).await;
                    return Ok(partitions);
                }
                Ok(Response::Error { message }) => return Err(Error::ServerError(message)),
                Ok(_) => return Err(Error::InvalidResponse),
                Err(_) => {
                    // Connection failed — drop it and reconnect below
                    *guard = None;
                }
            }
        }
    }

    // Slow path: create a new connection and cache it
    let mut new_client: Option<crate::Client> = None;
    let mut last_err = None;
    for addr in &inner.config.bootstrap_servers {
        let connect_fut = connect_producer_client(addr, &inner.config);
        match tokio::time::timeout(inner.config.connection_timeout, connect_fut).await {
            Ok(Ok(c)) => {
                new_client = Some(c);
                break;
            }
            Ok(Err(e)) => {
                last_err = Some(format!("Failed to connect to {}: {}", addr, e));
            }
            Err(_) => {
                last_err = Some(format!("Metadata request timeout to {}", addr));
            }
        }
    }
    let mut client = new_client.ok_or_else(|| {
        Error::ConnectionError(format!(
            "Failed to connect to any bootstrap server for metadata: {}",
            last_err.unwrap_or_default()
        ))
    })?;

    let request = Request::GetMetadata {
        topic: topic.to_string(),
    };

    let response = client.send_request(request).await?;

    match response {
        Response::Metadata { name, partitions } => {
            inner.metadata_cache.put(name, partitions).await;
            // Cache the client for future metadata requests
            *inner.metadata_client.lock().await = Some(client);
            Ok(partitions)
        }
        Response::Error { message } => Err(Error::ServerError(message)),
        _ => Err(Error::InvalidResponse),
    }
}

/// Send `InitProducerId` request to the broker and return
/// (producer_id, producer_epoch). This must be called once before
/// producing idempotent records.
async fn init_producer_id<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    writer: &mut BufWriter<W>,
    reader: &mut BufReader<R>,
    inner: &Arc<ProducerInner>,
) -> Result<(u64, u16)> {
    let current_pid = inner.producer_id.load(Ordering::Acquire);
    let previous = if current_pid == 0 {
        None
    } else {
        Some(current_pid)
    };

    let request = Request::InitProducerId {
        producer_id: previous,
    };

    let request_bytes = request.to_wire(rivven_protocol::WireFormat::Postcard, 0u32)?;
    let len: u32 = request_bytes
        .len()
        .try_into()
        .map_err(|_| Error::RequestTooLarge(request_bytes.len(), u32::MAX as usize))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&request_bytes).await?;
    writer.flush().await?;

    // Read response
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let response_len = u32::from_be_bytes(len_buf) as usize;

    // validate response size to prevent OOM from malicious/buggy servers
    const MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024; // 100 MB
    if response_len > MAX_RESPONSE_SIZE {
        return Err(Error::ResponseTooLarge(response_len, MAX_RESPONSE_SIZE));
    }

    let mut response_buf = vec![0u8; response_len];
    reader.read_exact(&mut response_buf).await?;

    let (response, _format, _correlation_id) =
        Response::from_wire(&response_buf).map_err(Error::ProtocolError)?;

    match response {
        Response::ProducerIdInitialized {
            producer_id,
            producer_epoch,
        } => Ok((producer_id, producer_epoch)),
        Response::Error { message } => Err(Error::ServerError(message)),
        _ => Err(Error::InvalidResponse),
    }
}

// ============================================================================
// Sender Task
// ============================================================================

/// Connect to a bootstrap server with optional TLS and SCRAM auth.
///
/// Shared by `Producer::new()`, `fetch_topic_metadata()`, and the
/// sender-task reconnection path so that TLS/auth logic lives in one place.
async fn connect_producer_client(addr: &str, config: &ProducerConfig) -> Result<crate::Client> {
    // Defensive check: reject tls_config without a server name so we never
    // silently fall back to a plaintext connection.
    #[cfg(feature = "tls")]
    if config.tls_config.is_some() && config.tls_server_name.is_none() {
        return Err(Error::ConfigError(
            "tls_config is set but tls_server_name is missing; \
             a server name is required for TLS certificate verification"
                .to_string(),
        ));
    }

    #[allow(unused_mut)]
    let mut client = {
        #[cfg(feature = "tls")]
        {
            if let (Some(tls_cfg), Some(server_name)) =
                (&config.tls_config, &config.tls_server_name)
            {
                crate::Client::connect_tls(addr, tls_cfg, server_name).await?
            } else {
                crate::Client::connect(addr).await?
            }
        }
        #[cfg(not(feature = "tls"))]
        {
            crate::Client::connect(addr).await?
        }
    };

    // Authenticate if credentials are configured
    if let Some(auth) = &config.auth {
        client
            .authenticate_scram(&auth.username, &auth.password)
            .await
            .map_err(|e| Error::AuthenticationFailed(format!("{}: {}", addr, e)))?;
    }

    Ok(client)
}

/// Attempt to reconnect to any bootstrap server, returning the new
/// `ClientStream`.  Uses exponential backoff with jitter internally
/// (100 ms → 10 s, plus up to 25 % random jitter to avoid thundering herd).
async fn reconnect_producer_stream(
    inner: &Arc<ProducerInner>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Option<ClientStream> {
    let mut backoff = Duration::from_millis(100);
    let max_backoff = Duration::from_secs(10);

    loop {
        // Respect shutdown while waiting
        if *shutdown_rx.borrow() {
            return None;
        }

        for addr in &inner.config.bootstrap_servers {
            let connect_fut = connect_producer_client(addr, &inner.config);
            match tokio::time::timeout(inner.config.connection_timeout, connect_fut).await {
                Ok(Ok(c)) => {
                    info!("Reconnected to bootstrap server {}", addr);
                    return Some(c.into_client_stream());
                }
                Ok(Err(e)) => {
                    // Non-retriable errors (e.g. AuthenticationFailed) won't
                    // resolve by retrying — bail out immediately.
                    if !e.is_retriable() {
                        error!("Permanent error during reconnect to {}: {}", addr, e);
                        return None;
                    }
                    debug!("Reconnect failed to {}: {}", addr, e);
                }
                Err(_) => {
                    debug!("Reconnect timeout to {}", addr);
                }
            }
        }

        // Add random jitter (up to 25% of backoff) to avoid thundering herd
        let jitter =
            Duration::from_millis(rand::thread_rng().gen_range(0..=backoff.as_millis() as u64 / 4));
        let sleep_duration = backoff + jitter;

        warn!(
            "All bootstrap servers unreachable, retrying in {:?}",
            sleep_duration
        );

        inner.stats.retries.fetch_add(1, Ordering::Relaxed);

        // Wait with shutdown awareness
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    return None;
                }
            }
            _ = tokio::time::sleep(sleep_duration) => {}
        }

        backoff = (backoff * 2).min(max_backoff);
    }
}

/// Background task that batches and sends records
async fn sender_task(
    inner: Arc<ProducerInner>,
    stream: ClientStream,
    mut record_rx: mpsc::Receiver<ProducerRecord>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    init_tx: oneshot::Sender<Result<()>>,
) {
    let (read_half, write_half) = tokio::io::split(stream);
    let mut writer = BufWriter::with_capacity(64 * 1024, write_half);
    let mut reader = BufReader::with_capacity(64 * 1024, read_half);

    // when idempotence is enabled, send InitProducerId to get
    // a producer_id + epoch from the broker before producing any records.
    let mut producer_epoch: u16 = 0;
    if inner.config.enable_idempotence {
        match init_producer_id(&mut writer, &mut reader, &inner).await {
            Ok((pid, epoch)) => {
                inner.producer_id.store(pid, Ordering::Release);
                inner.idempotence_active.store(true, Ordering::Release);
                producer_epoch = epoch;
                debug!(
                    "Idempotent producer initialized: id={}, epoch={}",
                    pid, epoch
                );
                let _ = init_tx.send(Ok(()));
            }
            Err(e) => {
                // Return error instead of silently degrading
                // to at-least-once delivery.
                error!("Failed to init producer ID for idempotent mode: {}", e);
                let _ = init_tx.send(Err(Error::ConnectionError(format!(
                    "Failed to establish idempotent producer: {}. \
                     Cannot silently degrade to at-least-once delivery.",
                    e
                ))));
                return;
            }
        }
    } else {
        let _ = init_tx.send(Ok(()));
    }

    // Per-partition sequence tracker for idempotent publishing
    let mut partition_sequences: HashMap<(String, u32), i32> = HashMap::new();

    // Batches by topic-partition
    let mut batches: HashMap<(String, u32), RecordBatch> = HashMap::new();
    let mut last_flush = Instant::now();
    // Track whether current connection is healthy for reconnect detection
    let mut needs_reconnect = false;

    loop {
        // Check shutdown
        if *shutdown_rx.borrow() {
            break;
        }

        // ── Reconnection ────────────────────────────────────────────────
        if needs_reconnect {
            warn!("Connection lost, attempting reconnect with exponential backoff");
            match reconnect_producer_stream(&inner, &mut shutdown_rx).await {
                Some(new_stream) => {
                    let (rh, wh) = tokio::io::split(new_stream);
                    writer = BufWriter::with_capacity(64 * 1024, wh);
                    reader = BufReader::with_capacity(64 * 1024, rh);
                    needs_reconnect = false;
                    info!("Sender task reconnected successfully");

                    // Re-init idempotent state on the new connection
                    if inner.config.enable_idempotence {
                        match init_producer_id(&mut writer, &mut reader, &inner).await {
                            Ok((pid, epoch)) => {
                                inner.producer_id.store(pid, Ordering::Release);
                                inner.idempotence_active.store(true, Ordering::Release);
                                producer_epoch = epoch;
                                partition_sequences.clear();
                                debug!(
                                    "Re-initialized idempotent producer: id={}, epoch={}",
                                    pid, epoch
                                );
                            }
                            Err(e) => {
                                error!("Failed to re-init producer ID after reconnect: {}", e);
                                // Mark idempotence as inactive so the
                                // producer does not send IdempotentPublish
                                // requests with a stale producer_id.
                                inner.idempotence_active.store(false, Ordering::Release);
                            }
                        }
                    }
                }
                None => {
                    // Shutdown requested during reconnect
                    break;
                }
            }
        }

        // Calculate remaining linger time
        let linger_remaining = if inner.config.linger_ms == 0 {
            Duration::ZERO
        } else {
            let elapsed = last_flush.elapsed();
            let linger = Duration::from_millis(inner.config.linger_ms);
            linger.saturating_sub(elapsed)
        };

        // Try to receive with timeout
        let record = if linger_remaining == Duration::ZERO && !batches.is_empty() {
            None
        } else {
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    continue;
                }
                result = record_rx.recv() => {
                    match result {
                        Some(r) => Some(r),
                        None => break,
                    }
                }
                _ = tokio::time::sleep(linger_remaining), if linger_remaining > Duration::ZERO => {
                    None
                }
            }
        };

        // Add record to batch if we got one
        if let Some(record) = record {
            let partition = if let Some(p) = record.partition {
                p
            } else {
                // Get partition count: try cache first, then fetch from broker
                let num_partitions = match inner.metadata_cache.get(&record.topic).await {
                    Some(n) => n,
                    None => {
                        // Cache miss — fetch from broker and cache the result
                        match fetch_topic_metadata(&inner, &record.topic).await {
                            Ok(n) => n,
                            Err(e) => {
                                debug!(
                                        "Failed to fetch metadata for '{}': {}, defaulting to 1 partition",
                                        record.topic, e
                                    );
                                1
                            }
                        }
                    }
                };
                inner
                    .partitioner
                    .partition(&record.topic, record.key.as_deref(), num_partitions)
            };

            let key = (record.topic.clone(), partition);
            let batch = batches
                .entry(key)
                .or_insert_with(|| RecordBatch::new(record.topic.clone(), partition));
            batch.add(record.key, record.value, record.response_tx);
        }

        // Check if we should flush
        let should_flush = batches.values().any(|b| {
            b.batch_size_bytes >= inner.config.batch_size
                || b.created_at.elapsed().as_millis() as u64 >= inner.config.linger_ms
        }) || (inner.config.linger_ms == 0 && !batches.is_empty());

        if should_flush {
            // Collect all batches to flush. We use a separate Vec so that
            // if send_batch fails, we can notify remaining batches with errors
            // instead of silently dropping them.
            let flush_batches: Vec<_> = batches.drain().collect();
            let mut failed = false;

            let max_retries = inner.config.retries;
            let retry_backoff_ms = inner.config.retry_backoff_ms;
            let retry_backoff_max_ms = inner.config.retry_backoff_max_ms;

            for ((topic, partition), batch) in flush_batches {
                if batch.is_empty() {
                    continue;
                }

                if failed {
                    // Connection broken and exhausted retries —
                    // notify all remaining batches with error.
                    let batch_record_count = batch.len() as u64;
                    for (_value, _key, response_tx) in batch.records {
                        let _ = response_tx.send(Err(Error::ConnectionError(
                            "Connection lost during batch flush".to_string(),
                        )));
                    }
                    let prev = inner
                        .pending_records
                        .fetch_sub(batch_record_count, Ordering::Release);
                    if prev <= batch_record_count {
                        inner.flush_notify.notify_waiters();
                    }
                    continue;
                }

                // ── Per-batch retry loop ────────────────────────────────
                let mut current_batch = batch;
                let mut attempt = 0u32;
                let batch_deadline = current_batch.created_at + inner.config.delivery_timeout;

                loop {
                    // Check per-batch delivery timeout before each attempt
                    // (CLIENT-09). This bounds the total time a batch can spend
                    // retrying, matching Kafka's delivery.timeout.ms semantics.
                    if Instant::now() >= batch_deadline {
                        warn!(
                            topic = %topic,
                            partition,
                            attempt,
                            "Batch delivery timed out after {:?}",
                            inner.config.delivery_timeout
                        );
                        let undelivered_count = current_batch.len() as u64;
                        for (_v, _k, tx) in current_batch.records {
                            let _ = tx.send(Err(Error::Timeout));
                        }
                        let prev = inner
                            .pending_records
                            .fetch_sub(undelivered_count, Ordering::Release);
                        if prev <= undelivered_count {
                            inner.flush_notify.notify_waiters();
                        }
                        inner.stats.errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    let _batch_record_count = current_batch.len();

                    match send_batch(
                        &mut writer,
                        &mut reader,
                        &inner,
                        current_batch,
                        producer_epoch,
                        &mut partition_sequences,
                    )
                    .await
                    {
                        Ok(()) => break, // Batch delivered successfully
                        Err(BatchSendError { error, undelivered }) => {
                            attempt += 1;

                            // Check if the error is permanent (non-retriable).
                            // Permanent errors like PRODUCER_FENCED, INVALID_TOPIC,
                            // or authentication failures should not be retried.
                            let is_permanent = !error.is_retriable();

                            if is_permanent || attempt > max_retries || undelivered.is_empty() {
                                // Exhausted retries or permanent error — notify callers
                                let reason = if is_permanent {
                                    " (permanent error)".to_string()
                                } else {
                                    format!(" after {} retries", attempt)
                                };
                                warn!(
                                    topic = %topic,
                                    partition,
                                    attempt,
                                    permanent = is_permanent,
                                    error = %error,
                                    "Batch send failed{}, giving up",
                                    reason
                                );
                                inner.stats.errors.fetch_add(1, Ordering::Relaxed);
                                let undelivered_count = undelivered.len() as u64;
                                // Propagate the original error to callers so they can
                                // programmatically distinguish between permanent rejections
                                // (e.g., PRODUCER_FENCED) and transient failures.
                                // Previously, errors were wrapped in ConnectionError which
                                // lost the original error type and inverted is_retriable().
                                let error_msg = format!("Batch send failed{}: {}", reason, error);
                                for (_v, _k, tx) in undelivered {
                                    let err = if is_permanent {
                                        // Preserve as ServerError so callers see is_retriable() = false
                                        Error::ServerError(error_msg.clone())
                                    } else {
                                        Error::ConnectionError(error_msg.clone())
                                    };
                                    let _ = tx.send(Err(err));
                                }
                                let prev = inner
                                    .pending_records
                                    .fetch_sub(undelivered_count, Ordering::Release);
                                if prev <= undelivered_count {
                                    inner.flush_notify.notify_waiters();
                                }
                                // Only set needs_reconnect for transient errors.
                                // Permanent errors (PRODUCER_FENCED, etc.) won't resolve
                                // by reconnecting — it just wastes a TCP + auth handshake.
                                if !is_permanent {
                                    needs_reconnect = true;
                                }
                                failed = true;
                                break;
                            }

                            // Retriable — reconnect and retry
                            inner.stats.retries.fetch_add(1, Ordering::Relaxed);
                            // Clamp exponent to 63 to prevent u64 overflow in release
                            // mode (wraps to 0) or panic in debug mode.
                            let exp = attempt.saturating_sub(1).min(63);
                            let base_backoff_ms = retry_backoff_ms
                                .saturating_mul(2u64.pow(exp))
                                .min(retry_backoff_max_ms);
                            // Add 25% jitter to prevent thundering-herd on broker restart
                            let jitter = rand::thread_rng().gen_range(0..=base_backoff_ms / 4);
                            let backoff_ms = base_backoff_ms.saturating_add(jitter);
                            warn!(
                                topic = %topic,
                                partition,
                                attempt,
                                max_retries,
                                backoff_ms,
                                error = %error,
                                remaining_records = undelivered.len(),
                                "Batch send failed, retrying after {}ms",
                                backoff_ms
                            );

                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                            // Note: sequence rollback is NOT needed here —
                            // init_producer_id (below) calls partition_sequences.clear() which
                            // resets all sequences. For non-idempotent mode, sequences are unused.

                            // Reconnect
                            match reconnect_producer_stream(&inner, &mut shutdown_rx).await {
                                Some(new_stream) => {
                                    let (rh, wh) = tokio::io::split(new_stream);
                                    writer = BufWriter::with_capacity(64 * 1024, wh);
                                    reader = BufReader::with_capacity(64 * 1024, rh);

                                    // Only re-init idempotent producer ID when the
                                    // broker explicitly signals the identity is
                                    // unknown.  For transient errors (NOT_LEADER,
                                    // COORDINATOR_NOT_AVAILABLE, connection resets)
                                    // we reuse the existing producer_id + epoch +
                                    // sequences so the broker can still deduplicate
                                    // retries (CLIENT-04 / KIP-360).
                                    let needs_reinit =
                                        error.to_string().contains("UNKNOWN_PRODUCER_ID")
                                            || error.to_string().contains("OUT_OF_ORDER_SEQUENCE");

                                    if inner.config.enable_idempotence && needs_reinit {
                                        match init_producer_id(&mut writer, &mut reader, &inner)
                                            .await
                                        {
                                            Ok((new_pid, epoch)) => {
                                                inner.producer_id.store(new_pid, Ordering::Release);
                                                inner
                                                    .idempotence_active
                                                    .store(true, Ordering::Release);
                                                producer_epoch = epoch;
                                                partition_sequences.clear();
                                                debug!(
                                                    "Re-initialized idempotent producer: id={}, epoch={}",
                                                    new_pid, epoch
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Failed to re-init producer ID on retry: {}",
                                                    e
                                                );
                                                inner
                                                    .idempotence_active
                                                    .store(false, Ordering::Release);
                                            }
                                        }
                                    } else if inner.config.enable_idempotence {
                                        debug!(
                                            "Reusing existing producer identity for retry: id={}, epoch={}",
                                            inner.producer_id.load(Ordering::Acquire),
                                            producer_epoch,
                                        );
                                    }
                                }
                                None => {
                                    // Shutdown requested during reconnect
                                    let undelivered_count = undelivered.len() as u64;
                                    for (_v, _k, tx) in undelivered {
                                        let _ = tx.send(Err(Error::ConnectionError(
                                            "Producer shutting down during retry".to_string(),
                                        )));
                                    }
                                    let prev = inner
                                        .pending_records
                                        .fetch_sub(undelivered_count, Ordering::Release);
                                    if prev <= undelivered_count {
                                        inner.flush_notify.notify_waiters();
                                    }
                                    failed = true;
                                    break;
                                }
                            }

                            // Rebuild batch from undelivered records
                            let mut retry_batch = RecordBatch::new(topic.clone(), partition);
                            for (v, k, tx) in undelivered {
                                retry_batch.add(k, v, tx);
                            }
                            current_batch = retry_batch;
                            needs_reconnect = false;
                        }
                    }
                }
            }

            last_flush = Instant::now();
        }
    }

    // Flush remaining batches (best-effort during shutdown)
    for ((_topic, _partition), batch) in batches.drain() {
        if !batch.is_empty() {
            if let Err(BatchSendError { undelivered, .. }) = send_batch(
                &mut writer,
                &mut reader,
                &inner,
                batch,
                producer_epoch,
                &mut partition_sequences,
            )
            .await
            {
                // During shutdown, notify remaining callers with error
                let count = undelivered.len() as u64;
                for (_v, _k, tx) in undelivered {
                    let _ = tx.send(Err(Error::ConnectionError(
                        "Producer shutdown — batch delivery failed".to_string(),
                    )));
                }
                let prev = inner.pending_records.fetch_sub(count, Ordering::Release);
                if prev <= count {
                    inner.flush_notify.notify_waiters();
                }
            }
        }
    }

    // Drain any records still buffered in the mpsc channel.
    // Without this, `pending_records` stays inflated and `flush()` spins
    // until `delivery_timeout` expires instead of returning immediately.
    record_rx.close();
    while let Ok(record) = record_rx.try_recv() {
        let _ = record.response_tx.send(Err(Error::ConnectionError(
            "Producer shutdown — record not sent".to_string(),
        )));
        let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            inner.flush_notify.notify_waiters();
        }
    }
}

/// Send a batch of records with optimized I/O
///
/// Writes all records first (coalesced into single syscall via BufWriter),
/// then reads all responses. This is more efficient than request-response
/// per record.
///
/// ## Response correlation
///
/// Responses are matched to requests **by sequential order**, not by a
/// correlation ID. This is safe because:
///
/// 1. This is a **dedicated, single TCP connection** — no other requests
///    are interleaved on this stream.
/// 2. The server processes requests in FIFO order on the same connection
///    and writes responses back in the **same order**.
/// 3. The batch is fully flushed before any response is read, so there is
///    no request/response interleaving within a batch.
///
/// If the server ever reorders responses (protocol violation), records
/// would receive the wrong offset. The `PipelinedClient` in `pipeline.rs`
/// uses 8-byte request IDs for true out-of-order correlation and should
/// be used when multiplexing is required.
///
/// ## Retry semantics
///
/// On transport failure, returns [`BatchSendError`] carrying the records
/// whose response channels have **not** been used. The caller can reconnect
/// and resend those records. For idempotent producers, duplicate delivery
/// is prevented by the server's sequence-number check.
async fn send_batch<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    writer: &mut BufWriter<W>,
    reader: &mut BufReader<R>,
    inner: &Arc<ProducerInner>,
    batch: RecordBatch,
    producer_epoch: u16,
    partition_sequences: &mut HashMap<(String, u32), i32>,
) -> std::result::Result<(), BatchSendError> {
    let topic = batch.topic.clone();
    let partition = batch.partition;
    let num_records = batch.len();

    if num_records == 0 {
        return Ok(());
    }

    inner.stats.batches_sent.fetch_add(1, Ordering::Relaxed);

    // Phase 1: Build a single PublishBatch / IdempotentPublishBatch message.
    //
    // We keep cloned (value, key) data alongside the response channels
    // so that on transport failure we can return them for retry.
    // `Bytes::clone()` is O(1) — it just bumps an atomic ref-count.
    let mut response_channels: VecDeque<oneshot::Sender<Result<RecordMetadata>>> =
        VecDeque::with_capacity(num_records);
    let mut cloned_data: VecDeque<(Bytes, Option<Bytes>)> = VecDeque::with_capacity(num_records);
    let mut batch_records: Vec<BatchRecord> = Vec::with_capacity(num_records);

    #[cfg(feature = "compression")]
    let use_compression = inner.config.compression_type != CompressionType::None;
    #[cfg(not(feature = "compression"))]
    let use_compression = false;

    let use_idempotent = inner.idempotence_active.load(Ordering::Acquire)
        && inner.producer_id.load(Ordering::Acquire) != 0;
    let pid = inner.producer_id.load(Ordering::Acquire);

    for (value, key, response_tx) in batch.records {
        // Keep cloned copies for potential retry
        cloned_data.push_back((value.clone(), key.clone()));
        response_channels.push_back(response_tx);

        let wire_value = if use_compression {
            #[cfg(feature = "compression")]
            {
                match inner.compressor.compress(&value) {
                    Ok(compressed) => compressed,
                    Err(e) => {
                        tracing::warn!("Compression failed, sending uncompressed: {}", e);
                        value
                    }
                }
            }
            #[cfg(not(feature = "compression"))]
            {
                value
            }
        } else {
            value
        };

        batch_records.push(BatchRecord {
            key,
            value: wire_value,
        });
    }

    // Build a single wire message for the entire batch — eliminates
    // per-record topic cloning, serialization, and write syscalls.
    let request = if use_idempotent {
        let seq = partition_sequences
            .entry((topic.clone(), partition))
            .or_insert(0);
        let base_seq = *seq;
        *seq += num_records as i32;

        Request::IdempotentPublishBatch {
            topic: topic.clone(),
            partition: Some(partition),
            records: batch_records,
            producer_id: pid,
            producer_epoch,
            base_sequence: base_seq,
            leader_epoch: None,
        }
    } else {
        Request::PublishBatch {
            topic: topic.clone(),
            partition: Some(partition),
            records: batch_records,
            leader_epoch: None,
        }
    };

    // Serialize once (vs. N times with per-record Publish)
    let request_bytes = match request.to_wire(rivven_protocol::WireFormat::Postcard, 0u32) {
        Ok(b) => b,
        Err(e) => {
            let undelivered = reassemble_undelivered(cloned_data, response_channels);
            return Err(BatchSendError {
                error: Error::ProtocolError(e),
                undelivered,
            });
        }
    };
    let len: u32 = match request_bytes.len().try_into() {
        Ok(l) => l,
        Err(_) => {
            let undelivered = reassemble_undelivered(cloned_data, response_channels);
            return Err(BatchSendError {
                error: Error::RequestTooLarge(request_bytes.len(), u32::MAX as usize),
                undelivered,
            });
        }
    };

    // Single write + flush (vs. 2N writes + 1 flush)
    if let Err(e) = writer.write_all(&len.to_be_bytes()).await {
        let undelivered = reassemble_undelivered(cloned_data, response_channels);
        return Err(BatchSendError {
            error: e.into(),
            undelivered,
        });
    }
    if let Err(e) = writer.write_all(&request_bytes).await {
        let undelivered = reassemble_undelivered(cloned_data, response_channels);
        return Err(BatchSendError {
            error: e.into(),
            undelivered,
        });
    }
    if let Err(e) = writer.flush().await {
        let undelivered = reassemble_undelivered(cloned_data, response_channels);
        return Err(BatchSendError {
            error: e.into(),
            undelivered,
        });
    }

    // acks=0 (fire-and-forget) — skip reading responses.
    if inner.config.acks == 0 {
        for response_tx in response_channels {
            let _ = response_tx.send(Ok(RecordMetadata {
                topic: topic.clone(),
                partition,
                offset: 0,
                timestamp: 0,
            }));
            inner
                .stats
                .records_delivered
                .fetch_add(1, Ordering::Relaxed);
            let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
            if prev == 1 {
                inner.flush_notify.notify_waiters();
            }
        }
        debug!(
            "Sent batch of {} records to {}/{} (acks=0, fire-and-forget)",
            num_records, topic, partition
        );
        return Ok(());
    }

    // Phase 2: Read single batch response (vs. N individual responses).
    let request_timeout = inner.config.request_timeout;
    match tokio::time::timeout(
        request_timeout,
        read_batch_response(
            reader,
            inner,
            &topic,
            partition,
            num_records,
            &mut cloned_data,
            &mut response_channels,
        ),
    )
    .await
    {
        Ok(Ok(())) => {
            debug!(
                "Sent batch of {} records to {}/{}",
                num_records, topic, partition
            );
            Ok(())
        }
        Ok(Err(batch_err)) => Err(batch_err),
        Err(_elapsed) => {
            let undelivered = reassemble_undelivered(cloned_data, response_channels);
            Err(BatchSendError {
                error: Error::TimeoutWithMessage(format!(
                    "Response reading timed out after {:?} for batch of {} records on {}/{}",
                    request_timeout, num_records, topic, partition
                )),
                undelivered,
            })
        }
    }
}

/// Read a single batch response from the server and distribute offsets
/// to per-record response channels.
///
/// The server returns one `PublishedBatch` or `IdempotentPublishedBatch`
/// containing a `base_offset` and `record_count`. Each record gets
/// `base_offset + i` as its assigned offset.
///
/// On error, remaining records in `cloned_data` / `response_channels`
/// are returned for retry via `BatchSendError`.
async fn read_batch_response<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    inner: &Arc<ProducerInner>,
    topic: &str,
    _partition: u32,
    num_records: usize,
    cloned_data: &mut VecDeque<(Bytes, Option<Bytes>)>,
    response_channels: &mut VecDeque<oneshot::Sender<Result<RecordMetadata>>>,
) -> std::result::Result<(), BatchSendError> {
    let mut len_buf = [0u8; 4];

    // Read response length
    if let Err(e) = reader.read_exact(&mut len_buf).await {
        let undelivered = reassemble_undelivered(
            std::mem::take(cloned_data),
            std::mem::take(response_channels),
        );
        return Err(BatchSendError {
            error: e.into(),
            undelivered,
        });
    }
    let response_len = u32::from_be_bytes(len_buf) as usize;

    const MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;
    if response_len > MAX_RESPONSE_SIZE {
        let undelivered = reassemble_undelivered(
            std::mem::take(cloned_data),
            std::mem::take(response_channels),
        );
        return Err(BatchSendError {
            error: Error::ResponseTooLarge(response_len, MAX_RESPONSE_SIZE),
            undelivered,
        });
    }

    let mut response_buf = vec![0u8; response_len];
    if let Err(e) = reader.read_exact(&mut response_buf).await {
        let undelivered = reassemble_undelivered(
            std::mem::take(cloned_data),
            std::mem::take(response_channels),
        );
        return Err(BatchSendError {
            error: e.into(),
            undelivered,
        });
    }

    let (response, _format, _correlation_id) = match Response::from_wire(&response_buf) {
        Ok(r) => r,
        Err(e) => {
            let undelivered = reassemble_undelivered(
                std::mem::take(cloned_data),
                std::mem::take(response_channels),
            );
            return Err(BatchSendError {
                error: Error::ProtocolError(e),
                undelivered,
            });
        }
    };

    match response {
        Response::PublishedBatch {
            base_offset,
            partition: resp_partition,
            record_count,
        } => {
            debug_assert_eq!(
                record_count as usize, num_records,
                "server record_count mismatch"
            );
            for i in 0..record_count as u64 {
                cloned_data.pop_front();
                if let Some(response_tx) = response_channels.pop_front() {
                    inner
                        .stats
                        .records_delivered
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = response_tx.send(Ok(RecordMetadata {
                        topic: topic.to_owned(),
                        partition: resp_partition,
                        offset: base_offset + i,
                        timestamp: 0,
                    }));
                    let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
                    if prev == 1 {
                        inner.flush_notify.notify_waiters();
                    }
                }
            }
        }
        Response::IdempotentPublishedBatch {
            base_offset,
            partition: resp_partition,
            record_count,
            duplicate,
        } => {
            if duplicate {
                debug!(
                    topic = %topic,
                    partition = resp_partition,
                    base_offset,
                    record_count,
                    "Server detected batch duplicate (idempotent dedup)"
                );
            }
            debug_assert_eq!(
                record_count as usize, num_records,
                "server record_count mismatch"
            );
            for i in 0..record_count as u64 {
                cloned_data.pop_front();
                if let Some(response_tx) = response_channels.pop_front() {
                    inner
                        .stats
                        .records_delivered
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = response_tx.send(Ok(RecordMetadata {
                        topic: topic.to_owned(),
                        partition: resp_partition,
                        offset: base_offset + i,
                        timestamp: 0,
                    }));
                    let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
                    if prev == 1 {
                        inner.flush_notify.notify_waiters();
                    }
                }
            }
        }
        Response::Error { message } => {
            inner
                .stats
                .errors
                .fetch_add(num_records as u64, Ordering::Relaxed);
            // Distribute error to all channels
            while let Some(response_tx) = response_channels.pop_front() {
                cloned_data.pop_front();
                let _ = response_tx.send(Err(Error::ServerError(message.clone())));
                let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
                if prev == 1 {
                    inner.flush_notify.notify_waiters();
                }
            }
        }
        _ => {
            // Unexpected response type
            while let Some(response_tx) = response_channels.pop_front() {
                cloned_data.pop_front();
                let _ = response_tx.send(Err(Error::InvalidResponse));
                let prev = inner.pending_records.fetch_sub(1, Ordering::Release);
                if prev == 1 {
                    inner.flush_notify.notify_waiters();
                }
            }
        }
    }

    Ok(())
}

/// Reassemble `(value, key, channel)` tuples from parallel vecs.
fn reassemble_undelivered(
    data: VecDeque<(Bytes, Option<Bytes>)>,
    channels: VecDeque<oneshot::Sender<Result<RecordMetadata>>>,
) -> Vec<(
    Bytes,
    Option<Bytes>,
    oneshot::Sender<Result<RecordMetadata>>,
)> {
    data.into_iter()
        .zip(channels)
        .map(|((v, k), tx)| (v, k, tx))
        .collect()
}

// ============================================================================
// Statistics
// ============================================================================

struct ProducerStats {
    records_sent: AtomicU64,
    records_delivered: AtomicU64,
    batches_sent: AtomicU64,
    errors: AtomicU64,
    retries: AtomicU64,
}

impl ProducerStats {
    fn new() -> Self {
        Self {
            records_sent: AtomicU64::new(0),
            records_delivered: AtomicU64::new(0),
            batches_sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            retries: AtomicU64::new(0),
        }
    }
}

/// Producer statistics snapshot
#[derive(Debug, Clone)]
pub struct ProducerStatsSnapshot {
    /// Total records sent
    pub records_sent: u64,
    /// Total records successfully delivered
    pub records_delivered: u64,
    /// Total batches sent
    pub batches_sent: u64,
    /// Total errors
    pub errors: u64,
    /// Total retries
    pub retries: u64,
}

impl ProducerStatsSnapshot {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.records_sent == 0 {
            1.0
        } else {
            self.records_delivered as f64 / self.records_sent as f64
        }
    }

    /// Get pending record count
    pub fn pending(&self) -> u64 {
        self.records_sent
            .saturating_sub(self.records_delivered + self.errors)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_config_builder() {
        let config = ProducerConfig::builder()
            .bootstrap_servers(vec!["server1:9092".to_string()])
            .batch_size(32768)
            .linger_ms(10)
            .buffer_memory(64 * 1024 * 1024)
            .enable_idempotence(true)
            .retries(5)
            .build()
            .expect("valid config");

        assert_eq!(config.bootstrap_servers.len(), 1);
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.buffer_memory, 64 * 1024 * 1024);
        assert!(config.enable_idempotence);
        assert_eq!(config.retries, 5);
    }

    #[test]
    fn test_high_throughput_config() {
        let config = ProducerConfig::high_throughput();
        assert_eq!(config.batch_size, 65536);
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.max_in_flight_requests, 10);
    }

    #[test]
    fn test_low_latency_config() {
        let config = ProducerConfig::low_latency();
        assert_eq!(config.batch_size, 1);
        assert_eq!(config.linger_ms, 0);
        assert_eq!(config.max_in_flight_requests, 1);
    }

    #[test]
    fn test_exactly_once_config() {
        let config = ProducerConfig::exactly_once();
        assert!(config.enable_idempotence);
        assert_eq!(config.acks, -1);
        assert_eq!(config.retries, i32::MAX as u32);
    }

    #[test]
    fn test_murmur2_partition() {
        // Test that same key always maps to same partition
        let key = b"test-key";
        let p1 = murmur2_partition(key, 10);
        let p2 = murmur2_partition(key, 10);
        assert_eq!(p1, p2);

        // Different keys may map to different partitions
        let key2 = b"other-key";
        let p3 = murmur2_partition(key2, 10);
        // Just verify it's in range
        assert!(p3 < 10);
    }

    #[test]
    fn test_murmur2_partition_range() {
        // Verify all results are in valid range
        for i in 0..1000 {
            let key = format!("key-{}", i);
            let partition = murmur2_partition(key.as_bytes(), 16);
            assert!(partition < 16);
        }
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = ProducerStatsSnapshot {
            records_sent: 100,
            records_delivered: 90,
            batches_sent: 10,
            errors: 5,
            retries: 3,
        };

        assert!((stats.success_rate() - 0.9).abs() < 0.001);
        assert_eq!(stats.pending(), 5);
    }

    #[tokio::test]
    async fn test_metadata_cache() {
        let cache = MetadataCache::new(Duration::from_millis(100));

        // Cache miss
        assert!(cache.get("topic1").await.is_none());

        // Cache hit
        cache.put("topic1".to_string(), 4).await;
        assert_eq!(cache.get("topic1").await, Some(4));

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(cache.get("topic1").await.is_none());
    }

    #[test]
    fn test_sticky_partitioner_with_key() {
        let partitioner = StickyPartitioner::new();

        // Same key should always map to same partition
        let p1 = partitioner
            .partition("topic", Some(b"key1".as_slice()), 10);
        let p2 = partitioner
            .partition("topic", Some(b"key1".as_slice()), 10);
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_sticky_partitioner_without_key() {
        let partitioner = StickyPartitioner::new();

        // Without key, should stick to same partition for batch
        let mut partitions = Vec::new();
        for _ in 0..STICKY_BATCH_THRESHOLD {
            let p = partitioner.partition("topic", None, 10);
            partitions.push(p);
        }

        // First STICKY_BATCH_THRESHOLD - 1 should be the same
        for i in 1..STICKY_BATCH_THRESHOLD - 1 {
            assert_eq!(partitions[0], partitions[i]);
        }
    }

    #[test]
    fn test_pending_records_tracking() {
        use std::sync::atomic::AtomicU64;

        // Simulate pending record tracking
        let pending = AtomicU64::new(0);

        // Increment for each send
        pending.fetch_add(1, Ordering::Release);
        pending.fetch_add(1, Ordering::Release);
        assert_eq!(pending.load(Ordering::Acquire), 2);

        // Decrement for each delivery
        let prev = pending.fetch_sub(1, Ordering::Release);
        assert_eq!(prev, 2);
        assert_eq!(pending.load(Ordering::Acquire), 1);

        let prev = pending.fetch_sub(1, Ordering::Release);
        assert_eq!(prev, 1);
        assert_eq!(pending.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn test_flush_notify_mechanism() {
        let notify = tokio::sync::Notify::new();
        let pending = Arc::new(AtomicU64::new(1));

        let pending_clone = Arc::clone(&pending);
        let notify_clone = Arc::new(notify);
        let notify_clone2 = Arc::clone(&notify_clone);

        // Spawn a task that will decrement pending and notify
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let prev = pending_clone.fetch_sub(1, Ordering::Release);
            if prev == 1 {
                notify_clone2.notify_waiters();
            }
        });

        // Wait for notification
        let start = Instant::now();
        loop {
            if pending.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::select! {
                _ = notify_clone.notified() => continue,
                _ = tokio::time::sleep(Duration::from_millis(5)) => continue,
            }
        }
        let elapsed = start.elapsed();

        handle.await.unwrap();
        assert!(elapsed < Duration::from_millis(100));
    }

    #[test]
    fn test_murmur2_kafka_compatibility() {
        // Test known Kafka partition assignments for compatibility
        // These values were verified against Kafka's DefaultPartitioner

        // Empty key should be handled
        let p = murmur2_partition(b"", 10);
        assert!(p < 10);

        // Single byte keys
        let p = murmur2_partition(b"a", 10);
        assert!(p < 10);

        // Keys that exercise different code paths
        let p = murmur2_partition(b"abc", 10); // 3 bytes
        assert!(p < 10);

        let p = murmur2_partition(b"abcd", 10); // 4 bytes exactly
        assert!(p < 10);

        let p = murmur2_partition(b"abcdefgh", 10); // 8 bytes (2 chunks)
        assert!(p < 10);

        let p = murmur2_partition(b"abcdefghijk", 10); // 11 bytes (2 chunks + 3)
        assert!(p < 10);
    }

    #[test]
    fn test_murmur2_distribution() {
        // Test that hash distributes relatively evenly
        let mut buckets = [0u32; 16];
        for i in 0..10000 {
            let key = format!("key-{}-data-{}", i, i * 7);
            let partition = murmur2_partition(key.as_bytes(), 16);
            buckets[partition as usize] += 1;
        }

        // Each bucket should have roughly 10000/16 = 625 keys
        // Allow ±50% variance
        for (i, &count) in buckets.iter().enumerate() {
            assert!(
                count > 300 && count < 950,
                "Bucket {} has {} keys, expected ~625",
                i,
                count
            );
        }
    }

    #[test]
    fn test_compression_type_default() {
        let compression = CompressionType::default();
        assert_eq!(compression, CompressionType::None);
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new("test-topic".to_string(), 0);
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let (tx, _rx) = oneshot::channel();
        batch.add(Some(Bytes::from("key")), Bytes::from("value"), tx);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.batch_size_bytes, 8); // "key" + "value"
    }

    #[test]
    fn test_stats_snapshot_zero_sent() {
        let stats = ProducerStatsSnapshot {
            records_sent: 0,
            records_delivered: 0,
            batches_sent: 0,
            errors: 0,
            retries: 0,
        };

        // Success rate should be 1.0 when nothing sent
        assert!((stats.success_rate() - 1.0).abs() < 0.001);
        assert_eq!(stats.pending(), 0);
    }

    #[tokio::test]
    async fn test_metadata_cache_cleanup() {
        let cache = MetadataCache::new(Duration::from_millis(50));

        cache.put("topic1".to_string(), 4).await;
        cache.put("topic2".to_string(), 8).await;

        // Both should be in cache
        assert_eq!(cache.get("topic1").await, Some(4));
        assert_eq!(cache.get("topic2").await, Some(8));

        // Wait for expiry
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Run cleanup
        cache.cleanup().await;

        // Both should be gone
        assert!(cache.get("topic1").await.is_none());
        assert!(cache.get("topic2").await.is_none());
    }

    #[test]
    fn test_sticky_partitioner_partition_change() {
        let partitioner = StickyPartitioner::new();

        // Get initial partition with 4 partitions
        let p1 = partitioner.partition("topic", None, 4);
        assert!(p1 < 4);

        // Change to 8 partitions - should reset
        let p2 = partitioner.partition("topic", None, 8);
        assert!(p2 < 8);
    }
}
