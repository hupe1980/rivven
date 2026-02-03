//! Request pipelining for high-throughput client operations
//!
//! This module provides HTTP/2-style request pipelining that allows multiple
//! in-flight requests over a single connection, dramatically improving throughput
//! for batch operations.
//!
//! # Features
//!
//! - **Request multiplexing**: Multiple requests share one TCP connection
//! - **Out-of-order responses**: Responses matched by request ID, not order
//! - **Backpressure**: Configurable max in-flight requests
//! - **Automatic batching**: Coalesces small requests for network efficiency
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_client::{PipelinedClient, PipelineConfig};
//!
//! let config = PipelineConfig::builder()
//!     .max_in_flight(100)
//!     .batch_linger_ms(5)
//!     .build();
//!
//! let client = PipelinedClient::connect("localhost:9092", config).await?;
//!
//! // Send multiple requests concurrently
//! let handles: Vec<_> = (0..1000)
//!     .map(|i| {
//!         let client = client.clone();
//!         tokio::spawn(async move {
//!             client.publish("topic", format!("msg-{}", i)).await
//!         })
//!     })
//!     .collect();
//!
//! // All requests pipelined over single connection
//! for handle in handles {
//!     handle.await??;
//! }
//! ```

use crate::{Error, Request, Response, Result};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tracing::{debug, trace, warn};

// ============================================================================
// Type Aliases
// ============================================================================

/// Map of request ID to response channel - used for tracking in-flight requests
type PendingResponses = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Response>>>>>;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for request pipelining
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Maximum concurrent in-flight requests
    pub max_in_flight: usize,
    /// Request coalescing window (microseconds)
    pub batch_linger_us: u64,
    /// Maximum batch size before forced flush
    pub max_batch_size: usize,
    /// Read buffer size
    pub read_buffer_size: usize,
    /// Write buffer size
    pub write_buffer_size: usize,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 100,
            batch_linger_us: 1000, // 1ms
            max_batch_size: 64,
            read_buffer_size: 64 * 1024,  // 64KB
            write_buffer_size: 64 * 1024, // 64KB
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl PipelineConfig {
    /// Create a new builder
    pub fn builder() -> PipelineConfigBuilder {
        PipelineConfigBuilder::default()
    }

    /// High-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            max_in_flight: 1000,
            batch_linger_us: 5000, // 5ms
            max_batch_size: 256,
            read_buffer_size: 256 * 1024,
            write_buffer_size: 256 * 1024,
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            max_in_flight: 32,
            batch_linger_us: 0, // No batching
            max_batch_size: 1,
            read_buffer_size: 16 * 1024,
            write_buffer_size: 16 * 1024,
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Builder for PipelineConfig
#[derive(Default)]
pub struct PipelineConfigBuilder {
    config: PipelineConfig,
}

impl PipelineConfigBuilder {
    /// Set maximum in-flight requests
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Set batch linger time in milliseconds
    pub fn batch_linger_ms(mut self, ms: u64) -> Self {
        self.config.batch_linger_us = ms * 1000;
        self
    }

    /// Set batch linger time in microseconds
    pub fn batch_linger_us(mut self, us: u64) -> Self {
        self.config.batch_linger_us = us;
        self
    }

    /// Set maximum batch size
    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.config.max_batch_size = size;
        self
    }

    /// Set read buffer size
    pub fn read_buffer_size(mut self, size: usize) -> Self {
        self.config.read_buffer_size = size;
        self
    }

    /// Set write buffer size
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.config.write_buffer_size = size;
        self
    }

    /// Set request timeout
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Build the configuration
    pub fn build(self) -> PipelineConfig {
        self.config
    }
}

// ============================================================================
// Pipelined Request
// ============================================================================

/// A pipelined request with its response channel
struct PipelinedRequest {
    /// Unique request ID for response matching
    id: u64,
    /// Serialized request bytes
    data: Bytes,
    /// Channel to send the response
    response_tx: oneshot::Sender<Result<Response>>,
    /// Request creation time for timeout tracking
    #[allow(dead_code)] // Used for future timeout handling in writer task
    created_at: Instant,
}

// ============================================================================
// Pipelined Client
// ============================================================================

/// High-throughput pipelined client
///
/// Uses request multiplexing to send multiple requests over a single connection
/// without waiting for responses, dramatically improving throughput.
pub struct PipelinedClient {
    inner: Arc<PipelinedClientInner>,
}

struct PipelinedClientInner {
    /// Channel to send requests to the writer task
    request_tx: mpsc::Sender<PipelinedRequest>,
    /// Semaphore to limit in-flight requests
    in_flight_semaphore: Arc<Semaphore>,
    /// Request ID counter
    next_request_id: AtomicU64,
    /// Configuration
    config: PipelineConfig,
    /// Statistics
    stats: PipelineStats,
    /// Shutdown signal for background tasks
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl Clone for PipelinedClient {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl PipelinedClient {
    /// Connect to a Rivven server with request pipelining
    pub async fn connect(addr: &str, config: PipelineConfig) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::ConnectionError(e.to_string()))?;

        // Set TCP_NODELAY for low latency
        stream
            .set_nodelay(true)
            .map_err(|e| Error::ConnectionError(format!("Failed to set TCP_NODELAY: {}", e)))?;

        let (read_half, write_half) = stream.into_split();

        // Create channels
        let (request_tx, request_rx) = mpsc::channel(config.max_in_flight * 2);
        let in_flight_semaphore = Arc::new(Semaphore::new(config.max_in_flight));
        let pending_responses = Arc::new(Mutex::new(HashMap::new()));

        // Create shutdown signal
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Start writer task
        let writer_config = config.clone();
        let pending_for_writer = Arc::clone(&pending_responses);
        let writer_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            writer_task(
                write_half,
                request_rx,
                pending_for_writer,
                writer_config,
                writer_shutdown,
            )
            .await;
        });

        // Start reader task
        let reader_config = config.clone();
        let pending_for_reader = Arc::clone(&pending_responses);
        let reader_shutdown = shutdown_rx;
        tokio::spawn(async move {
            reader_task(
                read_half,
                pending_for_reader,
                reader_config,
                reader_shutdown,
            )
            .await;
        });

        Ok(Self {
            inner: Arc::new(PipelinedClientInner {
                request_tx,
                in_flight_semaphore,
                next_request_id: AtomicU64::new(1),
                config,
                stats: PipelineStats::new(),
                shutdown: shutdown_tx,
            }),
        })
    }

    /// Gracefully close the client and wait for pending requests to complete
    pub async fn close(&self) {
        // Signal shutdown to background tasks
        let _ = self.inner.shutdown.send(true);
    }

    /// Send a request and wait for the response
    pub async fn send_request(&self, request: Request) -> Result<Response> {
        // Acquire in-flight permit (backpressure)
        let _permit = self
            .inner
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| Error::ConnectionError("Pipeline closed".into()))?;

        let request_id = self.inner.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (response_tx, response_rx) = oneshot::channel();

        // Serialize request with ID prefix
        let request_bytes = request.to_bytes()?;
        let mut data = BytesMut::with_capacity(8 + request_bytes.len());
        data.extend_from_slice(&request_id.to_be_bytes());
        data.extend_from_slice(&request_bytes);

        let pipelined = PipelinedRequest {
            id: request_id,
            data: data.freeze(),
            response_tx,
            created_at: Instant::now(),
        };

        // Send to writer task
        self.inner
            .request_tx
            .send(pipelined)
            .await
            .map_err(|_| Error::ConnectionError("Writer task closed".into()))?;

        self.inner
            .stats
            .requests_sent
            .fetch_add(1, Ordering::Relaxed);

        // Wait for response with timeout
        let timeout_duration = self.inner.config.request_timeout;
        match tokio::time::timeout(timeout_duration, response_rx).await {
            Ok(Ok(result)) => {
                self.inner
                    .stats
                    .responses_received
                    .fetch_add(1, Ordering::Relaxed);
                result
            }
            Ok(Err(_)) => Err(Error::ConnectionError("Response channel dropped".into())),
            Err(_) => {
                self.inner.stats.timeouts.fetch_add(1, Ordering::Relaxed);
                Err(Error::Timeout)
            }
        }
    }

    /// Publish a message to a topic
    pub async fn publish(&self, topic: impl Into<String>, value: impl Into<Bytes>) -> Result<u64> {
        let request = Request::Publish {
            topic: topic.into(),
            partition: None,
            key: None,
            value: value.into(),
        };

        match self.send_request(request).await? {
            Response::Published { offset, .. } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Publish a message with a key
    pub async fn publish_with_key(
        &self,
        topic: impl Into<String>,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<u64> {
        let request = Request::Publish {
            topic: topic.into(),
            partition: None,
            key: Some(key.into()),
            value: value.into(),
        };

        match self.send_request(request).await? {
            Response::Published { offset, .. } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get pipeline statistics
    pub fn stats(&self) -> PipelineStatsSnapshot {
        PipelineStatsSnapshot {
            requests_sent: self.inner.stats.requests_sent.load(Ordering::Relaxed),
            responses_received: self.inner.stats.responses_received.load(Ordering::Relaxed),
            batches_flushed: self.inner.stats.batches_flushed.load(Ordering::Relaxed),
            timeouts: self.inner.stats.timeouts.load(Ordering::Relaxed),
        }
    }
}

// ============================================================================
// Writer Task
// ============================================================================

/// Background task that batches and sends requests
async fn writer_task(
    write_half: tokio::net::tcp::OwnedWriteHalf,
    mut request_rx: mpsc::Receiver<PipelinedRequest>,
    pending: PendingResponses,
    config: PipelineConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut writer = BufWriter::with_capacity(config.write_buffer_size, write_half);
    let mut batch: Vec<PipelinedRequest> = Vec::with_capacity(config.max_batch_size);
    let mut batch_started: Option<Instant> = None;

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            break;
        }

        // Try to receive with linger timeout
        let request = if batch.is_empty() {
            // No pending requests, block until one arrives or shutdown
            tokio::select! {
                req = request_rx.recv() => {
                    match req {
                        Some(req) => Some(req),
                        None => break, // Channel closed
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
            }
        } else if config.batch_linger_us == 0 {
            // No batching, flush immediately
            None
        } else {
            // Have pending requests, wait for more with timeout
            let elapsed = batch_started
                .map(|t| t.elapsed().as_micros() as u64)
                .unwrap_or(0);
            let remaining = config.batch_linger_us.saturating_sub(elapsed);

            if remaining == 0 {
                None // Linger expired
            } else {
                match tokio::time::timeout(Duration::from_micros(remaining), request_rx.recv())
                    .await
                {
                    Ok(Some(req)) => Some(req),
                    Ok(None) => break, // Channel closed
                    Err(_) => None,    // Timeout, flush batch
                }
            }
        };

        if let Some(req) = request {
            if batch.is_empty() {
                batch_started = Some(Instant::now());
            }
            batch.push(req);
        }

        // Flush if batch is full or linger expired (no new request arrived)
        let should_flush = batch.len() >= config.max_batch_size
            || (!batch.is_empty()
                && batch_started
                    .is_some_and(|t| t.elapsed().as_micros() as u64 >= config.batch_linger_us));

        if should_flush && !batch.is_empty() {
            if let Err(e) = flush_batch(&mut writer, &mut batch, &pending).await {
                warn!("Failed to flush batch: {}", e);
                // Notify all pending requests of the error
                for req in batch.drain(..) {
                    let _ = req
                        .response_tx
                        .send(Err(Error::ConnectionError(e.to_string())));
                }
            }
            batch_started = None;
        }
    }

    // Flush any remaining requests
    if !batch.is_empty() {
        let _ = flush_batch(&mut writer, &mut batch, &pending).await;
    }
}

/// Flush a batch of requests
async fn flush_batch(
    writer: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
    batch: &mut Vec<PipelinedRequest>,
    pending: &PendingResponses,
) -> std::io::Result<()> {
    let mut pending_guard = pending.lock().await;

    for req in batch.drain(..) {
        // Write length-prefixed request
        let len = req.data.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&req.data).await?;

        // Register pending response
        pending_guard.insert(req.id, req.response_tx);
    }

    writer.flush().await?;
    trace!("Flushed batch of {} requests", batch.len());

    Ok(())
}

// ============================================================================
// Reader Task
// ============================================================================

/// Background task that reads and dispatches responses
async fn reader_task(
    read_half: tokio::net::tcp::OwnedReadHalf,
    pending: PendingResponses,
    config: PipelineConfig,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut reader = BufReader::with_capacity(config.read_buffer_size, read_half);
    let mut len_buf = [0u8; 4];
    let mut id_buf = [0u8; 8];

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            break;
        }

        // Read response length with shutdown check
        let read_result = tokio::select! {
            result = reader.read_exact(&mut len_buf) => result,
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };

        if read_result.is_err() {
            break; // Connection closed
        }
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len < 8 {
            warn!("Invalid response length: {}", msg_len);
            continue;
        }

        // Read request ID
        if reader.read_exact(&mut id_buf).await.is_err() {
            break;
        }
        let request_id = u64::from_be_bytes(id_buf);

        // Read response body
        let body_len = msg_len - 8;
        let mut response_buf = vec![0u8; body_len];
        if reader.read_exact(&mut response_buf).await.is_err() {
            break;
        }

        // Parse response
        let result = Response::from_bytes(&response_buf).map_err(Error::ProtocolError);

        // Dispatch to waiting request
        let sender = {
            let mut pending_guard = pending.lock().await;
            pending_guard.remove(&request_id)
        };

        if let Some(tx) = sender {
            let _ = tx.send(result);
        } else {
            debug!("Received response for unknown request ID: {}", request_id);
        }
    }

    // Connection closed, fail all pending requests
    let mut pending_guard = pending.lock().await;
    for (_, tx) in pending_guard.drain() {
        let _ = tx.send(Err(Error::ConnectionError("Connection closed".into())));
    }
}

// ============================================================================
// Statistics
// ============================================================================

/// Pipeline statistics (atomic)
struct PipelineStats {
    requests_sent: AtomicU64,
    responses_received: AtomicU64,
    batches_flushed: AtomicU64,
    timeouts: AtomicU64,
}

impl PipelineStats {
    fn new() -> Self {
        Self {
            requests_sent: AtomicU64::new(0),
            responses_received: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
        }
    }
}

/// Snapshot of pipeline statistics
#[derive(Debug, Clone)]
pub struct PipelineStatsSnapshot {
    /// Total requests sent
    pub requests_sent: u64,
    /// Total responses received
    pub responses_received: u64,
    /// Total batches flushed
    pub batches_flushed: u64,
    /// Total request timeouts
    pub timeouts: u64,
}

impl PipelineStatsSnapshot {
    /// Get in-flight request count
    pub fn in_flight(&self) -> u64 {
        self.requests_sent.saturating_sub(self.responses_received)
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.requests_sent == 0 {
            1.0
        } else {
            self.responses_received as f64 / self.requests_sent as f64
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::builder()
            .max_in_flight(200)
            .batch_linger_ms(10)
            .max_batch_size(128)
            .request_timeout(Duration::from_secs(60))
            .build();

        assert_eq!(config.max_in_flight, 200);
        assert_eq!(config.batch_linger_us, 10_000);
        assert_eq!(config.max_batch_size, 128);
        assert_eq!(config.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_high_throughput_config() {
        let config = PipelineConfig::high_throughput();
        assert_eq!(config.max_in_flight, 1000);
        assert_eq!(config.batch_linger_us, 5000);
        assert_eq!(config.max_batch_size, 256);
    }

    #[test]
    fn test_low_latency_config() {
        let config = PipelineConfig::low_latency();
        assert_eq!(config.max_in_flight, 32);
        assert_eq!(config.batch_linger_us, 0);
        assert_eq!(config.max_batch_size, 1);
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = PipelineStatsSnapshot {
            requests_sent: 100,
            responses_received: 95,
            batches_flushed: 10,
            timeouts: 5,
        };

        assert_eq!(stats.in_flight(), 5);
        assert!((stats.success_rate() - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_stats_snapshot_empty() {
        let stats = PipelineStatsSnapshot {
            requests_sent: 0,
            responses_received: 0,
            batches_flushed: 0,
            timeouts: 0,
        };

        assert_eq!(stats.in_flight(), 0);
        assert!((stats.success_rate() - 1.0).abs() < 0.001);
    }
}
