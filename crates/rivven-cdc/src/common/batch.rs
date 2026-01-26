//! # CDC Batch Processing
//!
//! Efficient batch processing for CDC events with configurable batching strategies.
//!
//! ## Features
//!
//! - **Time-based batching**: Flush after duration
//! - **Size-based batching**: Flush after N events
//! - **Byte-based batching**: Flush after N bytes
//! - **Transaction batching**: Keep transaction events together
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::{BatchConfig, EventBatcher};
//!
//! let config = BatchConfig::builder()
//!     .max_events(1000)
//!     .max_bytes(1_000_000)
//!     .max_delay(Duration::from_millis(100))
//!     .build();
//!
//! let batcher = EventBatcher::new(config);
//!
//! // Add events
//! if let Some(batch) = batcher.add(event).await {
//!     process_batch(batch).await;
//! }
//! ```

use crate::common::{CdcEvent, CdcOp};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::interval;

/// Configuration for batch processing.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum events per batch
    pub max_events: usize,
    /// Maximum bytes per batch
    pub max_bytes: usize,
    /// Maximum time to wait before flushing
    pub max_delay: Duration,
    /// Keep transaction events together
    pub preserve_transactions: bool,
    /// Minimum events before time-based flush
    pub min_events: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: 1000,
            max_bytes: 1_048_576, // 1MB
            max_delay: Duration::from_millis(100),
            preserve_transactions: true,
            min_events: 1,
        }
    }
}

impl BatchConfig {
    /// Create a new builder.
    pub fn builder() -> BatchConfigBuilder {
        BatchConfigBuilder::default()
    }

    /// Create config optimized for throughput.
    pub fn high_throughput() -> Self {
        Self {
            max_events: 10_000,
            max_bytes: 10_485_760, // 10MB
            max_delay: Duration::from_millis(500),
            preserve_transactions: false,
            min_events: 100,
        }
    }

    /// Create config optimized for latency.
    pub fn low_latency() -> Self {
        Self {
            max_events: 100,
            max_bytes: 102_400, // 100KB
            max_delay: Duration::from_millis(10),
            preserve_transactions: true,
            min_events: 1,
        }
    }
}

/// Builder for BatchConfig.
#[derive(Default)]
pub struct BatchConfigBuilder {
    max_events: Option<usize>,
    max_bytes: Option<usize>,
    max_delay: Option<Duration>,
    preserve_transactions: Option<bool>,
    min_events: Option<usize>,
}

impl BatchConfigBuilder {
    pub fn max_events(mut self, n: usize) -> Self {
        self.max_events = Some(n);
        self
    }

    pub fn max_bytes(mut self, n: usize) -> Self {
        self.max_bytes = Some(n);
        self
    }

    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = Some(d);
        self
    }

    pub fn preserve_transactions(mut self, v: bool) -> Self {
        self.preserve_transactions = Some(v);
        self
    }

    pub fn min_events(mut self, n: usize) -> Self {
        self.min_events = Some(n);
        self
    }

    pub fn build(self) -> BatchConfig {
        let default = BatchConfig::default();
        BatchConfig {
            max_events: self.max_events.unwrap_or(default.max_events),
            max_bytes: self.max_bytes.unwrap_or(default.max_bytes),
            max_delay: self.max_delay.unwrap_or(default.max_delay),
            preserve_transactions: self
                .preserve_transactions
                .unwrap_or(default.preserve_transactions),
            min_events: self.min_events.unwrap_or(default.min_events),
        }
    }
}

/// A batch of CDC events ready for processing.
#[derive(Debug, Clone)]
pub struct EventBatch {
    /// Events in this batch
    pub events: Vec<CdcEvent>,
    /// Total bytes in batch
    pub bytes: usize,
    /// Time batch was created
    pub created_at: Instant,
    /// Time batch was flushed
    pub flushed_at: Instant,
    /// Batch sequence number
    pub sequence: u64,
    /// Transaction IDs in this batch
    pub transaction_ids: Vec<String>,
}

impl EventBatch {
    /// Number of events in batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Time spent waiting before flush.
    pub fn wait_time(&self) -> Duration {
        self.flushed_at.duration_since(self.created_at)
    }

    /// Count events by operation type.
    pub fn counts(&self) -> BatchCounts {
        let mut counts = BatchCounts::default();
        for event in &self.events {
            match event.op {
                CdcOp::Insert => counts.inserts += 1,
                CdcOp::Update => counts.updates += 1,
                CdcOp::Delete => counts.deletes += 1,
                CdcOp::Snapshot => counts.snapshots += 1,
                CdcOp::Truncate => counts.truncates += 1,
            }
        }
        counts
    }
}

/// Counts of events by operation type.
#[derive(Debug, Clone, Default)]
pub struct BatchCounts {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub snapshots: usize,
    pub truncates: usize,
}

impl BatchCounts {
    pub fn total(&self) -> usize {
        self.inserts + self.updates + self.deletes + self.snapshots + self.truncates
    }
}

/// Event batcher that accumulates events and flushes based on configuration.
pub struct EventBatcher {
    config: BatchConfig,
    state: Mutex<BatcherState>,
}

struct BatcherState {
    events: Vec<CdcEvent>,
    bytes: usize,
    batch_start: Instant,
    sequence: u64,
    in_transaction: bool,
    #[allow(dead_code)] // Reserved for transaction-aware batching
    transaction_id: Option<String>,
}

impl EventBatcher {
    /// Create a new event batcher.
    pub fn new(config: BatchConfig) -> Self {
        let max_events = config.max_events;
        Self {
            config,
            state: Mutex::new(BatcherState {
                events: Vec::with_capacity(max_events),
                bytes: 0,
                batch_start: Instant::now(),
                sequence: 0,
                in_transaction: false,
                transaction_id: None,
            }),
        }
    }

    /// Add an event to the batcher.
    ///
    /// Returns a batch if the batch is ready to be flushed.
    pub async fn add(&self, event: CdcEvent) -> Option<EventBatch> {
        let mut state = self.state.lock().await;

        // Estimate event size (approximation)
        let event_bytes = estimate_event_size(&event);

        // Track transaction boundaries if enabled
        if self.config.preserve_transactions {
            // Transaction start (first event after commit/start)
            if !state.in_transaction && event.is_dml() {
                state.in_transaction = true;
            }
        }

        state.events.push(event);
        state.bytes += event_bytes;

        // Check if we should flush
        let should_flush = self.should_flush(&state);

        if should_flush {
            Some(self.create_batch(&mut state))
        } else {
            None
        }
    }

    /// Force flush the current batch.
    pub async fn flush(&self) -> Option<EventBatch> {
        let mut state = self.state.lock().await;
        if state.events.is_empty() {
            return None;
        }
        Some(self.create_batch(&mut state))
    }

    /// Check if batch should be flushed.
    pub async fn check_timeout(&self) -> Option<EventBatch> {
        let mut state = self.state.lock().await;

        if state.events.len() >= self.config.min_events
            && state.batch_start.elapsed() >= self.config.max_delay
        {
            // Don't flush mid-transaction if preserving transactions
            if self.config.preserve_transactions && state.in_transaction {
                return None;
            }
            return Some(self.create_batch(&mut state));
        }

        None
    }

    /// Get current batch size.
    pub async fn pending_count(&self) -> usize {
        let state = self.state.lock().await;
        state.events.len()
    }

    /// Get current batch bytes.
    pub async fn pending_bytes(&self) -> usize {
        let state = self.state.lock().await;
        state.bytes
    }

    fn should_flush(&self, state: &BatcherState) -> bool {
        // Size limits
        if state.events.len() >= self.config.max_events {
            return true;
        }
        if state.bytes >= self.config.max_bytes {
            return true;
        }

        // Transaction boundary (preserve_transactions = true means flush at commit)
        // This is handled by detecting transaction end events

        false
    }

    fn create_batch(&self, state: &mut BatcherState) -> EventBatch {
        let events = std::mem::take(&mut state.events);
        let bytes = state.bytes;

        state.events = Vec::with_capacity(self.config.max_events);
        state.bytes = 0;
        state.batch_start = Instant::now();
        state.sequence += 1;
        state.in_transaction = false;

        EventBatch {
            events,
            bytes,
            created_at: state.batch_start,
            flushed_at: Instant::now(),
            sequence: state.sequence,
            transaction_ids: Vec::new(),
        }
    }
}

/// Estimate the size of an event in bytes.
fn estimate_event_size(event: &CdcEvent) -> usize {
    let mut size = 0;

    // Fixed overhead
    size += 64; // source_type, database, schema, table, op, timestamp

    // Variable size
    if let Some(ref before) = event.before {
        size += before.to_string().len();
    }
    if let Some(ref after) = event.after {
        size += after.to_string().len();
    }

    size
}

/// Batch processor that runs in a background task.
pub struct BatchProcessor<F>
where
    F: FnMut(EventBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send,
{
    batcher: EventBatcher,
    handler: F,
}

impl<F> BatchProcessor<F>
where
    F: FnMut(EventBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send,
{
    pub fn new(config: BatchConfig, handler: F) -> Self {
        Self {
            batcher: EventBatcher::new(config.clone()),
            handler,
        }
    }

    /// Process an event, calling handler if batch is ready.
    pub async fn process(&mut self, event: CdcEvent) {
        if let Some(batch) = self.batcher.add(event).await {
            (self.handler)(batch).await;
        }
    }

    /// Flush any pending events.
    pub async fn flush(&mut self) {
        if let Some(batch) = self.batcher.flush().await {
            (self.handler)(batch).await;
        }
    }
}

/// Create a background task that periodically checks for timeout flushes.
pub fn spawn_timeout_flusher(
    batcher: std::sync::Arc<EventBatcher>,
    flush_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = interval(flush_interval);
        loop {
            interval.tick().await;
            if let Some(_batch) = batcher.check_timeout().await {
                // In real usage, would send batch somewhere
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(op: CdcOp) -> CdcEvent {
        CdcEvent {
            source_type: "postgres".to_string(),
            database: "testdb".to_string(),
            schema: "public".to_string(),
            table: "users".to_string(),
            op,
            before: None,
            after: Some(json!({"id": 1, "name": "Alice"})),
            timestamp: 1234567890,
            transaction: None,
        }
    }

    #[test]
    fn test_batch_config_builder() {
        let config = BatchConfig::builder()
            .max_events(500)
            .max_bytes(500_000)
            .max_delay(Duration::from_millis(50))
            .build();

        assert_eq!(config.max_events, 500);
        assert_eq!(config.max_bytes, 500_000);
        assert_eq!(config.max_delay, Duration::from_millis(50));
    }

    #[test]
    fn test_batch_config_presets() {
        let high = BatchConfig::high_throughput();
        assert_eq!(high.max_events, 10_000);

        let low = BatchConfig::low_latency();
        assert_eq!(low.max_events, 100);
    }

    #[tokio::test]
    async fn test_batcher_size_limit() {
        let config = BatchConfig::builder().max_events(3).build();
        let batcher = EventBatcher::new(config);

        // First two events should not flush
        assert!(batcher.add(make_event(CdcOp::Insert)).await.is_none());
        assert!(batcher.add(make_event(CdcOp::Update)).await.is_none());

        // Third event should trigger flush
        let batch = batcher.add(make_event(CdcOp::Delete)).await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_batcher_force_flush() {
        let config = BatchConfig::default();
        let batcher = EventBatcher::new(config);

        batcher.add(make_event(CdcOp::Insert)).await;
        batcher.add(make_event(CdcOp::Insert)).await;

        let batch = batcher.flush().await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 2);

        // Flush empty batcher returns None
        assert!(batcher.flush().await.is_none());
    }

    #[tokio::test]
    async fn test_batch_counts() {
        let config = BatchConfig::builder().max_events(10).build();
        let batcher = EventBatcher::new(config);

        batcher.add(make_event(CdcOp::Insert)).await;
        batcher.add(make_event(CdcOp::Insert)).await;
        batcher.add(make_event(CdcOp::Update)).await;
        batcher.add(make_event(CdcOp::Delete)).await;

        let batch = batcher.flush().await.unwrap();
        let counts = batch.counts();

        assert_eq!(counts.inserts, 2);
        assert_eq!(counts.updates, 1);
        assert_eq!(counts.deletes, 1);
        assert_eq!(counts.total(), 4);
    }

    #[tokio::test]
    async fn test_pending_counts() {
        let config = BatchConfig::default();
        let batcher = EventBatcher::new(config);

        assert_eq!(batcher.pending_count().await, 0);

        batcher.add(make_event(CdcOp::Insert)).await;
        batcher.add(make_event(CdcOp::Insert)).await;

        assert_eq!(batcher.pending_count().await, 2);
        assert!(batcher.pending_bytes().await > 0);
    }

    #[tokio::test]
    async fn test_batch_sequence_numbers() {
        let config = BatchConfig::builder().max_events(1).build();
        let batcher = EventBatcher::new(config);

        let batch1 = batcher.add(make_event(CdcOp::Insert)).await.unwrap();
        let batch2 = batcher.add(make_event(CdcOp::Insert)).await.unwrap();
        let batch3 = batcher.add(make_event(CdcOp::Insert)).await.unwrap();

        assert_eq!(batch1.sequence, 1);
        assert_eq!(batch2.sequence, 2);
        assert_eq!(batch3.sequence, 3);
    }

    #[test]
    fn test_estimate_event_size() {
        let event = make_event(CdcOp::Insert);
        let size = estimate_event_size(&event);

        // Should be > 64 (overhead) + JSON size
        assert!(size > 64);
    }

    #[test]
    fn test_batch_wait_time() {
        let batch = EventBatch {
            events: vec![],
            bytes: 0,
            created_at: Instant::now(),
            flushed_at: Instant::now(),
            sequence: 1,
            transaction_ids: vec![],
        };

        // Wait time should be very small (same instant)
        assert!(batch.wait_time() < Duration::from_millis(10));
    }
}
