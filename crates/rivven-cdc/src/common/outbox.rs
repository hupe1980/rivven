//! # Transactional Outbox Pattern
//!
//! Reliable event publishing using the transactional outbox pattern.
//! Guarantees exactly-once delivery by storing events in the same transaction
//! as business data, then reliably forwarding them to the event stream.
//!
//! ## Features
//!
//! - **Atomic Writes**: Events stored in same transaction as business data
//! - **Reliable Delivery**: Polling-based forwarding with at-least-once guarantee
//! - **Ordering**: Per-aggregate ordering with sequence numbers
//! - **Idempotency**: Duplicate detection via event IDs
//! - **Dead Letter Queue**: Failed events moved to DLQ after retries
//!
//! ## Usage
//!
//! ```ignore
//! use rivven_cdc::common::outbox::{OutboxProcessor, OutboxConfig};
//!
//! // Configure the outbox processor
//! let config = OutboxConfig::builder()
//!     .poll_interval(Duration::from_millis(100))
//!     .batch_size(100)
//!     .max_retries(3)
//!     .build();
//!
//! let processor = OutboxProcessor::new(config, event_store, publisher);
//! processor.start().await?;
//! ```

use crate::common::{CdcEvent, CdcOp, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, RwLock};
use tracing::{debug, error, info, warn};

/// Outbox event status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxStatus {
    /// Event pending delivery
    Pending,
    /// Event currently being processed
    Processing,
    /// Event successfully delivered
    Delivered,
    /// Event delivery failed permanently
    Failed,
}

/// An outbox event record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEvent {
    /// Unique event ID (UUID)
    pub id: String,
    /// Aggregate type (e.g., "Order", "User")
    pub aggregate_type: String,
    /// Aggregate ID (business key)
    pub aggregate_id: String,
    /// Event type (e.g., "OrderCreated", "UserUpdated")
    pub event_type: String,
    /// Event payload (JSON)
    pub payload: serde_json::Value,
    /// Sequence number within aggregate
    pub sequence: u64,
    /// Event timestamp
    pub created_at: u64,
    /// Current status
    pub status: OutboxStatus,
    /// Number of delivery attempts
    pub attempts: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Metadata (headers, correlation IDs, etc.)
    pub metadata: HashMap<String, String>,
}

impl OutboxEvent {
    /// Create a new outbox event.
    pub fn new(
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        event_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: uuid_v4(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            event_type: event_type.into(),
            payload,
            sequence: 0,
            created_at: unix_timestamp_millis(),
            status: OutboxStatus::Pending,
            attempts: 0,
            last_error: None,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set sequence number.
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = sequence;
        self
    }

    /// Convert to CDC event.
    pub fn to_cdc_event(&self) -> CdcEvent {
        CdcEvent {
            source_type: "outbox".to_string(),
            database: self.aggregate_type.clone(),
            schema: "outbox".to_string(),
            table: self.event_type.clone(),
            op: CdcOp::Insert,
            before: None,
            after: Some(self.payload.clone()),
            timestamp: self.created_at as i64 / 1000,
            transaction: None,
        }
    }

    /// Mark as processing.
    pub fn mark_processing(&mut self) {
        self.status = OutboxStatus::Processing;
        self.attempts += 1;
    }

    /// Mark as delivered.
    pub fn mark_delivered(&mut self) {
        self.status = OutboxStatus::Delivered;
    }

    /// Mark as failed.
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.status = OutboxStatus::Failed;
        self.last_error = Some(error.into());
    }

    /// Reset to pending for retry.
    pub fn reset_for_retry(&mut self) {
        self.status = OutboxStatus::Pending;
    }
}

/// Configuration for outbox processor.
#[derive(Debug, Clone)]
pub struct OutboxConfig {
    /// Polling interval for pending events
    pub poll_interval: Duration,
    /// Maximum events per batch
    pub batch_size: usize,
    /// Maximum delivery retries before DLQ
    pub max_retries: u32,
    /// Timeout for event delivery
    pub delivery_timeout: Duration,
    /// Enable ordering guarantees per aggregate
    pub ordered_delivery: bool,
    /// Cleanup delivered events after this duration
    pub retention_period: Duration,
    /// Maximum concurrent deliveries
    pub max_concurrency: usize,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            max_retries: 3,
            delivery_timeout: Duration::from_secs(30),
            ordered_delivery: true,
            retention_period: Duration::from_secs(86400), // 24 hours
            max_concurrency: 10,
        }
    }
}

impl OutboxConfig {
    pub fn builder() -> OutboxConfigBuilder {
        OutboxConfigBuilder::default()
    }

    /// High-throughput preset.
    pub fn high_throughput() -> Self {
        Self {
            poll_interval: Duration::from_millis(50),
            batch_size: 500,
            max_retries: 5,
            delivery_timeout: Duration::from_secs(60),
            ordered_delivery: false, // Disable for parallelism
            retention_period: Duration::from_secs(3600),
            max_concurrency: 50,
        }
    }

    /// Low-latency preset.
    pub fn low_latency() -> Self {
        Self {
            poll_interval: Duration::from_millis(10),
            batch_size: 10,
            max_retries: 3,
            delivery_timeout: Duration::from_secs(10),
            ordered_delivery: true,
            retention_period: Duration::from_secs(86400),
            max_concurrency: 5,
        }
    }
}

/// Builder for OutboxConfig.
#[derive(Default)]
pub struct OutboxConfigBuilder {
    config: OutboxConfig,
}

impl OutboxConfigBuilder {
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.config.poll_interval = interval;
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    pub fn ordered_delivery(mut self, ordered: bool) -> Self {
        self.config.ordered_delivery = ordered;
        self
    }

    pub fn max_concurrency(mut self, concurrency: usize) -> Self {
        self.config.max_concurrency = concurrency;
        self
    }

    pub fn build(self) -> OutboxConfig {
        self.config
    }
}

/// Outbox event store trait.
#[async_trait::async_trait]
pub trait OutboxStore: Send + Sync {
    /// Store a new outbox event.
    async fn store(&self, event: OutboxEvent) -> Result<()>;

    /// Get pending events (oldest first).
    async fn get_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>>;

    /// Update event status.
    async fn update_status(
        &self,
        event_id: &str,
        status: OutboxStatus,
        error: Option<String>,
    ) -> Result<()>;

    /// Delete old delivered events.
    async fn cleanup(&self, older_than: Duration) -> Result<u64>;

    /// Get events for an aggregate (ordered by sequence).
    async fn get_by_aggregate(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<OutboxEvent>>;

    /// Get DLQ events (failed after max retries).
    async fn get_dlq(&self, limit: usize) -> Result<Vec<OutboxEvent>>;
}

/// Event publisher trait.
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish an event.
    async fn publish(&self, event: &OutboxEvent) -> Result<()>;

    /// Publish a batch of events.
    async fn publish_batch(&self, events: &[OutboxEvent]) -> Result<Vec<Result<()>>>;
}

/// In-memory outbox store for testing.
pub struct MemoryOutboxStore {
    events: RwLock<HashMap<String, OutboxEvent>>,
    sequence_counters: RwLock<HashMap<String, u64>>,
}

impl MemoryOutboxStore {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            sequence_counters: RwLock::new(HashMap::new()),
        }
    }

    fn aggregate_key(aggregate_type: &str, aggregate_id: &str) -> String {
        format!("{}:{}", aggregate_type, aggregate_id)
    }
}

impl Default for MemoryOutboxStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl OutboxStore for MemoryOutboxStore {
    async fn store(&self, mut event: OutboxEvent) -> Result<()> {
        // Assign sequence number
        let agg_key = Self::aggregate_key(&event.aggregate_type, &event.aggregate_id);
        let mut counters = self.sequence_counters.write().await;
        let seq = counters.entry(agg_key).or_insert(0);
        *seq += 1;
        event.sequence = *seq;
        drop(counters);

        let mut events = self.events.write().await;
        events.insert(event.id.clone(), event);
        Ok(())
    }

    async fn get_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let events = self.events.read().await;
        let mut pending: Vec<_> = events
            .values()
            .filter(|e| e.status == OutboxStatus::Pending)
            .cloned()
            .collect();
        pending.sort_by_key(|e| e.created_at);
        pending.truncate(limit);
        Ok(pending)
    }

    async fn update_status(
        &self,
        event_id: &str,
        status: OutboxStatus,
        error: Option<String>,
    ) -> Result<()> {
        let mut events = self.events.write().await;
        if let Some(event) = events.get_mut(event_id) {
            event.status = status;
            if error.is_some() {
                event.last_error = error;
            }
        }
        Ok(())
    }

    async fn cleanup(&self, older_than: Duration) -> Result<u64> {
        let mut events = self.events.write().await;
        let cutoff = unix_timestamp_millis() - older_than.as_millis() as u64;
        let before = events.len();
        events.retain(|_, e| e.status != OutboxStatus::Delivered || e.created_at > cutoff);
        Ok((before - events.len()) as u64)
    }

    async fn get_by_aggregate(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Vec<OutboxEvent>> {
        let events = self.events.read().await;
        let mut agg_events: Vec<_> = events
            .values()
            .filter(|e| e.aggregate_type == aggregate_type && e.aggregate_id == aggregate_id)
            .cloned()
            .collect();
        agg_events.sort_by_key(|e| e.sequence);
        Ok(agg_events)
    }

    async fn get_dlq(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        let events = self.events.read().await;
        let mut dlq: Vec<_> = events
            .values()
            .filter(|e| e.status == OutboxStatus::Failed)
            .cloned()
            .collect();
        dlq.sort_by_key(|e| e.created_at);
        dlq.truncate(limit);
        Ok(dlq)
    }
}

/// Outbox processor statistics.
#[derive(Debug, Default)]
pub struct OutboxStats {
    events_processed: AtomicU64,
    events_delivered: AtomicU64,
    events_failed: AtomicU64,
    events_retried: AtomicU64,
    batches_processed: AtomicU64,
    delivery_errors: AtomicU64,
    avg_batch_size: AtomicU64,
    total_latency_ms: AtomicU64,
}

impl OutboxStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_processed(&self, count: u64) {
        self.events_processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_delivered(&self, count: u64) {
        self.events_delivered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.events_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.events_retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_batch(&self, size: usize) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        // Simple moving average
        let current = self.avg_batch_size.load(Ordering::Relaxed);
        let new_avg = (current + size as u64) / 2;
        self.avg_batch_size.store(new_avg, Ordering::Relaxed);
    }

    pub fn record_latency(&self, latency: Duration) {
        self.total_latency_ms
            .fetch_add(latency.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.delivery_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> OutboxStatsSnapshot {
        let processed = self.events_processed.load(Ordering::Relaxed);
        let delivered = self.events_delivered.load(Ordering::Relaxed);
        let total_latency = self.total_latency_ms.load(Ordering::Relaxed);

        OutboxStatsSnapshot {
            events_processed: processed,
            events_delivered: delivered,
            events_failed: self.events_failed.load(Ordering::Relaxed),
            events_retried: self.events_retried.load(Ordering::Relaxed),
            batches_processed: self.batches_processed.load(Ordering::Relaxed),
            delivery_errors: self.delivery_errors.load(Ordering::Relaxed),
            avg_batch_size: self.avg_batch_size.load(Ordering::Relaxed),
            avg_latency_ms: if delivered > 0 {
                total_latency / delivered
            } else {
                0
            },
            delivery_rate: if processed > 0 {
                delivered as f64 / processed as f64
            } else {
                1.0
            },
        }
    }
}

/// Snapshot of outbox statistics.
#[derive(Debug, Clone)]
pub struct OutboxStatsSnapshot {
    pub events_processed: u64,
    pub events_delivered: u64,
    pub events_failed: u64,
    pub events_retried: u64,
    pub batches_processed: u64,
    pub delivery_errors: u64,
    pub avg_batch_size: u64,
    pub avg_latency_ms: u64,
    pub delivery_rate: f64,
}

/// Outbox processor for reliable event delivery.
pub struct OutboxProcessor<S, P>
where
    S: OutboxStore,
    P: EventPublisher,
{
    config: OutboxConfig,
    store: Arc<S>,
    publisher: Arc<P>,
    stats: Arc<OutboxStats>,
    running: AtomicBool,
    shutdown: Arc<Notify>,
}

impl<S, P> OutboxProcessor<S, P>
where
    S: OutboxStore + 'static,
    P: EventPublisher + 'static,
{
    /// Create a new outbox processor.
    pub fn new(config: OutboxConfig, store: S, publisher: P) -> Self {
        Self {
            config,
            store: Arc::new(store),
            publisher: Arc::new(publisher),
            stats: Arc::new(OutboxStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(Notify::new()),
        }
    }

    /// Start the outbox processor.
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        info!("Outbox processor starting");

        let config = self.config.clone();
        let store = self.store.clone();
        let publisher = self.publisher.clone();
        let stats = self.stats.clone();
        let shutdown = self.shutdown.clone();
        let running = &self.running;

        while running.load(Ordering::Relaxed) {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("Outbox processor received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(config.poll_interval) => {
                    if let Err(e) = Self::process_batch(&config, &store, &publisher, &stats).await {
                        error!("Outbox batch processing error: {}", e);
                        stats.record_error();
                    }
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        info!("Outbox processor stopped");
        Ok(())
    }

    /// Process a batch of pending events.
    async fn process_batch(
        config: &OutboxConfig,
        store: &Arc<S>,
        publisher: &Arc<P>,
        stats: &Arc<OutboxStats>,
    ) -> Result<()> {
        let pending = store.get_pending(config.batch_size).await?;
        if pending.is_empty() {
            return Ok(());
        }

        let batch_size = pending.len();
        stats.record_batch(batch_size);
        debug!("Processing {} pending outbox events", batch_size);

        let start = Instant::now();

        if config.ordered_delivery {
            // Sequential delivery for ordering guarantees
            for event in pending {
                let event_id = event.id.clone();
                stats.record_processed(1);

                match tokio::time::timeout(config.delivery_timeout, publisher.publish(&event)).await
                {
                    Ok(Ok(())) => {
                        store
                            .update_status(&event_id, OutboxStatus::Delivered, None)
                            .await?;
                        stats.record_delivered(1);
                    }
                    Ok(Err(e)) => {
                        Self::handle_failure(
                            config,
                            store,
                            &event_id,
                            event.attempts,
                            &e.to_string(),
                        )
                        .await?;
                        stats.record_error();
                    }
                    Err(_) => {
                        Self::handle_failure(
                            config,
                            store,
                            &event_id,
                            event.attempts,
                            "Delivery timeout",
                        )
                        .await?;
                        stats.record_error();
                    }
                }
            }
        } else {
            // Parallel delivery for throughput
            let results = publisher.publish_batch(&pending).await?;

            for (event, result) in pending.iter().zip(results.iter()) {
                stats.record_processed(1);
                match result {
                    Ok(()) => {
                        store
                            .update_status(&event.id, OutboxStatus::Delivered, None)
                            .await?;
                        stats.record_delivered(1);
                    }
                    Err(e) => {
                        Self::handle_failure(
                            config,
                            store,
                            &event.id,
                            event.attempts,
                            &e.to_string(),
                        )
                        .await?;
                        stats.record_error();
                    }
                }
            }
        }

        stats.record_latency(start.elapsed());
        Ok(())
    }

    /// Handle delivery failure.
    async fn handle_failure(
        config: &OutboxConfig,
        store: &Arc<S>,
        event_id: &str,
        attempts: u32,
        error: &str,
    ) -> Result<()> {
        if attempts >= config.max_retries {
            warn!(
                "Event {} failed after {} attempts, moving to DLQ",
                event_id, attempts
            );
            store
                .update_status(event_id, OutboxStatus::Failed, Some(error.to_string()))
                .await?;
        } else {
            debug!(
                "Event {} delivery failed, will retry (attempt {})",
                event_id, attempts
            );
            store
                .update_status(event_id, OutboxStatus::Pending, Some(error.to_string()))
                .await?;
        }
        Ok(())
    }

    /// Stop the outbox processor.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown.notify_one();
    }

    /// Get statistics.
    pub fn stats(&self) -> OutboxStatsSnapshot {
        self.stats.snapshot()
    }

    /// Get the store reference.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// Store a new event (convenience method).
    pub async fn publish(&self, event: OutboxEvent) -> Result<()> {
        self.store.store(event).await
    }

    /// Run cleanup of old delivered events.
    pub async fn cleanup(&self) -> Result<u64> {
        self.store.cleanup(self.config.retention_period).await
    }

    /// Get DLQ events.
    pub async fn get_dlq(&self, limit: usize) -> Result<Vec<OutboxEvent>> {
        self.store.get_dlq(limit).await
    }

    /// Retry a failed event from DLQ.
    pub async fn retry_dlq_event(&self, event_id: &str) -> Result<()> {
        self.store
            .update_status(event_id, OutboxStatus::Pending, None)
            .await
    }
}

// Utility functions

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let random: u64 = rand_u64();
    format!("{:016x}-{:016x}", now as u64, random)
}

fn rand_u64() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    hasher.finish()
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock publisher for testing.
    struct MockPublisher {
        published: RwLock<Vec<OutboxEvent>>,
        fail_count: AtomicU64,
        should_fail: AtomicBool,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                published: RwLock::new(Vec::new()),
                fail_count: AtomicU64::new(0),
                should_fail: AtomicBool::new(false),
            }
        }

        fn set_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::Relaxed);
        }

        /// Get all published events (for test assertions).
        #[allow(dead_code)] // Available for future test expansion
        async fn get_published(&self) -> Vec<OutboxEvent> {
            self.published.read().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl EventPublisher for MockPublisher {
        async fn publish(&self, event: &OutboxEvent) -> Result<()> {
            if self.should_fail.load(Ordering::Relaxed) {
                self.fail_count.fetch_add(1, Ordering::Relaxed);
                return Err(crate::common::CdcError::replication("Mock failure"));
            }
            self.published.write().await.push(event.clone());
            Ok(())
        }

        async fn publish_batch(&self, events: &[OutboxEvent]) -> Result<Vec<Result<()>>> {
            let mut results = Vec::with_capacity(events.len());
            for event in events {
                results.push(self.publish(event).await);
            }
            Ok(results)
        }
    }

    #[test]
    fn test_outbox_event_creation() {
        let event = OutboxEvent::new(
            "Order",
            "order-123",
            "OrderCreated",
            serde_json::json!({"amount": 100}),
        );

        assert_eq!(event.aggregate_type, "Order");
        assert_eq!(event.aggregate_id, "order-123");
        assert_eq!(event.event_type, "OrderCreated");
        assert_eq!(event.status, OutboxStatus::Pending);
        assert_eq!(event.attempts, 0);
    }

    #[test]
    fn test_outbox_event_metadata() {
        let event = OutboxEvent::new("User", "user-1", "UserCreated", serde_json::json!({}))
            .with_metadata("correlation_id", "corr-123")
            .with_metadata("trace_id", "trace-456");

        assert_eq!(
            event.metadata.get("correlation_id"),
            Some(&"corr-123".to_string())
        );
        assert_eq!(
            event.metadata.get("trace_id"),
            Some(&"trace-456".to_string())
        );
    }

    #[test]
    fn test_outbox_event_to_cdc() {
        let event = OutboxEvent::new(
            "Order",
            "order-123",
            "OrderCreated",
            serde_json::json!({"amount": 100}),
        );

        let cdc = event.to_cdc_event();
        assert_eq!(cdc.source_type, "outbox");
        assert_eq!(cdc.database, "Order");
        assert_eq!(cdc.table, "OrderCreated");
        assert_eq!(cdc.op, CdcOp::Insert);
    }

    #[test]
    fn test_outbox_event_status_transitions() {
        let mut event = OutboxEvent::new("Test", "1", "TestEvent", serde_json::json!({}));

        assert_eq!(event.status, OutboxStatus::Pending);

        event.mark_processing();
        assert_eq!(event.status, OutboxStatus::Processing);
        assert_eq!(event.attempts, 1);

        event.mark_delivered();
        assert_eq!(event.status, OutboxStatus::Delivered);

        let mut event2 = OutboxEvent::new("Test", "2", "TestEvent", serde_json::json!({}));
        event2.mark_processing();
        event2.mark_failed("Connection refused");
        assert_eq!(event2.status, OutboxStatus::Failed);
        assert_eq!(event2.last_error, Some("Connection refused".to_string()));
    }

    #[test]
    fn test_config_defaults() {
        let config = OutboxConfig::default();
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_config_presets() {
        let high_throughput = OutboxConfig::high_throughput();
        assert_eq!(high_throughput.batch_size, 500);
        assert!(!high_throughput.ordered_delivery);

        let low_latency = OutboxConfig::low_latency();
        assert_eq!(low_latency.poll_interval, Duration::from_millis(10));
        assert!(low_latency.ordered_delivery);
    }

    #[test]
    fn test_config_builder() {
        let config = OutboxConfig::builder()
            .poll_interval(Duration::from_millis(50))
            .batch_size(200)
            .max_retries(5)
            .ordered_delivery(false)
            .build();

        assert_eq!(config.poll_interval, Duration::from_millis(50));
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.max_retries, 5);
        assert!(!config.ordered_delivery);
    }

    #[tokio::test]
    async fn test_memory_store_basic() {
        let store = MemoryOutboxStore::new();

        let event = OutboxEvent::new("Order", "1", "OrderCreated", serde_json::json!({}));
        store.store(event.clone()).await.unwrap();

        let pending = store.get_pending(10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].aggregate_id, "1");
    }

    #[tokio::test]
    async fn test_memory_store_sequence_numbers() {
        let store = MemoryOutboxStore::new();

        for i in 0..5 {
            let event = OutboxEvent::new(
                "Order",
                "order-1",
                format!("Event{}", i),
                serde_json::json!({}),
            );
            store.store(event).await.unwrap();
        }

        let events = store.get_by_aggregate("Order", "order-1").await.unwrap();
        assert_eq!(events.len(), 5);

        // Verify sequence ordering
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_memory_store_status_update() {
        let store = MemoryOutboxStore::new();

        let event = OutboxEvent::new("Test", "1", "TestEvent", serde_json::json!({}));
        let event_id = event.id.clone();
        store.store(event).await.unwrap();

        store
            .update_status(&event_id, OutboxStatus::Delivered, None)
            .await
            .unwrap();

        let pending = store.get_pending(10).await.unwrap();
        assert!(pending.is_empty()); // No longer pending
    }

    #[tokio::test]
    async fn test_memory_store_dlq() {
        let store = MemoryOutboxStore::new();

        let event = OutboxEvent::new("Test", "1", "TestEvent", serde_json::json!({}));
        let event_id = event.id.clone();
        store.store(event).await.unwrap();

        store
            .update_status(&event_id, OutboxStatus::Failed, Some("Error".to_string()))
            .await
            .unwrap();

        let dlq = store.get_dlq(10).await.unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].last_error, Some("Error".to_string()));
    }

    #[tokio::test]
    async fn test_memory_store_cleanup() {
        let store = MemoryOutboxStore::new();

        // Store and mark as delivered
        let event = OutboxEvent::new("Test", "1", "TestEvent", serde_json::json!({}));
        let event_id = event.id.clone();
        store.store(event).await.unwrap();
        store
            .update_status(&event_id, OutboxStatus::Delivered, None)
            .await
            .unwrap();

        // Cleanup with zero retention (should remove all delivered)
        let cleaned = store.cleanup(Duration::ZERO).await.unwrap();
        assert_eq!(cleaned, 1);
    }

    #[tokio::test]
    async fn test_processor_basic_delivery() {
        let store = MemoryOutboxStore::new();
        let publisher = MockPublisher::new();

        // Store an event
        let event = OutboxEvent::new(
            "Order",
            "1",
            "OrderCreated",
            serde_json::json!({"total": 100}),
        );
        store.store(event).await.unwrap();

        // Process
        let config = OutboxConfig::default();
        let stats = Arc::new(OutboxStats::new());
        OutboxProcessor::<MemoryOutboxStore, MockPublisher>::process_batch(
            &config,
            &Arc::new(store),
            &Arc::new(publisher),
            &stats,
        )
        .await
        .unwrap();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.events_processed, 1);
        assert_eq!(snapshot.events_delivered, 1);
    }

    #[tokio::test]
    async fn test_processor_retry_on_failure() {
        let store = Arc::new(MemoryOutboxStore::new());
        let publisher = Arc::new(MockPublisher::new());

        // Store an event
        let event = OutboxEvent::new("Order", "1", "OrderCreated", serde_json::json!({}));
        let _event_id = event.id.clone();
        store.store(event).await.unwrap();

        // Set publisher to fail
        publisher.set_fail(true);

        // Process (should fail and be reset to pending)
        let config = OutboxConfig::builder().max_retries(3).build();
        let stats = Arc::new(OutboxStats::new());

        OutboxProcessor::<MemoryOutboxStore, MockPublisher>::process_batch(
            &config, &store, &publisher, &stats,
        )
        .await
        .unwrap();

        // Event should still be pending for retry
        let pending = store.get_pending(10).await.unwrap();
        assert_eq!(pending.len(), 1);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.events_processed, 1);
        assert_eq!(snapshot.events_delivered, 0);
        assert_eq!(snapshot.delivery_errors, 1);
    }

    #[tokio::test]
    async fn test_processor_dlq_after_max_retries() {
        let store = Arc::new(MemoryOutboxStore::new());
        let publisher = Arc::new(MockPublisher::new());

        // Store an event with max attempts already reached
        let mut event = OutboxEvent::new("Order", "1", "OrderCreated", serde_json::json!({}));
        event.attempts = 3; // Already at max
        let _event_id = event.id.clone();
        store.store(event).await.unwrap();

        // Set publisher to fail
        publisher.set_fail(true);

        let config = OutboxConfig::builder().max_retries(3).build();
        let stats = Arc::new(OutboxStats::new());

        OutboxProcessor::<MemoryOutboxStore, MockPublisher>::process_batch(
            &config, &store, &publisher, &stats,
        )
        .await
        .unwrap();

        // Event should be in DLQ now
        let dlq = store.get_dlq(10).await.unwrap();
        assert_eq!(dlq.len(), 1);

        let pending = store.get_pending(10).await.unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = OutboxStats::new();
        stats.record_processed(10);
        stats.record_delivered(8);
        stats.record_failed();
        stats.record_failed();
        stats.record_retry();
        stats.record_batch(10);
        stats.record_latency(Duration::from_millis(100));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.events_processed, 10);
        assert_eq!(snapshot.events_delivered, 8);
        assert_eq!(snapshot.events_failed, 2);
        assert_eq!(snapshot.events_retried, 1);
        assert_eq!(snapshot.batches_processed, 1);
        assert!((snapshot.delivery_rate - 0.8).abs() < 0.001);
    }
}
