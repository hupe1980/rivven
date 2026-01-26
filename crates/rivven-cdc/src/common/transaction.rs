//! # Transaction Grouping
//!
//! Groups CDC events by database transaction for atomic processing.
//!
//! ## Features
//!
//! - **Transaction buffering**: Collect all events from a transaction before processing
//! - **Atomic delivery**: Deliver complete transactions or none (exactly-once semantics)
//! - **Timeout handling**: Flush incomplete transactions after timeout
//! - **Memory management**: Configurable limits on buffered transactions
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::transaction::{TransactionGrouper, TransactionBatch};
//!
//! let grouper = TransactionGrouper::new(TransactionConfig::default());
//!
//! // Add events - returns complete transactions
//! for event in events {
//!     if let Some(batch) = grouper.add(event).await {
//!         // Process complete transaction atomically
//!         process_batch(&batch).await;
//!     }
//! }
//!
//! // Flush any remaining transactions (e.g., on shutdown)
//! for batch in grouper.flush_all().await {
//!     process_batch(&batch).await;
//! }
//! ```

use crate::common::CdcEvent;
#[cfg(test)]
use crate::common::TransactionMetadata;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Configuration for transaction grouping.
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Maximum events per transaction before forced flush
    pub max_events_per_txn: usize,
    /// Maximum memory per transaction (bytes)
    pub max_bytes_per_txn: usize,
    /// Timeout for incomplete transactions
    pub txn_timeout: Duration,
    /// Maximum concurrent transactions in buffer
    pub max_buffered_txns: usize,
    /// Maximum total buffered events across all transactions
    pub max_buffered_events: usize,
    /// Whether to pass through events without transaction metadata
    pub pass_through_non_txn: bool,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            max_events_per_txn: 10_000,
            max_bytes_per_txn: 64 * 1024 * 1024, // 64MB
            txn_timeout: Duration::from_secs(60),
            max_buffered_txns: 1_000,
            max_buffered_events: 100_000,
            pass_through_non_txn: true,
        }
    }
}

impl TransactionConfig {
    /// Create a new config builder.
    pub fn builder() -> TransactionConfigBuilder {
        TransactionConfigBuilder::default()
    }
}

/// Builder for TransactionConfig.
#[derive(Default)]
pub struct TransactionConfigBuilder {
    config: TransactionConfig,
}

impl TransactionConfigBuilder {
    /// Set max events per transaction.
    pub fn max_events_per_txn(mut self, max: usize) -> Self {
        self.config.max_events_per_txn = max;
        self
    }

    /// Set max bytes per transaction.
    pub fn max_bytes_per_txn(mut self, max: usize) -> Self {
        self.config.max_bytes_per_txn = max;
        self
    }

    /// Set transaction timeout.
    pub fn txn_timeout(mut self, timeout: Duration) -> Self {
        self.config.txn_timeout = timeout;
        self
    }

    /// Set max buffered transactions.
    pub fn max_buffered_txns(mut self, max: usize) -> Self {
        self.config.max_buffered_txns = max;
        self
    }

    /// Set max buffered events.
    pub fn max_buffered_events(mut self, max: usize) -> Self {
        self.config.max_buffered_events = max;
        self
    }

    /// Set pass-through mode for non-transactional events.
    pub fn pass_through_non_txn(mut self, enabled: bool) -> Self {
        self.config.pass_through_non_txn = enabled;
        self
    }

    /// Build the config.
    pub fn build(self) -> TransactionConfig {
        self.config
    }
}

/// A complete transaction batch ready for processing.
#[derive(Debug, Clone)]
pub struct TransactionBatch {
    /// Transaction ID
    pub txn_id: String,
    /// Events in transaction order
    pub events: Vec<CdcEvent>,
    /// Transaction LSN/position (from last event)
    pub lsn: String,
    /// Transaction commit timestamp
    pub commit_ts: Option<i64>,
    /// Whether this batch was forced (timeout/overflow)
    pub forced: bool,
    /// Time spent buffering this transaction
    pub buffer_duration: Duration,
}

impl TransactionBatch {
    /// Create a new transaction batch.
    pub fn new(
        txn_id: String,
        events: Vec<CdcEvent>,
        lsn: String,
        commit_ts: Option<i64>,
        forced: bool,
        buffer_duration: Duration,
    ) -> Self {
        Self {
            txn_id,
            events,
            lsn,
            commit_ts,
            forced,
            buffer_duration,
        }
    }

    /// Get number of events in batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get events by table.
    pub fn events_by_table(&self) -> HashMap<String, Vec<&CdcEvent>> {
        let mut by_table: HashMap<String, Vec<&CdcEvent>> = HashMap::new();
        for event in &self.events {
            let key = format!("{}.{}", event.schema, event.table);
            by_table.entry(key).or_default().push(event);
        }
        by_table
    }

    /// Create batch for single non-transactional event.
    pub fn single(event: CdcEvent) -> Self {
        let txn_id = format!("single-{}", event.timestamp);
        Self {
            txn_id,
            lsn: String::new(),
            commit_ts: Some(event.timestamp * 1000),
            forced: false,
            buffer_duration: Duration::ZERO,
            events: vec![event],
        }
    }
}

/// Internal buffer for an in-progress transaction.
struct TransactionBuffer {
    events: Vec<CdcEvent>,
    estimated_bytes: usize,
    started_at: Instant,
    last_lsn: String,
    commit_ts: Option<i64>,
}

impl TransactionBuffer {
    fn new() -> Self {
        Self {
            events: Vec::new(),
            estimated_bytes: 0,
            started_at: Instant::now(),
            last_lsn: String::new(),
            commit_ts: None,
        }
    }

    fn add(&mut self, event: CdcEvent) {
        // Estimate bytes from JSON serialization
        self.estimated_bytes += serde_json::to_string(&event)
            .map(|s| s.len())
            .unwrap_or(512);

        if let Some(txn) = &event.transaction {
            self.last_lsn.clone_from(&txn.lsn);
            if txn.commit_ts.is_some() {
                self.commit_ts = txn.commit_ts;
            }
        }

        self.events.push(event);
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    fn into_batch(self, txn_id: String, forced: bool) -> TransactionBatch {
        TransactionBatch::new(
            txn_id,
            self.events,
            self.last_lsn,
            self.commit_ts,
            forced,
            self.started_at.elapsed(),
        )
    }
}

/// Statistics for transaction grouping.
#[derive(Debug, Default)]
pub struct TransactionStats {
    /// Total transactions completed
    transactions_completed: AtomicU64,
    /// Transactions completed normally (with commit marker)
    transactions_complete: AtomicU64,
    /// Transactions force-flushed due to timeout
    transactions_timeout: AtomicU64,
    /// Transactions force-flushed due to size limits
    transactions_overflow: AtomicU64,
    /// Total events processed
    events_processed: AtomicU64,
    /// Events passed through (no transaction metadata)
    events_pass_through: AtomicU64,
    /// Current buffered events
    current_buffered_events: AtomicU64,
    /// Current buffered transactions
    current_buffered_txns: AtomicU64,
}

impl TransactionStats {
    fn record_complete(&self, event_count: u64) {
        self.transactions_completed.fetch_add(1, Ordering::Relaxed);
        self.transactions_complete.fetch_add(1, Ordering::Relaxed);
        self.events_processed
            .fetch_add(event_count, Ordering::Relaxed);
    }

    fn record_timeout(&self, event_count: u64) {
        self.transactions_completed.fetch_add(1, Ordering::Relaxed);
        self.transactions_timeout.fetch_add(1, Ordering::Relaxed);
        self.events_processed
            .fetch_add(event_count, Ordering::Relaxed);
    }

    fn record_overflow(&self, event_count: u64) {
        self.transactions_completed.fetch_add(1, Ordering::Relaxed);
        self.transactions_overflow.fetch_add(1, Ordering::Relaxed);
        self.events_processed
            .fetch_add(event_count, Ordering::Relaxed);
    }

    fn record_pass_through(&self) {
        self.events_pass_through.fetch_add(1, Ordering::Relaxed);
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    fn set_buffered(&self, events: u64, txns: u64) {
        self.current_buffered_events
            .store(events, Ordering::Relaxed);
        self.current_buffered_txns.store(txns, Ordering::Relaxed);
    }

    /// Get total completed transactions.
    pub fn transactions_completed(&self) -> u64 {
        self.transactions_completed.load(Ordering::Relaxed)
    }

    /// Get normally completed transactions.
    pub fn transactions_complete(&self) -> u64 {
        self.transactions_complete.load(Ordering::Relaxed)
    }

    /// Get timed-out transactions.
    pub fn transactions_timeout(&self) -> u64 {
        self.transactions_timeout.load(Ordering::Relaxed)
    }

    /// Get overflowed transactions.
    pub fn transactions_overflow(&self) -> u64 {
        self.transactions_overflow.load(Ordering::Relaxed)
    }

    /// Get total events processed.
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    /// Get pass-through events.
    pub fn events_pass_through(&self) -> u64 {
        self.events_pass_through.load(Ordering::Relaxed)
    }

    /// Get current buffered events.
    pub fn current_buffered_events(&self) -> u64 {
        self.current_buffered_events.load(Ordering::Relaxed)
    }

    /// Get current buffered transactions.
    pub fn current_buffered_txns(&self) -> u64 {
        self.current_buffered_txns.load(Ordering::Relaxed)
    }
}

/// Groups CDC events by database transaction.
///
/// Collects events until a transaction is complete (marked by is_last),
/// then emits the complete batch for atomic processing.
pub struct TransactionGrouper {
    config: TransactionConfig,
    buffers: RwLock<HashMap<String, TransactionBuffer>>,
    stats: Arc<TransactionStats>,
    total_buffered_events: AtomicU64,
}

impl TransactionGrouper {
    /// Create a new transaction grouper.
    pub fn new(config: TransactionConfig) -> Self {
        Self {
            config,
            buffers: RwLock::new(HashMap::new()),
            stats: Arc::new(TransactionStats::default()),
            total_buffered_events: AtomicU64::new(0),
        }
    }

    /// Get statistics.
    pub fn stats(&self) -> &Arc<TransactionStats> {
        &self.stats
    }

    /// Add an event, returning completed batch if transaction is complete.
    pub async fn add(&self, event: CdcEvent) -> Option<TransactionBatch> {
        // Handle events without transaction metadata
        let txn_meta = match &event.transaction {
            Some(meta) => meta.clone(),
            None => {
                if self.config.pass_through_non_txn {
                    self.stats.record_pass_through();
                    return Some(TransactionBatch::single(event));
                } else {
                    return None;
                }
            }
        };

        let txn_id = txn_meta.id.clone();
        let is_last = txn_meta.is_last;

        let mut buffers = self.buffers.write().await;

        // Check if we need to evict oldest transaction
        if !buffers.contains_key(&txn_id) && buffers.len() >= self.config.max_buffered_txns {
            // Evict oldest transaction
            if let Some(oldest_id) = self.find_oldest_txn(&buffers) {
                if let Some(buffer) = buffers.remove(&oldest_id) {
                    let count = buffer.events.len() as u64;
                    self.total_buffered_events
                        .fetch_sub(count, Ordering::Relaxed);
                    self.stats.record_overflow(count);
                    warn!(
                        "Evicted oldest transaction {} due to buffer limit",
                        oldest_id
                    );
                    drop(buffers);
                    return Some(buffer.into_batch(oldest_id, true));
                }
            }
        }

        // Add event to buffer
        let buffer = buffers
            .entry(txn_id.clone())
            .or_insert_with(TransactionBuffer::new);
        buffer.add(event);
        self.total_buffered_events.fetch_add(1, Ordering::Relaxed);

        // Check completion conditions
        let should_complete = is_last;
        let should_force_size = buffer.events.len() >= self.config.max_events_per_txn
            || buffer.estimated_bytes >= self.config.max_bytes_per_txn;

        if should_complete || should_force_size {
            if let Some(buffer) = buffers.remove(&txn_id) {
                let count = buffer.events.len() as u64;
                self.total_buffered_events
                    .fetch_sub(count, Ordering::Relaxed);

                // Update stats
                self.stats.set_buffered(
                    self.total_buffered_events.load(Ordering::Relaxed),
                    buffers.len() as u64,
                );

                if should_complete {
                    self.stats.record_complete(count);
                    debug!("Transaction {} complete with {} events", txn_id, count);
                } else {
                    self.stats.record_overflow(count);
                    warn!(
                        "Transaction {} forced due to size limits ({} events)",
                        txn_id, count
                    );
                }

                return Some(buffer.into_batch(txn_id, !should_complete));
            }
        }

        // Update stats
        self.stats.set_buffered(
            self.total_buffered_events.load(Ordering::Relaxed),
            buffers.len() as u64,
        );

        None
    }

    /// Check for and flush timed-out transactions.
    pub async fn check_timeouts(&self) -> Vec<TransactionBatch> {
        let mut timed_out = Vec::new();
        let now = Instant::now();

        let mut buffers = self.buffers.write().await;

        let expired: Vec<String> = buffers
            .iter()
            .filter(|(_, buf)| now.duration_since(buf.started_at) > self.config.txn_timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for txn_id in expired {
            if let Some(buffer) = buffers.remove(&txn_id) {
                let count = buffer.events.len() as u64;
                self.total_buffered_events
                    .fetch_sub(count, Ordering::Relaxed);
                self.stats.record_timeout(count);
                warn!(
                    "Transaction {} timed out after {:?}",
                    txn_id,
                    buffer.elapsed()
                );
                timed_out.push(buffer.into_batch(txn_id, true));
            }
        }

        self.stats.set_buffered(
            self.total_buffered_events.load(Ordering::Relaxed),
            buffers.len() as u64,
        );

        timed_out
    }

    /// Flush all buffered transactions (e.g., on shutdown).
    pub async fn flush_all(&self) -> Vec<TransactionBatch> {
        let mut batches = Vec::new();

        let mut buffers = self.buffers.write().await;

        for (txn_id, buffer) in buffers.drain() {
            let count = buffer.events.len() as u64;
            self.stats.record_overflow(count);
            batches.push(buffer.into_batch(txn_id, true));
        }

        self.total_buffered_events.store(0, Ordering::Relaxed);
        self.stats.set_buffered(0, 0);

        batches
    }

    /// Get a specific transaction's current buffer size.
    pub async fn txn_buffer_size(&self, txn_id: &str) -> Option<usize> {
        self.buffers
            .read()
            .await
            .get(txn_id)
            .map(|b| b.events.len())
    }

    /// Get total buffered event count.
    pub fn total_buffered(&self) -> u64 {
        self.total_buffered_events.load(Ordering::Relaxed)
    }

    fn find_oldest_txn(&self, buffers: &HashMap<String, TransactionBuffer>) -> Option<String> {
        buffers
            .iter()
            .min_by_key(|(_, buf)| buf.started_at)
            .map(|(id, _)| id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_event(table: &str, txn_id: &str, seq: u64, total: u64, is_last: bool) -> CdcEvent {
        let mut txn =
            TransactionMetadata::new(txn_id, format!("lsn-{}", seq), seq).with_total(total);
        if is_last {
            txn = txn.with_last();
        }

        CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            table,
            json!({"id": seq}),
            1705000000,
        )
        .with_transaction(txn)
    }

    #[tokio::test]
    async fn test_single_event_transaction() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        let event = make_event("users", "txn-1", 0, 1, true);
        let batch = grouper.add(event).await;

        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.txn_id, "txn-1");
        assert_eq!(batch.len(), 1);
        assert!(!batch.forced);
    }

    #[tokio::test]
    async fn test_multi_event_transaction() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        // First event - not last
        let batch = grouper.add(make_event("users", "txn-2", 0, 3, false)).await;
        assert!(batch.is_none());

        // Second event - not last
        let batch = grouper.add(make_event("users", "txn-2", 1, 3, false)).await;
        assert!(batch.is_none());

        // Third event - last
        let batch = grouper.add(make_event("orders", "txn-2", 2, 3, true)).await;
        assert!(batch.is_some());

        let batch = batch.unwrap();
        assert_eq!(batch.txn_id, "txn-2");
        assert_eq!(batch.len(), 3);
        assert!(!batch.forced);

        // Verify event order
        assert_eq!(batch.events[0].table, "users");
        assert_eq!(batch.events[1].table, "users");
        assert_eq!(batch.events[2].table, "orders");
    }

    #[tokio::test]
    async fn test_concurrent_transactions() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        // Interleaved events from two transactions
        grouper.add(make_event("users", "txn-a", 0, 2, false)).await;
        grouper
            .add(make_event("orders", "txn-b", 0, 2, false))
            .await;
        grouper.add(make_event("users", "txn-a", 1, 2, true)).await;

        let stats = grouper.stats();
        assert_eq!(stats.transactions_completed(), 1);
        assert_eq!(stats.current_buffered_txns(), 1); // txn-b still buffered

        // Complete second transaction
        let batch = grouper.add(make_event("orders", "txn-b", 1, 2, true)).await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().txn_id, "txn-b");
        assert_eq!(stats.transactions_completed(), 2);
    }

    #[tokio::test]
    async fn test_pass_through_non_transactional() {
        let grouper = TransactionGrouper::new(
            TransactionConfig::builder()
                .pass_through_non_txn(true)
                .build(),
        );

        // Event without transaction metadata
        let event = CdcEvent::insert("pg", "db", "s", "users", json!({"id": 1}), 1705000000);
        let batch = grouper.add(event).await;

        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert!(batch.txn_id.starts_with("single-"));
        assert_eq!(batch.len(), 1);

        let stats = grouper.stats();
        assert_eq!(stats.events_pass_through(), 1);
    }

    #[tokio::test]
    async fn test_block_non_transactional() {
        let grouper = TransactionGrouper::new(
            TransactionConfig::builder()
                .pass_through_non_txn(false)
                .build(),
        );

        let event = CdcEvent::insert("pg", "db", "s", "users", json!({"id": 1}), 1705000000);
        let batch = grouper.add(event).await;

        assert!(batch.is_none());
    }

    #[tokio::test]
    async fn test_force_flush_on_size_limit() {
        let grouper =
            TransactionGrouper::new(TransactionConfig::builder().max_events_per_txn(3).build());

        // Add events until limit
        grouper.add(make_event("t", "txn-big", 0, 100, false)).await;
        grouper.add(make_event("t", "txn-big", 1, 100, false)).await;
        let batch = grouper.add(make_event("t", "txn-big", 2, 100, false)).await;

        // Should be forced even without is_last
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.len(), 3);
        assert!(batch.forced); // Was forced due to size limit

        let stats = grouper.stats();
        assert_eq!(stats.transactions_overflow(), 1);
    }

    #[tokio::test]
    async fn test_timeout_flush() {
        let grouper = TransactionGrouper::new(
            TransactionConfig::builder()
                .txn_timeout(Duration::from_millis(10))
                .build(),
        );

        // Add incomplete transaction
        grouper.add(make_event("t", "txn-slow", 0, 10, false)).await;

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        let batches = grouper.check_timeouts().await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].txn_id, "txn-slow");
        assert!(batches[0].forced);

        let stats = grouper.stats();
        assert_eq!(stats.transactions_timeout(), 1);
    }

    #[tokio::test]
    async fn test_flush_all() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        // Add multiple incomplete transactions
        grouper.add(make_event("t", "txn-1", 0, 5, false)).await;
        grouper.add(make_event("t", "txn-2", 0, 5, false)).await;
        grouper.add(make_event("t", "txn-2", 1, 5, false)).await;

        let batches = grouper.flush_all().await;
        assert_eq!(batches.len(), 2);

        // All should be forced
        for batch in &batches {
            assert!(batch.forced);
        }

        assert_eq!(grouper.total_buffered(), 0);
    }

    #[tokio::test]
    async fn test_evict_oldest_on_buffer_limit() {
        let grouper =
            TransactionGrouper::new(TransactionConfig::builder().max_buffered_txns(2).build());

        // Fill buffer
        grouper.add(make_event("t", "txn-old", 0, 10, false)).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        grouper.add(make_event("t", "txn-mid", 0, 10, false)).await;

        // Adding third should evict oldest
        let batch = grouper.add(make_event("t", "txn-new", 0, 10, false)).await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().txn_id, "txn-old"); // Evicted

        let stats = grouper.stats();
        assert_eq!(stats.transactions_overflow(), 1);
    }

    #[tokio::test]
    async fn test_events_by_table() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        grouper.add(make_event("users", "txn-1", 0, 3, false)).await;
        grouper
            .add(make_event("orders", "txn-1", 1, 3, false))
            .await;
        let batch = grouper
            .add(make_event("users", "txn-1", 2, 3, true))
            .await
            .unwrap();

        let by_table = batch.events_by_table();
        assert_eq!(by_table.get("public.users").map(|v| v.len()), Some(2));
        assert_eq!(by_table.get("public.orders").map(|v| v.len()), Some(1));
    }

    #[tokio::test]
    async fn test_transaction_batch_single() {
        let event = CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 1705000000);
        let batch = TransactionBatch::single(event.clone());

        assert_eq!(batch.len(), 1);
        assert!(!batch.forced);
        assert!(batch.txn_id.starts_with("single-"));
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        // Complete transaction
        grouper.add(make_event("t", "txn-1", 0, 2, false)).await;
        grouper.add(make_event("t", "txn-1", 1, 2, true)).await;

        // Pass-through event
        grouper
            .add(CdcEvent::insert("pg", "db", "s", "t", json!({}), 0))
            .await;

        let stats = grouper.stats();
        assert_eq!(stats.transactions_complete(), 1);
        assert_eq!(stats.events_pass_through(), 1);
        assert_eq!(stats.events_processed(), 3);
    }

    #[tokio::test]
    async fn test_buffer_size_tracking() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        assert_eq!(grouper.total_buffered(), 0);
        assert_eq!(grouper.txn_buffer_size("txn-1").await, None);

        grouper.add(make_event("t", "txn-1", 0, 5, false)).await;
        grouper.add(make_event("t", "txn-1", 1, 5, false)).await;

        assert_eq!(grouper.total_buffered(), 2);
        assert_eq!(grouper.txn_buffer_size("txn-1").await, Some(2));

        // Complete it
        grouper.add(make_event("t", "txn-1", 2, 5, true)).await;

        assert_eq!(grouper.total_buffered(), 0);
        assert_eq!(grouper.txn_buffer_size("txn-1").await, None);
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = TransactionConfig::builder()
            .max_events_per_txn(500)
            .max_bytes_per_txn(1024 * 1024)
            .txn_timeout(Duration::from_secs(30))
            .max_buffered_txns(100)
            .max_buffered_events(10000)
            .pass_through_non_txn(false)
            .build();

        assert_eq!(config.max_events_per_txn, 500);
        assert_eq!(config.max_bytes_per_txn, 1024 * 1024);
        assert_eq!(config.txn_timeout, Duration::from_secs(30));
        assert_eq!(config.max_buffered_txns, 100);
        assert_eq!(config.max_buffered_events, 10000);
        assert!(!config.pass_through_non_txn);
    }

    #[tokio::test]
    async fn test_batch_commit_timestamp() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        let mut txn = TransactionMetadata::new("txn-ts", "lsn-1", 0)
            .with_total(1)
            .with_last();
        txn.commit_ts = Some(1705000000000);

        let event = CdcEvent::insert("pg", "db", "s", "t", json!({}), 0).with_transaction(txn);
        let batch = grouper.add(event).await.unwrap();

        assert_eq!(batch.commit_ts, Some(1705000000000));
    }

    #[tokio::test]
    async fn test_large_transaction() {
        let grouper = TransactionGrouper::new(TransactionConfig::default());

        // Simulate a large transaction with many events
        let total = 100;
        for i in 0..total - 1 {
            let batch = grouper
                .add(make_event("t", "txn-large", i, total, false))
                .await;
            assert!(batch.is_none());
        }

        // Final event
        let batch = grouper
            .add(make_event("t", "txn-large", total - 1, total, true))
            .await;
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), total as usize);
    }

    #[test]
    fn test_transaction_batch_is_empty() {
        let batch = TransactionBatch::new(
            "empty".to_string(),
            vec![],
            String::new(),
            None,
            false,
            Duration::ZERO,
        );
        assert!(batch.is_empty());

        let batch = TransactionBatch::single(CdcEvent::insert("pg", "db", "s", "t", json!({}), 0));
        assert!(!batch.is_empty());
    }
}
