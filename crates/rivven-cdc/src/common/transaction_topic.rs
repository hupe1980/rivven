//! # Transaction Metadata Topic
//!
//! Transaction boundary events for exactly-once processing.
//!
//! When `provide.transaction.metadata` is enabled, emits:
//! - **BEGIN** events when transaction starts
//! - **END** events when transaction commits with event counts
//!
//! Also enriches individual CDC events with transaction context:
//! - `total_order` - Absolute position among all transaction events
//! - `data_collection_order` - Position within the table
//!
//! ## Topic Naming
//!
//! Transaction events are published to `<topic_prefix>.transaction`
//!
//! ## Event Format
//!
//! ```json
//! {
//!   "status": "BEGIN|END",
//!   "id": "txn-12345:0/ABC123",
//!   "ts_ms": 1705123456789,
//!   "event_count": 5,
//!   "data_collections": [
//!     {"data_collection": "public.orders", "event_count": 3},
//!     {"data_collection": "public.customers", "event_count": 2}
//!   ]
//! }
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use rivven_cdc::common::transaction_topic::{TransactionTopicEmitter, TransactionTopicConfig};
//!
//! let config = TransactionTopicConfig::builder()
//!     .topic_prefix("mydb")
//!     .build();
//!
//! let emitter = TransactionTopicEmitter::new(config);
//!
//! // Register transaction begin
//! let begin_event = emitter.begin_transaction("txn-123", "0/ABC", 1705123456789);
//!
//! // Track events
//! emitter.track_event("txn-123", "public.orders");
//! emitter.track_event("txn-123", "public.orders");
//! emitter.track_event("txn-123", "public.customers");
//!
//! // Emit end event
//! let end_event = emitter.end_transaction("txn-123", 1705123456999)?;
//! ```

use crate::common::CdcEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Transaction status in boundary events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransactionStatus {
    /// Transaction has started
    Begin,
    /// Transaction has committed
    End,
}

impl std::fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionStatus::Begin => write!(f, "BEGIN"),
            TransactionStatus::End => write!(f, "END"),
        }
    }
}

/// Event count per data collection (table) in a transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataCollectionEventCount {
    /// Fully qualified table name: schema.table
    pub data_collection: String,
    /// Number of events from this table
    pub event_count: u64,
}

/// Transaction boundary event (BEGIN or END).
///
/// Published to `<topic_prefix>.transaction` topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionEvent {
    /// Status: BEGIN or END
    pub status: TransactionStatus,
    /// Transaction ID (format: "txId:LSN")
    pub id: String,
    /// Event timestamp in milliseconds since epoch
    pub ts_ms: i64,
    /// Total event count (only for END events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_count: Option<u64>,
    /// Per-table event counts (only for END events)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_collections: Option<Vec<DataCollectionEventCount>>,
}

impl TransactionEvent {
    /// Create a BEGIN transaction event.
    pub fn begin(txn_id: &str, lsn: &str, ts_ms: i64) -> Self {
        Self {
            status: TransactionStatus::Begin,
            id: format!("{}:{}", txn_id, lsn),
            ts_ms,
            event_count: None,
            data_collections: None,
        }
    }

    /// Create an END transaction event.
    pub fn end(
        txn_id: &str,
        lsn: &str,
        ts_ms: i64,
        event_count: u64,
        data_collections: Vec<DataCollectionEventCount>,
    ) -> Self {
        Self {
            status: TransactionStatus::End,
            id: format!("{}:{}", txn_id, lsn),
            ts_ms,
            event_count: Some(event_count),
            data_collections: Some(data_collections),
        }
    }

    /// Get the topic name for transaction events.
    pub fn topic_name(topic_prefix: &str) -> String {
        format!("{}.transaction", topic_prefix)
    }

    /// Serialize to JSON bytes.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// Transaction context added to individual CDC events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionContext {
    /// Transaction ID (format: "txId:LSN")
    pub id: String,
    /// Absolute position among all events in the transaction (1-based)
    pub total_order: u64,
    /// Position among events for this data collection (1-based)
    pub data_collection_order: u64,
}

impl TransactionContext {
    /// Create a new transaction context.
    pub fn new(id: impl Into<String>, total_order: u64, data_collection_order: u64) -> Self {
        Self {
            id: id.into(),
            total_order,
            data_collection_order,
        }
    }
}

/// In-flight transaction state.
#[derive(Debug)]
struct InFlightTransaction {
    /// Transaction ID
    txn_id: String,
    /// LSN/position
    lsn: String,
    /// Start timestamp (for duration tracking)
    start_ts_ms: i64,
    /// Latest timestamp
    latest_ts_ms: i64,
    /// Global event counter
    total_event_count: u64,
    /// Per-table event counts
    data_collection_counts: HashMap<String, u64>,
}

impl InFlightTransaction {
    fn new(txn_id: String, lsn: String, ts_ms: i64) -> Self {
        Self {
            txn_id,
            lsn,
            start_ts_ms: ts_ms,
            latest_ts_ms: ts_ms,
            total_event_count: 0,
            data_collection_counts: HashMap::new(),
        }
    }

    fn track_event(&mut self, data_collection: &str, ts_ms: i64) -> (u64, u64) {
        self.total_event_count += 1;
        self.latest_ts_ms = ts_ms;

        let dc_count = self
            .data_collection_counts
            .entry(data_collection.to_string())
            .or_insert(0);
        *dc_count += 1;

        (self.total_event_count, *dc_count)
    }

    /// Get transaction duration in milliseconds
    fn duration_ms(&self) -> i64 {
        self.latest_ts_ms.saturating_sub(self.start_ts_ms)
    }
}

/// Configuration for transaction topic emission.
#[derive(Debug, Clone)]
pub struct TransactionTopicConfig {
    /// Topic prefix (e.g., "mydb" -> "mydb.transaction")
    pub topic_prefix: String,
    /// Whether to emit BEGIN events (default: true)
    pub emit_begin: bool,
    /// Whether to emit END events (default: true)
    pub emit_end: bool,
    /// Whether to enrich CDC events with transaction context (default: true)
    pub enrich_events: bool,
    /// Maximum in-flight transactions before warning
    pub max_in_flight: usize,
}

impl Default for TransactionTopicConfig {
    fn default() -> Self {
        Self {
            topic_prefix: "rivven".to_string(),
            emit_begin: true,
            emit_end: true,
            enrich_events: true,
            max_in_flight: 1000,
        }
    }
}

impl TransactionTopicConfig {
    /// Create a builder for TransactionTopicConfig.
    pub fn builder() -> TransactionTopicConfigBuilder {
        TransactionTopicConfigBuilder::default()
    }

    /// Get the transaction topic name.
    pub fn transaction_topic(&self) -> String {
        TransactionEvent::topic_name(&self.topic_prefix)
    }
}

/// Builder for TransactionTopicConfig.
#[derive(Default)]
pub struct TransactionTopicConfigBuilder {
    config: TransactionTopicConfig,
}

impl TransactionTopicConfigBuilder {
    /// Set the topic prefix.
    pub fn topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.topic_prefix = prefix.into();
        self
    }

    /// Enable/disable BEGIN event emission.
    pub fn emit_begin(mut self, emit: bool) -> Self {
        self.config.emit_begin = emit;
        self
    }

    /// Enable/disable END event emission.
    pub fn emit_end(mut self, emit: bool) -> Self {
        self.config.emit_end = emit;
        self
    }

    /// Enable/disable event enrichment with transaction context.
    pub fn enrich_events(mut self, enrich: bool) -> Self {
        self.config.enrich_events = enrich;
        self
    }

    /// Set max in-flight transactions.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Build the configuration.
    pub fn build(self) -> TransactionTopicConfig {
        self.config
    }
}

/// Statistics for transaction topic emission.
#[derive(Debug, Default)]
pub struct TransactionTopicStats {
    /// Total BEGIN events emitted
    pub begin_events: AtomicU64,
    /// Total END events emitted
    pub end_events: AtomicU64,
    /// Total events enriched with transaction context
    pub events_enriched: AtomicU64,
    /// Current in-flight transactions
    pub in_flight_transactions: AtomicU64,
    /// Transactions completed
    pub transactions_completed: AtomicU64,
    /// Transactions with missing BEGIN
    pub orphan_ends: AtomicU64,
}

impl TransactionTopicStats {
    /// Create a snapshot of current stats.
    pub fn snapshot(&self) -> TransactionTopicStatsSnapshot {
        TransactionTopicStatsSnapshot {
            begin_events: self.begin_events.load(Ordering::Relaxed),
            end_events: self.end_events.load(Ordering::Relaxed),
            events_enriched: self.events_enriched.load(Ordering::Relaxed),
            in_flight_transactions: self.in_flight_transactions.load(Ordering::Relaxed),
            transactions_completed: self.transactions_completed.load(Ordering::Relaxed),
            orphan_ends: self.orphan_ends.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of transaction topic stats.
#[derive(Debug, Clone, Default)]
pub struct TransactionTopicStatsSnapshot {
    pub begin_events: u64,
    pub end_events: u64,
    pub events_enriched: u64,
    pub in_flight_transactions: u64,
    pub transactions_completed: u64,
    pub orphan_ends: u64,
}

/// Emitter for transaction boundary events.
///
/// Tracks in-flight transactions and emits BEGIN/END events to dedicated topic.
#[derive(Debug)]
pub struct TransactionTopicEmitter {
    config: TransactionTopicConfig,
    /// In-flight transactions keyed by txn_id
    in_flight: Arc<RwLock<HashMap<String, InFlightTransaction>>>,
    /// Statistics
    stats: Arc<TransactionTopicStats>,
}

impl TransactionTopicEmitter {
    /// Create a new transaction topic emitter.
    pub fn new(config: TransactionTopicConfig) -> Self {
        Self {
            config,
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(TransactionTopicStats::default()),
        }
    }

    /// Get the transaction topic name.
    pub fn topic_name(&self) -> String {
        self.config.transaction_topic()
    }

    /// Get current statistics.
    pub fn stats(&self) -> TransactionTopicStatsSnapshot {
        self.stats.snapshot()
    }

    /// Register transaction begin.
    ///
    /// Returns BEGIN event if emission is enabled.
    pub async fn begin_transaction(
        &self,
        txn_id: &str,
        lsn: &str,
        ts_ms: i64,
    ) -> Option<TransactionEvent> {
        let mut in_flight = self.in_flight.write().await;

        // Check for limit
        if in_flight.len() >= self.config.max_in_flight {
            warn!(
                txn_id = txn_id,
                in_flight = in_flight.len(),
                max = self.config.max_in_flight,
                "High number of in-flight transactions"
            );
        }

        // Create in-flight entry
        let txn = InFlightTransaction::new(txn_id.to_string(), lsn.to_string(), ts_ms);
        in_flight.insert(txn_id.to_string(), txn);

        self.stats
            .in_flight_transactions
            .store(in_flight.len() as u64, Ordering::Relaxed);

        if self.config.emit_begin {
            self.stats.begin_events.fetch_add(1, Ordering::Relaxed);
            debug!(txn_id = txn_id, lsn = lsn, "Transaction BEGIN");
            Some(TransactionEvent::begin(txn_id, lsn, ts_ms))
        } else {
            None
        }
    }

    /// Track an event within a transaction.
    ///
    /// Returns (total_order, data_collection_order) for event enrichment.
    /// Returns None if transaction not found.
    pub async fn track_event(
        &self,
        txn_id: &str,
        data_collection: &str,
        ts_ms: i64,
    ) -> Option<TransactionContext> {
        let mut in_flight = self.in_flight.write().await;

        if let Some(txn) = in_flight.get_mut(txn_id) {
            let (total_order, dc_order) = txn.track_event(data_collection, ts_ms);

            if self.config.enrich_events {
                self.stats.events_enriched.fetch_add(1, Ordering::Relaxed);
                Some(TransactionContext::new(
                    format!("{}:{}", txn_id, txn.lsn),
                    total_order,
                    dc_order,
                ))
            } else {
                None
            }
        } else {
            debug!(txn_id = txn_id, "Event for unknown transaction");
            None
        }
    }

    /// End a transaction and emit END event.
    ///
    /// Returns END event if emission is enabled.
    pub async fn end_transaction(&self, txn_id: &str, ts_ms: i64) -> Option<TransactionEvent> {
        let mut in_flight = self.in_flight.write().await;

        if let Some(txn) = in_flight.remove(txn_id) {
            self.stats
                .in_flight_transactions
                .store(in_flight.len() as u64, Ordering::Relaxed);
            self.stats
                .transactions_completed
                .fetch_add(1, Ordering::Relaxed);

            let duration_ms = txn.duration_ms();

            if self.config.emit_end {
                self.stats.end_events.fetch_add(1, Ordering::Relaxed);

                // Build data collection counts
                let data_collections: Vec<DataCollectionEventCount> = txn
                    .data_collection_counts
                    .into_iter()
                    .map(|(dc, count)| DataCollectionEventCount {
                        data_collection: dc,
                        event_count: count,
                    })
                    .collect();

                debug!(
                    txn_id = txn_id,
                    event_count = txn.total_event_count,
                    tables = data_collections.len(),
                    duration_ms = duration_ms,
                    "Transaction END"
                );

                Some(TransactionEvent::end(
                    &txn.txn_id,
                    &txn.lsn,
                    ts_ms,
                    txn.total_event_count,
                    data_collections,
                ))
            } else {
                None
            }
        } else {
            // Orphan END without BEGIN
            self.stats.orphan_ends.fetch_add(1, Ordering::Relaxed);
            warn!(txn_id = txn_id, "END for unknown transaction (orphan)");
            None
        }
    }

    /// Get number of in-flight transactions.
    pub async fn in_flight_count(&self) -> usize {
        self.in_flight.read().await.len()
    }

    /// Clear all in-flight transactions (e.g., on reconnect).
    pub async fn clear_in_flight(&self) {
        let mut in_flight = self.in_flight.write().await;
        let count = in_flight.len();
        in_flight.clear();
        self.stats
            .in_flight_transactions
            .store(0, Ordering::Relaxed);

        if count > 0 {
            warn!(count = count, "Cleared in-flight transactions");
        }
    }

    /// Enrich a CDC event with transaction context.
    ///
    /// Modifies the event's transaction metadata in place.
    pub async fn enrich_event(&self, event: &mut CdcEvent) -> bool {
        if !self.config.enrich_events {
            return false;
        }

        // Get transaction ID from existing metadata
        let txn_id = match &event.transaction {
            Some(meta) => meta.id.clone(),
            None => return false,
        };

        let data_collection = format!("{}.{}", event.schema, event.table);

        if let Some(ctx) = self
            .track_event(&txn_id, &data_collection, event.timestamp)
            .await
        {
            // Update transaction metadata with order info
            if let Some(ref mut meta) = event.transaction {
                meta.id = ctx.id;
                meta.sequence = ctx.total_order;
            }
            true
        } else {
            false
        }
    }
}

impl Clone for TransactionTopicEmitter {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            in_flight: Arc::clone(&self.in_flight),
            stats: Arc::clone(&self.stats),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_event_begin() {
        let event = TransactionEvent::begin("555", "0/ABC123", 1705123456789);

        assert_eq!(event.status, TransactionStatus::Begin);
        assert_eq!(event.id, "555:0/ABC123");
        assert_eq!(event.ts_ms, 1705123456789);
        assert!(event.event_count.is_none());
        assert!(event.data_collections.is_none());
    }

    #[test]
    fn test_transaction_event_end() {
        let data_collections = vec![
            DataCollectionEventCount {
                data_collection: "public.orders".to_string(),
                event_count: 3,
            },
            DataCollectionEventCount {
                data_collection: "public.customers".to_string(),
                event_count: 2,
            },
        ];

        let event = TransactionEvent::end("555", "0/ABC123", 1705123456999, 5, data_collections);

        assert_eq!(event.status, TransactionStatus::End);
        assert_eq!(event.id, "555:0/ABC123");
        assert_eq!(event.event_count, Some(5));

        let dcs = event.data_collections.unwrap();
        assert_eq!(dcs.len(), 2);
        assert_eq!(dcs[0].data_collection, "public.orders");
        assert_eq!(dcs[0].event_count, 3);
    }

    #[test]
    fn test_transaction_event_serialization() {
        let event = TransactionEvent::begin("555", "0/ABC", 1705123456789);
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"status\":\"BEGIN\""));
        assert!(json.contains("\"id\":\"555:0/ABC\""));
        assert!(json.contains("\"ts_ms\":1705123456789"));
        // Should not contain event_count or data_collections for BEGIN
        assert!(!json.contains("event_count"));
        assert!(!json.contains("data_collections"));
    }

    #[test]
    fn test_transaction_event_end_serialization() {
        let data_collections = vec![DataCollectionEventCount {
            data_collection: "public.test".to_string(),
            event_count: 10,
        }];
        let event = TransactionEvent::end("123", "0/FF", 1705123456999, 10, data_collections);
        let json = serde_json::to_string(&event).unwrap();

        assert!(json.contains("\"status\":\"END\""));
        assert!(json.contains("\"event_count\":10"));
        assert!(json.contains("\"data_collections\""));
        assert!(json.contains("public.test"));
    }

    #[test]
    fn test_topic_name() {
        assert_eq!(
            TransactionEvent::topic_name("fulfillment"),
            "fulfillment.transaction"
        );
        assert_eq!(TransactionEvent::topic_name("mydb"), "mydb.transaction");
    }

    #[test]
    fn test_config_builder() {
        let config = TransactionTopicConfig::builder()
            .topic_prefix("testdb")
            .emit_begin(true)
            .emit_end(true)
            .enrich_events(false)
            .max_in_flight(500)
            .build();

        assert_eq!(config.topic_prefix, "testdb");
        assert!(config.emit_begin);
        assert!(config.emit_end);
        assert!(!config.enrich_events);
        assert_eq!(config.max_in_flight, 500);
        assert_eq!(config.transaction_topic(), "testdb.transaction");
    }

    #[tokio::test]
    async fn test_emitter_full_transaction() {
        let config = TransactionTopicConfig::builder()
            .topic_prefix("test")
            .emit_begin(true)
            .emit_end(true)
            .enrich_events(true)
            .build();

        let emitter = TransactionTopicEmitter::new(config);

        // Begin transaction
        let begin = emitter
            .begin_transaction("txn-1", "0/100", 1000)
            .await
            .unwrap();
        assert_eq!(begin.status, TransactionStatus::Begin);
        assert_eq!(begin.id, "txn-1:0/100");

        assert_eq!(emitter.in_flight_count().await, 1);

        // Track events
        let ctx1 = emitter
            .track_event("txn-1", "public.orders", 1001)
            .await
            .unwrap();
        assert_eq!(ctx1.total_order, 1);
        assert_eq!(ctx1.data_collection_order, 1);

        let ctx2 = emitter
            .track_event("txn-1", "public.orders", 1002)
            .await
            .unwrap();
        assert_eq!(ctx2.total_order, 2);
        assert_eq!(ctx2.data_collection_order, 2);

        let ctx3 = emitter
            .track_event("txn-1", "public.customers", 1003)
            .await
            .unwrap();
        assert_eq!(ctx3.total_order, 3);
        assert_eq!(ctx3.data_collection_order, 1); // First event for this table

        // End transaction
        let end = emitter.end_transaction("txn-1", 1010).await.unwrap();
        assert_eq!(end.status, TransactionStatus::End);
        assert_eq!(end.event_count, Some(3));

        let dcs = end.data_collections.unwrap();
        assert_eq!(dcs.len(), 2);

        // Find orders count
        let orders_count = dcs
            .iter()
            .find(|dc| dc.data_collection == "public.orders")
            .unwrap()
            .event_count;
        assert_eq!(orders_count, 2);

        // Find customers count
        let customers_count = dcs
            .iter()
            .find(|dc| dc.data_collection == "public.customers")
            .unwrap()
            .event_count;
        assert_eq!(customers_count, 1);

        // Should be cleared
        assert_eq!(emitter.in_flight_count().await, 0);

        // Check stats
        let stats = emitter.stats();
        assert_eq!(stats.begin_events, 1);
        assert_eq!(stats.end_events, 1);
        assert_eq!(stats.events_enriched, 3);
        assert_eq!(stats.transactions_completed, 1);
    }

    #[tokio::test]
    async fn test_emitter_orphan_end() {
        let config = TransactionTopicConfig::builder()
            .topic_prefix("test")
            .emit_end(true)
            .build();

        let emitter = TransactionTopicEmitter::new(config);

        // End without begin
        let end = emitter.end_transaction("unknown", 1000).await;
        assert!(end.is_none());

        let stats = emitter.stats();
        assert_eq!(stats.orphan_ends, 1);
    }

    #[tokio::test]
    async fn test_emitter_no_enrichment() {
        let config = TransactionTopicConfig::builder()
            .topic_prefix("test")
            .enrich_events(false)
            .build();

        let emitter = TransactionTopicEmitter::new(config);

        emitter.begin_transaction("txn-1", "0/100", 1000).await;

        // Track event but don't return context
        let ctx = emitter.track_event("txn-1", "public.test", 1001).await;
        assert!(ctx.is_none());

        let stats = emitter.stats();
        assert_eq!(stats.events_enriched, 0);
    }

    #[tokio::test]
    async fn test_emitter_clear_in_flight() {
        let config = TransactionTopicConfig::default();
        let emitter = TransactionTopicEmitter::new(config);

        emitter.begin_transaction("txn-1", "0/100", 1000).await;
        emitter.begin_transaction("txn-2", "0/200", 1001).await;

        assert_eq!(emitter.in_flight_count().await, 2);

        emitter.clear_in_flight().await;

        assert_eq!(emitter.in_flight_count().await, 0);
    }

    #[tokio::test]
    async fn test_transaction_context() {
        let ctx = TransactionContext::new("txn-1:0/ABC", 5, 3);

        assert_eq!(ctx.id, "txn-1:0/ABC");
        assert_eq!(ctx.total_order, 5);
        assert_eq!(ctx.data_collection_order, 3);

        // Serialize
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("\"id\":\"txn-1:0/ABC\""));
        assert!(json.contains("\"total_order\":5"));
        assert!(json.contains("\"data_collection_order\":3"));
    }

    #[test]
    fn test_data_collection_event_count_serialization() {
        let dc = DataCollectionEventCount {
            data_collection: "inventory.products".to_string(),
            event_count: 42,
        };

        let json = serde_json::to_string(&dc).unwrap();
        assert!(json.contains("\"data_collection\":\"inventory.products\""));
        assert!(json.contains("\"event_count\":42"));
    }
}
