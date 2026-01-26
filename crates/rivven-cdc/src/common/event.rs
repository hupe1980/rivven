//! CDC Event representation
//!
//! Unified event structure for all CDC sources (PostgreSQL, MySQL, MariaDB).
//!
//! ## Transaction Metadata
//!
//! CDC events include optional transaction metadata for:
//! - **Transaction grouping**: Events from same DB transaction share txn_id
//! - **Exactly-once processing**: LSN/position enables precise checkpointing
//! - **Ordering**: Sequence numbers ensure correct order within transactions
//!
//! ```ignore
//! // Events from same transaction
//! event1.transaction.as_ref().map(|t| &t.id) == event2.transaction.as_ref().map(|t| &t.id)
//! ```

use crate::common::Result;
use rivven_core::Message;

/// Transaction metadata for CDC events.
///
/// Enables grouping events by database transaction for atomic processing.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionMetadata {
    /// Transaction ID (PostgreSQL: xid, MySQL: GTID or XID)
    pub id: String,
    /// Log sequence number / position (PostgreSQL: LSN, MySQL: binlog position)
    pub lsn: String,
    /// Sequence number within transaction (0-indexed)
    pub sequence: u64,
    /// Total events in transaction (if known, 0 = unknown)
    pub total_events: u64,
    /// Transaction commit timestamp (Unix epoch millis)
    pub commit_ts: Option<i64>,
    /// Is this the last event in the transaction?
    pub is_last: bool,
}

impl TransactionMetadata {
    /// Create new transaction metadata.
    pub fn new(id: impl Into<String>, lsn: impl Into<String>, sequence: u64) -> Self {
        Self {
            id: id.into(),
            lsn: lsn.into(),
            sequence,
            total_events: 0,
            commit_ts: None,
            is_last: false,
        }
    }

    /// Set total events count.
    pub fn with_total(mut self, total: u64) -> Self {
        self.total_events = total;
        self
    }

    /// Set commit timestamp.
    pub fn with_commit_ts(mut self, ts: i64) -> Self {
        self.commit_ts = Some(ts);
        self
    }

    /// Mark as last event in transaction.
    pub fn with_last(mut self) -> Self {
        self.is_last = true;
        self
    }

    /// Check if this is a single-event transaction.
    pub fn is_single_event(&self) -> bool {
        self.total_events == 1 || (self.sequence == 0 && self.is_last)
    }
}

/// Represents a change captured from a database
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CdcEvent {
    /// Source database type: "postgres", "mysql", "mariadb"
    pub source_type: String,
    /// Database name
    pub database: String,
    /// Schema name (PostgreSQL) or database name (MySQL)
    pub schema: String,
    /// Table name
    pub table: String,
    /// Operation type
    pub op: CdcOp,
    /// Previous row state (for UPDATE/DELETE)
    pub before: Option<serde_json::Value>,
    /// Current row state (for INSERT/UPDATE)
    pub after: Option<serde_json::Value>,
    /// Event timestamp (Unix epoch seconds)
    pub timestamp: i64,
    /// Transaction metadata (optional, for transaction-aware processing)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionMetadata>,
}

/// CDC operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CdcOp {
    /// Row inserted
    Insert,
    /// Row updated
    Update,
    /// Row deleted
    Delete,
    /// Table truncated
    Truncate,
    /// Snapshot read (initial sync)
    Snapshot,
}

impl CdcEvent {
    /// Create a new INSERT event
    pub fn insert(
        source_type: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        data: serde_json::Value,
        timestamp: i64,
    ) -> Self {
        Self {
            source_type: source_type.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            op: CdcOp::Insert,
            before: None,
            after: Some(data),
            timestamp,
            transaction: None,
        }
    }

    /// Create a new UPDATE event
    pub fn update(
        source_type: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        before: Option<serde_json::Value>,
        after: serde_json::Value,
        timestamp: i64,
    ) -> Self {
        Self {
            source_type: source_type.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            op: CdcOp::Update,
            before,
            after: Some(after),
            timestamp,
            transaction: None,
        }
    }

    /// Create a new DELETE event
    pub fn delete(
        source_type: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        before: serde_json::Value,
        timestamp: i64,
    ) -> Self {
        Self {
            source_type: source_type.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            op: CdcOp::Delete,
            before: Some(before),
            after: None,
            timestamp,
            transaction: None,
        }
    }

    /// Attach transaction metadata to this event.
    pub fn with_transaction(mut self, txn: TransactionMetadata) -> Self {
        self.transaction = Some(txn);
        self
    }

    /// Get transaction ID if available.
    pub fn txn_id(&self) -> Option<&str> {
        self.transaction.as_ref().map(|t| t.id.as_str())
    }

    /// Check if this event is part of a transaction.
    pub fn has_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Check if this is the last event in a transaction.
    pub fn is_txn_end(&self) -> bool {
        self.transaction.as_ref().map(|t| t.is_last).unwrap_or(false)
    }

    /// Convert to a Rivven Message for topic publishing
    pub fn to_message(&self) -> Result<Message> {
        let json_bytes = serde_json::to_vec(self)?;

        let msg = Message::new(bytes::Bytes::from(json_bytes))
            .add_header("cdc_source".to_string(), self.source_type.as_bytes().to_vec())
            .add_header("cdc_database".to_string(), self.database.as_bytes().to_vec())
            .add_header("cdc_schema".to_string(), self.schema.as_bytes().to_vec())
            .add_header("cdc_table".to_string(), self.table.as_bytes().to_vec())
            .add_header("cdc_op".to_string(), format!("{:?}", self.op).as_bytes().to_vec());

        Ok(msg)
    }

    /// Get the topic name for this event
    /// Format: cdc.{database}.{schema}.{table}
    pub fn topic_name(&self) -> String {
        format!("cdc.{}.{}.{}", self.database, self.schema, self.table)
    }

    /// Check if this is a data modification event (INSERT/UPDATE/DELETE)
    pub fn is_dml(&self) -> bool {
        matches!(self.op, CdcOp::Insert | CdcOp::Update | CdcOp::Delete)
    }
}

impl std::fmt::Display for CdcOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcOp::Insert => write!(f, "INSERT"),
            CdcOp::Update => write!(f, "UPDATE"),
            CdcOp::Delete => write!(f, "DELETE"),
            CdcOp::Truncate => write!(f, "TRUNCATE"),
            CdcOp::Snapshot => write!(f, "SNAPSHOT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_event() {
        let event = CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 1, "name": "Alice"}),
            1705000000,
        );

        assert_eq!(event.op, CdcOp::Insert);
        assert!(event.before.is_none());
        assert!(event.after.is_some());
        assert_eq!(event.topic_name(), "cdc.mydb.public.users");
    }

    #[test]
    fn test_update_event() {
        let event = CdcEvent::update(
            "mysql",
            "mydb",
            "mydb",
            "users",
            Some(json!({"id": 1, "name": "Alice"})),
            json!({"id": 1, "name": "Bob"}),
            1705000000,
        );

        assert_eq!(event.op, CdcOp::Update);
        assert!(event.before.is_some());
        assert!(event.after.is_some());
    }

    #[test]
    fn test_delete_event() {
        let event = CdcEvent::delete(
            "mariadb",
            "mydb",
            "mydb",
            "users",
            json!({"id": 1}),
            1705000000,
        );

        assert_eq!(event.op, CdcOp::Delete);
        assert!(event.before.is_some());
        assert!(event.after.is_none());
    }

    #[test]
    fn test_to_message() {
        let event = CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 1}),
            1705000000,
        );

        let msg = event.to_message().unwrap();
        assert!(!msg.value.is_empty());
    }

    #[test]
    fn test_is_dml() {
        assert!(CdcEvent::insert("pg", "db", "s", "t", json!({}), 0).is_dml());
        assert!(CdcEvent::update("pg", "db", "s", "t", None, json!({}), 0).is_dml());
        assert!(CdcEvent::delete("pg", "db", "s", "t", json!({}), 0).is_dml());
    }

    #[test]
    fn test_transaction_metadata() {
        let txn = TransactionMetadata::new("txn-123", "0/1234", 0)
            .with_total(3)
            .with_commit_ts(1705000000);

        assert_eq!(txn.id, "txn-123");
        assert_eq!(txn.lsn, "0/1234");
        assert_eq!(txn.sequence, 0);
        assert_eq!(txn.total_events, 3);
        assert_eq!(txn.commit_ts, Some(1705000000));
        assert!(!txn.is_last);
        assert!(!txn.is_single_event());
    }

    #[test]
    fn test_transaction_metadata_single_event() {
        let txn = TransactionMetadata::new("txn-456", "0/5678", 0)
            .with_total(1)
            .with_last();

        assert!(txn.is_single_event());
        assert!(txn.is_last);
    }

    #[test]
    fn test_event_with_transaction() {
        let txn = TransactionMetadata::new("txn-789", "0/ABCD", 1)
            .with_total(5)
            .with_last();

        let event = CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 1}),
            1705000000,
        )
        .with_transaction(txn);

        assert!(event.has_transaction());
        assert_eq!(event.txn_id(), Some("txn-789"));
        assert!(event.is_txn_end());
    }

    #[test]
    fn test_event_without_transaction() {
        let event = CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 1}),
            1705000000,
        );

        assert!(!event.has_transaction());
        assert_eq!(event.txn_id(), None);
        assert!(!event.is_txn_end());
    }

    #[test]
    fn test_transaction_serialization() {
        let txn = TransactionMetadata::new("txn-ser", "0/FF00", 2)
            .with_total(10)
            .with_commit_ts(1705000000);

        let event = CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 0)
            .with_transaction(txn);

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("transaction"));
        assert!(json.contains("txn-ser"));
        assert!(json.contains("0/FF00"));

        let parsed: CdcEvent = serde_json::from_str(&json).unwrap();
        assert!(parsed.has_transaction());
        assert_eq!(parsed.txn_id(), Some("txn-ser"));
    }

    #[test]
    fn test_event_without_transaction_serialization() {
        // Transaction field should be omitted when None
        let event = CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 0);

        let json = serde_json::to_string(&event).unwrap();
        assert!(!json.contains("transaction"));

        let parsed: CdcEvent = serde_json::from_str(&json).unwrap();
        assert!(!parsed.has_transaction());
    }
}
