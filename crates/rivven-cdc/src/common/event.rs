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

/// CDC operation type representing the kind of change captured from the source database.
///
/// # CDC Semantics
///
/// These variants map to database-level operations as observed through the
/// replication stream. In CDC terminology:
///
/// - **Insert** / **Update** / **Delete** correspond to DML row-level changes.
///   For PostgreSQL logical replication, `Insert` maps to the WAL `INSERT` message,
///   `Update` to the `UPDATE` message (which may carry both before and after images
///   depending on `REPLICA IDENTITY`), and `Delete` to the `DELETE` message
///   (which carries the old key columns or full row depending on `REPLICA IDENTITY`).
///
/// - **Tombstone** is a Rivven-level concept (not a database operation) used for
///   downstream log compaction.
///
/// - **Snapshot** represents rows read during the initial consistent snapshot,
///   not a real-time change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CdcOp {
    /// A new row was inserted into the source table.
    ///
    /// The `after` field of the corresponding [`CdcEvent`] contains the full row data.
    /// The `before` field is `None`.
    Insert,
    /// An existing row was updated in the source table.
    ///
    /// The `after` field contains the new row state. The `before` field may contain
    /// the previous row state if the source database is configured to emit it
    /// (e.g., PostgreSQL `REPLICA IDENTITY FULL`). With the default replica identity,
    /// `before` only contains the primary key columns.
    Update,
    /// A row was deleted from the source table.
    ///
    /// The `before` field contains the deleted row's key columns (or the full row
    /// if `REPLICA IDENTITY FULL` is configured). The `after` field is `None`.
    Delete,
    /// Tombstone marker (null payload for log compaction).
    ///
    /// A tombstone is emitted after a `Delete` event with the same key
    /// but null value. This signals to log compaction that the key
    /// should be removed from the compacted log. This is a Rivven-internal
    /// concept and does not correspond to a database operation.
    Tombstone,
    /// All rows in the table were removed via a `TRUNCATE` statement.
    Truncate,
    /// A row read during the initial consistent snapshot (not a real-time change).
    ///
    /// Snapshot events are produced during the initial sync phase before
    /// streaming begins. They represent the table's state at a specific
    /// point-in-time and are semantically equivalent to inserts.
    Snapshot,
    /// Schema change (DDL event).
    ///
    /// Published to a dedicated schema_changes topic when table
    /// structure changes (CREATE/ALTER/DROP TABLE, etc.)
    Schema,
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

    /// Create a tombstone event (null payload for log compaction).
    ///
    /// Tombstones are emitted after DELETE events with the same key but
    /// null payload. Kafka log compaction uses tombstones to know when
    /// a key should be completely removed from the compacted log.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rivven_cdc::CdcEvent;
    /// use serde_json::json;
    ///
    /// // After a DELETE, emit a tombstone with the same key
    /// let delete = CdcEvent::delete("pg", "db", "public", "users", json!({"id": 1}), 1000);
    /// let tombstone = CdcEvent::tombstone(&delete);
    ///
    /// // Tombstone has same coordinates but null payload
    /// assert!(tombstone.before.is_none());
    /// assert!(tombstone.after.is_none());
    /// ```
    pub fn tombstone(delete_event: &CdcEvent) -> Self {
        Self {
            source_type: delete_event.source_type.clone(),
            database: delete_event.database.clone(),
            schema: delete_event.schema.clone(),
            table: delete_event.table.clone(),
            op: CdcOp::Tombstone,
            before: None,
            after: None,
            timestamp: delete_event.timestamp,
            transaction: delete_event.transaction.clone(),
        }
    }

    /// Create a tombstone with explicit key for log compaction.
    ///
    /// Use this when you need to specify the key explicitly rather than
    /// deriving it from a delete event.
    pub fn tombstone_with_key(
        source_type: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        key: serde_json::Value,
        timestamp: i64,
    ) -> Self {
        Self {
            source_type: source_type.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            op: CdcOp::Tombstone,
            // Store key in `before` for key extraction during compaction
            before: Some(key),
            after: None,
            timestamp,
            transaction: None,
        }
    }

    /// Convert a DELETE event to a tombstone.
    ///
    /// Returns None if this is not a DELETE event.
    pub fn to_tombstone(&self) -> Option<Self> {
        if self.op == CdcOp::Delete {
            Some(CdcEvent::tombstone(self))
        } else {
            None
        }
    }

    /// Check if this is a tombstone event.
    pub fn is_tombstone(&self) -> bool {
        self.op == CdcOp::Tombstone
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
        self.transaction
            .as_ref()
            .map(|t| t.is_last)
            .unwrap_or(false)
    }

    /// Serialize to JSON bytes.
    ///
    /// This is a convenience method for serialization. For production use
    /// with schema registry, use rivven-connect which handles Avro/Protobuf.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Get the topic name for this event
    /// Format: cdc.{database}.{schema}.{table}
    pub fn topic_name(&self) -> String {
        format!("cdc.{}.{}.{}", self.database, self.schema, self.table)
    }

    /// Check if this is a data modification event (INSERT/UPDATE/DELETE)
    pub fn is_dml(&self) -> bool {
        matches!(
            self.op,
            CdcOp::Insert | CdcOp::Update | CdcOp::Delete | CdcOp::Tombstone
        )
    }
}

impl std::fmt::Display for CdcOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdcOp::Insert => write!(f, "INSERT"),
            CdcOp::Update => write!(f, "UPDATE"),
            CdcOp::Delete => write!(f, "DELETE"),
            CdcOp::Tombstone => write!(f, "TOMBSTONE"),
            CdcOp::Truncate => write!(f, "TRUNCATE"),
            CdcOp::Snapshot => write!(f, "SNAPSHOT"),
            CdcOp::Schema => write!(f, "SCHEMA"),
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
    fn test_to_json_bytes() {
        let event = CdcEvent::insert(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 1}),
            1705000000,
        );

        let bytes = event.to_json_bytes().unwrap();
        assert!(!bytes.is_empty());

        // Verify it's valid JSON
        let _: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    }

    #[test]
    fn test_is_dml() {
        assert!(CdcEvent::insert("pg", "db", "s", "t", json!({}), 0).is_dml());
        assert!(CdcEvent::update("pg", "db", "s", "t", None, json!({}), 0).is_dml());
        assert!(CdcEvent::delete("pg", "db", "s", "t", json!({}), 0).is_dml());
        // Tombstone is also DML (for log compaction)
        let delete = CdcEvent::delete("pg", "db", "s", "t", json!({}), 0);
        let tombstone = CdcEvent::tombstone(&delete);
        assert!(tombstone.is_dml());
    }

    #[test]
    fn test_tombstone_from_delete() {
        let delete = CdcEvent::delete(
            "postgres",
            "mydb",
            "public",
            "users",
            json!({"id": 42, "name": "Alice"}),
            1705000000,
        );

        let tombstone = CdcEvent::tombstone(&delete);

        assert_eq!(tombstone.op, CdcOp::Tombstone);
        assert_eq!(tombstone.source_type, "postgres");
        assert_eq!(tombstone.database, "mydb");
        assert_eq!(tombstone.schema, "public");
        assert_eq!(tombstone.table, "users");
        assert!(tombstone.before.is_none());
        assert!(tombstone.after.is_none());
        assert_eq!(tombstone.timestamp, delete.timestamp);
        assert!(tombstone.is_tombstone());
    }

    #[test]
    fn test_tombstone_with_key() {
        let tombstone = CdcEvent::tombstone_with_key(
            "mysql",
            "inventory",
            "inventory",
            "products",
            json!({"product_id": 101}),
            1705000000,
        );

        assert_eq!(tombstone.op, CdcOp::Tombstone);
        assert!(tombstone.is_tombstone());
        // Key stored in before for compaction key extraction
        assert_eq!(tombstone.before, Some(json!({"product_id": 101})));
        assert!(tombstone.after.is_none());
    }

    #[test]
    fn test_to_tombstone_from_delete() {
        let delete = CdcEvent::delete("pg", "db", "s", "t", json!({"id": 1}), 0);
        let tombstone = delete.to_tombstone();

        assert!(tombstone.is_some());
        let t = tombstone.unwrap();
        assert!(t.is_tombstone());
    }

    #[test]
    fn test_to_tombstone_from_insert_returns_none() {
        let insert = CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 0);
        assert!(insert.to_tombstone().is_none());
    }

    #[test]
    fn test_tombstone_preserves_transaction() {
        let txn = TransactionMetadata::new("txn-del", "0/DEAD", 5)
            .with_total(10)
            .with_last();

        let delete =
            CdcEvent::delete("pg", "db", "s", "t", json!({"id": 1}), 1000).with_transaction(txn);

        let tombstone = CdcEvent::tombstone(&delete);

        assert!(tombstone.has_transaction());
        assert_eq!(tombstone.txn_id(), Some("txn-del"));
        assert!(tombstone.is_txn_end());
    }

    #[test]
    fn test_tombstone_serialization() {
        let delete = CdcEvent::delete("pg", "db", "s", "t", json!({"id": 99}), 0);
        let tombstone = CdcEvent::tombstone(&delete);

        let json = serde_json::to_string(&tombstone).unwrap();
        assert!(json.contains("\"op\":\"Tombstone\""));
        assert!(json.contains("\"before\":null"));
        assert!(json.contains("\"after\":null"));

        let parsed: CdcEvent = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_tombstone());
        assert!(parsed.before.is_none());
        assert!(parsed.after.is_none());
    }

    #[test]
    fn test_cdcop_display_tombstone() {
        assert_eq!(CdcOp::Tombstone.to_string(), "TOMBSTONE");
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

        let event =
            CdcEvent::insert("pg", "db", "s", "t", json!({"id": 1}), 0).with_transaction(txn);

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
