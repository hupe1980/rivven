use crate::serde_utils::{bytes_serde, option_bytes_serde};
use crate::transaction::TransactionMarker;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents a single message in Rivven
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique offset within the partition
    pub offset: u64,

    /// Message key (optional, used for partitioning)
    #[serde(with = "option_bytes_serde")]
    pub key: Option<Bytes>,

    /// Message payload
    #[serde(with = "bytes_serde")]
    pub value: Bytes,

    /// Timestamp when message was created
    pub timestamp: DateTime<Utc>,

    /// Optional headers for metadata
    pub headers: Vec<(String, Vec<u8>)>,

    /// Producer ID (for transactional/idempotent messages)
    /// None for non-transactional messages
    #[serde(default)]
    pub producer_id: Option<u64>,

    /// Producer epoch (for fencing)
    #[serde(default)]
    pub producer_epoch: Option<u16>,

    /// Transaction marker (Some for control records, None for data records)
    /// Control records mark transaction boundaries (COMMIT/ABORT)
    #[serde(default)]
    pub transaction_marker: Option<TransactionMarker>,

    /// Whether this message is part of an ongoing transaction
    /// Used for read_committed filtering
    #[serde(default)]
    pub is_transactional: bool,
}

impl Message {
    /// Create a new message
    pub fn new(value: Bytes) -> Self {
        Self {
            offset: 0,
            key: None,
            value,
            timestamp: Utc::now(),
            headers: Vec::new(),
            producer_id: None,
            producer_epoch: None,
            transaction_marker: None,
            is_transactional: false,
        }
    }

    /// Create a message with a key
    pub fn with_key(key: Bytes, value: Bytes) -> Self {
        Self {
            offset: 0,
            key: Some(key),
            value,
            timestamp: Utc::now(),
            headers: Vec::new(),
            producer_id: None,
            producer_epoch: None,
            transaction_marker: None,
            is_transactional: false,
        }
    }

    /// Create a transactional message
    pub fn transactional(value: Bytes, producer_id: u64, producer_epoch: u16) -> Self {
        Self {
            offset: 0,
            key: None,
            value,
            timestamp: Utc::now(),
            headers: Vec::new(),
            producer_id: Some(producer_id),
            producer_epoch: Some(producer_epoch),
            transaction_marker: None,
            is_transactional: true,
        }
    }

    /// Create a transactional message with a key
    pub fn transactional_with_key(
        key: Bytes,
        value: Bytes,
        producer_id: u64,
        producer_epoch: u16,
    ) -> Self {
        Self {
            offset: 0,
            key: Some(key),
            value,
            timestamp: Utc::now(),
            headers: Vec::new(),
            producer_id: Some(producer_id),
            producer_epoch: Some(producer_epoch),
            transaction_marker: None,
            is_transactional: true,
        }
    }

    /// Create a transaction control record (COMMIT or ABORT marker)
    pub fn control_record(
        marker: TransactionMarker,
        producer_id: u64,
        producer_epoch: u16,
    ) -> Self {
        Self {
            offset: 0,
            key: None,
            value: Bytes::new(), // Control records have empty value
            timestamp: Utc::now(),
            headers: Vec::new(),
            producer_id: Some(producer_id),
            producer_epoch: Some(producer_epoch),
            transaction_marker: Some(marker),
            is_transactional: true,
        }
    }

    /// Check if this is a control record (transaction marker)
    pub fn is_control_record(&self) -> bool {
        self.transaction_marker.is_some()
    }

    /// Check if this message is from a committed transaction
    /// Note: This is set after transaction completion, not during write
    pub fn is_committed(&self) -> bool {
        !self.is_transactional || matches!(self.transaction_marker, Some(TransactionMarker::Commit))
    }

    /// Add a header to the message
    pub fn add_header(mut self, key: String, value: Vec<u8>) -> Self {
        self.headers.push((key, value));
        self
    }

    /// Mark as transactional
    pub fn with_producer(
        mut self,
        producer_id: u64,
        producer_epoch: u16,
        transactional: bool,
    ) -> Self {
        self.producer_id = Some(producer_id);
        self.producer_epoch = Some(producer_epoch);
        self.is_transactional = transactional;
        self
    }

    /// Serialize to bytes (allocates a new Vec).
    ///
    /// For the hot path (segment append), prefer `postcard::to_extend` directly
    /// to avoid intermediate allocations — see `Segment::append()`.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> crate::Result<Self> {
        Ok(postcard::from_bytes(data)?)
    }
}
