use crate::serde_utils::{bytes_serde, option_bytes_serde};
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
        }
    }

    /// Add a header to the message
    pub fn add_header(mut self, key: String, value: Vec<u8>) -> Self {
        self.headers.push((key, value));
        self
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> crate::Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}
