//! Message data types for protocol transport

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Schema type (format) for schema registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    /// Apache Avro (recommended for production)
    #[default]
    #[serde(alias = "avro", alias = "AVRO")]
    Avro,

    /// JSON Schema
    #[serde(alias = "json", alias = "JSON")]
    Json,

    /// Protocol Buffers
    #[serde(alias = "protobuf", alias = "PROTOBUF")]
    Protobuf,
}

impl SchemaType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaType::Avro => "AVRO",
            SchemaType::Json => "JSON",
            SchemaType::Protobuf => "PROTOBUF",
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for SchemaType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "AVRO" => Ok(SchemaType::Avro),
            "JSON" | "JSONSCHEMA" | "JSON_SCHEMA" => Ok(SchemaType::Json),
            "PROTOBUF" | "PROTO" => Ok(SchemaType::Protobuf),
            _ => Err(format!("Unknown schema type: {}", s)),
        }
    }
}

/// Serialized message data for transport
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageData {
    /// Message offset in the partition
    pub offset: u64,
    /// Partition the message belongs to
    #[serde(default)]
    pub partition: u32,
    /// Optional message key
    #[serde(with = "crate::serde_utils::option_bytes_serde")]
    pub key: Option<Bytes>,
    /// Message value/payload
    #[serde(with = "crate::serde_utils::bytes_serde")]
    pub value: Bytes,
    /// Timestamp in milliseconds since epoch
    pub timestamp: i64,
    /// Record headers (key-value metadata)
    #[serde(default)]
    pub headers: Vec<(String, Vec<u8>)>,
}

impl MessageData {
    /// Create a new message
    pub fn new(offset: u64, value: impl Into<Bytes>, timestamp: i64) -> Self {
        Self {
            offset,
            partition: 0,
            key: None,
            value: value.into(),
            timestamp,
            headers: Vec::new(),
        }
    }

    /// Set the key
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the partition
    pub fn with_partition(mut self, partition: u32) -> Self {
        self.partition = partition;
        self
    }

    /// Add a header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Get the value as bytes
    pub fn value_bytes(&self) -> &[u8] {
        &self.value
    }

    /// Get the key as bytes if present
    pub fn key_bytes(&self) -> Option<&[u8]> {
        self.key.as_ref().map(|k| k.as_ref())
    }

    /// Get the size of this message (key + value + headers)
    pub fn size(&self) -> usize {
        let key_size = self.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let header_size: usize = self.headers.iter().map(|(k, v)| k.len() + v.len()).sum();
        key_size + self.value.len() + header_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_data() {
        let msg = MessageData::new(42, b"hello".to_vec(), 1234567890).with_key(b"key1".to_vec());

        assert_eq!(msg.offset, 42);
        assert_eq!(msg.value_bytes(), b"hello");
        assert_eq!(msg.key_bytes(), Some(b"key1".as_slice()));
        assert_eq!(msg.size(), 4 + 5); // key + value
    }

    #[test]
    fn test_message_data_no_key() {
        let msg = MessageData::new(0, b"data".to_vec(), 0);

        assert!(msg.key.is_none());
        assert_eq!(msg.key_bytes(), None);
        assert_eq!(msg.size(), 4);
    }
}
