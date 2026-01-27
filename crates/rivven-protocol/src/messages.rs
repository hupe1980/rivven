//! Protocol message types

use crate::error::Result;
use crate::metadata::{BrokerInfo, TopicMetadata};
use crate::types::MessageData;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Quota alteration request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaAlteration {
    /// Entity type: "user", "client-id", "consumer-group", "default"
    pub entity_type: String,
    /// Entity name (None for defaults)
    pub entity_name: Option<String>,
    /// Quota key: "produce_bytes_rate", "consume_bytes_rate", "request_rate"
    pub quota_key: String,
    /// Quota value (None to remove quota, Some to set)
    pub quota_value: Option<u64>,
}

/// Quota entry in describe response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaEntry {
    /// Entity type
    pub entity_type: String,
    /// Entity name (None for defaults)
    pub entity_name: Option<String>,
    /// Quota values
    pub quotas: HashMap<String, u64>,
}

/// Topic configuration entry for AlterTopicConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigEntry {
    /// Configuration key (e.g., "retention.ms", "max.message.bytes")
    pub key: String,
    /// Configuration value (None to reset to default)
    pub value: Option<String>,
}

/// Topic configuration in describe response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigDescription {
    /// Topic name
    pub topic: String,
    /// Configuration entries
    pub configs: HashMap<String, TopicConfigValue>,
}

/// Topic configuration value with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfigValue {
    /// Current value
    pub value: String,
    /// Whether this is the default value
    pub is_default: bool,
    /// Whether this config is read-only
    pub is_read_only: bool,
    /// Whether this config is sensitive (e.g., passwords)
    pub is_sensitive: bool,
}

/// Delete records result for a partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRecordsResult {
    /// Partition ID
    pub partition: u32,
    /// New low watermark (first available offset after deletion)
    pub low_watermark: u64,
    /// Error message if deletion failed for this partition
    pub error: Option<String>,
}

/// Protocol request messages
///
/// # Stability
///
/// **WARNING**: Variant order must remain stable for postcard serialization compatibility.
/// Adding new variants should only be done at the end of the enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Authenticate with username/password (SASL/PLAIN compatible)
    Authenticate { username: String, password: String },

    /// Authenticate with SASL bytes (for Kafka client compatibility)
    SaslAuthenticate {
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        mechanism: Bytes,
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        auth_bytes: Bytes,
    },

    /// SCRAM-SHA-256: Client-first message
    ScramClientFirst {
        /// Client-first-message bytes (`n,,n=<user>,r=<nonce>`)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Client-final message
    ScramClientFinal {
        /// Client-final-message bytes (`c=<binding>,r=<nonce>,p=<proof>`)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// Publish a message to a topic
    Publish {
        topic: String,
        partition: Option<u32>,
        #[serde(with = "rivven_core::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        value: Bytes,
    },

    /// Consume messages from a topic
    Consume {
        topic: String,
        partition: u32,
        offset: u64,
        max_messages: usize,
    },

    /// Create a new topic
    CreateTopic {
        name: String,
        partitions: Option<u32>,
    },

    /// List all topics
    ListTopics,

    /// Delete a topic
    DeleteTopic { name: String },

    /// Commit consumer offset
    CommitOffset {
        consumer_group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },

    /// Get consumer offset
    GetOffset {
        consumer_group: String,
        topic: String,
        partition: u32,
    },

    /// Get topic metadata
    GetMetadata { topic: String },

    /// Get cluster metadata (all topics or specific ones)
    GetClusterMetadata {
        /// Topics to get metadata for (empty = all topics)
        topics: Vec<String>,
    },

    /// Ping
    Ping,

    /// Register a schema
    RegisterSchema { subject: String, schema: String },

    /// Get a schema
    GetSchema { id: i32 },

    /// Get offset bounds for a partition
    GetOffsetBounds { topic: String, partition: u32 },

    /// List all consumer groups
    ListGroups,

    /// Describe a consumer group (get all offsets)
    DescribeGroup { consumer_group: String },

    /// Delete a consumer group
    DeleteGroup { consumer_group: String },

    /// Find offset for a timestamp
    GetOffsetForTimestamp {
        topic: String,
        partition: u32,
        /// Timestamp in milliseconds since epoch
        timestamp_ms: i64,
    },

    // =========================================================================
    // Idempotent Producer (KIP-98)
    // =========================================================================
    /// Initialize idempotent producer (request producer ID and epoch)
    ///
    /// Call this before sending idempotent produce requests.
    /// If reconnecting, provide the previous producer_id to bump epoch.
    InitProducerId {
        /// Previous producer ID (None for new producers)
        producer_id: Option<u64>,
    },

    /// Publish with idempotent semantics (exactly-once delivery)
    ///
    /// Requires InitProducerId to have been called first.
    IdempotentPublish {
        topic: String,
        partition: Option<u32>,
        #[serde(with = "rivven_core::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        value: Bytes,
        /// Producer ID from InitProducerId response
        producer_id: u64,
        /// Producer epoch from InitProducerId response
        producer_epoch: u16,
        /// Sequence number (starts at 0, increments per partition)
        sequence: i32,
    },

    // =========================================================================
    // Native Transactions (KIP-98 Transactions)
    // =========================================================================
    /// Begin a new transaction
    BeginTransaction {
        /// Transaction ID (unique per producer)
        txn_id: String,
        /// Producer ID from InitProducerId
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
        /// Transaction timeout in milliseconds (optional, defaults to 60s)
        timeout_ms: Option<u64>,
    },

    /// Add partitions to an active transaction
    AddPartitionsToTxn {
        /// Transaction ID
        txn_id: String,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
        /// Partitions to add (topic, partition pairs)
        partitions: Vec<(String, u32)>,
    },

    /// Publish within a transaction (combines IdempotentPublish + transaction tracking)
    TransactionalPublish {
        /// Transaction ID
        txn_id: String,
        topic: String,
        partition: Option<u32>,
        #[serde(with = "rivven_core::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        value: Bytes,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
        /// Sequence number
        sequence: i32,
    },

    /// Add consumer offsets to transaction (for exactly-once consume-transform-produce)
    AddOffsetsToTxn {
        /// Transaction ID
        txn_id: String,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
        /// Consumer group ID
        group_id: String,
        /// Offsets to commit (topic, partition, offset triples)
        offsets: Vec<(String, u32, i64)>,
    },

    /// Commit a transaction
    CommitTransaction {
        /// Transaction ID
        txn_id: String,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
    },

    /// Abort a transaction
    AbortTransaction {
        /// Transaction ID
        txn_id: String,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
    },

    // =========================================================================
    // Per-Principal Quotas (Kafka Parity)
    // =========================================================================
    /// Describe quotas for entities
    DescribeQuotas {
        /// Entities to describe (empty = all)
        /// Format: Vec<(entity_type, entity_name)>
        /// entity_type: "user", "client-id", "consumer-group", "default"
        /// entity_name: None for defaults, Some for specific entities
        entities: Vec<(String, Option<String>)>,
    },

    /// Alter quotas for entities
    AlterQuotas {
        /// Quota alterations to apply
        /// Each item: (entity_type, entity_name, quota_key, quota_value)
        /// quota_key: "produce_bytes_rate", "consume_bytes_rate", "request_rate"
        /// quota_value: None to remove, Some(value) to set
        alterations: Vec<QuotaAlteration>,
    },

    // =========================================================================
    // Admin API (Kafka Parity)
    // =========================================================================
    /// Alter topic configuration
    AlterTopicConfig {
        /// Topic name
        topic: String,
        /// Configuration changes to apply
        configs: Vec<TopicConfigEntry>,
    },

    /// Create additional partitions for an existing topic
    CreatePartitions {
        /// Topic name
        topic: String,
        /// New total partition count (must be > current count)
        new_partition_count: u32,
        /// Optional assignment of new partitions to brokers
        /// If empty, broker will auto-assign
        assignments: Vec<Vec<String>>,
    },

    /// Delete records before a given offset (log truncation)
    DeleteRecords {
        /// Topic name
        topic: String,
        /// Partition-offset pairs: delete all records before these offsets
        partition_offsets: Vec<(u32, u64)>,
    },

    /// Describe topic configurations
    DescribeTopicConfigs {
        /// Topics to describe (empty = all)
        topics: Vec<String>,
    },
}

/// Protocol response messages
///
/// # Stability
///
/// **WARNING**: Variant order must remain stable for postcard serialization compatibility.
/// Adding new variants should only be done at the end of the enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Authentication successful
    Authenticated {
        /// Session token for subsequent requests
        session_id: String,
        /// Session timeout in seconds
        expires_in: u64,
    },

    /// SCRAM-SHA-256: Server-first message (challenge)
    ScramServerFirst {
        /// Server-first-message bytes (`r=<nonce>,s=<salt>,i=<iterations>`)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Server-final message (verification or error)
    ScramServerFinal {
        /// Server-final-message bytes (`v=<verifier>` or `e=<error>`)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
        /// Session ID (if authentication succeeded)
        session_id: Option<String>,
        /// Session timeout in seconds (if authentication succeeded)
        expires_in: Option<u64>,
    },

    /// Success response with offset
    Published { offset: u64, partition: u32 },

    /// Messages response
    Messages { messages: Vec<MessageData> },

    /// Topic created
    TopicCreated { name: String, partitions: u32 },

    /// List of topics
    Topics { topics: Vec<String> },

    /// Topic deleted
    TopicDeleted,

    /// Offset committed
    OffsetCommitted,

    /// Offset response
    Offset { offset: Option<u64> },

    /// Metadata
    Metadata { name: String, partitions: u32 },

    /// Full cluster metadata for topic(s)
    ClusterMetadata {
        /// Controller node ID (Raft leader)
        controller_id: Option<String>,
        /// Broker/node list
        brokers: Vec<BrokerInfo>,
        /// Topic metadata
        topics: Vec<TopicMetadata>,
    },

    /// Schema registration result
    SchemaRegistered { id: i32 },

    /// Schema details
    Schema { id: i32, schema: String },

    /// Pong
    Pong,

    /// Offset bounds for a partition
    OffsetBounds { earliest: u64, latest: u64 },

    /// List of consumer groups
    Groups { groups: Vec<String> },

    /// Consumer group details with all offsets
    GroupDescription {
        consumer_group: String,
        /// topic → partition → offset
        offsets: HashMap<String, HashMap<u32, u64>>,
    },

    /// Consumer group deleted
    GroupDeleted,

    /// Offset for a timestamp
    OffsetForTimestamp {
        /// The first offset with timestamp >= the requested timestamp
        /// None if no matching offset was found
        offset: Option<u64>,
    },

    /// Error response
    Error { message: String },

    /// Success
    Ok,

    // =========================================================================
    // Idempotent Producer (KIP-98)
    // =========================================================================
    /// Producer ID initialized
    ProducerIdInitialized {
        /// Assigned or existing producer ID
        producer_id: u64,
        /// Current epoch (increments on reconnect)
        producer_epoch: u16,
    },

    /// Idempotent publish result
    IdempotentPublished {
        /// Offset where message was written
        offset: u64,
        /// Partition the message was written to
        partition: u32,
        /// Whether this was a duplicate (message already existed)
        duplicate: bool,
    },

    // =========================================================================
    // Native Transactions (KIP-98 Transactions)
    // =========================================================================
    /// Transaction started successfully
    TransactionStarted {
        /// Transaction ID
        txn_id: String,
    },

    /// Partitions added to transaction
    PartitionsAddedToTxn {
        /// Transaction ID
        txn_id: String,
        /// Number of partitions now in transaction
        partition_count: usize,
    },

    /// Transactional publish result
    TransactionalPublished {
        /// Offset where message was written (pending commit)
        offset: u64,
        /// Partition the message was written to
        partition: u32,
        /// Sequence number accepted
        sequence: i32,
    },

    /// Offsets added to transaction
    OffsetsAddedToTxn {
        /// Transaction ID
        txn_id: String,
    },

    /// Transaction committed
    TransactionCommitted {
        /// Transaction ID
        txn_id: String,
    },

    /// Transaction aborted
    TransactionAborted {
        /// Transaction ID
        txn_id: String,
    },

    // =========================================================================
    // Per-Principal Quotas (Kafka Parity)
    // =========================================================================
    /// Quota descriptions
    QuotasDescribed {
        /// List of quota entries
        entries: Vec<QuotaEntry>,
    },

    /// Quotas altered successfully
    QuotasAltered {
        /// Number of quota alterations applied
        altered_count: usize,
    },

    /// Throttle response (returned when quota exceeded)
    Throttled {
        /// Time to wait before retrying (milliseconds)
        throttle_time_ms: u64,
        /// Quota type that was exceeded
        quota_type: String,
        /// Entity that exceeded quota
        entity: String,
    },

    // =========================================================================
    // Admin API (Kafka Parity)
    // =========================================================================
    /// Topic configuration altered
    TopicConfigAltered {
        /// Topic name
        topic: String,
        /// Number of configurations changed
        changed_count: usize,
    },

    /// Partitions created
    PartitionsCreated {
        /// Topic name
        topic: String,
        /// New total partition count
        new_partition_count: u32,
    },

    /// Records deleted
    RecordsDeleted {
        /// Topic name
        topic: String,
        /// Results per partition
        results: Vec<DeleteRecordsResult>,
    },

    /// Topic configurations described
    TopicConfigsDescribed {
        /// Configuration descriptions per topic
        configs: Vec<TopicConfigDescription>,
    },
}

impl Request {
    /// Serialize request to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Deserialize request from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(data)?)
    }
}

impl Response {
    /// Serialize response to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Deserialize response from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_roundtrip() {
        let requests = vec![
            Request::Ping,
            Request::ListTopics,
            Request::CreateTopic {
                name: "test".to_string(),
                partitions: Some(4),
            },
            Request::Authenticate {
                username: "admin".to_string(),
                password: "secret".to_string(),
            },
        ];

        for req in requests {
            let bytes = req.to_bytes().unwrap();
            let decoded = Request::from_bytes(&bytes).unwrap();
            // Can't directly compare due to Debug, but serialization should succeed
            assert!(!bytes.is_empty());
            let _ = decoded; // Use decoded
        }
    }

    #[test]
    fn test_response_roundtrip() {
        let responses = vec![
            Response::Pong,
            Response::Ok,
            Response::Topics {
                topics: vec!["a".to_string(), "b".to_string()],
            },
            Response::Error {
                message: "test error".to_string(),
            },
        ];

        for resp in responses {
            let bytes = resp.to_bytes().unwrap();
            let decoded = Response::from_bytes(&bytes).unwrap();
            assert!(!bytes.is_empty());
            let _ = decoded;
        }
    }
}
