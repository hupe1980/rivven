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
#[derive(Clone, Serialize, Deserialize)]
pub enum Request {
    /// Authenticate with username/password (SASL/PLAIN compatible).
    ///
    /// # Security — transport encryption required
    ///
    /// The password is sent in **plaintext** on the wire (SASL/PLAIN).
    /// This variant must only be used over a TLS-encrypted connection;
    /// otherwise the password is exposed to network observers.
    ///
    /// # Deprecation
    ///
    /// Prefer `ScramClientFirst` / `ScramClientFinal` (SCRAM-SHA-256
    /// challenge-response) which never sends the password over the wire.
    #[deprecated(
        note = "Use SCRAM-SHA-256 (ScramClientFirst/ScramClientFinal) instead of plaintext auth"
    )]
    Authenticate {
        username: String,
        password: String,
        /// When `true` the server **must** reject this request if the
        /// connection is not TLS-encrypted, preventing accidental
        /// credential exposure on cleartext transports.
        #[serde(default)]
        require_tls: bool,
    },

    /// Authenticate with SASL bytes (for Kafka client compatibility)
    SaslAuthenticate {
        #[serde(with = "crate::serde_utils::bytes_serde")]
        mechanism: Bytes,
        #[serde(with = "crate::serde_utils::bytes_serde")]
        auth_bytes: Bytes,
    },

    /// SCRAM-SHA-256: Client-first message
    ScramClientFirst {
        /// Client-first-message bytes (`n,,n=<user>,r=<nonce>`)
        #[serde(with = "crate::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Client-final message
    ScramClientFinal {
        /// Client-final-message bytes (`c=<binding>,r=<nonce>,p=<proof>`)
        #[serde(with = "crate::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// Publish a message to a topic
    Publish {
        topic: String,
        partition: Option<u32>,
        #[serde(with = "crate::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "crate::serde_utils::bytes_serde")]
        value: Bytes,
        /// Leader epoch for data-path fencing (§2.4).
        /// When set, the broker rejects the write if its current leader epoch
        /// for this partition is higher, preventing stale-leader writes.
        #[serde(default)]
        leader_epoch: Option<u64>,
    },

    /// Consume messages from a topic
    Consume {
        topic: String,
        partition: u32,
        offset: u64,
        max_messages: usize,
        /// Isolation level for transactional reads
        /// None = read_uncommitted (default, backward compatible)
        /// Some(0) = read_uncommitted
        /// Some(1) = read_committed (filters aborted transaction messages)
        #[serde(default)]
        isolation_level: Option<u8>,
        /// Long-polling: maximum time (ms) to wait for new data before returning
        /// an empty response. None or 0 = immediate (no waiting). Capped at 30 000 ms.
        #[serde(default)]
        max_wait_ms: Option<u64>,
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
    // Idempotent Producer
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
        #[serde(with = "crate::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "crate::serde_utils::bytes_serde")]
        value: Bytes,
        /// Producer ID from InitProducerId response
        producer_id: u64,
        /// Producer epoch from InitProducerId response
        producer_epoch: u16,
        /// Sequence number (starts at 0, increments per partition)
        sequence: i32,
        /// Leader epoch for fencing stale writes (§2.4)
        #[serde(default)]
        leader_epoch: Option<u64>,
    },

    // =========================================================================
    // Native Transactions
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
        #[serde(with = "crate::serde_utils::option_bytes_serde")]
        key: Option<Bytes>,
        #[serde(with = "crate::serde_utils::bytes_serde")]
        value: Bytes,
        /// Producer ID
        producer_id: u64,
        /// Producer epoch
        producer_epoch: u16,
        /// Sequence number
        sequence: i32,
        /// Leader epoch for fencing stale writes (§2.4)
        #[serde(default)]
        leader_epoch: Option<u64>,
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

    /// Protocol version handshake
    ///
    /// Sent by the client as the first message after connecting.
    /// The server validates the protocol version and returns compatibility info.
    Handshake {
        /// Client's protocol version (must match `PROTOCOL_VERSION`)
        protocol_version: u32,
        /// Optional client identifier for diagnostics
        client_id: String,
    },
}

// Manual Debug impl to redact credentials from log output.
// `Request::Authenticate { password }` and SASL/SCRAM variants contain
// secrets that must never appear in debug logs.
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[allow(deprecated)]
            Self::Authenticate {
                username,
                require_tls,
                ..
            } => f
                .debug_struct("Authenticate")
                .field("username", username)
                .field("password", &"[REDACTED]")
                .field("require_tls", require_tls)
                .finish(),
            Self::SaslAuthenticate { mechanism, .. } => f
                .debug_struct("SaslAuthenticate")
                .field("mechanism", mechanism)
                .field("auth_bytes", &"[REDACTED]")
                .finish(),
            Self::ScramClientFirst { .. } => f
                .debug_struct("ScramClientFirst")
                .field("message", &"[REDACTED]")
                .finish(),
            Self::ScramClientFinal { .. } => f
                .debug_struct("ScramClientFinal")
                .field("message", &"[REDACTED]")
                .finish(),
            Self::Publish {
                topic,
                partition,
                key,
                value,
                leader_epoch,
            } => f
                .debug_struct("Publish")
                .field("topic", topic)
                .field("partition", partition)
                .field("key", key)
                .field("value_len", &value.len())
                .field("leader_epoch", leader_epoch)
                .finish(),
            Self::Consume {
                topic,
                partition,
                offset,
                max_messages,
                isolation_level,
                max_wait_ms,
            } => f
                .debug_struct("Consume")
                .field("topic", topic)
                .field("partition", partition)
                .field("offset", offset)
                .field("max_messages", max_messages)
                .field("isolation_level", isolation_level)
                .field("max_wait_ms", max_wait_ms)
                .finish(),
            Self::CreateTopic { name, partitions } => f
                .debug_struct("CreateTopic")
                .field("name", name)
                .field("partitions", partitions)
                .finish(),
            Self::ListTopics => write!(f, "ListTopics"),
            Self::DeleteTopic { name } => {
                f.debug_struct("DeleteTopic").field("name", name).finish()
            }
            Self::CommitOffset {
                consumer_group,
                topic,
                partition,
                offset,
            } => f
                .debug_struct("CommitOffset")
                .field("consumer_group", consumer_group)
                .field("topic", topic)
                .field("partition", partition)
                .field("offset", offset)
                .finish(),
            Self::GetOffset {
                consumer_group,
                topic,
                partition,
            } => f
                .debug_struct("GetOffset")
                .field("consumer_group", consumer_group)
                .field("topic", topic)
                .field("partition", partition)
                .finish(),
            Self::GetMetadata { topic } => {
                f.debug_struct("GetMetadata").field("topic", topic).finish()
            }
            Self::GetClusterMetadata { topics } => f
                .debug_struct("GetClusterMetadata")
                .field("topics", topics)
                .finish(),
            Self::Ping => write!(f, "Ping"),
            Self::GetOffsetBounds { topic, partition } => f
                .debug_struct("GetOffsetBounds")
                .field("topic", topic)
                .field("partition", partition)
                .finish(),
            Self::ListGroups => write!(f, "ListGroups"),
            Self::DescribeGroup { consumer_group } => f
                .debug_struct("DescribeGroup")
                .field("consumer_group", consumer_group)
                .finish(),
            Self::DeleteGroup { consumer_group } => f
                .debug_struct("DeleteGroup")
                .field("consumer_group", consumer_group)
                .finish(),
            Self::GetOffsetForTimestamp {
                topic,
                partition,
                timestamp_ms,
            } => f
                .debug_struct("GetOffsetForTimestamp")
                .field("topic", topic)
                .field("partition", partition)
                .field("timestamp_ms", timestamp_ms)
                .finish(),
            Self::InitProducerId { producer_id } => f
                .debug_struct("InitProducerId")
                .field("producer_id", producer_id)
                .finish(),
            Self::IdempotentPublish {
                topic,
                partition,
                producer_id,
                producer_epoch,
                sequence,
                ..
            } => f
                .debug_struct("IdempotentPublish")
                .field("topic", topic)
                .field("partition", partition)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .field("sequence", sequence)
                .finish(),
            Self::BeginTransaction {
                txn_id,
                producer_id,
                producer_epoch,
                timeout_ms,
            } => f
                .debug_struct("BeginTransaction")
                .field("txn_id", txn_id)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .field("timeout_ms", timeout_ms)
                .finish(),
            Self::AddPartitionsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                partitions,
            } => f
                .debug_struct("AddPartitionsToTxn")
                .field("txn_id", txn_id)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .field("partitions", partitions)
                .finish(),
            Self::TransactionalPublish {
                txn_id,
                topic,
                partition,
                producer_id,
                producer_epoch,
                sequence,
                ..
            } => f
                .debug_struct("TransactionalPublish")
                .field("txn_id", txn_id)
                .field("topic", topic)
                .field("partition", partition)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .field("sequence", sequence)
                .finish(),
            Self::AddOffsetsToTxn {
                txn_id,
                producer_id,
                producer_epoch,
                group_id,
                offsets,
            } => f
                .debug_struct("AddOffsetsToTxn")
                .field("txn_id", txn_id)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .field("group_id", group_id)
                .field("offsets", offsets)
                .finish(),
            Self::CommitTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => f
                .debug_struct("CommitTransaction")
                .field("txn_id", txn_id)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .finish(),
            Self::AbortTransaction {
                txn_id,
                producer_id,
                producer_epoch,
            } => f
                .debug_struct("AbortTransaction")
                .field("txn_id", txn_id)
                .field("producer_id", producer_id)
                .field("producer_epoch", producer_epoch)
                .finish(),
            Self::DescribeQuotas { entities } => f
                .debug_struct("DescribeQuotas")
                .field("entities", entities)
                .finish(),
            Self::AlterQuotas { alterations } => f
                .debug_struct("AlterQuotas")
                .field("alterations", alterations)
                .finish(),
            Self::AlterTopicConfig { topic, configs } => f
                .debug_struct("AlterTopicConfig")
                .field("topic", topic)
                .field("configs", configs)
                .finish(),
            Self::CreatePartitions {
                topic,
                new_partition_count,
                assignments,
            } => f
                .debug_struct("CreatePartitions")
                .field("topic", topic)
                .field("new_partition_count", new_partition_count)
                .field("assignments", assignments)
                .finish(),
            Self::DeleteRecords {
                topic,
                partition_offsets,
            } => f
                .debug_struct("DeleteRecords")
                .field("topic", topic)
                .field("partition_offsets", partition_offsets)
                .finish(),
            Self::DescribeTopicConfigs { topics } => f
                .debug_struct("DescribeTopicConfigs")
                .field("topics", topics)
                .finish(),
            Self::Handshake {
                protocol_version,
                client_id,
            } => f
                .debug_struct("Handshake")
                .field("protocol_version", protocol_version)
                .field("client_id", client_id)
                .finish(),
        }
    }
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
        #[serde(with = "crate::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Server-final message (verification or error)
    ScramServerFinal {
        /// Server-final-message bytes (`v=<verifier>` or `e=<error>`)
        #[serde(with = "crate::serde_utils::bytes_serde")]
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
    // Idempotent Producer
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
    // Native Transactions
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

    /// Protocol version handshake response
    HandshakeResult {
        /// Server's protocol version
        server_version: u32,
        /// Whether the client version is compatible
        compatible: bool,
        /// Human-readable message (e.g. reason for incompatibility)
        message: String,
    },
}

impl Request {
    /// Serialize request to bytes (postcard format, no format prefix)
    ///
    /// For internal Rust-to-Rust communication where format is known.
    /// Use `to_wire()` for wire transmission with format detection support.
    #[inline]
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Deserialize request from bytes (postcard format)
    ///
    /// For internal Rust-to-Rust communication where format is known.
    /// Use `from_wire()` for wire transmission with format detection support.
    #[inline]
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(data)?)
    }

    /// Serialize request with wire format prefix
    ///
    /// Wire format: `[format_byte][correlation_id (4 bytes BE)][payload]`
    /// - format_byte: 0x00 = postcard, 0x01 = protobuf
    /// - correlation_id: 4-byte big-endian u32 for request-response matching
    /// - payload: serialized message
    ///
    /// Note: Length prefix is NOT included (handled by transport layer)
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::MessageTooLarge`](crate::ProtocolError::MessageTooLarge) if the serialized message
    /// exceeds [`MAX_MESSAGE_SIZE`](crate::MAX_MESSAGE_SIZE).
    #[inline]
    pub fn to_wire(&self, format: crate::WireFormat, correlation_id: u32) -> Result<Vec<u8>> {
        let result = match format {
            crate::WireFormat::Postcard => {
                // Single allocation — serialize directly into the output
                // Vec via `postcard::to_extend` instead of double-allocating
                // (to_allocvec → intermediate Vec → copy into result Vec).
                let mut result = Vec::with_capacity(crate::WIRE_HEADER_SIZE + 128);
                result.push(format.as_byte());
                result.extend_from_slice(&correlation_id.to_be_bytes());
                postcard::to_extend(self, result)?
            }
            crate::WireFormat::Protobuf => {
                // Protobuf requires the `protobuf` feature
                #[cfg(feature = "protobuf")]
                {
                    let payload = self.to_proto_bytes()?;
                    let mut result = Vec::with_capacity(crate::WIRE_HEADER_SIZE + payload.len());
                    result.push(format.as_byte());
                    result.extend_from_slice(&correlation_id.to_be_bytes());
                    result.extend_from_slice(&payload);
                    result
                }
                #[cfg(not(feature = "protobuf"))]
                {
                    return Err(crate::ProtocolError::Serialization(
                        "Protobuf support requires the 'protobuf' feature".into(),
                    ));
                }
            }
        };

        // Enforce MAX_MESSAGE_SIZE before the bytes leave this crate.
        if result.len() > crate::MAX_MESSAGE_SIZE {
            return Err(crate::ProtocolError::MessageTooLarge(
                result.len(),
                crate::MAX_MESSAGE_SIZE,
            ));
        }

        Ok(result)
    }

    /// Deserialize request with format auto-detection
    ///
    /// Detects format from first byte, reads correlation_id, and deserializes accordingly.
    /// Returns the deserialized request, the detected format, and the correlation_id.
    #[inline]
    pub fn from_wire(data: &[u8]) -> Result<(Self, crate::WireFormat, u32)> {
        if data.len() < crate::WIRE_HEADER_SIZE {
            return Err(crate::ProtocolError::Serialization(
                "Wire data too short (need format byte + correlation_id)".into(),
            ));
        }

        let format_byte = data[0];
        let format = crate::WireFormat::from_byte(format_byte).ok_or_else(|| {
            crate::ProtocolError::Serialization(format!(
                "Unknown wire format: 0x{:02x}",
                format_byte
            ))
        })?;

        let correlation_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let payload = &data[crate::WIRE_HEADER_SIZE..];

        match format {
            crate::WireFormat::Postcard => {
                let request = postcard::from_bytes(payload)?;
                Ok((request, format, correlation_id))
            }
            crate::WireFormat::Protobuf => {
                #[cfg(feature = "protobuf")]
                {
                    let request = Self::from_proto_bytes(payload)?;
                    Ok((request, format, correlation_id))
                }
                #[cfg(not(feature = "protobuf"))]
                {
                    Err(crate::ProtocolError::Serialization(
                        "Protobuf support requires the 'protobuf' feature".into(),
                    ))
                }
            }
        }
    }
}

impl Response {
    /// Serialize response to bytes (postcard format, no format prefix)
    #[inline]
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(postcard::to_allocvec(self)?)
    }

    /// Deserialize response from bytes (postcard format)
    #[inline]
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(data)?)
    }

    /// Serialize response with wire format prefix
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::MessageTooLarge`](crate::ProtocolError::MessageTooLarge) if the serialized message
    /// exceeds [`MAX_MESSAGE_SIZE`](crate::MAX_MESSAGE_SIZE).
    #[inline]
    pub fn to_wire(&self, format: crate::WireFormat, correlation_id: u32) -> Result<Vec<u8>> {
        let result = match format {
            crate::WireFormat::Postcard => {
                // Estimate payload size to avoid reallocations.
                // For Messages responses, use message count × estimated per-message
                // size (offset 8 + partition 4 + value ~256 + timestamp 8 + overhead ~24 ≈ 300 bytes).
                // For other variants the default 128-byte hint is usually sufficient.
                let size_hint = match self {
                    Response::Messages { messages } => messages.len().saturating_mul(300).max(128),
                    _ => 128,
                };
                let mut result = Vec::with_capacity(crate::WIRE_HEADER_SIZE + size_hint);
                result.push(format.as_byte());
                result.extend_from_slice(&correlation_id.to_be_bytes());
                postcard::to_extend(self, result)?
            }
            crate::WireFormat::Protobuf => {
                #[cfg(feature = "protobuf")]
                {
                    let payload = self.to_proto_bytes()?;
                    let mut result = Vec::with_capacity(crate::WIRE_HEADER_SIZE + payload.len());
                    result.push(format.as_byte());
                    result.extend_from_slice(&correlation_id.to_be_bytes());
                    result.extend_from_slice(&payload);
                    result
                }
                #[cfg(not(feature = "protobuf"))]
                {
                    return Err(crate::ProtocolError::Serialization(
                        "Protobuf support requires the 'protobuf' feature".into(),
                    ));
                }
            }
        };

        // Enforce MAX_MESSAGE_SIZE before the bytes leave this crate.
        if result.len() > crate::MAX_MESSAGE_SIZE {
            return Err(crate::ProtocolError::MessageTooLarge(
                result.len(),
                crate::MAX_MESSAGE_SIZE,
            ));
        }

        Ok(result)
    }

    /// Deserialize response with format auto-detection
    #[inline]
    pub fn from_wire(data: &[u8]) -> Result<(Self, crate::WireFormat, u32)> {
        if data.len() < crate::WIRE_HEADER_SIZE {
            return Err(crate::ProtocolError::Serialization(
                "Wire data too short (need format byte + correlation_id)".into(),
            ));
        }

        let format_byte = data[0];
        let format = crate::WireFormat::from_byte(format_byte).ok_or_else(|| {
            crate::ProtocolError::Serialization(format!(
                "Unknown wire format: 0x{:02x}",
                format_byte
            ))
        })?;

        let correlation_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let payload = &data[crate::WIRE_HEADER_SIZE..];

        match format {
            crate::WireFormat::Postcard => {
                let response = postcard::from_bytes(payload)?;
                Ok((response, format, correlation_id))
            }
            crate::WireFormat::Protobuf => {
                #[cfg(feature = "protobuf")]
                {
                    let response = Self::from_proto_bytes(payload)?;
                    Ok((response, format, correlation_id))
                }
                #[cfg(not(feature = "protobuf"))]
                {
                    Err(crate::ProtocolError::Serialization(
                        "Protobuf support requires the 'protobuf' feature".into(),
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
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
                require_tls: true,
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

    #[test]
    fn test_request_wire_roundtrip() {
        let request = Request::Ping;

        // Serialize with format prefix and correlation_id
        let wire_bytes = request.to_wire(crate::WireFormat::Postcard, 42).unwrap();

        // First byte should be format identifier
        assert_eq!(wire_bytes[0], 0x00); // Postcard format

        // Deserialize with auto-detection
        let (decoded, format, correlation_id) = Request::from_wire(&wire_bytes).unwrap();
        assert_eq!(format, crate::WireFormat::Postcard);
        assert_eq!(correlation_id, 42);
        assert!(matches!(decoded, Request::Ping));
    }

    #[test]
    fn test_response_wire_roundtrip() {
        let response = Response::Pong;

        // Serialize with format prefix and correlation_id
        let wire_bytes = response.to_wire(crate::WireFormat::Postcard, 99).unwrap();

        // First byte should be format identifier
        assert_eq!(wire_bytes[0], 0x00); // Postcard format

        // Deserialize with auto-detection
        let (decoded, format, correlation_id) = Response::from_wire(&wire_bytes).unwrap();
        assert_eq!(format, crate::WireFormat::Postcard);
        assert_eq!(correlation_id, 99);
        assert!(matches!(decoded, Response::Pong));
    }

    #[test]
    fn test_wire_format_empty_data() {
        let result = Request::from_wire(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wire_format_complex_request() {
        use bytes::Bytes;

        let request = Request::Publish {
            topic: "test-topic".to_string(),
            partition: Some(3),
            key: Some(Bytes::from("my-key")),
            value: Bytes::from("hello world"),
            leader_epoch: None,
        };

        let wire_bytes = request.to_wire(crate::WireFormat::Postcard, 1).unwrap();
        assert_eq!(wire_bytes[0], 0x00);

        let (decoded, format, correlation_id) = Request::from_wire(&wire_bytes).unwrap();
        assert_eq!(format, crate::WireFormat::Postcard);
        assert_eq!(correlation_id, 1);

        // Verify the decoded request matches
        if let Request::Publish {
            topic, partition, ..
        } = decoded
        {
            assert_eq!(topic, "test-topic");
            assert_eq!(partition, Some(3));
        } else {
            panic!("Expected Publish request");
        }
    }

    #[test]
    fn test_wire_format_complex_response() {
        let response = Response::Published {
            offset: 12345,
            partition: 7,
        };

        let wire_bytes = response.to_wire(crate::WireFormat::Postcard, 2).unwrap();
        assert_eq!(wire_bytes[0], 0x00);

        let (decoded, format, correlation_id) = Response::from_wire(&wire_bytes).unwrap();
        assert_eq!(format, crate::WireFormat::Postcard);
        assert_eq!(correlation_id, 2);

        if let Response::Published { offset, partition } = decoded {
            assert_eq!(offset, 12345);
            assert_eq!(partition, 7);
        } else {
            panic!("Expected Published response");
        }
    }

    /// Snapshot test: postcard serializes enum variants by ordinal position.
    /// If someone reorders variants in Request or Response, this test fails
    /// because the byte prefix (discriminant) will change, breaking wire compat.
    ///
    /// Exhaustive — every Request variant is pinned.
    #[test]
    #[allow(deprecated)]
    fn test_postcard_wire_stability_request_discriminants() {
        use bytes::Bytes;

        // Serialize a representative of each variant and check the leading
        // discriminant byte(s) haven't shifted. Postcard uses varint encoding
        // for enum discriminants.
        let test_cases: Vec<(Request, u8)> = vec![
            // variant 0: Authenticate
            (
                Request::Authenticate {
                    username: "u".into(),
                    password: "p".into(),
                    require_tls: false,
                },
                0,
            ),
            // variant 1: SaslAuthenticate
            (
                Request::SaslAuthenticate {
                    mechanism: Bytes::from("PLAIN"),
                    auth_bytes: Bytes::from("data"),
                },
                1,
            ),
            // variant 2: ScramClientFirst
            (
                Request::ScramClientFirst {
                    message: Bytes::from("n,,n=user,r=nonce"),
                },
                2,
            ),
            // variant 3: ScramClientFinal
            (
                Request::ScramClientFinal {
                    message: Bytes::from("c=bind,r=nonce,p=proof"),
                },
                3,
            ),
            // variant 4: Publish
            (
                Request::Publish {
                    topic: "t".into(),
                    partition: None,
                    key: None,
                    value: Bytes::from("v"),
                    leader_epoch: None,
                },
                4,
            ),
            // variant 5: Consume
            (
                Request::Consume {
                    topic: "t".into(),
                    partition: 0,
                    offset: 0,
                    max_messages: 1,
                    isolation_level: None,
                    max_wait_ms: None,
                },
                5,
            ),
            // variant 6: CreateTopic
            (
                Request::CreateTopic {
                    name: "t".into(),
                    partitions: None,
                },
                6,
            ),
            // variant 7: ListTopics
            (Request::ListTopics, 7),
            // variant 8: DeleteTopic
            (Request::DeleteTopic { name: "t".into() }, 8),
            // variant 9: CommitOffset
            (
                Request::CommitOffset {
                    consumer_group: "g".into(),
                    topic: "t".into(),
                    partition: 0,
                    offset: 0,
                },
                9,
            ),
            // variant 10: GetOffset
            (
                Request::GetOffset {
                    consumer_group: "g".into(),
                    topic: "t".into(),
                    partition: 0,
                },
                10,
            ),
            // variant 11: GetMetadata
            (Request::GetMetadata { topic: "t".into() }, 11),
            // variant 12: GetClusterMetadata
            (Request::GetClusterMetadata { topics: vec![] }, 12),
            // variant 13: Ping
            (Request::Ping, 13),
            // variant 14: GetOffsetBounds
            (
                Request::GetOffsetBounds {
                    topic: "t".into(),
                    partition: 0,
                },
                14,
            ),
            // variant 15: ListGroups
            (Request::ListGroups, 15),
            // variant 16: DescribeGroup
            (
                Request::DescribeGroup {
                    consumer_group: "g".into(),
                },
                16,
            ),
            // variant 17: DeleteGroup
            (
                Request::DeleteGroup {
                    consumer_group: "g".into(),
                },
                17,
            ),
            // variant 18: GetOffsetForTimestamp
            (
                Request::GetOffsetForTimestamp {
                    topic: "t".into(),
                    partition: 0,
                    timestamp_ms: 0,
                },
                18,
            ),
            // variant 19: InitProducerId
            (Request::InitProducerId { producer_id: None }, 19),
            // variant 20: IdempotentPublish
            (
                Request::IdempotentPublish {
                    topic: "t".into(),
                    partition: None,
                    key: None,
                    value: Bytes::from("v"),
                    producer_id: 1,
                    producer_epoch: 0,
                    sequence: 0,
                    leader_epoch: None,
                },
                20,
            ),
            // variant 21: BeginTransaction
            (
                Request::BeginTransaction {
                    txn_id: "tx".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                    timeout_ms: None,
                },
                21,
            ),
            // variant 22: AddPartitionsToTxn
            (
                Request::AddPartitionsToTxn {
                    txn_id: "tx".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                    partitions: vec![],
                },
                22,
            ),
            // variant 23: TransactionalPublish
            (
                Request::TransactionalPublish {
                    txn_id: "tx".into(),
                    topic: "t".into(),
                    partition: None,
                    key: None,
                    value: Bytes::from("v"),
                    producer_id: 1,
                    producer_epoch: 0,
                    sequence: 0,
                    leader_epoch: None,
                },
                23,
            ),
            // variant 24: AddOffsetsToTxn
            (
                Request::AddOffsetsToTxn {
                    txn_id: "tx".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                    group_id: "g".into(),
                    offsets: vec![],
                },
                24,
            ),
            // variant 25: CommitTransaction
            (
                Request::CommitTransaction {
                    txn_id: "tx".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                },
                25,
            ),
            // variant 26: AbortTransaction
            (
                Request::AbortTransaction {
                    txn_id: "tx".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                },
                26,
            ),
            // variant 27: DescribeQuotas
            (Request::DescribeQuotas { entities: vec![] }, 27),
            // variant 28: AlterQuotas
            (
                Request::AlterQuotas {
                    alterations: vec![],
                },
                28,
            ),
            // variant 29: AlterTopicConfig
            (
                Request::AlterTopicConfig {
                    topic: "t".into(),
                    configs: vec![],
                },
                29,
            ),
            // variant 30: CreatePartitions
            (
                Request::CreatePartitions {
                    topic: "t".into(),
                    new_partition_count: 2,
                    assignments: vec![],
                },
                30,
            ),
            // variant 31: DeleteRecords
            (
                Request::DeleteRecords {
                    topic: "t".into(),
                    partition_offsets: vec![],
                },
                31,
            ),
            // variant 32: DescribeTopicConfigs
            (Request::DescribeTopicConfigs { topics: vec![] }, 32),
            // variant 33: Handshake
            (
                Request::Handshake {
                    protocol_version: crate::PROTOCOL_VERSION,
                    client_id: "test".into(),
                },
                33,
            ),
        ];

        for (request, expected_discriminant) in test_cases {
            let bytes = request.to_bytes().unwrap();
            assert_eq!(
                bytes[0], expected_discriminant,
                "Wire discriminant changed for {:?} — enum variant order may have shifted!",
                request
            );
        }
    }

    /// Exhaustive — every Response variant is pinned.
    #[test]
    fn test_postcard_wire_stability_response_discriminants() {
        use bytes::Bytes;

        let test_cases: Vec<(Response, u8)> = vec![
            // variant 0: Authenticated
            (
                Response::Authenticated {
                    session_id: String::new(),
                    expires_in: 0,
                },
                0,
            ),
            // variant 1: ScramServerFirst
            (
                Response::ScramServerFirst {
                    message: Bytes::from("r=nonce,s=salt,i=4096"),
                },
                1,
            ),
            // variant 2: ScramServerFinal
            (
                Response::ScramServerFinal {
                    message: Bytes::from("v=verifier"),
                    session_id: None,
                    expires_in: None,
                },
                2,
            ),
            // variant 3: Published
            (
                Response::Published {
                    offset: 0,
                    partition: 0,
                },
                3,
            ),
            // variant 4: Messages
            (Response::Messages { messages: vec![] }, 4),
            // variant 5: TopicCreated
            (
                Response::TopicCreated {
                    name: "t".into(),
                    partitions: 1,
                },
                5,
            ),
            // variant 6: Topics
            (Response::Topics { topics: vec![] }, 6),
            // variant 7: TopicDeleted
            (Response::TopicDeleted, 7),
            // variant 8: OffsetCommitted
            (Response::OffsetCommitted, 8),
            // variant 9: Offset
            (Response::Offset { offset: None }, 9),
            // variant 10: Metadata
            (
                Response::Metadata {
                    name: "t".into(),
                    partitions: 1,
                },
                10,
            ),
            // variant 11: ClusterMetadata
            (
                Response::ClusterMetadata {
                    controller_id: None,
                    brokers: vec![],
                    topics: vec![],
                },
                11,
            ),
            // variant 12: Pong
            (Response::Pong, 12),
            // variant 13: OffsetBounds
            (
                Response::OffsetBounds {
                    earliest: 0,
                    latest: 0,
                },
                13,
            ),
            // variant 14: Groups
            (Response::Groups { groups: vec![] }, 14),
            // variant 15: GroupDescription
            (
                Response::GroupDescription {
                    consumer_group: "g".into(),
                    offsets: HashMap::new(),
                },
                15,
            ),
            // variant 16: GroupDeleted
            (Response::GroupDeleted, 16),
            // variant 17: OffsetForTimestamp
            (Response::OffsetForTimestamp { offset: None }, 17),
            // variant 18: Error
            (
                Response::Error {
                    message: "e".into(),
                },
                18,
            ),
            // variant 19: Ok
            (Response::Ok, 19),
            // variant 20: ProducerIdInitialized
            (
                Response::ProducerIdInitialized {
                    producer_id: 1,
                    producer_epoch: 0,
                },
                20,
            ),
            // variant 21: IdempotentPublished
            (
                Response::IdempotentPublished {
                    offset: 0,
                    partition: 0,
                    duplicate: false,
                },
                21,
            ),
            // variant 22: TransactionStarted
            (
                Response::TransactionStarted {
                    txn_id: "tx".into(),
                },
                22,
            ),
            // variant 23: PartitionsAddedToTxn
            (
                Response::PartitionsAddedToTxn {
                    txn_id: "tx".into(),
                    partition_count: 0,
                },
                23,
            ),
            // variant 24: TransactionalPublished
            (
                Response::TransactionalPublished {
                    offset: 0,
                    partition: 0,
                    sequence: 0,
                },
                24,
            ),
            // variant 25: OffsetsAddedToTxn
            (
                Response::OffsetsAddedToTxn {
                    txn_id: "tx".into(),
                },
                25,
            ),
            // variant 26: TransactionCommitted
            (
                Response::TransactionCommitted {
                    txn_id: "tx".into(),
                },
                26,
            ),
            // variant 27: TransactionAborted
            (
                Response::TransactionAborted {
                    txn_id: "tx".into(),
                },
                27,
            ),
            // variant 28: QuotasDescribed
            (Response::QuotasDescribed { entries: vec![] }, 28),
            // variant 29: QuotasAltered
            (Response::QuotasAltered { altered_count: 0 }, 29),
            // variant 30: Throttled
            (
                Response::Throttled {
                    throttle_time_ms: 0,
                    quota_type: "produce_bytes_rate".into(),
                    entity: "user".into(),
                },
                30,
            ),
            // variant 31: TopicConfigAltered
            (
                Response::TopicConfigAltered {
                    topic: "t".into(),
                    changed_count: 0,
                },
                31,
            ),
            // variant 32: PartitionsCreated
            (
                Response::PartitionsCreated {
                    topic: "t".into(),
                    new_partition_count: 2,
                },
                32,
            ),
            // variant 33: RecordsDeleted
            (
                Response::RecordsDeleted {
                    topic: "t".into(),
                    results: vec![],
                },
                33,
            ),
            // variant 34: TopicConfigsDescribed
            (Response::TopicConfigsDescribed { configs: vec![] }, 34),
            // variant 35: HandshakeResult
            (
                Response::HandshakeResult {
                    server_version: crate::PROTOCOL_VERSION,
                    compatible: true,
                    message: String::new(),
                },
                35,
            ),
        ];

        for (response, expected_discriminant) in test_cases {
            let bytes = response.to_bytes().unwrap();
            assert_eq!(
                bytes[0], expected_discriminant,
                "Wire discriminant changed for {:?} — enum variant order may have shifted!",
                response
            );
        }
    }

    /// Verify that to_wire rejects oversized messages.
    #[test]
    fn test_to_wire_rejects_oversized_request() {
        use bytes::Bytes;
        // Create a request with a payload larger than MAX_MESSAGE_SIZE
        let huge_value = vec![0u8; crate::MAX_MESSAGE_SIZE + 1];
        let request = Request::Publish {
            topic: "t".into(),
            partition: None,
            key: None,
            value: Bytes::from(huge_value),
            leader_epoch: None,
        };
        let result = request.to_wire(crate::WireFormat::Postcard, 0);
        assert!(
            matches!(result, Err(crate::ProtocolError::MessageTooLarge(_, _))),
            "Expected MessageTooLarge error for oversized request"
        );
    }

    #[test]
    fn test_to_wire_rejects_oversized_response() {
        let huge_messages = vec![MessageData {
            offset: 0,
            partition: 0,
            timestamp: 0,
            key: None,
            value: bytes::Bytes::from(vec![0u8; crate::MAX_MESSAGE_SIZE + 1]),
            headers: vec![],
        }];
        let response = Response::Messages {
            messages: huge_messages,
        };
        let result = response.to_wire(crate::WireFormat::Postcard, 0);
        assert!(
            matches!(result, Err(crate::ProtocolError::MessageTooLarge(_, _))),
            "Expected MessageTooLarge error for oversized response"
        );
    }
}
