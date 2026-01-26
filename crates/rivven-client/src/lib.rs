pub mod client;
pub mod error;
pub mod resilient;

pub use client::{AuthSession, Client};
pub use error::{Error, Result};
pub use resilient::{ResilientClient, ResilientClientConfig, ResilientClientConfigBuilder};

// Re-export TLS configuration when available
#[cfg(feature = "tls")]
pub use rivven_core::tls::TlsConfig;

// Protocol types - MUST match rivven-server/src/protocol.rs exactly!
// The variant order matters for bincode serialization.
// TODO: Move to shared crate (rivven-protocol)
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Protocol messages for Rivven
///
/// WARNING: Variant order must match server's protocol.rs exactly!
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
        /// Client-first-message bytes (n,,n=<user>,r=<nonce>)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Client-final message  
    ScramClientFinal {
        /// Client-final-message bytes (c=<binding>,r=<nonce>,p=<proof>)
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
}

/// Protocol responses
///
/// WARNING: Variant order must match server's protocol.rs exactly!
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
        /// Server-first-message bytes (r=<nonce>,s=<salt>,i=<iterations>)
        #[serde(with = "rivven_core::serde_utils::bytes_serde")]
        message: Bytes,
    },

    /// SCRAM-SHA-256: Server-final message (verification or error)
    ScramServerFinal {
        /// Server-final-message bytes (v=<verifier> or e=<error>)
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
        offsets: std::collections::HashMap<String, std::collections::HashMap<u32, u64>>,
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
}

/// Broker/node information for metadata discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    /// Node ID
    pub node_id: String,
    /// Host for client connections
    pub host: String,
    /// Port for client connections
    pub port: u16,
    /// Optional rack ID
    pub rack: Option<String>,
}

/// Topic metadata for cluster discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Is the topic internal (e.g., __consumer_offsets)
    pub is_internal: bool,
    /// Partition metadata
    pub partitions: Vec<PartitionMetadata>,
}

/// Partition metadata for cluster discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    /// Partition ID
    pub partition: u32,
    /// Leader node ID (None if no leader)
    pub leader: Option<String>,
    /// Replica node IDs
    pub replicas: Vec<String>,
    /// ISR (in-sync replica) node IDs
    pub isr: Vec<String>,
    /// Is offline (no leader available)
    pub offline: bool,
}

/// Serialized message data for transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    pub offset: u64,
    #[serde(with = "rivven_core::serde_utils::option_bytes_serde")]
    pub key: Option<Bytes>,
    #[serde(with = "rivven_core::serde_utils::bytes_serde")]
    pub value: Bytes,
    pub timestamp: i64,
}

impl Request {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}

impl Response {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(data)?)
    }
}
