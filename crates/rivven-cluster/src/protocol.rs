//! Wire protocol for cluster communication

use crate::error::{ClusterError, Result};
use crate::metadata::MetadataCommand;
use crate::node::NodeId;
use crate::partition::PartitionId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Protocol version for compatibility checking
pub const PROTOCOL_VERSION: u16 = 1;

/// Minimum protocol version we can interoperate with
pub const MIN_PROTOCOL_VERSION: u16 = 1;

/// Maximum message size (16 MB)
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Error codes for protocol-level errors
pub mod error_codes {
    /// Unsupported protocol version
    pub const UNSUPPORTED_VERSION: u16 = 1;
    /// Message too large
    pub const MESSAGE_TOO_LARGE: u16 = 2;
    /// Unknown request type
    pub const UNKNOWN_REQUEST: u16 = 3;
}

/// Request header included in all requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestHeader {
    /// Protocol version
    pub version: u16,
    /// Correlation ID for matching responses
    pub correlation_id: u64,
    /// Source node ID
    pub source: NodeId,
    /// Request timeout
    pub timeout_ms: u32,
}

impl RequestHeader {
    pub fn new(correlation_id: u64, source: NodeId) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            correlation_id,
            source,
            timeout_ms: 30000,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout_ms = timeout.as_millis() as u32;
        self
    }

    /// Validate that the protocol version is supported.
    /// Returns an error ResponseHeader if the version is out of range.
    pub fn validate_version(&self) -> std::result::Result<(), ResponseHeader> {
        if self.version < MIN_PROTOCOL_VERSION || self.version > PROTOCOL_VERSION {
            Err(ResponseHeader::error(
                self.correlation_id,
                error_codes::UNSUPPORTED_VERSION,
                format!(
                    "unsupported protocol version {}: supported range [{}, {}]",
                    self.version, MIN_PROTOCOL_VERSION, PROTOCOL_VERSION
                ),
            ))
        } else {
            Ok(())
        }
    }
}

/// Response header included in all responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseHeader {
    /// Correlation ID matching the request
    pub correlation_id: u64,
    /// Error code (0 = success)
    pub error_code: u16,
    /// Error message (if any)
    pub error_message: Option<String>,
}

impl ResponseHeader {
    pub fn success(correlation_id: u64) -> Self {
        Self {
            correlation_id,
            error_code: 0,
            error_message: None,
        }
    }

    pub fn error(correlation_id: u64, code: u16, message: impl Into<String>) -> Self {
        Self {
            correlation_id,
            error_code: code,
            error_message: Some(message.into()),
        }
    }

    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }
}

/// Request types for cluster operations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)] // Acceptable for protocol enums - they're short-lived
pub enum ClusterRequest {
    // ==================== Metadata Requests ====================
    /// Fetch metadata for topics
    FetchMetadata {
        header: RequestHeader,
        topics: Option<Vec<String>>, // None = all topics
    },

    /// Propose a metadata change (forwarded to Raft leader)
    ProposeMetadata {
        header: RequestHeader,
        command: MetadataCommand,
    },

    // ==================== Replication Requests ====================
    /// Fetch records from a partition (follower -> leader)
    Fetch {
        header: RequestHeader,
        partition: PartitionId,
        offset: u64,
        max_bytes: u32,
    },

    /// Append records to a partition (client -> leader -> followers)
    Append {
        header: RequestHeader,
        partition: PartitionId,
        records: Vec<u8>, // Serialized records batch
        required_acks: Acks,
    },

    /// Report replica state to leader
    ReplicaState {
        header: RequestHeader,
        partition: PartitionId,
        log_end_offset: u64,
        high_watermark: u64,
    },

    // ==================== Leader Election ====================
    /// Request leader election for a partition
    ElectLeader {
        header: RequestHeader,
        partition: PartitionId,
        preferred_leader: Option<NodeId>,
    },

    // ==================== Heartbeat ====================
    /// Heartbeat from leader to followers
    Heartbeat {
        header: RequestHeader,
        partitions: Vec<HeartbeatPartition>,
    },
}

/// Partition info in heartbeat
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPartition {
    pub partition: PartitionId,
    pub leader_epoch: u64,
    pub high_watermark: u64,
}

/// Response types for cluster operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterResponse {
    // ==================== Metadata Responses ====================
    /// Metadata response
    Metadata {
        header: ResponseHeader,
        cluster_id: String,
        controller_id: Option<NodeId>,
        topics: Vec<TopicMetadata>,
        brokers: Vec<BrokerMetadata>,
    },

    /// Metadata proposal response
    MetadataProposal { header: ResponseHeader },

    // ==================== Replication Responses ====================
    /// Fetch response with records
    Fetch {
        header: ResponseHeader,
        partition: PartitionId,
        high_watermark: u64,
        log_start_offset: u64,
        records: Vec<u8>, // Serialized records
    },

    /// Append response
    Append {
        header: ResponseHeader,
        partition: PartitionId,
        base_offset: u64,
        log_append_time: i64,
    },

    /// Replica state acknowledgment
    ReplicaStateAck {
        header: ResponseHeader,
        partition: PartitionId,
        in_sync: bool,
    },

    // ==================== Leader Election ====================
    /// Leader election response
    ElectLeader {
        header: ResponseHeader,
        partition: PartitionId,
        leader: Option<NodeId>,
        epoch: u64,
    },

    // ==================== Heartbeat ====================
    /// Heartbeat response
    Heartbeat { header: ResponseHeader },

    // ==================== Error ====================
    /// Generic error response
    Error { header: ResponseHeader },
}

/// Topic metadata in response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
    pub is_internal: bool,
}

/// Partition metadata in response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub partition_index: u32,
    pub leader_id: Option<NodeId>,
    pub leader_epoch: u64,
    pub replica_nodes: Vec<NodeId>,
    pub isr_nodes: Vec<NodeId>,
    pub offline_replicas: Vec<NodeId>,
}

/// Broker metadata in response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetadata {
    pub node_id: NodeId,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

/// Required acknowledgments for writes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Acks {
    /// No acknowledgment (fire and forget)
    None,
    /// Leader acknowledgment only
    #[default]
    Leader,
    /// All ISR acknowledgment
    All,
}

impl Acks {
    pub fn from_i8(v: i8) -> Self {
        match v {
            0 => Acks::None,
            1 => Acks::Leader,
            -1 => Acks::All,
            _ => Acks::Leader,
        }
    }

    pub fn to_i8(self) -> i8 {
        match self {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }
}

/// Error codes for cluster protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    None = 0,
    Unknown = 1,
    CorruptMessage = 2,
    UnknownTopic = 3,
    InvalidPartition = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    NotEnoughReplicas = 8,
    NotEnoughReplicasAfterAppend = 9,
    InvalidRequiredAcks = 10,
    NotController = 11,
    InvalidRequest = 12,
    UnsupportedVersion = 13,
    TopicAlreadyExists = 14,
    InvalidReplicationFactor = 15,
    IneligibleReplica = 16,
    OffsetOutOfRange = 17,
    NotReplicaForPartition = 18,
    GroupAuthorizationFailed = 19,
    UnknownMemberId = 20,
}

impl ErrorCode {
    pub fn is_retriable(self) -> bool {
        matches!(
            self,
            ErrorCode::LeaderNotAvailable
                | ErrorCode::NotLeaderForPartition
                | ErrorCode::RequestTimedOut
                | ErrorCode::NotEnoughReplicas
                | ErrorCode::NotController
        )
    }
}

/// Encode a request to bytes
pub fn encode_request(request: &ClusterRequest) -> Result<Vec<u8>> {
    postcard::to_allocvec(request).map_err(|e| ClusterError::Serialization(e.to_string()))
}

/// Decode a request from bytes
pub fn decode_request(bytes: &[u8]) -> Result<ClusterRequest> {
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(ClusterError::MessageTooLarge {
            size: bytes.len(),
            max: MAX_MESSAGE_SIZE,
        });
    }
    postcard::from_bytes(bytes).map_err(|e| ClusterError::Deserialization(e.to_string()))
}

/// Encode a response to bytes
pub fn encode_response(response: &ClusterResponse) -> Result<Vec<u8>> {
    postcard::to_allocvec(response).map_err(|e| ClusterError::Serialization(e.to_string()))
}

/// Decode a response from bytes
pub fn decode_response(bytes: &[u8]) -> Result<ClusterResponse> {
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(ClusterError::MessageTooLarge {
            size: bytes.len(),
            max: MAX_MESSAGE_SIZE,
        });
    }
    postcard::from_bytes(bytes).map_err(|e| ClusterError::Deserialization(e.to_string()))
}

/// Frame a message with length prefix for TCP transmission
pub fn frame_message(data: &[u8]) -> Vec<u8> {
    let len = data.len() as u32;
    let mut framed = Vec::with_capacity(4 + data.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(data);
    framed
}

/// Extract message length from frame header
pub fn frame_length(header: &[u8; 4]) -> usize {
    u32::from_be_bytes(*header) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_request_roundtrip() {
        let header = RequestHeader::new(42, "node-1".to_string());
        let request = ClusterRequest::FetchMetadata {
            header,
            topics: Some(vec!["test-topic".to_string()]),
        };

        let bytes = encode_request(&request).unwrap();
        let decoded = decode_request(&bytes).unwrap();

        match decoded {
            ClusterRequest::FetchMetadata { header, topics } => {
                assert_eq!(header.correlation_id, 42);
                assert_eq!(topics, Some(vec!["test-topic".to_string()]));
            }
            _ => panic!("Wrong request type"),
        }
    }

    #[test]
    fn test_response_roundtrip() {
        let header = ResponseHeader::success(42);
        let response = ClusterResponse::Metadata {
            header,
            cluster_id: "test-cluster".to_string(),
            controller_id: Some("node-1".to_string()),
            topics: vec![],
            brokers: vec![],
        };

        let bytes = encode_response(&response).unwrap();
        let decoded = decode_response(&bytes).unwrap();

        match decoded {
            ClusterResponse::Metadata {
                header, cluster_id, ..
            } => {
                assert!(header.is_success());
                assert_eq!(cluster_id, "test-cluster");
            }
            _ => panic!("Wrong response type"),
        }
    }

    #[test]
    fn test_framing() {
        let data = b"hello world";
        let framed = frame_message(data);

        assert_eq!(framed.len(), 4 + data.len());

        let mut header = [0u8; 4];
        header.copy_from_slice(&framed[..4]);
        assert_eq!(frame_length(&header), data.len());
    }

    #[test]
    fn test_acks_conversion() {
        assert_eq!(Acks::from_i8(0), Acks::None);
        assert_eq!(Acks::from_i8(1), Acks::Leader);
        assert_eq!(Acks::from_i8(-1), Acks::All);

        assert_eq!(Acks::None.to_i8(), 0);
        assert_eq!(Acks::Leader.to_i8(), 1);
        assert_eq!(Acks::All.to_i8(), -1);
    }

    #[test]
    fn test_version_validation_ok() {
        let header = RequestHeader::new(1, "node-1".to_string());
        assert!(header.validate_version().is_ok());
    }

    #[test]
    fn test_version_validation_too_high() {
        let mut header = RequestHeader::new(1, "node-1".to_string());
        header.version = PROTOCOL_VERSION + 1;
        let err = header.validate_version().unwrap_err();
        assert_eq!(err.error_code, error_codes::UNSUPPORTED_VERSION);
    }

    #[test]
    fn test_version_validation_zero() {
        let mut header = RequestHeader::new(1, "node-1".to_string());
        header.version = 0;
        assert!(header.validate_version().is_err());
    }
}
