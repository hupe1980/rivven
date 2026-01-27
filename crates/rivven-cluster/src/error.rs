//! Cluster error types

use std::net::SocketAddr;
use thiserror::Error;

/// Result type for cluster operations
pub type Result<T> = std::result::Result<T, ClusterError>;

/// Cluster errors
#[derive(Debug, Error)]
pub enum ClusterError {
    // ==================== Configuration Errors ====================
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("node ID conflict: {0} already exists")]
    NodeIdConflict(String),

    // ==================== Membership Errors ====================
    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("node unreachable: {addr}")]
    NodeUnreachable { addr: SocketAddr },

    #[error("node failed: {node_id}")]
    NodeFailed { node_id: String },

    #[error("no seed nodes available")]
    NoSeedNodes,

    #[error("cluster join failed: {0}")]
    JoinFailed(String),

    #[error("cluster not ready: need {required} nodes, have {current}")]
    ClusterNotReady { required: usize, current: usize },

    // ==================== Consensus Errors ====================
    #[error("not leader: current leader is {leader:?}")]
    NotLeader { leader: Option<String> },

    #[error("leader election in progress")]
    LeaderElectionInProgress,

    #[error("raft error: {0}")]
    Raft(String),

    #[error("raft storage error: {0}")]
    RaftStorage(String),

    #[error("proposal timeout")]
    ProposalTimeout,

    #[error("proposal rejected: {0}")]
    ProposalRejected(String),

    // ==================== Topic/Partition Errors ====================
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("topic already exists: {0}")]
    TopicAlreadyExists(String),

    #[error("partition not found: {topic}/{partition}")]
    PartitionNotFound { topic: String, partition: u32 },

    #[error("partition leader not found: {topic}/{partition}")]
    PartitionLeaderNotFound { topic: String, partition: u32 },

    #[error("invalid partition count: {0}")]
    InvalidPartitionCount(u32),

    #[error("invalid replication factor: {factor} (have {nodes} nodes)")]
    InvalidReplicationFactor { factor: u16, nodes: usize },

    // ==================== Replication Errors ====================
    #[error("not enough ISR: need {required}, have {current}")]
    NotEnoughIsr { required: u16, current: u16 },

    #[error("replication failed: {0}")]
    ReplicationFailed(String),

    #[error("replica lag exceeded: {node_id} lag {lag_messages} messages")]
    ReplicaLagExceeded { node_id: String, lag_messages: u64 },

    #[error("high watermark not advanced")]
    HighWatermarkStalled,

    // ==================== Protocol Errors ====================
    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: usize, max: usize },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    // ==================== Network Errors ====================
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("request timeout")]
    Timeout,

    #[error("network error: {0}")]
    Network(String),

    // ==================== Storage Errors ====================
    #[error("storage error: {0}")]
    Storage(String),

    #[error("corrupt data: {0}")]
    CorruptData(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    // ==================== Crypto/TLS Errors ====================
    #[error("crypto error: {0}")]
    CryptoError(String),

    // ==================== Internal Errors ====================
    #[error("internal error: {0}")]
    Internal(String),

    #[error("channel closed")]
    ChannelClosed,

    #[error("shutdown in progress")]
    ShuttingDown,
}

impl ClusterError {
    /// Check if this error is retriable
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            ClusterError::NodeUnreachable { .. }
                | ClusterError::Timeout
                | ClusterError::LeaderElectionInProgress
                | ClusterError::NotLeader { .. }
                | ClusterError::ClusterNotReady { .. }
                | ClusterError::NotEnoughIsr { .. }
                | ClusterError::Network(_)
        )
    }

    /// Check if this error indicates the node should redirect to leader
    pub fn should_redirect(&self) -> bool {
        matches!(self, ClusterError::NotLeader { leader: Some(_) })
    }

    /// Get the leader address if this is a NotLeader error
    pub fn leader(&self) -> Option<&str> {
        match self {
            ClusterError::NotLeader { leader } => leader.as_deref(),
            _ => None,
        }
    }

    /// Check if this is a fatal error requiring shutdown
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            ClusterError::CorruptData(_) | ClusterError::RaftStorage(_)
        )
    }
}

// Conversion from channel errors
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ClusterError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ClusterError::ChannelClosed
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for ClusterError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        ClusterError::ChannelClosed
    }
}

// Conversion from postcard for serialization
impl From<postcard::Error> for ClusterError {
    fn from(e: postcard::Error) -> Self {
        ClusterError::Serialization(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retriable_errors() {
        assert!(ClusterError::Timeout.is_retriable());
        assert!(ClusterError::LeaderElectionInProgress.is_retriable());
        assert!(!ClusterError::TopicNotFound("test".into()).is_retriable());
        assert!(!ClusterError::CorruptData("bad".into()).is_retriable());
    }

    #[test]
    fn test_redirect_to_leader() {
        let err = ClusterError::NotLeader {
            leader: Some("node-1".into()),
        };
        assert!(err.should_redirect());
        assert_eq!(err.leader(), Some("node-1"));

        let err = ClusterError::NotLeader { leader: None };
        assert!(!err.should_redirect());
    }

    #[test]
    fn test_fatal_errors() {
        assert!(ClusterError::CorruptData("bad crc".into()).is_fatal());
        assert!(ClusterError::RaftStorage("disk full".into()).is_fatal());
        assert!(!ClusterError::Timeout.is_fatal());
    }
}
