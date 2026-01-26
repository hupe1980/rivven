//! Node types and management

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Instant;

/// Unique node identifier (UUID or human-readable string)
pub type NodeId = String;

/// Node state in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum NodeState {
    /// Node is healthy and responding
    Alive,
    /// Node missed some pings, suspected but not confirmed dead
    Suspect,
    /// Node confirmed dead, will be removed
    Dead,
    /// Node is leaving gracefully
    Leaving,
    /// Node state is unknown (just joined)
    #[default]
    Unknown,
}

impl NodeState {
    /// Check if node is considered healthy for routing
    pub fn is_healthy(&self) -> bool {
        matches!(self, NodeState::Alive)
    }

    /// Check if node might be reachable
    pub fn is_reachable(&self) -> bool {
        matches!(self, NodeState::Alive | NodeState::Suspect)
    }
}

/// Node capabilities and roles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NodeCapabilities {
    /// Can this node be a Raft voter?
    pub voter: bool,
    /// Can this node host partition leaders?
    pub leader_eligible: bool,
    /// Can this node host partition replicas?
    pub replica_eligible: bool,
}

impl NodeCapabilities {
    /// Full capabilities (voter + leader + replica)
    pub fn full() -> Self {
        Self {
            voter: true,
            leader_eligible: true,
            replica_eligible: true,
        }
    }

    /// Observer capabilities (replica only, no voting/leading)
    pub fn observer() -> Self {
        Self {
            voter: false,
            leader_eligible: false,
            replica_eligible: true,
        }
    }
}

/// Information about a cluster node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique node identifier
    pub id: NodeId,

    /// Human-readable name
    pub name: Option<String>,

    /// Rack identifier for rack-aware placement
    pub rack: Option<String>,

    /// Client-facing address
    pub client_addr: SocketAddr,

    /// Cluster communication address
    pub cluster_addr: SocketAddr,

    /// Node capabilities
    pub capabilities: NodeCapabilities,

    /// Node version (for compatibility checking)
    pub version: String,

    /// Custom metadata/tags
    pub tags: std::collections::HashMap<String, String>,
}

impl NodeInfo {
    /// Create new node info
    pub fn new(id: impl Into<String>, client_addr: SocketAddr, cluster_addr: SocketAddr) -> Self {
        Self {
            id: id.into(),
            name: None,
            rack: None,
            client_addr,
            cluster_addr,
            capabilities: NodeCapabilities::full(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            tags: std::collections::HashMap::new(),
        }
    }

    /// Set human-readable name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set rack identifier
    pub fn with_rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }

    /// Set capabilities
    pub fn with_capabilities(mut self, capabilities: NodeCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Full node state including runtime information
#[derive(Debug, Clone)]
pub struct Node {
    /// Static node information
    pub info: NodeInfo,

    /// Current node state
    pub state: NodeState,

    /// Incarnation number (for SWIM protocol)
    pub incarnation: u64,

    /// Last time we heard from this node
    pub last_seen: Instant,

    /// Number of partitions led by this node
    pub partition_leader_count: u32,

    /// Number of partition replicas on this node
    pub partition_replica_count: u32,

    /// Whether this node is the Raft leader
    pub is_raft_leader: bool,
}

impl Node {
    /// Create a new node from info
    pub fn new(info: NodeInfo) -> Self {
        Self {
            info,
            state: NodeState::Unknown,
            incarnation: 0,
            last_seen: Instant::now(),
            partition_leader_count: 0,
            partition_replica_count: 0,
            is_raft_leader: false,
        }
    }

    /// Update last seen time
    pub fn touch(&mut self) {
        self.last_seen = Instant::now();
    }

    /// Mark as alive
    pub fn mark_alive(&mut self, incarnation: u64) {
        self.state = NodeState::Alive;
        self.incarnation = incarnation;
        self.touch();
    }

    /// Mark as suspect
    pub fn mark_suspect(&mut self) {
        if self.state == NodeState::Alive {
            self.state = NodeState::Suspect;
        }
    }

    /// Mark as dead
    pub fn mark_dead(&mut self) {
        self.state = NodeState::Dead;
    }

    /// Mark as leaving
    pub fn mark_leaving(&mut self) {
        self.state = NodeState::Leaving;
    }

    /// Check if node is healthy
    pub fn is_healthy(&self) -> bool {
        self.state.is_healthy()
    }

    /// Get node ID
    pub fn id(&self) -> &str {
        &self.info.id
    }

    /// Get cluster address
    pub fn cluster_addr(&self) -> SocketAddr {
        self.info.cluster_addr
    }

    /// Get client address  
    pub fn client_addr(&self) -> SocketAddr {
        self.info.client_addr
    }

    /// Calculate load score (lower is better for placement)
    pub fn load_score(&self) -> u32 {
        // Weight leaders more than replicas
        self.partition_leader_count * 3 + self.partition_replica_count
    }
}

/// Serializable node state for gossip
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGossipState {
    pub id: NodeId,
    pub state: NodeState,
    pub incarnation: u64,
    pub cluster_addr: SocketAddr,
    pub client_addr: SocketAddr,
    pub rack: Option<String>,
    pub capabilities: NodeCapabilities,
}

impl From<&Node> for NodeGossipState {
    fn from(node: &Node) -> Self {
        Self {
            id: node.info.id.clone(),
            state: node.state,
            incarnation: node.incarnation,
            cluster_addr: node.info.cluster_addr,
            client_addr: node.info.client_addr,
            rack: node.info.rack.clone(),
            capabilities: node.info.capabilities,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_state_transitions() {
        let info = NodeInfo::new(
            "node-1",
            "127.0.0.1:9092".parse().unwrap(),
            "127.0.0.1:9093".parse().unwrap(),
        );
        let mut node = Node::new(info);

        assert_eq!(node.state, NodeState::Unknown);
        assert!(!node.is_healthy());

        node.mark_alive(1);
        assert_eq!(node.state, NodeState::Alive);
        assert!(node.is_healthy());

        node.mark_suspect();
        assert_eq!(node.state, NodeState::Suspect);
        assert!(!node.is_healthy());
        assert!(node.state.is_reachable());

        node.mark_dead();
        assert_eq!(node.state, NodeState::Dead);
        assert!(!node.state.is_reachable());
    }

    #[test]
    fn test_load_score() {
        let info = NodeInfo::new(
            "node-1",
            "127.0.0.1:9092".parse().unwrap(),
            "127.0.0.1:9093".parse().unwrap(),
        );
        let mut node = Node::new(info);

        node.partition_leader_count = 2;
        node.partition_replica_count = 4;

        // Leaders weighted 3x
        assert_eq!(node.load_score(), 2 * 3 + 4);
    }

    #[test]
    fn test_node_capabilities() {
        let full = NodeCapabilities::full();
        assert!(full.voter && full.leader_eligible && full.replica_eligible);

        let observer = NodeCapabilities::observer();
        assert!(!observer.voter && !observer.leader_eligible && observer.replica_eligible);
    }
}
