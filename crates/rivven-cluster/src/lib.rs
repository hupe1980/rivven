//! # Rivven Cluster
//!
//! Distributed clustering for Rivven with:
//! - **SWIM Protocol**: Scalable membership and failure detection
//! - **Raft Consensus**: Metadata coordination and leader election
//! - **ISR Replication**: Partition data replication with high watermarks
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Rivven Node                            │
//! ├──────────────┬──────────────┬───────────────────────────────┤
//! │    SWIM      │     Raft     │       ISR Replication         │
//! │  Membership  │   Metadata   │         Data Layer            │
//! ├──────────────┼──────────────┼───────────────────────────────┤
//! │ • Node disco │ • Topics     │ • Partition leaders           │
//! │ • Health chk │ • Partitions │ • Follower fetch              │
//! │ • Failure    │ • Assignments│ • High watermark              │
//! │   detection  │ • ISR state  │ • Catch-up sync               │
//! └──────────────┴──────────────┴───────────────────────────────┘
//! ```
//!
//! ## Deployment Modes
//!
//! - **Standalone**: Single node, zero configuration, all partitions local
//! - **Cluster**: Multi-node with SWIM membership, Raft metadata, ISR replication
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use rivven_cluster::{ClusterConfig, ClusterCoordinator, TopicConfig};
//!
//! // Standalone mode (zero config)
//! let config = ClusterConfig::standalone();
//! let coordinator = ClusterCoordinator::standalone(config).await?;
//!
//! // Create a topic
//! coordinator.create_topic(TopicConfig::new("events", 6, 1)).await?;
//!
//! // Cluster mode
//! let config = ClusterConfig::cluster()
//!     .node_id("node-1")
//!     .seeds(vec!["node-2:9093", "node-3:9093"])
//!     .build();
//! let mut coordinator = ClusterCoordinator::new(config).await?;
//! coordinator.start().await?;
//! ```

pub mod config;
pub mod consumer_coordinator;
pub mod coordinator;
pub mod error;
pub mod membership;
pub mod metadata;
pub mod node;
pub mod observability;
pub mod partition;
pub mod placement;
pub mod protocol;
#[cfg(feature = "quic")]
pub mod quic_transport;
pub mod raft;
pub mod replication;
pub mod transport;

// Re-export main types
pub use config::{
    ClusterConfig, ClusterMode, RaftConfig, ReplicationConfig, SwimConfig, TopicDefaults,
};
pub use consumer_coordinator::{ConsumerCoordinator, CoordinatorError, CoordinatorResult};
pub use coordinator::{ClusterCoordinator, ClusterHealth, CoordinatorState};
pub use error::{ClusterError, Result};
pub use membership::{Membership, MembershipEvent, SwimMessage};
pub use metadata::{ClusterMetadata, MetadataCommand, MetadataResponse, MetadataStore};
pub use node::{Node, NodeCapabilities, NodeId, NodeInfo, NodeState};
pub use observability::{init_metrics, ClusterMetrics, NetworkMetrics, RaftMetrics};
pub use partition::{PartitionId, PartitionState, TopicConfig, TopicState};
pub use placement::{PartitionPlacer, PlacementConfig, PlacementStrategy};
pub use protocol::{Acks, ClusterRequest, ClusterResponse};
#[cfg(feature = "quic")]
pub use quic_transport::{QuicConfig, QuicStats, QuicTransport, TlsConfig};
pub use raft::{
    hash_node_id, LogStore, NetworkFactory as RaftNetworkFactory, RaftController, RaftNode,
    RaftNodeConfig, RaftNodeId, StateMachine as MetadataStateMachine, TypeConfig as RaftTypeConfig,
};
pub use replication::{PartitionReplication, ReplicationManager};
pub use transport::{Transport, TransportConfig};

/// Re-export common types
pub mod prelude {
    pub use crate::config::*;
    pub use crate::coordinator::*;
    pub use crate::error::*;
    pub use crate::node::*;
    pub use crate::partition::*;
    pub use crate::protocol::Acks;
    pub use crate::raft::RaftController;
}
