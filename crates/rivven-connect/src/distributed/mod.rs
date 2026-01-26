//! Distributed Connector Coordination
//!
//! Provides distributed execution of connectors across multiple Connect nodes
//! with support for:
//!
//! - **Singleton Connectors**: Only one instance runs at a time (e.g., CDC)
//! - **Scalable Connectors**: Multiple instances with partitioned work
//! - **Leader Election**: Automatic failover for singleton connectors
//! - **Task Distribution**: Balanced assignment across nodes
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   Connect Coordinator                           │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
//! │  │ Membership   │  │ Leader       │  │ Task         │          │
//! │  │ (SWIM)       │  │ Election     │  │ Assignment   │          │
//! │  └──────────────┘  └──────────────┘  └──────────────┘          │
//! │         │                 │                 │                   │
//! │         ▼                 ▼                 ▼                   │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │               Connector Tasks                            │   │
//! │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │   │
//! │  │  │ CDC Source  │  │ S3 Sink     │  │ S3 Sink     │      │   │
//! │  │  │ (Singleton) │  │ (Task 1)    │  │ (Task 2)    │      │   │
//! │  │  └─────────────┘  └─────────────┘  └─────────────┘      │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_connect::distributed::{ConnectCoordinator, CoordinatorConfig};
//!
//! // Create coordinator
//! let config = CoordinatorConfig {
//!     node_id: "node-1".to_string(),
//!     cluster_topic: "_connect_status".to_string(),
//!     ..Default::default()
//! };
//!
//! let coordinator = ConnectCoordinator::new(config, broker).await?;
//!
//! // Register a singleton connector (CDC)
//! coordinator.register_connector(ConnectorConfig {
//!     name: "postgres-cdc",
//!     mode: ConnectorMode::Singleton,
//!     ..Default::default()
//! }).await?;
//!
//! // Start coordination
//! coordinator.start().await?;
//! ```

mod assignment;
mod coordinator;
mod election;
mod membership;
mod protocol;
mod task;
mod types;

pub use assignment::{AssignmentDecision, AssignmentStrategy, NodeLoad, TaskAssigner};
pub use coordinator::{
    ConnectCoordinator, ConnectorInfo, ConnectorState, CoordinatorConfig, CoordinatorResult,
    CoordinatorStatus,
};
pub use election::{ElectionEvent, ElectionState, LeaderElection, LeaderInfo};
pub use membership::{
    ConnectMembership, MemberInfo, MemberState, MembershipManager, MembershipSnapshot,
    NodeResources,
};
pub use protocol::{
    CoordinationMessage, HeartbeatMessage, HeartbeatResponse, JoinRequest, JoinResponse,
    LeaderElectionMessage, NodeCapabilities, NodeLoadInfo, RebalanceReason, RebalanceTrigger,
    TaskAssignmentMessage, TaskStatusReport, TaskStatusSummary, WorkerAction, PROTOCOL_VERSION,
};
pub use task::{ConnectorTask, SingletonState, TaskConfig, TaskState, TaskStatus, WorkRange};
pub use types::{
    ConnectorConfig, ConnectorId, ConnectorMode, DistributedError, DistributedResult, Epoch,
    FailoverConfig, Generation, NodeId, TaskId,
};
