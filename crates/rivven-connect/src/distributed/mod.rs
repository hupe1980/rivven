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
//! │  │ Manager      │  │ Election     │  │ Assignment   │          │
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
//! # Connector Modes
//!
//! | Mode        | Description                          | Use Case                     |
//! |-------------|--------------------------------------|------------------------------|
//! | `Singleton` | Single instance with failover        | CDC, ordered processing      |
//! | `Scalable`  | Multiple instances, auto-partitioned | S3 sink, Elasticsearch       |
//! | `Partitioned` | One task per configured partition  | Explicit parallelism         |
//!
//! # Example
//!
//! ```rust,no_run
//! use rivven_connect::distributed::{
//!     ConnectCoordinator, CoordinatorConfig, ConnectorId, ConnectorMode, NodeId,
//! };
//!
//! // Create coordinator
//! let config = CoordinatorConfig {
//!     node_id: NodeId::new("coordinator-1"),
//!     ..Default::default()
//! };
//! let mut coordinator = ConnectCoordinator::new(config);
//!
//! // Add worker nodes
//! coordinator.add_member(NodeId::new("worker-1")).unwrap();
//! coordinator.add_member(NodeId::new("worker-2")).unwrap();
//!
//! // Register a singleton connector (CDC - one instance with failover)
//! coordinator.register_connector(
//!     ConnectorId::new("postgres-cdc"),
//!     "postgres-cdc".to_string(),
//!     ConnectorMode::Singleton,
//!     1,
//!     serde_json::json!({"host": "localhost", "database": "mydb"}),
//! ).unwrap();
//!
//! // Register a scalable connector (multiple parallel tasks)
//! coordinator.register_connector(
//!     ConnectorId::new("s3-sink"),
//!     "s3-sink".to_string(),
//!     ConnectorMode::Scalable,
//!     4, // 4 parallel tasks
//!     serde_json::json!({"bucket": "events", "region": "us-east-1"}),
//! ).unwrap();
//!
//! // Start connectors and trigger task assignment
//! coordinator.start_connector(&ConnectorId::new("postgres-cdc")).unwrap();
//! coordinator.start_connector(&ConnectorId::new("s3-sink")).unwrap();
//! coordinator.rebalance().unwrap();
//!
//! // Check task assignments
//! let assignments = coordinator.get_task_assignments();
//! for (task_id, node_id) in assignments {
//!     println!("Task {} assigned to {}", task_id, node_id);
//! }
//! ```

mod assignment;
mod coordinator;
mod election;
mod membership;
mod protocol;
mod task;
mod types;

// ============================================================================
// Core Types (most commonly used)
// ============================================================================

pub use types::{
    ConnectorConfig, ConnectorId, ConnectorMode, DistributedError, DistributedResult, Epoch,
    FailoverConfig, Generation, NodeId, ResourceRequirements, TaskId,
};

// ============================================================================
// Coordinator (central orchestration)
// ============================================================================

pub use coordinator::{
    ConnectCoordinator, ConnectorInfo, ConnectorState, CoordinatorConfig, CoordinatorResult,
    CoordinatorStatus,
};

// ============================================================================
// Task Assignment
// ============================================================================

pub use assignment::{AssignmentDecision, AssignmentStrategy, NodeLoad, TaskAssigner};
pub use task::{ConnectorTask, SingletonState, TaskConfig, TaskState, TaskStatus, WorkRange};

// ============================================================================
// Membership & Election
// ============================================================================

pub use election::{ElectionEvent, ElectionManager, ElectionState, LeaderElection, LeaderInfo};
pub use membership::{
    ConnectMembership, MemberInfo, MemberState, MembershipEvent, MembershipManager,
    MembershipSnapshot, NodeResources,
};

// ============================================================================
// Protocol Messages (for distributed communication)
// ============================================================================

pub use protocol::{
    CoordinationMessage, HeartbeatMessage, HeartbeatResponse, JoinRequest, JoinResponse,
    LeaderElectionMessage, NodeCapabilities, NodeLoadInfo, RebalanceReason, RebalanceTrigger,
    TaskAssignmentMessage, TaskStatusReport, TaskStatusSummary, WorkerAction, PROTOCOL_VERSION,
};
