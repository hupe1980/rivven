//! Coordination protocol messages for distributed Connect

use crate::distributed::task::{TaskState, TaskStatus};
use crate::distributed::types::*;
use serde::{Deserialize, Serialize};

/// Protocol version for coordination messages
pub const PROTOCOL_VERSION: u32 = 1;

/// Coordination message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinationMessage {
    /// Join request from a worker node
    JoinRequest(JoinRequest),
    /// Join response from coordinator
    JoinResponse(JoinResponse),
    /// Leave notification
    LeaveNotification(LeaveNotification),
    /// Heartbeat from worker
    Heartbeat(HeartbeatMessage),
    /// Heartbeat response from coordinator
    HeartbeatResponse(HeartbeatResponse),
    /// Task assignment from coordinator
    TaskAssignment(TaskAssignmentMessage),
    /// Task status report from worker
    TaskStatusReport(TaskStatusReport),
    /// Leader election for singleton connectors
    LeaderElection(LeaderElectionMessage),
    /// Configuration update
    ConfigUpdate(ConfigUpdateMessage),
    /// Rebalance trigger
    RebalanceTrigger(RebalanceTrigger),
}

/// Worker join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Protocol version
    pub protocol_version: u32,
    /// Node ID
    pub node_id: NodeId,
    /// Node address
    pub address: String,
    /// Node capabilities
    pub capabilities: NodeCapabilities,
    /// Timestamp
    pub timestamp: i64,
}

/// Worker capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Maximum tasks this node can handle
    pub max_tasks: u32,
    /// Supported connector types
    pub connector_types: Vec<String>,
    /// Available memory in bytes
    pub memory_bytes: u64,
    /// CPU cores available
    pub cpu_cores: u32,
    /// Rack/zone identifier
    pub rack: Option<String>,
    /// Custom labels
    pub labels: std::collections::HashMap<String, String>,
}

impl Default for NodeCapabilities {
    fn default() -> Self {
        Self {
            max_tasks: 100,
            connector_types: Vec::new(),
            memory_bytes: 0,
            cpu_cores: 0,
            rack: None,
            labels: std::collections::HashMap::new(),
        }
    }
}

/// Join response from coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    /// Whether join was successful
    pub success: bool,
    /// Error message if join failed
    pub error: Option<String>,
    /// Current cluster epoch
    pub epoch: Epoch,
    /// Coordinator node ID
    pub coordinator: NodeId,
    /// List of current cluster members
    pub members: Vec<NodeId>,
    /// Assigned tasks (if any)
    pub initial_tasks: Vec<TaskAssignmentMessage>,
}

/// Leave notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveNotification {
    /// Node leaving the cluster
    pub node_id: NodeId,
    /// Reason for leaving
    pub reason: LeaveReason,
    /// Timestamp
    pub timestamp: i64,
}

/// Reason for leaving the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LeaveReason {
    /// Graceful shutdown
    Shutdown,
    /// Configuration change
    Reconfiguration,
    /// Error/failure
    Error(String),
    /// Kicked by coordinator
    Kicked(String),
}

/// Heartbeat from worker to coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Node ID
    pub node_id: NodeId,
    /// Current epoch
    pub epoch: Epoch,
    /// Node load information
    pub load: NodeLoadInfo,
    /// Status of assigned tasks
    pub task_statuses: Vec<TaskStatusSummary>,
    /// Timestamp
    pub timestamp: i64,
}

/// Node load information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLoadInfo {
    /// CPU usage (0.0 - 1.0)
    pub cpu_usage: f32,
    /// Memory usage (0.0 - 1.0)
    pub memory_usage: f32,
    /// Current task count
    pub task_count: u32,
    /// Events per second
    pub events_per_second: f64,
    /// Bytes per second
    pub bytes_per_second: f64,
}

/// Summary of a task's status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusSummary {
    /// Task ID
    pub task_id: TaskId,
    /// Connector ID
    pub connector_id: ConnectorId,
    /// Current state
    pub state: TaskState,
    /// Events processed since last heartbeat
    pub events_delta: u64,
    /// Bytes processed since last heartbeat
    pub bytes_delta: u64,
    /// Lag in seconds (for sources)
    pub lag_seconds: Option<f64>,
    /// Error if failed
    pub error: Option<String>,
}

/// Heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Current epoch (in case it changed)
    pub epoch: Epoch,
    /// Actions for the worker to take
    pub actions: Vec<WorkerAction>,
    /// Whether rebalance is pending
    pub rebalance_pending: bool,
}

/// Actions for a worker to perform
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum WorkerAction {
    /// Start a task
    StartTask { task_id: TaskId },
    /// Stop a task
    StopTask { task_id: TaskId },
    /// Pause a task (singleton standby)
    PauseTask { task_id: TaskId },
    /// Resume a task
    ResumeTask { task_id: TaskId },
    /// Update task configuration
    UpdateTask {
        task_id: TaskId,
        config: serde_json::Value,
    },
}

/// Task assignment message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignmentMessage {
    /// Task ID
    pub task_id: TaskId,
    /// Connector ID
    pub connector_id: ConnectorId,
    /// Task number
    pub task_number: u32,
    /// Assigned node
    pub node_id: NodeId,
    /// Generation
    pub generation: Generation,
    /// Epoch
    pub epoch: Epoch,
    /// Whether this is a singleton task
    pub is_singleton: bool,
    /// Whether this node is the leader (for singletons)
    pub is_leader: bool,
    /// Task configuration
    pub config: serde_json::Value,
}

/// Task status report from worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusReport {
    /// Node ID
    pub node_id: NodeId,
    /// Task ID
    pub task_id: TaskId,
    /// Full status
    pub status: TaskStatus,
    /// Timestamp
    pub timestamp: i64,
}

/// Leader election message for singleton connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderElectionMessage {
    /// Connector ID
    pub connector_id: ConnectorId,
    /// Election type
    pub election_type: ElectionType,
    /// Candidate node (for vote requests)
    pub candidate: Option<NodeId>,
    /// New leader (for leader announcements)
    pub new_leader: Option<NodeId>,
    /// Generation
    pub generation: Generation,
    /// Timestamp
    pub timestamp: i64,
}

/// Type of leader election event
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ElectionType {
    /// Request to become leader
    VoteRequest,
    /// Vote for a candidate
    Vote,
    /// Announce new leader
    LeaderAnnouncement,
    /// Leader resignation
    Resignation,
    /// Failover triggered
    Failover,
}

/// Configuration update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateMessage {
    /// Update type
    pub update_type: ConfigUpdateType,
    /// Connector ID (if connector-specific)
    pub connector_id: Option<ConnectorId>,
    /// New configuration
    pub config: serde_json::Value,
    /// Generation
    pub generation: Generation,
}

/// Type of configuration update
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigUpdateType {
    /// Connector configuration changed
    ConnectorConfig,
    /// New connector added
    ConnectorAdded,
    /// Connector removed
    ConnectorRemoved,
    /// Global configuration changed
    GlobalConfig,
}

/// Rebalance trigger message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceTrigger {
    /// Reason for rebalance
    pub reason: RebalanceReason,
    /// New epoch after rebalance
    pub new_epoch: Epoch,
    /// Timestamp
    pub timestamp: i64,
}

/// Reason for triggering a rebalance
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceReason {
    /// Node joined the cluster
    NodeJoined(NodeId),
    /// Node left the cluster
    NodeLeft(NodeId),
    /// Connector added
    ConnectorAdded(ConnectorId),
    /// Connector removed
    ConnectorRemoved(ConnectorId),
    /// Manual rebalance request
    Manual,
    /// Periodic rebalance
    Periodic,
    /// Load imbalance detected
    LoadImbalance,
}
