//! Connector task and state management

use crate::distributed::types::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// State of a connector task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    /// Task is pending assignment
    Pending,
    /// Task is assigned but not yet running
    Assigned,
    /// Task is running
    Running,
    /// Task is paused (for singleton standby)
    Paused,
    /// Task failed and needs restart
    Failed,
    /// Task is being stopped
    Stopping,
    /// Task is stopped
    Stopped,
}

impl TaskState {
    pub fn is_active(&self) -> bool {
        matches!(self, TaskState::Running | TaskState::Paused)
    }

    pub fn can_start(&self) -> bool {
        matches!(
            self,
            TaskState::Pending | TaskState::Assigned | TaskState::Paused | TaskState::Failed
        )
    }
}

/// Status of a running task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatus {
    /// Current state
    pub state: TaskState,
    /// Events processed
    pub events_processed: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Last error message (if failed)
    pub error: Option<String>,
    /// When the task started
    pub started_at: Option<i64>,
    /// Last heartbeat timestamp
    pub last_heartbeat: i64,
    /// Lag in seconds (for sources)
    pub lag_seconds: Option<f64>,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self {
            state: TaskState::Pending,
            events_processed: 0,
            bytes_processed: 0,
            error: None,
            started_at: None,
            last_heartbeat: chrono::Utc::now().timestamp(),
            lag_seconds: None,
        }
    }
}

/// A connector task that can be assigned to a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorTask {
    /// Unique task identifier
    pub id: TaskId,
    /// Connector this task belongs to
    pub connector: ConnectorId,
    /// Task number (0 for singleton, 0..N for scalable)
    pub task_number: u32,
    /// Assigned node (None if unassigned)
    pub assigned_node: Option<NodeId>,
    /// Current status
    pub status: TaskStatus,
    /// Generation when assigned
    pub generation: Generation,
    /// Epoch when assigned
    pub epoch: Epoch,
    /// Configuration for this task
    pub config: TaskConfig,
}

impl ConnectorTask {
    pub fn new(connector: &ConnectorId, task_number: u32) -> Self {
        Self {
            id: TaskId::new(&connector.0, task_number),
            connector: connector.clone(),
            task_number,
            assigned_node: None,
            status: TaskStatus::default(),
            generation: Generation::default(),
            epoch: Epoch::default(),
            config: TaskConfig::default(),
        }
    }

    /// Create a singleton task
    pub fn singleton(connector: &ConnectorId) -> Self {
        let mut task = Self::new(connector, 0);
        task.config.is_singleton = true;
        task
    }

    /// Get the assigned node (if any)
    pub fn assigned_node(&self) -> Option<&NodeId> {
        self.assigned_node.as_ref()
    }

    /// Check if task is assigned to a specific node
    pub fn is_assigned_to(&self, node: &NodeId) -> bool {
        self.assigned_node.as_ref() == Some(node)
    }

    /// Assign task to a node
    pub fn assign(&mut self, node: NodeId, generation: Generation, epoch: Epoch) {
        self.assigned_node = Some(node);
        self.generation = generation;
        self.epoch = epoch;
        self.status.state = TaskState::Assigned;
    }

    /// Mark task as running
    pub fn start(&mut self) {
        self.status.state = TaskState::Running;
        self.status.started_at = Some(chrono::Utc::now().timestamp());
        self.status.last_heartbeat = chrono::Utc::now().timestamp();
    }

    /// Pause task (for singleton standby)
    pub fn pause(&mut self) {
        self.status.state = TaskState::Paused;
    }

    /// Mark task as failed
    pub fn fail(&mut self, error: impl Into<String>) {
        self.status.state = TaskState::Failed;
        self.status.error = Some(error.into());
    }

    /// Unassign task
    pub fn unassign(&mut self) {
        self.assigned_node = None;
        self.status.state = TaskState::Pending;
    }
}

/// Task-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskConfig {
    /// Whether this is a singleton task
    pub is_singleton: bool,
    /// Partitions assigned to this task (for partitioned connectors)
    pub partitions: Vec<u32>,
    /// Work range (for scalable connectors)
    pub work_range: Option<WorkRange>,
}

/// Work range for scalable connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRange {
    pub start: u64,
    pub end: u64,
}

/// Singleton task state with leader tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingletonState {
    /// Connector ID
    pub connector: ConnectorId,
    /// Current leader node
    pub leader: Option<NodeId>,
    /// Leader generation
    pub generation: Generation,
    /// Standby nodes (ordered by priority)
    pub standbys: Vec<NodeId>,
    /// Last leader heartbeat
    pub last_heartbeat: i64,
    /// Whether failover is in progress
    pub failover_in_progress: bool,
}

impl SingletonState {
    pub fn new(connector: ConnectorId) -> Self {
        Self {
            connector,
            leader: None,
            generation: Generation::default(),
            standbys: Vec::new(),
            last_heartbeat: chrono::Utc::now().timestamp(),
            failover_in_progress: false,
        }
    }

    /// Check if a node is the leader
    pub fn is_leader(&self, node: &NodeId) -> bool {
        self.leader.as_ref() == Some(node)
    }

    /// Check if leader is healthy based on heartbeat timeout
    pub fn is_leader_healthy(&self, timeout: Duration) -> bool {
        if self.leader.is_none() {
            return false;
        }
        let now = chrono::Utc::now().timestamp();
        let elapsed = Duration::from_secs((now - self.last_heartbeat).max(0) as u64);
        elapsed < timeout
    }

    /// Set new leader
    pub fn set_leader(&mut self, node: NodeId) {
        self.leader = Some(node.clone());
        self.generation.increment();
        self.last_heartbeat = chrono::Utc::now().timestamp();
        self.failover_in_progress = false;

        // Remove from standbys if present
        self.standbys.retain(|n| n != &node);
    }

    /// Add standby node
    pub fn add_standby(&mut self, node: NodeId) {
        if self.leader.as_ref() != Some(&node) && !self.standbys.contains(&node) {
            self.standbys.push(node);
        }
    }

    /// Remove node (from leader or standbys)
    pub fn remove_node(&mut self, node: &NodeId) {
        if self.leader.as_ref() == Some(node) {
            self.leader = None;
        }
        self.standbys.retain(|n| n != node);
    }

    /// Update leader heartbeat
    pub fn heartbeat(&mut self) {
        self.last_heartbeat = chrono::Utc::now().timestamp();
    }

    /// Start failover process
    pub fn start_failover(&mut self) -> Option<NodeId> {
        if self.failover_in_progress {
            return None;
        }

        self.failover_in_progress = true;
        self.leader = None;

        // Promote first standby
        if let Some(new_leader) = self.standbys.first().cloned() {
            self.set_leader(new_leader.clone());
            Some(new_leader)
        } else {
            None
        }
    }
}
