//! Connect cluster coordinator with singleton connector enforcement
//!
//! The coordinator is responsible for:
//! - Managing cluster membership
//! - Assigning tasks to worker nodes
//! - Enforcing singleton mode for CDC connectors
//! - Handling failover when singleton leaders fail
//! - Triggering rebalances when the cluster changes

use crate::distributed::assignment::{AssignmentDecision, AssignmentStrategy, NodeLoad, TaskAssigner};
use crate::distributed::membership::MembershipManager;
use crate::distributed::protocol::*;
use crate::distributed::task::{ConnectorTask, SingletonState};
use crate::distributed::types::*;
use crate::error::ConnectError;

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Result type for coordinator operations
pub type CoordinatorResult<T> = std::result::Result<T, ConnectError>;

/// Configuration for the coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Node ID for this coordinator
    pub node_id: NodeId,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Heartbeat timeout (consider node dead if no heartbeat)
    pub heartbeat_timeout: Duration,
    /// Session timeout
    pub session_timeout: Duration,
    /// Rebalance delay after membership change
    pub rebalance_delay: Duration,
    /// Assignment strategy
    pub assignment_strategy: AssignmentStrategy,
    /// Singleton failover configuration
    pub failover_config: FailoverConfig,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::generate(),
            heartbeat_interval: Duration::from_millis(1000),
            heartbeat_timeout: Duration::from_secs(10),
            session_timeout: Duration::from_secs(30),
            rebalance_delay: Duration::from_secs(3),
            assignment_strategy: AssignmentStrategy::LeastLoaded,
            failover_config: FailoverConfig::default(),
        }
    }
}

/// Registered connector information
#[derive(Debug, Clone)]
pub struct ConnectorInfo {
    /// Connector ID
    pub id: ConnectorId,
    /// Connector name
    pub name: String,
    /// Connector mode (Singleton, Scalable, or Partitioned)
    pub mode: ConnectorMode,
    /// Number of tasks (for scalable connectors)
    pub tasks_max: u32,
    /// Current task count
    pub tasks_current: u32,
    /// Connector configuration
    pub config: serde_json::Value,
    /// Generation
    pub generation: Generation,
    /// State
    pub state: ConnectorState,
}

/// State of a connector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorState {
    /// Connector is registered but not running
    Registered,
    /// Connector is starting
    Starting,
    /// Connector is running
    Running,
    /// Connector is paused
    Paused,
    /// Connector failed
    Failed,
    /// Connector is being deleted
    Deleting,
}

/// Connect cluster coordinator
pub struct ConnectCoordinator {
    /// Configuration
    config: CoordinatorConfig,
    /// Membership manager
    membership: MembershipManager,
    /// Task assigner
    assigner: TaskAssigner,
    /// Registered connectors
    connectors: HashMap<ConnectorId, ConnectorInfo>,
    /// All tasks
    tasks: HashMap<TaskId, ConnectorTask>,
    /// Current epoch
    epoch: Epoch,
    /// Pending rebalance
    rebalance_pending: bool,
    /// Last rebalance time
    last_rebalance: Option<Instant>,
}

impl ConnectCoordinator {
    /// Create a new coordinator
    pub fn new(config: CoordinatorConfig) -> Self {
        Self {
            membership: MembershipManager::new(config.node_id.clone()),
            assigner: TaskAssigner::new(config.assignment_strategy),
            connectors: HashMap::new(),
            tasks: HashMap::new(),
            epoch: Epoch::default(),
            rebalance_pending: false,
            last_rebalance: None,
            config,
        }
    }

    /// Get the current epoch
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Increment epoch
    fn increment_epoch(&mut self) {
        self.epoch.increment();
    }

    // =========================================================================
    // Connector Registration
    // =========================================================================

    /// Register a connector
    ///
    /// For singleton connectors (like CDC), this enforces that only one task
    /// is created and managed with leader election for failover.
    pub fn register_connector(
        &mut self,
        id: ConnectorId,
        name: String,
        mode: ConnectorMode,
        tasks_max: u32,
        config: serde_json::Value,
    ) -> CoordinatorResult<()> {
        if self.connectors.contains_key(&id) {
            return Err(ConnectError::Config(format!(
                "Connector '{}' already registered",
                id.0
            )));
        }

        // Validate mode and task count
        let actual_tasks = match mode {
            ConnectorMode::Singleton => {
                // Singleton connectors can only have one task
                if tasks_max != 1 {
                    warn!(
                        connector = %id.0,
                        requested_tasks = tasks_max,
                        "Singleton connector can only have one task, ignoring tasks_max"
                    );
                }
                1
            }
            ConnectorMode::Scalable => tasks_max.max(1),
            ConnectorMode::Partitioned => tasks_max.max(1),
        };

        let connector_info = ConnectorInfo {
            id: id.clone(),
            name,
            mode,
            tasks_max: actual_tasks,
            tasks_current: 0,
            config,
            generation: Generation::default(),
            state: ConnectorState::Registered,
        };

        info!(
            connector = %id.0,
            mode = ?mode,
            tasks = actual_tasks,
            "Registering connector"
        );

        self.connectors.insert(id.clone(), connector_info);

        // For singleton connectors, register with the assigner for leader election
        if mode == ConnectorMode::Singleton {
            self.assigner.register_singleton(id.clone());
        }

        // Create tasks
        self.create_tasks_for_connector(&id)?;

        // Trigger rebalance
        self.schedule_rebalance();

        Ok(())
    }

    /// Unregister a connector
    pub fn unregister_connector(&mut self, id: &ConnectorId) -> CoordinatorResult<()> {
        let _connector = self.connectors.remove(id).ok_or_else(|| {
            ConnectError::Config(format!("Connector '{}' not found", id.0))
        })?;

        info!(connector = %id.0, "Unregistering connector");

        // Remove all tasks for this connector
        let task_ids: Vec<_> = self
            .tasks
            .iter()
            .filter(|(_, t)| &t.connector == id)
            .map(|(id, _)| id.clone())
            .collect();

        for task_id in task_ids {
            self.tasks.remove(&task_id);
        }

        self.schedule_rebalance();
        Ok(())
    }

    /// Create tasks for a connector
    fn create_tasks_for_connector(&mut self, connector_id: &ConnectorId) -> CoordinatorResult<()> {
        let connector = self.connectors.get(connector_id).ok_or_else(|| {
            ConnectError::Config(format!("Connector '{}' not found", connector_id.0))
        })?;

        let mode = connector.mode;
        let tasks_max = connector.tasks_max;

        for i in 0..tasks_max {
            let task = match mode {
                ConnectorMode::Singleton => ConnectorTask::singleton(connector_id),
                _ => ConnectorTask::new(connector_id, i),
            };

            self.tasks.insert(task.id.clone(), task);
        }

        // Update current task count
        if let Some(c) = self.connectors.get_mut(connector_id) {
            c.tasks_current = tasks_max;
        }

        Ok(())
    }

    /// Update connector configuration
    pub fn update_connector_config(
        &mut self,
        id: &ConnectorId,
        config: serde_json::Value,
    ) -> CoordinatorResult<()> {
        let connector = self.connectors.get_mut(id).ok_or_else(|| {
            ConnectError::Config(format!("Connector '{}' not found", id.0))
        })?;

        connector.config = config;
        connector.generation.increment();

        info!(connector = %id.0, generation = %connector.generation.0, "Updated connector configuration");

        Ok(())
    }

    /// Get connector info
    pub fn get_connector(&self, id: &ConnectorId) -> Option<&ConnectorInfo> {
        self.connectors.get(id)
    }

    /// List all connectors
    pub fn list_connectors(&self) -> Vec<&ConnectorInfo> {
        self.connectors.values().collect()
    }

    /// Check if a connector is singleton
    pub fn is_singleton(&self, id: &ConnectorId) -> bool {
        self.connectors
            .get(id)
            .map(|c| c.mode == ConnectorMode::Singleton)
            .unwrap_or(false)
    }

    // =========================================================================
    // Node Management
    // =========================================================================

    /// Handle a node joining the cluster
    pub fn handle_node_join(&mut self, request: JoinRequest) -> JoinResponse {
        if request.protocol_version != PROTOCOL_VERSION {
            return JoinResponse {
                success: false,
                error: Some(format!(
                    "Protocol version mismatch: expected {}, got {}",
                    PROTOCOL_VERSION, request.protocol_version
                )),
                epoch: self.epoch,
                coordinator: self.config.node_id.clone(),
                members: Vec::new(),
                initial_tasks: Vec::new(),
            };
        }

        info!(node = %request.node_id.0, "Node joining cluster");

        // Add to membership
        self.membership.add_member(request.node_id.clone());

        // Update assigner with node capacity
        let load = NodeLoad {
            node: request.node_id.clone(),
            max_tasks: request.capabilities.max_tasks,
            current_tasks: 0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            is_healthy: true,
            rack: request.capabilities.rack,
        };
        self.assigner.update_node_load(load);

        // Schedule rebalance
        self.schedule_rebalance();

        JoinResponse {
            success: true,
            error: None,
            epoch: self.epoch,
            coordinator: self.config.node_id.clone(),
            members: self.membership.members().cloned().collect(),
            initial_tasks: Vec::new(), // Tasks will be assigned after rebalance
        }
    }

    /// Handle a node leaving the cluster
    pub fn handle_node_leave(&mut self, notification: LeaveNotification) {
        info!(
            node = %notification.node_id.0,
            reason = ?notification.reason,
            "Node leaving cluster"
        );

        self.remove_node(&notification.node_id);
    }

    /// Remove a node from the cluster
    fn remove_node(&mut self, node_id: &NodeId) {
        // Remove from membership
        self.membership.remove_member(node_id);

        // Remove from assigner
        self.assigner.remove_node(node_id);

        // Unassign tasks from this node
        for task in self.tasks.values_mut() {
            if task.is_assigned_to(node_id) {
                task.unassign();
            }
        }

        // Handle singleton failover
        for connector in self.connectors.values() {
            if connector.mode == ConnectorMode::Singleton {
                if let Some(state) = self.assigner.singleton_state(&connector.id) {
                    if state.is_leader(node_id) {
                        info!(
                            connector = %connector.id.0,
                            node = %node_id.0,
                            "Singleton leader left, triggering failover"
                        );
                        // Failover will happen during next assignment cycle
                    }
                }
            }
        }

        self.schedule_rebalance();
    }

    /// Handle heartbeat from a worker
    pub fn handle_heartbeat(&mut self, heartbeat: HeartbeatMessage) -> HeartbeatResponse {
        // Update membership
        self.membership.record_heartbeat(&heartbeat.node_id);

        // Update node load
        let load = NodeLoad {
            node: heartbeat.node_id.clone(),
            max_tasks: 100, // TODO: track from join request
            current_tasks: heartbeat.load.task_count,
            cpu_usage: heartbeat.load.cpu_usage,
            memory_usage: heartbeat.load.memory_usage,
            is_healthy: true,
            rack: None,
        };
        self.assigner.update_node_load(load);

        // Update task statuses
        for status in &heartbeat.task_statuses {
            if let Some(task) = self.tasks.get_mut(&status.task_id) {
                task.status.state = status.state;
                task.status.events_processed += status.events_delta;
                task.status.bytes_processed += status.bytes_delta;
                task.status.lag_seconds = status.lag_seconds;
                task.status.error = status.error.clone();
                task.status.last_heartbeat = heartbeat.timestamp;
            }

            // Update singleton heartbeat
            if let Some(connector) = self.connectors.get(&status.connector_id) {
                if connector.mode == ConnectorMode::Singleton {
                    if let Some(state) = self.assigner.singleton_state_mut(&status.connector_id) {
                        if state.is_leader(&heartbeat.node_id) {
                            state.heartbeat();
                        }
                    }
                }
            }
        }

        HeartbeatResponse {
            epoch: self.epoch,
            actions: Vec::new(), // TODO: populate with pending actions
            rebalance_pending: self.rebalance_pending,
        }
    }

    // =========================================================================
    // Task Assignment
    // =========================================================================

    /// Schedule a rebalance
    fn schedule_rebalance(&mut self) {
        self.rebalance_pending = true;
    }

    /// Execute rebalance if pending
    pub fn maybe_rebalance(&mut self) -> Vec<TaskAssignmentMessage> {
        if !self.rebalance_pending {
            return Vec::new();
        }

        // Check if we should delay
        if let Some(last) = self.last_rebalance {
            if last.elapsed() < self.config.rebalance_delay {
                return Vec::new();
            }
        }

        self.execute_rebalance()
    }

    /// Execute a full rebalance
    pub fn execute_rebalance(&mut self) -> Vec<TaskAssignmentMessage> {
        info!(epoch = %self.epoch.0, "Executing rebalance");

        self.rebalance_pending = false;
        self.last_rebalance = Some(Instant::now());
        self.increment_epoch();

        let mut assignments = Vec::new();

        // Collect tasks to assign
        let task_ids: Vec<_> = self.tasks.keys().cloned().collect();

        for task_id in task_ids {
            if let Some(task) = self.tasks.get_mut(&task_id) {
                let decision = self.assigner.assign_task(task);

                match decision {
                    AssignmentDecision::Assign(node) => {
                        let connector = self.connectors.get(&task.connector);
                        let is_singleton = connector
                            .map(|c| c.mode == ConnectorMode::Singleton)
                            .unwrap_or(false);
                        let is_leader = is_singleton; // For singleton, assigned node is always leader

                        task.assign(node.clone(), Generation::default(), self.epoch);

                        assignments.push(TaskAssignmentMessage {
                            task_id: task.id.clone(),
                            connector_id: task.connector.clone(),
                            task_number: task.task_number,
                            node_id: node,
                            generation: task.generation,
                            epoch: self.epoch,
                            is_singleton,
                            is_leader,
                            config: connector
                                .map(|c| c.config.clone())
                                .unwrap_or(serde_json::Value::Null),
                        });

                        debug!(
                            task = %task.id.0,
                            connector = %task.connector.0,
                            node = %task.assigned_node.as_ref().map(|n| n.0.as_str()).unwrap_or("none"),
                            "Task assigned"
                        );
                    }
                    AssignmentDecision::Reassign { from, to } => {
                        let connector = self.connectors.get(&task.connector);
                        let is_singleton = connector
                            .map(|c| c.mode == ConnectorMode::Singleton)
                            .unwrap_or(false);

                        task.assign(to.clone(), Generation::default(), self.epoch);

                        assignments.push(TaskAssignmentMessage {
                            task_id: task.id.clone(),
                            connector_id: task.connector.clone(),
                            task_number: task.task_number,
                            node_id: to.clone(),
                            generation: task.generation,
                            epoch: self.epoch,
                            is_singleton,
                            is_leader: is_singleton,
                            config: connector
                                .map(|c| c.config.clone())
                                .unwrap_or(serde_json::Value::Null),
                        });

                        info!(
                            task = %task.id.0,
                            from = %from.0,
                            to = %to.0,
                            "Task reassigned"
                        );
                    }
                    AssignmentDecision::Keep => {
                        // No change needed
                    }
                    AssignmentDecision::Unassign => {
                        task.unassign();
                        warn!(
                            task = %task.id.0,
                            "No suitable node for task"
                        );
                    }
                }
            }
        }

        info!(
            epoch = %self.epoch.0,
            assignments = assignments.len(),
            "Rebalance complete"
        );

        assignments
    }

    /// Get task assignments for a specific node
    pub fn get_node_tasks(&self, node_id: &NodeId) -> Vec<&ConnectorTask> {
        self.tasks
            .values()
            .filter(|t| t.is_assigned_to(node_id))
            .collect()
    }

    /// Get a specific task
    pub fn get_task(&self, task_id: &TaskId) -> Option<&ConnectorTask> {
        self.tasks.get(task_id)
    }

    /// List all tasks
    pub fn list_tasks(&self) -> Vec<&ConnectorTask> {
        self.tasks.values().collect()
    }

    // =========================================================================
    // Singleton Management
    // =========================================================================

    /// Check and handle singleton failover
    pub fn check_singleton_failover(&mut self) -> Vec<TaskAssignmentMessage> {
        let timeout = Duration::from_millis(self.config.failover_config.failure_timeout_ms);
        let mut failovers = Vec::new();

        let singleton_connectors: Vec<_> = self
            .connectors
            .iter()
            .filter(|(_, c)| c.mode == ConnectorMode::Singleton)
            .map(|(id, _)| id.clone())
            .collect();

        for connector_id in singleton_connectors {
            if let Some(state) = self.assigner.singleton_state(&connector_id) {
                if !state.is_leader_healthy(timeout) {
                    info!(
                        connector = %connector_id.0,
                        "Singleton leader unhealthy, triggering failover"
                    );

                    // Get the singleton task
                    let task_id = TaskId::new(&connector_id.0, 0);
                    if let Some(task) = self.tasks.get_mut(&task_id) {
                        if let Some(state) = self.assigner.singleton_state_mut(&connector_id) {
                            if let Some(new_leader) = state.start_failover() {
                                task.assign(
                                    new_leader.clone(),
                                    Generation::default(),
                                    self.epoch,
                                );

                                let connector = self.connectors.get(&connector_id);
                                failovers.push(TaskAssignmentMessage {
                                    task_id: task.id.clone(),
                                    connector_id: connector_id.clone(),
                                    task_number: 0,
                                    node_id: new_leader.clone(),
                                    generation: task.generation,
                                    epoch: self.epoch,
                                    is_singleton: true,
                                    is_leader: true,
                                    config: connector
                                        .map(|c| c.config.clone())
                                        .unwrap_or(serde_json::Value::Null),
                                });

                                info!(
                                    connector = %connector_id.0,
                                    new_leader = %new_leader.0,
                                    "Singleton failover complete"
                                );
                            }
                        }
                    }
                }
            }
        }

        failovers
    }

    /// Add a standby node for a singleton connector
    pub fn add_singleton_standby(&mut self, connector_id: &ConnectorId, node_id: NodeId) {
        if let Some(state) = self.assigner.singleton_state_mut(connector_id) {
            state.add_standby(node_id);
        }
    }

    /// Get singleton state
    pub fn get_singleton_state(&self, connector_id: &ConnectorId) -> Option<&SingletonState> {
        self.assigner.singleton_state(connector_id)
    }

    // =========================================================================
    // Status and Metrics
    // =========================================================================

    /// Get coordinator status
    pub fn status(&self) -> CoordinatorStatus {
        CoordinatorStatus {
            node_id: self.config.node_id.clone(),
            epoch: self.epoch,
            member_count: self.membership.member_count(),
            connector_count: self.connectors.len(),
            task_count: self.tasks.len(),
            rebalance_pending: self.rebalance_pending,
        }
    }
}

/// Coordinator status
#[derive(Debug, Clone)]
pub struct CoordinatorStatus {
    pub node_id: NodeId,
    pub epoch: Epoch,
    pub member_count: usize,
    pub connector_count: usize,
    pub task_count: usize,
    pub rebalance_pending: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton_connector_registration() {
        let config = CoordinatorConfig::default();
        let mut coordinator = ConnectCoordinator::new(config);

        // Register a singleton connector (like CDC)
        let result = coordinator.register_connector(
            ConnectorId::new("postgres-cdc"),
            "PostgreSQL CDC".to_string(),
            ConnectorMode::Singleton,
            10, // This should be ignored for singleton
            serde_json::json!({"slot": "test_slot"}),
        );

        assert!(result.is_ok());

        let connector = coordinator.get_connector(&ConnectorId::new("postgres-cdc"));
        assert!(connector.is_some());

        let connector = connector.unwrap();
        assert_eq!(connector.mode, ConnectorMode::Singleton);
        assert_eq!(connector.tasks_max, 1); // Should be 1 regardless of input
    }

    #[test]
    fn test_singleton_task_creation() {
        let config = CoordinatorConfig::default();
        let mut coordinator = ConnectCoordinator::new(config);

        coordinator
            .register_connector(
                ConnectorId::new("cdc"),
                "CDC".to_string(),
                ConnectorMode::Singleton,
                1,
                serde_json::json!({}),
            )
            .unwrap();

        // Should have exactly one task
        let tasks: Vec<_> = coordinator
            .tasks
            .values()
            .filter(|t| t.connector.0 == "cdc")
            .collect();

        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].config.is_singleton);
    }

    #[test]
    fn test_scalable_connector() {
        let config = CoordinatorConfig::default();
        let mut coordinator = ConnectCoordinator::new(config);

        coordinator
            .register_connector(
                ConnectorId::new("file-source"),
                "File Source".to_string(),
                ConnectorMode::Scalable,
                4,
                serde_json::json!({}),
            )
            .unwrap();

        let connector = coordinator
            .get_connector(&ConnectorId::new("file-source"))
            .unwrap();
        assert_eq!(connector.mode, ConnectorMode::Scalable);
        assert_eq!(connector.tasks_max, 4);

        // Should have 4 tasks
        let tasks: Vec<_> = coordinator
            .tasks
            .values()
            .filter(|t| t.connector.0 == "file-source")
            .collect();

        assert_eq!(tasks.len(), 4);
    }
}
