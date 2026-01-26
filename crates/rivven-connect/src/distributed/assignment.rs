//! Task assignment strategies for distributed connectors

use crate::distributed::task::{ConnectorTask, SingletonState};
use crate::distributed::types::*;
use std::collections::HashMap;

/// Assignment decision for a task
#[derive(Debug, Clone)]
pub enum AssignmentDecision {
    /// Assign to a specific node
    Assign(NodeId),
    /// Keep current assignment
    Keep,
    /// Reassign to a different node
    Reassign { from: NodeId, to: NodeId },
    /// Unassign (no suitable node)
    Unassign,
}

/// Strategy for assigning tasks to nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AssignmentStrategy {
    /// Round-robin assignment
    RoundRobin,
    /// Least-loaded node first (default)
    #[default]
    LeastLoaded,
    /// Sticky assignment (prefer previous node)
    Sticky,
    /// Random assignment
    Random,
    /// Rack-aware assignment
    RackAware,
}

/// Node capacity and load information
#[derive(Debug, Clone)]
pub struct NodeLoad {
    /// Node identifier
    pub node: NodeId,
    /// Maximum tasks this node can handle
    pub max_tasks: u32,
    /// Current number of assigned tasks
    pub current_tasks: u32,
    /// CPU usage (0.0 - 1.0)
    pub cpu_usage: f32,
    /// Memory usage (0.0 - 1.0)
    pub memory_usage: f32,
    /// Whether node is healthy
    pub is_healthy: bool,
    /// Rack/zone identifier for rack-aware placement
    pub rack: Option<String>,
}

impl NodeLoad {
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            max_tasks: 100,
            current_tasks: 0,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            is_healthy: true,
            rack: None,
        }
    }

    /// Calculate available capacity (0.0 - 1.0)
    pub fn available_capacity(&self) -> f32 {
        if !self.is_healthy || self.current_tasks >= self.max_tasks {
            return 0.0;
        }

        let task_capacity = 1.0 - (self.current_tasks as f32 / self.max_tasks as f32);
        let resource_capacity = 1.0 - (self.cpu_usage.max(self.memory_usage));

        task_capacity.min(resource_capacity)
    }

    /// Check if node can accept more tasks
    pub fn can_accept_tasks(&self) -> bool {
        self.is_healthy && self.current_tasks < self.max_tasks && self.available_capacity() > 0.1
    }
}

/// Task assigner for distributing connector tasks across nodes
pub struct TaskAssigner {
    /// Assignment strategy
    strategy: AssignmentStrategy,
    /// Node loads
    node_loads: HashMap<NodeId, NodeLoad>,
    /// Task assignments
    assignments: HashMap<TaskId, NodeId>,
    /// Singleton states
    singletons: HashMap<ConnectorId, SingletonState>,
    /// Round-robin counter
    rr_counter: usize,
}

impl TaskAssigner {
    pub fn new(strategy: AssignmentStrategy) -> Self {
        Self {
            strategy,
            node_loads: HashMap::new(),
            assignments: HashMap::new(),
            singletons: HashMap::new(),
            rr_counter: 0,
        }
    }

    /// Update node load information
    pub fn update_node_load(&mut self, load: NodeLoad) {
        self.node_loads.insert(load.node.clone(), load);
    }

    /// Remove a node (when it leaves the cluster)
    pub fn remove_node(&mut self, node: &NodeId) {
        self.node_loads.remove(node);

        // Update singleton states
        for state in self.singletons.values_mut() {
            state.remove_node(node);
        }
    }

    /// Get healthy nodes
    pub fn healthy_nodes(&self) -> Vec<&NodeLoad> {
        self.node_loads.values().filter(|n| n.is_healthy).collect()
    }

    /// Register a singleton connector
    pub fn register_singleton(&mut self, connector: ConnectorId) {
        if !self.singletons.contains_key(&connector) {
            self.singletons
                .insert(connector.clone(), SingletonState::new(connector));
        }
    }

    /// Get singleton state for a connector
    pub fn singleton_state(&self, connector: &ConnectorId) -> Option<&SingletonState> {
        self.singletons.get(connector)
    }

    /// Get mutable singleton state
    pub fn singleton_state_mut(&mut self, connector: &ConnectorId) -> Option<&mut SingletonState> {
        self.singletons.get_mut(connector)
    }

    /// Assign a task to a node
    ///
    /// For singleton tasks, this handles leader election.
    /// For scalable tasks, this uses the configured strategy.
    pub fn assign_task(&mut self, task: &mut ConnectorTask) -> AssignmentDecision {
        if task.config.is_singleton {
            return self.assign_singleton_task(task);
        }

        match task.assigned_node.as_ref() {
            // Already assigned - check if we should reassign
            Some(current) => {
                if let Some(load) = self.node_loads.get(current) {
                    if load.is_healthy {
                        // Keep current assignment if node is healthy
                        if self.strategy == AssignmentStrategy::Sticky {
                            return AssignmentDecision::Keep;
                        }

                        // Only reassign if there's significant imbalance
                        if load.available_capacity() > 0.2 {
                            return AssignmentDecision::Keep;
                        }
                    }
                }

                // Node unhealthy or overloaded - reassign
                if let Some(new_node) = self.select_node_for_task(task) {
                    let from = current.clone();
                    self.assignments.insert(task.id.clone(), new_node.clone());
                    AssignmentDecision::Reassign { from, to: new_node }
                } else {
                    AssignmentDecision::Unassign
                }
            }

            // Not assigned - find a node
            None => {
                if let Some(node) = self.select_node_for_task(task) {
                    self.assignments.insert(task.id.clone(), node.clone());
                    AssignmentDecision::Assign(node)
                } else {
                    AssignmentDecision::Unassign
                }
            }
        }
    }

    /// Assign a singleton task (leader election)
    fn assign_singleton_task(&mut self, task: &mut ConnectorTask) -> AssignmentDecision {
        // Ensure singleton state exists
        if !self.singletons.contains_key(&task.connector) {
            self.singletons.insert(
                task.connector.clone(),
                SingletonState::new(task.connector.clone()),
            );
        }

        // First, gather information we need
        let connector = task.connector.clone();
        let task_assigned_node = task.assigned_node.clone();
        let task_id = task.id.clone();

        // Check current leader status
        let leader_status = {
            let state = self.singletons.get(&connector).unwrap();
            if let Some(leader) = &state.leader {
                if let Some(load) = self.node_loads.get(leader) {
                    if load.is_healthy {
                        Some((leader.clone(), true)) // (leader, healthy)
                    } else {
                        Some((leader.clone(), false)) // (leader, unhealthy)
                    }
                } else {
                    Some((leader.clone(), false)) // (leader, unhealthy - not in loads)
                }
            } else {
                None
            }
        };

        // Handle based on leader status
        match leader_status {
            Some((leader, true)) => {
                // Healthy leader exists
                if task_assigned_node.as_ref() == Some(&leader) {
                    return AssignmentDecision::Keep;
                } else {
                    self.assignments.insert(task_id, leader.clone());
                    return AssignmentDecision::Assign(leader);
                }
            }
            Some((old_leader, false)) => {
                // Leader unhealthy - trigger failover
                let state = self.singletons.get_mut(&connector).unwrap();
                if let Some(new_leader) = state.start_failover() {
                    self.assignments.insert(task_id, new_leader.clone());
                    return AssignmentDecision::Reassign {
                        from: old_leader,
                        to: new_leader,
                    };
                }
            }
            None => {
                // No leader
            }
        }

        // No leader or failover failed - elect one from healthy nodes
        let healthy: Vec<NodeId> = self
            .healthy_nodes()
            .iter()
            .map(|n| n.node.clone())
            .collect();

        if healthy.is_empty() {
            return AssignmentDecision::Unassign;
        }

        // Get standbys
        let standbys: Vec<NodeId> = {
            let state = self.singletons.get(&connector).unwrap();
            state.standbys.clone()
        };

        // Prefer nodes in standby list first
        for standby in standbys {
            if healthy.contains(&standby) {
                let state = self.singletons.get_mut(&connector).unwrap();
                state.set_leader(standby.clone());
                self.assignments.insert(task_id, standby.clone());
                return AssignmentDecision::Assign(standby);
            }
        }

        // Otherwise pick the least loaded healthy node
        if let Some(node) = self.least_loaded_node() {
            let state = self.singletons.get_mut(&connector).unwrap();
            state.set_leader(node.clone());
            self.assignments.insert(task_id, node.clone());
            return AssignmentDecision::Assign(node);
        }

        AssignmentDecision::Unassign
    }

    /// Select a node for a task based on strategy
    fn select_node_for_task(&mut self, _task: &ConnectorTask) -> Option<NodeId> {
        match self.strategy {
            AssignmentStrategy::RoundRobin => self.round_robin_select(),
            AssignmentStrategy::LeastLoaded => self.least_loaded_node(),
            AssignmentStrategy::Sticky => self.least_loaded_node(),
            AssignmentStrategy::Random => self.random_select(),
            AssignmentStrategy::RackAware => self.rack_aware_select(),
        }
    }

    fn round_robin_select(&mut self) -> Option<NodeId> {
        let healthy: Vec<NodeId> = self
            .healthy_nodes()
            .iter()
            .filter(|n| n.can_accept_tasks())
            .map(|n| n.node.clone())
            .collect();

        if healthy.is_empty() {
            return None;
        }

        self.rr_counter = (self.rr_counter + 1) % healthy.len();
        Some(healthy[self.rr_counter].clone())
    }

    fn least_loaded_node(&self) -> Option<NodeId> {
        self.healthy_nodes()
            .into_iter()
            .filter(|n| n.can_accept_tasks())
            .max_by(|a, b| {
                a.available_capacity()
                    .partial_cmp(&b.available_capacity())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|n| n.node.clone())
    }

    fn random_select(&self) -> Option<NodeId> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let binding = self.healthy_nodes();
        let healthy: Vec<_> = binding.iter().filter(|n| n.can_accept_tasks()).collect();

        if healthy.is_empty() {
            return None;
        }

        // Use timestamp for pseudo-randomness
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);
        let index = (hasher.finish() as usize) % healthy.len();

        Some(healthy[index].node.clone())
    }

    fn rack_aware_select(&self) -> Option<NodeId> {
        // Group nodes by rack
        let mut racks: HashMap<Option<String>, Vec<&NodeLoad>> = HashMap::new();

        for node in self.healthy_nodes() {
            if node.can_accept_tasks() {
                racks.entry(node.rack.clone()).or_default().push(node);
            }
        }

        // Find the rack with most available capacity
        let best_rack = racks.iter().max_by_key(|(_, nodes)| {
            (nodes
                .iter()
                .map(|n| (n.available_capacity() * 100.0) as u32)
                .sum::<u32>())
                / nodes.len() as u32
        });

        if let Some((_, nodes)) = best_rack {
            // Pick least loaded node in that rack
            nodes
                .iter()
                .max_by(|a, b| {
                    a.available_capacity()
                        .partial_cmp(&b.available_capacity())
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|n| n.node.clone())
        } else {
            None
        }
    }

    /// Rebalance tasks across nodes
    ///
    /// Returns a list of tasks that need to be moved.
    pub fn rebalance(&mut self, tasks: &mut [ConnectorTask]) -> Vec<(TaskId, NodeId, NodeId)> {
        let mut moves = Vec::new();

        // Don't rebalance singletons - they use leader election
        let scalable_tasks: Vec<_> = tasks
            .iter_mut()
            .filter(|t| !t.config.is_singleton)
            .collect();

        if scalable_tasks.is_empty() {
            return moves;
        }

        // Calculate ideal distribution
        let healthy_count = self.healthy_nodes().len();
        if healthy_count == 0 {
            return moves;
        }

        let ideal_per_node = scalable_tasks.len() / healthy_count;
        let remainder = scalable_tasks.len() % healthy_count;

        // Count current distribution
        let mut current_distribution: HashMap<NodeId, usize> = HashMap::new();
        for task in &scalable_tasks {
            if let Some(node) = &task.assigned_node {
                *current_distribution.entry(node.clone()).or_default() += 1;
            }
        }

        // Find overloaded and underloaded nodes
        let mut overloaded: Vec<(NodeId, usize)> = Vec::new();
        let mut underloaded: Vec<(NodeId, usize)> = Vec::new();

        for load in self.healthy_nodes() {
            let current = *current_distribution.get(&load.node).unwrap_or(&0);
            let target = if underloaded.len() < remainder {
                ideal_per_node + 1
            } else {
                ideal_per_node
            };

            if current > target + 1 {
                overloaded.push((load.node.clone(), current - target));
            } else if current < target {
                underloaded.push((load.node.clone(), target - current));
            }
        }

        // Move tasks from overloaded to underloaded nodes
        // This is a simplified algorithm - production would use more sophisticated balancing
        for task in scalable_tasks {
            if let Some(from) = &task.assigned_node {
                if let Some(pos) = overloaded.iter().position(|(n, _)| n == from) {
                    if let Some((to, needed)) = underloaded.first_mut() {
                        if *needed > 0 {
                            moves.push((task.id.clone(), from.clone(), to.clone()));
                            *needed -= 1;
                            overloaded[pos].1 -= 1;

                            if overloaded[pos].1 == 0 {
                                overloaded.remove(pos);
                            }
                            if *needed == 0 {
                                underloaded.remove(0);
                            }
                        }
                    }
                }
            }
        }

        moves
    }
}
