//! Connect cluster membership management

use crate::distributed::types::*;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Member state in the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MemberState {
    /// Just joined, syncing state
    Joining,
    /// Fully active member
    Active,
    /// Suspected to be down
    Suspect,
    /// Confirmed down
    Down,
    /// Gracefully leaving
    Leaving,
}

/// Information about a cluster member
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    /// Node ID
    pub node_id: NodeId,
    /// Node address (host:port)
    pub address: String,
    /// Current state
    pub state: MemberState,
    /// When the node joined
    pub joined_at: i64,
    /// Last heartbeat time
    pub last_heartbeat: i64,
    /// Available resources
    #[serde(default)]
    pub resources: NodeResources,
    /// Node labels/tags
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Currently assigned tasks
    #[serde(default)]
    pub assigned_tasks: Vec<TaskId>,
}

impl MemberInfo {
    pub fn new(node_id: NodeId, address: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            node_id,
            address,
            state: MemberState::Joining,
            joined_at: now,
            last_heartbeat: now,
            resources: NodeResources::default(),
            labels: HashMap::new(),
            assigned_tasks: Vec::new(),
        }
    }

    /// Check if node is healthy (active or joining)
    pub fn is_healthy(&self) -> bool {
        matches!(self.state, MemberState::Active | MemberState::Joining)
    }

    /// Update heartbeat timestamp
    pub fn touch(&mut self) {
        self.last_heartbeat = chrono::Utc::now().timestamp_millis();
    }

    /// Get time since last heartbeat
    pub fn time_since_heartbeat(&self) -> Duration {
        let now = chrono::Utc::now().timestamp_millis();
        Duration::from_millis((now - self.last_heartbeat).max(0) as u64)
    }
}

/// Node resource availability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResources {
    /// Available memory in MB
    #[serde(default)]
    pub memory_mb: u32,
    /// Available CPU cores
    #[serde(default)]
    pub cpu_cores: f32,
    /// Maximum tasks this node can handle
    #[serde(default = "default_max_tasks")]
    pub max_tasks: u32,
    /// Current task count
    #[serde(default)]
    pub current_tasks: u32,
}

fn default_max_tasks() -> u32 {
    10
}

impl Default for NodeResources {
    fn default() -> Self {
        Self {
            memory_mb: 1024,
            cpu_cores: 2.0,
            max_tasks: default_max_tasks(),
            current_tasks: 0,
        }
    }
}

impl NodeResources {
    /// Check if node has capacity for more tasks
    pub fn has_capacity(&self) -> bool {
        self.current_tasks < self.max_tasks
    }

    /// Available task slots
    pub fn available_slots(&self) -> u32 {
        self.max_tasks.saturating_sub(self.current_tasks)
    }
}

/// Membership events
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub enum MembershipEvent {
    /// New member joined
    MemberJoined(NodeId),
    /// Member became active
    MemberActive(NodeId),
    /// Member suspected down
    MemberSuspect(NodeId),
    /// Member confirmed down
    MemberDown(NodeId),
    /// Member left gracefully
    MemberLeft(NodeId),
}

/// Connect cluster membership
#[allow(dead_code)] // Part of distributed mode infrastructure
pub struct ConnectMembership {
    /// Our node ID
    node_id: NodeId,
    /// Our address
    address: String,
    /// All known members
    members: RwLock<HashMap<NodeId, MemberInfo>>,
    /// Suspect timeout (before marking suspect)
    suspect_timeout: Duration,
    /// Down timeout (after suspect, before marking down)
    down_timeout: Duration,
    /// Event sender
    event_tx: broadcast::Sender<MembershipEvent>,
}

impl ConnectMembership {
    /// Create new membership manager
    pub fn new(
        node_id: NodeId,
        address: String,
        suspect_timeout: Duration,
        down_timeout: Duration,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(64);

        let mut members = HashMap::new();
        let mut self_info = MemberInfo::new(node_id.clone(), address.clone());
        self_info.state = MemberState::Active;
        members.insert(node_id.clone(), self_info);

        Self {
            node_id,
            address,
            members: RwLock::new(members),
            suspect_timeout,
            down_timeout,
            event_tx,
        }
    }

    /// Get our node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Subscribe to membership events
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.event_tx.subscribe()
    }

    /// Add or update a member
    pub fn upsert_member(&self, mut info: MemberInfo) {
        let mut members = self.members.write();

        if let Some(existing) = members.get_mut(&info.node_id) {
            // Update existing member
            let old_state = existing.state;
            existing.last_heartbeat = info.last_heartbeat;
            existing.resources = info.resources;
            existing.assigned_tasks = info.assigned_tasks;

            // Handle state transitions
            if old_state != info.state {
                match info.state {
                    MemberState::Active if old_state == MemberState::Joining => {
                        let _ = self
                            .event_tx
                            .send(MembershipEvent::MemberActive(info.node_id.clone()));
                    }
                    MemberState::Suspect => {
                        let _ = self
                            .event_tx
                            .send(MembershipEvent::MemberSuspect(info.node_id.clone()));
                    }
                    MemberState::Down => {
                        let _ = self
                            .event_tx
                            .send(MembershipEvent::MemberDown(info.node_id.clone()));
                    }
                    _ => {}
                }
                existing.state = info.state;
            }
        } else {
            // New member
            info.state = MemberState::Joining;
            let _ = self
                .event_tx
                .send(MembershipEvent::MemberJoined(info.node_id.clone()));
            members.insert(info.node_id.clone(), info);
        }
    }

    /// Process a heartbeat from a member
    pub fn process_heartbeat(&self, node_id: &NodeId) {
        let mut members = self.members.write();

        if let Some(member) = members.get_mut(node_id) {
            member.touch();

            // If suspect, mark as active
            if member.state == MemberState::Suspect {
                member.state = MemberState::Active;
                let _ = self
                    .event_tx
                    .send(MembershipEvent::MemberActive(node_id.clone()));
            }
        }
    }

    /// Mark a member as leaving
    pub fn mark_leaving(&self, node_id: &NodeId) {
        let mut members = self.members.write();

        if let Some(member) = members.get_mut(node_id) {
            member.state = MemberState::Leaving;
            let _ = self
                .event_tx
                .send(MembershipEvent::MemberLeft(node_id.clone()));
        }
    }

    /// Remove a member
    pub fn remove_member(&self, node_id: &NodeId) -> Option<MemberInfo> {
        let mut members = self.members.write();
        members.remove(node_id)
    }

    /// Get a member by ID
    pub fn get_member(&self, node_id: &NodeId) -> Option<MemberInfo> {
        let members = self.members.read();
        members.get(node_id).cloned()
    }

    /// Get all members
    pub fn all_members(&self) -> Vec<MemberInfo> {
        let members = self.members.read();
        members.values().cloned().collect()
    }

    /// Get healthy members (active or joining)
    pub fn healthy_members(&self) -> Vec<MemberInfo> {
        let members = self.members.read();
        members
            .values()
            .filter(|m| m.is_healthy())
            .cloned()
            .collect()
    }

    /// Get active members
    pub fn active_members(&self) -> Vec<MemberInfo> {
        let members = self.members.read();
        members
            .values()
            .filter(|m| m.state == MemberState::Active)
            .cloned()
            .collect()
    }

    /// Get member count
    pub fn member_count(&self) -> usize {
        let members = self.members.read();
        members.len()
    }

    /// Check members for timeouts and update states
    pub fn check_timeouts(&self) -> Vec<MembershipEvent> {
        let mut events = Vec::new();
        let mut members = self.members.write();
        let now = chrono::Utc::now().timestamp_millis();

        for member in members.values_mut() {
            // Skip ourselves
            if member.node_id == self.node_id {
                continue;
            }

            let elapsed = Duration::from_millis((now - member.last_heartbeat).max(0) as u64);

            match member.state {
                MemberState::Active | MemberState::Joining => {
                    if elapsed > self.suspect_timeout {
                        member.state = MemberState::Suspect;
                        events.push(MembershipEvent::MemberSuspect(member.node_id.clone()));
                    }
                }
                MemberState::Suspect => {
                    if elapsed > self.suspect_timeout + self.down_timeout {
                        member.state = MemberState::Down;
                        events.push(MembershipEvent::MemberDown(member.node_id.clone()));
                    }
                }
                _ => {}
            }
        }

        // Send events
        for event in &events {
            let _ = self.event_tx.send(event.clone());
        }

        events
    }

    /// Update our own resources
    pub fn update_self_resources(&self, resources: NodeResources) {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(&self.node_id) {
            member.resources = resources;
        }
    }

    /// Update assigned tasks for a member
    pub fn update_assigned_tasks(&self, node_id: &NodeId, tasks: Vec<TaskId>) {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(node_id) {
            member.resources.current_tasks = tasks.len() as u32;
            member.assigned_tasks = tasks;
        }
    }

    /// Get members with available capacity
    pub fn members_with_capacity(&self) -> Vec<MemberInfo> {
        let members = self.members.read();
        members
            .values()
            .filter(|m| m.state == MemberState::Active && m.resources.has_capacity())
            .cloned()
            .collect()
    }

    /// Create a snapshot of current membership
    pub fn snapshot(&self) -> MembershipSnapshot {
        let members = self.members.read();
        MembershipSnapshot {
            timestamp: chrono::Utc::now().timestamp_millis(),
            members: members.values().cloned().collect(),
        }
    }
}

/// Snapshot of membership state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MembershipSnapshot {
    pub timestamp: i64,
    pub members: Vec<MemberInfo>,
}

/// Simple membership manager for the coordinator
///
/// This is a lightweight wrapper that tracks node membership without
/// the full SWIM protocol complexity.
pub struct MembershipManager {
    /// Our node ID
    node_id: NodeId,
    /// Known members
    members: HashMap<NodeId, Instant>,
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(node_id: NodeId) -> Self {
        let mut members = HashMap::new();
        members.insert(node_id.clone(), Instant::now());

        Self { node_id, members }
    }

    /// Add a member
    pub fn add_member(&mut self, node_id: NodeId) {
        self.members.insert(node_id, Instant::now());
    }

    /// Remove a member
    pub fn remove_member(&mut self, node_id: &NodeId) {
        self.members.remove(node_id);
    }

    /// Record heartbeat from a member
    pub fn record_heartbeat(&mut self, node_id: &NodeId) {
        if let Some(ts) = self.members.get_mut(node_id) {
            *ts = Instant::now();
        }
    }

    /// Get all members
    pub fn members(&self) -> impl Iterator<Item = &NodeId> {
        self.members.keys()
    }

    /// Get member count
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Check if a node is a member
    pub fn is_member(&self, node_id: &NodeId) -> bool {
        self.members.contains_key(node_id)
    }

    /// Get members that haven't sent heartbeat within timeout
    pub fn stale_members(&self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        self.members
            .iter()
            .filter(|(id, ts)| *id != &self.node_id && now.duration_since(**ts) > timeout)
            .map(|(id, _)| id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_lifecycle() {
        let membership = ConnectMembership::new(
            NodeId::new("node-1"),
            "localhost:9093".to_string(),
            Duration::from_secs(5),
            Duration::from_secs(10),
        );

        // Add a member
        let member = MemberInfo::new(NodeId::new("node-2"), "localhost:9094".to_string());
        membership.upsert_member(member);

        // Should be joining
        let m = membership.get_member(&NodeId::new("node-2")).unwrap();
        assert_eq!(m.state, MemberState::Joining);

        // Process heartbeat
        membership.process_heartbeat(&NodeId::new("node-2"));

        // Should still be joining (need explicit state change)
        let m = membership.get_member(&NodeId::new("node-2")).unwrap();
        assert_eq!(m.state, MemberState::Joining);

        // Update to active
        let mut active_member = m;
        active_member.state = MemberState::Active;
        membership.upsert_member(active_member);

        let m = membership.get_member(&NodeId::new("node-2")).unwrap();
        assert_eq!(m.state, MemberState::Active);
    }

    #[test]
    fn test_healthy_members() {
        let membership = ConnectMembership::new(
            NodeId::new("node-1"),
            "localhost:9093".to_string(),
            Duration::from_secs(5),
            Duration::from_secs(10),
        );

        // Add members in different states
        let mut m2 = MemberInfo::new(NodeId::new("node-2"), "localhost:9094".to_string());
        m2.state = MemberState::Active;
        membership.upsert_member(m2.clone());
        // Update state after initial insert (which sets to Joining)
        m2.state = MemberState::Active;
        membership.upsert_member(m2);

        let mut m3 = MemberInfo::new(NodeId::new("node-3"), "localhost:9095".to_string());
        membership.upsert_member(m3.clone());
        // Update state to Suspect after initial insert
        m3.state = MemberState::Suspect;
        membership.upsert_member(m3);

        // Should have 2 healthy (node-1 self + node-2)
        // node-3 is Suspect, not healthy
        let healthy = membership.healthy_members();
        assert_eq!(healthy.len(), 2);

        // Should have 2 active (node-1 self + node-2)
        let active = membership.active_members();
        assert_eq!(active.len(), 2);
    }

    #[test]
    fn test_resource_capacity() {
        let mut resources = NodeResources {
            memory_mb: 1024,
            cpu_cores: 2.0,
            max_tasks: 5,
            current_tasks: 3,
        };

        assert!(resources.has_capacity());
        assert_eq!(resources.available_slots(), 2);

        resources.current_tasks = 5;
        assert!(!resources.has_capacity());
        assert_eq!(resources.available_slots(), 0);
    }
}
