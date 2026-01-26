//! Partition types and state management

use crate::node::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Unique partition identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionId {
    pub topic: String,
    pub partition: u32,
}

impl PartitionId {
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

/// Partition replica state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaState {
    /// Replica is in sync with leader
    InSync,
    /// Replica is catching up
    CatchingUp,
    /// Replica is offline
    Offline,
    /// Replica is being added
    Adding,
    /// Replica is being removed
    Removing,
}

/// Information about a partition replica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    /// Node hosting this replica
    pub node_id: NodeId,
    /// Replica state
    pub state: ReplicaState,
    /// Log end offset (latest message)
    pub log_end_offset: u64,
    /// High watermark (committed)
    pub high_watermark: u64,
    /// Lag behind leader
    pub lag: u64,
}

impl ReplicaInfo {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            state: ReplicaState::Adding,
            log_end_offset: 0,
            high_watermark: 0,
            lag: 0,
        }
    }

    /// Check if replica is in sync
    pub fn is_in_sync(&self) -> bool {
        matches!(self.state, ReplicaState::InSync)
    }
}

/// Partition state and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    /// Partition identifier
    pub id: PartitionId,

    /// Current leader node
    pub leader: Option<NodeId>,

    /// Preferred leader (for rebalancing)
    pub preferred_leader: NodeId,

    /// All replicas (ordered: leader first, then followers)
    pub replicas: Vec<ReplicaInfo>,

    /// In-sync replica set (ISR)
    pub isr: HashSet<NodeId>,

    /// Epoch for leader election (increments on leader change)
    pub leader_epoch: u64,

    /// High watermark (committed offset)
    pub high_watermark: u64,

    /// Log start offset (after truncation)
    pub log_start_offset: u64,

    /// Is the partition online?
    pub online: bool,

    /// Is the partition under-replicated?
    pub under_replicated: bool,
}

impl PartitionState {
    /// Create new partition state
    pub fn new(id: PartitionId, replicas: Vec<NodeId>) -> Self {
        let preferred_leader = replicas.first().cloned().unwrap_or_default();
        let replica_infos: Vec<_> = replicas
            .iter()
            .map(|n| ReplicaInfo::new(n.clone()))
            .collect();
        let isr: HashSet<_> = replicas.into_iter().collect();

        // Partition is under-replicated if ISR size < replica count
        let under_replicated = isr.len() < replica_infos.len();

        Self {
            id,
            leader: None,
            preferred_leader,
            replicas: replica_infos,
            isr,
            leader_epoch: 0,
            high_watermark: 0,
            log_start_offset: 0,
            online: false,
            under_replicated,
        }
    }

    /// Elect a new leader from ISR
    pub fn elect_leader(&mut self) -> Option<&NodeId> {
        // Prefer the preferred leader if in ISR
        if self.isr.contains(&self.preferred_leader) {
            self.leader = Some(self.preferred_leader.clone());
        } else {
            // Otherwise pick first available from ISR
            self.leader = self.isr.iter().next().cloned();
        }

        if self.leader.is_some() {
            self.leader_epoch += 1;
            self.online = true;
        }

        self.leader.as_ref()
    }

    /// Add a node to ISR
    pub fn add_to_isr(&mut self, node_id: &NodeId) {
        self.isr.insert(node_id.clone());
        self.update_under_replicated();

        // Update replica state
        if let Some(replica) = self.replicas.iter_mut().find(|r| &r.node_id == node_id) {
            replica.state = ReplicaState::InSync;
        }
    }

    /// Remove a node from ISR
    pub fn remove_from_isr(&mut self, node_id: &NodeId) {
        self.isr.remove(node_id);
        self.update_under_replicated();

        // Update replica state
        if let Some(replica) = self.replicas.iter_mut().find(|r| &r.node_id == node_id) {
            replica.state = ReplicaState::CatchingUp;
        }

        // If leader was removed, need new election
        if self.leader.as_ref() == Some(node_id) {
            self.leader = None;
            self.online = false;
        }
    }

    /// Update replica offset
    pub fn update_replica_offset(&mut self, node_id: &NodeId, log_end_offset: u64) {
        // First, get the leader's LEO if we need to calculate lag
        let leader_leo = self.leader.as_ref().and_then(|leader| {
            self.replicas
                .iter()
                .find(|r| &r.node_id == leader)
                .map(|r| r.log_end_offset)
        });

        // Now update the replica
        if let Some(replica) = self.replicas.iter_mut().find(|r| &r.node_id == node_id) {
            replica.log_end_offset = log_end_offset;

            // Calculate lag from leader
            if let Some(leo) = leader_leo {
                replica.lag = leo.saturating_sub(log_end_offset);
            }
        }
    }

    /// Advance high watermark
    pub fn advance_high_watermark(&mut self) {
        // HWM is the minimum LEO across all ISR members
        let min_leo = self
            .replicas
            .iter()
            .filter(|r| self.isr.contains(&r.node_id))
            .map(|r| r.log_end_offset)
            .min()
            .unwrap_or(self.high_watermark);

        if min_leo > self.high_watermark {
            self.high_watermark = min_leo;

            // Update all replica HWMs
            for replica in &mut self.replicas {
                replica.high_watermark = self.high_watermark;
            }
        }
    }

    /// Check if partition has enough replicas
    fn update_under_replicated(&mut self) {
        let expected = self.replicas.len();
        let in_sync = self.isr.len();
        self.under_replicated = in_sync < expected;
    }

    /// Get replica node IDs
    pub fn replica_nodes(&self) -> Vec<&NodeId> {
        self.replicas.iter().map(|r| &r.node_id).collect()
    }

    /// Get ISR nodes
    pub fn isr_nodes(&self) -> Vec<&NodeId> {
        self.isr.iter().collect()
    }

    /// Check if a node is the leader
    pub fn is_leader(&self, node_id: &NodeId) -> bool {
        self.leader.as_ref() == Some(node_id)
    }

    /// Check if a node is a replica
    pub fn is_replica(&self, node_id: &NodeId) -> bool {
        self.replicas.iter().any(|r| &r.node_id == node_id)
    }

    /// Get replication factor
    pub fn replication_factor(&self) -> usize {
        self.replicas.len()
    }
}

/// Topic configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicConfig {
    /// Topic name
    pub name: String,

    /// Number of partitions
    pub partitions: u32,

    /// Replication factor
    pub replication_factor: u16,

    /// Retention period in milliseconds
    pub retention_ms: u64,

    /// Segment size in bytes
    pub segment_bytes: u64,

    /// Minimum ISR required for writes
    pub min_isr: u16,

    /// Custom configuration
    pub config: std::collections::HashMap<String, String>,
}

impl TopicConfig {
    pub fn new(name: impl Into<String>, partitions: u32, replication_factor: u16) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
            retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            segment_bytes: 1024 * 1024 * 1024,     // 1 GB
            min_isr: 1,
            config: std::collections::HashMap::new(),
        }
    }

    pub fn with_retention_ms(mut self, ms: u64) -> Self {
        self.retention_ms = ms;
        self
    }

    pub fn with_min_isr(mut self, min_isr: u16) -> Self {
        self.min_isr = min_isr;
        self
    }
}

/// Topic state including all partitions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicState {
    /// Topic configuration
    pub config: TopicConfig,

    /// Partition states
    pub partitions: Vec<PartitionState>,
}

impl TopicState {
    pub fn new(config: TopicConfig, partition_assignments: Vec<Vec<NodeId>>) -> Self {
        let partitions = partition_assignments
            .into_iter()
            .enumerate()
            .map(|(i, replicas)| {
                let mut state =
                    PartitionState::new(PartitionId::new(&config.name, i as u32), replicas);
                // Automatically elect leader from ISR (first replica is preferred)
                state.elect_leader();
                state
            })
            .collect();

        Self { config, partitions }
    }

    /// Get partition by index
    pub fn partition(&self, idx: u32) -> Option<&PartitionState> {
        self.partitions.get(idx as usize)
    }

    /// Get mutable partition by index
    pub fn partition_mut(&mut self, idx: u32) -> Option<&mut PartitionState> {
        self.partitions.get_mut(idx as usize)
    }

    /// Check if all partitions have a leader
    pub fn is_fully_online(&self) -> bool {
        self.partitions.iter().all(|p| p.online)
    }

    /// Check if any partition is under-replicated
    pub fn is_under_replicated(&self) -> bool {
        self.partitions.iter().any(|p| p.under_replicated)
    }

    /// Get offline partitions
    pub fn offline_partitions(&self) -> Vec<u32> {
        self.partitions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.online)
            .map(|(i, _)| i as u32)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_leader_election() {
        let id = PartitionId::new("test-topic", 0);
        let replicas = vec![
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ];
        let mut partition = PartitionState::new(id, replicas);

        // Initially no leader
        assert!(partition.leader.is_none());
        assert!(!partition.online);

        // Elect leader
        let leader = partition.elect_leader();
        assert!(leader.is_some());
        assert_eq!(leader.unwrap(), "node-1"); // Preferred leader
        assert!(partition.online);
        assert_eq!(partition.leader_epoch, 1);
    }

    #[test]
    fn test_isr_management() {
        let id = PartitionId::new("test-topic", 0);
        let replicas = vec![
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ];
        let mut partition = PartitionState::new(id, replicas);
        partition.elect_leader();

        // All in ISR initially
        assert_eq!(partition.isr.len(), 3);
        assert!(!partition.under_replicated);

        // Remove node-2 from ISR
        partition.remove_from_isr(&"node-2".to_string());
        assert_eq!(partition.isr.len(), 2);
        assert!(partition.under_replicated);

        // Add back
        partition.add_to_isr(&"node-2".to_string());
        assert_eq!(partition.isr.len(), 3);
        assert!(!partition.under_replicated);
    }

    #[test]
    fn test_high_watermark_advancement() {
        let id = PartitionId::new("test-topic", 0);
        let replicas = vec!["node-1".to_string(), "node-2".to_string()];
        let mut partition = PartitionState::new(id, replicas);
        partition.elect_leader();

        // Update replica offsets
        partition.update_replica_offset(&"node-1".to_string(), 100);
        partition.update_replica_offset(&"node-2".to_string(), 80);

        // HWM should be min of ISR
        partition.advance_high_watermark();
        assert_eq!(partition.high_watermark, 80);

        // node-2 catches up
        partition.update_replica_offset(&"node-2".to_string(), 100);
        partition.advance_high_watermark();
        assert_eq!(partition.high_watermark, 100);
    }
}
