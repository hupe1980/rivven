//! Cluster metadata and Raft state machine
//!
//! This module implements the Raft state machine for cluster metadata consensus.
//! Unlike systems like Redpanda that use Raft per-partition, we use a single
//! Raft group for all metadata which is simpler and more memory efficient.
//!
//! Metadata managed by Raft:
//! - Topic configurations
//! - Partition assignments (which nodes host which partitions)
//! - In-Sync Replica (ISR) state
//! - Node registry (known nodes and their capabilities)

use crate::error::{ClusterError, Result};
use crate::node::{NodeId, NodeInfo};
use crate::partition::{PartitionId, PartitionState, TopicConfig, TopicState};
use rivven_core::consumer_group::{ConsumerGroup, GroupId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Raft log entry types for metadata operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataCommand {
    // ==================== Topic Operations ====================
    /// Create a new topic
    CreateTopic {
        config: TopicConfig,
        partition_assignments: Vec<Vec<NodeId>>,
    },

    /// Delete a topic
    DeleteTopic { name: String },

    /// Update topic configuration
    UpdateTopicConfig {
        name: String,
        config: HashMap<String, String>,
    },

    /// Add partitions to existing topic
    AddPartitions {
        topic: String,
        new_assignments: Vec<Vec<NodeId>>,
    },

    // ==================== Partition Operations ====================
    /// Update partition leader
    UpdatePartitionLeader {
        partition: PartitionId,
        leader: NodeId,
        epoch: u64,
    },

    /// Update partition ISR
    UpdatePartitionIsr {
        partition: PartitionId,
        isr: Vec<NodeId>,
    },

    /// Reassign partition replicas
    ReassignPartition {
        partition: PartitionId,
        replicas: Vec<NodeId>,
    },

    // ==================== Node Operations ====================
    /// Register a new node
    RegisterNode { info: NodeInfo },

    /// Deregister a node
    DeregisterNode { node_id: NodeId },

    /// Update node metadata
    UpdateNode { node_id: NodeId, info: NodeInfo },

    // ==================== Cluster Operations ====================
    /// Update cluster-wide configuration
    UpdateClusterConfig { config: HashMap<String, String> },

    // ==================== Consumer Group Operations ====================
    /// Update consumer group state (full snapshot)
    UpdateConsumerGroup { group: ConsumerGroup },

    /// Delete consumer group
    DeleteConsumerGroup { group_id: GroupId },

    /// No-op (for heartbeats/leader election)
    Noop,
}

/// Result of applying a metadata command
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataResponse {
    Success,
    TopicCreated {
        name: String,
        partitions: u32,
    },
    TopicDeleted {
        name: String,
    },
    PartitionLeaderUpdated {
        partition: PartitionId,
        leader: NodeId,
    },
    IsrUpdated {
        partition: PartitionId,
        isr: Vec<NodeId>,
    },
    NodeRegistered {
        node_id: NodeId,
    },
    NodeDeregistered {
        node_id: NodeId,
    },
    ConsumerGroupUpdated {
        group_id: GroupId,
    },
    ConsumerGroupDeleted {
        group_id: GroupId,
    },
    Error {
        message: String,
    },
}

/// Cluster metadata state (Raft state machine)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterMetadata {
    /// Topics and their states
    pub topics: HashMap<String, TopicState>,

    /// Registered nodes
    pub nodes: HashMap<NodeId, NodeInfo>,

    /// Consumer groups
    pub consumer_groups: HashMap<GroupId, ConsumerGroup>,

    /// Cluster-wide configuration
    pub config: HashMap<String, String>,

    /// Last applied Raft index
    pub last_applied_index: u64,

    /// Cluster epoch (increments on significant changes)
    pub epoch: u64,
}

impl ClusterMetadata {
    /// Create new empty metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a command to the state machine
    pub fn apply(&mut self, index: u64, cmd: MetadataCommand) -> MetadataResponse {
        self.last_applied_index = index;

        match cmd {
            MetadataCommand::CreateTopic {
                config,
                partition_assignments,
            } => self.create_topic(config, partition_assignments),
            MetadataCommand::DeleteTopic { name } => self.delete_topic(&name),
            MetadataCommand::UpdateTopicConfig { name, config } => {
                self.update_topic_config(&name, config)
            }
            MetadataCommand::AddPartitions {
                topic,
                new_assignments,
            } => self.add_partitions(&topic, new_assignments),
            MetadataCommand::UpdatePartitionLeader {
                partition,
                leader,
                epoch,
            } => self.update_partition_leader(&partition, leader, epoch),
            MetadataCommand::UpdatePartitionIsr { partition, isr } => {
                self.update_partition_isr(&partition, isr)
            }
            MetadataCommand::ReassignPartition {
                partition,
                replicas,
            } => self.reassign_partition(&partition, replicas),
            MetadataCommand::RegisterNode { info } => self.register_node(info),
            MetadataCommand::DeregisterNode { node_id } => self.deregister_node(&node_id),
            MetadataCommand::UpdateNode { node_id, info } => self.update_node(&node_id, info),
            MetadataCommand::UpdateClusterConfig { config } => self.update_cluster_config(config),
            MetadataCommand::UpdateConsumerGroup { group } => self.update_consumer_group(group),
            MetadataCommand::DeleteConsumerGroup { group_id } => {
                self.delete_consumer_group(&group_id)
            }
            MetadataCommand::Noop => MetadataResponse::Success,
        }
    }

    /// Create a new topic
    fn create_topic(
        &mut self,
        config: TopicConfig,
        partition_assignments: Vec<Vec<NodeId>>,
    ) -> MetadataResponse {
        if self.topics.contains_key(&config.name) {
            return MetadataResponse::Error {
                message: format!("Topic {} already exists", config.name),
            };
        }

        let name = config.name.clone();
        let partitions = config.partitions;
        let topic_state = TopicState::new(config, partition_assignments);

        self.topics.insert(name.clone(), topic_state);
        self.epoch += 1;

        MetadataResponse::TopicCreated { name, partitions }
    }

    /// Delete a topic
    fn delete_topic(&mut self, name: &str) -> MetadataResponse {
        if self.topics.remove(name).is_some() {
            self.epoch += 1;
            MetadataResponse::TopicDeleted {
                name: name.to_string(),
            }
        } else {
            MetadataResponse::Error {
                message: format!("Topic {} not found", name),
            }
        }
    }

    /// Update topic configuration
    fn update_topic_config(
        &mut self,
        name: &str,
        config: HashMap<String, String>,
    ) -> MetadataResponse {
        if let Some(topic) = self.topics.get_mut(name) {
            topic.config.config.extend(config);
            MetadataResponse::Success
        } else {
            MetadataResponse::Error {
                message: format!("Topic {} not found", name),
            }
        }
    }

    /// Add partitions to a topic
    fn add_partitions(
        &mut self,
        topic: &str,
        new_assignments: Vec<Vec<NodeId>>,
    ) -> MetadataResponse {
        if let Some(topic_state) = self.topics.get_mut(topic) {
            let start_idx = topic_state.partitions.len() as u32;

            for (i, replicas) in new_assignments.into_iter().enumerate() {
                let partition_id = PartitionId::new(topic, start_idx + i as u32);
                let partition_state = PartitionState::new(partition_id, replicas);
                topic_state.partitions.push(partition_state);
            }

            topic_state.config.partitions = topic_state.partitions.len() as u32;
            self.epoch += 1;

            MetadataResponse::Success
        } else {
            MetadataResponse::Error {
                message: format!("Topic {} not found", topic),
            }
        }
    }

    /// Update partition leader
    fn update_partition_leader(
        &mut self,
        partition: &PartitionId,
        leader: NodeId,
        epoch: u64,
    ) -> MetadataResponse {
        if let Some(topic) = self.topics.get_mut(&partition.topic) {
            if let Some(p) = topic.partition_mut(partition.partition) {
                // Only update if epoch is higher
                if epoch > p.leader_epoch {
                    p.leader = Some(leader.clone());
                    p.leader_epoch = epoch;
                    p.online = true;
                    return MetadataResponse::PartitionLeaderUpdated {
                        partition: partition.clone(),
                        leader,
                    };
                }
            }
        }

        MetadataResponse::Error {
            message: format!("Partition {} not found or stale epoch", partition),
        }
    }

    /// Update partition ISR
    fn update_partition_isr(
        &mut self,
        partition: &PartitionId,
        isr: Vec<NodeId>,
    ) -> MetadataResponse {
        if let Some(topic) = self.topics.get_mut(&partition.topic) {
            if let Some(p) = topic.partition_mut(partition.partition) {
                p.isr = isr.iter().cloned().collect();
                p.under_replicated = p.isr.len() < p.replicas.len();

                // Update replica states
                for replica in &mut p.replicas {
                    if isr.contains(&replica.node_id) {
                        replica.state = crate::partition::ReplicaState::InSync;
                    } else {
                        replica.state = crate::partition::ReplicaState::CatchingUp;
                    }
                }

                return MetadataResponse::IsrUpdated {
                    partition: partition.clone(),
                    isr,
                };
            }
        }

        MetadataResponse::Error {
            message: format!("Partition {} not found", partition),
        }
    }

    /// Reassign partition replicas
    fn reassign_partition(
        &mut self,
        partition: &PartitionId,
        replicas: Vec<NodeId>,
    ) -> MetadataResponse {
        if let Some(topic) = self.topics.get_mut(&partition.topic) {
            if let Some(p) = topic.partition_mut(partition.partition) {
                // Create new replica info
                p.replicas = replicas
                    .iter()
                    .map(|n| crate::partition::ReplicaInfo::new(n.clone()))
                    .collect();

                // Reset ISR to just the leader
                if let Some(leader) = &p.leader {
                    p.isr.clear();
                    p.isr.insert(leader.clone());
                }

                p.under_replicated = true;
                self.epoch += 1;

                return MetadataResponse::Success;
            }
        }

        MetadataResponse::Error {
            message: format!("Partition {} not found", partition),
        }
    }

    /// Register a new node
    fn register_node(&mut self, info: NodeInfo) -> MetadataResponse {
        let node_id = info.id.clone();
        self.nodes.insert(node_id.clone(), info);
        self.epoch += 1;
        MetadataResponse::NodeRegistered { node_id }
    }

    /// Deregister a node
    fn deregister_node(&mut self, node_id: &NodeId) -> MetadataResponse {
        if self.nodes.remove(node_id).is_some() {
            self.epoch += 1;
            MetadataResponse::NodeDeregistered {
                node_id: node_id.clone(),
            }
        } else {
            MetadataResponse::Error {
                message: format!("Node {} not found", node_id),
            }
        }
    }

    /// Update node information
    fn update_node(&mut self, node_id: &NodeId, info: NodeInfo) -> MetadataResponse {
        if self.nodes.contains_key(node_id) {
            self.nodes.insert(node_id.clone(), info);
            MetadataResponse::Success
        } else {
            MetadataResponse::Error {
                message: format!("Node {} not found", node_id),
            }
        }
    }

    /// Update cluster configuration
    fn update_cluster_config(&mut self, config: HashMap<String, String>) -> MetadataResponse {
        self.config.extend(config);
        self.epoch += 1;
        MetadataResponse::Success
    }

    /// Update consumer group state
    fn update_consumer_group(&mut self, group: ConsumerGroup) -> MetadataResponse {
        let group_id = group.group_id.clone();
        self.consumer_groups.insert(group_id.clone(), group);
        MetadataResponse::ConsumerGroupUpdated { group_id }
    }

    /// Delete consumer group
    fn delete_consumer_group(&mut self, group_id: &GroupId) -> MetadataResponse {
        if self.consumer_groups.remove(group_id).is_some() {
            MetadataResponse::ConsumerGroupDeleted {
                group_id: group_id.clone(),
            }
        } else {
            MetadataResponse::Error {
                message: format!("Consumer group {} not found", group_id),
            }
        }
    }

    // ==================== Query Methods ====================

    /// Get topic by name
    pub fn get_topic(&self, name: &str) -> Option<&TopicState> {
        self.topics.get(name)
    }

    /// Get partition by ID
    pub fn get_partition(&self, id: &PartitionId) -> Option<&PartitionState> {
        self.topics.get(&id.topic)?.partition(id.partition)
    }

    /// Get node by ID
    pub fn get_node(&self, node_id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(node_id)
    }

    /// Get all topic names
    pub fn topic_names(&self) -> Vec<&str> {
        self.topics.keys().map(|s| s.as_str()).collect()
    }

    /// Get all node IDs
    pub fn node_ids(&self) -> Vec<&NodeId> {
        self.nodes.keys().collect()
    }

    /// Get consumer group by ID
    pub fn get_consumer_group(&self, group_id: &GroupId) -> Option<&ConsumerGroup> {
        self.consumer_groups.get(group_id)
    }

    /// Get all consumer group IDs
    pub fn consumer_group_ids(&self) -> Vec<&GroupId> {
        self.consumer_groups.keys().collect()
    }

    /// Find partition leader
    pub fn find_leader(&self, topic: &str, partition: u32) -> Option<&NodeId> {
        self.topics
            .get(topic)?
            .partition(partition)?
            .leader
            .as_ref()
    }

    /// Get partitions led by a node
    pub fn partitions_led_by(&self, node_id: &NodeId) -> Vec<PartitionId> {
        let mut result = vec![];
        for (topic_name, topic) in &self.topics {
            for (i, partition) in topic.partitions.iter().enumerate() {
                if partition.leader.as_ref() == Some(node_id) {
                    result.push(PartitionId::new(topic_name, i as u32));
                }
            }
        }
        result
    }

    /// Get partitions hosted by a node (leader or replica)
    pub fn partitions_on_node(&self, node_id: &NodeId) -> Vec<PartitionId> {
        let mut result = vec![];
        for (topic_name, topic) in &self.topics {
            for (i, partition) in topic.partitions.iter().enumerate() {
                if partition.is_replica(node_id) {
                    result.push(PartitionId::new(topic_name, i as u32));
                }
            }
        }
        result
    }

    /// Get under-replicated partitions
    pub fn under_replicated_partitions(&self) -> Vec<PartitionId> {
        let mut result = vec![];
        for (topic_name, topic) in &self.topics {
            for (i, partition) in topic.partitions.iter().enumerate() {
                if partition.under_replicated {
                    result.push(PartitionId::new(topic_name, i as u32));
                }
            }
        }
        result
    }

    /// Get offline partitions
    pub fn offline_partitions(&self) -> Vec<PartitionId> {
        let mut result = vec![];
        for (topic_name, topic) in &self.topics {
            for (i, partition) in topic.partitions.iter().enumerate() {
                if !partition.online {
                    result.push(PartitionId::new(topic_name, i as u32));
                }
            }
        }
        result
    }

    /// Serialize for snapshot
    pub fn serialize(&self) -> Result<Vec<u8>> {
        postcard::to_allocvec(self).map_err(|e| ClusterError::Serialization(e.to_string()))
    }

    /// Deserialize from snapshot
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        postcard::from_bytes(data).map_err(|e| ClusterError::Deserialization(e.to_string()))
    }
}

/// Thread-safe metadata store wrapper
pub struct MetadataStore {
    metadata: Arc<RwLock<ClusterMetadata>>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            metadata: Arc::new(RwLock::new(ClusterMetadata::new())),
        }
    }

    pub fn from_metadata(metadata: ClusterMetadata) -> Self {
        Self {
            metadata: Arc::new(RwLock::new(metadata)),
        }
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterMetadata> {
        self.metadata.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, ClusterMetadata> {
        self.metadata.write().await
    }

    pub async fn apply(&self, index: u64, cmd: MetadataCommand) -> MetadataResponse {
        self.metadata.write().await.apply(index, cmd)
    }

    pub async fn get_topic(&self, name: &str) -> Option<TopicState> {
        self.metadata.read().await.topics.get(name).cloned()
    }

    pub async fn get_partition(&self, id: &PartitionId) -> Option<PartitionState> {
        let meta = self.metadata.read().await;
        meta.topics.get(&id.topic)?.partition(id.partition).cloned()
    }

    pub async fn epoch(&self) -> u64 {
        self.metadata.read().await.epoch
    }
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let mut metadata = ClusterMetadata::new();

        let config = TopicConfig::new("test-topic", 3, 2);
        let assignments = vec![
            vec!["node-1".to_string(), "node-2".to_string()],
            vec!["node-2".to_string(), "node-3".to_string()],
            vec!["node-3".to_string(), "node-1".to_string()],
        ];

        let cmd = MetadataCommand::CreateTopic {
            config,
            partition_assignments: assignments,
        };
        let result = metadata.apply(1, cmd);

        match result {
            MetadataResponse::TopicCreated { name, partitions } => {
                assert_eq!(name, "test-topic");
                assert_eq!(partitions, 3);
            }
            _ => panic!("Expected TopicCreated"),
        }

        assert!(metadata.topics.contains_key("test-topic"));
        assert_eq!(metadata.topics["test-topic"].partitions.len(), 3);
    }

    #[test]
    fn test_update_partition_leader() {
        let mut metadata = ClusterMetadata::new();

        // Create topic first
        let config = TopicConfig::new("test", 1, 2);
        let assignments = vec![vec!["node-1".to_string(), "node-2".to_string()]];
        metadata.apply(
            1,
            MetadataCommand::CreateTopic {
                config,
                partition_assignments: assignments,
            },
        );

        // Initial leader was elected in TopicState::new with epoch 1
        // Update leader with higher epoch
        let partition = PartitionId::new("test", 0);
        let result = metadata.apply(
            2,
            MetadataCommand::UpdatePartitionLeader {
                partition: partition.clone(),
                leader: "node-2".to_string(),
                epoch: 2, // Must be higher than current epoch (1)
            },
        );

        assert!(matches!(
            result,
            MetadataResponse::PartitionLeaderUpdated { .. }
        ));
        assert_eq!(
            metadata.topics["test"].partition(0).unwrap().leader,
            Some("node-2".to_string())
        );
    }

    #[test]
    fn test_register_deregister_node() {
        let mut metadata = ClusterMetadata::new();

        let info = NodeInfo::new(
            "node-1",
            "127.0.0.1:9092".parse().unwrap(),
            "127.0.0.1:9093".parse().unwrap(),
        );

        // Register
        let result = metadata.apply(1, MetadataCommand::RegisterNode { info });
        assert!(matches!(result, MetadataResponse::NodeRegistered { .. }));
        assert!(metadata.nodes.contains_key("node-1"));

        // Deregister
        let result = metadata.apply(
            2,
            MetadataCommand::DeregisterNode {
                node_id: "node-1".to_string(),
            },
        );
        assert!(matches!(result, MetadataResponse::NodeDeregistered { .. }));
        assert!(!metadata.nodes.contains_key("node-1"));
    }

    #[test]
    fn test_serialization() {
        let mut metadata = ClusterMetadata::new();
        metadata.apply(
            1,
            MetadataCommand::CreateTopic {
                config: TopicConfig::new("test", 2, 1),
                partition_assignments: vec![vec!["node-1".to_string()], vec!["node-1".to_string()]],
            },
        );

        let bytes = metadata.serialize().unwrap();
        let restored = ClusterMetadata::deserialize(&bytes).unwrap();

        assert_eq!(restored.topics.len(), metadata.topics.len());
        assert_eq!(restored.last_applied_index, metadata.last_applied_index);
    }
}
