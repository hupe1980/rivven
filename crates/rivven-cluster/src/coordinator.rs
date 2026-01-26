//! Cluster coordinator - orchestrates all cluster components
//!
//! The ClusterCoordinator is the main entry point for cluster operations.
//! It manages:
//! - Node lifecycle (join, leave, failure)
//! - Metadata consensus (Raft)
//! - Partition placement and rebalancing
//! - Leader election for partitions

use crate::config::{ClusterConfig, ClusterMode};
use crate::error::{ClusterError, Result};
use crate::membership::{Membership, MembershipEvent};
use crate::metadata::{MetadataCommand, MetadataStore};
use crate::node::{NodeId, NodeInfo};
use crate::partition::{PartitionId, TopicConfig};
use crate::placement::{PartitionPlacer, PlacementConfig};
use crate::replication::ReplicationManager;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

/// Cluster coordinator state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorState {
    /// Starting up
    Starting,
    /// Joining cluster
    Joining,
    /// Running as follower
    Follower,
    /// Running as leader (controller)
    Leader,
    /// Shutting down gracefully
    Leaving,
    /// Shutdown complete
    Stopped,
}

/// Cluster coordinator manages all cluster operations
pub struct ClusterCoordinator {
    /// Cluster configuration
    config: ClusterConfig,
    
    /// Our node information
    local_node: NodeInfo,
    
    /// Current coordinator state
    state: RwLock<CoordinatorState>,
    
    /// Metadata store (Raft-replicated)
    metadata: Arc<MetadataStore>,
    
    /// Cluster membership (SWIM protocol)
    membership: Option<Arc<Membership>>,
    
    /// Partition placer
    placer: RwLock<PartitionPlacer>,
    
    /// Replication manager
    replication: Arc<ReplicationManager>,
    
    /// Current Raft leader
    raft_leader: RwLock<Option<NodeId>>,
    
    /// Whether we are the Raft leader
    is_leader: RwLock<bool>,
    
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        let local_node = NodeInfo::new(
            &config.node_id,
            config.client_addr,
            config.cluster_addr,
        );
        
        let local_node = if let Some(rack) = &config.rack {
            local_node.with_rack(rack)
        } else {
            local_node
        };
        
        let metadata = Arc::new(MetadataStore::new());
        let replication = Arc::new(ReplicationManager::new(
            config.node_id.clone(),
            config.replication.clone(),
        ));
        
        let placer = PartitionPlacer::new(PlacementConfig {
            rack_aware: config.rack.is_some(),
            ..Default::default()
        });
        
        let (shutdown_tx, _) = broadcast::channel(1);
        
        Ok(Self {
            config,
            local_node,
            state: RwLock::new(CoordinatorState::Starting),
            metadata,
            membership: None,
            placer: RwLock::new(placer),
            replication,
            raft_leader: RwLock::new(None),
            is_leader: RwLock::new(false),
            shutdown_tx,
        })
    }
    
    /// Create standalone coordinator (no clustering)
    pub async fn standalone(config: ClusterConfig) -> Result<Self> {
        let coordinator = Self::new(config).await?;
        
        // In standalone mode, we are always the leader
        *coordinator.state.write().await = CoordinatorState::Leader;
        *coordinator.is_leader.write().await = true;
        
        // Register ourselves in metadata
        let local_node = coordinator.local_node.clone();
        coordinator.metadata.apply(
            0,
            MetadataCommand::RegisterNode { info: local_node.clone() },
        ).await;
        
        // Also add ourselves to the placer
        {
            let mut placer = coordinator.placer.write().await;
            let mut node = crate::node::Node::new(local_node);
            node.mark_alive(1);  // Initial incarnation
            placer.add_node(&node);
        }
        
        info!("Coordinator running in standalone mode");
        Ok(coordinator)
    }
    
    /// Start the coordinator
    pub async fn start(&mut self) -> Result<()> {
        *self.state.write().await = CoordinatorState::Starting;
        
        match self.config.mode {
            ClusterMode::Standalone => {
                self.start_standalone().await?;
            }
            ClusterMode::Cluster => {
                self.start_cluster().await?;
            }
        }
        
        Ok(())
    }
    
    /// Start in standalone mode
    async fn start_standalone(&mut self) -> Result<()> {
        // Register ourselves
        self.metadata.apply(
            0,
            MetadataCommand::RegisterNode { info: self.local_node.clone() },
        ).await;
        
        // Add ourselves to the placer
        {
            let mut placer = self.placer.write().await;
            let mut node = crate::node::Node::new(self.local_node.clone());
            node.mark_alive(1);  // Initial incarnation
            placer.add_node(&node);
        }
        
        *self.state.write().await = CoordinatorState::Leader;
        *self.is_leader.write().await = true;
        
        info!(node_id = %self.config.node_id, "Started in standalone mode");
        Ok(())
    }
    
    /// Start in cluster mode
    async fn start_cluster(&mut self) -> Result<()> {
        *self.state.write().await = CoordinatorState::Joining;
        
        // Initialize SWIM membership
        let membership = Membership::new(
            self.local_node.clone(),
            self.config.swim.clone(),
            self.shutdown_tx.subscribe(),
        ).await?;
        
        // Subscribe to membership events
        let mut events = membership.subscribe();
        let metadata = self.metadata.clone();
        let node_id = self.config.node_id.clone();
        
        // Spawn membership event handler
        tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                Self::handle_membership_event(&metadata, &node_id, event).await;
            }
        });
        
        // Join cluster if we have seeds
        if !self.config.seeds.is_empty() {
            membership.join(&self.config.seeds).await?;
        }
        
        self.membership = Some(Arc::new(membership));
        *self.state.write().await = CoordinatorState::Follower;
        
        // Raft consensus is managed by RaftNode which is started separately.
        // The coordinator tracks local leadership state based on SWIM membership.
        // For the initial cluster bootstrap, the first node (no seeds) becomes leader.
        // In steady state, leadership is determined by Raft consensus in RaftNode.
        if self.config.seeds.is_empty() {
            *self.state.write().await = CoordinatorState::Leader;
            *self.is_leader.write().await = true;
            info!(node_id = %self.config.node_id, "Started as cluster leader (first node)");
        } else {
            info!(node_id = %self.config.node_id, "Started as cluster follower");
        }
        
        Ok(())
    }
    
    /// Handle membership events
    async fn handle_membership_event(
        metadata: &MetadataStore,
        _local_node_id: &str,
        event: MembershipEvent,
    ) {
        // Note: These are local metadata updates triggered by SWIM membership events.
        // We use index 0 to indicate "locally applied, not Raft-replicated" changes.
        // These are hints for routing - actual authoritative state comes from Raft.
        // The Raft consensus layer will replicate proper node registrations with
        // actual log indices during normal cluster operation.
        match event {
            MembershipEvent::NodeJoined(info) => {
                info!(node_id = %info.id, "Node joined cluster");
                // Apply local hint for immediate routing (Raft will replicate authoritatively)
                metadata.apply(
                    0, // Local hint index - Raft replication uses proper indices
                    MetadataCommand::RegisterNode { info },
                ).await;
            }
            MembershipEvent::NodeLeft(node_id) => {
                info!(node_id = %node_id, "Node left cluster gracefully");
                metadata.apply(
                    0, // Local hint index
                    MetadataCommand::DeregisterNode { node_id },
                ).await;
            }
            MembershipEvent::NodeFailed(node_id) => {
                warn!(node_id = %node_id, "Node failed");
                // Trigger partition reassignment for failed node
                metadata.apply(
                    0, // Local hint index
                    MetadataCommand::DeregisterNode { node_id },
                ).await;
            }
            MembershipEvent::NodeSuspected(node_id) => {
                debug!(node_id = %node_id, "Node suspected");
            }
            MembershipEvent::NodeRecovered(node_id) => {
                info!(node_id = %node_id, "Node recovered");
            }
            MembershipEvent::NodeStateChanged { node_id, old, new } => {
                debug!(node_id = %node_id, ?old, ?new, "Node state changed");
            }
        }
    }
    
    /// Create a new topic
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        if !*self.is_leader.read().await {
            return Err(ClusterError::NotLeader {
                leader: self.raft_leader.read().await.clone(),
            });
        }
        
        // Check if topic already exists
        if self.metadata.get_topic(&config.name).await.is_some() {
            return Err(ClusterError::TopicAlreadyExists(config.name));
        }
        
        // Calculate partition assignments
        let placer = self.placer.read().await;
        let mut assignments = Vec::with_capacity(config.partitions as usize);
        
        for partition in 0..config.partitions {
            let replicas = placer.assign_partition(
                &config.name,
                partition,
                config.replication_factor,
            )?;
            assignments.push(replicas);
        }
        
        drop(placer);
        
        // Apply via Raft
        let cmd = MetadataCommand::CreateTopic {
            config,
            partition_assignments: assignments,
        };
        
        self.metadata.apply(0, cmd).await;
        
        Ok(())
    }
    
    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        if !*self.is_leader.read().await {
            return Err(ClusterError::NotLeader {
                leader: self.raft_leader.read().await.clone(),
            });
        }
        
        // Check topic exists
        if self.metadata.get_topic(name).await.is_none() {
            return Err(ClusterError::TopicNotFound(name.to_string()));
        }
        
        let cmd = MetadataCommand::DeleteTopic { name: name.to_string() };
        self.metadata.apply(0, cmd).await;
        
        Ok(())
    }
    
    /// Get partition leader
    pub async fn get_partition_leader(&self, topic: &str, partition: u32) -> Result<Option<NodeId>> {
        let meta = self.metadata.read().await;
        let leader = meta.find_leader(topic, partition).cloned();
        Ok(leader)
    }
    
    /// Trigger leader election for a partition
    pub async fn elect_partition_leader(&self, partition_id: &PartitionId) -> Result<NodeId> {
        if !*self.is_leader.read().await {
            return Err(ClusterError::NotLeader {
                leader: self.raft_leader.read().await.clone(),
            });
        }
        
        let partition = self.metadata.get_partition(partition_id).await
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: partition_id.topic.clone(),
                partition: partition_id.partition,
            })?;
        
        // Elect from ISR
        let new_leader = partition.isr.iter().next()
            .ok_or(ClusterError::NotEnoughIsr {
                required: 1,
                current: 0,
            })?;
        
        let cmd = MetadataCommand::UpdatePartitionLeader {
            partition: partition_id.clone(),
            leader: new_leader.clone(),
            epoch: partition.leader_epoch + 1,
        };
        
        self.metadata.apply(0, cmd).await;
        
        Ok(new_leader.clone())
    }
    
    /// Get current coordinator state
    pub async fn state(&self) -> CoordinatorState {
        *self.state.read().await
    }
    
    /// Check if we are the cluster leader
    pub async fn is_leader(&self) -> bool {
        *self.is_leader.read().await
    }
    
    /// Get metadata store
    pub fn metadata(&self) -> &Arc<MetadataStore> {
        &self.metadata
    }
    
    /// Get replication manager
    pub fn replication(&self) -> &Arc<ReplicationManager> {
        &self.replication
    }
    
    /// Get local node info
    pub fn local_node(&self) -> &NodeInfo {
        &self.local_node
    }
    
    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        *self.state.write().await = CoordinatorState::Leaving;
        
        // Notify cluster we're leaving
        if let Some(membership) = &self.membership {
            membership.leave().await?;
        }
        
        // Signal all tasks to shutdown
        let _ = self.shutdown_tx.send(());
        
        *self.state.write().await = CoordinatorState::Stopped;
        info!(node_id = %self.config.node_id, "Coordinator shutdown complete");
        
        Ok(())
    }
    
    /// Get cluster health status
    pub async fn health(&self) -> ClusterHealth {
        let state = *self.state.read().await;
        let is_leader = *self.is_leader.read().await;
        
        let (node_count, healthy_nodes) = if let Some(membership) = &self.membership {
            (membership.member_count(), membership.healthy_count())
        } else {
            (1, 1) // Standalone
        };
        
        let meta = self.metadata.read().await;
        let topic_count = meta.topics.len();
        let partition_count: usize = meta.topics.values()
            .map(|t| t.partitions.len())
            .sum();
        let offline_partitions = meta.offline_partitions().len();
        let under_replicated = meta.under_replicated_partitions().len();
        
        ClusterHealth {
            state,
            is_leader,
            node_count,
            healthy_nodes,
            topic_count,
            partition_count,
            offline_partitions,
            under_replicated_partitions: under_replicated,
        }
    }
    
    // ========== Routing Helper Methods ==========
    
    /// Select a partition for a message (used when partition not specified)
    /// 
    /// If key is provided, uses consistent hashing
    /// Otherwise, uses round-robin across partitions
    pub async fn select_partition(&self, topic: &str, key: Option<&[u8]>) -> Option<u32> {
        let meta = self.metadata.read().await;
        let topic_state = meta.topics.get(topic)?;
        let partition_count = topic_state.partitions.len();
        
        if partition_count == 0 {
            return None;
        }
        
        if let Some(key) = key {
            // Consistent hashing based on key
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            let hash = hasher.finish();
            Some((hash % partition_count as u64) as u32)
        } else {
            // Simple round-robin (uses modulo based on current time for distribution)
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            Some((now % partition_count as u128) as u32)
        }
    }
    
    /// Get the leader for a partition (async wrapper for routing)
    pub async fn partition_leader(&self, topic: &str, partition: u32) -> Option<String> {
        let meta = self.metadata.read().await;
        meta.find_leader(topic, partition).cloned()
    }
    
    /// Check if a node is in the ISR for a partition
    pub async fn is_in_isr(&self, topic: &str, partition: u32, node_id: &str) -> bool {
        let meta = self.metadata.read().await;
        
        if let Some(topic_state) = meta.topics.get(topic) {
            if let Some(partition_state) = topic_state.partition(partition) {
                return partition_state.isr.contains(node_id);
            }
        }
        false
    }
    
    /// Get any ISR member for a partition (for read routing)
    pub async fn get_isr_member(&self, topic: &str, partition: u32) -> Option<String> {
        let meta = self.metadata.read().await;
        
        meta.topics.get(topic)
            .and_then(|t| t.partition(partition))
            .and_then(|p| p.isr.iter().next().cloned())
    }
}

/// Cluster health information
#[derive(Debug, Clone)]
pub struct ClusterHealth {
    pub state: CoordinatorState,
    pub is_leader: bool,
    pub node_count: usize,
    pub healthy_nodes: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub offline_partitions: usize,
    pub under_replicated_partitions: usize,
}

impl ClusterHealth {
    /// Check if cluster is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.state, CoordinatorState::Leader | CoordinatorState::Follower)
            && self.healthy_nodes > 0
            && self.offline_partitions == 0
    }
    
    /// Get health status string
    pub fn status(&self) -> &'static str {
        if self.is_healthy() {
            if self.under_replicated_partitions > 0 {
                "degraded"
            } else {
                "healthy"
            }
        } else {
            "unhealthy"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_standalone_coordinator() {
        let config = ClusterConfig::standalone();
        let coordinator = ClusterCoordinator::standalone(config).await.unwrap();
        
        assert!(coordinator.is_leader().await);
        assert_eq!(coordinator.state().await, CoordinatorState::Leader);
        
        let health = coordinator.health().await;
        assert!(health.is_healthy());
        assert_eq!(health.node_count, 1);
    }
    
    #[tokio::test]
    async fn test_create_topic_standalone() {
        let config = ClusterConfig::standalone();
        let coordinator = ClusterCoordinator::standalone(config).await.unwrap();
        
        let topic_config = TopicConfig::new("test-topic", 3, 1);
        coordinator.create_topic(topic_config).await.unwrap();
        
        // Verify topic was created
        let topic = coordinator.metadata().get_topic("test-topic").await;
        assert!(topic.is_some());
        assert_eq!(topic.unwrap().partitions.len(), 3);
    }
}
