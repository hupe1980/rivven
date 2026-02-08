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
use crate::raft::RaftNode;
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

    /// Local metadata store (used in standalone/test mode when no Raft node is wired)
    metadata: Arc<MetadataStore>,

    /// Raft consensus node — when available, ALL state mutations go through Raft
    /// and reads use the Raft state machine's authoritative metadata.
    /// This is `None` only in standalone test scenarios (e.g. unit tests).
    /// Wrapped in Arc<RwLock<Option<...>>> so spawned tasks (e.g. membership event handler)
    /// can access it even when set_raft_node() is called after start().
    raft_node: Arc<RwLock<Option<Arc<RwLock<RaftNode>>>>>,

    /// Cluster membership (SWIM protocol)
    membership: Option<Arc<Membership>>,

    /// Partition placer
    placer: RwLock<PartitionPlacer>,

    /// Replication manager
    replication: Arc<ReplicationManager>,

    /// Current Raft leader (used only when raft_node is None)
    raft_leader: RwLock<Option<NodeId>>,

    /// Whether we are the Raft leader (used only when raft_node is None)
    is_leader_flag: RwLock<bool>,

    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
}

impl ClusterCoordinator {
    /// Create a new cluster coordinator
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        let local_node = NodeInfo::new(&config.node_id, config.client_addr, config.cluster_addr);

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
            raft_node: Arc::new(RwLock::new(None)),
            membership: None,
            placer: RwLock::new(placer),
            replication,
            raft_leader: RwLock::new(None),
            is_leader_flag: RwLock::new(false),
            shutdown_tx,
        })
    }

    /// Create standalone coordinator (no clustering)
    pub async fn standalone(config: ClusterConfig) -> Result<Self> {
        let coordinator = Self::new(config).await?;

        // In standalone mode, we are always the leader
        *coordinator.state.write().await = CoordinatorState::Leader;
        *coordinator.is_leader_flag.write().await = true;

        // Register ourselves in metadata
        let local_node = coordinator.local_node.clone();
        coordinator
            .metadata
            .apply(
                0,
                MetadataCommand::RegisterNode {
                    info: local_node.clone(),
                },
            )
            .await;

        // Also add ourselves to the placer
        {
            let mut placer = coordinator.placer.write().await;
            let mut node = crate::node::Node::new(local_node);
            node.mark_alive(1); // Initial incarnation
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
        self.metadata
            .apply(
                0,
                MetadataCommand::RegisterNode {
                    info: self.local_node.clone(),
                },
            )
            .await;

        // Add ourselves to the placer
        {
            let mut placer = self.placer.write().await;
            let mut node = crate::node::Node::new(self.local_node.clone());
            node.mark_alive(1); // Initial incarnation
            placer.add_node(&node);
        }

        *self.state.write().await = CoordinatorState::Leader;
        *self.is_leader_flag.write().await = true;

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
        )
        .await?;

        // Subscribe to membership events
        let mut events = membership.subscribe();
        let metadata = self.metadata.clone();
        let node_id = self.config.node_id.clone();
        let raft_node = self.raft_node.clone();

        // Spawn membership event handler
        tokio::spawn(async move {
            while let Ok(event) = events.recv().await {
                Self::handle_membership_event(&metadata, &node_id, event, &raft_node).await;
            }
        });

        // Join cluster if we have seeds
        if !self.config.seeds.is_empty() {
            membership.join(&self.config.seeds).await?;
        }

        self.membership = Some(Arc::new(membership));
        *self.state.write().await = CoordinatorState::Follower;

        // Raft consensus is managed by RaftNode which is wired in via set_raft_node().
        // The coordinator checks RaftNode.is_leader() for authoritative leadership.
        // For the initial cluster bootstrap, the first node (no seeds) starts as leader.
        if self.config.seeds.is_empty() {
            *self.state.write().await = CoordinatorState::Leader;
            *self.is_leader_flag.write().await = true;
            info!(node_id = %self.config.node_id, "Started as cluster leader (first node)");
        } else {
            info!(node_id = %self.config.node_id, "Started as cluster follower");
        }

        Ok(())
    }

    /// Handle membership events.
    ///
    /// When Raft is wired, proposes RegisterNode/DeregisterNode through Raft consensus
    /// for replicated consistency. Falls back to local MetadataStore hints when Raft
    /// is not yet available (e.g. during bootstrap before set_raft_node() is called).
    async fn handle_membership_event(
        metadata: &MetadataStore,
        _local_node_id: &str,
        event: MembershipEvent,
        raft_node: &RwLock<Option<Arc<RwLock<RaftNode>>>>,
    ) {
        match event {
            MembershipEvent::NodeJoined(info) => {
                info!(node_id = %info.id, "Node joined cluster");
                let cmd = MetadataCommand::RegisterNode { info };
                Self::apply_membership_command(metadata, cmd, raft_node).await;
            }
            MembershipEvent::NodeLeft(node_id) => {
                info!(node_id = %node_id, "Node left cluster gracefully");
                let cmd = MetadataCommand::DeregisterNode { node_id };
                Self::apply_membership_command(metadata, cmd, raft_node).await;
            }
            MembershipEvent::NodeFailed(node_id) => {
                warn!(node_id = %node_id, "Node failed");
                let cmd = MetadataCommand::DeregisterNode { node_id };
                Self::apply_membership_command(metadata, cmd, raft_node).await;
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

    /// Apply a membership command: through Raft if available (leader only proposes),
    /// otherwise apply locally as a hint.
    async fn apply_membership_command(
        metadata: &MetadataStore,
        cmd: MetadataCommand,
        raft_node: &RwLock<Option<Arc<RwLock<RaftNode>>>>,
    ) {
        let raft_guard = raft_node.read().await;
        if let Some(ref raft) = *raft_guard {
            let raft_lock = raft.read().await;
            // Only the leader proposes through Raft to avoid duplicate proposals
            if raft_lock.is_leader() {
                if let Err(e) = raft_lock.propose(cmd).await {
                    warn!("Failed to propose membership command through Raft: {e}");
                }
                return;
            }
            // Followers: apply locally as hint for routing.
            // The leader's Raft proposal will eventually replicate to all nodes.
            drop(raft_lock);
            drop(raft_guard);
            metadata.apply(0, cmd).await;
        } else {
            // No Raft node yet (bootstrap phase): apply locally
            drop(raft_guard);
            metadata.apply(0, cmd).await;
        }
    }

    /// Wire a Raft consensus node into this coordinator.
    ///
    /// When set, all state mutations (create_topic, delete_topic, elect leader) go through
    /// Raft consensus for replicated consistency. Reads use the Raft state machine's
    /// authoritative metadata. Without a Raft node, the coordinator falls back to the
    /// local MetadataStore (standalone/test mode).
    pub fn set_raft_node(&mut self, raft_node: Arc<RwLock<RaftNode>>) {
        // Update the shared raft_node so all spawned tasks (membership handler, etc.)
        // immediately start routing mutations through Raft consensus.
        // Note: we use try_write() to avoid blocking; falling back to blocking write
        // since set_raft_node is called once during initialization.
        let mut guard = self
            .raft_node
            .try_write()
            .expect("raft_node lock not contended during init");
        *guard = Some(raft_node);
    }

    /// Apply a metadata command through the appropriate channel.
    ///
    /// When Raft is wired, uses Raft consensus for replicated consistency.
    /// Falls back to local metadata store for standalone/test usage.
    async fn apply_command(&self, cmd: MetadataCommand) -> Result<()> {
        let raft_guard = self.raft_node.read().await;
        if let Some(ref raft_node) = *raft_guard {
            let raft = raft_node.read().await;
            raft.propose(cmd).await?;
        } else {
            drop(raft_guard);
            self.metadata.apply(0, cmd).await;
        }
        Ok(())
    }

    /// Execute a closure with read access to the current cluster metadata.
    ///
    /// Routes to Raft state machine when available for authoritative reads,
    /// otherwise uses the local metadata store.
    async fn with_metadata<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::metadata::ClusterMetadata) -> R,
    {
        let raft_guard = self.raft_node.read().await;
        if let Some(ref raft_node) = *raft_guard {
            let raft = raft_node.read().await;
            let meta = raft.metadata().await;
            f(&meta)
        } else {
            drop(raft_guard);
            let meta = self.metadata.read().await;
            f(&meta)
        }
    }

    /// Check if we are the cluster leader.
    ///
    /// When Raft is wired, checks the Raft node's authoritative leadership state.
    /// Otherwise falls back to the local is_leader flag.
    async fn check_is_leader(&self) -> bool {
        let raft_guard = self.raft_node.read().await;
        if let Some(ref raft_node) = *raft_guard {
            let raft = raft_node.read().await;
            raft.is_leader()
        } else {
            drop(raft_guard);
            *self.is_leader_flag.read().await
        }
    }

    /// Get current Raft leader node ID.
    async fn current_leader(&self) -> Option<NodeId> {
        let raft_guard = self.raft_node.read().await;
        if let Some(ref raft_node) = *raft_guard {
            let raft = raft_node.read().await;
            raft.leader().map(|leader_id| {
                if leader_id == raft.node_id() {
                    raft.node_id_str().to_string()
                } else {
                    leader_id.to_string()
                }
            })
        } else {
            drop(raft_guard);
            self.raft_leader.read().await.clone()
        }
    }

    /// Create a new topic
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        if !self.check_is_leader().await {
            return Err(ClusterError::NotLeader {
                leader: self.current_leader().await,
            });
        }

        // Check if topic already exists
        let exists = self
            .with_metadata(|meta| meta.topics.contains_key(&config.name))
            .await;
        if exists {
            return Err(ClusterError::TopicAlreadyExists(config.name));
        }

        // Calculate partition assignments
        let placer = self.placer.read().await;
        let mut assignments = Vec::with_capacity(config.partitions as usize);

        for partition in 0..config.partitions {
            let replicas =
                placer.assign_partition(&config.name, partition, config.replication_factor)?;
            assignments.push(replicas);
        }

        drop(placer);

        // Apply via Raft consensus (or local metadata in standalone)
        let cmd = MetadataCommand::CreateTopic {
            config,
            partition_assignments: assignments,
        };

        self.apply_command(cmd).await?;

        Ok(())
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        if !self.check_is_leader().await {
            return Err(ClusterError::NotLeader {
                leader: self.current_leader().await,
            });
        }

        // Check topic exists
        let exists = self
            .with_metadata(|meta| meta.topics.contains_key(name))
            .await;
        if !exists {
            return Err(ClusterError::TopicNotFound(name.to_string()));
        }

        let cmd = MetadataCommand::DeleteTopic {
            name: name.to_string(),
        };
        self.apply_command(cmd).await?;

        Ok(())
    }

    /// Get partition leader
    pub async fn get_partition_leader(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<Option<NodeId>> {
        let leader = self
            .with_metadata(|meta| meta.find_leader(topic, partition).cloned())
            .await;
        Ok(leader)
    }

    /// Trigger leader election for a partition
    pub async fn elect_partition_leader(&self, partition_id: &PartitionId) -> Result<NodeId> {
        if !self.check_is_leader().await {
            return Err(ClusterError::NotLeader {
                leader: self.current_leader().await,
            });
        }

        let partition = self
            .with_metadata(|meta| {
                meta.topics
                    .get(&partition_id.topic)
                    .and_then(|t| t.partition(partition_id.partition).cloned())
            })
            .await
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: partition_id.topic.clone(),
                partition: partition_id.partition,
            })?;

        // Deterministic election: pick the lexicographically smallest ISR member
        // so all nodes agree on the same leader for the same ISR state.
        let mut sorted_isr: Vec<_> = partition.isr.iter().cloned().collect();
        sorted_isr.sort();
        let new_leader = sorted_isr
            .into_iter()
            .next()
            .ok_or(ClusterError::NotEnoughIsr {
                required: 1,
                current: 0,
            })?;

        let cmd = MetadataCommand::UpdatePartitionLeader {
            partition: partition_id.clone(),
            leader: new_leader.clone(),
            epoch: partition.leader_epoch + 1,
        };

        self.apply_command(cmd).await?;

        Ok(new_leader)
    }

    /// Get current coordinator state
    pub async fn state(&self) -> CoordinatorState {
        *self.state.read().await
    }

    /// Check if we are the cluster leader (public API)
    pub async fn is_leader(&self) -> bool {
        self.check_is_leader().await
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
        let is_leader = self.check_is_leader().await;

        let (node_count, healthy_nodes) = if let Some(membership) = &self.membership {
            (membership.member_count(), membership.healthy_count())
        } else {
            (1, 1) // Standalone
        };

        let (topic_count, partition_count, offline_partitions, under_replicated) = self
            .with_metadata(|meta| {
                let tc = meta.topics.len();
                let pc: usize = meta.topics.values().map(|t| t.partitions.len()).sum();
                let op = meta.offline_partitions().len();
                let ur = meta.under_replicated_partitions().len();
                (tc, pc, op, ur)
            })
            .await;

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
        let partition_count = self
            .with_metadata(|meta| {
                meta.topics
                    .get(topic)
                    .map(|t| t.partitions.len())
                    .unwrap_or(0)
            })
            .await;

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
        self.with_metadata(|meta| meta.find_leader(topic, partition).cloned())
            .await
    }

    /// Check if a node is in the ISR for a partition
    pub async fn is_in_isr(&self, topic: &str, partition: u32, node_id: &str) -> bool {
        self.with_metadata(|meta| {
            meta.topics
                .get(topic)
                .and_then(|t| t.partition(partition))
                .map(|p| p.isr.contains(node_id))
                .unwrap_or(false)
        })
        .await
    }

    /// Get any ISR member for a partition (for read routing)
    pub async fn get_isr_member(&self, topic: &str, partition: u32) -> Option<String> {
        self.with_metadata(|meta| {
            meta.topics
                .get(topic)
                .and_then(|t| t.partition(partition))
                .and_then(|p| p.isr.iter().next().cloned())
        })
        .await
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
        matches!(
            self.state,
            CoordinatorState::Leader | CoordinatorState::Follower
        ) && self.healthy_nodes > 0
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
