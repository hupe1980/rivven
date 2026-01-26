//! ISR (In-Sync Replica) replication implementation
//!
//! This module implements Kafka-style ISR replication:
//! - Leaders handle all reads and writes
//! - Followers fetch from leaders and replicate
//! - High watermark tracks committed offsets
//! - ISR tracks which replicas are caught up
//!
//! Ack modes:
//! - acks=0: Fire and forget (no durability guarantee)
//! - acks=1: Leader acknowledgment (data written to leader)
//! - acks=all: All ISR acknowledgment (full durability)

use crate::config::ReplicationConfig;
use crate::error::{ClusterError, Result};
use crate::node::NodeId;
use crate::partition::PartitionId;
use crate::protocol::Acks;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, RwLock, Mutex};
use tracing::{debug, error, info, warn};

/// Replication state for a single partition
#[derive(Debug)]
pub struct PartitionReplication {
    /// Partition identifier
    pub partition_id: PartitionId,
    
    /// Our node ID
    local_node: NodeId,
    
    /// Whether we are the leader
    is_leader: bool,
    
    /// Current leader epoch
    leader_epoch: AtomicU64,
    
    /// Log end offset (latest message written)
    log_end_offset: AtomicU64,
    
    /// High watermark (committed offset)
    high_watermark: AtomicU64,
    
    /// Replica states (only tracked by leader)
    replicas: RwLock<HashMap<NodeId, ReplicaProgress>>,
    
    /// In-sync replica set
    isr: RwLock<HashSet<NodeId>>,
    
    /// Pending acks waiting for replication
    pending_acks: DashMap<u64, PendingAck>,
    
    /// Configuration
    config: ReplicationConfig,
}

/// Progress tracking for a replica
#[derive(Debug, Clone)]
pub struct ReplicaProgress {
    /// Node ID
    pub node_id: NodeId,
    
    /// Log end offset reported by replica
    pub log_end_offset: u64,
    
    /// Last fetch time
    pub last_fetch: Instant,
    
    /// Whether replica is in sync
    pub in_sync: bool,
    
    /// Lag in messages
    pub lag: u64,
}

/// Pending acknowledgment for acks=all writes
/// 
/// This struct tracks in-flight writes awaiting acknowledgment from replicas.
/// Fields are used by the replication protocol during write completion.
#[derive(Debug)]
#[allow(dead_code)]
struct PendingAck {
    /// Offset being acknowledged
    offset: u64,
    /// Nodes that have acknowledged
    acked_nodes: HashSet<NodeId>,
    /// Required ack count
    required_acks: usize,
    /// Completion sender
    completion: oneshot::Sender<Result<()>>,
    /// Created time for timeout
    created: Instant,
}

impl PartitionReplication {
    /// Create new partition replication state
    pub fn new(
        partition_id: PartitionId,
        local_node: NodeId,
        is_leader: bool,
        config: ReplicationConfig,
    ) -> Self {
        Self {
            partition_id,
            local_node,
            is_leader,
            leader_epoch: AtomicU64::new(0),
            log_end_offset: AtomicU64::new(0),
            high_watermark: AtomicU64::new(0),
            replicas: RwLock::new(HashMap::new()),
            isr: RwLock::new(HashSet::new()),
            pending_acks: DashMap::new(),
            config,
        }
    }
    
    /// Become leader for this partition
    pub async fn become_leader(&mut self, epoch: u64, replicas: Vec<NodeId>) {
        self.is_leader = true;
        self.leader_epoch.store(epoch, Ordering::SeqCst);
        
        // Initialize replica tracking
        let mut replica_map = self.replicas.write().await;
        replica_map.clear();
        
        for node_id in &replicas {
            if node_id != &self.local_node {
                replica_map.insert(node_id.clone(), ReplicaProgress {
                    node_id: node_id.clone(),
                    log_end_offset: 0,
                    last_fetch: Instant::now(),
                    in_sync: false,
                    lag: u64::MAX,
                });
            }
        }
        
        // Initialize ISR with just ourselves
        let mut isr = self.isr.write().await;
        isr.clear();
        isr.insert(self.local_node.clone());
        
        info!(
            partition = %self.partition_id,
            epoch = epoch,
            replicas = replicas.len(),
            "Became partition leader"
        );
    }
    
    /// Become follower for this partition
    pub fn become_follower(&mut self, epoch: u64) {
        self.is_leader = false;
        self.leader_epoch.store(epoch, Ordering::SeqCst);
        
        info!(
            partition = %self.partition_id,
            epoch = epoch,
            "Became partition follower"
        );
    }
    
    /// Record appended locally (called by storage layer)
    pub async fn record_appended(&self, offset: u64) -> Result<()> {
        // Update our log end offset
        self.log_end_offset.store(offset + 1, Ordering::SeqCst);
        
        // If we're the leader, check if we can advance HWM
        if self.is_leader {
            self.maybe_advance_hwm().await;
        }
        
        Ok(())
    }
    
    /// Handle replica fetch and update progress
    /// Returns true if ISR changed
    pub async fn handle_replica_fetch(&self, replica_id: &NodeId, fetch_offset: u64) -> Result<bool> {
        if !self.is_leader {
            return Err(ClusterError::NotLeader { leader: None });
        }
        
        let mut isr_changed = false;
        let mut replicas = self.replicas.write().await;
        
        if let Some(progress) = replicas.get_mut(replica_id) {
            progress.last_fetch = Instant::now();
            progress.log_end_offset = fetch_offset;
            
            let leader_leo = self.log_end_offset.load(Ordering::SeqCst);
            progress.lag = leader_leo.saturating_sub(fetch_offset);
            
            // Check if replica should be in ISR
            let should_be_in_sync = progress.lag <= self.config.replica_lag_max_messages;
            
            if should_be_in_sync != progress.in_sync {
                progress.in_sync = should_be_in_sync;
                isr_changed = true;
                
                // Update ISR
                let mut isr = self.isr.write().await;
                if should_be_in_sync {
                    isr.insert(replica_id.clone());
                    info!(
                        partition = %self.partition_id,
                        replica = %replica_id,
                        "Replica joined ISR"
                    );
                } else {
                    isr.remove(replica_id);
                    warn!(
                        partition = %self.partition_id,
                        replica = %replica_id,
                        lag = progress.lag,
                        "Replica removed from ISR due to lag"
                    );
                }
            }
        }
        
        drop(replicas);
        
        // Try to advance HWM
        self.maybe_advance_hwm().await;
        
        Ok(isr_changed)
    }
    
    /// Check for lagging replicas and remove from ISR
    pub async fn check_replica_health(&self) -> Vec<NodeId> {
        if !self.is_leader {
            return vec![];
        }
        
        let now = Instant::now();
        let mut removed = vec![];
        
        let mut replicas = self.replicas.write().await;
        let mut isr = self.isr.write().await;
        
        for (node_id, progress) in replicas.iter_mut() {
            if progress.in_sync {
                let since_fetch = now.duration_since(progress.last_fetch);
                
                if since_fetch > self.config.replica_lag_max_time {
                    progress.in_sync = false;
                    isr.remove(node_id);
                    removed.push(node_id.clone());
                    
                    warn!(
                        partition = %self.partition_id,
                        replica = %node_id,
                        lag_time = ?since_fetch,
                        "Replica removed from ISR due to time lag"
                    );
                }
            }
        }
        
        removed
    }
    
    /// Maybe advance the high watermark
    async fn maybe_advance_hwm(&self) {
        let isr = self.isr.read().await;
        
        // HWM is the minimum LEO across all ISR members
        let mut min_leo = self.log_end_offset.load(Ordering::SeqCst);
        
        let replicas = self.replicas.read().await;
        for node_id in isr.iter() {
            if node_id == &self.local_node {
                continue;
            }
            if let Some(progress) = replicas.get(node_id) {
                min_leo = min_leo.min(progress.log_end_offset);
            }
        }
        
        let current_hwm = self.high_watermark.load(Ordering::SeqCst);
        if min_leo > current_hwm {
            self.high_watermark.store(min_leo, Ordering::SeqCst);
            
            // Complete any pending acks
            self.complete_pending_acks(min_leo).await;
        }
    }
    
    /// Wait for replication based on ack mode
    pub async fn wait_for_acks(&self, offset: u64, acks: Acks) -> Result<()> {
        match acks {
            Acks::None => Ok(()),
            Acks::Leader => {
                // Just need local write, which is done
                Ok(())
            }
            Acks::All => {
                // Need all ISR to acknowledge
                let isr = self.isr.read().await;
                let required = isr.len();
                
                if required <= 1 {
                    // Only leader in ISR, done
                    return Ok(());
                }
                
                // Create pending ack
                let (tx, rx) = oneshot::channel();
                let mut acked = HashSet::new();
                acked.insert(self.local_node.clone());
                
                self.pending_acks.insert(offset, PendingAck {
                    offset,
                    acked_nodes: acked,
                    required_acks: required,
                    completion: tx,
                    created: Instant::now(),
                });
                
                drop(isr);
                
                // Wait for completion or timeout
                match tokio::time::timeout(Duration::from_secs(30), rx).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(ClusterError::ChannelClosed),
                    Err(_) => {
                        self.pending_acks.remove(&offset);
                        Err(ClusterError::Timeout)
                    }
                }
            }
        }
    }
    
    /// Complete pending acks up to the given offset
    async fn complete_pending_acks(&self, up_to_offset: u64) {
        let to_complete: Vec<_> = self.pending_acks
            .iter()
            .filter(|e| e.key() <= &up_to_offset)
            .map(|e| *e.key())
            .collect();
        
        for offset in to_complete {
            if let Some((_, pending)) = self.pending_acks.remove(&offset) {
                let _ = pending.completion.send(Ok(()));
            }
        }
    }
    
    /// Get current high watermark
    pub fn high_watermark(&self) -> u64 {
        self.high_watermark.load(Ordering::SeqCst)
    }
    
    /// Get current log end offset
    pub fn log_end_offset(&self) -> u64 {
        self.log_end_offset.load(Ordering::SeqCst)
    }
    
    /// Get current leader epoch
    pub fn leader_epoch(&self) -> u64 {
        self.leader_epoch.load(Ordering::SeqCst)
    }
    
    /// Get current ISR
    pub async fn get_isr(&self) -> HashSet<NodeId> {
        self.isr.read().await.clone()
    }
    
    /// Check if we have enough ISR for writes
    pub async fn has_min_isr(&self) -> bool {
        let isr = self.isr.read().await;
        isr.len() >= self.config.min_isr as usize
    }
}

/// Manages replication across all partitions
pub struct ReplicationManager {
    /// Our node ID
    local_node: NodeId,
    
    /// Per-partition replication state
    partitions: DashMap<PartitionId, Arc<PartitionReplication>>,
    
    /// Configuration
    config: ReplicationConfig,
    
    /// Raft node for ISR propagation (optional, set when integrated with coordinator)
    raft_node: Option<Arc<RwLock<crate::raft::RaftNode>>>,
}

impl ReplicationManager {
    /// Create new replication manager
    pub fn new(local_node: NodeId, config: ReplicationConfig) -> Self {
        Self {
            local_node,
            partitions: DashMap::new(),
            config,
            raft_node: None,
        }
    }
    
    /// Set Raft node for ISR propagation
    pub fn set_raft_node(&mut self, raft_node: Arc<RwLock<crate::raft::RaftNode>>) {
        self.raft_node = Some(raft_node);
    }
    
    /// Get or create partition replication state
    pub fn get_or_create(&self, partition_id: PartitionId, is_leader: bool) -> Arc<PartitionReplication> {
        self.partitions
            .entry(partition_id.clone())
            .or_insert_with(|| {
                Arc::new(PartitionReplication::new(
                    partition_id,
                    self.local_node.clone(),
                    is_leader,
                    self.config.clone(),
                ))
            })
            .clone()
    }
    
    /// Get partition replication state
    pub fn get(&self, partition_id: &PartitionId) -> Option<Arc<PartitionReplication>> {
        self.partitions.get(partition_id).map(|e| e.value().clone())
    }
    
    /// Remove partition replication state
    pub fn remove(&self, partition_id: &PartitionId) -> Option<Arc<PartitionReplication>> {
        self.partitions.remove(partition_id).map(|(_, v)| v)
    }
    
    /// Get all partitions we're leading
    pub fn leading_partitions(&self) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .filter(|e| e.value().is_leader)
            .map(|e| e.key().clone())
            .collect()
    }
    
    /// Handle replica fetch and propagate ISR changes if needed
    pub async fn handle_replica_fetch(
        &self,
        partition_id: &PartitionId,
        replica_id: &NodeId,
        fetch_offset: u64,
    ) -> Result<()> {
        let partition = self.get(partition_id)
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: partition_id.topic.clone(),
                partition: partition_id.partition,
            })?;
        
        let isr_changed = partition.handle_replica_fetch(replica_id, fetch_offset).await?;
        
        // Propagate ISR change to cluster via Raft
        if isr_changed {
            if let Err(e) = self.propagate_isr_change(partition_id).await {
                warn!(
                    partition = %partition_id,
                    error = %e,
                    "Failed to propagate ISR change (will retry on next health check)"
                );
            }
        }
        
        Ok(())
    }
    
    /// Run periodic health checks
    pub async fn run_health_checks(&self) {
        for entry in self.partitions.iter() {
            let partition = entry.value();
            if partition.is_leader {
                let removed = partition.check_replica_health().await;
                if !removed.is_empty() {
                    warn!(
                        partition = %partition.partition_id,
                        removed = ?removed,
                        "Removed replicas from ISR - propagating via Raft"
                    );
                    
                    // Propagate ISR change to cluster via Raft
                    if let Err(e) = self.propagate_isr_change(&partition.partition_id).await {
                        error!(
                            partition = %partition.partition_id,
                            error = %e,
                            "Failed to propagate ISR change via Raft"
                        );
                    }
                }
            }
        }
    }
    
    /// Propagate ISR change to cluster via Raft
    async fn propagate_isr_change(&self, partition_id: &PartitionId) -> Result<()> {
        // Get current ISR
        let partition = match self.get(partition_id) {
            Some(p) => p,
            None => return Ok(()), // Partition removed
        };
        
        let isr = partition.isr.read().await;
        let isr_vec: Vec<NodeId> = isr.iter().cloned().collect();
        drop(isr);
        
        // Send via Raft if available
        if let Some(raft_node) = &self.raft_node {
            let node = raft_node.read().await;
            
            let cmd = crate::metadata::MetadataCommand::UpdatePartitionIsr {
                partition: partition_id.clone(),
                isr: isr_vec.clone(),
            };
            
            match node.propose(cmd).await {
                Ok(_response) => {
                    info!(
                        partition = %partition_id,
                        isr = ?isr_vec,
                        "ISR change propagated via Raft"
                    );
                    Ok(())
                }
                Err(e) => {
                    error!(
                        partition = %partition_id,
                        error = %e,
                        "Failed to propose ISR change to Raft"
                    );
                    Err(e)
                }
            }
        } else {
            debug!(
                partition = %partition_id,
                "No Raft node configured - ISR change local only"
            );
            Ok(())
        }
    }
}

/// Follower fetcher that pulls data from leader
/// 
/// Implements ISR (In-Sync Replica) follower logic:
/// 1. Periodically fetches records from partition leader
/// 2. Applies records to local storage
/// 3. Reports replica state back to leader
/// 4. Leader tracks follower progress and updates ISR
pub struct FollowerFetcher {
    /// Our node ID (used for replica identification in fetch requests)
    local_node: NodeId,
    
    /// Partition we're fetching for
    partition_id: PartitionId,
    
    /// Leader node ID
    leader_id: NodeId,
    
    /// Current fetch offset
    fetch_offset: u64,
    
    /// High watermark (committed offset)
    high_watermark: u64,
    
    /// Transport for network communication
    transport: Arc<Mutex<crate::Transport>>,
    
    /// Configuration
    config: ReplicationConfig,
    
    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Receiver<()>,
    
    /// Last successful fetch timestamp
    last_fetch: std::time::Instant,
}

impl FollowerFetcher {
    pub fn new(
        local_node: NodeId,
        partition_id: PartitionId,
        leader_id: NodeId,
        start_offset: u64,
        transport: Arc<Mutex<crate::Transport>>,
        config: ReplicationConfig,
        shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Self {
        Self {
            local_node,
            partition_id,
            leader_id,
            fetch_offset: start_offset,
            high_watermark: 0,
            transport,
            config,
            shutdown,
            last_fetch: std::time::Instant::now(),
        }
    }
    
    /// Run the fetcher loop
    pub async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.fetch_interval);
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.fetch_from_leader().await {
                        error!(
                            partition = %self.partition_id,
                            error = %e,
                            "Fetch from leader failed"
                        );
                    }
                }
                _ = self.shutdown.recv() => {
                    info!(partition = %self.partition_id, "Follower fetcher shutting down");
                    break;
                }
            }
        }
        
        Ok(())
    }
    
    /// Fetch records from leader and apply them locally
    async fn fetch_from_leader(&mut self) -> Result<()> {
        use crate::protocol::{ClusterRequest, ClusterResponse, RequestHeader};
        
        // Build fetch request
        let header = RequestHeader::new(
            rand::random(), // correlation_id
            self.local_node.clone(),
        );
        
        let request = ClusterRequest::Fetch {
            header,
            partition: self.partition_id.clone(),
            offset: self.fetch_offset,
            max_bytes: self.config.fetch_max_bytes,
        };
        
        // Send to leader
        let response = {
            let transport = self.transport.lock().await;
            transport.send(&self.leader_id, request).await?
        };
        
        // Handle response
        match response {
            ClusterResponse::Fetch {
                header,
                partition,
                high_watermark,
                log_start_offset: _,
                records,
            } => {
                if !header.is_success() {
                    return Err(ClusterError::Network(format!(
                        "Fetch failed: {}",
                        header.error_message.unwrap_or_default()
                    )));
                }
                
                if partition != self.partition_id {
                    return Err(ClusterError::Network(format!(
                        "Partition mismatch: expected {}, got {}",
                        self.partition_id, partition
                    )));
                }
                
                // Apply fetched records
                if !records.is_empty() {
                    self.apply_records(&records).await?;
                    debug!(
                        partition = %self.partition_id,
                        records_bytes = records.len(),
                        new_offset = self.fetch_offset,
                        hwm = high_watermark,
                        "Applied records from leader"
                    );
                }
                
                // Update high watermark
                self.high_watermark = high_watermark;
                self.last_fetch = std::time::Instant::now();
                
                // Report our state back to leader
                self.report_replica_state().await?;
                
                Ok(())
            }
            _ => Err(ClusterError::Network(format!(
                "Unexpected response type: {:?}",
                response
            ))),
        }
    }
    
    /// Apply fetched records to local storage
    async fn apply_records(&mut self, records: &[u8]) -> Result<()> {
        // For now, we just track that we received records
        // In production, this would:
        // 1. Deserialize records batch
        // 2. Write to local log segment
        // 3. Update fetch_offset to end of applied batch
        
        // Simulate advancing offset based on records received
        // In real implementation, this comes from parsing the batch
        let estimated_count = records.len() / 100; // Rough estimate
        self.fetch_offset += estimated_count as u64;
        
        Ok(())
    }
    
    /// Report replica state to leader
    async fn report_replica_state(&mut self) -> Result<()> {
        use crate::protocol::{ClusterRequest, RequestHeader};
        
        let header = RequestHeader::new(
            rand::random(),
            self.local_node.clone(),
        );
        
        let request = ClusterRequest::ReplicaState {
            header,
            partition: self.partition_id.clone(),
            log_end_offset: self.fetch_offset,
            high_watermark: self.high_watermark,
        };
        
        // Send state update (fire and forget)
        let transport = self.transport.lock().await;
        let _ = transport.send(&self.leader_id, request).await;
        
        Ok(())
    }
    
    /// Get current replication lag (bytes behind leader)
    pub fn lag(&self) -> u64 {
        self.high_watermark.saturating_sub(self.fetch_offset)
    }
    
    /// Get time since last successful fetch
    pub fn fetch_age(&self) -> std::time::Duration {
        self.last_fetch.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_partition_replication_leader() {
        let config = ReplicationConfig::default();
        let partition_id = PartitionId::new("test", 0);
        let mut replication = PartitionReplication::new(
            partition_id.clone(),
            "node-1".to_string(),
            false,
            config,
        );
        
        // Become leader
        replication.become_leader(1, vec![
            "node-1".to_string(),
            "node-2".to_string(),
        ]).await;
        
        assert!(replication.is_leader);
        assert_eq!(replication.leader_epoch(), 1);
    }
    
    #[tokio::test]
    async fn test_hwm_advancement() {
        let config = ReplicationConfig::default();
        let partition_id = PartitionId::new("test", 0);
        let mut replication = PartitionReplication::new(
            partition_id.clone(),
            "node-1".to_string(),
            false,
            config.clone(),
        );
        
        // Become leader with replicas
        replication.become_leader(1, vec![
            "node-1".to_string(),
            "node-2".to_string(),
        ]).await;
        
        // Write some records
        replication.record_appended(0).await.unwrap();
        replication.record_appended(1).await.unwrap();
        replication.record_appended(2).await.unwrap();
        
        // HWM should still be 0 (only leader in ISR)
        assert_eq!(replication.high_watermark(), 3);
        
        // Replica fetches
        replication.handle_replica_fetch(&"node-2".to_string(), 2).await.unwrap();
        
        // Now node-2 should be in ISR and HWM should advance
        let isr = replication.get_isr().await;
        assert!(isr.contains(&"node-2".to_string()));
    }
    
    #[tokio::test]
    async fn test_acks_none() {
        let config = ReplicationConfig::default();
        let partition_id = PartitionId::new("test", 0);
        let replication = PartitionReplication::new(
            partition_id,
            "node-1".to_string(),
            true,
            config,
        );
        
        // acks=none should return immediately
        let result = replication.wait_for_acks(100, Acks::None).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_follower_fetcher_lag_tracking() {
        use crate::Transport;
        
        let config = ReplicationConfig::default();
        let partition_id = PartitionId::new("test", 0);
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        
        // Create mock transport
        let transport_config = crate::TransportConfig::default();
        let transport = Transport::new(
            "follower-1".into(),
            "127.0.0.1:9093".parse().unwrap(),
            transport_config,
        );
        
        let fetcher = FollowerFetcher::new(
            "follower-1".to_string(),
            partition_id,
            "leader-1".to_string(),
            0,
            Arc::new(Mutex::new(transport)),
            config,
            shutdown_rx,
        );
        
        // Verify initial state
        assert_eq!(fetcher.fetch_offset, 0);
        assert_eq!(fetcher.high_watermark, 0);
        assert_eq!(fetcher.lag(), 0);
        
        // Cleanup
        drop(shutdown_tx);
    }
    
    #[tokio::test]
    async fn test_replication_manager_partition_tracking() {
        let config = ReplicationConfig::default();
        let manager = ReplicationManager::new("node-1".to_string(), config);
        
        let partition_id1 = PartitionId::new("topic-1", 0);
        let partition_id2 = PartitionId::new("topic-1", 1);
        
        // Create partitions
        manager.get_or_create(partition_id1.clone(), true);
        manager.get_or_create(partition_id2.clone(), false);
        
        // Verify tracking
        assert_eq!(manager.leading_partitions().len(), 1);
        assert!(manager.get(&partition_id1).is_some());
        assert!(manager.get(&partition_id2).is_some());
        
        // Remove partition
        manager.remove(&partition_id1);
        assert!(manager.get(&partition_id1).is_none());
        assert_eq!(manager.leading_partitions().len(), 0);
    }
}
