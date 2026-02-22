//! ISR (In-Sync Replica) replication implementation
//!
//! This module implements Kafka-style ISR replication:
//! - Leaders handle all reads and writes
//! - Followers fetch from leaders and replicate
//! - High watermark tracks committed offsets
//! - ISR tracks which replicas are caught up
//!
//! # Ack Modes
//!
//! - `acks=0`: Fire and forget (no durability guarantee)
//! - `acks=1`: Leader acknowledgment (data written to leader)
//! - `acks=all`: All ISR acknowledgment (full durability)
//!
//! # Lock Ordering
//!
//! When acquiring multiple locks in this module, always follow this order
//! to prevent deadlocks:
//!
//! 1. `replicas` lock (RwLock)
//! 2. `isr` lock (RwLock)
//!
//! Never acquire `isr` before `replicas` in any code path.

use crate::config::ReplicationConfig;
use crate::error::{ClusterError, Result};
use crate::node::NodeId;
use crate::partition::PartitionId;
use crate::protocol::Acks;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Replication state for a single partition
#[derive(Debug)]
pub struct PartitionReplication {
    /// Partition identifier
    pub partition_id: PartitionId,

    /// Our node ID
    local_node: NodeId,

    /// Whether we are the leader (AtomicBool for safe concurrent access through Arc)
    is_leader: AtomicBool,

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
            is_leader: AtomicBool::new(is_leader),
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
    pub async fn become_leader(&self, epoch: u64, replicas: Vec<NodeId>) {
        self.is_leader.store(true, Ordering::SeqCst);
        self.leader_epoch.store(epoch, Ordering::SeqCst);

        // Initialize replica tracking
        let mut replica_map = self.replicas.write().await;
        replica_map.clear();

        for node_id in &replicas {
            if node_id != &self.local_node {
                replica_map.insert(
                    node_id.clone(),
                    ReplicaProgress {
                        node_id: node_id.clone(),
                        log_end_offset: 0,
                        last_fetch: Instant::now(),
                        in_sync: true,
                        lag: 0,
                    },
                );
            }
        }

        // Initialize ISR with all replicas (including ourselves).
        // Replicas that participated in the leader election are assumed
        // healthy; the normal ISR-shrink mechanism will remove any that
        // subsequently fall behind.
        let mut isr = self.isr.write().await;
        isr.clear();
        for node_id in &replicas {
            isr.insert(node_id.clone());
        }
        isr.insert(self.local_node.clone());

        info!(
            partition = %self.partition_id,
            epoch = epoch,
            replicas = replicas.len(),
            "Became partition leader"
        );
    }

    /// Become follower for this partition
    pub fn become_follower(&self, epoch: u64) {
        self.is_leader.store(false, Ordering::SeqCst);
        self.leader_epoch.store(epoch, Ordering::SeqCst);

        info!(
            partition = %self.partition_id,
            epoch = epoch,
            "Became partition follower"
        );
    }

    /// Record appended locally (called by storage layer)
    pub async fn record_appended(&self, offset: u64) -> Result<()> {
        // Enforce min-ISR before accepting writes.
        // If the ISR has fallen below the configured minimum, reject the write
        // to prevent data loss from under-replicated partitions.
        let is_leader = self.is_leader.load(Ordering::SeqCst);
        if is_leader && !self.has_min_isr().await {
            return Err(ClusterError::NotEnoughIsr {
                required: self.config.min_isr,
                current: self.isr.read().await.len() as u16,
            });
        }

        // Update our log end offset (fetch_max prevents regression under concurrent appends)
        self.log_end_offset.fetch_max(offset + 1, Ordering::SeqCst);

        // If we're the leader, check if we can advance HWM
        if is_leader {
            self.maybe_advance_hwm().await;
        }

        Ok(())
    }

    /// Handle replica fetch and update progress
    /// Returns true if ISR changed
    pub async fn handle_replica_fetch(
        &self,
        replica_id: &NodeId,
        fetch_offset: u64,
    ) -> Result<bool> {
        if !self.is_leader.load(Ordering::SeqCst) {
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
        if !self.is_leader.load(Ordering::SeqCst) {
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
    ///
    /// Note: Acquires locks in order: replicas -> isr (following module lock ordering)
    async fn maybe_advance_hwm(&self) {
        // Acquire replicas first to follow lock ordering (replicas -> isr)
        let replicas = self.replicas.read().await;
        let isr = self.isr.read().await;

        // HWM is the minimum LEO across all ISR members
        let mut min_leo = self.log_end_offset.load(Ordering::SeqCst);

        for node_id in isr.iter() {
            if node_id == &self.local_node {
                continue;
            }
            if let Some(progress) = replicas.get(node_id) {
                min_leo = min_leo.min(progress.log_end_offset);
            }
        }

        // Drop locks before potentially acquiring more locks in complete_pending_acks
        drop(isr);
        drop(replicas);

        // Use fetch_max to atomically advance HWM without regression.
        // load+store was racy — two concurrent callers could regress HWM.
        let prev_hwm = self.high_watermark.fetch_max(min_leo, Ordering::SeqCst);
        if min_leo > prev_hwm {
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
                // Read ISR size and insert pending ack atomically
                // under the same lock to prevent TOCTOU race where ISR changes
                // between reading the count and inserting the ack.
                let isr = self.isr.read().await;
                let required = isr.len();

                if required <= 1 {
                    // Only leader in ISR, done
                    return Ok(());
                }

                // Create pending ack while still holding ISR lock
                let (tx, rx) = oneshot::channel();
                let mut acked = HashSet::new();
                acked.insert(self.local_node.clone());

                self.pending_acks.insert(
                    offset,
                    PendingAck {
                        offset,
                        acked_nodes: acked,
                        required_acks: required,
                        completion: tx,
                        created: Instant::now(),
                    },
                );

                // Now safe to drop ISR lock — the pending ack's required count
                // is consistent with the ISR snapshot taken above.
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

    /// Complete pending acks up to the given offset.
    ///
    /// Collects completeable offsets in a single scan, then batch-removes
    /// and sends completion signals. This avoids holding DashMap shard locks
    /// while executing `oneshot::send()`.
    async fn complete_pending_acks(&self, up_to_offset: u64) {
        // Phase 1: Collect keys to complete (single scan, shard locks held briefly)
        let to_complete: Vec<u64> = self
            .pending_acks
            .iter()
            .filter_map(|e| {
                if *e.key() <= up_to_offset {
                    Some(*e.key())
                } else {
                    None
                }
            })
            .collect();

        // Phase 2: Remove and signal (no shard locks held during send)
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

    /// Clean up stale pending acks that have exceeded the timeout
    ///
    /// Sends `Err(Timeout)` to waiters before removing stale entries, so callers
    /// get a clear timeout error instead of a misleading `ChannelClosed`.
    /// This should be called periodically to prevent memory leaks from
    /// pending acks that were never completed (e.g., due to network partitions).
    /// Returns the number of cleaned up entries.
    pub fn cleanup_stale_pending_acks(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut cleaned = 0;

        // Phase 1: Collect stale keys
        let stale_keys: Vec<u64> = self
            .pending_acks
            .iter()
            .filter_map(|e| {
                if now.duration_since(e.value().created) >= timeout {
                    Some(*e.key())
                } else {
                    None
                }
            })
            .collect();

        // Phase 2: Remove and signal timeout (no shard locks held during send)
        for key in stale_keys {
            if let Some((_, pending)) = self.pending_acks.remove(&key) {
                let _ = pending.completion.send(Err(ClusterError::Timeout));
                cleaned += 1;
            }
        }

        if cleaned > 0 {
            debug!(
                partition = %self.partition_id,
                cleaned = cleaned,
                "Cleaned up stale pending acks"
            );
        }

        cleaned
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
    pub fn get_or_create(
        &self,
        partition_id: PartitionId,
        is_leader: bool,
    ) -> Arc<PartitionReplication> {
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
            .filter(|e| e.value().is_leader.load(Ordering::Relaxed))
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
        let partition = self
            .get(partition_id)
            .ok_or_else(|| ClusterError::PartitionNotFound {
                topic: partition_id.topic.clone(),
                partition: partition_id.partition,
            })?;

        let isr_changed = partition
            .handle_replica_fetch(replica_id, fetch_offset)
            .await?;

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
            if partition.is_leader.load(Ordering::Relaxed) {
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
                // attach current leader epoch for fencing
                leader_epoch: partition.leader_epoch(),
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
/// 2. Applies records to local storage via the local `Partition`
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

    /// Local partition storage for persisting replicated data.
    /// Without this, followers would track offsets without actually writing
    /// data — causing complete data loss on leader failover.
    local_partition: Arc<rivven_core::Partition>,

    /// Configuration
    config: ReplicationConfig,

    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Receiver<()>,

    /// Last successful fetch timestamp
    last_fetch: std::time::Instant,

    /// Consecutive report failures (retry with backoff)
    report_failures: u32,
}

impl FollowerFetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_node: NodeId,
        partition_id: PartitionId,
        leader_id: NodeId,
        start_offset: u64,
        transport: Arc<Mutex<crate::Transport>>,
        local_partition: Arc<rivven_core::Partition>,
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
            local_partition,
            config,
            shutdown,
            last_fetch: std::time::Instant::now(),
            report_failures: 0,
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

    /// Apply fetched records to local storage.
    ///
    /// Deserializes each record from the leader's response and persists it
    /// to the local `Partition` via `append_replicated_batch`, which writes
    /// to the log segment preserving the leader-assigned offset.
    ///
    /// The fetch_offset is advanced only AFTER successful persistence,
    /// guaranteeing at-least-once delivery to local storage.
    async fn apply_records(&mut self, records: &[u8]) -> Result<()> {
        // Properly deserialize the record batch.
        // Each record is length-prefixed (4-byte BE u32).
        let mut cursor = 0;
        let mut messages: Vec<rivven_core::Message> = Vec::new();
        let mut last_offset: Option<u64> = None;

        while cursor + 4 <= records.len() {
            let len = u32::from_be_bytes([
                records[cursor],
                records[cursor + 1],
                records[cursor + 2],
                records[cursor + 3],
            ]) as usize;
            cursor += 4;

            if cursor + len > records.len() {
                tracing::warn!(
                    "Truncated record at byte {} in apply_records (expected {} bytes, have {})",
                    cursor,
                    len,
                    records.len() - cursor
                );
                break;
            }

            match rivven_core::Message::from_bytes(&records[cursor..cursor + len]) {
                Ok(msg) => {
                    last_offset = Some(msg.offset);
                    messages.push(msg);
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize record at byte {}: {}", cursor, e);
                    break;
                }
            }
            cursor += len;
        }

        // Persist to local storage BEFORE advancing fetch_offset.
        // This ensures that if we crash after persisting but before reporting,
        // we'll re-fetch (idempotent) rather than lose data.
        if !messages.is_empty() {
            self.local_partition
                .append_replicated_batch(messages)
                .await
                .map_err(|e| {
                    ClusterError::Internal(format!(
                        "Failed to persist replicated records to partition {}: {}",
                        self.partition_id, e
                    ))
                })?;
        }

        // Advance fetch_offset to one past the last successfully persisted record
        if let Some(offset) = last_offset {
            self.fetch_offset = offset + 1;
        }

        Ok(())
    }

    /// Report replica state to leader (retry with backoff + logging)
    async fn report_replica_state(&mut self) -> Result<()> {
        use crate::protocol::{ClusterRequest, RequestHeader};

        let header = RequestHeader::new(rand::random(), self.local_node.clone());

        let request = ClusterRequest::ReplicaState {
            header,
            partition: self.partition_id.clone(),
            log_end_offset: self.fetch_offset,
            high_watermark: self.high_watermark,
        };

        let transport = self.transport.lock().await;
        match transport.send(&self.leader_id, request).await {
            Ok(_) => {
                if self.report_failures > 0 {
                    info!(
                        partition = %self.partition_id,
                        previous_failures = self.report_failures,
                        "Replica state report succeeded after previous failures"
                    );
                }
                self.report_failures = 0;
                Ok(())
            }
            Err(e) => {
                self.report_failures += 1;
                if self.report_failures >= 5 {
                    warn!(
                        partition = %self.partition_id,
                        consecutive_failures = self.report_failures,
                        error = %e,
                        "Replica state report to leader consistently failing — \
                         leader may not update ISR for this replica"
                    );
                }
                // Still return Ok — report failure should not stop the fetch loop.
                // The leader will eventually notice the stale last_fetch and may
                // shrink the ISR, which is the correct self-healing behavior.
                Ok(())
            }
        }
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
        let replication =
            PartitionReplication::new(partition_id.clone(), "node-1".to_string(), false, config);

        // Become leader
        replication
            .become_leader(1, vec!["node-1".to_string(), "node-2".to_string()])
            .await;

        assert!(replication.is_leader.load(Ordering::Relaxed));
        assert_eq!(replication.leader_epoch(), 1);
    }

    #[tokio::test]
    async fn test_hwm_advancement() {
        let config = ReplicationConfig {
            // Use min_isr=1 so writes succeed with only the leader in ISR.
            // The test verifies HWM advancement after replica catch-up, not
            // min-ISR enforcement (covered by test_min_isr_enforcement).
            min_isr: 1,
            ..ReplicationConfig::default()
        };
        let partition_id = PartitionId::new("test", 0);
        let replication = PartitionReplication::new(
            partition_id.clone(),
            "node-1".to_string(),
            false,
            config.clone(),
        );

        // Become leader with replicas
        replication
            .become_leader(1, vec!["node-1".to_string(), "node-2".to_string()])
            .await;

        // Write some records
        replication.record_appended(0).await.unwrap();
        replication.record_appended(1).await.unwrap();
        replication.record_appended(2).await.unwrap();

        // HWM should be 0: all replicas are in ISR from the start, and
        // node-2 has not fetched yet (LEO = 0), so min(LEO) across ISR = 0.
        assert_eq!(replication.high_watermark(), 0);

        // Replica fetches up to offset 2 (LEO = 2)
        replication
            .handle_replica_fetch(&"node-2".to_string(), 2)
            .await
            .unwrap();

        // node-2 is still in ISR (lag = 1, within replica_lag_max_messages).
        // HWM advances to min(3, 2) = 2.
        let isr = replication.get_isr().await;
        assert!(isr.contains("node-2"));
        assert_eq!(replication.high_watermark(), 2);

        // Replica catches up fully (LEO = 3)
        replication
            .handle_replica_fetch(&"node-2".to_string(), 3)
            .await
            .unwrap();

        // HWM advances to min(3, 3) = 3
        assert_eq!(replication.high_watermark(), 3);
    }

    #[tokio::test]
    async fn test_acks_none() {
        let config = ReplicationConfig::default();
        let partition_id = PartitionId::new("test", 0);
        let replication =
            PartitionReplication::new(partition_id, "node-1".to_string(), true, config);

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

        // Create local partition for the follower
        let core_config = rivven_core::Config {
            data_dir: format!("/tmp/rivven-test-follower-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let local_partition = Arc::new(
            rivven_core::Partition::new(&core_config, "test", 0)
                .await
                .unwrap(),
        );

        let fetcher = FollowerFetcher::new(
            "follower-1".to_string(),
            partition_id,
            "leader-1".to_string(),
            0,
            Arc::new(Mutex::new(transport)),
            local_partition,
            config,
            shutdown_rx,
        );

        // Verify initial state
        assert_eq!(fetcher.fetch_offset, 0);
        assert_eq!(fetcher.high_watermark, 0);
        assert_eq!(fetcher.lag(), 0);

        // Cleanup
        drop(shutdown_tx);
        let _ = std::fs::remove_dir_all(&core_config.data_dir);
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
