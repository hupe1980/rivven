// Consumer Group Coordinator - Handles consumer group management and rebalancing
//
// This module implements the Kafka-compatible consumer group protocol:
// - JoinGroup: Consumer joins a group, triggers rebalance
// - SyncGroup: Leader sends assignments to coordinator, members fetch them
// - Heartbeat: Keep-alive mechanism for failure detection
// - LeaveGroup: Graceful consumer departure
// - CommitOffset: Persist consumer offsets
// - FetchOffset: Retrieve committed offsets
//
// # Persistence
//
// Consumer group state is persisted using a pluggable persistence backend:
// - In-memory (default): Fast but not durable across restarts
// - Raft-based: Durable and consistent across cluster nodes
//
// The persistence layer ensures:
// - Consumer group memberships survive coordinator restarts
// - Committed offsets are not lost
// - Rebalance state is recoverable

use rivven_core::consumer_group::{
    ConsumerGroup, GroupId, GroupState, MemberId, PartitionAssignment,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;
use thiserror::Error;

pub type CoordinatorResult<T> = Result<T, CoordinatorError>;

#[derive(Debug, Error, Clone, Serialize, Deserialize)]
pub enum CoordinatorError {
    #[error("Group not found: {0}")]
    GroupNotFound(String),

    #[error("Member not found: {0}")]
    MemberNotFound(String),

    #[error("Rebalance in progress")]
    RebalanceInProgress,

    #[error("Invalid generation: expected {expected}, got {actual}")]
    InvalidGeneration { expected: u32, actual: u32 },

    #[error("Not group leader")]
    NotGroupLeader,

    #[error("Illegal generation: {0}")]
    IllegalGeneration(String),

    #[error("Unknown member: {0}")]
    UnknownMember(String),

    #[error("Invalid session timeout: {0}")]
    InvalidSessionTimeout(String),

    #[error("Internal error: lock poisoned")]
    LockPoisoned,

    #[error("Persistence error: {0}")]
    PersistenceError(String),
}

// Implement From for PoisonError to allow ? operator
impl<T> From<PoisonError<T>> for CoordinatorError {
    fn from(_: PoisonError<T>) -> Self {
        CoordinatorError::LockPoisoned
    }
}

// ===== Persistence Layer =====

/// Persisted state for a consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedGroupState {
    /// Group ID
    pub group_id: GroupId,
    /// Current generation
    pub generation_id: u32,
    /// Group state
    pub state: String,
    /// Member IDs (full member data is transient)
    pub members: Vec<String>,
    /// Committed offsets: topic -> partition -> offset
    pub offsets: HashMap<String, HashMap<u32, i64>>,
    /// Last update timestamp
    pub updated_at: i64,
}

impl PersistedGroupState {
    pub fn from_group(group: &ConsumerGroup) -> Self {
        Self {
            group_id: group.group_id.clone(),
            generation_id: group.generation_id,
            state: format!("{:?}", group.state),
            members: group.members.keys().cloned().collect(),
            offsets: group.offsets.clone(),
            updated_at: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// Trait for consumer group persistence backends
pub trait GroupPersistence: Send + Sync {
    /// Save a consumer group's state
    fn save_group(&self, state: &PersistedGroupState) -> CoordinatorResult<()>;

    /// Load a consumer group's state
    fn load_group(&self, group_id: &str) -> CoordinatorResult<Option<PersistedGroupState>>;

    /// List all persisted group IDs
    fn list_groups(&self) -> CoordinatorResult<Vec<String>>;

    /// Delete a consumer group's state
    fn delete_group(&self, group_id: &str) -> CoordinatorResult<()>;

    /// Save a committed offset
    fn save_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> CoordinatorResult<()>;

    /// Load committed offsets for a group
    fn load_offsets(&self, group_id: &str)
        -> CoordinatorResult<HashMap<String, HashMap<u32, i64>>>;
}

/// In-memory persistence (default, non-durable)
#[derive(Default)]
pub struct InMemoryPersistence {
    groups: RwLock<HashMap<String, PersistedGroupState>>,
}

impl InMemoryPersistence {
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
        }
    }
}

impl GroupPersistence for InMemoryPersistence {
    fn save_group(&self, state: &PersistedGroupState) -> CoordinatorResult<()> {
        let mut groups = self
            .groups
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        groups.insert(state.group_id.clone(), state.clone());
        Ok(())
    }

    fn load_group(&self, group_id: &str) -> CoordinatorResult<Option<PersistedGroupState>> {
        let groups = self
            .groups
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(groups.get(group_id).cloned())
    }

    fn list_groups(&self) -> CoordinatorResult<Vec<String>> {
        let groups = self
            .groups
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(groups.keys().cloned().collect())
    }

    fn delete_group(&self, group_id: &str) -> CoordinatorResult<()> {
        let mut groups = self
            .groups
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        groups.remove(group_id);
        Ok(())
    }

    fn save_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> CoordinatorResult<()> {
        let mut groups = self
            .groups
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)?;

        if let Some(state) = groups.get_mut(group_id) {
            state
                .offsets
                .entry(topic.to_string())
                .or_default()
                .insert(partition, offset);
            state.updated_at = chrono::Utc::now().timestamp_millis();
        }
        Ok(())
    }

    fn load_offsets(
        &self,
        group_id: &str,
    ) -> CoordinatorResult<HashMap<String, HashMap<u32, i64>>> {
        let groups = self
            .groups
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(groups
            .get(group_id)
            .map(|s| s.offsets.clone())
            .unwrap_or_default())
    }
}

/// Callback type for Raft persistence operations
///
/// Receives serialized state changes and returns true if accepted by Raft.
type PersistCallback = Box<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// Raft-based persistence for durable consumer group state
///
/// This implementation writes consumer group state changes as Raft log entries,
/// ensuring durability and consistency across cluster nodes.
pub struct RaftPersistence {
    /// In-memory cache (populated from Raft state machine)
    cache: RwLock<HashMap<String, PersistedGroupState>>,
    /// Raft log callback for persistence
    /// In a full implementation, this would be connected to the Raft consensus module
    #[allow(dead_code)]
    on_persist: Option<PersistCallback>,
}

impl RaftPersistence {
    /// Create a new Raft persistence backend
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            on_persist: None,
        }
    }

    /// Create with a persistence callback
    ///
    /// The callback receives serialized state changes and should write them to the Raft log.
    /// Returns true if the write was accepted by Raft.
    pub fn with_callback<F>(callback: F) -> Self
    where
        F: Fn(&[u8]) -> bool + Send + Sync + 'static,
    {
        Self {
            cache: RwLock::new(HashMap::new()),
            on_persist: Some(Box::new(callback)),
        }
    }

    /// Apply a state change from the Raft log
    ///
    /// This is called by the Raft state machine when a log entry is committed.
    pub fn apply_log_entry(&self, entry: &RaftLogEntry) -> CoordinatorResult<()> {
        let mut cache = self
            .cache
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)?;

        match entry {
            RaftLogEntry::GroupStateChange(state) => {
                cache.insert(state.group_id.clone(), state.clone());
            }
            RaftLogEntry::OffsetCommit {
                group_id,
                topic,
                partition,
                offset,
            } => {
                if let Some(state) = cache.get_mut(group_id) {
                    state
                        .offsets
                        .entry(topic.clone())
                        .or_default()
                        .insert(*partition, *offset);
                    state.updated_at = chrono::Utc::now().timestamp_millis();
                }
            }
            RaftLogEntry::GroupDeleted(group_id) => {
                cache.remove(group_id);
            }
        }
        Ok(())
    }

    /// Restore state from a snapshot
    pub fn restore_snapshot(
        &self,
        groups: HashMap<String, PersistedGroupState>,
    ) -> CoordinatorResult<()> {
        let mut cache = self
            .cache
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        *cache = groups;
        Ok(())
    }

    /// Create a snapshot of current state
    pub fn create_snapshot(&self) -> CoordinatorResult<HashMap<String, PersistedGroupState>> {
        let cache = self
            .cache
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(cache.clone())
    }

    fn persist_entry(&self, entry: &RaftLogEntry) -> CoordinatorResult<()> {
        if let Some(ref callback) = self.on_persist {
            let bytes = serde_json::to_vec(entry)
                .map_err(|e| CoordinatorError::PersistenceError(e.to_string()))?;
            if !callback(&bytes) {
                return Err(CoordinatorError::PersistenceError(
                    "Raft rejected write".to_string(),
                ));
            }
        }
        // Also apply to local cache
        self.apply_log_entry(entry)
    }
}

impl Default for RaftPersistence {
    fn default() -> Self {
        Self::new()
    }
}

/// Raft log entry types for consumer group state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftLogEntry {
    /// Full group state change
    GroupStateChange(PersistedGroupState),
    /// Offset commit
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: u32,
        offset: i64,
    },
    /// Group deleted
    GroupDeleted(String),
}

impl GroupPersistence for RaftPersistence {
    fn save_group(&self, state: &PersistedGroupState) -> CoordinatorResult<()> {
        self.persist_entry(&RaftLogEntry::GroupStateChange(state.clone()))
    }

    fn load_group(&self, group_id: &str) -> CoordinatorResult<Option<PersistedGroupState>> {
        let cache = self
            .cache
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(cache.get(group_id).cloned())
    }

    fn list_groups(&self) -> CoordinatorResult<Vec<String>> {
        let cache = self
            .cache
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(cache.keys().cloned().collect())
    }

    fn delete_group(&self, group_id: &str) -> CoordinatorResult<()> {
        self.persist_entry(&RaftLogEntry::GroupDeleted(group_id.to_string()))
    }

    fn save_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: i64,
    ) -> CoordinatorResult<()> {
        self.persist_entry(&RaftLogEntry::OffsetCommit {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
            offset,
        })
    }

    fn load_offsets(
        &self,
        group_id: &str,
    ) -> CoordinatorResult<HashMap<String, HashMap<u32, i64>>> {
        let cache = self
            .cache
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)?;
        Ok(cache
            .get(group_id)
            .map(|s| s.offsets.clone())
            .unwrap_or_default())
    }
}

/// Consumer group coordinator - manages multiple consumer groups
///
/// Uses `tokio::sync::RwLock` instead of `std::sync::RwLock`
/// to avoid blocking the async executor during lock contention.
pub struct ConsumerCoordinator {
    groups: Arc<tokio::sync::RwLock<HashMap<GroupId, ConsumerGroup>>>,
    /// Persistence backend
    persistence: Arc<dyn GroupPersistence>,
}

impl ConsumerCoordinator {
    /// Create a new coordinator with in-memory persistence (non-durable)
    pub fn new() -> Self {
        Self {
            groups: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            persistence: Arc::new(InMemoryPersistence::new()),
        }
    }

    /// Create a coordinator with custom persistence backend
    pub fn with_persistence(persistence: Arc<dyn GroupPersistence>) -> Self {
        Self {
            groups: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            persistence,
        }
    }

    /// Create a coordinator with Raft-based persistence
    pub fn with_raft_persistence() -> Self {
        Self {
            groups: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            persistence: Arc::new(RaftPersistence::new()),
        }
    }

    /// Restore state from persistence on startup
    pub async fn restore(&self) -> CoordinatorResult<usize> {
        let group_ids = self.persistence.list_groups()?;
        let mut restored = 0;

        for group_id in group_ids {
            if let Some(state) = self.persistence.load_group(&group_id)? {
                // Restore group with persisted offsets
                // Note: Members are transient and will rejoin
                let mut groups = self.groups.write().await;
                use std::collections::hash_map::Entry;
                if let Entry::Vacant(entry) = groups.entry(group_id) {
                    let mut group = ConsumerGroup::new(
                        entry.key().clone(),
                        Duration::from_secs(30),
                        Duration::from_secs(60),
                    );
                    group.offsets = state.offsets;
                    group.generation_id = state.generation_id;
                    entry.insert(group);
                    restored += 1;
                }
            }
        }

        tracing::info!(
            restored_groups = restored,
            "Restored consumer groups from persistence"
        );
        Ok(restored)
    }

    /// Acquire a read lock on groups (async, non-blocking)
    async fn read_groups(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<GroupId, ConsumerGroup>> {
        self.groups.read().await
    }

    /// Acquire a write lock on groups (async, non-blocking)
    async fn write_groups(
        &self,
    ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<GroupId, ConsumerGroup>> {
        self.groups.write().await
    }

    /// JoinGroup - Consumer joins a group (triggers rebalance)
    #[allow(clippy::too_many_arguments)] // Protocol-defined function signature
    pub async fn join_group(
        &self,
        group_id: GroupId,
        member_id: Option<MemberId>,
        client_id: String,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        subscriptions: Vec<String>,
        metadata: Vec<u8>,
    ) -> CoordinatorResult<JoinGroupResponse> {
        let mut groups = self.write_groups().await;

        // Get or create group
        let _is_new_group = !groups.contains_key(&group_id);
        let group = groups.entry(group_id.clone()).or_insert_with(|| {
            ConsumerGroup::new(group_id.clone(), session_timeout, rebalance_timeout)
        });

        // Generate member_id if not provided (rejoin uses existing)
        let member_id =
            member_id.unwrap_or_else(|| format!("{}-{}", client_id, uuid::Uuid::new_v4()));

        // Add member (triggers rebalance)
        group.add_member(member_id.clone(), client_id, subscriptions, metadata);

        let generation_id = group.generation_id;
        let leader_id = group.leader_id.clone().unwrap_or_default();
        let members = &group.members;

        let response = JoinGroupResponse {
            generation_id,
            member_id,
            leader_id,
            members: members
                .iter()
                .map(|(id, member)| MemberInfo {
                    member_id: id.clone(),
                    metadata: member.metadata.clone(),
                })
                .collect(),
        };

        // Persist group state on every join (not just new groups) so that
        // membership changes, generation bumps, and rebalances survive restart.
        {
            let state = PersistedGroupState::from_group(group);
            // Best effort persistence - don't fail the join if persistence fails
            if let Err(e) = self.persistence.save_group(&state) {
                tracing::warn!(group_id = %group_id, error = %e, "Failed to persist group state");
            }
        }

        Ok(response)
    }

    /// SyncGroup - Leader sends assignments, members receive them
    pub async fn sync_group(
        &self,
        group_id: GroupId,
        member_id: MemberId,
        generation_id: u32,
        assignments: HashMap<MemberId, Vec<PartitionAssignment>>,
    ) -> CoordinatorResult<SyncGroupResponse> {
        let mut groups = self.write_groups().await;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        // Validate generation
        if group.generation_id != generation_id {
            return Err(CoordinatorError::InvalidGeneration {
                expected: group.generation_id,
                actual: generation_id,
            });
        }

        // Validate member exists
        if !group.members.contains_key(&member_id) {
            return Err(CoordinatorError::UnknownMember(member_id));
        }

        // Only leader can provide assignments
        if Some(&member_id) == group.leader_id.as_ref() && !assignments.is_empty() {
            group.complete_rebalance(assignments);
        }

        // Return this member's assignment
        let assignment = group
            .members
            .get(&member_id)
            .map(|m| m.assignment.clone())
            .unwrap_or_default();

        Ok(SyncGroupResponse { assignment })
    }

    /// Heartbeat - Keep-alive for failure detection
    pub async fn heartbeat(
        &self,
        group_id: GroupId,
        member_id: MemberId,
        generation_id: u32,
    ) -> CoordinatorResult<HeartbeatResponse> {
        let mut groups = self.write_groups().await;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        // Validate generation
        if group.generation_id != generation_id {
            return Err(CoordinatorError::InvalidGeneration {
                expected: group.generation_id,
                actual: generation_id,
            });
        }

        // Validate member exists
        if !group.members.contains_key(&member_id) {
            return Err(CoordinatorError::UnknownMember(member_id));
        }

        // Update heartbeat
        let _ = group.heartbeat(&member_id);

        // Check for timeouts
        let timed_out = group.check_timeouts();
        if !timed_out.is_empty() {
            // Rebalance triggered by timeout
            return Ok(HeartbeatResponse {
                rebalance_required: true,
            });
        }

        Ok(HeartbeatResponse {
            rebalance_required: group.state == GroupState::PreparingRebalance,
        })
    }

    /// LeaveGroup - Graceful consumer departure
    pub async fn leave_group(
        &self,
        group_id: GroupId,
        member_id: MemberId,
    ) -> CoordinatorResult<LeaveGroupResponse> {
        let mut groups = self.write_groups().await;
        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        group.remove_member(&member_id);

        // Persist updated group state after member removal
        {
            let state = PersistedGroupState::from_group(group);
            if let Err(e) = self.persistence.save_group(&state) {
                tracing::warn!(group_id = %group_id, error = %e, "Failed to persist group state after leave");
            }
        }

        Ok(LeaveGroupResponse {})
    }

    /// CommitOffset - Persist consumer offsets
    pub async fn commit_offset(
        &self,
        group_id: GroupId,
        topic: String,
        partition: u32,
        offset: i64,
    ) -> CoordinatorResult<CommitOffsetResponse> {
        let mut groups = self.write_groups().await;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        group.commit_offset(&topic, partition, offset);

        // Persist offset to backend
        self.persistence
            .save_offset(&group_id, &topic, partition, offset)?;

        Ok(CommitOffsetResponse {})
    }

    /// FetchOffset - Retrieve committed offsets
    pub async fn fetch_offset(
        &self,
        group_id: GroupId,
        topic: String,
        partition: u32,
    ) -> CoordinatorResult<FetchOffsetResponse> {
        let groups = self.read_groups().await;

        let group = groups
            .get(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        let offset = group.fetch_offset(&topic, partition);

        Ok(FetchOffsetResponse { offset })
    }

    /// Background task: Check for timeouts and trigger rebalances
    ///
    /// Note: In production, the caller should handle errors by logging
    /// and continuing, as timeout checks are best-effort.
    pub async fn check_timeouts(&self) -> CoordinatorResult<()> {
        let mut groups = self.write_groups().await;

        for group in groups.values_mut() {
            let timed_out = group.check_timeouts();
            if !timed_out.is_empty() {
                // Members timed out - rebalance triggered automatically by remove_member
                for member_id in timed_out {
                    group.remove_member(&member_id);
                }
                // Persist updated group state after timeout-driven member removal
                let state = PersistedGroupState::from_group(group);
                if let Err(e) = self.persistence.save_group(&state) {
                    tracing::warn!(
                        group_id = %group.group_id,
                        error = %e,
                        "Failed to persist group state after timeout cleanup"
                    );
                }
            }
        }

        Ok(())
    }
}

impl Default for ConsumerCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// ===== Request/Response Types =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    pub generation_id: u32,
    pub member_id: MemberId,
    pub leader_id: MemberId,
    pub members: Vec<MemberInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub member_id: MemberId,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncGroupResponse {
    pub assignment: Vec<PartitionAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub rebalance_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveGroupResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOffsetResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchOffsetResponse {
    pub offset: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_join_group_creates_group() {
        let coordinator = ConsumerCoordinator::new();

        let response = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        assert_eq!(response.generation_id, 0);
        assert!(response.member_id.starts_with("client-1"));
        assert_eq!(response.members.len(), 1);
    }

    #[tokio::test]
    async fn test_join_group_first_member_is_leader() {
        let coordinator = ConsumerCoordinator::new();

        let response = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        assert_eq!(response.leader_id, response.member_id);
    }

    #[tokio::test]
    async fn test_join_group_triggers_rebalance() {
        let coordinator = ConsumerCoordinator::new();

        // First member
        let _response1 = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        // Second member - should trigger rebalance
        let response2 = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-2".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        // Generation should still be 0 until sync_group completes rebalance
        assert_eq!(response2.generation_id, 0);
        assert_eq!(response2.members.len(), 2);
    }

    #[tokio::test]
    async fn test_sync_group_completes_rebalance() {
        let coordinator = ConsumerCoordinator::new();

        let response1 = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        let member_id = response1.member_id.clone();
        let generation_id = response1.generation_id;

        // Leader sends assignments
        let mut assignments = HashMap::new();
        assignments.insert(
            member_id.clone(),
            vec![PartitionAssignment {
                topic: "topic-1".to_string(),
                partition: 0,
            }],
        );

        let sync_response = coordinator
            .sync_group(
                "test-group".to_string(),
                member_id,
                generation_id,
                assignments,
            )
            .await
            .unwrap();

        assert_eq!(sync_response.assignment.len(), 1);
        assert_eq!(sync_response.assignment[0].partition, 0);
    }

    #[tokio::test]
    async fn test_heartbeat_detects_invalid_generation() {
        let coordinator = ConsumerCoordinator::new();

        let response = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        let result = coordinator
            .heartbeat(
                "test-group".to_string(),
                response.member_id,
                999, // Wrong generation
            )
            .await;

        assert!(matches!(
            result,
            Err(CoordinatorError::InvalidGeneration { .. })
        ));
    }

    #[tokio::test]
    async fn test_leave_group_removes_member() {
        let coordinator = ConsumerCoordinator::new();

        let response = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        coordinator
            .leave_group("test-group".to_string(), response.member_id.clone())
            .await
            .unwrap();

        // Heartbeat should fail after leaving
        let result = coordinator
            .heartbeat(
                "test-group".to_string(),
                response.member_id,
                response.generation_id,
            )
            .await;

        assert!(matches!(result, Err(CoordinatorError::UnknownMember(_))));
    }

    #[tokio::test]
    async fn test_commit_and_fetch_offset() {
        let coordinator = ConsumerCoordinator::new();

        coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        // Commit offset
        coordinator
            .commit_offset("test-group".to_string(), "topic-1".to_string(), 0, 42)
            .await
            .unwrap();

        // Fetch offset
        let response = coordinator
            .fetch_offset("test-group".to_string(), "topic-1".to_string(), 0)
            .await
            .unwrap();

        assert_eq!(response.offset, Some(42));
    }

    #[tokio::test]
    async fn test_fetch_offset_returns_none_when_not_committed() {
        let coordinator = ConsumerCoordinator::new();

        coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        let response = coordinator
            .fetch_offset("test-group".to_string(), "topic-1".to_string(), 0)
            .await
            .unwrap();

        assert_eq!(response.offset, None);
    }

    #[test]
    fn test_in_memory_persistence() {
        let persistence = InMemoryPersistence::new();

        // Test save and load group
        let state = PersistedGroupState {
            group_id: "test-group".to_string(),
            generation_id: 1,
            state: "Stable".to_string(),
            members: vec!["member-1".to_string()],
            offsets: HashMap::new(),
            updated_at: 12345678,
        };

        persistence.save_group(&state).unwrap();

        let loaded = persistence.load_group("test-group").unwrap();
        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.group_id, "test-group");
        assert_eq!(loaded.generation_id, 1);
        assert_eq!(loaded.members.len(), 1);

        // Test list groups
        let groups = persistence.list_groups().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], "test-group");

        // Test delete group
        persistence.delete_group("test-group").unwrap();
        let loaded = persistence.load_group("test-group").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_in_memory_persistence_offsets() {
        let persistence = InMemoryPersistence::new();

        // First, create the group
        let state = PersistedGroupState {
            group_id: "test-group".to_string(),
            generation_id: 1,
            state: "Stable".to_string(),
            members: vec!["member-1".to_string()],
            offsets: HashMap::new(),
            updated_at: 12345678,
        };
        persistence.save_group(&state).unwrap();

        // Save offsets
        persistence
            .save_offset("test-group", "topic-1", 0, 100)
            .unwrap();
        persistence
            .save_offset("test-group", "topic-1", 1, 200)
            .unwrap();
        persistence
            .save_offset("test-group", "topic-2", 0, 50)
            .unwrap();

        // Load offsets - returns HashMap<String, HashMap<u32, i64>> (topic -> partition -> offset)
        let offsets = persistence.load_offsets("test-group").unwrap();
        assert_eq!(offsets.len(), 2); // 2 topics

        let topic1_offsets = offsets.get("topic-1").unwrap();
        assert_eq!(topic1_offsets.get(&0), Some(&100));
        assert_eq!(topic1_offsets.get(&1), Some(&200));

        let topic2_offsets = offsets.get("topic-2").unwrap();
        assert_eq!(topic2_offsets.get(&0), Some(&50));
    }

    #[test]
    fn test_raft_persistence_log_entries() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let log_count = Arc::new(AtomicUsize::new(0));
        let log_count_clone = log_count.clone();

        let persistence = RaftPersistence::with_callback(move |_bytes| {
            log_count_clone.fetch_add(1, Ordering::SeqCst);
            true
        });

        // Save group (should create log entry)
        let state = PersistedGroupState {
            group_id: "test-group".to_string(),
            generation_id: 1,
            state: "Stable".to_string(),
            members: vec!["member-1".to_string()],
            offsets: HashMap::new(),
            updated_at: 12345678,
        };

        persistence.save_group(&state).unwrap();
        assert_eq!(log_count.load(Ordering::SeqCst), 1);

        // Save offset (should create log entry)
        persistence
            .save_offset("test-group", "topic-1", 0, 100)
            .unwrap();
        assert_eq!(log_count.load(Ordering::SeqCst), 2);

        // Delete group (should create log entry)
        persistence.delete_group("test-group").unwrap();
        assert_eq!(log_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_raft_persistence_apply_log_entry() {
        let persistence = RaftPersistence::new();

        // Apply GroupStateChange entry (tuple variant)
        let entry = RaftLogEntry::GroupStateChange(PersistedGroupState {
            group_id: "test-group".to_string(),
            generation_id: 1,
            state: "Stable".to_string(),
            members: vec!["member-1".to_string()],
            offsets: HashMap::new(),
            updated_at: 12345678,
        });
        persistence.apply_log_entry(&entry).unwrap();

        let loaded = persistence.load_group("test-group").unwrap();
        assert!(loaded.is_some());

        // Apply OffsetCommit entry
        let entry = RaftLogEntry::OffsetCommit {
            group_id: "test-group".to_string(),
            topic: "topic-1".to_string(),
            partition: 0,
            offset: 100,
        };
        persistence.apply_log_entry(&entry).unwrap();

        let offsets = persistence.load_offsets("test-group").unwrap();
        let topic1_offsets = offsets.get("topic-1").unwrap();
        assert_eq!(topic1_offsets.get(&0), Some(&100));

        // Apply GroupDeleted entry (tuple variant)
        let entry = RaftLogEntry::GroupDeleted("test-group".to_string());
        persistence.apply_log_entry(&entry).unwrap();

        let loaded = persistence.load_group("test-group").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_raft_persistence_snapshot() {
        let persistence = RaftPersistence::new();

        // Build up some state
        let entry = RaftLogEntry::GroupStateChange(PersistedGroupState {
            group_id: "group-1".to_string(),
            generation_id: 1,
            state: "Stable".to_string(),
            members: vec!["member-1".to_string()],
            offsets: HashMap::new(),
            updated_at: 12345678,
        });
        persistence.apply_log_entry(&entry).unwrap();

        let entry = RaftLogEntry::OffsetCommit {
            group_id: "group-1".to_string(),
            topic: "topic-1".to_string(),
            partition: 0,
            offset: 100,
        };
        persistence.apply_log_entry(&entry).unwrap();

        // Create snapshot
        let snapshot = persistence.create_snapshot().unwrap();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot.contains_key("group-1"));
        assert_eq!(snapshot.get("group-1").unwrap().group_id, "group-1");

        // Create new persistence and restore
        let persistence2 = RaftPersistence::new();
        persistence2.restore_snapshot(snapshot).unwrap();

        let loaded = persistence2.load_group("group-1").unwrap();
        assert!(loaded.is_some());

        let offsets = persistence2.load_offsets("group-1").unwrap();
        let topic1_offsets = offsets.get("topic-1").unwrap();
        assert_eq!(topic1_offsets.get(&0), Some(&100));
    }

    #[tokio::test]
    async fn test_coordinator_with_raft_persistence() {
        let entries = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
        let entries_clone = entries.clone();

        // Create RaftPersistence with callback
        let raft_persistence = Arc::new(RaftPersistence::with_callback(move |bytes| {
            entries_clone.lock().unwrap().push(bytes.to_vec());
            true
        }));

        let coordinator = ConsumerCoordinator::with_persistence(raft_persistence);

        // Join group should persist state
        let _response = coordinator
            .join_group(
                "test-group".to_string(),
                None,
                "client-1".to_string(),
                Duration::from_secs(10),
                Duration::from_secs(60),
                vec!["topic-1".to_string()],
                vec![],
            )
            .await
            .unwrap();

        // Check that at least one entry was logged
        let logged_entries = entries.lock().unwrap();
        assert!(!logged_entries.is_empty());

        // First entry should be deserializable as GroupStateChange
        let entry: RaftLogEntry = serde_json::from_slice(&logged_entries[0]).unwrap();
        match entry {
            RaftLogEntry::GroupStateChange(state) => {
                assert_eq!(state.group_id, "test-group");
            }
            _ => panic!("Expected GroupStateChange entry"),
        }
    }

    #[tokio::test]
    async fn test_coordinator_restore() {
        // Create persistence with some state
        let raft_persistence = Arc::new(RaftPersistence::new());

        // Manually add state to persistence
        let mut topic_offsets = HashMap::new();
        topic_offsets.insert(0u32, 500i64);
        let mut offsets = HashMap::new();
        offsets.insert("topic-1".to_string(), topic_offsets);

        let entry = RaftLogEntry::GroupStateChange(PersistedGroupState {
            group_id: "restored-group".to_string(),
            generation_id: 5,
            state: "Stable".to_string(),
            members: vec!["member-1-uuid".to_string()],
            offsets,
            updated_at: 12345678,
        });
        raft_persistence.apply_log_entry(&entry).unwrap();

        // Create coordinator and restore
        let coordinator = ConsumerCoordinator::with_persistence(raft_persistence);
        coordinator.restore().await.unwrap();

        // Verify the group was restored - we can check via fetch_offset
        // since the group exists
        let response = coordinator
            .fetch_offset("restored-group".to_string(), "topic-1".to_string(), 0)
            .await
            .unwrap();

        assert_eq!(response.offset, Some(500));
    }
}
