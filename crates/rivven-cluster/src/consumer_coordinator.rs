// Consumer Group Coordinator - Handles consumer group management and rebalancing
//
// This module implements the Kafka-compatible consumer group protocol:
// - JoinGroup: Consumer joins a group, triggers rebalance
// - SyncGroup: Leader sends assignments to coordinator, members fetch them
// - Heartbeat: Keep-alive mechanism for failure detection
// - LeaveGroup: Graceful consumer departure
// - CommitOffset: Persist consumer offsets
// - FetchOffset: Retrieve committed offsets

use rivven_core::consumer_group::{
    ConsumerGroup, GroupId, GroupState, MemberId, PartitionAssignment,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
}

// Implement From for PoisonError to allow ? operator
impl<T> From<PoisonError<T>> for CoordinatorError {
    fn from(_: PoisonError<T>) -> Self {
        CoordinatorError::LockPoisoned
    }
}

/// Consumer group coordinator - manages multiple consumer groups
pub struct ConsumerCoordinator {
    groups: Arc<RwLock<HashMap<GroupId, ConsumerGroup>>>,
    // TODO: Add Raft-based persistence
}

impl ConsumerCoordinator {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Acquire a read lock on groups, handling poisoned locks gracefully
    fn read_groups(
        &self,
    ) -> CoordinatorResult<RwLockReadGuard<'_, HashMap<GroupId, ConsumerGroup>>> {
        self.groups
            .read()
            .map_err(|_| CoordinatorError::LockPoisoned)
    }

    /// Acquire a write lock on groups, handling poisoned locks gracefully
    fn write_groups(
        &self,
    ) -> CoordinatorResult<RwLockWriteGuard<'_, HashMap<GroupId, ConsumerGroup>>> {
        self.groups
            .write()
            .map_err(|_| CoordinatorError::LockPoisoned)
    }

    /// JoinGroup - Consumer joins a group (triggers rebalance)
    #[allow(clippy::too_many_arguments)] // Protocol-defined function signature
    pub fn join_group(
        &self,
        group_id: GroupId,
        member_id: Option<MemberId>,
        client_id: String,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        subscriptions: Vec<String>,
        metadata: Vec<u8>,
    ) -> CoordinatorResult<JoinGroupResponse> {
        let mut groups = self.write_groups()?;

        // Get or create group
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

        Ok(JoinGroupResponse {
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
        })
    }

    /// SyncGroup - Leader sends assignments, members receive them
    pub fn sync_group(
        &self,
        group_id: GroupId,
        member_id: MemberId,
        generation_id: u32,
        assignments: HashMap<MemberId, Vec<PartitionAssignment>>,
    ) -> CoordinatorResult<SyncGroupResponse> {
        let mut groups = self.write_groups()?;

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
    pub fn heartbeat(
        &self,
        group_id: GroupId,
        member_id: MemberId,
        generation_id: u32,
    ) -> CoordinatorResult<HeartbeatResponse> {
        let mut groups = self.write_groups()?;

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
    pub fn leave_group(
        &self,
        group_id: GroupId,
        member_id: MemberId,
    ) -> CoordinatorResult<LeaveGroupResponse> {
        let mut groups = self.write_groups()?;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        group.remove_member(&member_id);

        Ok(LeaveGroupResponse {})
    }

    /// CommitOffset - Persist consumer offsets
    pub fn commit_offset(
        &self,
        group_id: GroupId,
        topic: String,
        partition: u32,
        offset: i64,
    ) -> CoordinatorResult<CommitOffsetResponse> {
        let mut groups = self.write_groups()?;

        let group = groups
            .get_mut(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        group.commit_offset(&topic, partition, offset);

        Ok(CommitOffsetResponse {})
    }

    /// FetchOffset - Retrieve committed offsets
    pub fn fetch_offset(
        &self,
        group_id: GroupId,
        topic: String,
        partition: u32,
    ) -> CoordinatorResult<FetchOffsetResponse> {
        let groups = self.read_groups()?;

        let group = groups
            .get(&group_id)
            .ok_or_else(|| CoordinatorError::GroupNotFound(group_id.clone()))?;

        let offset = group.fetch_offset(&topic, partition);

        Ok(FetchOffsetResponse { offset })
    }

    /// Background task: Check for timeouts and trigger rebalances
    ///
    /// Note: This can fail if the lock is poisoned. In production, the caller
    /// should handle this by logging and continuing, as timeout checks are
    /// best-effort.
    pub fn check_timeouts(&self) -> CoordinatorResult<()> {
        let mut groups = self.write_groups()?;

        for group in groups.values_mut() {
            let timed_out = group.check_timeouts();
            if !timed_out.is_empty() {
                // Members timed out - rebalance triggered automatically by remove_member
                for member_id in timed_out {
                    group.remove_member(&member_id);
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

    #[test]
    fn test_join_group_creates_group() {
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
            .unwrap();

        assert_eq!(response.generation_id, 0);
        assert!(response.member_id.starts_with("client-1"));
        assert_eq!(response.members.len(), 1);
    }

    #[test]
    fn test_join_group_first_member_is_leader() {
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
            .unwrap();

        assert_eq!(response.leader_id, response.member_id);
    }

    #[test]
    fn test_join_group_triggers_rebalance() {
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
            .unwrap();

        // Generation should still be 0 until sync_group completes rebalance
        assert_eq!(response2.generation_id, 0);
        assert_eq!(response2.members.len(), 2);
    }

    #[test]
    fn test_sync_group_completes_rebalance() {
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
            .unwrap();

        assert_eq!(sync_response.assignment.len(), 1);
        assert_eq!(sync_response.assignment[0].partition, 0);
    }

    #[test]
    fn test_heartbeat_detects_invalid_generation() {
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
            .unwrap();

        let result = coordinator.heartbeat(
            "test-group".to_string(),
            response.member_id,
            999, // Wrong generation
        );

        assert!(matches!(
            result,
            Err(CoordinatorError::InvalidGeneration { .. })
        ));
    }

    #[test]
    fn test_leave_group_removes_member() {
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
            .unwrap();

        coordinator
            .leave_group("test-group".to_string(), response.member_id.clone())
            .unwrap();

        // Heartbeat should fail after leaving
        let result = coordinator.heartbeat(
            "test-group".to_string(),
            response.member_id,
            response.generation_id,
        );

        assert!(matches!(result, Err(CoordinatorError::UnknownMember(_))));
    }

    #[test]
    fn test_commit_and_fetch_offset() {
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
            .unwrap();

        // Commit offset
        coordinator
            .commit_offset("test-group".to_string(), "topic-1".to_string(), 0, 42)
            .unwrap();

        // Fetch offset
        let response = coordinator
            .fetch_offset("test-group".to_string(), "topic-1".to_string(), 0)
            .unwrap();

        assert_eq!(response.offset, Some(42));
    }

    #[test]
    fn test_fetch_offset_returns_none_when_not_committed() {
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
            .unwrap();

        let response = coordinator
            .fetch_offset("test-group".to_string(), "topic-1".to_string(), 0)
            .unwrap();

        assert_eq!(response.offset, None);
    }
}
