//! Consumer group coordination handlers
//!
//! Implements JoinGroup, SyncGroup, Heartbeat, and LeaveGroup protocol
//! handlers, bridging the wire protocol to
//! [`rivven_core::consumer_group::ConsumerGroup`].

use crate::group_coordinator::GroupCoordinator;
use crate::protocol::{Response, SyncGroupAssignments};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Handle a JoinGroup request.
///
/// Creates or looks up the consumer group, adds the member, and returns
/// the generation, leader, and member list (the leader uses the member
/// list to compute partition assignments via SyncGroup).
pub async fn handle_join_group(
    coordinator: &Arc<GroupCoordinator>,
    group_id: String,
    member_id: String,
    session_timeout_ms: u32,
    rebalance_timeout_ms: u32,
    protocol_type: String,
    subscriptions: Vec<String>,
) -> Response {
    // Generate member ID if the client doesn't supply one (first join)
    let member_id = if member_id.is_empty() {
        coordinator.generate_member_id(&protocol_type)
    } else {
        member_id
    };

    let session_timeout = std::time::Duration::from_millis(session_timeout_ms as u64);
    let rebalance_timeout = std::time::Duration::from_millis(rebalance_timeout_ms as u64);

    let result = coordinator
        .join_group(
            &group_id,
            &member_id,
            session_timeout,
            rebalance_timeout,
            &protocol_type,
            subscriptions,
        )
        .await;

    match result {
        Ok(join_result) => {
            info!(
                group_id = %group_id,
                member_id = %join_result.member_id,
                generation = join_result.generation_id,
                leader = %join_result.leader_id,
                members = join_result.members.len(),
                "Member joined group"
            );
            Response::JoinGroupResult {
                generation_id: join_result.generation_id,
                protocol_type: join_result.protocol_type,
                member_id: join_result.member_id,
                leader_id: join_result.leader_id,
                members: join_result.members,
            }
        }
        Err(e) => {
            warn!(group_id = %group_id, error = %e, "JoinGroup failed");
            Response::Error {
                message: format!("JOIN_GROUP_FAILED: {}", e),
            }
        }
    }
}

/// Handle a SyncGroup request.
///
/// The group leader sends partition assignments for all members;
/// followers send an empty assignment list.  All members receive
/// their own assignment in the response.
pub async fn handle_sync_group(
    coordinator: &Arc<GroupCoordinator>,
    group_id: String,
    generation_id: u32,
    member_id: String,
    assignments: SyncGroupAssignments,
) -> Response {
    let result = coordinator
        .sync_group(&group_id, generation_id, &member_id, assignments)
        .await;

    match result {
        Ok(my_assignments) => {
            debug!(
                group_id = %group_id,
                member_id = %member_id,
                generation = generation_id,
                partitions = my_assignments.len(),
                "SyncGroup completed"
            );
            Response::SyncGroupResult {
                assignments: my_assignments,
            }
        }
        Err(e) => {
            warn!(group_id = %group_id, member_id = %member_id, error = %e, "SyncGroup failed");
            Response::Error {
                message: format!("SYNC_GROUP_FAILED: {}", e),
            }
        }
    }
}

/// Handle a Heartbeat request.
///
/// Returns error_code 0 on success or 27 (REBALANCE_IN_PROGRESS) when
/// the group is rebalancing and the member should rejoin.
pub async fn handle_heartbeat(
    coordinator: &Arc<GroupCoordinator>,
    group_id: String,
    generation_id: u32,
    member_id: String,
) -> Response {
    let result = coordinator
        .heartbeat(&group_id, generation_id, &member_id)
        .await;

    match result {
        Ok(error_code) => Response::HeartbeatResult { error_code },
        Err(e) => {
            warn!(group_id = %group_id, member_id = %member_id, error = %e, "Heartbeat failed");
            Response::Error {
                message: format!("HEARTBEAT_FAILED: {}", e),
            }
        }
    }
}

/// Handle a LeaveGroup request.
///
/// Removes the member from the group and triggers rebalance if needed.
pub async fn handle_leave_group(
    coordinator: &Arc<GroupCoordinator>,
    group_id: String,
    member_id: String,
) -> Response {
    let result = coordinator.leave_group(&group_id, &member_id).await;

    match result {
        Ok(()) => {
            info!(
                group_id = %group_id,
                member_id = %member_id,
                "Member left group"
            );
            Response::LeaveGroupResult
        }
        Err(e) => {
            warn!(group_id = %group_id, member_id = %member_id, error = %e, "LeaveGroup failed");
            Response::Error {
                message: format!("LEAVE_GROUP_FAILED: {}", e),
            }
        }
    }
}
