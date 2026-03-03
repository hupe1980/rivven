//! Group Coordinator — broker-side consumer group management
//!
//! Wraps [`rivven_core::consumer_group::ConsumerGroup`] behind an
//! `Arc<RwLock<_>>` map and provides the protocol-level operations
//! (JoinGroup, SyncGroup, Heartbeat, LeaveGroup).
//!
//! A background task periodically calls [`GroupCoordinator::check_timeouts`]
//! to expire members that stop sending heartbeats.

use crate::protocol::SyncGroupAssignments;
use rivven_core::consumer_group::{
    AssignmentStrategy, ConsumerGroup, GroupState, PartitionAssignment,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ============================================================================
// Result types
// ============================================================================

/// Returned by [`GroupCoordinator::join_group`].
pub struct JoinGroupResult {
    /// Incremented generation ID.
    pub generation_id: u32,
    /// Protocol type (always `"consumer"`).
    pub protocol_type: String,
    /// Assigned member ID (generated on first join).
    pub member_id: String,
    /// Current group leader.
    pub leader_id: String,
    /// All group members with their subscriptions.
    /// `(member_id, subscribed_topics)`.
    pub members: Vec<(String, Vec<String>)>,
}

// ============================================================================
// GroupCoordinator
// ============================================================================

/// Broker-side coordinator managing all consumer groups.
///
/// Thread-safe behind `Arc`; individual groups are additionally locked
/// inside the outer `RwLock` to minimise contention across groups.
pub struct GroupCoordinator {
    groups: RwLock<HashMap<String, ConsumerGroup>>,
    /// Per-group SyncGroup barrier.
    ///
    /// When the leader calls SyncGroup and applies assignments, it notifies
    /// all waiting followers via this `Notify`. Followers that call SyncGroup
    /// before the leader block here until the leader is done.
    ///
    /// Keyed by `group_id`. A single `Notify` per group is sufficient because
    /// all waiters for a given generation are released together; stale-generation
    /// waiters are rejected by the generation check before reaching the barrier.
    sync_barriers: RwLock<HashMap<String, Arc<Notify>>>,
    /// Default session timeout if not explicitly provided.
    default_session_timeout: Duration,
    /// Default rebalance timeout if not explicitly provided.
    default_rebalance_timeout: Duration,
    /// Default partition assignment strategy (used by describe and future
    /// server-side assignment; currently the client-side leader computes
    /// assignments via SyncGroup).
    #[allow(dead_code)]
    assignment_strategy: AssignmentStrategy,
}

impl GroupCoordinator {
    /// Create a new coordinator with sensible defaults.
    pub fn new() -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            sync_barriers: RwLock::new(HashMap::new()),
            default_session_timeout: Duration::from_secs(10),
            default_rebalance_timeout: Duration::from_secs(30),
            assignment_strategy: AssignmentStrategy::Range,
        }
    }

    /// Create with custom defaults.
    pub fn with_config(
        default_session_timeout: Duration,
        default_rebalance_timeout: Duration,
        assignment_strategy: AssignmentStrategy,
    ) -> Self {
        Self {
            groups: RwLock::new(HashMap::new()),
            sync_barriers: RwLock::new(HashMap::new()),
            default_session_timeout,
            default_rebalance_timeout,
            assignment_strategy,
        }
    }

    /// Generate a unique member ID (UUID-based).
    pub fn generate_member_id(&self, protocol_type: &str) -> String {
        format!("{}-{}", protocol_type, Uuid::new_v4())
    }

    // =========================================================================
    // Protocol operations
    // =========================================================================

    /// Handle a JoinGroup request.
    ///
    /// * Creates the group if it doesn't exist.
    /// * Assigns or reuses a member ID.
    /// * Transitions the group to PreparingRebalance → CompletingRebalance.
    /// * Returns the generation, leader, and member list.
    pub async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        protocol_type: &str,
        subscriptions: Vec<String>,
    ) -> Result<JoinGroupResult, String> {
        // LOCK ORDERING: `groups` lock may be held while acquiring `sync_barriers`
        // (see L170 below). In `sync_group()`, `sync_barriers` is acquired and
        // RELEASED before `groups` is acquired — never held simultaneously.
        // Do NOT hold both locks at once in `sync_group()` to avoid deadlock.
        let mut groups = self.groups.write().await;

        let session_timeout = if session_timeout.is_zero() {
            self.default_session_timeout
        } else {
            session_timeout
        };
        let rebalance_timeout = if rebalance_timeout.is_zero() {
            self.default_rebalance_timeout
        } else {
            rebalance_timeout
        };

        let group = groups.entry(group_id.to_string()).or_insert_with(|| {
            ConsumerGroup::new(group_id.to_string(), session_timeout, rebalance_timeout)
        });

        // Allow re-creation of Dead groups (race between last member leaving
        // and a new consumer joining the same group_id before check_timeouts
        // removes the entry). Instead of rejecting, reset the group to Empty
        // so the new member can join.
        if group.state == GroupState::Dead {
            *group = ConsumerGroup::new(group_id.to_string(), session_timeout, rebalance_timeout);
        }

        // Add the member (handles static membership, fencing, rebalance trigger)
        group.add_member(
            member_id.to_string(),
            String::new(), // client_id — not in the wire message; could be extended later
            subscriptions,
            Vec::new(),
        );

        // Transition: PreparingRebalance → CompletingRebalance
        // The group is now in PreparingRebalance (set by add_member).
        // We immediately move to CompletingRebalance because this is
        // a synchronous protocol — all members must call JoinGroup
        // before proceeding, and we don't have a delayed-rebalance timer.
        if group.state == GroupState::PreparingRebalance {
            group.state = GroupState::CompletingRebalance;

            // Reset the sync barrier for this generation — any previous Notify is
            // stale from the prior rebalance cycle. A fresh Notify ensures followers
            // that call SyncGroup for THIS generation will properly block.
            let mut barriers = self.sync_barriers.write().await;
            barriers.insert(group_id.to_string(), Arc::new(Notify::new()));
        }

        let generation_id = group.generation_id;
        let leader_id = group.leader_id.clone().unwrap_or_default();

        // Build the member list (only the leader needs it, but Kafka sends
        // it to everyone and the leader uses it to compute assignments).
        let members: Vec<(String, Vec<String>)> = group
            .members
            .iter()
            .map(|(mid, m)| (mid.clone(), m.subscriptions.clone()))
            .collect();

        Ok(JoinGroupResult {
            generation_id,
            protocol_type: protocol_type.to_string(),
            member_id: member_id.to_string(),
            leader_id,
            members,
        })
    }

    /// Handle a SyncGroup request.
    ///
    /// * The leader provides assignments for all members.
    /// * Followers send an empty assignment list.
    /// * All members block until the leader's assignments are applied.
    /// * Returns this member's partition assignment.
    pub async fn sync_group(
        &self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
        // leader assignments: Vec<(member_id, Vec<(topic, Vec<partition>)>)>
        assignments: SyncGroupAssignments,
    ) -> Result<Vec<(String, Vec<u32>)>, String> {
        // Obtain (or create) the barrier Notify for this group.
        let barrier = {
            let mut barriers = self.sync_barriers.write().await;
            barriers
                .entry(group_id.to_string())
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone()
        };

        // CRITICAL: Register the `Notified` future BEFORE acquiring the groups
        // lock, so the notification is captured even if `notify_waiters()` fires
        // between the lock drop and the `.await` below.
        //
        // `tokio::sync::Notify::notify_waiters()` only wakes futures that have
        // already been created via `.notified()`. If we created the future after
        // dropping the groups lock (as was done before), there was a race window
        // where the leader could call `notify_waiters()` before the follower
        // registered — losing the notification and stalling for the full
        // rebalance timeout.
        let notified = barrier.notified();
        // Pin the future so it can be polled after the lock is dropped.
        tokio::pin!(notified);
        // Enable the Notified to capture a notification even before we .await it.
        // This call makes the future "listen" immediately.
        notified.as_mut().enable();

        {
            let mut groups = self.groups.write().await;

            let group = groups
                .get_mut(group_id)
                .ok_or_else(|| format!("UNKNOWN_GROUP: '{}'", group_id))?;

            // Validate member exists
            if !group.members.contains_key(member_id) {
                return Err(format!(
                    "UNKNOWN_MEMBER_ID: '{}' in group '{}'",
                    member_id, group_id
                ));
            }

            // Validate generation
            if generation_id != group.generation_id {
                return Err(format!(
                    "ILLEGAL_GENERATION: expected {}, got {}",
                    group.generation_id, generation_id
                ));
            }

            // If this is the leader with assignments, apply them and wake followers
            let is_leader = group.leader_id.as_deref() == Some(member_id);
            if is_leader && !assignments.is_empty() {
                // Validate that assignments only reference known group members
                let current_members: std::collections::HashSet<&str> =
                    group.members.keys().map(|m| m.as_str()).collect();
                for (mid, _) in &assignments {
                    if !current_members.contains(mid.as_str()) {
                        return Err(format!(
                            "INCONSISTENT_GROUP_PROTOCOL: assignment references unknown member '{}'",
                            mid
                        ));
                    }
                }

                // Convert wire format to ConsumerGroup's internal format
                let mut assignment_map: HashMap<String, Vec<PartitionAssignment>> = HashMap::new();
                for (mid, topic_partitions) in &assignments {
                    let mut member_assignments = Vec::new();
                    for (topic, parts) in topic_partitions {
                        for &p in parts {
                            member_assignments.push(PartitionAssignment {
                                topic: topic.clone(),
                                partition: p,
                            });
                        }
                    }
                    assignment_map.insert(mid.clone(), member_assignments);
                }

                group.complete_rebalance(assignment_map);

                debug!(
                    group_id = %group_id,
                    generation = group.generation_id,
                    "Leader applied partition assignments"
                );

                // Wake all followers waiting on the barrier
                barrier.notify_waiters();

                // Return leader's own assignment
                let my_assignments = self.get_member_assignments_wire(group, member_id);
                return Ok(my_assignments);
            }

            // If the group is already Stable (leader already synced), return
            // immediately — no need to wait.
            if group.state == GroupState::Stable {
                let my_assignments = self.get_member_assignments_wire(group, member_id);
                return Ok(my_assignments);
            }

            // Follower: leader hasn't synced yet. Drop the lock and wait.
        }

        // ── Follower barrier wait ──
        // Wait for the leader to apply assignments (with a timeout to avoid
        // indefinite hangs if the leader crashes or leaves).
        // The `notified` future was registered BEFORE the lock was acquired,
        // so even if `notify_waiters()` fired while we held the lock, the
        // notification is captured and this will return immediately.
        let wait_result = tokio::time::timeout(self.default_rebalance_timeout, notified).await;

        if wait_result.is_err() {
            return Err(format!(
                "REBALANCE_TIMEOUT: leader did not sync within {:?} for group '{}'",
                self.default_rebalance_timeout, group_id
            ));
        }

        // Re-acquire the lock and return our assignment
        let groups = self.groups.read().await;
        let group = groups
            .get(group_id)
            .ok_or_else(|| format!("UNKNOWN_GROUP: '{}'", group_id))?;

        // Re-validate generation — a new rebalance may have started while we waited
        if generation_id != group.generation_id {
            return Err(format!(
                "ILLEGAL_GENERATION: expected {}, got {} (rebalance occurred while waiting)",
                group.generation_id, generation_id
            ));
        }

        let my_assignments = self.get_member_assignments_wire(group, member_id);
        Ok(my_assignments)
    }

    /// Handle a Heartbeat request.
    ///
    /// Returns:
    /// * `0` — OK
    /// * `27` — REBALANCE_IN_PROGRESS (member should rejoin)
    pub async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: u32,
        member_id: &str,
    ) -> Result<i32, String> {
        let mut groups = self.groups.write().await;

        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| format!("UNKNOWN_GROUP: '{}'", group_id))?;

        if !group.members.contains_key(member_id) {
            return Err(format!(
                "UNKNOWN_MEMBER_ID: '{}' in group '{}'",
                member_id, group_id
            ));
        }

        if generation_id != group.generation_id {
            return Err(format!(
                "ILLEGAL_GENERATION: expected {}, got {}",
                group.generation_id, generation_id
            ));
        }

        group.heartbeat(member_id).map_err(|e| e.to_string())?;

        // Signal rebalance if group is not stable
        let error_code = match group.state {
            GroupState::Stable => 0,
            _ => 27, // REBALANCE_IN_PROGRESS
        };

        Ok(error_code)
    }

    /// Handle a LeaveGroup request.
    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<(), String> {
        let mut groups = self.groups.write().await;

        let group = groups
            .get_mut(group_id)
            .ok_or_else(|| format!("UNKNOWN_GROUP: '{}'", group_id))?;

        if !group.remove_member(member_id) {
            return Err(format!(
                "UNKNOWN_MEMBER_ID: '{}' in group '{}'",
                member_id, group_id
            ));
        }

        info!(
            group_id = %group_id,
            member_id = %member_id,
            remaining_members = group.members.len(),
            "Member left consumer group"
        );

        // If group is now empty, mark as Dead for cleanup
        if group.members.is_empty() {
            group.state = GroupState::Dead;
            debug!(group_id = %group_id, "Group is empty, marked Dead");
        }

        Ok(())
    }

    // =========================================================================
    // Heartbeat timeout monitoring
    // =========================================================================

    /// Check all groups for timed-out members.
    ///
    /// Call this periodically from a background task (e.g. every 1 second).
    /// Returns the number of members that were expired.
    pub async fn check_timeouts(&self) -> usize {
        let mut groups = self.groups.write().await;
        let mut total_expired = 0;

        // Collect dead group IDs for cleanup
        let mut dead_groups = Vec::new();

        for (group_id, group) in groups.iter_mut() {
            if group.state == GroupState::Dead || group.state == GroupState::Empty {
                if group.members.is_empty() {
                    dead_groups.push(group_id.clone());
                }
                continue;
            }

            let expired = group.check_timeouts();
            if !expired.is_empty() {
                warn!(
                    group_id = %group_id,
                    expired_members = ?expired,
                    remaining = group.members.len(),
                    "Expired timed-out group members"
                );
                total_expired += expired.len();
            }

            // Also check pending static member timeouts (2× session timeout)
            let pending_timeout = group.session_timeout * 2;
            let pending_expired = group.check_pending_static_timeouts(pending_timeout);
            if !pending_expired.is_empty() {
                debug!(
                    group_id = %group_id,
                    expired_instances = ?pending_expired,
                    "Expired pending static members"
                );
            }
        }

        // Remove dead/empty groups and their sync barriers to prevent
        // unbounded memory growth from transient consumer groups.
        if !dead_groups.is_empty() {
            for id in &dead_groups {
                groups.remove(id);
            }
            // Release groups lock before acquiring barriers lock to
            // maintain the lock ordering invariant.
            drop(groups);
            let mut barriers = self.sync_barriers.write().await;
            for id in &dead_groups {
                barriers.remove(id);
            }
        }

        total_expired
    }

    /// Spawn a background task that periodically checks heartbeat timeouts.
    ///
    /// Runs every `interval` until the returned handle is dropped.
    pub fn spawn_heartbeat_checker(
        self: &Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                coordinator.check_timeouts().await;
            }
        })
    }

    // =========================================================================
    // Dashboard / describe helpers
    // =========================================================================

    /// Get the state of a group as a string (for dashboard/describe APIs).
    pub async fn get_group_state(&self, group_id: &str) -> Option<String> {
        let groups = self.groups.read().await;
        groups.get(group_id).map(|g| format!("{:?}", g.state))
    }

    /// Get the member count for a group (for dashboard).
    pub async fn get_member_count(&self, group_id: &str) -> usize {
        let groups = self.groups.read().await;
        groups.get(group_id).map(|g| g.members.len()).unwrap_or(0)
    }

    /// Get detailed group info: (state, member_count, generation_id).
    pub async fn describe_group(&self, group_id: &str) -> Option<(String, usize, u32)> {
        let groups = self.groups.read().await;
        groups
            .get(group_id)
            .map(|g| (format!("{:?}", g.state), g.members.len(), g.generation_id))
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Convert a member's PartitionAssignment list to the wire format:
    /// `Vec<(topic, Vec<partition>)>`.
    fn get_member_assignments_wire(
        &self,
        group: &ConsumerGroup,
        member_id: &str,
    ) -> Vec<(String, Vec<u32>)> {
        let member = match group.members.get(member_id) {
            Some(m) => m,
            None => return Vec::new(),
        };

        // Group assignments by topic
        let mut by_topic: HashMap<String, Vec<u32>> = HashMap::new();
        for pa in &member.assignment {
            by_topic
                .entry(pa.topic.clone())
                .or_default()
                .push(pa.partition);
        }

        by_topic.into_iter().collect()
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_join_leave_lifecycle() {
        let coordinator = Arc::new(GroupCoordinator::new());

        // Join
        let result = coordinator
            .join_group(
                "test-group",
                "member-1",
                Duration::from_secs(10),
                Duration::from_secs(30),
                "consumer",
                vec!["topic-a".into()],
            )
            .await
            .unwrap();

        assert_eq!(result.member_id, "member-1");
        assert_eq!(result.leader_id, "member-1");
        assert_eq!(result.members.len(), 1);

        // Heartbeat
        let code = coordinator
            .heartbeat("test-group", result.generation_id, "member-1")
            .await
            .unwrap();
        // Group is in CompletingRebalance (not yet synced), so code is 27
        assert_eq!(code, 27);

        // Leave
        coordinator
            .leave_group("test-group", "member-1")
            .await
            .unwrap();

        // Group should be dead/cleaned up
        assert_eq!(coordinator.get_member_count("test-group").await, 0);
    }

    #[tokio::test]
    async fn test_sync_group_leader_applies_assignments() {
        let coordinator = Arc::new(GroupCoordinator::new());

        // Two members join
        let r1 = coordinator
            .join_group(
                "grp",
                "m1",
                Duration::from_secs(10),
                Duration::from_secs(30),
                "consumer",
                vec!["t1".into()],
            )
            .await
            .unwrap();

        let r2 = coordinator
            .join_group(
                "grp",
                "m2",
                Duration::from_secs(10),
                Duration::from_secs(30),
                "consumer",
                vec!["t1".into()],
            )
            .await
            .unwrap();

        // Both members joined during the same rebalance cycle (before Stable),
        // so they share the same generation.
        assert_eq!(r1.generation_id, r2.generation_id);
        let gen = r1.generation_id;

        // Leader (m1) syncs with assignments
        let assignments = vec![
            ("m1".into(), vec![("t1".into(), vec![0, 1])]),
            ("m2".into(), vec![("t1".into(), vec![2, 3])]),
        ];

        let my = coordinator
            .sync_group("grp", gen, "m1", assignments)
            .await
            .unwrap();
        assert!(!my.is_empty()); // m1 got partitions 0,1

        // Follower syncs using the generation from join_group response
        // (NOT reading from live server state) — this validates the
        // generation_id is consistent across leader and follower sync.
        let my2 = coordinator
            .sync_group("grp", r2.generation_id, "m2", vec![])
            .await
            .unwrap();
        assert!(!my2.is_empty()); // m2 got partitions 2,3
    }

    /// Verify that a follower calling SyncGroup BEFORE the leader
    /// blocks until the leader applies assignments (the barrier works).
    #[tokio::test]
    async fn test_sync_group_follower_waits_for_leader() {
        let coordinator = Arc::new(GroupCoordinator::new());

        // Two members join
        let r1 = coordinator
            .join_group(
                "grp-barrier",
                "m1",
                Duration::from_secs(10),
                Duration::from_secs(30),
                "consumer",
                vec!["t1".into()],
            )
            .await
            .unwrap();

        let _r2 = coordinator
            .join_group(
                "grp-barrier",
                "m2",
                Duration::from_secs(10),
                Duration::from_secs(30),
                "consumer",
                vec!["t1".into()],
            )
            .await
            .unwrap();

        let gen = r1.generation_id;
        let coord = Arc::clone(&coordinator);

        // Follower calls SyncGroup FIRST (before leader)
        let follower_handle =
            tokio::spawn(async move { coord.sync_group("grp-barrier", gen, "m2", vec![]).await });

        // Small delay to ensure follower is waiting
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Leader syncs with assignments
        let assignments = vec![
            ("m1".into(), vec![("t1".into(), vec![0, 1])]),
            ("m2".into(), vec![("t1".into(), vec![2, 3])]),
        ];

        let leader_result = coordinator
            .sync_group("grp-barrier", gen, "m1", assignments)
            .await
            .unwrap();
        assert!(!leader_result.is_empty());

        // Follower should now unblock and get its assignments
        let follower_result = follower_handle.await.unwrap().unwrap();
        assert!(!follower_result.is_empty());

        // Verify the follower got the correct partitions
        let follower_partitions: Vec<u32> = follower_result
            .iter()
            .flat_map(|(_, parts)| parts.iter().copied())
            .collect();
        assert!(follower_partitions.contains(&2));
        assert!(follower_partitions.contains(&3));
    }

    #[tokio::test]
    async fn test_unknown_group_heartbeat() {
        let coordinator = Arc::new(GroupCoordinator::new());
        let result = coordinator.heartbeat("no-such-group", 0, "m").await;
        assert!(result.is_err());
    }
}
