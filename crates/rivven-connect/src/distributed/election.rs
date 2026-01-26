//! Leader election for singleton connectors
//!
//! Uses a coordination topic for distributed leader election.
//! Only one node can be leader for a given connector at a time.

use crate::distributed::types::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

/// Election state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionState {
    /// Not participating in election
    Idle,
    /// Candidate seeking votes
    Candidate,
    /// Won election, am the leader
    Leader,
    /// Following another leader
    Follower,
}

/// Information about the current leader
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderInfo {
    /// Leader node ID
    pub node_id: NodeId,
    /// Connector this leader is for
    pub connector_id: ConnectorId,
    /// Election generation
    pub generation: Generation,
    /// When the leader was elected
    pub elected_at: i64,
    /// Last heartbeat time
    pub last_heartbeat: i64,
}

impl LeaderInfo {
    pub fn new(node_id: NodeId, connector_id: ConnectorId, generation: Generation) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            node_id,
            connector_id,
            generation,
            elected_at: now,
            last_heartbeat: now,
        }
    }
}

use serde::{Deserialize, Serialize};

/// Leader election for a specific connector
pub struct LeaderElection {
    /// Our node ID
    node_id: NodeId,
    /// Connector ID
    connector_id: ConnectorId,
    /// Current state
    state: RwLock<ElectionState>,
    /// Current generation
    generation: RwLock<Generation>,
    /// Current leader info (if any)
    leader: RwLock<Option<LeaderInfo>>,
    /// Failure timeout
    failure_timeout: Duration,
    /// Event sender for state changes
    event_tx: broadcast::Sender<ElectionEvent>,
}

/// Election events
#[derive(Debug, Clone)]
pub enum ElectionEvent {
    /// We became the leader
    BecameLeader(Generation),
    /// We lost leadership
    LostLeadership,
    /// New leader elected
    NewLeader(LeaderInfo),
    /// Leader failed (timeout)
    LeaderFailed(NodeId),
}

impl LeaderElection {
    /// Create a new leader election
    pub fn new(node_id: NodeId, connector_id: ConnectorId, failure_timeout: Duration) -> Self {
        let (event_tx, _) = broadcast::channel(16);

        Self {
            node_id,
            connector_id,
            state: RwLock::new(ElectionState::Idle),
            generation: RwLock::new(Generation::new(0)),
            leader: RwLock::new(None),
            failure_timeout,
            event_tx,
        }
    }

    /// Get current state
    pub fn state(&self) -> ElectionState {
        *self.state.read()
    }

    /// Get current generation
    pub fn generation(&self) -> Generation {
        *self.generation.read()
    }

    /// Check if we are the leader
    pub fn is_leader(&self) -> bool {
        *self.state.read() == ElectionState::Leader
    }

    /// Get current leader info
    pub fn leader(&self) -> Option<LeaderInfo> {
        self.leader.read().clone()
    }

    /// Subscribe to election events
    pub fn subscribe(&self) -> broadcast::Receiver<ElectionEvent> {
        self.event_tx.subscribe()
    }

    /// Start an election
    pub fn start_election(&self) -> DistributedResult<()> {
        let mut state = self.state.write();
        let mut generation = self.generation.write();

        match *state {
            ElectionState::Idle | ElectionState::Follower => {
                *state = ElectionState::Candidate;
                generation.increment();
                tracing::info!(
                    connector = %self.connector_id,
                    generation = %*generation,
                    "Starting election"
                );
                Ok(())
            }
            ElectionState::Candidate => {
                // Already in election
                Ok(())
            }
            ElectionState::Leader => {
                // Already leader
                Ok(())
            }
        }
    }

    /// Claim leadership (after winning election)
    pub fn claim_leadership(&self, generation: Generation) -> DistributedResult<()> {
        let mut state = self.state.write();
        let mut current_gen = self.generation.write();
        let mut leader = self.leader.write();

        // Must be candidate and generation must match
        if *state != ElectionState::Candidate {
            return Err(DistributedError::InvalidStateTransition(format!(
                "Cannot claim leadership from state {:?}",
                *state
            )));
        }

        if generation < *current_gen {
            return Err(DistributedError::InvalidStateTransition(format!(
                "Stale generation {} < {}",
                generation, *current_gen
            )));
        }

        *state = ElectionState::Leader;
        *current_gen = generation;
        *leader = Some(LeaderInfo::new(
            self.node_id.clone(),
            self.connector_id.clone(),
            generation,
        ));

        let _ = self.event_tx.send(ElectionEvent::BecameLeader(generation));

        tracing::info!(
            connector = %self.connector_id,
            generation = %generation,
            "Claimed leadership"
        );

        Ok(())
    }

    /// Accept a new leader
    pub fn accept_leader(&self, leader_info: LeaderInfo) -> DistributedResult<()> {
        let mut state = self.state.write();
        let mut generation = self.generation.write();
        let mut leader = self.leader.write();

        // Check generation
        if leader_info.generation < *generation {
            return Err(DistributedError::InvalidStateTransition(format!(
                "Stale leader generation {} < {}",
                leader_info.generation, *generation
            )));
        }

        // If we were leader, we lost it
        if *state == ElectionState::Leader {
            let _ = self.event_tx.send(ElectionEvent::LostLeadership);
            tracing::warn!(
                connector = %self.connector_id,
                new_leader = %leader_info.node_id,
                "Lost leadership"
            );
        }

        *state = ElectionState::Follower;
        *generation = leader_info.generation;

        let _ = self
            .event_tx
            .send(ElectionEvent::NewLeader(leader_info.clone()));

        *leader = Some(leader_info);

        Ok(())
    }

    /// Process a heartbeat from the leader
    pub fn process_heartbeat(
        &self,
        leader_id: &NodeId,
        generation: Generation,
    ) -> DistributedResult<()> {
        let mut leader = self.leader.write();
        let current_gen = self.generation.read();

        if generation < *current_gen {
            return Err(DistributedError::InvalidStateTransition(
                "Stale heartbeat".to_string(),
            ));
        }

        if let Some(ref mut info) = *leader {
            if &info.node_id == leader_id {
                info.last_heartbeat = chrono::Utc::now().timestamp_millis();
            }
        }

        Ok(())
    }

    /// Check if leader has failed (timeout)
    pub fn check_leader_timeout(&self) -> Option<NodeId> {
        let state = self.state.read();
        let leader = self.leader.read();

        if *state != ElectionState::Follower {
            return None;
        }

        if let Some(ref info) = *leader {
            let now = chrono::Utc::now().timestamp_millis();
            let elapsed = Duration::from_millis((now - info.last_heartbeat) as u64);

            if elapsed > self.failure_timeout {
                return Some(info.node_id.clone());
            }
        }

        None
    }

    /// Step down from leadership
    pub fn step_down(&self) {
        let mut state = self.state.write();
        let mut leader = self.leader.write();

        if *state == ElectionState::Leader {
            *state = ElectionState::Follower;
            *leader = None;

            let _ = self.event_tx.send(ElectionEvent::LostLeadership);

            tracing::info!(
                connector = %self.connector_id,
                "Stepped down from leadership"
            );
        }
    }

    /// Reset election state
    pub fn reset(&self) {
        let mut state = self.state.write();
        let mut leader = self.leader.write();

        if *state == ElectionState::Leader {
            let _ = self.event_tx.send(ElectionEvent::LostLeadership);
        }

        *state = ElectionState::Idle;
        *leader = None;
    }
}

/// Manager for multiple leader elections (one per singleton connector)
#[allow(dead_code)] // Part of distributed mode infrastructure
pub struct ElectionManager {
    /// Our node ID
    node_id: NodeId,
    /// Elections by connector
    elections: RwLock<HashMap<ConnectorId, Arc<LeaderElection>>>,
    /// Default failure timeout
    failure_timeout: Duration,
}

#[allow(dead_code)] // Part of distributed mode infrastructure
impl ElectionManager {
    pub fn new(node_id: NodeId, failure_timeout: Duration) -> Self {
        Self {
            node_id,
            elections: RwLock::new(HashMap::new()),
            failure_timeout,
        }
    }

    /// Get or create election for a connector
    pub fn get_or_create(&self, connector_id: &ConnectorId) -> Arc<LeaderElection> {
        {
            let elections = self.elections.read();
            if let Some(election) = elections.get(connector_id) {
                return election.clone();
            }
        }

        let mut elections = self.elections.write();
        elections
            .entry(connector_id.clone())
            .or_insert_with(|| {
                Arc::new(LeaderElection::new(
                    self.node_id.clone(),
                    connector_id.clone(),
                    self.failure_timeout,
                ))
            })
            .clone()
    }

    /// Get election for a connector
    pub fn get(&self, connector_id: &ConnectorId) -> Option<Arc<LeaderElection>> {
        let elections = self.elections.read();
        elections.get(connector_id).cloned()
    }

    /// Remove election for a connector
    pub fn remove(&self, connector_id: &ConnectorId) -> Option<Arc<LeaderElection>> {
        let mut elections = self.elections.write();
        elections.remove(connector_id)
    }

    /// Get all connectors where we are leader
    pub fn get_leaderships(&self) -> Vec<ConnectorId> {
        let elections = self.elections.read();
        elections
            .iter()
            .filter(|(_, e)| e.is_leader())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check all elections for timeouts
    pub fn check_timeouts(&self) -> Vec<(ConnectorId, NodeId)> {
        let elections = self.elections.read();
        elections
            .iter()
            .filter_map(|(id, e)| e.check_leader_timeout().map(|node| (id.clone(), node)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_election_lifecycle() {
        let node = NodeId::new("node-1");
        let connector = ConnectorId::new("test-cdc");
        let election = LeaderElection::new(node.clone(), connector, Duration::from_secs(10));

        // Initial state
        assert_eq!(election.state(), ElectionState::Idle);
        assert!(!election.is_leader());

        // Start election
        election.start_election().unwrap();
        assert_eq!(election.state(), ElectionState::Candidate);
        assert_eq!(election.generation(), Generation::new(1));

        // Claim leadership
        election.claim_leadership(Generation::new(1)).unwrap();
        assert_eq!(election.state(), ElectionState::Leader);
        assert!(election.is_leader());
        assert!(election.leader().is_some());

        // Step down
        election.step_down();
        assert_eq!(election.state(), ElectionState::Follower);
        assert!(!election.is_leader());
    }

    #[test]
    fn test_accept_leader() {
        let node1 = NodeId::new("node-1");
        let node2 = NodeId::new("node-2");
        let connector = ConnectorId::new("test-cdc");

        let election =
            LeaderElection::new(node1.clone(), connector.clone(), Duration::from_secs(10));

        // Accept external leader
        let leader_info = LeaderInfo::new(node2.clone(), connector, Generation::new(5));
        election.accept_leader(leader_info).unwrap();

        assert_eq!(election.state(), ElectionState::Follower);
        assert_eq!(election.generation(), Generation::new(5));
        assert_eq!(election.leader().unwrap().node_id, node2);
    }

    #[test]
    fn test_stale_generation_rejected() {
        let node = NodeId::new("node-1");
        let connector = ConnectorId::new("test-cdc");
        let election =
            LeaderElection::new(node.clone(), connector.clone(), Duration::from_secs(10));

        // Start election - this sets generation to 1
        election.start_election().unwrap();
        assert_eq!(election.generation(), Generation::new(1));

        // Claim leadership to become leader, then we can start again
        election.claim_leadership(Generation::new(1)).unwrap();

        // Step down and start a new election (generation becomes 2)
        election.step_down();
        election.start_election().unwrap();
        assert_eq!(election.generation(), Generation::new(2));

        // Try to accept stale leader with generation 1
        let leader_info = LeaderInfo::new(NodeId::new("node-2"), connector, Generation::new(1));
        let result = election.accept_leader(leader_info);
        assert!(result.is_err()); // Should reject because gen 1 < gen 2
    }
}
