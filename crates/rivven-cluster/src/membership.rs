//! SWIM protocol implementation for cluster membership
//!
//! SWIM (Scalable Weakly-consistent Infection-style Process Group Membership Protocol)
//! provides:
//! - O(1) message complexity per node per protocol period
//! - Failure detection in O(log N) time
//! - No single point of failure (no leader required for membership)
//!
//! Reference: <https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf>

use crate::config::SwimConfig;
use crate::error::{ClusterError, Result};
use crate::node::{Node, NodeGossipState, NodeId, NodeInfo, NodeState};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, trace, warn};

#[cfg(feature = "swim")]
use hmac::{Hmac, Mac};
#[cfg(feature = "swim")]
use sha2::Sha256;

/// HMAC tag length in bytes (SHA-256 → 32 bytes)
const HMAC_TAG_LEN: usize = 32;

/// Maximum gossip items piggybacked per message
const MAX_GOSSIP_PER_MSG: usize = 8;

/// A gossip item queued for dissemination via piggyback
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GossipItem {
    /// The state-change message to disseminate
    message: SwimMessage,
    /// Number of times this item has been piggybacked
    #[serde(skip)]
    transmissions: u32,
}

/// SWIM message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwimMessage {
    /// Direct ping to check liveness
    Ping { source: NodeId, incarnation: u64 },
    /// Response to ping
    Ack { source: NodeId, incarnation: u64 },
    /// Request indirect ping through another node
    PingReq {
        source: NodeId,
        target: NodeId,
        incarnation: u64,
    },
    /// State sync request
    Sync {
        source: NodeId,
        states: Vec<NodeGossipState>,
    },
    /// Node joining announcement
    Join { info: NodeInfo },
    /// Node leaving announcement
    Leave { node_id: NodeId, incarnation: u64 },
    /// Suspect a node
    Suspect {
        node_id: NodeId,
        incarnation: u64,
        from: NodeId,
    },
    /// Refute a suspicion
    Alive { node_id: NodeId, incarnation: u64 },
    /// Confirm a node is dead
    Dead { node_id: NodeId, incarnation: u64 },
}

/// Membership change event
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// A new node joined the cluster
    NodeJoined(NodeInfo),
    /// A node left the cluster gracefully
    NodeLeft(NodeId),
    /// A node failed (detected as dead)
    NodeFailed(NodeId),
    /// A node is suspected to be failing
    NodeSuspected(NodeId),
    /// A suspected node recovered
    NodeRecovered(NodeId),
    /// A node's state changed
    NodeStateChanged {
        node_id: NodeId,
        old: NodeState,
        new: NodeState,
    },
}

/// Cluster membership manager using SWIM protocol
pub struct Membership {
    /// Our node info
    local_node: NodeInfo,

    /// Our incarnation number (increases when we refute suspicion)
    incarnation: Arc<RwLock<u64>>,

    /// All known cluster members
    members: Arc<DashMap<NodeId, Node>>,

    /// SWIM configuration
    config: SwimConfig,

    /// UDP socket for gossip
    socket: Arc<UdpSocket>,

    /// Pending pings awaiting ack
    pending_pings: Arc<DashMap<NodeId, Instant>>,

    /// Pending indirect ping requests: key = (requester, target), value = (requester_addr, timestamp)
    pending_ping_reqs: Arc<DashMap<(NodeId, NodeId), (SocketAddr, Instant)>>,

    /// Event broadcaster
    event_tx: broadcast::Sender<MembershipEvent>,

    /// Shutdown signal receiver (held for lifecycle management)
    #[allow(dead_code)]
    shutdown: broadcast::Receiver<()>,

    /// Pre-validated HMAC key for message authentication (None = no auth)
    hmac_key: Option<String>,

    /// Gossip dissemination queue — items piggyback on Ping/Ack messages
    gossip_queue: Arc<tokio::sync::Mutex<Vec<GossipItem>>>,
}

impl Membership {
    /// Create new membership manager
    pub async fn new(
        local_node: NodeInfo,
        config: SwimConfig,
        shutdown: broadcast::Receiver<()>,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(local_node.cluster_addr)
            .await
            .map_err(|e| ClusterError::Network(e.to_string()))?;

        let (event_tx, _) = broadcast::channel(1000);

        let members = Arc::new(DashMap::new());

        // Add ourselves
        let mut self_node = Node::new(local_node.clone());
        self_node.mark_alive(0);
        members.insert(local_node.id.clone(), self_node);

        // Pre-compute HMAC key if auth is configured
        #[cfg(feature = "swim")]
        let hmac_key = config
            .auth_token
            .as_ref()
            .map(|token| {
                Hmac::<Sha256>::new_from_slice(token.as_bytes())
                    .expect("HMAC can accept key of any length")
            })
            .map(|_| config.auth_token.clone().unwrap());

        #[cfg(not(feature = "swim"))]
        let hmac_key: Option<String> = None;

        Ok(Self {
            local_node,
            incarnation: Arc::new(RwLock::new(0)),
            members: members.clone(),
            config,
            socket: Arc::new(socket),
            pending_pings: Arc::new(DashMap::new()),
            pending_ping_reqs: Arc::new(DashMap::new()),
            event_tx,
            shutdown,
            hmac_key,
            gossip_queue: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }

    /// Send a SWIM message to a target address with optional HMAC authentication
    async fn send_message(&self, msg: &SwimMessage, addr: SocketAddr) -> Result<()> {
        let data = postcard::to_allocvec(msg)?;
        let packet = self.sign_message(&data);
        self.socket.send_to(&packet, addr).await?;
        Ok(())
    }

    /// Sign a serialized message by appending an HMAC-SHA256 tag
    fn sign_message(&self, data: &[u8]) -> Vec<u8> {
        #[cfg(feature = "swim")]
        if let Some(ref key) = self.hmac_key {
            let mut mac =
                Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC key always valid");
            mac.update(data);
            let tag = mac.finalize().into_bytes();
            let mut signed = Vec::with_capacity(data.len() + HMAC_TAG_LEN);
            signed.extend_from_slice(data);
            signed.extend_from_slice(&tag);
            return signed;
        }
        data.to_vec()
    }

    /// Verify and strip an HMAC-SHA256 tag from received data.
    /// Returns the payload (without tag) on success.
    fn verify_message<'a>(&self, data: &'a [u8]) -> std::result::Result<&'a [u8], &'static str> {
        #[cfg(feature = "swim")]
        if let Some(ref key) = self.hmac_key {
            if data.len() < HMAC_TAG_LEN {
                return Err("message too short for HMAC tag");
            }
            let (payload, tag) = data.split_at(data.len() - HMAC_TAG_LEN);
            let mut mac =
                Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC key always valid");
            mac.update(payload);
            mac.verify_slice(tag)
                .map_err(|_| "HMAC verification failed")?;
            return Ok(payload);
        }
        Ok(data)
    }

    /// Subscribe to membership events
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipEvent> {
        self.event_tx.subscribe()
    }

    /// Get all healthy members
    pub fn healthy_members(&self) -> Vec<Node> {
        self.members
            .iter()
            .filter(|r| r.value().is_healthy())
            .map(|r| r.value().clone())
            .collect()
    }

    /// Get a specific member
    pub fn get_member(&self, node_id: &NodeId) -> Option<Node> {
        self.members.get(node_id).map(|r| r.value().clone())
    }

    /// Get member count
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Get healthy member count
    pub fn healthy_count(&self) -> usize {
        self.members
            .iter()
            .filter(|r| r.value().is_healthy())
            .count()
    }

    /// Join cluster via seed nodes
    pub async fn join(&self, seeds: &[String]) -> Result<()> {
        if seeds.is_empty() {
            return Err(ClusterError::NoSeedNodes);
        }

        let join_msg = SwimMessage::Join {
            info: self.local_node.clone(),
        };
        let mut joined = false;
        for seed in seeds {
            let addr: SocketAddr = match seed.parse() {
                Ok(a) => a,
                Err(_) => {
                    warn!("Invalid seed address: {}", seed);
                    continue;
                }
            };

            // Skip if this is ourselves
            if addr == self.local_node.cluster_addr {
                continue;
            }

            match self.send_message(&join_msg, addr).await {
                Ok(()) => {
                    info!("Sent join request to seed {}", seed);
                    joined = true;
                }
                Err(e) => {
                    warn!("Failed to contact seed {}: {}", seed, e);
                }
            }
        }

        if !joined {
            return Err(ClusterError::JoinFailed(
                "Could not contact any seed nodes".into(),
            ));
        }

        Ok(())
    }

    /// Gracefully leave the cluster
    pub async fn leave(&self) -> Result<()> {
        let incarnation = *self.incarnation.read().await;
        let leave_msg = SwimMessage::Leave {
            node_id: self.local_node.id.clone(),
            incarnation,
        };

        // Broadcast to all known members
        self.broadcast(&leave_msg).await?;

        info!("Sent leave announcements to cluster");
        Ok(())
    }

    /// Start the SWIM protocol
    pub async fn run(self) -> Result<()> {
        let membership = Arc::new(self);

        // Spawn receiver task
        let recv_membership = membership.clone();
        let mut recv_handle = tokio::spawn(async move { recv_membership.run_receiver().await });

        // Spawn failure detector task
        let detector_membership = membership.clone();
        let mut detector_handle =
            tokio::spawn(async move { detector_membership.run_failure_detector().await });

        // Spawn sync task
        let sync_membership = membership.clone();
        let mut sync_handle = tokio::spawn(async move { sync_membership.run_sync().await });

        // Wait for any task to complete
        tokio::select! {
            r = &mut recv_handle => {
                error!("Receiver task ended: {:?}", r);
            }
            r = &mut detector_handle => {
                error!("Failure detector task ended: {:?}", r);
            }
            r = &mut sync_handle => {
                error!("Sync task ended: {:?}", r);
            }
        }

        // Abort all remaining tasks to prevent leaks
        recv_handle.abort();
        detector_handle.abort();
        sync_handle.abort();

        Ok(())
    }

    /// Receive and process SWIM messages
    async fn run_receiver(&self) -> Result<()> {
        let mut buf = vec![0u8; 65536];

        // Rate-limit inbound UDP processing to prevent a flood from
        // exhausting CPU. 10,000 messages/sec is well above normal SWIM traffic
        // but caps attack surface during a UDP flood.
        const MAX_MSGS_PER_SEC: u32 = 10_000;
        let mut msg_count: u32 = 0;
        let mut window_start = tokio::time::Instant::now();

        loop {
            // Reset counter every second
            if window_start.elapsed() >= std::time::Duration::from_secs(1) {
                msg_count = 0;
                window_start = tokio::time::Instant::now();
            }

            if msg_count >= MAX_MSGS_PER_SEC {
                // Budget exhausted — sleep until next window
                tokio::time::sleep_until(window_start + std::time::Duration::from_secs(1)).await;
                continue;
            }

            let (len, from) = match self.socket.recv_from(&mut buf).await {
                Ok(r) => r,
                Err(e) => {
                    error!("Socket recv error: {}", e);
                    continue;
                }
            };

            msg_count += 1;

            // Verify HMAC if authentication is enabled
            let payload = match self.verify_message(&buf[..len]) {
                Ok(p) => p,
                Err(reason) => {
                    warn!("Dropping unauthenticated message from {}: {}", from, reason);
                    continue;
                }
            };

            let msg: SwimMessage = match postcard::from_bytes(payload) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Failed to deserialize message from {}: {}", from, e);
                    continue;
                }
            };

            trace!("Received {:?} from {}", msg, from);

            if let Err(e) = self.handle_message(msg, from).await {
                warn!("Error handling message from {}: {}", from, e);
            }
        }
    }

    /// Handle incoming SWIM message
    async fn handle_message(&self, msg: SwimMessage, from: SocketAddr) -> Result<()> {
        match msg {
            SwimMessage::Ping {
                source,
                incarnation,
            } => {
                self.handle_ping(&source, incarnation, from).await?;
            }
            SwimMessage::Ack {
                source,
                incarnation,
            } => {
                self.handle_ack(&source, incarnation).await?;
            }
            SwimMessage::PingReq {
                source,
                target,
                incarnation,
            } => {
                self.handle_ping_req(&source, &target, incarnation, from)
                    .await?;
            }
            SwimMessage::Sync { source, states } => {
                self.handle_sync(&source, states).await?;
            }
            SwimMessage::Join { info } => {
                self.handle_join(info).await?;
            }
            SwimMessage::Leave {
                node_id,
                incarnation,
            } => {
                self.handle_leave(&node_id, incarnation).await?;
            }
            SwimMessage::Suspect {
                node_id,
                incarnation,
                from: suspect_from,
            } => {
                self.handle_suspect(&node_id, incarnation, &suspect_from)
                    .await?;
            }
            SwimMessage::Alive {
                node_id,
                incarnation,
            } => {
                self.handle_alive(&node_id, incarnation).await?;
            }
            SwimMessage::Dead {
                node_id,
                incarnation,
            } => {
                self.handle_dead(&node_id, incarnation).await?;
            }
        }
        Ok(())
    }

    /// Handle ping message
    async fn handle_ping(
        &self,
        source: &NodeId,
        _incarnation: u64,
        from: SocketAddr,
    ) -> Result<()> {
        let our_incarnation = *self.incarnation.read().await;
        let ack = SwimMessage::Ack {
            source: self.local_node.id.clone(),
            incarnation: our_incarnation,
        };
        // Send Ack with piggybacked gossip
        self.send_with_gossip(&ack, from).await?;

        // Update member last seen
        if let Some(mut member) = self.members.get_mut(source) {
            member.touch();
        }

        Ok(())
    }

    /// Handle ack message
    async fn handle_ack(&self, source: &NodeId, incarnation: u64) -> Result<()> {
        // Remove from pending pings
        self.pending_pings.remove(source);

        // Forward ack to any requester waiting for an indirect probe result
        let mut to_remove = Vec::new();
        for entry in self.pending_ping_reqs.iter() {
            let (requester, target) = entry.key();
            if target == source {
                let (requester_addr, _) = entry.value();
                let ack = SwimMessage::Ack {
                    source: source.clone(),
                    incarnation,
                };
                let _ = self.send_message(&ack, *requester_addr).await;
                to_remove.push((requester.clone(), target.clone()));
            }
        }
        for key in to_remove {
            self.pending_ping_reqs.remove(&key);
        }

        // Update member state
        if let Some(mut member) = self.members.get_mut(source) {
            let old_state = member.state;
            member.mark_alive(incarnation);

            if old_state != NodeState::Alive {
                let _ = self
                    .event_tx
                    .send(MembershipEvent::NodeRecovered(source.clone()));
            }
        }

        Ok(())
    }

    /// Handle indirect ping request
    ///
    /// When we receive PingReq{source=A, target=B}, we ping B on A's behalf.
    /// We use *our own* source ID so the target sends the Ack back to us via UDP,
    /// and we store A's address so we can forward the Ack.
    async fn handle_ping_req(
        &self,
        source: &NodeId,
        target: &NodeId,
        _incarnation: u64,
        from: SocketAddr,
    ) -> Result<()> {
        // Try to ping the target on behalf of the requester
        if let Some(target_node) = self.members.get(target) {
            let our_incarnation = *self.incarnation.read().await;
            let ping = SwimMessage::Ping {
                source: self.local_node.id.clone(),
                incarnation: our_incarnation,
            };
            self.send_message(&ping, target_node.cluster_addr()).await?;

            // Track that we're doing an indirect ping so we can forward acks
            self.pending_ping_reqs
                .insert((source.clone(), target.clone()), (from, Instant::now()));
        }

        Ok(())
    }

    /// Handle state sync message
    async fn handle_sync(&self, _source: &NodeId, states: Vec<NodeGossipState>) -> Result<()> {
        for state in states {
            self.merge_state(state).await?;
        }
        Ok(())
    }

    /// Handle join message
    async fn handle_join(&self, info: NodeInfo) -> Result<()> {
        let node_id = info.id.clone();

        // Check if already known
        if self.members.contains_key(&node_id) {
            debug!("Node {} already known, updating info", node_id);
        } else {
            info!("Node {} joining cluster", node_id);
        }

        // Add/update member
        let mut node = Node::new(info.clone());
        node.mark_alive(0);
        self.members.insert(node_id.clone(), node);

        // Send current membership state back
        let states: Vec<NodeGossipState> = self
            .members
            .iter()
            .map(|r| NodeGossipState::from(r.value()))
            .collect();

        let sync = SwimMessage::Sync {
            source: self.local_node.id.clone(),
            states,
        };
        self.send_message(&sync, info.cluster_addr).await?;

        // Broadcast join event
        let _ = self.event_tx.send(MembershipEvent::NodeJoined(info));

        Ok(())
    }

    /// Handle leave message
    async fn handle_leave(&self, node_id: &NodeId, _incarnation: u64) -> Result<()> {
        if let Some(mut member) = self.members.get_mut(node_id) {
            member.mark_leaving();
        }

        info!("Node {} leaving cluster gracefully", node_id);
        let _ = self
            .event_tx
            .send(MembershipEvent::NodeLeft(node_id.clone()));

        // Remove after a delay to allow propagation
        let members = self.members.clone();
        let node_id = node_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            members.remove(&node_id);
        });

        Ok(())
    }

    /// Handle suspect message
    async fn handle_suspect(
        &self,
        node_id: &NodeId,
        incarnation: u64,
        _from: &NodeId,
    ) -> Result<()> {
        // If it's about us, refute it
        if node_id == &self.local_node.id {
            let mut our_incarnation = self.incarnation.write().await;
            if incarnation >= *our_incarnation {
                *our_incarnation = incarnation + 1;

                // Enqueue alive message to refute (piggybacked on next Ping/Ack)
                let alive = SwimMessage::Alive {
                    node_id: self.local_node.id.clone(),
                    incarnation: *our_incarnation,
                };
                self.enqueue_gossip(alive).await;
            }
            return Ok(());
        }

        // Update member state if incarnation is newer
        if let Some(mut member) = self.members.get_mut(node_id) {
            if incarnation >= member.incarnation && member.state == NodeState::Alive {
                member.mark_suspect();
                let _ = self
                    .event_tx
                    .send(MembershipEvent::NodeSuspected(node_id.clone()));
            }
        }

        Ok(())
    }

    /// Handle alive message
    async fn handle_alive(&self, node_id: &NodeId, incarnation: u64) -> Result<()> {
        if let Some(mut member) = self.members.get_mut(node_id) {
            if incarnation > member.incarnation {
                let old_state = member.state;
                member.mark_alive(incarnation);

                if old_state == NodeState::Suspect {
                    let _ = self
                        .event_tx
                        .send(MembershipEvent::NodeRecovered(node_id.clone()));
                }
            }
        }
        Ok(())
    }

    /// Handle dead message
    async fn handle_dead(&self, node_id: &NodeId, incarnation: u64) -> Result<()> {
        // Can't kill ourselves
        if node_id == &self.local_node.id {
            return Ok(());
        }

        if let Some(mut member) = self.members.get_mut(node_id) {
            if incarnation >= member.incarnation {
                member.mark_dead();
                let _ = self
                    .event_tx
                    .send(MembershipEvent::NodeFailed(node_id.clone()));
            }
        }

        Ok(())
    }

    /// Merge state from another node
    async fn merge_state(&self, state: NodeGossipState) -> Result<()> {
        if let Some(mut member) = self.members.get_mut(&state.id) {
            // Only update if incarnation is newer
            if state.incarnation > member.incarnation {
                match state.state {
                    NodeState::Alive => member.mark_alive(state.incarnation),
                    NodeState::Suspect => member.mark_suspect(),
                    NodeState::Dead => member.mark_dead(),
                    NodeState::Leaving => member.mark_leaving(),
                    _ => {}
                }
            }
        } else if state.state != NodeState::Dead {
            // Add new member
            let info = NodeInfo {
                id: state.id.clone(),
                name: None,
                rack: state.rack,
                client_addr: state.client_addr,
                cluster_addr: state.cluster_addr,
                capabilities: state.capabilities,
                version: env!("CARGO_PKG_VERSION").to_string(),
                tags: std::collections::HashMap::new(),
            };
            let mut node = Node::new(info.clone());
            match state.state {
                NodeState::Alive => node.mark_alive(state.incarnation),
                NodeState::Suspect => node.mark_suspect(),
                _ => {}
            }
            self.members.insert(state.id.clone(), node);
            let _ = self.event_tx.send(MembershipEvent::NodeJoined(info));
        }

        Ok(())
    }

    /// Run the SWIM failure detector loop
    async fn run_failure_detector(&self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.ping_interval);

        loop {
            interval.tick().await;

            // Get a random member to probe (round-robin would be more fair)
            let target = self.select_probe_target();

            if let Some(target_node) = target {
                let target_id = target_node.id().to_string();
                let target_addr = target_node.cluster_addr();

                // Send direct ping with piggybacked gossip
                let ping = SwimMessage::Ping {
                    source: self.local_node.id.clone(),
                    incarnation: *self.incarnation.read().await,
                };
                self.send_with_gossip(&ping, target_addr).await?;

                // Track pending ping
                self.pending_pings.insert(target_id.clone(), Instant::now());

                // Wait for ack or timeout
                tokio::time::sleep(self.config.ping_timeout).await;

                // Check if we got an ack
                if self.pending_pings.contains_key(&target_id) {
                    // No ack, try indirect probes
                    self.send_indirect_probes(&target_id).await?;

                    // Wait again
                    tokio::time::sleep(self.config.ping_timeout * 2).await;

                    // Still no ack? Mark as suspect
                    if self.pending_pings.remove(&target_id).is_some() {
                        self.mark_suspect(&target_id).await?;
                    }
                }
            }

            // Check for suspect timeouts
            self.check_suspect_timeouts().await?;
        }
    }

    /// Select a random member to probe
    fn select_probe_target(&self) -> Option<Node> {
        use rand::seq::IteratorRandom;

        self.members
            .iter()
            .filter(|r| r.key() != &self.local_node.id)
            .filter(|r| r.value().state.is_reachable())
            .choose(&mut rand::thread_rng())
            .map(|r| r.value().clone())
    }

    /// Send indirect ping requests
    async fn send_indirect_probes(&self, target: &NodeId) -> Result<()> {
        use rand::seq::IteratorRandom;

        let intermediaries: Vec<_> = self
            .members
            .iter()
            .filter(|r| r.key() != &self.local_node.id && r.key() != target)
            .filter(|r| r.value().is_healthy())
            .choose_multiple(&mut rand::thread_rng(), self.config.indirect_probes);

        let incarnation = *self.incarnation.read().await;
        let ping_req = SwimMessage::PingReq {
            source: self.local_node.id.clone(),
            target: target.clone(),
            incarnation,
        };

        for intermediate in intermediaries {
            let _ = self
                .send_message(&ping_req, intermediate.value().cluster_addr())
                .await;
        }

        Ok(())
    }

    /// Mark a node as suspect
    async fn mark_suspect(&self, node_id: &NodeId) -> Result<()> {
        if let Some(mut member) = self.members.get_mut(node_id) {
            if member.state == NodeState::Alive {
                member.mark_suspect();

                // Enqueue suspicion for piggyback dissemination
                let suspect = SwimMessage::Suspect {
                    node_id: node_id.clone(),
                    incarnation: member.incarnation,
                    from: self.local_node.id.clone(),
                };
                self.enqueue_gossip(suspect).await;

                let _ = self
                    .event_tx
                    .send(MembershipEvent::NodeSuspected(node_id.clone()));
            }
        }
        Ok(())
    }
    /// Check for suspects that have timed out
    async fn check_suspect_timeouts(&self) -> Result<()> {
        let timeout = self.config.ping_interval * self.config.suspicion_multiplier;
        let now = Instant::now();

        let mut dead_nodes = vec![];

        for member in self.members.iter() {
            if member.state == NodeState::Suspect && now.duration_since(member.last_seen) > timeout
            {
                dead_nodes.push(member.key().clone());
            }
        }

        for node_id in dead_nodes {
            if let Some(mut member) = self.members.get_mut(&node_id) {
                member.mark_dead();

                // Enqueue death notice for piggyback dissemination
                let dead = SwimMessage::Dead {
                    node_id: node_id.clone(),
                    incarnation: member.incarnation,
                };
                self.enqueue_gossip(dead).await;

                let _ = self.event_tx.send(MembershipEvent::NodeFailed(node_id));
            }
        }

        Ok(())
    }

    /// Periodically sync full state with random members
    async fn run_sync(&self) -> Result<()> {
        let mut interval = tokio::time::interval(self.config.sync_interval);

        loop {
            interval.tick().await;

            // Select random member to sync with
            if let Some(target) = self.select_probe_target() {
                let states: Vec<NodeGossipState> = self
                    .members
                    .iter()
                    .map(|r| NodeGossipState::from(r.value()))
                    .collect();

                let sync = SwimMessage::Sync {
                    source: self.local_node.id.clone(),
                    states,
                };
                let _ = self.send_message(&sync, target.cluster_addr()).await;
            }
        }
    }

    /// Enqueue a state-change message for piggyback dissemination.
    ///
    /// Instead of O(N) broadcasting, items are piggybacked onto
    /// routine Ping/Ack messages. Each item is retransmitted up to
    /// ⌈log₂(N)⌉+1 times before being dropped (SWIM protocol guarantee).
    async fn enqueue_gossip(&self, msg: SwimMessage) {
        let mut queue = self.gossip_queue.lock().await;
        queue.push(GossipItem {
            message: msg,
            transmissions: 0,
        });
    }

    /// Drain up to `MAX_GOSSIP_PER_MSG` items from the gossip queue,
    /// incrementing their transmission counter. Items that have been
    /// transmitted enough times (≥ ⌈log₂(N)⌉+1) are removed.
    async fn drain_gossip(&self) -> Vec<SwimMessage> {
        let member_count = self.members.len().max(2) as f64;
        let max_transmissions = (member_count.log2().ceil() as u32) + 1;

        let mut queue = self.gossip_queue.lock().await;
        let take = queue.len().min(MAX_GOSSIP_PER_MSG);
        let mut items = Vec::with_capacity(take);

        for item in queue.iter_mut().take(take) {
            item.transmissions += 1;
            items.push(item.message.clone());
        }

        // Remove items that have been transmitted enough times
        queue.retain(|item| item.transmissions < max_transmissions);

        items
    }

    /// Send a primary message with piggybacked gossip items to `addr`.
    async fn send_with_gossip(&self, msg: &SwimMessage, addr: SocketAddr) -> Result<()> {
        // Always send the primary message
        self.send_message(msg, addr).await?;

        // Piggyback pending gossip items
        let gossip = self.drain_gossip().await;
        for item in gossip {
            self.send_message(&item, addr).await?;
        }

        Ok(())
    }

    /// Broadcast a message to all members (used only for leave/join announcements
    /// where immediate dissemination is required).
    async fn broadcast(&self, msg: &SwimMessage) -> Result<()> {
        for member in self.members.iter() {
            if member.key() != &self.local_node.id {
                let _ = self.send_message(msg, member.value().cluster_addr()).await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_membership_creation() {
        let node_info = NodeInfo::new(
            "test-node",
            "127.0.0.1:9092".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );
        let config = SwimConfig::default();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let membership = Membership::new(node_info, config, shutdown_rx)
            .await
            .unwrap();

        assert_eq!(membership.member_count(), 1);
        assert_eq!(membership.healthy_count(), 1);
    }
}
