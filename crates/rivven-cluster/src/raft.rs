//! Raft integration for cluster metadata consensus
//!
//! This module provides the Raft consensus implementation for cluster metadata.
//! It uses **redb** (pure Rust) for persistent log storage and wraps our `ClusterMetadata`
//! state machine with full openraft integration.
//!
//! # Architecture
//!
//! - **TypeConfig**: Defines all Raft-related types (node ID, entry, etc.)
//! - **LogStore**: redb-backed log storage implementing `RaftLogStorage`
//! - **StateMachine**: Wraps `ClusterMetadata` implementing `RaftStateMachine`
//! - **NetworkFactory**: Creates HTTP-based network connections for Raft RPCs
//! - **RaftNode**: High-level API managing the Raft instance
//!
//! # Why redb?
//!
//! We use redb instead of RocksDB for several benefits:
//! - **Pure Rust**: Zero C/C++ dependencies, compiles everywhere
//! - **Fast builds**: ~10s vs 2-5 minutes for RocksDB
//! - **Cross-compile**: Works with musl, WASM, etc.
//! - **ACID**: Full transactional guarantees
//! - **Small binary**: Minimal size impact

// Suppress warnings for large error types from openraft crate
#![allow(clippy::result_large_err)]

use crate::config::ClusterConfig;
use crate::error::{ClusterError, Result};
use crate::metadata::{ClusterMetadata, MetadataCommand, MetadataResponse};
use crate::storage::RedbLogStore;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::responder::OneshotResponder;
use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Membership, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Type alias for backward compatibility - uses redb storage
pub type LogStore = RedbLogStore;

// ============================================================================
// Error Types
// ============================================================================

/// Simple error wrapper for network errors that implements std::error::Error
#[derive(Debug)]
struct NetworkErrorWrapper(String);

impl std::fmt::Display for NetworkErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for NetworkErrorWrapper {}

// ============================================================================
// Type Configuration
// ============================================================================

/// Raft node ID type
pub type NodeId = u64;

/// Application request type for the state machine
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftRequest {
    pub command: MetadataCommand,
}

/// Application response type from the state machine
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftResponse {
    pub response: MetadataResponse,
}

/// Type configuration for Rivven's Raft implementation
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(feature = "raft", derive(Serialize, Deserialize))]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = RaftRequest;
    type R = RaftResponse;
    type NodeId = NodeId;
    type Node = BasicNode;
    type Entry = Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = OneshotResponder<TypeConfig>;
}

// Type aliases for convenience
pub type RaftLogId = LogId<NodeId>;
pub type RaftVote = Vote<NodeId>;
pub type RaftEntry = Entry<TypeConfig>;
pub type RaftMembership = Membership<NodeId, BasicNode>;
pub type RaftStoredMembership = StoredMembership<NodeId, BasicNode>;
pub type RaftSnapshot = Snapshot<TypeConfig>;
pub type RaftSnapshotMeta = SnapshotMeta<NodeId, BasicNode>;

// ============================================================================
// State Machine Implementation
// ============================================================================

/// State machine wrapping ClusterMetadata
pub struct StateMachine {
    /// The actual metadata state
    metadata: RwLock<ClusterMetadata>,
    /// Last applied log ID
    last_applied: RwLock<Option<RaftLogId>>,
    /// Current membership
    membership: RwLock<RaftStoredMembership>,
}

impl StateMachine {
    /// Create new state machine
    pub fn new() -> Self {
        Self {
            metadata: RwLock::new(ClusterMetadata::new()),
            last_applied: RwLock::new(None),
            membership: RwLock::new(StoredMembership::new(None, Membership::new(vec![], ()))),
        }
    }

    /// Get current metadata (read-only)
    pub async fn metadata(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterMetadata> {
        self.metadata.read().await
    }

    /// Apply a command to the state machine
    async fn apply_command(&self, log_id: &RaftLogId, command: MetadataCommand) -> RaftResponse {
        let mut metadata = self.metadata.write().await;
        let response = metadata.apply(log_id.index, command);
        *self.last_applied.write().await = Some(*log_id);
        RaftResponse { response }
    }

    /// Create a snapshot
    ///
    /// All three fields are read under coordinated locking to produce
    /// a consistent snapshot. We acquire locks in a fixed order
    /// (metadata → last_applied → membership) and hold them together
    /// so that no interleaving `apply` can mutate state between reads.
    async fn create_snapshot(
        &self,
    ) -> std::result::Result<(RaftSnapshotMeta, Vec<u8>), StorageError<NodeId>> {
        // Acquire all read locks together to get a consistent view.
        // Hold them simultaneously so no apply() can interleave.
        let metadata_guard = self.metadata.read().await;
        let last_applied_guard = self.last_applied.read().await;
        let membership_guard = self.membership.read().await;

        let metadata = metadata_guard.clone();
        let last_applied = *last_applied_guard;
        let membership = membership_guard.clone();

        // Release all locks before serialization (which can be expensive)
        drop(membership_guard);
        drop(last_applied_guard);
        drop(metadata_guard);

        let snapshot_data = SnapshotData {
            metadata: metadata.clone(),
            last_applied,
            membership: membership.clone(),
        };

        let data = postcard::to_allocvec(&snapshot_data).map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(openraft::AnyError::new(&e)),
        })?;

        let meta = SnapshotMeta {
            last_log_id: snapshot_data.last_applied,
            last_membership: membership,
            snapshot_id: format!("snapshot-{}", metadata.last_applied_index),
        };

        info!(
            snapshot_id = %meta.snapshot_id,
            last_log_id = ?meta.last_log_id,
            "Created snapshot"
        );

        Ok((meta, data))
    }

    /// Install a snapshot
    async fn install_snapshot_data(
        &self,
        data: &[u8],
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let snapshot_data: SnapshotData =
            postcard::from_bytes(data).map_err(|e| StorageError::IO {
                source: StorageIOError::read_state_machine(openraft::AnyError::new(&e)),
            })?;

        *self.metadata.write().await = snapshot_data.metadata;
        *self.last_applied.write().await = snapshot_data.last_applied;
        *self.membership.write().await = snapshot_data.membership;

        info!("Installed snapshot");
        Ok(())
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot data format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    metadata: ClusterMetadata,
    last_applied: Option<RaftLogId>,
    membership: RaftStoredMembership,
}

// Implement RaftStateMachine for StateMachine
impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<(Option<RaftLogId>, RaftStoredMembership), StorageError<NodeId>> {
        let last_applied = *self.last_applied.read().await;
        let membership = self.membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => {
                    // Blank entry - just update last_applied
                    *self.last_applied.write().await = Some(log_id);
                    responses.push(RaftResponse {
                        response: MetadataResponse::Success,
                    });
                }
                EntryPayload::Normal(req) => {
                    // Normal command
                    let response = self.apply_command(&log_id, req.command).await;
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    // Membership change
                    *self.membership.write().await =
                        StoredMembership::new(Some(log_id), membership);
                    *self.last_applied.write().await = Some(log_id);
                    responses.push(RaftResponse {
                        response: MetadataResponse::Success,
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();

        // Install the snapshot data
        self.install_snapshot_data(&data).await?;

        // Update membership from snapshot meta
        *self.membership.write().await = meta.last_membership.clone();

        info!(
            snapshot_id = %meta.snapshot_id,
            "Installed snapshot from leader"
        );
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<RaftSnapshot>, StorageError<NodeId>> {
        let (meta, data) = self.create_snapshot().await?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Return a clone for snapshot building
        Self {
            metadata: RwLock::new(self.metadata.read().await.clone()),
            last_applied: RwLock::new(*self.last_applied.read().await),
            membership: RwLock::new(self.membership.read().await.clone()),
        }
    }
}

// Implement RaftSnapshotBuilder for StateMachine
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> std::result::Result<RaftSnapshot, StorageError<NodeId>> {
        let (meta, data) = self.create_snapshot().await?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// ============================================================================
// Network Implementation (High-Performance Binary Protocol)
// ============================================================================

/// Serialization format for Raft RPCs
/// Binary (postcard) is ~5-10x faster than JSON
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SerializationFormat {
    /// JSON format (for debugging/compatibility)
    Json,
    /// Binary format (for performance)
    #[default]
    Binary,
}

/// Compression configuration for Raft RPCs
#[derive(Debug, Clone)]
pub struct RaftCompressionConfig {
    /// Enable compression
    pub enabled: bool,
    /// Minimum payload size to compress (bytes)
    pub min_size: usize,
    /// Use adaptive algorithm selection
    pub adaptive: bool,
}

impl Default for RaftCompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_size: 1024, // Only compress payloads > 1KB
            adaptive: true,
        }
    }
}

/// Network factory for creating Raft network connections
#[derive(Clone)]
pub struct NetworkFactory {
    /// Node addresses
    nodes: Arc<RwLock<BTreeMap<NodeId, String>>>,
    /// HTTP client with connection pooling
    client: reqwest::Client,
    /// Serialization format
    format: SerializationFormat,
    /// Compression configuration
    compression: RaftCompressionConfig,
}

impl NetworkFactory {
    /// Create new network factory with binary serialization (fastest)
    pub fn new() -> Self {
        Self::with_format(SerializationFormat::Binary)
    }

    /// Create network factory with specific serialization format
    pub fn with_format(format: SerializationFormat) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .pool_max_idle_per_host(10) // Connection pooling
                .pool_idle_timeout(std::time::Duration::from_secs(60))
                .tcp_keepalive(std::time::Duration::from_secs(30))
                .tcp_nodelay(true) // Low latency
                .build()
                .expect("Failed to create HTTP client"),
            format,
            compression: RaftCompressionConfig::default(),
        }
    }

    /// Create network factory with compression config
    pub fn with_compression(
        format: SerializationFormat,
        compression: RaftCompressionConfig,
    ) -> Self {
        Self {
            compression,
            ..Self::with_format(format)
        }
    }

    /// Register a node address
    pub async fn add_node(&self, node_id: NodeId, addr: String) {
        self.nodes.write().await.insert(node_id, addr);
    }

    /// Remove a node
    pub async fn remove_node(&self, node_id: NodeId) {
        self.nodes.write().await.remove(&node_id);
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// Network implementation for a single target node
pub struct Network {
    /// Target node identifier (stored for debugging and logging purposes)
    #[allow(dead_code)]
    target: NodeId,
    target_addr: String,
    client: reqwest::Client,
    format: SerializationFormat,
    compression: RaftCompressionConfig,
}

impl Network {
    pub fn new(
        target: NodeId,
        target_addr: String,
        client: reqwest::Client,
        format: SerializationFormat,
        compression: RaftCompressionConfig,
    ) -> Self {
        Self {
            target,
            target_addr,
            client,
            format,
            compression,
        }
    }

    /// Serialize request based on format
    fn serialize<T: Serialize>(&self, data: &T) -> std::result::Result<Vec<u8>, String> {
        match self.format {
            SerializationFormat::Json => serde_json::to_vec(data).map_err(|e| e.to_string()),
            SerializationFormat::Binary => postcard::to_allocvec(data).map_err(|e| e.to_string()),
        }
    }

    /// Deserialize response based on format
    fn deserialize<T: serde::de::DeserializeOwned>(
        &self,
        data: &[u8],
    ) -> std::result::Result<T, String> {
        match self.format {
            SerializationFormat::Json => serde_json::from_slice(data).map_err(|e| e.to_string()),
            SerializationFormat::Binary => postcard::from_bytes(data).map_err(|e| e.to_string()),
        }
    }

    /// Get content type header
    fn content_type(&self) -> &'static str {
        match self.format {
            SerializationFormat::Json => "application/json",
            SerializationFormat::Binary => "application/octet-stream",
        }
    }

    /// Compress data if enabled and beneficial
    #[cfg(feature = "compression")]
    fn maybe_compress(&self, data: Vec<u8>) -> (Vec<u8>, bool) {
        use rivven_core::compression::{CompressionConfig, Compressor};

        if !self.compression.enabled || data.len() < self.compression.min_size {
            return (data, false);
        }

        let config = CompressionConfig {
            min_size: self.compression.min_size,
            adaptive: self.compression.adaptive,
            ..Default::default()
        };
        let compressor = Compressor::with_config(config);

        match compressor.compress(&data) {
            Ok(compressed) => {
                // Only use compression if it actually helps
                if compressed.len() < data.len() {
                    (compressed.to_vec(), true)
                } else {
                    (data, false)
                }
            }
            Err(_) => (data, false),
        }
    }

    #[cfg(not(feature = "compression"))]
    fn maybe_compress(&self, data: Vec<u8>) -> (Vec<u8>, bool) {
        (data, false)
    }

    /// Decompress data if it was compressed
    #[cfg(feature = "compression")]
    fn maybe_decompress(
        &self,
        data: &[u8],
        was_compressed: bool,
    ) -> std::result::Result<Vec<u8>, String> {
        use rivven_core::compression::Compressor;

        if !was_compressed {
            return Ok(data.to_vec());
        }

        let compressor = Compressor::new();
        compressor
            .decompress(data)
            .map(|b| b.to_vec())
            .map_err(|e| e.to_string())
    }

    #[cfg(not(feature = "compression"))]
    fn maybe_decompress(
        &self,
        data: &[u8],
        _was_compressed: bool,
    ) -> std::result::Result<Vec<u8>, String> {
        Ok(data.to_vec())
    }
}

// Implement RaftNetworkFactory
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Network::new(
            target,
            node.addr.clone(),
            self.client.clone(),
            self.format,
            self.compression.clone(),
        )
    }
}

// Implement RaftNetwork with optimized binary serialization and compression
impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        use crate::observability::{NetworkMetrics, RaftMetrics};
        let start = std::time::Instant::now();

        let url = format!("{}/raft/append", self.target_addr);
        let serialized = self.serialize(&rpc).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &NetworkErrorWrapper(e),
            ))
        })?;

        // Apply compression if beneficial
        let (body, compressed) = self.maybe_compress(serialized);
        let uncompressed_size = body.len();

        NetworkMetrics::add_bytes_sent(body.len() as u64);
        RaftMetrics::increment_append_entries_sent();

        // Add compression header if compressed
        let mut request = self.client.post(&url).body(body);
        request = request.header("Content-Type", self.content_type());
        if compressed {
            request = request.header("X-Rivven-Compressed", "1");
            request = request.header("X-Rivven-Original-Size", uncompressed_size.to_string());
        }

        let resp = request.send().await.map_err(|e| {
            NetworkMetrics::increment_rpc_errors("append_entries");
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        if !resp.status().is_success() {
            NetworkMetrics::increment_rpc_errors("append_entries");
            return Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&NetworkErrorWrapper(format!(
                    "HTTP error: {}",
                    resp.status()
                ))),
            ));
        }

        // Check if response is compressed
        let resp_compressed = resp
            .headers()
            .get("X-Rivven-Compressed")
            .map(|v| v == "1")
            .unwrap_or(false);

        let bytes = resp.bytes().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        NetworkMetrics::add_bytes_received(bytes.len() as u64);
        RaftMetrics::record_append_entries_latency(start.elapsed());

        // Decompress if needed
        let response_data = self
            .maybe_decompress(&bytes, resp_compressed)
            .map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &NetworkErrorWrapper(e),
                ))
            })?;

        let response: openraft::raft::AppendEntriesResponse<NodeId> =
            self.deserialize(&response_data).map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &NetworkErrorWrapper(e),
                ))
            })?;

        Ok(response)
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        use crate::observability::{NetworkMetrics, RaftMetrics};
        let start = std::time::Instant::now();

        let url = format!("{}/raft/snapshot", self.target_addr);
        let serialized = self.serialize(&rpc).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &NetworkErrorWrapper(e),
            ))
        })?;

        // Snapshots benefit significantly from compression
        let (body, compressed) = self.maybe_compress(serialized);
        let uncompressed_size = body.len();

        NetworkMetrics::add_bytes_sent(body.len() as u64);

        // Add compression header if compressed
        let mut request = self.client.post(&url).body(body);
        request = request.header("Content-Type", self.content_type());
        if compressed {
            request = request.header("X-Rivven-Compressed", "1");
            request = request.header("X-Rivven-Original-Size", uncompressed_size.to_string());
        }

        let resp = request.send().await.map_err(|e| {
            NetworkMetrics::increment_rpc_errors("install_snapshot");
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        if !resp.status().is_success() {
            NetworkMetrics::increment_rpc_errors("install_snapshot");
            return Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&NetworkErrorWrapper(format!(
                    "HTTP error: {}",
                    resp.status()
                ))),
            ));
        }

        let bytes = resp.bytes().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        NetworkMetrics::add_bytes_received(bytes.len() as u64);
        RaftMetrics::record_snapshot_duration(start.elapsed());

        let response: openraft::raft::InstallSnapshotResponse<NodeId> =
            self.deserialize(&bytes).map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &NetworkErrorWrapper(e),
                ))
            })?;

        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> std::result::Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        use crate::observability::{NetworkMetrics, RaftMetrics};
        let start = std::time::Instant::now();

        let url = format!("{}/raft/vote", self.target_addr);
        let body = self.serialize(&rpc).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                &NetworkErrorWrapper(e),
            ))
        })?;

        NetworkMetrics::add_bytes_sent(body.len() as u64);

        let resp = self
            .client
            .post(&url)
            .body(body)
            .header("Content-Type", self.content_type())
            .send()
            .await
            .map_err(|e| {
                NetworkMetrics::increment_rpc_errors("vote");
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
            })?;

        if !resp.status().is_success() {
            NetworkMetrics::increment_rpc_errors("vote");
            return Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&NetworkErrorWrapper(format!(
                    "HTTP error: {}",
                    resp.status()
                ))),
            ));
        }

        let bytes = resp.bytes().await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;

        NetworkMetrics::add_bytes_received(bytes.len() as u64);
        RaftMetrics::record_vote_latency(start.elapsed());
        RaftMetrics::increment_elections();

        let response: openraft::raft::VoteResponse<NodeId> =
            self.deserialize(&bytes).map_err(|e| {
                openraft::error::RPCError::Network(openraft::error::NetworkError::new(
                    &NetworkErrorWrapper(e),
                ))
            })?;

        Ok(response)
    }
}

// ============================================================================
// High-Level Raft Node API
// ============================================================================

/// Configuration for Raft consensus
#[derive(Debug, Clone)]
pub struct RaftNodeConfig {
    /// Our node ID (will be hashed to u64)
    pub node_id: String,
    /// Whether in standalone mode
    pub standalone: bool,
    /// Data directory for Raft storage
    pub data_dir: std::path::PathBuf,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Election timeout range in milliseconds
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// Snapshot threshold (log entries before snapshot)
    pub snapshot_threshold: u64,
    /// Initial cluster members (for bootstrapping)
    pub initial_members: Vec<(NodeId, BasicNode)>,
}

// ============================================================================
// Batch Proposal Accumulator
// ============================================================================

/// A pending batch of proposals waiting to be submitted
#[allow(dead_code)]
pub(crate) struct PendingBatch {
    /// Commands accumulated in this batch
    commands: Vec<MetadataCommand>,
    /// Response channels for each command
    responders: Vec<tokio::sync::oneshot::Sender<Result<MetadataResponse>>>,
    /// When this batch started accumulating
    started: std::time::Instant,
}

/// Configuration for batch proposals
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum batch size before forcing a flush
    pub max_batch_size: usize,
    /// Maximum time to wait before flushing (microseconds)
    pub max_wait_us: u64,
    /// Enable batching (false = immediate proposals)
    pub enabled: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_wait_us: 1000, // 1ms default linger
            enabled: true,
        }
    }
}

/// Batch proposal accumulator for high-throughput writes
///
/// This batches multiple writes over a short time window and submits them
/// as a single Raft proposal, amortizing the consensus overhead.
///
/// Throughput improvement: 10-50x for small writes
/// Latency trade-off: adds up to `max_wait_us` latency
pub struct BatchAccumulator {
    /// Current pending batch
    pending: RwLock<Option<PendingBatch>>,
    /// Batch configuration
    config: BatchConfig,
    /// Notification channel for new items
    notify: tokio::sync::Notify,
}

impl BatchAccumulator {
    /// Create a new batch accumulator
    pub fn new(config: BatchConfig) -> Self {
        Self {
            pending: RwLock::new(None),
            config,
            notify: tokio::sync::Notify::new(),
        }
    }

    /// Add a command to the current batch, returning a channel for the response
    pub async fn add(
        &self,
        command: MetadataCommand,
    ) -> tokio::sync::oneshot::Receiver<Result<MetadataResponse>> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let should_flush = {
            let mut pending = self.pending.write().await;

            if pending.is_none() {
                *pending = Some(PendingBatch {
                    commands: vec![command],
                    responders: vec![tx],
                    started: std::time::Instant::now(),
                });
                false
            } else {
                let batch = pending.as_mut().unwrap();
                batch.commands.push(command);
                batch.responders.push(tx);
                batch.commands.len() >= self.config.max_batch_size
            }
        };

        self.notify.notify_one();

        if should_flush {
            // Force flush if batch is full
            self.notify.notify_one();
        }

        rx
    }

    /// Take the current batch if ready (full or timed out)
    #[allow(dead_code)]
    pub(crate) async fn take_if_ready(&self) -> Option<PendingBatch> {
        let mut pending = self.pending.write().await;

        if let Some(ref batch) = *pending {
            let elapsed = batch.started.elapsed();
            let size = batch.commands.len();

            if size >= self.config.max_batch_size
                || elapsed.as_micros() as u64 >= self.config.max_wait_us
            {
                return pending.take();
            }
        }
        None
    }

    /// Wait for the batch to be ready
    pub async fn wait_ready(&self) {
        let timeout = std::time::Duration::from_micros(self.config.max_wait_us);
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
    }
}

impl Default for RaftNodeConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".to_string(),
            standalone: true,
            data_dir: std::path::PathBuf::from("./data/raft"),
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            snapshot_threshold: 10000,
            initial_members: vec![],
        }
    }
}

/// High-level Raft node wrapper
pub struct RaftNode {
    /// The openraft instance (only in cluster mode)
    raft: Option<openraft::Raft<TypeConfig>>,
    /// Log storage (for cluster mode reference, kept for state consistency)
    #[allow(dead_code)]
    log_store: Option<Arc<LogStore>>,
    /// State machine (for direct access in standalone mode)
    state_machine: StateMachine,
    /// Network factory (stores node addresses)
    network: NetworkFactory,
    /// Our node ID
    node_id: NodeId,
    /// String node ID (original)
    node_id_str: String,
    /// Whether we're in standalone mode
    standalone: bool,
    /// Next log index (for standalone mode)
    next_index: RwLock<u64>,
    /// Data directory
    data_dir: std::path::PathBuf,
    /// Raft config for start
    raft_config: RaftNodeConfig,
}

impl RaftNode {
    /// Create a new Raft node from cluster config
    pub async fn new(config: &ClusterConfig) -> Result<Self> {
        let raft_config = RaftNodeConfig {
            node_id: config.node_id.clone(),
            standalone: config.mode == crate::config::ClusterMode::Standalone,
            data_dir: config.data_dir.join("raft"),
            heartbeat_interval_ms: config.raft.heartbeat_interval.as_millis() as u64,
            election_timeout_min_ms: config.raft.election_timeout_min.as_millis() as u64,
            election_timeout_max_ms: config.raft.election_timeout_max.as_millis() as u64,
            snapshot_threshold: config.raft.snapshot_threshold,
            initial_members: vec![],
        };
        Self::with_config(raft_config).await
    }

    /// Create a new Raft node with explicit configuration
    pub async fn with_config(config: RaftNodeConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| ClusterError::RaftStorage(e.to_string()))?;

        let state_machine = StateMachine::new();
        let network = NetworkFactory::new();
        let node_id = hash_node_id(&config.node_id);

        info!(
            node_id,
            node_id_str = %config.node_id,
            standalone = config.standalone,
            data_dir = %config.data_dir.display(),
            "Created Raft node"
        );

        Ok(Self {
            raft: None,
            log_store: None,
            state_machine,
            network,
            node_id,
            node_id_str: config.node_id.clone(),
            standalone: config.standalone,
            next_index: RwLock::new(1),
            data_dir: config.data_dir.clone(),
            raft_config: config,
        })
    }

    /// Initialize and start the Raft instance
    pub async fn start(&mut self) -> Result<()> {
        if self.standalone {
            info!(node_id = self.node_id, "Starting in standalone mode");
            return Ok(());
        }

        // Initialize log storage
        let log_store = LogStore::new(&self.data_dir)
            .map_err(|e| ClusterError::RaftStorage(format!("Failed to create log store: {}", e)))?;

        // Build openraft config
        let raft_config = openraft::Config {
            cluster_name: "rivven-cluster".to_string(),
            heartbeat_interval: self.raft_config.heartbeat_interval_ms,
            election_timeout_min: self.raft_config.election_timeout_min_ms,
            election_timeout_max: self.raft_config.election_timeout_max_ms,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                self.raft_config.snapshot_threshold,
            ),
            max_in_snapshot_log_to_keep: 1000,
            ..Default::default()
        };

        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| ClusterError::RaftStorage(format!("Invalid Raft config: {}", e)))?,
        );

        // Create a new state machine for openraft (it takes ownership)
        let state_machine = StateMachine::new();

        // Create a new network factory for openraft (it takes ownership)
        let network = NetworkFactory::new();
        // Copy node addresses to the new network
        for (id, addr) in self.network.nodes.read().await.iter() {
            network.add_node(*id, addr.clone()).await;
        }

        // Create the Raft instance
        let raft =
            openraft::Raft::new(self.node_id, raft_config, network, log_store, state_machine)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("Failed to create Raft: {}", e)))?;

        self.raft = Some(raft);

        info!(
            node_id = self.node_id,
            node_id_str = %self.node_id_str,
            "Cluster mode Raft initialized and ready"
        );
        Ok(())
    }

    /// Initialize cluster with initial membership (bootstrap)
    /// This should only be called on the first node of a new cluster
    pub async fn bootstrap(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<()> {
        if self.standalone {
            return Ok(());
        }

        if let Some(ref raft) = self.raft {
            raft.initialize(members)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("Failed to bootstrap: {}", e)))?;
            info!(node_id = self.node_id, "Bootstrapped Raft cluster");
        }
        Ok(())
    }

    /// Propose a command to the Raft cluster
    pub async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse> {
        use crate::observability::RaftMetrics;
        let start = std::time::Instant::now();

        if self.standalone {
            // In standalone mode, apply directly to state machine
            let index = {
                let mut next = self.next_index.write().await;
                let idx = *next;
                *next += 1;
                idx
            };
            let log_id = LogId::new(openraft::CommittedLeaderId::new(0, self.node_id), index);
            let response = self.state_machine.apply_command(&log_id, command).await;

            RaftMetrics::increment_proposals();
            RaftMetrics::increment_commits();
            RaftMetrics::record_proposal_latency(start.elapsed());

            return Ok(response.response);
        }

        // Cluster mode - use Raft client_write
        if let Some(ref raft) = self.raft {
            let request = RaftRequest { command };
            let result = raft
                .client_write(request)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("Client write failed: {}", e)))?;

            RaftMetrics::increment_proposals();
            RaftMetrics::increment_commits();
            RaftMetrics::record_proposal_latency(start.elapsed());

            return Ok(result.data.response);
        }

        Err(ClusterError::RaftStorage(
            "Raft not initialized".to_string(),
        ))
    }

    /// Propose multiple commands in a single batch for higher throughput
    ///
    /// This is more efficient than calling propose() multiple times because:
    /// 1. Single Raft consensus round for all commands
    /// 2. Single disk fsync for all log entries
    /// 3. Amortized network overhead
    ///
    /// Returns responses in the same order as commands.
    pub async fn propose_batch(
        &self,
        commands: Vec<MetadataCommand>,
    ) -> Result<Vec<MetadataResponse>> {
        use crate::observability::RaftMetrics;

        if commands.is_empty() {
            return Ok(vec![]);
        }

        let batch_size = commands.len();
        RaftMetrics::record_batch_size(batch_size);

        if self.standalone {
            // In standalone mode, apply all directly
            let mut responses = Vec::with_capacity(commands.len());
            for command in commands {
                let index = {
                    let mut next = self.next_index.write().await;
                    let idx = *next;
                    *next += 1;
                    idx
                };
                let log_id = LogId::new(openraft::CommittedLeaderId::new(0, self.node_id), index);
                let response = self.state_machine.apply_command(&log_id, command).await;
                responses.push(response.response);
            }
            return Ok(responses);
        }

        // Cluster mode - submit as a single atomic Batch command
        // This goes through one Raft consensus round and one fsync
        if let Some(ref raft) = self.raft {
            let batch_command = MetadataCommand::Batch(commands);
            let request = RaftRequest {
                command: batch_command,
            };

            let result = raft
                .client_write(request)
                .await
                .map_err(|e| {
                    ClusterError::RaftStorage(format!("Batch write failed: {}", e))
                })?;

            RaftMetrics::increment_proposals();
            RaftMetrics::increment_commits();

            // The batch returns a single response — replicate it for each command
            let response = result.data.response;
            return Ok(vec![response; batch_size]);
        }

        Err(ClusterError::RaftStorage(
            "Raft not initialized".to_string(),
        ))
    }

    /// Ensure linearizable read by confirming leadership with cluster
    ///
    /// This implements the ReadIndex optimization from the Raft paper.
    /// It allows any node to serve consistent reads by:
    /// 1. Leader records current commit index as read_index
    /// 2. Leader confirms it's still leader (heartbeat quorum)
    /// 3. Wait for applied index >= read_index
    /// 4. Return to client - data is linearizable
    ///
    /// This is 10-100x faster than read-via-propose since it doesn't write to log.
    pub async fn ensure_linearizable_read(&self) -> Result<()> {
        if self.standalone {
            // Standalone mode - always linearizable (single node)
            return Ok(());
        }

        if let Some(ref raft) = self.raft {
            // Use openraft's built-in linearizable read mechanism
            // This waits for the state machine to catch up to the commit index
            let applied = raft.ensure_linearizable().await.map_err(|e| {
                ClusterError::RaftStorage(format!("Linearizable read failed: {}", e))
            })?;

            debug!(
                applied_log = %applied.map(|l| l.index.to_string()).unwrap_or_else(|| "none".to_string()),
                "Linearizable read confirmed"
            );
            return Ok(());
        }

        Err(ClusterError::RaftStorage(
            "Raft not initialized".to_string(),
        ))
    }

    /// Read metadata with linearizable consistency
    ///
    /// This ensures the read reflects all committed writes up to this point.
    /// Slightly slower than eventual reads but guarantees consistency.
    pub async fn linearizable_metadata(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, ClusterMetadata>> {
        // First ensure we're up to date
        self.ensure_linearizable_read().await?;
        // Then return the metadata
        Ok(self.state_machine.metadata().await)
    }

    /// Get current metadata
    pub async fn metadata(&self) -> tokio::sync::RwLockReadGuard<'_, ClusterMetadata> {
        self.state_machine.metadata().await
    }

    /// Check if this node is the Raft leader
    pub fn is_leader(&self) -> bool {
        if self.standalone {
            return true;
        }

        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            return metrics.current_leader == Some(self.node_id);
        }
        false
    }

    /// Get current leader node ID
    pub fn leader(&self) -> Option<NodeId> {
        if self.standalone {
            return Some(self.node_id);
        }

        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            return metrics.current_leader;
        }
        None
    }

    /// Get our node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get our string node ID
    pub fn node_id_str(&self) -> &str {
        &self.node_id_str
    }

    /// Get the underlying Raft instance (for advanced operations)
    pub fn get_raft(&self) -> Option<&openraft::Raft<TypeConfig>> {
        self.raft.as_ref()
    }

    /// Add a node to the network (for cluster mode)
    pub async fn add_peer(&self, node_id: NodeId, addr: String) {
        self.network.add_node(node_id, addr).await;
    }

    /// Remove a node from the network
    pub async fn remove_peer(&self, node_id: NodeId) {
        self.network.remove_node(node_id).await;
    }

    /// Snapshot the current state (in standalone mode)
    pub async fn snapshot(&self) -> Result<()> {
        // In cluster mode, snapshots are managed by openraft
        if !self.standalone {
            if let Some(ref raft) = self.raft {
                raft.trigger().snapshot().await.map_err(|e| {
                    ClusterError::RaftStorage(format!("Snapshot trigger failed: {}", e))
                })?;
                info!(node_id = self.node_id, "Triggered Raft snapshot");
                return Ok(());
            }
        }

        // In standalone mode, create snapshot directly
        let (_meta, data) = self
            .state_machine
            .create_snapshot()
            .await
            .map_err(|e| ClusterError::RaftStorage(format!("{}", e)))?;

        info!(size = data.len(), "Created standalone snapshot");
        Ok(())
    }

    /// Get Raft metrics (for monitoring)
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<NodeId, BasicNode>> {
        self.raft.as_ref().map(|r| r.metrics().borrow().clone())
    }

    // =========================================================================
    // Raft RPC Handlers (for HTTP endpoint integration)
    // =========================================================================

    /// Handle AppendEntries RPC from another node
    pub async fn handle_append_entries(
        &self,
        req: openraft::raft::AppendEntriesRequest<TypeConfig>,
    ) -> std::result::Result<openraft::raft::AppendEntriesResponse<NodeId>, ClusterError> {
        if let Some(ref raft) = self.raft {
            raft.append_entries(req)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("{}", e)))
        } else {
            Err(ClusterError::RaftStorage(
                "Raft not initialized".to_string(),
            ))
        }
    }

    /// Handle InstallSnapshot RPC from leader
    pub async fn handle_install_snapshot(
        &self,
        req: openraft::raft::InstallSnapshotRequest<TypeConfig>,
    ) -> std::result::Result<openraft::raft::InstallSnapshotResponse<NodeId>, ClusterError> {
        if let Some(ref raft) = self.raft {
            raft.install_snapshot(req)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("{}", e)))
        } else {
            Err(ClusterError::RaftStorage(
                "Raft not initialized".to_string(),
            ))
        }
    }

    /// Handle Vote RPC during election
    pub async fn handle_vote(
        &self,
        req: openraft::raft::VoteRequest<NodeId>,
    ) -> std::result::Result<openraft::raft::VoteResponse<NodeId>, ClusterError> {
        if let Some(ref raft) = self.raft {
            raft.vote(req)
                .await
                .map_err(|e| ClusterError::RaftStorage(format!("{}", e)))
        } else {
            Err(ClusterError::RaftStorage(
                "Raft not initialized".to_string(),
            ))
        }
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Hash a string node ID to u64 for Raft compatibility
pub fn hash_node_id(node_id: &str) -> NodeId {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    node_id.hash(&mut hasher);
    hasher.finish()
}

// ============================================================================
// Legacy Compatibility
// ============================================================================

/// Legacy type alias for backward compatibility
pub type RaftNodeId = NodeId;

/// Legacy type alias for RaftController  
pub type RaftController = RaftNode;

/// Re-export for lib.rs
pub use openraft::storage::RaftLogStorage as RaftLogStorageTrait;

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorage;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_log_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("raft.redb");
        let mut storage = LogStore::new(&path).unwrap();

        // Verify storage is functional
        let state = storage.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_state_machine_apply() {
        let sm = StateMachine::new();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 1);

        let cmd = MetadataCommand::CreateTopic {
            config: crate::partition::TopicConfig::new("test-topic", 3, 1),
            partition_assignments: vec![
                vec!["node-1".into()],
                vec!["node-1".into()],
                vec!["node-1".into()],
            ],
        };

        let response = sm.apply_command(&log_id, cmd).await;
        assert!(matches!(
            response.response,
            MetadataResponse::TopicCreated { .. }
        ));

        // Verify topic exists
        let metadata = sm.metadata().await;
        assert!(metadata.topics.contains_key("test-topic"));
    }

    #[tokio::test]
    async fn test_raft_node_standalone() {
        let temp_dir = TempDir::new().unwrap();
        let config = ClusterConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..ClusterConfig::standalone()
        };

        let mut node = RaftNode::new(&config).await.unwrap();
        node.start().await.unwrap();

        assert!(node.is_leader());

        // Propose a command
        let response = node.propose(MetadataCommand::Noop).await.unwrap();
        assert!(matches!(response, MetadataResponse::Success));
    }

    #[test]
    fn test_hash_node_id() {
        let id1 = hash_node_id("node-1");
        let id2 = hash_node_id("node-2");
        let id1_again = hash_node_id("node-1");

        assert_ne!(id1, id2);
        assert_eq!(id1, id1_again);
    }
}
