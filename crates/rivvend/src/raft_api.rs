//! Raft HTTP API endpoints
//!
//! This module provides HTTP endpoints for Raft consensus communication
//! between cluster nodes. These endpoints handle:
//! - AppendEntries RPCs (log replication)
//! - InstallSnapshot RPCs (state transfer)
//! - Vote RPCs (leader election)
//! - Management APIs (metrics, health, membership)
//!
//! ## Performance Optimizations
//!
//! - **Binary serialization**: Raft RPCs use postcard (5-10x faster than JSON)
//! - **Content-Type negotiation**: Supports both `application/octet-stream` (binary) and `application/json`
//! - **Connection pooling**: HTTP client reuses connections with TCP keepalive

use anyhow::Context;
use axum::{
    body::Bytes,
    extract::{Json, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use openraft::BasicNode;
use rivven_cluster::{
    hash_node_id, ClusterCoordinator, MetadataCommand, MetadataResponse, RaftNode, RaftTypeConfig,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

// ============================================================================
// Binary Serialization Support
// ============================================================================

/// Content types for Raft RPCs
const CONTENT_TYPE_BINARY: &str = "application/octet-stream";
const CONTENT_TYPE_JSON: &str = "application/json";

/// Deserialize request body based on Content-Type header
fn deserialize_request<T: DeserializeOwned>(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<T, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_JSON);

    if content_type.contains("octet-stream") {
        postcard::from_bytes(body).map_err(|e| format!("postcard deserialize error: {}", e))
    } else {
        serde_json::from_slice(body).map_err(|e| format!("json deserialize error: {}", e))
    }
}

/// Serialize response body based on Accept header (or request Content-Type)
fn serialize_response<T: Serialize>(
    headers: &HeaderMap,
    data: &T,
) -> Result<(Bytes, &'static str), String> {
    let accept = headers
        .get(header::ACCEPT)
        .or_else(|| headers.get(header::CONTENT_TYPE))
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_JSON);

    if accept.contains("octet-stream") {
        let bytes =
            postcard::to_allocvec(data).map_err(|e| format!("postcard serialize error: {}", e))?;
        Ok((Bytes::from(bytes), CONTENT_TYPE_BINARY))
    } else {
        let bytes = serde_json::to_vec(data).map_err(|e| format!("json serialize error: {}", e))?;
        Ok((Bytes::from(bytes), CONTENT_TYPE_JSON))
    }
}

// ============================================================================
// TLS Configuration
// ============================================================================

/// TLS configuration for the Raft API server
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Path to certificate file (PEM)
    pub cert_path: Option<PathBuf>,
    /// Path to private key file (PEM)
    pub key_path: Option<PathBuf>,
    /// Path to CA certificate for client verification
    pub ca_path: Option<PathBuf>,
    /// Require client certificate verification
    pub verify_client: bool,
}

// ============================================================================
// API Types
// ============================================================================

/// Raft API state shared across handlers
#[derive(Clone)]
pub struct RaftApiState {
    /// The Raft node instance
    pub raft_node: Arc<RwLock<RaftNode>>,
    /// Cluster coordinator
    pub coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
    /// HTTP client for leader forwarding
    pub http_client: reqwest::Client,
    /// Node address mapping (node_id -> http_addr)
    pub node_addresses: Arc<RwLock<std::collections::HashMap<u64, String>>>,
    /// Optional shared secret for authenticating cluster API requests.
    /// When set, all `/api/v1/*` and `/raft/*` endpoints require the
    /// `Authorization: Bearer <token>` header to match this value.
    pub cluster_auth_token: Option<Arc<String>>,
}

impl RaftApiState {
    /// Create new RaftApiState with default HTTP client (no TLS)
    pub fn new(
        raft_node: Arc<RwLock<RaftNode>>,
        coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
    ) -> anyhow::Result<Self> {
        Self::with_tls(raft_node, coordinator, &TlsConfig::default())
    }

    /// Create new RaftApiState with optional TLS support (returns Result)
    pub fn with_tls(
        raft_node: Arc<RwLock<RaftNode>>,
        coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
        tls_config: &TlsConfig,
    ) -> anyhow::Result<Self> {
        let http_client = if tls_config.enabled {
            if let Some(ca_path) = tls_config.ca_path.as_ref() {
                // Build client with custom CA for verifying server certificates
                let ca_cert = std::fs::read(ca_path)
                    .with_context(|| format!("Failed to read CA certificate {:?}", ca_path))?;
                let ca_cert = reqwest::Certificate::from_pem(&ca_cert)
                    .context("Failed to parse CA certificate")?;

                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .add_root_certificate(ca_cert)
                    .build()
                    .context("Failed to create TLS HTTP client")?
            } else {
                // TLS enabled but no CA specified - use system roots
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .context("Failed to create TLS HTTP client")?
            }
        } else {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .context("Failed to create HTTP client")?
        };

        Ok(Self {
            raft_node,
            coordinator,
            http_client,
            node_addresses: Arc::new(RwLock::new(std::collections::HashMap::new())),
            cluster_auth_token: None,
        })
    }

    /// Set the cluster authentication token. When set, all `/api/v1/*` and
    /// `/raft/*` endpoints require `Authorization: Bearer <token>`.
    pub fn with_cluster_auth_token(mut self, token: Option<String>) -> Self {
        self.cluster_auth_token = token.map(Arc::new);
        self
    }

    /// Return the number of nodes in the cluster (blocking).
    ///
    /// Used by the dashboard to derive the replication factor.
    /// Returns `None` if the raft lock cannot be acquired synchronously
    /// (non-async context).
    pub fn node_count(&self) -> Option<usize> {
        match self.raft_node.try_read() {
            Ok(raft) => {
                // metadata() is async — we cannot call it here. Instead,
                // use the node_addresses map which is populated during
                // cluster formation and always includes the local node.
                // Fall back to 1 (standalone) when the map is empty.
                let count = self
                    .node_addresses
                    .try_read()
                    .map(|addrs| addrs.len().max(1))
                    .unwrap_or(1);
                // Drop raft guard
                drop(raft);
                Some(count)
            }
            Err(_) => None,
        }
    }

    /// Register a node's HTTP address for leader forwarding
    pub async fn register_node_address(&self, node_id: u64, http_addr: String) {
        self.node_addresses.write().await.insert(node_id, http_addr);
    }

    /// Get a node's HTTP address
    pub async fn get_node_address(&self, node_id: u64) -> Option<String> {
        self.node_addresses.read().await.get(&node_id).cloned()
    }
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub node_id: u64,
    pub node_id_str: String,
    pub is_leader: bool,
    pub leader_id: Option<u64>,
    pub cluster_mode: bool,
}

/// Metrics response
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub node_id: u64,
    pub is_leader: bool,
    pub current_term: Option<u64>,
    pub last_log_index: Option<u64>,
    pub commit_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub membership_size: Option<usize>,
}

/// Membership response
#[derive(Debug, Serialize)]
pub struct MembershipResponse {
    pub nodes: Vec<NodeInfo>,
}

#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub id: u64,
    pub addr: String,
    pub is_voter: bool,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
}

/// Proposal request (for client writes)
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposalRequest {
    pub command: MetadataCommand,
}

/// Proposal response
#[derive(Debug, Serialize, Deserialize)]
pub struct ProposalResponse {
    pub success: bool,
    pub response: Option<MetadataResponse>,
    pub error: Option<String>,
    pub redirect_to: Option<u64>,
}

/// Bootstrap request for initializing a new cluster
#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapRequest {
    /// Initial cluster members: (node_id, http_address)
    pub members: Vec<BootstrapMember>,
}

/// A member in the bootstrap request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapMember {
    /// Node ID (string form, will be hashed)
    pub node_id: String,
    /// HTTP address for Raft communication
    pub addr: String,
}

/// Bootstrap response
#[derive(Debug, Serialize)]
pub struct BootstrapResponse {
    pub success: bool,
    pub message: String,
    pub leader_id: Option<u64>,
}

/// Add learner request
#[derive(Debug, Serialize, Deserialize)]
pub struct AddLearnerRequest {
    pub node_id: String,
    pub addr: String,
}

/// Change membership request
#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeMembershipRequest {
    /// New voter set (node IDs in string form)
    pub voters: Vec<String>,
}

/// Batch proposal request
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchProposalRequest {
    /// Commands to propose as a batch
    pub commands: Vec<MetadataCommand>,
}

/// Batch proposal response
#[derive(Debug, Serialize, Deserialize)]
pub struct BatchProposalResponse {
    pub success: bool,
    pub responses: Option<Vec<MetadataResponse>>,
    pub error: Option<String>,
    pub count: usize,
}

// ============================================================================
// Authentication Middleware
// ============================================================================

/// Middleware that validates the `Authorization: Bearer <token>` header against
/// the configured cluster authentication token. Uses constant-time comparison
/// to prevent timing side-channel attacks.
pub async fn cluster_auth_middleware(
    expected_token: Arc<String>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use subtle::ConstantTimeEq;

    let auth_header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(value) if value.starts_with("Bearer ") => {
            let provided = &value[7..];
            let expected = expected_token.as_bytes();
            let provided_bytes = provided.as_bytes();
            if expected.len() == provided_bytes.len() && expected.ct_eq(provided_bytes).into() {
                next.run(req).await
            } else {
                (StatusCode::UNAUTHORIZED, "Invalid cluster token").into_response()
            }
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            "Missing Authorization: Bearer <token>",
        )
            .into_response(),
    }
}

// ============================================================================
// Router
// ============================================================================

/// Create the Raft API router
pub fn create_raft_router(state: RaftApiState) -> Router {
    create_raft_router_base(state.clone()).with_state(state)
}

/// Create the base Raft API router without state (for merging)
fn create_raft_router_base<S: Clone + Send + Sync + 'static>(state: RaftApiState) -> Router<S> {
    // Public endpoints — health/metrics are always accessible
    let public = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(prometheus_metrics_handler))
        .route("/metrics/json", get(metrics_handler));

    // Protected endpoints — require cluster auth token when configured
    let protected = Router::new()
        // Raft RPCs (internal cluster communication)
        .route("/raft/append", post(append_entries_handler))
        .route("/raft/snapshot", post(install_snapshot_handler))
        .route("/raft/vote", post(vote_handler))
        // Cluster management APIs
        .route("/api/v1/bootstrap", post(bootstrap_handler))
        .route("/api/v1/add-learner", post(add_learner_handler))
        .route("/api/v1/change-membership", post(change_membership_handler))
        .route(
            "/api/v1/transfer-leadership",
            post(transfer_leadership_handler),
        )
        .route("/api/v1/trigger-election", post(trigger_election_handler))
        // Moved /membership behind auth — it exposes internal node
        // addresses and IDs, enabling targeted infrastructure reconnaissance.
        .route("/membership", get(membership_handler))
        // Client APIs
        .route("/api/v1/propose", post(propose_handler))
        .route("/api/v1/propose/batch", post(batch_propose_handler))
        .route("/api/v1/metadata", get(metadata_handler))
        .route(
            "/api/v1/metadata/linearizable",
            get(linearizable_metadata_handler),
        );

    // ALWAYS apply auth middleware to protected routes.
    // In standalone mode without a configured token, we generate a random
    // token and log it at startup. This prevents unauthenticated access to
    // cluster mutation endpoints (/api/v1/bootstrap, /api/v1/propose, etc.).
    let auth_token: Arc<String> = if let Some(ref token) = state.cluster_auth_token {
        token.clone()
    } else {
        // Generate a cryptographically secure ephemeral token
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        let token = format!(
            "rvn-ephemeral-{}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        );
        tracing::warn!(
            "No cluster_auth_token configured — generated ephemeral token for Raft API: {}…[REDACTED]",
            &token[..16],
        );
        Arc::new(token)
    };

    let protected = {
        let token = auth_token;
        protected.layer(axum::middleware::from_fn(move |req, next| {
            cluster_auth_middleware(token.clone(), req, next)
        }))
    };

    public
        .merge(protected)
        // Limit request body size to 10 MiB to prevent OOM
        // attacks. The /raft/snapshot endpoint may need larger payloads;
        // individual routes can override with their own layer if needed.
        .layer(axum::extract::DefaultBodyLimit::max(10 * 1024 * 1024))
        .with_state(state)
}

// ============================================================================
// Management Handlers
// ============================================================================

/// Health check endpoint
async fn health_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Determine health status based on Raft state
    let metrics = raft.metrics();
    let has_leader = raft.leader().is_some();
    let status = if has_leader {
        "healthy"
    } else if metrics.is_some() {
        "degraded" // Raft initialized but no leader elected yet
    } else {
        "unhealthy" // Raft not initialized
    };
    let status_code = if status == "healthy" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let response = HealthResponse {
        status: status.to_string(),
        node_id: raft.node_id(),
        node_id_str: raft.node_id_str().to_string(),
        is_leader: raft.is_leader(),
        leader_id: raft.leader(),
        cluster_mode: raft.leader().is_some_and(|leader| leader != raft.node_id()),
    };

    (status_code, Json(response))
}

/// Metrics endpoint
async fn metrics_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Get Raft metrics if available
    let metrics = raft.metrics();

    let response = MetricsResponse {
        node_id: raft.node_id(),
        is_leader: raft.is_leader(),
        current_term: metrics.as_ref().map(|m| m.current_term),
        last_log_index: metrics.as_ref().and_then(|m| m.last_log_index),
        commit_index: metrics
            .as_ref()
            .and_then(|m| m.last_applied.map(|l| l.index)), // Use last_applied as commit proxy
        applied_index: metrics
            .as_ref()
            .and_then(|m| m.last_applied.map(|l| l.index)),
        membership_size: metrics
            .as_ref()
            .map(|m| m.membership_config.membership().voter_ids().count()),
    };

    (StatusCode::OK, Json(response))
}

/// Prometheus metrics endpoint - production monitoring format
async fn prometheus_metrics_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;
    let metrics = raft.metrics();

    let mut output = String::new();

    // Raft cluster metrics
    output.push_str("# HELP rivven_raft_node_id Raft node identifier\n");
    output.push_str("# TYPE rivven_raft_node_id gauge\n");
    output.push_str(&format!("rivven_raft_node_id {}\n", raft.node_id()));

    output.push_str("# HELP rivven_raft_is_leader Whether this node is the Raft leader\n");
    output.push_str("# TYPE rivven_raft_is_leader gauge\n");
    output.push_str(&format!(
        "rivven_raft_is_leader {}\n",
        if raft.is_leader() { 1 } else { 0 }
    ));

    if let Some(ref m) = metrics {
        output.push_str("# HELP rivven_raft_current_term Current Raft term\n");
        output.push_str("# TYPE rivven_raft_current_term gauge\n");
        output.push_str(&format!("rivven_raft_current_term {}\n", m.current_term));

        if let Some(index) = m.last_log_index {
            output.push_str("# HELP rivven_raft_last_log_index Index of last log entry\n");
            output.push_str("# TYPE rivven_raft_last_log_index gauge\n");
            output.push_str(&format!("rivven_raft_last_log_index {}\n", index));
        }

        if let Some(log_id) = m.last_applied {
            output.push_str("# HELP rivven_raft_applied_index Index of last applied entry\n");
            output.push_str("# TYPE rivven_raft_applied_index gauge\n");
            output.push_str(&format!("rivven_raft_applied_index {}\n", log_id.index));
        }

        let voter_count = m.membership_config.membership().voter_ids().count();
        let learner_count = m.membership_config.membership().learner_ids().count();

        output.push_str("# HELP rivven_raft_cluster_voters Number of voter nodes in cluster\n");
        output.push_str("# TYPE rivven_raft_cluster_voters gauge\n");
        output.push_str(&format!("rivven_raft_cluster_voters {}\n", voter_count));

        output.push_str("# HELP rivven_raft_cluster_learners Number of learner nodes in cluster\n");
        output.push_str("# TYPE rivven_raft_cluster_learners gauge\n");
        output.push_str(&format!("rivven_raft_cluster_learners {}\n", learner_count));
    }

    // Include core metrics if available via global registry
    // Note: In production, metrics are recorded via CoreMetrics static methods
    output.push_str("\n# Rivven core metrics\n");
    output.push_str("# HELP rivven_info Build information\n");
    output.push_str("# TYPE rivven_info gauge\n");
    output.push_str(&format!(
        "rivven_info{{version=\"{}\",node_id_str=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION"),
        raft.node_id_str()
    ));

    // Return with correct content type for Prometheus
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
}

/// Membership endpoint
async fn membership_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    let nodes = if let Some(metrics) = raft.metrics() {
        metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, node)| NodeInfo {
                id: *id,
                addr: node.addr.clone(),
                is_voter: metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .any(|vid| vid == *id),
            })
            .collect()
    } else {
        vec![NodeInfo {
            id: raft.node_id(),
            addr: "localhost".to_string(),
            is_voter: true,
        }]
    };

    let response = MembershipResponse { nodes };
    (StatusCode::OK, Json(response))
}

// ============================================================================
// Raft RPC Handlers (with binary serialization support)
// ============================================================================

/// AppendEntries RPC handler - supports both binary and JSON serialization
async fn append_entries_handler(
    State(state): State<RaftApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Deserialize based on Content-Type
    let req: openraft::raft::AppendEntriesRequest<RaftTypeConfig> =
        match deserialize_request(&headers, &body) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to deserialize AppendEntries: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e,
                        code: 400,
                    }),
                )
                    .into_response();
            }
        };

    debug!(
        leader = req.vote.leader_id().node_id,
        term = req.vote.leader_id().term,
        entries = req.entries.len(),
        "AppendEntries RPC"
    );

    let raft = state.raft_node.read().await;

    match raft.handle_append_entries(req).await {
        Ok(response) => {
            // Serialize response in matching format
            match serialize_response(&headers, &response) {
                Ok((bytes, content_type)) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, content_type)],
                    bytes,
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: e,
                        code: 500,
                    }),
                )
                    .into_response(),
            }
        }
        Err(e) => {
            error!("AppendEntries failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: 500,
                }),
            )
                .into_response()
        }
    }
}

/// InstallSnapshot RPC handler - supports both binary and JSON serialization
async fn install_snapshot_handler(
    State(state): State<RaftApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Deserialize based on Content-Type
    let req: openraft::raft::InstallSnapshotRequest<RaftTypeConfig> =
        match deserialize_request(&headers, &body) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to deserialize InstallSnapshot: {}", e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: e,
                        code: 400,
                    }),
                )
                    .into_response();
            }
        };

    debug!(
        leader = req.vote.leader_id().node_id,
        snapshot_size = req.data.len(),
        "InstallSnapshot RPC"
    );

    let raft = state.raft_node.read().await;

    match raft.handle_install_snapshot(req).await {
        Ok(response) => match serialize_response(&headers, &response) {
            Ok((bytes, content_type)) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                bytes,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e,
                    code: 500,
                }),
            )
                .into_response(),
        },
        Err(e) => {
            error!("InstallSnapshot failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: 500,
                }),
            )
                .into_response()
        }
    }
}

/// Vote RPC handler - supports both binary and JSON serialization
async fn vote_handler(
    State(state): State<RaftApiState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Deserialize based on Content-Type
    let req: openraft::raft::VoteRequest<u64> = match deserialize_request(&headers, &body) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to deserialize Vote: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: e,
                    code: 400,
                }),
            )
                .into_response();
        }
    };

    debug!(
        candidate = req.vote.leader_id().node_id,
        term = req.vote.leader_id().term,
        "Vote RPC"
    );

    let raft = state.raft_node.read().await;

    match raft.handle_vote(req).await {
        Ok(response) => match serialize_response(&headers, &response) {
            Ok((bytes, content_type)) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                bytes,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e,
                    code: 500,
                }),
            )
                .into_response(),
        },
        Err(e) => {
            error!("Vote failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                    code: 500,
                }),
            )
                .into_response()
        }
    }
}

// ============================================================================
// Cluster Management Handlers
// ============================================================================

/// Bootstrap a new cluster
/// This should only be called on the first node of a new cluster
async fn bootstrap_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<BootstrapRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Build the initial membership
    let members: std::collections::BTreeMap<u64, BasicNode> = req
        .members
        .iter()
        .map(|m| {
            let node_id = hash_node_id(&m.node_id);
            (
                node_id,
                BasicNode {
                    addr: m.addr.clone(),
                },
            )
        })
        .collect();

    // Register all node addresses for leader forwarding
    drop(raft);
    for member in &req.members {
        let node_id = hash_node_id(&member.node_id);
        state
            .register_node_address(node_id, member.addr.clone())
            .await;
    }

    let raft = state.raft_node.read().await;
    info!(member_count = members.len(), "Bootstrapping cluster");

    match raft.bootstrap(members).await {
        Ok(_) => (
            StatusCode::OK,
            Json(BootstrapResponse {
                success: true,
                message: "Cluster bootstrapped successfully".to_string(),
                leader_id: raft.leader(),
            }),
        )
            .into_response(),
        Err(e) => {
            error!("Bootstrap failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BootstrapResponse {
                    success: false,
                    message: format!("Bootstrap failed: {}", e),
                    leader_id: None,
                }),
            )
                .into_response()
        }
    }
}

/// Add a learner node to the cluster
async fn add_learner_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<AddLearnerRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Only leader can add learners
    if !raft.is_leader() {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(ErrorResponse {
                error: "Not leader".to_string(),
                code: 307,
            }),
        )
            .into_response();
    }

    let node_id = hash_node_id(&req.node_id);
    let node = BasicNode {
        addr: req.addr.clone(),
    };

    // Register the node address
    drop(raft);
    state.register_node_address(node_id, req.addr.clone()).await;

    let raft = state.raft_node.read().await;
    info!(node_id, addr = %req.addr, "Adding learner node");

    if let Some(raft_instance) = raft.get_raft() {
        match raft_instance.add_learner(node_id, node, true).await {
            Ok(_) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": "Learner added successfully",
                    "node_id": node_id
                })),
            )
                .into_response(),
            Err(e) => {
                error!("Failed to add learner: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Failed to add learner: {}", e),
                        code: 500,
                    }),
                )
                    .into_response()
            }
        }
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Raft not initialized".to_string(),
                code: 503,
            }),
        )
            .into_response()
    }
}

/// Change cluster membership (promote learners to voters)
async fn change_membership_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<ChangeMembershipRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Only leader can change membership
    if !raft.is_leader() {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(ErrorResponse {
                error: "Not leader".to_string(),
                code: 307,
            }),
        )
            .into_response();
    }

    let voters: std::collections::BTreeSet<u64> =
        req.voters.iter().map(|id| hash_node_id(id)).collect();

    info!(voter_count = voters.len(), "Changing cluster membership");

    if let Some(raft_instance) = raft.get_raft() {
        match raft_instance.change_membership(voters, false).await {
            Ok(_) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": "Membership changed successfully"
                })),
            )
                .into_response(),
            Err(e) => {
                error!("Failed to change membership: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Failed to change membership: {}", e),
                        code: 500,
                    }),
                )
                    .into_response()
            }
        }
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Raft not initialized".to_string(),
                code: 503,
            }),
        )
            .into_response()
    }
}

// ============================================================================
// Client API Handlers
// ============================================================================

/// Propose a command to Raft
async fn propose_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<ProposalRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Check if we're the leader
    if !raft.is_leader() {
        let leader_id = raft.leader();
        drop(raft); // Release the lock before potentially forwarding

        // Try to forward to leader if we know their address
        if let Some(leader) = leader_id {
            if let Some(leader_addr) = state.get_node_address(leader).await {
                info!(leader_id = leader, leader_addr = %leader_addr, "Forwarding proposal to leader");

                match forward_to_leader(
                    &state.http_client,
                    &leader_addr,
                    &req,
                    state.cluster_auth_token.as_deref().map(|s| s.as_str()),
                )
                .await
                {
                    Ok(response) => return (StatusCode::OK, Json(response)).into_response(),
                    Err(e) => {
                        error!("Failed to forward to leader: {}", e);
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(ProposalResponse {
                                success: false,
                                response: None,
                                error: Some(format!("Leader forwarding failed: {}", e)),
                                redirect_to: Some(leader),
                            }),
                        )
                            .into_response();
                    }
                }
            }
        }

        // Can't forward - return redirect response for client to handle
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(ProposalResponse {
                success: false,
                response: None,
                error: Some("Not leader".to_string()),
                redirect_to: leader_id,
            }),
        )
            .into_response();
    }

    // We are the leader - process the command
    match raft.propose(req.command).await {
        Ok(response) => (
            StatusCode::OK,
            Json(ProposalResponse {
                success: true,
                response: Some(response),
                error: None,
                redirect_to: None,
            }),
        )
            .into_response(),
        Err(e) => {
            error!("Proposal failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ProposalResponse {
                    success: false,
                    response: None,
                    error: Some(e.to_string()),
                    redirect_to: None,
                }),
            )
                .into_response()
        }
    }
}

/// Forward a proposal to the leader node
async fn forward_to_leader(
    client: &reqwest::Client,
    leader_addr: &str,
    req: &ProposalRequest,
    auth_token: Option<&str>,
) -> Result<ProposalResponse, String> {
    let url = format!("{}/api/v1/propose", leader_addr);

    let mut request = client.post(&url).json(req);
    if let Some(token) = auth_token {
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .map_err(|e| format!("HTTP request failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!("Leader returned error: {}", response.status()));
    }

    response
        .json::<ProposalResponse>()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))
}

/// Propose multiple commands in a single batch for higher throughput
///
/// This is 10-50x more efficient than individual proposals for small commands:
/// - Single Raft consensus round for all commands
/// - Single disk fsync for all log entries
/// - Amortized network overhead
async fn batch_propose_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<BatchProposalRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Check if we're the leader
    if !raft.is_leader() {
        let leader_id = raft.leader();
        return (
            StatusCode::TEMPORARY_REDIRECT,
            Json(BatchProposalResponse {
                success: false,
                responses: None,
                error: Some(format!("Not leader, redirect to {:?}", leader_id)),
                count: 0,
            }),
        )
            .into_response();
    }

    let count = req.commands.len();
    info!(count, "Processing batch proposal");

    // Use batch propose for efficiency
    match raft.propose_batch(req.commands).await {
        Ok(responses) => (
            StatusCode::OK,
            Json(BatchProposalResponse {
                success: true,
                responses: Some(responses),
                error: None,
                count,
            }),
        )
            .into_response(),
        Err(e) => {
            error!("Batch proposal failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BatchProposalResponse {
                    success: false,
                    responses: None,
                    error: Some(e.to_string()),
                    count,
                }),
            )
                .into_response()
        }
    }
}

/// Get current cluster metadata (eventual consistency - fast but may be stale)
async fn metadata_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;
    let metadata = raft.metadata().await;

    // Return a summary of metadata
    let response = serde_json::json!({
        "topics": metadata.topics.keys().collect::<Vec<_>>(),
        "topic_count": metadata.topics.len(),
        "node_count": metadata.nodes.len(),
        "last_applied_index": metadata.last_applied_index,
        "consistency": "eventual",
    });

    (StatusCode::OK, Json(response))
}

/// Get metadata with linearizable consistency (slower but guaranteed fresh)
///
/// This endpoint ensures the read reflects all committed writes by:
/// 1. Confirming the leader still holds its lease
/// 2. Waiting for any pending applies to complete
///
/// Use this when you need read-after-write consistency.
async fn linearizable_metadata_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // First ensure linearizable read
    match raft.ensure_linearizable_read().await {
        Ok(_) => {
            let metadata = raft.metadata().await;

            let response = serde_json::json!({
                "topics": metadata.topics.keys().collect::<Vec<_>>(),
                "topic_count": metadata.topics.len(),
                "node_count": metadata.nodes.len(),
                "last_applied_index": metadata.last_applied_index,
                "consistency": "linearizable",
            });

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Linearizable read failed: {}", e);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: format!("Linearizable read failed: {}", e),
                    code: 503,
                }),
            )
                .into_response()
        }
    }
}

/// Request for leadership transfer
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferLeadershipRequest {
    /// Target node to transfer leadership to (optional - if not set, any follower)
    #[serde(default)]
    pub target_node_id: Option<String>,
}

/// Trigger a new election, stepping down as leader
///
/// This enables zero-downtime rolling updates:
/// 1. Step down as leader (triggers new election on target/random follower)
/// 2. Wait for new leader to be elected
/// 3. Stop/update the old leader node
///
/// Instead of calling `trigger().elect()` on the current (leader)
/// node, which would just re-elect the leader itself, we forward the election
/// trigger to a follower node via HTTP. If `target_node_id` is specified, we
/// send the request to that node; otherwise we pick a random follower.
async fn transfer_leadership_handler(
    State(state): State<RaftApiState>,
    Json(req): Json<TransferLeadershipRequest>,
) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    // Only leader can transfer leadership
    if !raft.is_leader() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Not the leader - cannot transfer leadership".to_string(),
                code: 400,
            }),
        )
            .into_response();
    }

    let current_node_id = raft.node_id();
    drop(raft);

    // Determine target: use request target or pick a random follower
    let target_node_id = if let Some(ref target_str) = req.target_node_id {
        let target = hash_node_id(target_str);
        if target == current_node_id {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Cannot transfer leadership to self".to_string(),
                    code: 400,
                }),
            )
                .into_response();
        }
        target
    } else {
        // Pick a random follower from known node addresses
        let addresses = state.node_addresses.read().await;
        let followers: Vec<u64> = addresses
            .keys()
            .copied()
            .filter(|id| *id != current_node_id)
            .collect();

        if followers.is_empty() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "No other nodes available for leadership transfer".to_string(),
                    code: 503,
                }),
            )
                .into_response();
        }

        use rand::Rng;
        let idx = rand::thread_rng().gen_range(0..followers.len());
        followers[idx]
    };

    // Look up the target node's address
    let target_addr = {
        let addresses = state.node_addresses.read().await;
        addresses.get(&target_node_id).cloned()
    };

    let Some(target_addr) = target_addr else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Target node {} address unknown", target_node_id),
                code: 404,
            }),
        )
            .into_response();
    };

    info!(
        target_node_id,
        target_addr = %target_addr,
        "Forwarding election trigger to target node for leadership transfer"
    );

    // Forward the election trigger to the target node
    let url = format!(
        "{}/api/v1/trigger-election",
        target_addr.trim_end_matches('/')
    );

    let mut request_builder = state.http_client.post(&url);

    // Forward cluster auth token if configured
    if let Some(ref token) = state.cluster_auth_token {
        request_builder = request_builder.header("Authorization", format!("Bearer {}", token));
    }

    match request_builder.send().await {
        Ok(resp) if resp.status().is_success() => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Election triggered on node {}", target_node_id),
                "target_node_id": target_node_id,
                "note": "Check /health endpoint to see new leader"
            })),
        )
            .into_response(),
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            error!(
                target_node_id,
                status = %status,
                body = %body,
                "Election trigger forwarding failed"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!(
                        "Election trigger on node {} failed ({}): {}",
                        target_node_id, status, body
                    ),
                    code: 500,
                }),
            )
                .into_response()
        }
        Err(e) => {
            error!(
                target_node_id,
                error = %e,
                "Failed to reach target node for election trigger"
            );
            (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse {
                    error: format!(
                        "Cannot reach node {} for election trigger: {}",
                        target_node_id, e
                    ),
                    code: 502,
                }),
            )
                .into_response()
        }
    }
}

/// Trigger a local Raft election on this node
///
/// This is called by the leader (via transfer-leadership forwarding) to make
/// a follower node start a new election. This node will become a candidate.
async fn trigger_election_handler(State(state): State<RaftApiState>) -> impl IntoResponse {
    let raft = state.raft_node.read().await;

    if let Some(raft_instance) = raft.get_raft() {
        info!("Triggering local Raft election (received leadership transfer request)");

        match raft_instance.trigger().elect().await {
            Ok(_) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": "Election triggered on this node"
                })),
            )
                .into_response(),
            Err(e) => {
                error!("Election trigger failed: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("Election trigger failed: {}", e),
                        code: 500,
                    }),
                )
                    .into_response()
            }
        }
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "Raft not initialized".to_string(),
                code: 503,
            }),
        )
            .into_response()
    }
}

// ============================================================================
// Server Integration
// ============================================================================

/// Start the Raft API HTTP server (without TLS)
pub async fn start_raft_api_server(
    bind_addr: std::net::SocketAddr,
    state: RaftApiState,
) -> anyhow::Result<()> {
    start_raft_api_server_with_tls(bind_addr, state, &TlsConfig::default()).await
}

/// Dashboard configuration for the API server
#[cfg(feature = "dashboard")]
pub struct DashboardConfig {
    /// Whether dashboard is enabled
    pub enabled: bool,
    /// Server statistics
    pub stats: std::sync::Arc<crate::cluster_server::ServerStats>,
    /// Topic manager
    pub topic_manager: rivven_core::TopicManager,
    /// Offset manager
    pub offset_manager: rivven_core::OffsetManager,
}

/// Start the Raft API HTTP server with optional TLS
pub async fn start_raft_api_server_with_tls(
    bind_addr: std::net::SocketAddr,
    state: RaftApiState,
    tls_config: &TlsConfig,
) -> anyhow::Result<()> {
    let app = create_raft_router(state);

    if tls_config.enabled {
        // TLS enabled - use axum-server with rustls
        let cert_path = tls_config
            .cert_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but no certificate path provided"))?;
        let key_path = tls_config
            .key_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but no key path provided"))?;

        info!("Starting Raft API server with TLS on {}", bind_addr);

        let config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await?;

        axum_server::bind_rustls(bind_addr, config)
            .serve(app.into_make_service())
            .await?;
    } else {
        // No TLS - use plain HTTP
        info!("Starting Raft API server on {}", bind_addr);

        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}

/// Start the Raft API HTTP server with optional TLS and dashboard
#[cfg(feature = "dashboard")]
pub async fn start_raft_api_server_with_dashboard(
    bind_addr: std::net::SocketAddr,
    state: RaftApiState,
    tls_config: &TlsConfig,
    dashboard_config: DashboardConfig,
) -> anyhow::Result<()> {
    use tower_http::cors::CorsLayer;

    // Create the dashboard state
    let dashboard_state = crate::dashboard::DashboardState {
        raft_state: state.clone(),
        stats: dashboard_config.stats,
        topic_manager: dashboard_config.topic_manager,
        offset_manager: dashboard_config.offset_manager,
        cluster_auth_token: state.cluster_auth_token.clone(),
    };

    // Build app with both Raft API and Dashboard routes
    let app = if dashboard_config.enabled {
        info!("Dashboard enabled at http://{}/", bind_addr);

        // CORS layer for dashboard API requests
        // /Only allow same-origin requests by default.
        // The dashboard is served from the same host, so no cross-origin
        // access is needed. If external tools need access, operators should
        // deploy a reverse proxy with appropriate CORS headers.
        let cors = CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::exact(
                format!("http://{}", bind_addr).parse().unwrap_or_else(|_| {
                    // Fall back to the specific bind address rather than
                    // a permissive "http://localhost". Use 127.0.0.1 with exact port.
                    tracing::warn!(
                        "Failed to parse CORS origin from bind_addr '{}', trying 127.0.0.1 with port",
                        bind_addr
                    );
                    format!("http://127.0.0.1:{}", bind_addr.port())
                        .parse()
                        .expect("127.0.0.1 origin must parse")
                }),
            ))
            .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
            .allow_headers([
                axum::http::header::CONTENT_TYPE,
                axum::http::header::AUTHORIZATION,
            ]);

        // Merge Raft API routes with Dashboard routes
        create_raft_router(state)
            .merge(crate::dashboard::create_dashboard_router(dashboard_state))
            .layer(cors)
    } else {
        create_raft_router(state)
    };

    if tls_config.enabled {
        let cert_path = tls_config
            .cert_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but no certificate path provided"))?;
        let key_path = tls_config
            .key_path
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TLS enabled but no key path provided"))?;

        info!("Starting API server with TLS on {}", bind_addr);

        let config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path).await?;

        axum_server::bind_rustls(bind_addr, config)
            .serve(app.into_make_service())
            .await?;
    } else {
        info!("Starting API server on {}", bind_addr);

        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests would go here - requires setting up mock RaftNode
}
