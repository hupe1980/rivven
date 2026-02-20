//! Cluster-aware server implementation
//!
//! This module provides a server that can operate in both standalone
//! and cluster modes using the same codebase.
//!
//! ## TLS Support
//!
//! When the `tls` feature is enabled, the server supports:
//! - TLS encryption for client connections
//! - mTLS for service-to-service authentication
//! - Certificate-based identity extraction
//!
//! Enable TLS with CLI flags:
//! ```bash
//! rivvend --tls-enabled --tls-cert server.crt --tls-key server.key
//! ```
//!
//! For mTLS (require client certificates):
//! ```bash
//! rivvend --tls-enabled --tls-cert server.crt --tls-key server.key \
//!         --tls-ca ca.crt --tls-verify-client
//! ```

use crate::cli::Cli;
use crate::handler::RequestHandler;
use crate::partitioner::StickyPartitionerConfig;
use crate::protocol::{
    BrokerInfo, PartitionMetadata, Request, Response, TopicMetadata, WireFormat,
};
use crate::raft_api::RaftApiState;
use bytes::{Bytes, BytesMut};
use rivven_cluster::{
    hash_node_id, ClusterCoordinator, ClusterHealth, CoordinatorState, RaftNode, TopicState,
    Transport, TransportConfig,
};
use rivven_core::{OffsetManager, TopicManager};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, RwLock};
use tracing::{debug, error, info, warn};

#[cfg(feature = "tls")]
use rivven_core::tls::{MtlsMode, TlsAcceptor, TlsConfig, TlsIdentity};

/// Shared statistics for tracking server metrics
#[derive(Debug)]
pub struct ServerStats {
    /// Number of currently active connections
    pub active_connections: AtomicU64,
    /// Total number of requests handled
    pub total_requests: AtomicU64,
    /// Server start time
    pub start_time: std::time::Instant,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            total_requests: AtomicU64::new(0),
            start_time: std::time::Instant::now(),
        }
    }
}

impl ServerStats {
    /// Create new server statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment active connections
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections
    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment total requests
    pub fn request_handled(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current active connections
    pub fn get_active_connections(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Get total requests
    pub fn get_total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Get server uptime
    pub fn uptime(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
}

/// Cluster-aware Rivven server
pub struct ClusterServer {
    /// CLI arguments
    cli: Arc<Cli>,
    /// Topic manager
    topic_manager: TopicManager,
    /// Offset manager
    offset_manager: OffsetManager,
    /// Cluster coordinator (None in standalone mode)
    coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
    /// Cluster transport (None in standalone mode)  
    transport: Option<Arc<Mutex<Transport>>>,
    /// Raft node for consensus
    raft_node: Arc<RwLock<RaftNode>>,
    /// Server statistics
    stats: Arc<ServerStats>,
    /// Shutdown signal
    shutdown_tx: broadcast::Sender<()>,
    /// Rate limiter for DoS protection
    rate_limiter: Arc<crate::rate_limiter::RateLimiter>,
    /// §3.3: Write-ahead log for crash-safe durability on the produce path.
    /// Instantiated after WAL replay during server startup.
    wal: Option<Arc<rivven_core::GroupCommitWal>>,
    /// TLS acceptor for secure connections
    #[cfg(feature = "tls")]
    tls_acceptor: Option<Arc<TlsAcceptor>>,
}

/// Server status information
#[derive(Debug, Clone)]
pub struct ServerStatus {
    /// Server bind address
    pub bind_address: SocketAddr,
    /// Deployment mode
    pub mode: String,
    /// Node ID (if in cluster mode)
    pub node_id: Option<String>,
    /// Cluster health (if in cluster mode)
    pub cluster_health: Option<ClusterHealth>,
    /// Coordinator state (if in cluster mode)
    pub coordinator_state: Option<CoordinatorState>,
    /// Number of active connections
    pub active_connections: u64,
    /// Total requests handled
    pub total_requests: u64,
}

impl ClusterServer {
    /// Create a new cluster-aware server
    pub async fn new(cli: Cli) -> anyhow::Result<Self> {
        // Validate configuration
        cli.validate()
            .map_err(|e| anyhow::anyhow!("Configuration error: {}", e))?;

        let core_config = cli.to_core_config();
        let cluster_config = cli.to_cluster_config();
        let cli = Arc::new(cli);

        // Initialize core components
        let topic_manager = TopicManager::new(core_config.clone());

        // Recover persisted topics from disk
        if let Err(e) = topic_manager.recover().await {
            tracing::warn!("Failed to recover topics from disk: {}", e);
        }

        // Replay WAL records at startup.
        // After topic recovery (which restores partition structure), replay the
        // WAL to re-apply any committed writes that may not have been flushed
        // to segment files before the previous shutdown. Records already
        // reflected in segments are idempotent and safe to skip.
        {
            let wal_dir = std::path::PathBuf::from(&core_config.data_dir).join("wal");
            if wal_dir.exists() {
                let wal_config = rivven_core::WalConfig {
                    dir: wal_dir,
                    ..Default::default()
                };
                match rivven_core::GroupCommitWal::replay_all(&wal_config).await {
                    Ok(records) if !records.is_empty() => {
                        tracing::info!(
                            count = records.len(),
                            "WAL replay: {} records recovered at startup",
                            records.len()
                        );
                        // Apply replayed records to the topic manager
                        for record in &records {
                            if let Err(e) = topic_manager.apply_wal_record(record).await {
                                tracing::warn!(
                                    lsn = record.lsn,
                                    error = %e,
                                    "WAL replay: failed to apply record"
                                );
                            }
                        }
                    }
                    Ok(_) => {
                        tracing::debug!("WAL replay: no records to replay");
                    }
                    Err(e) => {
                        tracing::error!(
                            error = %e,
                            "WAL replay: failed to read WAL files — starting with potential data loss!"
                        );
                    }
                }
            }
        }

        // §3.3 Instantiate the WAL for the produce path.
        // This must happen after replay (which reads old WAL files) so that
        // the new WAL picks up the correct LSN from any existing files.
        // WAL init failure is a hard error — running without WAL risks silent
        // data loss on crash, which is worse than refusing to start.
        let wal = {
            let wal_dir = std::path::PathBuf::from(&core_config.data_dir).join("wal");
            let wal_config = rivven_core::WalConfig {
                dir: wal_dir,
                ..Default::default()
            };
            let w = rivven_core::GroupCommitWal::new(wal_config)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialise WAL: {}", e))?;
            tracing::info!("WAL initialised for produce-path durability");
            Some(w)
        };

        let offset_manager = OffsetManager::with_persistence(
            std::path::PathBuf::from(&core_config.data_dir).join("offsets"),
        )?;

        let (shutdown_tx, _) = broadcast::channel(1);

        // Initialize Raft node (works in both standalone and cluster modes)
        let mut raft_node = RaftNode::new(&cluster_config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create Raft node: {}", e))?;
        raft_node
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start Raft node: {}", e))?;
        let raft_node = Arc::new(RwLock::new(raft_node));

        // Initialize cluster components if in cluster mode
        let (coordinator, transport) = if cli.is_cluster_mode() {
            info!(
                node_id = %cluster_config.node_id,
                cluster_addr = %cluster_config.cluster_addr,
                rack = ?cluster_config.rack,
                "Initializing cluster mode"
            );

            // Create transport with default config (simpler initialization)
            let transport_config = TransportConfig {
                connect_timeout: Duration::from_millis(cli.connect_timeout_ms),
                tcp_nodelay: cli.tcp_nodelay,
                ..Default::default()
            };

            let transport = Transport::new(
                cluster_config.node_id.clone(),
                cluster_config.cluster_addr,
                transport_config,
            );

            // Create coordinator and wire in Raft node
            let mut coordinator = ClusterCoordinator::new(cluster_config).await?;
            coordinator.set_raft_node(raft_node.clone()).await;

            (
                Some(Arc::new(RwLock::new(coordinator))),
                Some(Arc::new(Mutex::new(transport))),
            )
        } else {
            info!("Running in standalone mode");
            (None, None)
        };

        // Initialize rate limiter for DoS protection
        let rate_limit_config = crate::rate_limiter::RateLimitConfig {
            max_connections_per_ip: cli.max_connections_per_ip,
            max_total_connections: cli.max_total_connections,
            rate_limit_per_ip: cli.rate_limit_per_ip,
            max_request_size: cli.max_request_size,
            idle_timeout: Duration::from_secs(cli.idle_timeout_secs),
        };
        let rate_limiter = Arc::new(crate::rate_limiter::RateLimiter::new(rate_limit_config));

        info!(
            max_connections_per_ip = cli.max_connections_per_ip,
            max_total_connections = cli.max_total_connections,
            rate_limit_per_ip = cli.rate_limit_per_ip,
            max_request_size = cli.max_request_size,
            "Rate limiting enabled"
        );

        // Initialize TLS acceptor if enabled
        #[cfg(feature = "tls")]
        let tls_acceptor = if cli.tls_enabled {
            let cert_path = cli
                .tls_cert
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("TLS enabled but --tls-cert not specified"))?;
            let key_path = cli
                .tls_key
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("TLS enabled but --tls-key not specified"))?;

            // Build TLS configuration
            let mut tls_config = if let Some(ref ca_path) = cli.tls_ca {
                // mTLS with CA for client verification
                TlsConfig::mtls_from_pem_files(cert_path, key_path, ca_path)
            } else {
                // TLS without client verification
                TlsConfig::from_pem_files(cert_path, key_path)
            };

            // Set mTLS mode based on CLI flag
            tls_config.mtls_mode = if cli.tls_verify_client {
                MtlsMode::Required
            } else if cli.tls_ca.is_some() {
                MtlsMode::Optional
            } else {
                MtlsMode::Disabled
            };

            let acceptor = TlsAcceptor::new(&tls_config)
                .map_err(|e| anyhow::anyhow!("Failed to initialize TLS: {}", e))?;

            info!(
                tls_mode = ?tls_config.mtls_mode,
                cert = %cert_path.display(),
                "TLS enabled for client connections"
            );

            Some(Arc::new(acceptor))
        } else {
            None
        };

        Ok(Self {
            cli,
            topic_manager,
            offset_manager,
            coordinator,
            transport,
            raft_node,
            stats: Arc::new(ServerStats::new()),
            shutdown_tx,
            rate_limiter,
            wal,
            #[cfg(feature = "tls")]
            tls_acceptor,
        })
    }

    /// Start the server
    pub async fn start(self) -> anyhow::Result<()> {
        let bind_addr = self.cli.bind;
        let api_bind_addr = self.cli.api_bind;
        info!("Starting Rivven server on {}", bind_addr);

        // Build TLS configuration from CLI args
        let tls_config = crate::raft_api::TlsConfig {
            enabled: self.cli.tls_enabled,
            cert_path: self.cli.tls_cert.clone(),
            key_path: self.cli.tls_key.clone(),
            ca_path: self.cli.tls_ca.clone(),
            verify_client: self.cli.tls_verify_client,
        };

        // Start Raft API server with optional TLS and dashboard
        let raft_api_state = RaftApiState::with_tls(
            self.raft_node.clone(),
            self.coordinator.clone(),
            &tls_config,
        )?
        .with_cluster_auth_token(self.cli.cluster_auth_token.clone());
        let tls_config_clone = tls_config.clone();
        let mut shutdown_rx_api = self.shutdown_tx.subscribe();

        // Track all infrastructure task JoinHandles for graceful shutdown.
        // Previously these were fire-and-forget spawns, which leaked resources on
        // shutdown and silently swallowed panics.
        let mut infra_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Start the API server (with or without dashboard based on feature)
        #[cfg(feature = "dashboard")]
        {
            let dashboard_enabled = !self.cli.no_dashboard;
            let stats_clone = self.stats.clone();
            let topic_manager_clone = self.topic_manager.clone();
            let offset_manager_clone = self.offset_manager.clone();

            infra_handles.push(tokio::spawn(async move {
                let dashboard_config = crate::raft_api::DashboardConfig {
                    enabled: dashboard_enabled,
                    stats: stats_clone,
                    topic_manager: topic_manager_clone,
                    offset_manager: offset_manager_clone,
                };
                tokio::select! {
                    result = crate::raft_api::start_raft_api_server_with_dashboard(
                        api_bind_addr,
                        raft_api_state,
                        &tls_config_clone,
                        dashboard_config,
                    ) => {
                        if let Err(e) = result {
                            error!("API server failed: {}", e);
                        }
                    }
                    _ = shutdown_rx_api.recv() => {
                        info!("API server shutting down");
                    }
                }
            }));
        }

        #[cfg(not(feature = "dashboard"))]
        infra_handles.push(tokio::spawn(async move {
            tokio::select! {
                result = crate::raft_api::start_raft_api_server_with_tls(api_bind_addr, raft_api_state, &tls_config_clone) => {
                    if let Err(e) = result {
                        error!("Raft API server failed: {}", e);
                    }
                }
                _ = shutdown_rx_api.recv() => {
                    info!("Raft API server shutting down");
                }
            }
        }));

        // Start cluster coordinator if in cluster mode
        if let Some(coordinator) = &self.coordinator {
            let coord = coordinator.clone();
            let mut coord_shutdown = self.shutdown_tx.subscribe();
            infra_handles.push(tokio::spawn(async move {
                let mut coord = coord.write().await;
                tokio::select! {
                    result = coord.start() => {
                        if let Err(e) = result {
                            error!("Cluster coordinator failed: {}", e);
                        }
                    }
                    _ = coord_shutdown.recv() => {
                        info!("Cluster coordinator shutting down");
                    }
                }
            }));

            // Start cluster transport
            if let Some(transport) = &self.transport {
                let t = transport.clone();
                let mut shutdown_rx = self.shutdown_tx.subscribe();
                infra_handles.push(tokio::spawn(async move {
                    tokio::select! {
                        result = async {
                            let mut transport = t.lock().await;
                            transport.start().await
                        } => {
                            if let Err(e) = result {
                                error!("Cluster transport failed: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            info!("Cluster transport shutting down");
                        }
                    }
                }));
            }

            // Log cluster health periodically
            let coordinator_clone = coordinator.clone();
            let mut health_shutdown = self.shutdown_tx.subscribe();
            infra_handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let coord = coordinator_clone.read().await;
                            let health = coord.health().await;
                            info!(
                                nodes = health.node_count,
                                healthy = health.healthy_nodes,
                                offline_partitions = health.offline_partitions,
                                under_replicated = health.under_replicated_partitions,
                                "Cluster health"
                            );
                        }
                        _ = health_shutdown.recv() => {
                            info!("Cluster health logger shutting down");
                            break;
                        }
                    }
                }
            }));
        }

        // Start client listener
        let listener = TcpListener::bind(bind_addr).await?;

        #[cfg(feature = "tls")]
        let tls_mode = if self.tls_acceptor.is_some() {
            if self.cli.tls_verify_client {
                "mTLS (client cert required)"
            } else if self.cli.tls_ca.is_some() {
                "TLS (client cert optional)"
            } else {
                "TLS"
            }
        } else {
            "plaintext"
        };

        #[cfg(not(feature = "tls"))]
        let tls_mode = "plaintext";

        info!("Server listening on {} (mode: {})", bind_addr, tls_mode);

        // Configure partitioner from CLI
        let partitioner_config = StickyPartitionerConfig {
            batch_size: self.cli.partitioner_batch_size,
            linger_duration: std::time::Duration::from_millis(self.cli.partitioner_linger_ms),
        };

        let mut handler = RequestHandler::with_partitioner_config(
            self.topic_manager.clone(),
            self.offset_manager.clone(),
            partitioner_config,
        );

        // §3.3: Wire the WAL into the request handler for produce-path durability
        if let Some(ref wal) = self.wal {
            handler.set_wal(wal.clone());
        }

        let handler = Arc::new(handler);

        // Spawn background transaction reaper that aborts timed-out transactions.
        // Without this, a client that begins a transaction and disconnects will leak
        // resources indefinitely, potentially blocking partition progress.
        {
            let txn_coordinator = handler.transaction_coordinator().clone();
            let mut txn_shutdown = self.shutdown_tx.subscribe();
            infra_handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let timed_out = txn_coordinator.cleanup_timed_out_transactions();
                            if !timed_out.is_empty() {
                                tracing::warn!(
                                    count = timed_out.len(),
                                    "Transaction reaper aborted timed-out transactions"
                                );
                            }
                        }
                        _ = txn_shutdown.recv() => {
                            break;
                        }
                    }
                }
            }));
        }

        // Create router for partition-aware request handling
        let router = Arc::new(RequestRouter::new(
            self.cli.effective_node_id(),
            self.cli.bind,
            self.coordinator.clone(),
            self.raft_node.clone(),
            handler.clone(),
        ));

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let stats = self.stats.clone();
        let rate_limiter = self.rate_limiter.clone();
        let max_request_size = self.cli.max_request_size;

        // Read timeout for connection data - prevents slowloris attacks
        let read_timeout = rate_limiter.idle_timeout();

        // Get TLS acceptor for connection handling
        #[cfg(feature = "tls")]
        let tls_acceptor = self.tls_acceptor.clone();

        // Connection timeout for TLS handshake
        #[cfg(feature = "tls")]
        let connection_timeout = Duration::from_secs(30);

        // Use JoinSet instead of Mutex<Vec<JoinHandle>> to avoid
        // lock contention on every connection open/close. JoinSet handles
        // automatic cleanup of finished tasks and provides efficient drain.
        let active_connections: Arc<tokio::sync::Mutex<tokio::task::JoinSet<()>>> =
            Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new()));

        // Spawn periodic rate limiter cleanup task
        let cleanup_limiter = rate_limiter.clone();
        let cleanup_shutdown = self.shutdown_tx.subscribe();
        infra_handles.push(tokio::spawn(async move {
            let mut shutdown_rx = cleanup_shutdown;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {
                        cleanup_limiter.cleanup_stale().await;
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        }));

        // Background log compaction worker.
        // Periodically iterates topics whose cleanup policy includes compaction
        // and runs key-based dedup + tombstone removal on sealed segments.
        {
            let compaction_topic_manager = self.topic_manager.clone();
            let compaction_config_manager = handler.topic_config_manager().clone();
            let mut compaction_shutdown = self.shutdown_tx.subscribe();
            infra_handles.push(tokio::spawn(async move {
                // Initial delay to avoid compaction during startup recovery
                tokio::time::sleep(Duration::from_secs(60)).await;
                let mut interval = tokio::time::interval(Duration::from_secs(300));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            match compaction_topic_manager
                                .compact_topics(&compaction_config_manager)
                                .await
                            {
                                Ok(removed) => {
                                    if removed > 0 {
                                        info!(
                                            removed,
                                            "Background compaction cycle completed"
                                        );
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Background compaction cycle failed");
                                }
                            }
                        }
                        _ = compaction_shutdown.recv() => {
                            info!("Background compaction worker shutting down");
                            break;
                        }
                    }
                }
            }));
        }

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((tcp_stream, addr)) => {
                            let client_ip = addr.ip();

                            // enable TCP keepalive so dead connections
                            // (network partition, client crash) are detected instead of
                            // waiting for the full idle timeout.
                            let sock_ref = socket2::SockRef::from(&tcp_stream);
                            let keepalive = socket2::TcpKeepalive::new()
                                .with_time(std::time::Duration::from_secs(60))
                                .with_interval(std::time::Duration::from_secs(10));
                            if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
                                debug!("TCP keepalive setup failed for {}: {}", addr, e);
                            }

                            // Check rate limiter before accepting connection
                            match rate_limiter.try_connection(client_ip).await {
                                Ok(conn_guard) => {
                                    debug!("Connection accepted from {} (rate limit ok)", addr);
                                    let router = router.clone();
                                    let conn_stats = stats.clone();
                                    let connections = active_connections.clone();
                                    let req_limiter = rate_limiter.clone();

                                    #[cfg(feature = "tls")]
                                    let tls_acceptor = tls_acceptor.clone();

                                    // Track connection in stats
                                    conn_stats.connection_opened();

                                    // Spawn into JoinSet directly — no separate tokio::spawn
                                    // + Mutex<Vec> retain loop. JoinSet auto-cleans finished tasks.
                                    let mut conns = connections.lock().await;
                                    conns.spawn(async move {
                                        // conn_guard will be dropped when connection ends

                                        // Handle TLS handshake if enabled
                                        #[cfg(feature = "tls")]
                                        let result = if let Some(ref acceptor) = tls_acceptor {
                                            // TLS connection
                                            match tokio::time::timeout(
                                                connection_timeout,
                                                acceptor.accept_tcp(tcp_stream)
                                            ).await {
                                                Ok(Ok(tls_stream)) => {
                                                    // Extract client certificate info if available
                                                    if let Some(certs) = tls_stream.peer_certificates() {
                                                        if !certs.is_empty() {
                                                            let identity = TlsIdentity::from_certificate(&certs[0]);
                                                            debug!(
                                                                "mTLS client: cn={:?}, org={:?}, fingerprint={}",
                                                                identity.common_name,
                                                                identity.organization,
                                                                &identity.fingerprint[..16]
                                                            );
                                                        }
                                                    }

                                                    handle_connection_with_rate_limit_async(
                                                        tls_stream,
                                                        router,
                                                        conn_stats.clone(),
                                                        req_limiter,
                                                        client_ip,
                                                        max_request_size,
                                                        read_timeout,
                                                    ).await
                                                }
                                                Ok(Err(e)) => {
                                                    warn!("TLS handshake failed from {}: {}", addr, e);
                                                    Ok(())
                                                }
                                                Err(_) => {
                                                    warn!("TLS handshake timeout from {}", addr);
                                                    Ok(())
                                                }
                                            }
                                        } else {
                                            // Plaintext connection
                                            handle_connection_with_rate_limit_async(
                                                tcp_stream,
                                                router,
                                                conn_stats.clone(),
                                                req_limiter,
                                                client_ip,
                                                max_request_size,
                                                read_timeout,
                                            ).await
                                        };

                                        #[cfg(not(feature = "tls"))]
                                        let result = handle_connection_with_rate_limit_async(
                                            tcp_stream,
                                            router,
                                            conn_stats.clone(),
                                            req_limiter,
                                            client_ip,
                                            max_request_size,
                                            read_timeout,
                                        ).await;

                                        conn_stats.connection_closed();
                                        drop(conn_guard); // Explicitly drop to release rate limit slot
                                        if let Err(e) = result {
                                            debug!("Connection from {} closed: {}", addr, e);
                                        }
                                    });
                                }
                                Err(crate::rate_limiter::ConnectionResult::TooManyConnectionsFromIp) => {
                                    warn!("Connection from {} rejected: too many connections from IP", addr);
                                    // Close the stream immediately
                                    drop(tcp_stream);
                                }
                                Err(crate::rate_limiter::ConnectionResult::TooManyTotalConnections) => {
                                    warn!("Connection from {} rejected: global limit reached", addr);
                                    drop(tcp_stream);
                                }
                                Err(crate::rate_limiter::ConnectionResult::Allowed) => {
                                    // This shouldn't happen, but handle gracefully
                                    error!("BUG: Allowed returned as error variant in rate limiter");
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, draining connections...");
                    break;
                }
            }
        }

        // Graceful shutdown: stop accepting new connections and wait for existing ones
        drop(listener);

        // Drain JoinSet — automatically handles finished tasks
        let drain_timeout = tokio::time::Duration::from_secs(10);
        let mut active = active_connections.lock().await;
        let connection_count = active.len();

        if connection_count > 0 {
            info!(
                "Waiting for {} active connections to complete...",
                connection_count
            );

            match tokio::time::timeout(drain_timeout, async {
                while active.join_next().await.is_some() {}
            })
            .await
            {
                Ok(_) => info!("All connections drained"),
                Err(_) => {
                    warn!(
                        "Connection drain timeout, aborting {} remaining",
                        active.len()
                    );
                    active.abort_all();
                }
            }
        }
        drop(active);

        // Await all infrastructure task handles with timeout.
        // The shutdown broadcast has already been sent above, so tasks that
        // listen on shutdown_rx will begin their graceful exit path.
        let infra_count = infra_handles.iter().filter(|h| !h.is_finished()).count();
        if infra_count > 0 {
            info!(
                "Waiting for {} infrastructure tasks to complete...",
                infra_count
            );
            let infra_timeout = Duration::from_secs(5);
            match tokio::time::timeout(infra_timeout, async {
                for handle in infra_handles {
                    if let Err(e) = handle.await {
                        warn!("Infrastructure task panicked: {}", e);
                    }
                }
            })
            .await
            {
                Ok(_) => info!("All infrastructure tasks stopped"),
                Err(_) => warn!("Infrastructure task drain timeout"),
            }
        }

        // §3.3: Shut down WAL before flushing topics.
        // This ensures all group-commit batches are flushed to disk and the
        // background writer thread terminates cleanly.
        if let Some(ref wal) = self.wal {
            info!("Shutting down WAL...");
            if let Err(e) = wal.shutdown().await {
                warn!("WAL shutdown error: {}", e);
            }
        }

        // Flush topic data
        info!("Flushing topic data...");
        if let Err(e) = self.topic_manager.flush_all().await {
            warn!("Error flushing topic data: {}", e);
        }

        // §3.3: Checkpoint WAL — remove WAL files after successful segment flush.
        // All committed writes are now durable in segment files, so WAL records
        // are no longer needed for recovery. This prevents duplicate replay on
        // the next startup.
        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.checkpoint() {
                warn!("WAL checkpoint error: {}", e);
            }
        }

        // If we're the leader, try to step down gracefully
        {
            let raft = self.raft_node.read().await;
            if raft.is_leader() {
                info!("Stepping down from Raft leadership...");
                // Leadership will transfer automatically when we disconnect
            }
        }

        info!("Server shutdown complete");
        Ok(())
    }

    /// Get server status
    pub async fn status(&self) -> ServerStatus {
        let (cluster_health, coordinator_state) = if let Some(coord) = &self.coordinator {
            let coord = coord.read().await;
            let health = coord.health().await;
            let state = coord.state().await;
            (Some(health), Some(state))
        } else {
            (None, None)
        };

        ServerStatus {
            bind_address: self.cli.bind,
            mode: if self.cli.is_cluster_mode() {
                "cluster".to_string()
            } else {
                "standalone".to_string()
            },
            node_id: self.cli.node_id.clone(),
            cluster_health,
            coordinator_state,
            active_connections: self.stats.get_active_connections(),
            total_requests: self.stats.get_total_requests(),
        }
    }

    /// Initiate graceful shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get a shutdown handle that can be used to trigger shutdown from outside
    pub fn get_shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
}

/// Handle for triggering server shutdown from outside
#[derive(Clone)]
pub struct ShutdownHandle {
    shutdown_tx: broadcast::Sender<()>,
}

impl ShutdownHandle {
    /// Trigger graceful shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Request router for partition-aware request handling
pub struct RequestRouter {
    /// Our node ID
    local_node_id: String,
    /// Server bind address (for standalone metadata)
    bind_address: SocketAddr,
    /// Cluster coordinator (None in standalone)
    coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
    /// Raft node for leader information
    raft_node: Arc<RwLock<RaftNode>>,
    /// Local request handler
    handler: Arc<RequestHandler>,
}

/// Routing decision for a request
#[derive(Debug)]
enum RoutingDecision {
    /// Handle locally
    Local,
    /// Forward to another node
    Forward { node_id: String, partition: u32 },
    /// Error - topic or partition not found
    NotFound { reason: String },
}

impl RequestRouter {
    /// Create a new request router
    pub fn new(
        local_node_id: String,
        bind_address: SocketAddr,
        coordinator: Option<Arc<RwLock<ClusterCoordinator>>>,
        raft_node: Arc<RwLock<RaftNode>>,
        handler: Arc<RequestHandler>,
    ) -> Self {
        Self {
            local_node_id,
            bind_address,
            coordinator,
            raft_node,
            handler,
        }
    }

    /// Determine routing for a publish request
    async fn route_publish(
        &self,
        topic: &str,
        partition: Option<u32>,
        key: &Option<Bytes>,
    ) -> RoutingDecision {
        let coord = match &self.coordinator {
            Some(c) => c.read().await,
            None => return RoutingDecision::Local, // Standalone mode
        };

        // Get partition from coordinator
        let partition_id = match partition {
            Some(p) => p,
            None => {
                // Select partition based on key or round-robin
                match coord
                    .select_partition(topic, key.as_ref().map(|b| b.as_ref()))
                    .await
                {
                    Some(p) => p,
                    None => {
                        return RoutingDecision::NotFound {
                            reason: format!("Topic '{}' not found", topic),
                        }
                    }
                }
            }
        };

        // Find partition leader
        match coord.partition_leader(topic, partition_id).await {
            Some(leader) if leader == self.local_node_id => RoutingDecision::Local,
            Some(leader) => RoutingDecision::Forward {
                node_id: leader,
                partition: partition_id,
            },
            None => RoutingDecision::NotFound {
                reason: format!("No leader for {}/{}", topic, partition_id),
            },
        }
    }

    /// Determine routing for a consume request  
    async fn route_consume(&self, topic: &str, partition: u32) -> RoutingDecision {
        let coord = match &self.coordinator {
            Some(c) => c.read().await,
            None => return RoutingDecision::Local,
        };

        // Check if we are in ISR for this partition
        if coord.is_in_isr(topic, partition, &self.local_node_id).await {
            return RoutingDecision::Local;
        }

        // Get ISR members and pick one
        match coord.get_isr_member(topic, partition).await {
            Some(node) => RoutingDecision::Forward {
                node_id: node,
                partition,
            },
            None => RoutingDecision::NotFound {
                reason: format!("No ISR member for {}/{}", topic, partition),
            },
        }
    }

    /// Handle cluster metadata request
    async fn handle_cluster_metadata(&self, topics: &[String]) -> Response {
        // In standalone mode, return minimal info
        let Some(coordinator) = self.coordinator.as_ref() else {
            return Response::ClusterMetadata {
                controller_id: Some(self.local_node_id.clone()),
                brokers: vec![BrokerInfo {
                    node_id: self.local_node_id.clone(),
                    host: self.bind_address.ip().to_string(),
                    port: self.bind_address.port(),
                    rack: None,
                }],
                topics: vec![], // Standalone doesn't track topics in coordinator
            };
        };

        let coord = coordinator.read().await;

        // §6.4: Double read-lock pattern — coordinator guard + nested metadata guard.
        //
        // Both are `tokio::sync::RwLock::read()` guards on **different** objects:
        //   1. `coordinator: RwLock<ClusterCoordinator>` — outer
        //   2. `coord.metadata(): RwLock<Metadata>` — inner (field of coordinator)
        //
        // No deadlock risk: both are read guards, and Tokio's RwLock permits
        // concurrent reads. A **writer** waiting on either lock may be delayed
        // while both reads are held, but the critical section is brief (O(n)
        // where n = number of nodes/topics — typically <100). Metadata mutations
        // go through Raft proposals which acquire the write lock independently,
        // never from within a read guard path.
        let metadata = coord.metadata().read().await;

        // Build broker list from registered nodes
        let brokers: Vec<BrokerInfo> = metadata
            .nodes
            .iter()
            .map(|(node_id, info)| BrokerInfo {
                node_id: node_id.clone(),
                host: info.client_addr.ip().to_string(),
                port: info.client_addr.port(),
                rack: info.rack.clone(),
            })
            .collect();

        // Get controller (Raft leader) from the Raft node
        let raft = self.raft_node.read().await;
        let controller_id = raft.leader().map(|id| {
            // Convert RaftNodeId to string node ID
            // The Raft node ID is a hash, so we need to look up the actual node
            // If we are the leader, return our node ID
            if raft.is_leader() {
                self.local_node_id.clone()
            } else {
                // Find the node with this Raft ID in metadata
                metadata
                    .nodes
                    .iter()
                    .find(|(_, info)| hash_node_id(&info.id) == id)
                    .map(|(node_id, _)| node_id.clone())
                    .unwrap_or_else(|| format!("raft-node-{}", id))
            }
        });

        // Build topic metadata
        let topic_metadata: Vec<TopicMetadata> = if topics.is_empty() {
            // Return all topics
            metadata
                .topics
                .iter()
                .map(|(name, state)| self.build_topic_metadata(name, state))
                .collect()
        } else {
            // Return requested topics
            topics
                .iter()
                .filter_map(|name| {
                    metadata
                        .topics
                        .get(name)
                        .map(|state| self.build_topic_metadata(name, state))
                })
                .collect()
        };

        Response::ClusterMetadata {
            controller_id,
            brokers,
            topics: topic_metadata,
        }
    }

    /// Build topic metadata from topic state
    fn build_topic_metadata(&self, name: &str, state: &TopicState) -> TopicMetadata {
        let partitions: Vec<PartitionMetadata> = state
            .partitions
            .iter()
            .enumerate()
            .map(|(idx, pstate)| PartitionMetadata {
                partition: idx as u32,
                leader: pstate.leader.clone(),
                replicas: pstate.replica_nodes().into_iter().cloned().collect(),
                isr: pstate.isr_nodes().into_iter().cloned().collect(),
                offline: !pstate.online,
            })
            .collect();

        TopicMetadata {
            name: name.to_string(),
            is_internal: name.starts_with("__"), // Convention for internal topics
            partitions,
        }
    }

    /// Route a request to the appropriate handler
    ///
    /// In standalone mode, all requests go to local handler.
    /// In cluster mode:
    /// - Metadata requests can be handled by any node
    /// - Publish requests go to partition leaders
    /// - Consume requests can go to any ISR member
    pub async fn route(&self, request: Request) -> crate::protocol::Response {
        // Handle GetClusterMetadata specially since it needs coordinator access
        if let Request::GetClusterMetadata { topics } = &request {
            return self.handle_cluster_metadata(topics).await;
        }

        // Determine routing decision
        let decision = match &request {
            Request::Publish {
                topic,
                partition,
                key,
                ..
            } => self.route_publish(topic, *partition, key).await,
            Request::IdempotentPublish {
                topic,
                partition,
                key,
                ..
            } => self.route_publish(topic, *partition, key).await,
            Request::Consume {
                topic, partition, ..
            } => self.route_consume(topic, *partition).await,
            // Authentication - handle locally (auth manager is local)
            Request::Authenticate { .. }
            | Request::SaslAuthenticate { .. }
            | Request::ScramClientFirst { .. }
            | Request::ScramClientFinal { .. }
            | Request::Handshake { .. } => RoutingDecision::Local,
            // Metadata and control operations - handle locally (any node can serve)
            Request::CreateTopic { .. }
            | Request::ListTopics
            | Request::DeleteTopic { .. }
            | Request::GetOffset { .. }
            | Request::GetOffsetBounds { .. }
            | Request::GetOffsetForTimestamp { .. }
            | Request::CommitOffset { .. }
            | Request::GetMetadata { .. }
            | Request::Ping
            | Request::GetClusterMetadata { .. }
            | Request::ListGroups
            | Request::DescribeGroup { .. }
            | Request::DeleteGroup { .. }
            | Request::InitProducerId { .. }
            // Transaction operations - coordinator runs locally
            | Request::BeginTransaction { .. }
            | Request::AddPartitionsToTxn { .. }
            | Request::AddOffsetsToTxn { .. }
            | Request::CommitTransaction { .. }
            | Request::AbortTransaction { .. }
            // Quota operations - handle locally (any node can serve)
            | Request::DescribeQuotas { .. }
            | Request::AlterQuotas { .. }
            // Admin API operations - handle locally or on leader
            | Request::AlterTopicConfig { .. }
            | Request::CreatePartitions { .. }
            | Request::DeleteRecords { .. }
            | Request::DescribeTopicConfigs { .. } => RoutingDecision::Local,

            // Transactional publish - route to partition leader like regular publish
            Request::TransactionalPublish {
                topic,
                partition,
                ref key,
                ..
            } => self.route_publish(topic, *partition, key).await,
        };

        // Data-path leader epoch fencing.
        // If the client supplied a leader_epoch, validate it against the current
        // partition leader epoch. If the client's epoch is stale, reject the write
        // immediately instead of letting a stale leader accept it.
        if let Request::Publish {
            topic,
            partition: Some(partition),
            leader_epoch: Some(client_epoch),
            ..
        } = &request
        {
            if let Some(coordinator) = &self.coordinator {
                let coord = coordinator.read().await;
                if let Some(current_epoch) = coord.partition_leader_epoch(topic, *partition).await {
                    if *client_epoch < current_epoch {
                        return Response::Error {
                            message: format!(
                                "FENCED_LEADER_EPOCH: client epoch {} < current epoch {} for {}/{}",
                                client_epoch, current_epoch, topic, partition
                            ),
                        };
                    }
                }
            }
        }

        match decision {
            RoutingDecision::Local => {
                // Handle locally
                self.handler.handle(request).await
            }
            RoutingDecision::Forward { node_id, partition } => {
                debug!(
                    target_node = %node_id,
                    partition = partition,
                    "Request routed to partition leader"
                );

                // Return NOT_LEADER_FOR_PARTITION error following Kafka protocol.
                // Clients should handle this by fetching fresh metadata and
                // connecting directly to the partition leader. This is the standard
                // Kafka approach rather than server-side proxy forwarding.
                Response::Error {
                    message: format!(
                        "NOT_LEADER_FOR_PARTITION: Not leader for partition {}. Leader is node '{}'",
                        partition, node_id
                    ),
                }
            }
            RoutingDecision::NotFound { reason } => Response::Error {
                message: format!("UNKNOWN_TOPIC_OR_PARTITION: {}", reason),
            },
        }
    }
}

/// Handle a single client connection with rate limiting (generic over stream type)
///
/// # Security
/// - Read timeout protects against slowloris-style DoS attacks
/// - Request size limit prevents memory exhaustion
/// - Rate limiting prevents request flooding
async fn handle_connection_with_rate_limit_async<S>(
    mut stream: S,
    router: Arc<RequestRouter>,
    stats: Arc<ServerStats>,
    rate_limiter: Arc<crate::rate_limiter::RateLimiter>,
    client_ip: std::net::IpAddr,
    max_request_size: usize,
    read_timeout: Duration,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buffer = BytesMut::with_capacity(8192);
    let peer_label = client_ip.to_string();

    loop {
        // Use shared framing for read + parse
        let frame = match crate::framing::read_framed_request(
            &mut stream,
            &mut buffer,
            max_request_size,
            read_timeout,
            &peer_label,
        )
        .await?
        {
            Some(crate::framing::ReadFrame::Request {
                request,
                wire_format,
                correlation_id,
            }) => (request, wire_format, correlation_id),
            Some(crate::framing::ReadFrame::Disconnected | crate::framing::ReadFrame::Timeout) => {
                return Ok(());
            }
            None => continue, // size-exceeded or parse error — already sent error response
        };

        let (request, wire_format, correlation_id) = frame;

        // Check rate limit before processing
        // Use a small size estimate (1) since we already validated the message size above
        match rate_limiter.check_request(&client_ip, 1).await {
            crate::rate_limiter::RequestResult::Allowed => {}
            crate::rate_limiter::RequestResult::RateLimited => {
                debug!("Rate limited request from {}", client_ip);
                let error_response = Response::Error {
                    message: "RATE_LIMIT_EXCEEDED: Too many requests".to_string(),
                };
                crate::framing::send_response(
                    &mut stream,
                    &error_response,
                    wire_format,
                    correlation_id,
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(1000)).await;
                return Ok(());
            }
            crate::rate_limiter::RequestResult::RequestTooLarge => {
                return Ok(());
            }
        }

        // Track request
        stats.request_handled();

        // Route and handle request
        let response = router.route(request).await;

        // Serialize and send response using same wire format as request
        let response_bytes = match response.to_wire(wire_format, correlation_id) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                let error_response = Response::Error {
                    message: format!("INTERNAL_ERROR: response serialization failed: {}", e),
                };
                match error_response
                    .to_wire(wire_format, correlation_id)
                    .or_else(|_| error_response.to_wire(WireFormat::Postcard, correlation_id))
                {
                    Ok(err_bytes) => err_bytes,
                    Err(_) => {
                        return Ok(());
                    }
                }
            }
        };

        let len: u32 = response_bytes.len().try_into().map_err(|_| {
            anyhow::anyhow!("response size {} exceeds u32::MAX", response_bytes.len())
        })?;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&response_bytes).await?;
        stream.flush().await?;
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::Cli;
    use clap::Parser;

    #[tokio::test]
    async fn test_standalone_server_creation() {
        let cli = Cli::parse_from(["rivvend"]);
        // Just verify we can create the CLI - full server needs more setup
        assert!(!cli.is_cluster_mode());
    }
}
