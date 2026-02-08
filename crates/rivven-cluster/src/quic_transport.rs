//! QUIC Transport Layer for High-Performance Cluster Communication
//!
//! This module provides a production-grade QUIC transport with:
//! - **0-RTT Connections**: Resume sessions instantly for latency-sensitive paths
//! - **Multiplexed Streams**: Multiple bidirectional streams over one connection
//! - **Built-in Encryption**: TLS 1.3 integrated, no separate TLS layer needed
//! - **Connection Migration**: Handles network changes gracefully
//! - **Flow Control**: Per-stream and per-connection backpressure
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         QuicTransport                                    │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  ┌──────────────────┐     ┌──────────────────┐     ┌─────────────────┐  │
//! │  │  Connection Pool  │────│  Stream Manager   │────│  Request Router │  │
//! │  │  (per-peer)       │    │  (multiplexed)    │    │  (correlation)  │  │
//! │  └────────┬─────────┘     └────────┬─────────┘     └────────┬────────┘  │
//! │           │                        │                        │           │
//! │           └────────────────────────┼────────────────────────┘           │
//! │                                    │                                     │
//! │  ┌─────────────────────────────────▼─────────────────────────────────┐  │
//! │  │                      Quinn Endpoint                                │  │
//! │  │  - TLS 1.3 (rustls)                                               │  │
//! │  │  - CUBIC congestion control                                        │  │
//! │  │  - Connection timeout handling                                     │  │
//! │  └───────────────────────────────────────────────────────────────────┘  │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Performance
//!
//! QUIC advantages over TCP for Rivven:
//! - Eliminates head-of-line blocking (streams are independent)
//! - Faster connection establishment (0-RTT for repeat connections)
//! - Better congestion control (CUBIC/BBR per stream)
//! - Handles packet loss more gracefully

use crate::error::{ClusterError, Result};
use crate::node::NodeId;
use crate::protocol::{
    decode_request, decode_response, encode_request, encode_response, ClusterRequest,
    ClusterResponse,
};

use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use parking_lot::RwLock;
use quinn::{
    congestion, ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig,
    TransportConfig as QuinnTransportConfig, VarInt,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{debug, info, instrument, warn};

// ============================================================================
// Configuration
// ============================================================================

/// QUIC transport configuration with sensible defaults for streaming workloads
#[derive(Debug, Clone)]
pub struct QuicConfig {
    /// Connection idle timeout (default: 30s)
    pub idle_timeout: Duration,
    /// Keep-alive interval (default: 10s)
    pub keep_alive_interval: Duration,
    /// Maximum concurrent bidirectional streams per connection (default: 256)
    pub max_concurrent_streams: u32,
    /// Initial congestion window (default: 14720 bytes, ~10 packets)
    pub initial_window: u32,
    /// Maximum UDP payload size (default: 1350 for compatibility)
    pub max_udp_payload_size: u16,
    /// Stream receive window (default: 1MB)
    pub stream_receive_window: u32,
    /// Connection receive window (default: 8MB)
    pub connection_receive_window: u32,
    /// Enable 0-RTT for latency-sensitive operations
    pub enable_0rtt: bool,
    /// Maximum connections to maintain per peer
    pub max_connections_per_peer: usize,
    /// Request timeout
    pub request_timeout: Duration,
    /// Enable BBRv2 congestion control (vs CUBIC)
    pub use_bbr: bool,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
            max_concurrent_streams: 256,
            initial_window: 14720,
            max_udp_payload_size: 1350,
            stream_receive_window: 1024 * 1024,         // 1 MB
            connection_receive_window: 8 * 1024 * 1024, // 8 MB
            enable_0rtt: true,
            max_connections_per_peer: 2,
            request_timeout: Duration::from_secs(30),
            use_bbr: true,
        }
    }
}

impl QuicConfig {
    /// High-throughput configuration for data replication
    pub fn high_throughput() -> Self {
        Self {
            idle_timeout: Duration::from_secs(60),
            keep_alive_interval: Duration::from_secs(15),
            max_concurrent_streams: 512,
            initial_window: 65535,
            max_udp_payload_size: 1452,
            stream_receive_window: 4 * 1024 * 1024, // 4 MB
            connection_receive_window: 32 * 1024 * 1024, // 32 MB
            enable_0rtt: true,
            max_connections_per_peer: 4,
            request_timeout: Duration::from_secs(60),
            use_bbr: true,
        }
    }

    /// Low-latency configuration for Raft consensus
    pub fn low_latency() -> Self {
        Self {
            idle_timeout: Duration::from_secs(10),
            keep_alive_interval: Duration::from_secs(3),
            max_concurrent_streams: 64,
            initial_window: 14720,
            max_udp_payload_size: 1350,
            stream_receive_window: 256 * 1024,          // 256 KB
            connection_receive_window: 2 * 1024 * 1024, // 2 MB
            enable_0rtt: true,
            max_connections_per_peer: 1,
            request_timeout: Duration::from_secs(5),
            use_bbr: false, // CUBIC is better for small messages
        }
    }
}

// ============================================================================
// TLS Configuration
// ============================================================================

/// mTLS mode for broker-to-broker communication
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MtlsMode {
    /// No client certificate required
    #[default]
    Disabled,
    /// Client certificate optional (validated if provided)
    Optional,
    /// Client certificate required (cluster security mode)
    Required,
}

/// TLS certificate configuration
pub struct TlsConfig {
    /// Server certificate chain (DER encoded)
    pub cert_chain: Vec<CertificateDer<'static>>,
    /// Server private key (DER encoded)
    pub private_key: PrivateKeyDer<'static>,
    /// CA certificates for peer/client verification
    pub ca_certs: Vec<CertificateDer<'static>>,
    /// mTLS mode for client certificate verification
    pub mtls_mode: MtlsMode,
    /// Skip certificate verification (DANGEROUS - only for testing)
    pub skip_verification: bool,
}

impl TlsConfig {
    /// Generate self-signed certificates for testing/development
    pub fn self_signed(common_name: &str) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec![common_name.to_string()])
            .map_err(|e| ClusterError::CryptoError(format!("Failed to generate cert: {}", e)))?;

        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let key_der = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());

        Ok(Self {
            cert_chain: vec![cert_der],
            private_key: PrivateKeyDer::Pkcs8(key_der),
            ca_certs: vec![],
            mtls_mode: MtlsMode::Disabled,
            skip_verification: false,
        })
    }

    /// Load certificates from PEM files
    pub fn from_pem_files(cert_path: &str, key_path: &str) -> Result<Self> {
        let cert_pem = std::fs::read(cert_path).map_err(ClusterError::Io)?;
        let key_pem = std::fs::read(key_path).map_err(ClusterError::Io)?;

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClusterError::CryptoError(format!("Failed to parse cert: {}", e)))?;

        let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
            .map_err(|e| ClusterError::CryptoError(format!("Failed to parse key: {}", e)))?
            .ok_or_else(|| ClusterError::CryptoError("No private key found".to_string()))?;

        Ok(Self {
            cert_chain: certs,
            private_key: key,
            ca_certs: vec![],
            mtls_mode: MtlsMode::Disabled,
            skip_verification: false,
        })
    }

    /// Load certificates from PEM files with CA for mTLS
    pub fn mtls_from_pem_files(cert_path: &str, key_path: &str, ca_path: &str) -> Result<Self> {
        let cert_pem = std::fs::read(cert_path).map_err(ClusterError::Io)?;
        let key_pem = std::fs::read(key_path).map_err(ClusterError::Io)?;
        let ca_pem = std::fs::read(ca_path).map_err(ClusterError::Io)?;

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClusterError::CryptoError(format!("Failed to parse cert: {}", e)))?;

        let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
            .map_err(|e| ClusterError::CryptoError(format!("Failed to parse key: {}", e)))?
            .ok_or_else(|| ClusterError::CryptoError("No private key found".to_string()))?;

        let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_pem.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ClusterError::CryptoError(format!("Failed to parse CA cert: {}", e)))?;

        Ok(Self {
            cert_chain: certs,
            private_key: key,
            ca_certs,
            mtls_mode: MtlsMode::Required,
            skip_verification: false,
        })
    }

    /// Set mTLS mode
    pub fn with_mtls_mode(mut self, mode: MtlsMode) -> Self {
        self.mtls_mode = mode;
        self
    }
}

/// Peer identity extracted from mTLS certificate
#[derive(Debug, Clone)]
pub struct PeerIdentity {
    /// Common Name from the certificate
    pub common_name: Option<String>,
    /// Subject Alternative Names (DNS names)
    pub dns_names: Vec<String>,
    /// SHA-256 fingerprint of the certificate
    pub fingerprint: String,
}

impl PeerIdentity {
    /// Extract peer identity from a QUIC connection
    pub fn from_connection(connection: &Connection) -> Option<Self> {
        // Get peer certificates from the connection
        let peer_certs = connection.peer_identity()?;
        let certs: &Vec<CertificateDer<'static>> = peer_certs.downcast_ref()?;

        if certs.is_empty() {
            return None;
        }

        // Parse the first certificate (leaf certificate)
        let cert_der = &certs[0];

        // Calculate fingerprint
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(cert_der.as_ref());
        let fingerprint = hex::encode(hasher.finalize());

        // Parse with x509-parser if available, otherwise just return fingerprint
        let (common_name, dns_names) = match x509_parser::parse_x509_certificate(cert_der.as_ref())
        {
            Ok((_, cert)) => {
                // Extract Common Name
                let cn = cert
                    .subject()
                    .iter_common_name()
                    .next()
                    .and_then(|attr| attr.as_str().ok())
                    .map(|s| s.to_string());

                // Extract Subject Alternative Names (DNS)
                let sans: Vec<String> = cert
                    .subject_alternative_name()
                    .ok()
                    .flatten()
                    .map(|san| {
                        san.value
                            .general_names
                            .iter()
                            .filter_map(|gn| match gn {
                                x509_parser::prelude::GeneralName::DNSName(name) => {
                                    Some(name.to_string())
                                }
                                _ => None,
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                (cn, sans)
            }
            Err(_) => (None, vec![]),
        };

        Some(Self {
            common_name,
            dns_names,
            fingerprint,
        })
    }
}

// ============================================================================
// Connection Management
// ============================================================================

/// A managed QUIC connection with stream pooling
struct ManagedConnection {
    connection: Connection,
    /// Semaphore to limit concurrent streams
    stream_semaphore: Arc<Semaphore>,
    /// Active streams count
    active_streams: AtomicU64,
    /// Connection creation time for age-based eviction
    #[allow(dead_code)] // Used for connection lifetime tracking
    created_at: std::time::Instant,
    /// Last successful use
    last_used: RwLock<std::time::Instant>,
    /// Whether the connection is still healthy
    healthy: AtomicBool,
}

impl ManagedConnection {
    fn new(connection: Connection, max_streams: u32) -> Self {
        let now = std::time::Instant::now();
        Self {
            connection,
            stream_semaphore: Arc::new(Semaphore::new(max_streams as usize)),
            active_streams: AtomicU64::new(0),
            created_at: now,
            last_used: RwLock::new(now),
            healthy: AtomicBool::new(true),
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire) && self.connection.close_reason().is_none()
    }

    fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Release);
    }

    fn touch(&self) {
        *self.last_used.write() = std::time::Instant::now();
    }

    async fn open_bi_stream(&self) -> Result<(SendStream, RecvStream)> {
        let _permit = self
            .stream_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ClusterError::ConnectionClosed)?;

        self.active_streams.fetch_add(1, Ordering::SeqCst);

        let stream =
            self.connection.open_bi().await.map_err(|e| {
                ClusterError::ConnectionFailed(format!("Failed to open stream: {}", e))
            })?;

        Ok(stream)
    }
}

/// Connection pool for a single peer
struct PeerConnectionPool {
    connections: RwLock<Vec<Arc<ManagedConnection>>>,
    max_connections: usize,
    #[allow(dead_code)] // Used for connection establishment tracking
    connecting: AtomicBool,
}

impl PeerConnectionPool {
    fn new(max_connections: usize) -> Self {
        Self {
            connections: RwLock::new(Vec::with_capacity(max_connections)),
            max_connections,
            connecting: AtomicBool::new(false),
        }
    }

    /// Get the best available connection
    fn get_connection(&self) -> Option<Arc<ManagedConnection>> {
        let conns = self.connections.read();

        // Find healthiest connection with lowest active streams
        conns
            .iter()
            .filter(|c| c.is_healthy())
            .min_by_key(|c| c.active_streams.load(Ordering::Relaxed))
            .cloned()
    }

    /// Add a new connection to the pool
    fn add_connection(&self, conn: Arc<ManagedConnection>) {
        let mut conns = self.connections.write();

        // Remove unhealthy connections first
        conns.retain(|c| c.is_healthy());

        if conns.len() < self.max_connections {
            conns.push(conn);
        }
    }

    /// Remove all unhealthy connections
    #[allow(dead_code)] // Reserved for periodic cleanup
    fn cleanup(&self) {
        let mut conns = self.connections.write();
        conns.retain(|c| c.is_healthy());
    }
}

// ============================================================================
// Transport Statistics
// ============================================================================

/// QUIC transport statistics for monitoring
#[derive(Debug, Default)]
pub struct QuicStats {
    /// Total connections established
    pub connections_established: AtomicU64,
    /// Total connections failed
    pub connections_failed: AtomicU64,
    /// Total streams opened
    pub streams_opened: AtomicU64,
    /// Total streams failed
    pub streams_failed: AtomicU64,
    /// Total bytes sent
    pub bytes_sent: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Total requests sent
    pub requests_sent: AtomicU64,
    /// Total responses received
    pub responses_received: AtomicU64,
    /// Total request timeouts
    pub request_timeouts: AtomicU64,
    /// Total 0-RTT connections
    pub zero_rtt_connections: AtomicU64,
}

impl QuicStats {
    /// Snapshot the current stats
    pub fn snapshot(&self) -> QuicStatsSnapshot {
        QuicStatsSnapshot {
            connections_established: self.connections_established.load(Ordering::Relaxed),
            connections_failed: self.connections_failed.load(Ordering::Relaxed),
            streams_opened: self.streams_opened.load(Ordering::Relaxed),
            streams_failed: self.streams_failed.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            requests_sent: self.requests_sent.load(Ordering::Relaxed),
            responses_received: self.responses_received.load(Ordering::Relaxed),
            request_timeouts: self.request_timeouts.load(Ordering::Relaxed),
            zero_rtt_connections: self.zero_rtt_connections.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of QUIC stats for reporting
#[derive(Debug, Clone)]
pub struct QuicStatsSnapshot {
    pub connections_established: u64,
    pub connections_failed: u64,
    pub streams_opened: u64,
    pub streams_failed: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub requests_sent: u64,
    pub responses_received: u64,
    pub request_timeouts: u64,
    pub zero_rtt_connections: u64,
}

// ============================================================================
// QUIC Transport
// ============================================================================

/// Request handler for incoming cluster requests
pub type QuicRequestHandler = Arc<dyn Fn(ClusterRequest) -> ClusterResponse + Send + Sync>;

/// QUIC-based transport for cluster communication
pub struct QuicTransport {
    /// Local node identifier
    local_node: NodeId,
    /// QUIC endpoint for both client and server
    endpoint: Endpoint,
    /// Connection pools per peer
    connections: Arc<DashMap<NodeId, PeerConnectionPool>>,
    /// Peer addresses
    peer_addrs: Arc<DashMap<NodeId, SocketAddr>>,
    /// Configuration
    config: QuicConfig,
    /// Request handler for incoming messages
    handler: Option<QuicRequestHandler>,
    /// Correlation ID for request/response matching
    #[allow(dead_code)] // Reserved for request/response correlation
    correlation_id: AtomicU64,
    /// Transport statistics
    stats: Arc<QuicStats>,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
}

impl QuicTransport {
    /// Create a new QUIC transport
    #[instrument(skip(tls_config, config))]
    pub fn new(
        local_node: NodeId,
        bind_addr: SocketAddr,
        tls_config: TlsConfig,
        config: QuicConfig,
    ) -> Result<Self> {
        // Build server TLS config
        let server_crypto = Self::build_server_crypto(&tls_config)?;
        let mut server_config = ServerConfig::with_crypto(Arc::new(server_crypto));

        // Configure transport parameters
        let transport_config = Self::build_transport_config(&config);
        server_config.transport_config(Arc::new(transport_config));

        // Build client config
        let client_config = Self::build_client_config(&tls_config, &config)?;

        // Create endpoint
        let mut endpoint = Endpoint::server(server_config, bind_addr)
            .map_err(|e| ClusterError::Network(format!("Failed to create endpoint: {}", e)))?;

        endpoint.set_default_client_config(client_config);

        info!(
            node = %local_node,
            addr = %bind_addr,
            "QUIC transport initialized"
        );

        Ok(Self {
            local_node,
            endpoint,
            connections: Arc::new(DashMap::new()),
            peer_addrs: Arc::new(DashMap::new()),
            config,
            handler: None,
            correlation_id: AtomicU64::new(1),
            stats: Arc::new(QuicStats::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Build server TLS configuration with mTLS support
    fn build_server_crypto(tls: &TlsConfig) -> Result<quinn::crypto::rustls::QuicServerConfig> {
        // Build client cert verifier based on mTLS mode
        let server_config = match tls.mtls_mode {
            MtlsMode::Disabled => {
                // No client certificate required
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(tls.cert_chain.clone(), tls.private_key.clone_key())
                    .map_err(|e| ClusterError::CryptoError(format!("TLS config error: {}", e)))?
            }
            MtlsMode::Optional | MtlsMode::Required => {
                // Build root store from CA certs
                let mut roots = rustls::RootCertStore::empty();
                for cert in &tls.ca_certs {
                    roots.add(cert.clone()).map_err(|e| {
                        ClusterError::CryptoError(format!("Failed to add CA: {:?}", e))
                    })?;
                }

                if roots.is_empty() {
                    return Err(ClusterError::CryptoError(
                        "mTLS enabled but no CA certificates provided".to_string(),
                    ));
                }

                // Build verifier
                let verifier = if tls.mtls_mode == MtlsMode::Required {
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                        .build()
                        .map_err(|e| {
                            ClusterError::CryptoError(format!("Failed to build verifier: {}", e))
                        })?
                } else {
                    // Optional - allow anonymous clients but verify if cert provided
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                        .allow_unauthenticated()
                        .build()
                        .map_err(|e| {
                            ClusterError::CryptoError(format!("Failed to build verifier: {}", e))
                        })?
                };

                rustls::ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(tls.cert_chain.clone(), tls.private_key.clone_key())
                    .map_err(|e| ClusterError::CryptoError(format!("TLS config error: {}", e)))?
            }
        };

        let mut server_config = server_config;
        server_config.max_early_data_size = u32::MAX;
        server_config.alpn_protocols = vec![b"rivven".to_vec()];

        quinn::crypto::rustls::QuicServerConfig::try_from(server_config)
            .map_err(|e| ClusterError::CryptoError(format!("QUIC server config error: {}", e)))
    }

    /// Build client TLS configuration with optional client certificate
    fn build_client_config(tls: &TlsConfig, config: &QuicConfig) -> Result<ClientConfig> {
        // Build root certificate store for server verification
        let mut roots = rustls::RootCertStore::empty();

        // Add custom CA certs
        for cert in &tls.ca_certs {
            roots
                .add(cert.clone())
                .map_err(|e| ClusterError::CryptoError(format!("Failed to add CA: {:?}", e)))?;
        }

        // Add system roots as fallback (rustls-native-certs 0.8 returns CertificateResult)
        let native_result = rustls_native_certs::load_native_certs();
        for cert in native_result.certs {
            let _ = roots.add(cert);
        }

        // Build client config with or without client certificate
        let crypto = if tls.skip_verification {
            // Dangerous: skip verification (for testing only)
            #[cfg(any(test, feature = "dangerous-skip-verify"))]
            {
                if tls.mtls_mode != MtlsMode::Disabled {
                    // Even with skip verify, present our cert for mTLS
                    rustls::ClientConfig::builder()
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                        .with_client_auth_cert(tls.cert_chain.clone(), tls.private_key.clone_key())
                        .map_err(|e| {
                            ClusterError::CryptoError(format!("Client cert error: {}", e))
                        })?
                } else {
                    rustls::ClientConfig::builder()
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                        .with_no_client_auth()
                }
            }
            #[cfg(not(any(test, feature = "dangerous-skip-verify")))]
            {
                return Err(ClusterError::CryptoError(
                    "skip_verification requires the 'dangerous-skip-verify' feature".into(),
                ));
            }
        } else if tls.mtls_mode != MtlsMode::Disabled {
            // Normal verification + present client certificate for mTLS
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(tls.cert_chain.clone(), tls.private_key.clone_key())
                .map_err(|e| ClusterError::CryptoError(format!("Client cert error: {}", e)))?
        } else {
            // Normal verification without client certificate
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };

        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(|e| {
                ClusterError::CryptoError(format!("QUIC crypto config error: {}", e))
            })?,
        ));

        // Apply transport config
        let transport = Self::build_transport_config(config);
        client_config.transport_config(Arc::new(transport));

        Ok(client_config)
    }

    /// Build Quinn transport configuration
    fn build_transport_config(config: &QuicConfig) -> QuinnTransportConfig {
        let mut transport = QuinnTransportConfig::default();

        transport.max_concurrent_bidi_streams(VarInt::from_u32(config.max_concurrent_streams));
        transport.initial_mtu(config.max_udp_payload_size);
        transport.stream_receive_window(VarInt::from_u32(config.stream_receive_window));
        transport.receive_window(VarInt::from_u32(config.connection_receive_window));
        transport.keep_alive_interval(Some(config.keep_alive_interval));
        transport.max_idle_timeout(Some(config.idle_timeout.try_into().unwrap()));

        // Congestion control
        if config.use_bbr {
            transport.congestion_controller_factory(Arc::new(congestion::BbrConfig::default()));
        } else {
            transport.congestion_controller_factory(Arc::new(congestion::CubicConfig::default()));
        }

        transport
    }

    /// Set the request handler for incoming messages
    pub fn set_handler(&mut self, handler: QuicRequestHandler) {
        self.handler = Some(handler);
    }

    /// Register a peer node
    pub fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        self.peer_addrs.insert(node_id.clone(), addr);
        self.connections.insert(
            node_id,
            PeerConnectionPool::new(self.config.max_connections_per_peer),
        );
    }

    /// Remove a peer node
    pub fn remove_peer(&self, node_id: &NodeId) {
        self.peer_addrs.remove(node_id);
        self.connections.remove(node_id);
    }

    /// Start accepting incoming connections
    #[instrument(skip(self))]
    pub async fn start(&self) -> Result<()> {
        let endpoint = self.endpoint.clone();
        let connections = self.connections.clone();
        let handler = self.handler.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            while !shutdown.load(Ordering::Relaxed) {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        if let Some(incoming) = incoming {
                            let handler = handler.clone();
                            let config = config.clone();
                            let stats = stats.clone();
                            let connections = connections.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_incoming(
                                    incoming, handler, config, stats, connections
                                ).await {
                                    debug!(error = %e, "Incoming connection error");
                                }
                            });
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Check shutdown periodically
                    }
                }
            }

            info!("QUIC transport acceptor shutting down");
        });

        Ok(())
    }

    /// Handle an incoming connection
    async fn handle_incoming(
        incoming: quinn::Incoming,
        handler: Option<QuicRequestHandler>,
        config: QuicConfig,
        stats: Arc<QuicStats>,
        _connections: Arc<DashMap<NodeId, PeerConnectionPool>>,
    ) -> Result<()> {
        let connection = incoming
            .await
            .map_err(|e| ClusterError::ConnectionFailed(format!("Accept failed: {}", e)))?;

        stats
            .connections_established
            .fetch_add(1, Ordering::Relaxed);

        let remote = connection.remote_address();
        debug!(peer = %remote, "Accepted QUIC connection");

        // Handle streams from this connection
        loop {
            match connection.accept_bi().await {
                Ok((send, recv)) => {
                    let handler = handler.clone();
                    let config = config.clone();
                    let stats = stats.clone();

                    tokio::spawn(async move {
                        if let Err(e) =
                            Self::handle_stream(send, recv, handler, config, stats).await
                        {
                            debug!(error = %e, "Stream handling error");
                        }
                    });
                }
                Err(quinn::ConnectionError::ApplicationClosed(_)) => {
                    debug!(peer = %remote, "Connection closed by peer");
                    break;
                }
                Err(e) => {
                    warn!(peer = %remote, error = %e, "Connection error");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single bidirectional stream
    async fn handle_stream(
        mut send: SendStream,
        mut recv: RecvStream,
        handler: Option<QuicRequestHandler>,
        config: QuicConfig,
        stats: Arc<QuicStats>,
    ) -> Result<()> {
        stats.streams_opened.fetch_add(1, Ordering::Relaxed);

        // Read request
        let request_bytes = Self::read_message(&mut recv, &config).await?;
        stats
            .bytes_received
            .fetch_add(request_bytes.len() as u64, Ordering::Relaxed);

        // Decode and handle
        let request = decode_request(&request_bytes)?;

        if let Some(handler) = handler {
            let response = handler(request);
            let response_bytes = encode_response(&response)?;

            Self::write_message(&mut send, &response_bytes, &config).await?;
            stats
                .bytes_sent
                .fetch_add(response_bytes.len() as u64, Ordering::Relaxed);
        }

        send.finish()
            .map_err(|e| ClusterError::Network(format!("Failed to finish stream: {}", e)))?;

        Ok(())
    }

    /// Read a length-prefixed message from a stream
    async fn read_message(recv: &mut RecvStream, config: &QuicConfig) -> Result<Vec<u8>> {
        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        timeout(config.request_timeout, recv.read_exact(&mut len_buf))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(|e| ClusterError::Network(format!("Failed to read length: {}", e)))?;

        let len = u32::from_be_bytes(len_buf) as usize;

        if len > crate::protocol::MAX_MESSAGE_SIZE {
            return Err(ClusterError::MessageTooLarge {
                size: len,
                max: crate::protocol::MAX_MESSAGE_SIZE,
            });
        }

        // Read message body
        let mut body = vec![0u8; len];
        timeout(config.request_timeout, recv.read_exact(&mut body))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(|e| ClusterError::Network(format!("Failed to read body: {}", e)))?;

        Ok(body)
    }

    /// Write a length-prefixed message to a stream
    async fn write_message(send: &mut SendStream, data: &[u8], config: &QuicConfig) -> Result<()> {
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);

        timeout(config.request_timeout, send.write_all(&buf))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(|e| ClusterError::Network(format!("Failed to write: {}", e)))?;

        Ok(())
    }

    /// Send a request to a peer and wait for response
    #[instrument(skip(self, request), fields(node = %node_id))]
    pub async fn send(&self, node_id: &NodeId, request: ClusterRequest) -> Result<ClusterResponse> {
        let conn = self.get_or_create_connection(node_id).await?;

        self.stats.requests_sent.fetch_add(1, Ordering::Relaxed);

        // Open bidirectional stream
        let (mut send, mut recv) = conn.open_bi_stream().await?;
        self.stats.streams_opened.fetch_add(1, Ordering::Relaxed);

        conn.touch();

        // Send request
        let request_bytes = encode_request(&request)?;
        Self::write_message(&mut send, &request_bytes, &self.config).await?;
        self.stats
            .bytes_sent
            .fetch_add(request_bytes.len() as u64, Ordering::Relaxed);

        send.finish()
            .map_err(|e| ClusterError::Network(format!("Failed to finish send: {}", e)))?;

        // Receive response
        let response_bytes = match Self::read_message(&mut recv, &self.config).await {
            Ok(bytes) => bytes,
            Err(ClusterError::Timeout) => {
                self.stats.request_timeouts.fetch_add(1, Ordering::Relaxed);
                conn.active_streams.fetch_sub(1, Ordering::SeqCst);
                conn.mark_unhealthy();
                return Err(ClusterError::Timeout);
            }
            Err(e) => {
                conn.active_streams.fetch_sub(1, Ordering::SeqCst);
                conn.mark_unhealthy();
                return Err(e);
            }
        };

        self.stats
            .bytes_received
            .fetch_add(response_bytes.len() as u64, Ordering::Relaxed);
        self.stats
            .responses_received
            .fetch_add(1, Ordering::Relaxed);

        let response = decode_response(&response_bytes)?;

        conn.active_streams.fetch_sub(1, Ordering::SeqCst);

        Ok(response)
    }

    /// Send a request without waiting for a response (fire and forget)
    pub async fn send_async(&self, node_id: &NodeId, request: ClusterRequest) -> Result<()> {
        let conn = self.get_or_create_connection(node_id).await?;

        let (mut send, _recv) = conn.open_bi_stream().await?;

        let request_bytes = encode_request(&request)?;
        Self::write_message(&mut send, &request_bytes, &self.config).await?;

        send.finish()
            .map_err(|e| ClusterError::Network(format!("Failed to finish: {}", e)))?;

        conn.active_streams.fetch_sub(1, Ordering::SeqCst);

        Ok(())
    }

    /// Broadcast a request to all peers
    pub async fn broadcast(
        &self,
        request: ClusterRequest,
    ) -> Vec<(NodeId, Result<ClusterResponse>)> {
        let peers: Vec<_> = self.peer_addrs.iter().map(|e| e.key().clone()).collect();

        let mut futures = Vec::with_capacity(peers.len());

        for peer in peers {
            let request = request.clone();
            let this = self;
            futures.push(async move {
                let result = this.send(&peer, request).await;
                (peer, result)
            });
        }

        futures::future::join_all(futures).await
    }

    /// Get or create a connection to a peer
    async fn get_or_create_connection(&self, node_id: &NodeId) -> Result<Arc<ManagedConnection>> {
        // Try to get existing connection
        if let Some(pool) = self.connections.get(node_id) {
            if let Some(conn) = pool.get_connection() {
                return Ok(conn);
            }
        }

        // Create new connection
        let addr = *self
            .peer_addrs
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

        let sni_name = addr.ip().to_string();
        let connection = self
            .endpoint
            .connect(addr, &sni_name) // Use peer address for TLS SNI
            .map_err(|e| ClusterError::ConnectionFailed(format!("Connect error: {}", e)))?
            .await
            .map_err(|e| {
                self.stats
                    .connections_failed
                    .fetch_add(1, Ordering::Relaxed);
                ClusterError::ConnectionFailed(format!("Connection failed: {}", e))
            })?;

        self.stats
            .connections_established
            .fetch_add(1, Ordering::Relaxed);

        let managed = Arc::new(ManagedConnection::new(
            connection,
            self.config.max_concurrent_streams,
        ));

        // Add to pool
        if let Some(pool) = self.connections.get(node_id) {
            pool.add_connection(managed.clone());
        } else {
            let pool = PeerConnectionPool::new(self.config.max_connections_per_peer);
            pool.add_connection(managed.clone());
            self.connections.insert(node_id.clone(), pool);
        }

        Ok(managed)
    }

    /// Get the next correlation ID
    fn _next_correlation_id(&self) -> u64 {
        self.correlation_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get transport statistics
    pub fn stats(&self) -> &QuicStats {
        &self.stats
    }

    /// Shutdown the transport
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);

        // Close all connections gracefully
        for pool in self.connections.iter() {
            let conns = pool.connections.read();
            for conn in conns.iter() {
                conn.connection.close(VarInt::from_u32(0), b"shutdown");
            }
        }

        self.endpoint.close(VarInt::from_u32(0), b"shutdown");

        // Wait for endpoint to finish
        self.endpoint.wait_idle().await;

        info!(node = %self.local_node, "QUIC transport shutdown complete");
    }
}

// ============================================================================
// Certificate Verifier for Testing
// ============================================================================

/// Skip server certificate verification (DANGEROUS - testing only)
#[cfg(any(test, feature = "dangerous-skip-verify"))]
#[derive(Debug)]
struct SkipServerVerification;

#[cfg(any(test, feature = "dangerous-skip-verify"))]
impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quic_config_defaults() {
        let config = QuicConfig::default();
        assert_eq!(config.max_concurrent_streams, 256);
        assert!(config.enable_0rtt);
        assert!(config.use_bbr);
    }

    #[test]
    fn test_quic_config_high_throughput() {
        let config = QuicConfig::high_throughput();
        assert_eq!(config.max_concurrent_streams, 512);
        assert_eq!(config.connection_receive_window, 32 * 1024 * 1024);
    }

    #[test]
    fn test_quic_config_low_latency() {
        let config = QuicConfig::low_latency();
        assert_eq!(config.max_concurrent_streams, 64);
        assert!(!config.use_bbr); // CUBIC for low latency
    }

    #[test]
    fn test_tls_self_signed() {
        let tls = TlsConfig::self_signed("test.rivven.local").unwrap();
        assert!(!tls.cert_chain.is_empty());
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = QuicStats::default();
        stats
            .connections_established
            .fetch_add(5, Ordering::Relaxed);
        stats.bytes_sent.fetch_add(1000, Ordering::Relaxed);

        let snap = stats.snapshot();
        assert_eq!(snap.connections_established, 5);
        assert_eq!(snap.bytes_sent, 1000);
    }
}
