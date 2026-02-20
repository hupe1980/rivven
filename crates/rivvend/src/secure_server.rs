//! Secure Server with TLS/mTLS Support
//!
//! This module provides a security-hardened server that:
//! - Supports TLS encryption for all connections
//! - Optionally enforces mTLS for service-to-service authentication  
//! - Integrates certificate-based identity with service auth
//! - Provides connection-level security context
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    SecureServer                                  │
//! │  ┌─────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
//! │  │ TlsAcceptor │─►│ SecureConnection │─►│ AuthenticatedHndl│   │
//! │  │  (mTLS opt) │  │ (TLS + Identity) │  │  (RBAC checks)   │   │
//! │  └─────────────┘  └──────────────────┘  └──────────────────┘   │
//! │                                                                  │
//! │  Security Flow:                                                  │
//! │  1. TCP Accept                                                   │
//! │  2. TLS Handshake (extract client cert if mTLS)                 │
//! │  3. Map cert subject → ServiceAccount (service_auth.rs)         │
//! │  4. Create authenticated session                                 │
//! │  5. Authorize requests via Cedar/RBAC                           │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use rivven_core::{
    AuthManager, Config, OffsetManager, ServiceAuthConfig, ServiceAuthManager, TopicManager,
};

#[cfg(feature = "tls")]
use rivven_core::{
    tls::{MtlsMode, TlsAcceptor, TlsConfig, TlsIdentity, TlsServerStream},
    AuthSession,
};

use crate::auth_handler::{AuthenticatedHandler, ConnectionAuth};
use crate::handler::RequestHandler;

// ============================================================================
// Configuration
// ============================================================================

/// Secure server configuration
#[derive(Debug, Clone)]
pub struct SecureServerConfig {
    /// Bind address
    pub bind_addr: SocketAddr,

    /// TLS configuration (None = plaintext)
    #[cfg(feature = "tls")]
    pub tls_config: Option<TlsConfig>,

    /// Maximum concurrent connections
    pub max_connections: usize,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Idle timeout (close connection after inactivity)
    pub idle_timeout: Duration,

    /// Maximum message size (bytes)
    pub max_message_size: usize,

    /// Require authentication
    pub require_auth: bool,

    /// Enable service-to-service auth (mTLS → auto-auth)
    pub enable_service_auth: bool,

    /// Service auth configuration
    pub service_auth_config: Option<ServiceAuthConfig>,
}

impl Default for SecureServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: ([0, 0, 0, 0], 9092).into(),
            #[cfg(feature = "tls")]
            tls_config: None,
            max_connections: 10_000,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            max_message_size: 10 * 1024 * 1024, // 10 MB
            require_auth: false,
            enable_service_auth: false,
            service_auth_config: None,
        }
    }
}

// ============================================================================
// Connection Security Context
// ============================================================================

/// Security context for a connection
#[derive(Debug, Clone)]
pub struct ConnectionSecurityContext {
    /// Client IP address
    pub client_addr: SocketAddr,

    /// TLS information (if TLS enabled)
    #[cfg(feature = "tls")]
    pub tls_info: Option<TlsConnectionInfo>,

    /// Authentication state
    pub auth_state: ConnectionAuth,

    /// Service identity (if mTLS authenticated)
    #[cfg(feature = "tls")]
    pub service_identity: Option<ServiceIdentity>,
}

/// TLS-specific connection information
#[cfg(feature = "tls")]
#[derive(Debug, Clone)]
pub struct TlsConnectionInfo {
    /// TLS protocol version (e.g., "TLSv1.3")
    pub protocol_version: String,

    /// Cipher suite
    pub cipher_suite: Option<String>,

    /// Client certificate identity (if mTLS)
    pub client_cert: Option<TlsIdentity>,

    /// ALPN protocol
    pub alpn_protocol: Option<String>,
}

/// Service identity from mTLS certificate
#[derive(Debug, Clone)]
pub struct ServiceIdentity {
    /// Service account ID
    pub service_id: String,

    /// Certificate common name
    pub common_name: String,

    /// Certificate subject
    pub subject: String,

    /// Certificate fingerprint
    pub fingerprint: String,

    /// Roles/permissions from service account
    pub roles: Vec<String>,
}

// ============================================================================
// Secure Server
// ============================================================================

/// Production-grade secure server
pub struct SecureServer {
    config: SecureServerConfig,
    topic_manager: TopicManager,
    offset_manager: OffsetManager,
    auth_manager: Arc<AuthManager>,
    /// Service auth for mTLS certificate-based authentication (enabled via config)
    #[allow(dead_code)]
    service_auth_manager: Option<Arc<ServiceAuthManager>>,

    #[cfg(feature = "tls")]
    tls_acceptor: Option<TlsAcceptor>,

    /// Connection limiter
    connection_semaphore: Arc<Semaphore>,
    /// §3.3: Write-ahead log for crash-safe durability on the produce path.
    wal: Option<Arc<rivven_core::GroupCommitWal>>,
}

impl SecureServer {
    /// Create a new secure server
    pub async fn new(
        core_config: Config,
        server_config: SecureServerConfig,
    ) -> anyhow::Result<Self> {
        Self::with_auth_manager(core_config, server_config, None).await
    }

    /// Create a secure server with a custom AuthManager
    ///
    /// This allows injecting a pre-configured AuthManager with users and roles,
    /// useful for testing RBAC without needing to create users via the protocol.
    pub async fn with_auth_manager(
        core_config: Config,
        server_config: SecureServerConfig,
        auth_manager: Option<Arc<AuthManager>>,
    ) -> anyhow::Result<Self> {
        let topic_manager = TopicManager::new(core_config.clone());

        // Recover persisted topics from disk
        if let Err(e) = topic_manager.recover().await {
            tracing::warn!("Failed to recover topics from disk: {}", e);
        }

        // §3.3 Replay WAL records at startup, identical to ClusterServer.
        // Without this, writes committed to WAL but not flushed to segment
        // files are silently lost when using SecureServer.
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
                            "WAL replay: {} records recovered at startup (SecureServer)",
                            records.len()
                        );
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

        let offset_manager = OffsetManager::with_persistence(
            std::path::PathBuf::from(&core_config.data_dir).join("offsets"),
        )?;

        // Use provided AuthManager or create a new one with default config
        let auth_manager =
            auth_manager.unwrap_or_else(|| Arc::new(AuthManager::new(Default::default())));

        // Initialize service auth if configured
        // Pass the stored ServiceAuthConfig to the manager instead
        // of ignoring it. Previously used ServiceAuthManager::new() (hardcoded
        // defaults), discarding session_duration, rate limits, and service accounts.
        let service_auth_manager = if server_config.enable_service_auth {
            if let Some(ref config) = server_config.service_auth_config {
                Some(Arc::new(ServiceAuthManager::with_config(config)))
            } else {
                Some(Arc::new(ServiceAuthManager::new()))
            }
        } else {
            None
        };

        // Initialize TLS acceptor if configured
        #[cfg(feature = "tls")]
        let tls_acceptor = if let Some(ref tls_config) = server_config.tls_config {
            if tls_config.enabled {
                Some(TlsAcceptor::new(tls_config)?)
            } else {
                None
            }
        } else {
            None
        };

        let connection_semaphore = Arc::new(Semaphore::new(server_config.max_connections));

        // §3.3 Instantiate WAL for produce-path durability.
        // WAL init failure is a hard error — running without WAL risks data loss.
        let wal = {
            let wal_dir = std::path::PathBuf::from(&core_config.data_dir).join("wal");
            let wal_config = rivven_core::WalConfig {
                dir: wal_dir,
                ..Default::default()
            };
            let w = rivven_core::GroupCommitWal::new(wal_config)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to initialise WAL: {}", e))?;
            Some(w)
        };

        Ok(Self {
            config: server_config,
            topic_manager,
            offset_manager,
            auth_manager,
            service_auth_manager,
            #[cfg(feature = "tls")]
            tls_acceptor,
            connection_semaphore,
            wal,
        })
    }

    /// Start the secure server
    pub async fn start(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr).await?;

        #[cfg(feature = "tls")]
        let mode = if self.tls_acceptor.is_some() {
            if let Some(ref cfg) = self.config.tls_config {
                match cfg.mtls_mode {
                    MtlsMode::Required => "mTLS (client cert required)",
                    MtlsMode::Optional => "TLS (client cert optional)",
                    MtlsMode::Disabled => "TLS",
                }
            } else {
                "plaintext"
            }
        } else {
            "plaintext"
        };

        #[cfg(not(feature = "tls"))]
        let mode = "plaintext";

        info!(
            "Secure server listening on {} (mode: {}, auth: {})",
            self.config.bind_addr,
            mode,
            if self.config.require_auth {
                "required"
            } else {
                "optional"
            }
        );

        // Create handler for the AuthenticatedHandler
        let mut auth_handler_inner =
            RequestHandler::new(self.topic_manager.clone(), self.offset_manager.clone());

        // §3.3: Wire WAL into the request handler
        if let Some(ref wal) = self.wal {
            auth_handler_inner.set_wal(wal.clone());
        }

        let auth_handler = Arc::new(AuthenticatedHandler::new(
            auth_handler_inner,
            self.auth_manager.clone(),
            self.config.require_auth,
        ));

        // Server state for spawned tasks
        let server = Arc::new(self);

        // Spawn background TLS certificate reload task if configured
        #[cfg(feature = "tls")]
        if let Some(ref tls_acceptor) = server.tls_acceptor {
            let reload_interval = tls_acceptor.cert_reload_interval();
            if reload_interval > Duration::ZERO {
                let server_ref = server.clone();
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(reload_interval);
                    ticker.tick().await; // skip first immediate tick
                    loop {
                        ticker.tick().await;
                        if let Some(ref acceptor) = server_ref.tls_acceptor {
                            match acceptor.reload() {
                                Ok(()) => {
                                    info!("TLS certificates reloaded successfully");
                                }
                                Err(e) => {
                                    warn!(
                                        "TLS certificate reload failed (keeping existing): {}",
                                        e
                                    );
                                }
                            }
                        }
                    }
                });
            }
        }

        loop {
            // Acquire connection permit
            let permit = match server.connection_semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!(
                        "Max connections reached ({}), rejecting",
                        server.config.max_connections
                    );
                    // Accept and immediately close to avoid kernel backlog
                    if let Ok((stream, _)) = listener.accept().await {
                        drop(stream);
                    }
                    continue;
                }
            };

            match listener.accept().await {
                Ok((tcp_stream, client_addr)) => {
                    let server = server.clone();
                    let auth_handler = auth_handler.clone();

                    tokio::spawn(async move {
                        // Permit is dropped when task completes
                        let _permit = permit;

                        if let Err(e) = server
                            .handle_connection(tcp_stream, client_addr, auth_handler)
                            .await
                        {
                            debug!("Connection error from {}: {}", client_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Handle a single connection
    async fn handle_connection(
        &self,
        tcp_stream: TcpStream,
        client_addr: SocketAddr,
        auth_handler: Arc<AuthenticatedHandler>,
    ) -> anyhow::Result<()> {
        // Set TCP options
        tcp_stream.set_nodelay(true)?;

        // Apply connection timeout for TLS handshake
        let _timeout = self.config.connection_timeout;

        #[cfg(feature = "tls")]
        if let Some(ref tls_acceptor) = self.tls_acceptor {
            // TLS connection
            let tls_stream =
                match tokio::time::timeout(_timeout, tls_acceptor.accept_tcp(tcp_stream)).await {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(e)) => {
                        warn!("TLS handshake failed from {}: {}", client_addr, e);
                        return Ok(());
                    }
                    Err(_) => {
                        warn!("TLS handshake timeout from {}", client_addr);
                        return Ok(());
                    }
                };

            // Extract security context from TLS
            let security_ctx = self
                .build_tls_security_context(client_addr, &tls_stream)
                .await?;

            // Handle the secure connection
            return self
                .handle_secure_connection(tls_stream, security_ctx, auth_handler)
                .await;
        }

        // Plaintext connection (TLS not configured or TLS feature disabled)
        let security_ctx = ConnectionSecurityContext {
            client_addr,
            #[cfg(feature = "tls")]
            tls_info: None,
            auth_state: if self.config.require_auth {
                ConnectionAuth::Unauthenticated
            } else {
                ConnectionAuth::Anonymous
            },
            #[cfg(feature = "tls")]
            service_identity: None,
        };

        self.handle_secure_connection(tcp_stream, security_ctx, auth_handler)
            .await
    }

    /// Build security context from TLS connection
    #[cfg(feature = "tls")]
    async fn build_tls_security_context(
        &self,
        client_addr: SocketAddr,
        tls_stream: &TlsServerStream<TcpStream>,
    ) -> anyhow::Result<ConnectionSecurityContext> {
        // Extract TLS info
        let protocol_version = tls_stream
            .protocol_version()
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| "unknown".to_string());

        let alpn = tls_stream
            .alpn_protocol()
            .map(|p| String::from_utf8_lossy(p).to_string());

        // Extract client certificate if present
        let client_cert = tls_stream.peer_certificates().and_then(|certs| {
            if certs.is_empty() {
                None
            } else {
                Some(TlsIdentity::from_certificate(&certs[0]))
            }
        });

        // Build TLS info
        let tls_info = TlsConnectionInfo {
            protocol_version,
            cipher_suite: tls_stream.cipher_suite_name(),
            client_cert: client_cert.clone(),
            alpn_protocol: alpn,
        };

        // Determine auth state based on client certificate
        let (auth_state, service_identity) = if let Some(ref cert_identity) = client_cert {
            // mTLS: Try to authenticate via service auth
            if let Some(ref svc_auth) = self.service_auth_manager {
                let cert_subject = cert_identity
                    .subject
                    .clone()
                    .unwrap_or_else(|| cert_identity.common_name.clone().unwrap_or_default());

                if !cert_subject.is_empty() {
                    let client_ip_str = client_addr.ip().to_string();

                    match svc_auth.authenticate_certificate(&cert_subject, &client_ip_str) {
                        Ok(session) => {
                            info!(
                                "mTLS authenticated service '{}' from {} (cert: {})",
                                session.service_account,
                                client_addr,
                                cert_identity.common_name.as_deref().unwrap_or("?")
                            );

                            let svc_identity = ServiceIdentity {
                                service_id: session.service_account.clone(),
                                common_name: cert_identity.common_name.clone().unwrap_or_default(),
                                subject: cert_subject,
                                fingerprint: cert_identity.fingerprint.clone(),
                                roles: session.permissions.clone(),
                            };

                            // Create a corresponding AuthSession for RBAC
                            // Use the auth_manager to create a proper session
                            let auth_session = AuthSession {
                                id: session.id.clone(),
                                principal_name: session.service_account.clone(),
                                principal_type: rivven_core::PrincipalType::ServiceAccount,
                                permissions: std::collections::HashSet::new(), // Will be populated from roles
                                created_at: std::time::Instant::now(),
                                expires_at: std::time::Instant::now()
                                    + session.time_until_expiration(),
                                client_ip: client_addr.ip().to_string(),
                            };

                            (
                                ConnectionAuth::Authenticated(auth_session),
                                Some(svc_identity),
                            )
                        }
                        Err(e) => {
                            warn!(
                                "mTLS auth failed for cert '{}' from {}: {}",
                                cert_subject, client_addr, e
                            );
                            (ConnectionAuth::Unauthenticated, None)
                        }
                    }
                } else {
                    warn!("Client cert has no subject from {}", client_addr);
                    (ConnectionAuth::Unauthenticated, None)
                }
            } else {
                // No service auth configured, but client provided cert
                debug!(
                    "Client cert provided but service auth not enabled from {}",
                    client_addr
                );
                (
                    if self.config.require_auth {
                        ConnectionAuth::Unauthenticated
                    } else {
                        ConnectionAuth::Anonymous
                    },
                    None,
                )
            }
        } else {
            // No client certificate
            (
                if self.config.require_auth {
                    ConnectionAuth::Unauthenticated
                } else {
                    ConnectionAuth::Anonymous
                },
                None,
            )
        };

        Ok(ConnectionSecurityContext {
            client_addr,
            tls_info: Some(tls_info),
            auth_state,
            service_identity,
        })
    }

    /// Handle a connection with security context
    async fn handle_secure_connection<S>(
        &self,
        mut stream: S,
        mut security_ctx: ConnectionSecurityContext,
        auth_handler: Arc<AuthenticatedHandler>,
    ) -> anyhow::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut buffer = BytesMut::with_capacity(8192);
        let client_addr = security_ctx.client_addr;
        let client_ip = client_addr.ip().to_string();

        #[cfg(feature = "tls")]
        let has_tls = security_ctx.tls_info.is_some();
        #[cfg(not(feature = "tls"))]
        let has_tls = false;

        debug!(
            "Connection established: addr={}, tls={}, auth={:?}",
            client_addr,
            has_tls,
            std::mem::discriminant(&security_ctx.auth_state)
        );

        loop {
            // Use shared framing to read next request
            let frame = match crate::framing::read_framed_request(
                &mut stream,
                &mut buffer,
                self.config.max_message_size,
                self.config.idle_timeout,
                &client_addr.to_string(),
            )
            .await?
            {
                Some(crate::framing::ReadFrame::Request {
                    request,
                    wire_format,
                    correlation_id,
                }) => (request, wire_format, correlation_id),
                Some(crate::framing::ReadFrame::Disconnected) => {
                    debug!("Client {} disconnected gracefully", client_addr);
                    return Ok(());
                }
                Some(crate::framing::ReadFrame::Timeout) => {
                    return Ok(());
                }
                None => continue, // size-exceeded or parse error — already sent error response
            };

            let (request, wire_format, correlation_id) = frame;

            // Handle request with auth
            let response = auth_handler
                .handle(request, &mut security_ctx.auth_state, &client_ip, has_tls)
                .await;

            // Send response in the same format the client used
            crate::framing::send_response(&mut stream, &response, wire_format, correlation_id)
                .await?;
        }
    }
}

// ============================================================================
// Server Builder
// ============================================================================

/// Builder for SecureServer configuration
pub struct SecureServerBuilder {
    core_config: Config,
    server_config: SecureServerConfig,
}

impl SecureServerBuilder {
    /// Create a new builder with default configuration
    pub fn new(core_config: Config) -> Self {
        Self {
            core_config,
            server_config: SecureServerConfig::default(),
        }
    }

    /// Set bind address
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.server_config.bind_addr = addr;
        self
    }

    /// Enable TLS
    #[cfg(feature = "tls")]
    pub fn with_tls(mut self, tls_config: TlsConfig) -> Self {
        self.server_config.tls_config = Some(tls_config);
        self
    }

    /// Set maximum connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.server_config.max_connections = max;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.server_config.connection_timeout = timeout;
        self
    }

    /// Set idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.server_config.idle_timeout = timeout;
        self
    }

    /// Set maximum message size
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.server_config.max_message_size = size;
        self
    }

    /// Require authentication
    pub fn require_auth(mut self, require: bool) -> Self {
        self.server_config.require_auth = require;
        self
    }

    /// Enable service-to-service authentication
    pub fn enable_service_auth(mut self, config: ServiceAuthConfig) -> Self {
        self.server_config.enable_service_auth = true;
        self.server_config.service_auth_config = Some(config);
        self
    }

    /// Build and start the server
    pub async fn build(self) -> anyhow::Result<SecureServer> {
        SecureServer::new(self.core_config, self.server_config).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SecureServerConfig::default();
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.max_message_size, 10 * 1024 * 1024);
        assert!(!config.require_auth);
    }

    #[test]
    fn test_builder() {
        let core_config = Config::default();
        let builder = SecureServerBuilder::new(core_config)
            .bind("127.0.0.1:9999".parse().unwrap())
            .max_connections(5000)
            .require_auth(true);

        assert_eq!(
            builder.server_config.bind_addr,
            "127.0.0.1:9999".parse().unwrap()
        );
        assert_eq!(builder.server_config.max_connections, 5000);
        assert!(builder.server_config.require_auth);
    }
}
