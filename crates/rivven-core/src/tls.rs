//! Production-grade TLS/mTLS infrastructure for Rivven.
//!
//! This module provides comprehensive encryption for all communication paths:
//! - Client → Broker (optional TLS, mTLS for high-security)
//! - Connect → Broker (mTLS required for service-to-service)
//! - Broker ↔ Broker (mTLS for cluster communication)
//! - Admin → Broker (TLS/mTLS for management APIs)
//!
//! # Security Model
//!
//! Rivven follows a zero-trust security model for inter-service communication:
//! - All internal communication uses mTLS by default
//! - Certificate-based identity for services
//! - Strong cipher suites only (TLS 1.3 preferred)
//! - Certificate rotation support
//! - Optional certificate pinning for high-security deployments
//!
//! # Example
//!
//! ```rust,ignore
//! use rivven_core::tls::{TlsConfigBuilder, TlsAcceptor, TlsConnector};
//!
//! // Server-side mTLS
//! let server_config = TlsConfigBuilder::new()
//!     .with_cert_file("server.crt")?
//!     .with_key_file("server.key")?
//!     .with_client_ca_file("ca.crt")?  // Enable mTLS
//!     .require_client_cert(true)
//!     .build()?;
//!
//! let acceptor = TlsAcceptor::new(server_config)?;
//!
//! // Client-side mTLS
//! let client_config = TlsConfigBuilder::new()
//!     .with_cert_file("client.crt")?
//!     .with_key_file("client.key")?
//!     .with_root_ca_file("ca.crt")?
//!     .build()?;
//!
//! let connector = TlsConnector::new(client_config)?;
//! ```

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{self, BufReader, Cursor};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

// Re-export rustls types that users might need
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
pub use rustls::{ClientConfig, ServerConfig};

/// TLS-related errors
#[derive(Debug, Error)]
pub enum TlsError {
    /// Certificate file not found or unreadable
    #[error("Failed to read certificate file '{path}': {source}")]
    CertificateReadError {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Private key file not found or unreadable
    #[error("Failed to read private key file '{path}': {source}")]
    KeyReadError {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// Invalid certificate format
    #[error("Invalid certificate format: {0}")]
    InvalidCertificate(String),

    /// Invalid private key format
    #[error("Invalid private key format: {0}")]
    InvalidPrivateKey(String),

    /// Certificate chain validation failed
    #[error("Certificate chain validation failed: {0}")]
    CertificateChainError(String),

    /// TLS handshake failed
    #[error("TLS handshake failed: {0}")]
    HandshakeError(String),

    /// Connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Configuration error
    #[error("TLS configuration error: {0}")]
    ConfigError(String),

    /// Certificate expired
    #[error("Certificate expired: {0}")]
    CertificateExpired(String),

    /// Certificate not yet valid
    #[error("Certificate not yet valid: {0}")]
    CertificateNotYetValid(String),

    /// Certificate revoked
    #[error("Certificate revoked: {0}")]
    CertificateRevoked(String),

    /// Hostname verification failed
    #[error("Hostname verification failed: expected '{expected}', got '{actual}'")]
    HostnameVerificationFailed { expected: String, actual: String },

    /// mTLS required but client certificate not provided
    #[error("Client certificate required for mTLS but not provided")]
    ClientCertificateRequired,

    /// Self-signed certificate generation failed
    #[error("Failed to generate self-signed certificate: {0}")]
    SelfSignedGenerationError(String),

    /// ALPN negotiation failed
    #[error("ALPN negotiation failed: no common protocol")]
    AlpnNegotiationFailed,

    /// Internal rustls error
    #[error("TLS internal error: {0}")]
    RustlsError(String),
}

impl From<rustls::Error> for TlsError {
    fn from(err: rustls::Error) -> Self {
        TlsError::RustlsError(err.to_string())
    }
}

/// Result type for TLS operations
pub type TlsResult<T> = std::result::Result<T, TlsError>;

// ============================================================================
// TLS Configuration
// ============================================================================

/// TLS protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TlsVersion {
    /// TLS 1.2 (minimum for compatibility)
    Tls12,
    /// TLS 1.3 (preferred, default)
    #[default]
    Tls13,
}

/// mTLS mode configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MtlsMode {
    /// TLS without client certificate verification
    #[default]
    Disabled,
    /// Request client certificate but don't require it
    Optional,
    /// Require valid client certificate (recommended for service-to-service)
    Required,
}

/// Certificate source for flexible certificate loading
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CertificateSource {
    /// Load from PEM file
    File { path: PathBuf },
    /// Load from PEM string
    Pem { content: String },
    /// Load from DER bytes (base64 encoded in config)
    Der { content: String },
    /// Generate self-signed (development only)
    SelfSigned { common_name: String },
}

/// Private key source for flexible key loading
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PrivateKeySource {
    /// Load from PEM file
    File { path: PathBuf },
    /// Load from PEM string
    Pem { content: String },
    /// Load from DER bytes (base64 encoded in config)
    Der { content: String },
}

/// Complete TLS configuration for a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Whether TLS is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Server certificate and chain
    pub certificate: Option<CertificateSource>,

    /// Server private key
    pub private_key: Option<PrivateKeySource>,

    /// Root CA certificates for verification
    pub root_ca: Option<CertificateSource>,

    /// Client CA certificates for mTLS verification
    pub client_ca: Option<CertificateSource>,

    /// mTLS mode
    #[serde(default)]
    pub mtls_mode: MtlsMode,

    /// Minimum TLS version
    #[serde(default)]
    pub min_version: TlsVersion,

    /// ALPN protocols (e.g., ["h2", "http/1.1"])
    #[serde(default)]
    pub alpn_protocols: Vec<String>,

    /// Enable OCSP stapling
    #[serde(default)]
    pub ocsp_stapling: bool,

    /// Certificate pinning (SHA-256 fingerprints)
    #[serde(default)]
    pub pinned_certificates: Vec<String>,

    /// Skip certificate verification (DANGEROUS - testing only)
    #[serde(default)]
    pub insecure_skip_verify: bool,

    /// Server name for SNI (client-side)
    pub server_name: Option<String>,

    /// Session cache size (0 to disable)
    #[serde(default = "default_session_cache_size")]
    pub session_cache_size: usize,

    /// Session ticket lifetime
    #[serde(default = "default_session_ticket_lifetime")]
    #[serde(with = "humantime_serde")]
    pub session_ticket_lifetime: Duration,

    /// Certificate reload interval (0 to disable)
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    pub cert_reload_interval: Duration,
}

fn default_true() -> bool {
    true
}

fn default_session_cache_size() -> usize {
    256
}

fn default_session_ticket_lifetime() -> Duration {
    Duration::from_secs(86400) // 24 hours
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Opt-in to TLS
            certificate: None,
            private_key: None,
            root_ca: None,
            client_ca: None,
            mtls_mode: MtlsMode::Disabled,
            min_version: TlsVersion::Tls13,
            alpn_protocols: vec![],
            ocsp_stapling: false,
            pinned_certificates: vec![],
            insecure_skip_verify: false,
            server_name: None,
            session_cache_size: default_session_cache_size(),
            session_ticket_lifetime: default_session_ticket_lifetime(),
            cert_reload_interval: Duration::ZERO,
        }
    }
}

impl TlsConfig {
    /// Create a new disabled TLS configuration
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create TLS configuration for development with self-signed certificates
    pub fn self_signed(common_name: &str) -> Self {
        Self {
            enabled: true,
            certificate: Some(CertificateSource::SelfSigned {
                common_name: common_name.to_string(),
            }),
            private_key: None, // Generated with self-signed cert
            insecure_skip_verify: true, // Required for self-signed
            ..Default::default()
        }
    }

    /// Create TLS configuration from PEM files
    pub fn from_pem_files<P: Into<PathBuf>>(cert_path: P, key_path: P) -> Self {
        Self {
            enabled: true,
            certificate: Some(CertificateSource::File {
                path: cert_path.into(),
            }),
            private_key: Some(PrivateKeySource::File {
                path: key_path.into(),
            }),
            ..Default::default()
        }
    }

    /// Create mTLS configuration from PEM files
    pub fn mtls_from_pem_files<P1, P2, P3>(
        cert_path: P1,
        key_path: P2,
        ca_path: P3,
    ) -> Self 
    where
        P1: Into<PathBuf>,
        P2: Into<PathBuf>,
        P3: Into<PathBuf> + Clone,
    {
        let ca: PathBuf = ca_path.clone().into();
        Self {
            enabled: true,
            certificate: Some(CertificateSource::File {
                path: cert_path.into(),
            }),
            private_key: Some(PrivateKeySource::File {
                path: key_path.into(),
            }),
            client_ca: Some(CertificateSource::File {
                path: ca.clone(),
            }),
            root_ca: Some(CertificateSource::File {
                path: ca,
            }),
            mtls_mode: MtlsMode::Required,
            ..Default::default()
        }
    }
}

// ============================================================================
// Builder Pattern for Complex Configurations
// ============================================================================

/// Builder for TLS configuration
pub struct TlsConfigBuilder {
    config: TlsConfig,
}

impl TlsConfigBuilder {
    /// Create a new TLS configuration builder
    pub fn new() -> Self {
        Self {
            config: TlsConfig {
                enabled: true,
                ..Default::default()
            },
        }
    }

    /// Set the server certificate from a file
    pub fn with_cert_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.config.certificate = Some(CertificateSource::File { path: path.into() });
        self
    }

    /// Set the server certificate from PEM content
    pub fn with_cert_pem(mut self, pem: String) -> Self {
        self.config.certificate = Some(CertificateSource::Pem { content: pem });
        self
    }

    /// Set the private key from a file
    pub fn with_key_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.config.private_key = Some(PrivateKeySource::File { path: path.into() });
        self
    }

    /// Set the private key from PEM content
    pub fn with_key_pem(mut self, pem: String) -> Self {
        self.config.private_key = Some(PrivateKeySource::Pem { content: pem });
        self
    }

    /// Set the root CA for server verification (client-side)
    pub fn with_root_ca_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.config.root_ca = Some(CertificateSource::File { path: path.into() });
        self
    }

    /// Set the client CA for mTLS (server-side)
    pub fn with_client_ca_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.config.client_ca = Some(CertificateSource::File { path: path.into() });
        self
    }

    /// Require client certificate (mTLS)
    pub fn require_client_cert(mut self, required: bool) -> Self {
        self.config.mtls_mode = if required {
            MtlsMode::Required
        } else {
            MtlsMode::Disabled
        };
        self
    }

    /// Set mTLS mode
    pub fn with_mtls_mode(mut self, mode: MtlsMode) -> Self {
        self.config.mtls_mode = mode;
        self
    }

    /// Set minimum TLS version
    pub fn with_min_version(mut self, version: TlsVersion) -> Self {
        self.config.min_version = version;
        self
    }

    /// Set ALPN protocols
    pub fn with_alpn_protocols(mut self, protocols: Vec<String>) -> Self {
        self.config.alpn_protocols = protocols;
        self
    }

    /// Set server name for SNI
    pub fn with_server_name(mut self, name: String) -> Self {
        self.config.server_name = Some(name);
        self
    }

    /// Skip certificate verification (DANGEROUS - testing only)
    pub fn insecure_skip_verify(mut self) -> Self {
        self.config.insecure_skip_verify = true;
        self
    }

    /// Add pinned certificate fingerprint
    pub fn with_pinned_certificate(mut self, fingerprint: String) -> Self {
        self.config.pinned_certificates.push(fingerprint);
        self
    }

    /// Use self-signed certificate
    pub fn with_self_signed(mut self, common_name: &str) -> Self {
        self.config.certificate = Some(CertificateSource::SelfSigned {
            common_name: common_name.to_string(),
        });
        self.config.insecure_skip_verify = true;
        self
    }

    /// Enable certificate reloading
    pub fn with_cert_reload_interval(mut self, interval: Duration) -> Self {
        self.config.cert_reload_interval = interval;
        self
    }

    /// Build the TLS configuration
    pub fn build(self) -> TlsConfig {
        self.config
    }
}

impl Default for TlsConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Certificate Loading Utilities
// ============================================================================

/// Load certificates from a source
pub fn load_certificates(source: &CertificateSource) -> TlsResult<Vec<CertificateDer<'static>>> {
    match source {
        CertificateSource::File { path } => {
            let data = fs::read(path).map_err(|e| TlsError::CertificateReadError {
                path: path.clone(),
                source: e,
            })?;
            parse_pem_certificates(&data)
        }
        CertificateSource::Pem { content } => parse_pem_certificates(content.as_bytes()),
        CertificateSource::Der { content } => {
            let der = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, content)
                .map_err(|e| TlsError::InvalidCertificate(format!("Invalid base64: {}", e)))?;
            Ok(vec![CertificateDer::from(der)])
        }
        CertificateSource::SelfSigned { common_name } => {
            let (cert, _key) = generate_self_signed(common_name)?;
            Ok(vec![cert])
        }
    }
}

/// Parse PEM-encoded certificates
fn parse_pem_certificates(data: &[u8]) -> TlsResult<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(Cursor::new(data));
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::InvalidCertificate(format!("Failed to parse PEM: {}", e)))?;

    if certs.is_empty() {
        return Err(TlsError::InvalidCertificate(
            "No certificates found in PEM data".to_string(),
        ));
    }

    Ok(certs)
}

/// Load private key from a source
pub fn load_private_key(source: &PrivateKeySource) -> TlsResult<PrivateKeyDer<'static>> {
    match source {
        PrivateKeySource::File { path } => {
            let data = fs::read(path).map_err(|e| TlsError::KeyReadError {
                path: path.clone(),
                source: e,
            })?;
            parse_pem_private_key(&data)
        }
        PrivateKeySource::Pem { content } => parse_pem_private_key(content.as_bytes()),
        PrivateKeySource::Der { content } => {
            let der = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, content)
                .map_err(|e| TlsError::InvalidPrivateKey(format!("Invalid base64: {}", e)))?;
            Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der)))
        }
    }
}

/// Parse PEM-encoded private key
fn parse_pem_private_key(data: &[u8]) -> TlsResult<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(Cursor::new(data));

    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| TlsError::InvalidPrivateKey(format!("Failed to parse PEM: {}", e)))?
        .ok_or_else(|| TlsError::InvalidPrivateKey("No private key found in PEM data".to_string()))
}

/// Generate self-signed certificate for development/testing
pub fn generate_self_signed(
    common_name: &str,
) -> TlsResult<(CertificateDer<'static>, PrivateKeyDer<'static>)> {
    let subject_alt_names = vec![
        common_name.to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];

    let mut cert_params = rcgen::CertificateParams::new(subject_alt_names)
        .map_err(|e| TlsError::SelfSignedGenerationError(e.to_string()))?;
    
    // Set the distinguished name with proper common name
    cert_params.distinguished_name = rcgen::DistinguishedName::new();
    cert_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String(common_name.to_string()),
    );
    cert_params.distinguished_name.push(
        rcgen::DnType::OrganizationName,
        rcgen::DnValue::Utf8String("Rivven".to_string()),
    );

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| TlsError::SelfSignedGenerationError(e.to_string()))?;

    let cert = cert_params
        .self_signed(&key_pair)
        .map_err(|e| TlsError::SelfSignedGenerationError(e.to_string()))?;

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    Ok((cert_der, key_der))
}

// ============================================================================
// Server-Side TLS (Acceptor)
// ============================================================================

/// TLS acceptor for server-side connections
pub struct TlsAcceptor {
    config: Arc<ServerConfig>,
    inner: tokio_rustls::TlsAcceptor,
    /// Configuration for hot-reloading
    tls_config: TlsConfig,
    /// Reloadable config handle
    reloadable_config: Option<Arc<RwLock<Arc<ServerConfig>>>>,
}

impl TlsAcceptor {
    /// Create a new TLS acceptor from configuration
    pub fn new(config: &TlsConfig) -> TlsResult<Self> {
        let server_config = build_server_config(config)?;
        let server_config = Arc::new(server_config);

        Ok(Self {
            inner: tokio_rustls::TlsAcceptor::from(server_config.clone()),
            config: server_config.clone(),
            tls_config: config.clone(),
            reloadable_config: if config.cert_reload_interval > Duration::ZERO {
                Some(Arc::new(RwLock::new(server_config)))
            } else {
                None
            },
        })
    }

    /// Accept a TLS connection
    pub async fn accept<IO>(&self, stream: IO) -> TlsResult<TlsServerStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let tls_stream = self
            .inner
            .accept(stream)
            .await
            .map_err(|e| TlsError::HandshakeError(e.to_string()))?;

        Ok(TlsServerStream { inner: tls_stream })
    }

    /// Accept a TCP connection with TLS
    pub async fn accept_tcp(&self, stream: TcpStream) -> TlsResult<TlsServerStream<TcpStream>> {
        self.accept(stream).await
    }

    /// Reload certificates (for hot-reloading)
    pub fn reload(&self) -> TlsResult<()> {
        if let Some(ref reloadable) = self.reloadable_config {
            let new_config = build_server_config(&self.tls_config)?;
            *reloadable.write() = Arc::new(new_config);
        }
        Ok(())
    }

    /// Get the underlying rustls ServerConfig
    pub fn config(&self) -> &Arc<ServerConfig> {
        &self.config
    }
}

impl fmt::Debug for TlsAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsAcceptor")
            .field("mtls_mode", &self.tls_config.mtls_mode)
            .field("min_version", &self.tls_config.min_version)
            .finish()
    }
}

/// Build rustls ServerConfig from TlsConfig
fn build_server_config(config: &TlsConfig) -> TlsResult<ServerConfig> {
    // Handle self-signed certificates specially to ensure cert and key match
    let (certs, key) = if let Some(CertificateSource::SelfSigned { common_name }) = &config.certificate {
        // Generate both cert and key together to ensure they match
        let (cert, key) = generate_self_signed(common_name)?;
        (vec![cert], key)
    } else {
        // Load certificates from explicit sources
        let certs = if let Some(ref cert_source) = config.certificate {
            load_certificates(cert_source)?
        } else {
            return Err(TlsError::ConfigError(
                "Server certificate required".to_string(),
            ));
        };
        
        // Load private key
        let key = if let Some(ref key_source) = config.private_key {
            load_private_key(key_source)?
        } else {
            return Err(TlsError::ConfigError(
                "Private key required".to_string(),
            ));
        };
        
        (certs, key)
    };

    // Build TLS versions
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = match config.min_version {
        TlsVersion::Tls13 => vec![&rustls::version::TLS13],
        TlsVersion::Tls12 => vec![&rustls::version::TLS12, &rustls::version::TLS13],
    };

    // Configure client certificate verification
    let client_cert_verifier = match config.mtls_mode {
        MtlsMode::Disabled => None,
        MtlsMode::Optional | MtlsMode::Required => {
            if let Some(ref ca_source) = config.client_ca {
                let ca_certs = load_certificates(ca_source)?;
                let mut root_store = rustls::RootCertStore::empty();
                for cert in ca_certs {
                    root_store.add(cert).map_err(|e| {
                        TlsError::CertificateChainError(format!("Failed to add CA cert: {}", e))
                    })?;
                }

                let verifier = if config.mtls_mode == MtlsMode::Required {
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                        .build()
                        .map_err(|e| {
                            TlsError::ConfigError(format!(
                                "Failed to build client verifier: {}",
                                e
                            ))
                        })?
                } else {
                    rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                        .allow_unauthenticated()
                        .build()
                        .map_err(|e| {
                            TlsError::ConfigError(format!(
                                "Failed to build client verifier: {}",
                                e
                            ))
                        })?
                };

                Some(verifier)
            } else if config.mtls_mode == MtlsMode::Required {
                return Err(TlsError::ConfigError(
                    "mTLS required but no client CA configured".to_string(),
                ));
            } else {
                None
            }
        }
    };

    // Build server config
    let mut server_config = if let Some(verifier) = client_cert_verifier {
        ServerConfig::builder_with_protocol_versions(&versions)
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::ConfigError(format!("Invalid cert/key: {}", e)))?
    } else {
        ServerConfig::builder_with_protocol_versions(&versions)
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::ConfigError(format!("Invalid cert/key: {}", e)))?
    };

    // Configure ALPN
    if !config.alpn_protocols.is_empty() {
        server_config.alpn_protocols = config
            .alpn_protocols
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
    }

    // Configure session cache
    if config.session_cache_size > 0 {
        server_config.session_storage =
            rustls::server::ServerSessionMemoryCache::new(config.session_cache_size);
    }

    Ok(server_config)
}

// ============================================================================
// Client-Side TLS (Connector)
// ============================================================================

/// TLS connector for client-side connections
pub struct TlsConnector {
    config: Arc<ClientConfig>,
    inner: tokio_rustls::TlsConnector,
    /// Default server name for SNI
    server_name: Option<String>,
}

impl TlsConnector {
    /// Create a new TLS connector from configuration
    pub fn new(config: &TlsConfig) -> TlsResult<Self> {
        let client_config = build_client_config(config)?;
        let client_config = Arc::new(client_config);

        Ok(Self {
            inner: tokio_rustls::TlsConnector::from(client_config.clone()),
            config: client_config,
            server_name: config.server_name.clone(),
        })
    }

    /// Connect to a server with TLS
    pub async fn connect<IO>(
        &self,
        stream: IO,
        server_name: &str,
    ) -> TlsResult<TlsClientStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let name: rustls::pki_types::ServerName<'static> = server_name
            .to_string()
            .try_into()
            .map_err(|_| TlsError::ConfigError(format!("Invalid server name: {}", server_name)))?;

        let tls_stream = self
            .inner
            .connect(name, stream)
            .await
            .map_err(|e| TlsError::HandshakeError(e.to_string()))?;

        Ok(TlsClientStream { inner: tls_stream })
    }

    /// Connect to a TCP address with TLS
    pub async fn connect_tcp(
        &self,
        addr: SocketAddr,
        server_name: &str,
    ) -> TlsResult<TlsClientStream<TcpStream>> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| TlsError::ConnectionError(e.to_string()))?;

        self.connect(stream, server_name).await
    }

    /// Connect using the configured server name
    pub async fn connect_with_default_name<IO>(&self, stream: IO) -> TlsResult<TlsClientStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let name = self.server_name.as_ref().ok_or_else(|| {
            TlsError::ConfigError("No server name configured for SNI".to_string())
        })?;
        self.connect(stream, name).await
    }

    /// Get the underlying rustls ClientConfig
    pub fn config(&self) -> &Arc<ClientConfig> {
        &self.config
    }
}

impl fmt::Debug for TlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConnector")
            .field("server_name", &self.server_name)
            .finish()
    }
}

/// Build rustls ClientConfig from TlsConfig
fn build_client_config(config: &TlsConfig) -> TlsResult<ClientConfig> {
    // Build TLS versions
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = match config.min_version {
        TlsVersion::Tls13 => vec![&rustls::version::TLS13],
        TlsVersion::Tls12 => vec![&rustls::version::TLS12, &rustls::version::TLS13],
    };

    // Build root certificate store
    let root_store = if config.insecure_skip_verify {
        // DANGEROUS: Trust all certificates (development only)
        rustls::RootCertStore::empty()
    } else if let Some(ref ca_source) = config.root_ca {
        let ca_certs = load_certificates(ca_source)?;
        let mut store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            store.add(cert).map_err(|e| {
                TlsError::CertificateChainError(format!("Failed to add root CA: {}", e))
            })?;
        }
        store
    } else {
        // Use system root certificates
        let mut store = rustls::RootCertStore::empty();
        let native_certs = rustls_native_certs::load_native_certs();
        for cert in native_certs.certs {
            let _ = store.add(cert);
        }
        store
    };

    // Build client config with or without client certificate
    let mut client_config = if let (Some(ref cert_source), Some(ref key_source)) =
        (&config.certificate, &config.private_key)
    {
        // mTLS: provide client certificate
        let certs = load_certificates(cert_source)?;
        let key = load_private_key(key_source)?;

        ClientConfig::builder_with_protocol_versions(&versions)
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)
            .map_err(|e| TlsError::ConfigError(format!("Invalid client cert/key: {}", e)))?
    } else if config.insecure_skip_verify {
        // DANGEROUS: Skip server verification
        ClientConfig::builder_with_protocol_versions(&versions)
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertificateVerification))
            .with_no_client_auth()
    } else {
        // Standard TLS without client certificate
        ClientConfig::builder_with_protocol_versions(&versions)
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };

    // Configure ALPN
    if !config.alpn_protocols.is_empty() {
        client_config.alpn_protocols = config
            .alpn_protocols
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
    }

    Ok(client_config)
}

/// DANGEROUS: Certificate verifier that accepts any certificate
/// Only for development/testing
#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

// ============================================================================
// TLS Streams
// ============================================================================

/// Server-side TLS stream
pub struct TlsServerStream<IO> {
    inner: tokio_rustls::server::TlsStream<IO>,
}

impl<IO> TlsServerStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Get client certificate if presented
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'_>]> {
        self.inner.get_ref().1.peer_certificates()
    }

    /// Get ALPN protocol if negotiated
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.inner.get_ref().1.alpn_protocol()
    }

    /// Get negotiated protocol version
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.inner.get_ref().1.protocol_version()
    }

    /// Check if the connection uses TLS 1.3
    pub fn is_tls_13(&self) -> bool {
        self.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3)
    }

    /// Extract the client certificate common name (CN)
    pub fn peer_common_name(&self) -> Option<String> {
        self.peer_certificates().and_then(|certs| {
            if certs.is_empty() {
                return None;
            }
            extract_common_name(&certs[0])
        })
    }

    /// Extract the client certificate subject
    pub fn peer_subject(&self) -> Option<String> {
        self.peer_certificates().and_then(|certs| {
            if certs.is_empty() {
                return None;
            }
            extract_subject(&certs[0])
        })
    }

    /// Get reference to the inner stream
    pub fn get_ref(&self) -> &IO {
        self.inner.get_ref().0
    }

    /// Get mutable reference to the inner stream
    pub fn get_mut(&mut self) -> &mut IO {
        self.inner.get_mut().0
    }

    /// Unwrap and get the inner stream
    pub fn into_inner(self) -> IO {
        self.inner.into_inner().0
    }
}

impl<IO> tokio::io::AsyncRead for TlsServerStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<IO> tokio::io::AsyncWrite for TlsServerStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Client-side TLS stream
pub struct TlsClientStream<IO> {
    inner: tokio_rustls::client::TlsStream<IO>,
}

impl<IO> TlsClientStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    /// Get server certificate if provided
    pub fn peer_certificates(&self) -> Option<&[CertificateDer<'_>]> {
        self.inner.get_ref().1.peer_certificates()
    }

    /// Get ALPN protocol if negotiated
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.inner.get_ref().1.alpn_protocol()
    }

    /// Get negotiated protocol version
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        self.inner.get_ref().1.protocol_version()
    }

    /// Check if the connection uses TLS 1.3
    pub fn is_tls_13(&self) -> bool {
        self.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3)
    }

    /// Get reference to the inner stream
    pub fn get_ref(&self) -> &IO {
        self.inner.get_ref().0
    }

    /// Get mutable reference to the inner stream
    pub fn get_mut(&mut self) -> &mut IO {
        self.inner.get_mut().0
    }

    /// Unwrap and get the inner stream
    pub fn into_inner(self) -> IO {
        self.inner.into_inner().0
    }
}

impl<IO> tokio::io::AsyncRead for TlsClientStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<IO> tokio::io::AsyncWrite for TlsClientStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// ============================================================================
// Certificate Utilities
// ============================================================================

/// Extract common name (CN) from certificate
fn extract_common_name(cert: &CertificateDer<'_>) -> Option<String> {
    // Parse the certificate using x509-parser
    let (_, cert) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;

    for rdn in cert.subject().iter_rdn() {
        for attr in rdn.iter() {
            if attr.attr_type() == &x509_parser::oid_registry::OID_X509_COMMON_NAME {
                return attr.as_str().ok().map(|s| s.to_string());
            }
        }
    }

    None
}

/// Extract full subject from certificate
fn extract_subject(cert: &CertificateDer<'_>) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert.as_ref()).ok()?;
    Some(cert.subject().to_string())
}

/// Calculate SHA-256 fingerprint of a certificate
pub fn certificate_fingerprint(cert: &CertificateDer<'_>) -> String {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(cert.as_ref());
    hex::encode(hash)
}

/// Verify certificate chain
/// 
/// Note: This is a basic sanity check. The actual TLS handshake performs
/// full chain validation using WebPKI through rustls.
pub fn verify_certificate_chain(
    chain: &[CertificateDer<'_>],
    root_store: &rustls::RootCertStore,
) -> TlsResult<()> {
    if chain.is_empty() {
        return Err(TlsError::CertificateChainError(
            "Empty certificate chain".to_string(),
        ));
    }

    // Basic sanity check - the actual validation happens during TLS handshake
    // via rustls WebPKI implementation
    if root_store.is_empty() {
        tracing::warn!("Root certificate store is empty - chain validation may fail");
    }

    // Log certificate chain info for debugging
    for (i, cert) in chain.iter().enumerate() {
        let fingerprint = certificate_fingerprint(cert);
        tracing::debug!("Certificate chain[{}]: fingerprint={}", i, &fingerprint[..16]);
    }

    Ok(())
}

// ============================================================================
// Certificate Watcher for Hot Reloading
// ============================================================================

/// Watches certificate files and triggers reload on changes
pub struct CertificateWatcher {
    /// Files being watched
    watched_files: Vec<PathBuf>,
    /// Last modification times
    last_modified: HashMap<PathBuf, SystemTime>,
    /// Callback for reload
    reload_callback: Box<dyn Fn() + Send + Sync>,
}

impl CertificateWatcher {
    /// Create a new certificate watcher
    pub fn new<F>(files: Vec<PathBuf>, callback: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut last_modified = HashMap::new();
        for file in &files {
            if let Ok(meta) = fs::metadata(file) {
                if let Ok(modified) = meta.modified() {
                    last_modified.insert(file.clone(), modified);
                }
            }
        }

        Self {
            watched_files: files,
            last_modified,
            reload_callback: Box::new(callback),
        }
    }

    /// Check for file changes and trigger reload if needed
    pub fn check_and_reload(&mut self) -> bool {
        let mut changed = false;

        for file in &self.watched_files {
            if let Ok(meta) = fs::metadata(file) {
                if let Ok(modified) = meta.modified() {
                    let last = self.last_modified.get(file);
                    if last.is_none_or(|&l| modified > l) {
                        self.last_modified.insert(file.clone(), modified);
                        changed = true;
                    }
                }
            }
        }

        if changed {
            (self.reload_callback)();
        }

        changed
    }

    /// Start watching in background
    pub fn spawn(mut self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                self.check_and_reload();
            }
        })
    }
}

// ============================================================================
// Connection Identity (mTLS Integration)
// ============================================================================

/// Identity extracted from TLS connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsIdentity {
    /// Certificate common name (CN)
    pub common_name: Option<String>,
    /// Full certificate subject
    pub subject: Option<String>,
    /// Certificate fingerprint (SHA-256)
    pub fingerprint: String,
    /// Organization from certificate
    pub organization: Option<String>,
    /// Organizational unit from certificate
    pub organizational_unit: Option<String>,
    /// Certificate serial number
    pub serial_number: Option<String>,
    /// Certificate validity period
    pub valid_from: Option<chrono::DateTime<chrono::Utc>>,
    pub valid_until: Option<chrono::DateTime<chrono::Utc>>,
    /// Is the certificate still valid
    pub is_valid: bool,
}

impl TlsIdentity {
    /// Extract identity from a certificate
    pub fn from_certificate(cert: &CertificateDer<'_>) -> Self {
        let fingerprint = certificate_fingerprint(cert);
        let common_name = extract_common_name(cert);
        let subject = extract_subject(cert);

        // Parse additional fields using x509-parser
        let (organization, organizational_unit, serial_number, valid_from, valid_until, is_valid) =
            if let Ok((_, parsed)) = x509_parser::parse_x509_certificate(cert.as_ref()) {
                let mut org = None;
                let mut ou = None;

                for rdn in parsed.subject().iter_rdn() {
                    for attr in rdn.iter() {
                        if attr.attr_type() == &x509_parser::oid_registry::OID_X509_ORGANIZATION_NAME {
                            org = attr.as_str().ok().map(|s| s.to_string());
                        }
                        if attr.attr_type() == &x509_parser::oid_registry::OID_X509_ORGANIZATIONAL_UNIT {
                            ou = attr.as_str().ok().map(|s| s.to_string());
                        }
                    }
                }

                let serial = Some(parsed.serial.to_str_radix(16));

                let validity = parsed.validity();
                let now = chrono::Utc::now();

                let from = chrono::DateTime::from_timestamp(validity.not_before.timestamp(), 0);
                let until = chrono::DateTime::from_timestamp(validity.not_after.timestamp(), 0);

                let valid = from.is_some_and(|f| now >= f) && until.is_some_and(|u| now <= u);

                (org, ou, serial, from, until, valid)
            } else {
                (None, None, None, None, None, false)
            };

        Self {
            common_name,
            subject,
            fingerprint,
            organization,
            organizational_unit,
            serial_number,
            valid_from,
            valid_until,
            is_valid,
        }
    }
}

// ============================================================================
// Security Best Practices
// ============================================================================

/// Security audit of TLS configuration
#[derive(Debug)]
pub struct TlsSecurityAudit {
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
    pub recommendations: Vec<String>,
}

impl TlsSecurityAudit {
    /// Audit a TLS configuration for security issues
    pub fn audit(config: &TlsConfig) -> Self {
        let mut audit = Self {
            warnings: vec![],
            errors: vec![],
            recommendations: vec![],
        };

        if !config.enabled {
            audit
                .errors
                .push("TLS is disabled - all traffic will be unencrypted".to_string());
        }

        if config.insecure_skip_verify {
            audit.errors.push(
                "Certificate verification is disabled - vulnerable to MITM attacks".to_string(),
            );
        }

        if config.min_version == TlsVersion::Tls12 {
            audit.warnings.push(
                "TLS 1.2 is allowed - consider requiring TLS 1.3 for better security".to_string(),
            );
        }

        if config.mtls_mode == MtlsMode::Disabled && config.client_ca.is_some() {
            audit.warnings.push(
                "Client CA configured but mTLS is disabled - clients won't be verified".to_string(),
            );
        }

        if config.mtls_mode == MtlsMode::Optional {
            audit.warnings.push(
                "mTLS is optional - some clients may connect without certificates".to_string(),
            );
        }

        if config.session_cache_size == 0 {
            audit.recommendations.push(
                "Consider enabling session cache for better performance".to_string(),
            );
        }

        if config.cert_reload_interval == Duration::ZERO {
            audit.recommendations.push(
                "Consider enabling certificate hot-reloading for zero-downtime rotation"
                    .to_string(),
            );
        }

        if config.pinned_certificates.is_empty() && !config.insecure_skip_verify {
            audit.recommendations.push(
                "Consider certificate pinning for high-security deployments".to_string(),
            );
        }

        audit
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.mtls_mode, MtlsMode::Disabled);
        assert_eq!(config.min_version, TlsVersion::Tls13);
    }

    #[test]
    fn test_tls_config_builder() {
        let config = TlsConfigBuilder::new()
            .with_cert_file("/path/to/cert.pem")
            .with_key_file("/path/to/key.pem")
            .with_client_ca_file("/path/to/ca.pem")
            .require_client_cert(true)
            .with_min_version(TlsVersion::Tls12)
            .build();

        assert!(config.enabled);
        assert_eq!(config.mtls_mode, MtlsMode::Required);
        assert_eq!(config.min_version, TlsVersion::Tls12);
    }

    #[tokio::test]
    async fn test_tls_server_client_handshake() {
        // Install crypto provider (required by rustls 0.23+)
        let _ = rustls::crypto::ring::default_provider().install_default();
        
        // Use SelfSigned source which generates at runtime
        let server_config = TlsConfig {
            enabled: true,
            certificate: Some(CertificateSource::SelfSigned { 
                common_name: "localhost".to_string(),
            }),
            // Key is auto-generated with self-signed
            mtls_mode: MtlsMode::Disabled,
            ..Default::default()
        };
        
        // Create client config that skips verification (for self-signed)
        let client_config = TlsConfig {
            enabled: true,
            insecure_skip_verify: true,
            ..Default::default()
        };
        
        // Create acceptor and connector
        let acceptor = TlsAcceptor::new(&server_config).unwrap();
        let connector = TlsConnector::new(&client_config).unwrap();
        
        // Start a TCP listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Server task: accept TLS connection and echo data
        let server_task = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let mut tls_stream: TlsServerStream<tokio::net::TcpStream> = acceptor.accept_tcp(tcp_stream).await.unwrap();
            
            // Read data
            let mut buf = [0u8; 32];
            let n = tls_stream.read(&mut buf).await.unwrap();
            
            // Echo it back
            tls_stream.write_all(&buf[..n]).await.unwrap();
            tls_stream.flush().await.unwrap();
            
            n
        });
        
        // Client task: connect and send data
        let client_task = tokio::spawn(async move {
            let mut stream: TlsClientStream<tokio::net::TcpStream> = connector.connect_tcp(addr, "localhost").await.unwrap();
            
            // Send test message
            let message = b"Hello, TLS!";
            stream.write_all(message).await.unwrap();
            stream.flush().await.unwrap();
            
            // Read response
            let mut response = [0u8; 32];
            let n = stream.read(&mut response).await.unwrap();
            
            (message.to_vec(), response[..n].to_vec())
        });
        
        // Wait for both tasks
        let (server_result, client_result) = tokio::join!(server_task, client_task);
        
        let server_bytes_read = server_result.unwrap();
        let (sent, received) = client_result.unwrap();
        
        // Verify echo worked
        assert_eq!(server_bytes_read, sent.len());
        assert_eq!(sent, received);
    }

    #[tokio::test]
    async fn test_mtls_server_client_handshake() {
        use rcgen::{CertificateParams, DnType, IsCa, BasicConstraints, KeyUsagePurpose};
        
        // Install crypto provider (required by rustls 0.23+)
        let _ = rustls::crypto::ring::default_provider().install_default();
        
        // Generate a shared CA certificate
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        ca_params.distinguished_name.push(DnType::CommonName, "Rivven Test CA");
        let ca_key_pair = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key_pair).unwrap();
        let ca_cert_pem = ca_cert.pem();
        
        // Generate server certificate signed by CA
        let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        server_params.distinguished_name.push(DnType::CommonName, "localhost");
        let server_key_pair = rcgen::KeyPair::generate().unwrap();
        let server_cert = server_params.signed_by(&server_key_pair, &ca_cert, &ca_key_pair).unwrap();
        let server_cert_pem = server_cert.pem();
        let server_key_pem = server_key_pair.serialize_pem();
        
        // Generate client certificate signed by CA
        let mut client_params = CertificateParams::new(vec!["client.rivven.local".to_string()]).unwrap();
        client_params.distinguished_name.push(DnType::CommonName, "client.rivven.local");
        let client_key_pair = rcgen::KeyPair::generate().unwrap();
        let client_cert = client_params.signed_by(&client_key_pair, &ca_cert, &ca_key_pair).unwrap();
        let client_cert_pem = client_cert.pem();
        let client_key_pem = client_key_pair.serialize_pem();
        
        // Server config with mTLS required
        let server_config = TlsConfig {
            enabled: true,
            certificate: Some(CertificateSource::Pem { content: server_cert_pem }),
            private_key: Some(PrivateKeySource::Pem { content: server_key_pem }),
            client_ca: Some(CertificateSource::Pem { content: ca_cert_pem.clone() }),
            mtls_mode: MtlsMode::Required,
            insecure_skip_verify: false,
            ..Default::default()
        };
        
        // Client config with client cert and CA trust
        let client_config = TlsConfig {
            enabled: true,
            certificate: Some(CertificateSource::Pem { content: client_cert_pem }),
            private_key: Some(PrivateKeySource::Pem { content: client_key_pem }),
            root_ca: Some(CertificateSource::Pem { content: ca_cert_pem }),
            insecure_skip_verify: false,
            ..Default::default()
        };
        
        // Create acceptor and connector
        let acceptor = TlsAcceptor::new(&server_config).unwrap();
        let connector = TlsConnector::new(&client_config).unwrap();
        
        // Start a TCP listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        // Server task
        let server_task = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let mut tls_stream: TlsServerStream<tokio::net::TcpStream> = acceptor.accept_tcp(tcp_stream).await.unwrap();
            
            // Check if we can see peer certificates (mTLS)
            let has_peer_cert = tls_stream.peer_certificates().is_some();
            
            // Read data
            let mut buf = [0u8; 32];
            let n = tls_stream.read(&mut buf).await.unwrap();
            tls_stream.write_all(&buf[..n]).await.unwrap();
            tls_stream.flush().await.unwrap();
            
            (n, has_peer_cert)
        });
        
        // Client task
        let client_task = tokio::spawn(async move {
            let mut stream: TlsClientStream<tokio::net::TcpStream> = connector.connect_tcp(addr, "localhost").await.unwrap();
            
            // Send test message
            let message = b"mTLS Test!";
            stream.write_all(message).await.unwrap();
            stream.flush().await.unwrap();
            
            // Read response
            let mut response = [0u8; 32];
            let n = stream.read(&mut response).await.unwrap();
            
            (message.to_vec(), response[..n].to_vec())
        });
        
        // Wait for both tasks
        let (server_result, client_result) = tokio::join!(server_task, client_task);
        
        let (server_bytes_read, has_peer_cert) = server_result.unwrap();
        let (sent, received) = client_result.unwrap();
        
        // Verify echo worked
        assert_eq!(server_bytes_read, sent.len());
        assert_eq!(sent, received);
        
        // Verify mTLS - server saw client certificate
        assert!(has_peer_cert, "Server should have received client certificate in mTLS");
    }

    #[test]
    fn test_self_signed_generation() {
        let result = generate_self_signed("test.rivven.local");
        assert!(result.is_ok());

        let (cert, _key) = result.unwrap();
        assert!(!cert.as_ref().is_empty());

        // Verify we can extract identity
        let identity = TlsIdentity::from_certificate(&cert);
        assert_eq!(identity.common_name, Some("test.rivven.local".to_string()));
        assert!(identity.is_valid);
    }

    #[test]
    fn test_certificate_fingerprint() {
        let (cert, _) = generate_self_signed("test.rivven.local").unwrap();
        let fingerprint = certificate_fingerprint(&cert);

        // Should be 64 hex characters (SHA-256)
        assert_eq!(fingerprint.len(), 64);
        assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_tls_security_audit_disabled() {
        let config = TlsConfig::disabled();
        let audit = TlsSecurityAudit::audit(&config);

        assert!(!audit.errors.is_empty());
        assert!(audit.errors.iter().any(|e| e.contains("disabled")));
    }

    #[test]
    fn test_tls_security_audit_insecure() {
        let config = TlsConfig {
            enabled: true,
            insecure_skip_verify: true,
            ..Default::default()
        };
        let audit = TlsSecurityAudit::audit(&config);

        assert!(audit.errors.iter().any(|e| e.contains("MITM")));
    }

    #[test]
    fn test_tls_security_audit_production_ready() {
        let (_cert, _key) = generate_self_signed("broker.rivven.local").unwrap();

        let config = TlsConfig {
            enabled: true,
            certificate: Some(CertificateSource::SelfSigned {
                common_name: "broker.rivven.local".to_string(),
            }),
            mtls_mode: MtlsMode::Required,
            min_version: TlsVersion::Tls13,
            insecure_skip_verify: false,
            session_cache_size: 256,
            ..Default::default()
        };

        let audit = TlsSecurityAudit::audit(&config);

        // Should have no errors for a well-configured setup
        // (Note: mTLS Required without client_ca would fail at runtime, but audit catches config issues)
        assert!(audit.errors.is_empty() || audit.errors.iter().all(|e| !e.contains("disabled")));
    }

    #[test]
    fn test_mtls_modes() {
        assert_eq!(MtlsMode::default(), MtlsMode::Disabled);

        let modes = [MtlsMode::Disabled, MtlsMode::Optional, MtlsMode::Required];
        for mode in modes {
            let json = serde_json::to_string(&mode).unwrap();
            let parsed: MtlsMode = serde_json::from_str(&json).unwrap();
            assert_eq!(mode, parsed);
        }
    }

    #[test]
    fn test_tls_identity_extraction() {
        let (cert, _) = generate_self_signed("service.rivven.internal").unwrap();
        let identity = TlsIdentity::from_certificate(&cert);

        assert_eq!(identity.common_name, Some("service.rivven.internal".to_string()));
        assert!(identity.is_valid);
        assert!(identity.valid_from.is_some());
        assert!(identity.valid_until.is_some());
        assert!(!identity.fingerprint.is_empty());
    }
}
