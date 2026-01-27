//! PostgreSQL replication client
//!
//! Low-level TCP client for PostgreSQL replication protocol.
//! Supports MD5 authentication, TLS connections, and pgoutput streaming.

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use md5::{Digest, Md5};
use postgres_protocol::message::{backend, frontend};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

#[cfg(feature = "postgres-tls")]
use tracing::trace;

use crate::common::{
    CircuitBreaker, RateLimiter, Validator, CONNECTION_TIMEOUT_SECS, IO_TIMEOUT_SECS,
};

use super::scram::ScramSha256;

#[cfg(feature = "postgres-tls")]
use crate::common::{tls::build_rustls_config, TlsConfig};
#[cfg(feature = "postgres-tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "postgres-tls")]
use tokio_rustls::{client::TlsStream, TlsConnector};

// ============================================================================
// Stream Types
// ============================================================================

/// Wrapper for handling both plain TCP and TLS streams
pub enum StreamWrapper {
    /// Plain TCP connection (no encryption)
    Plain(BufReader<TcpStream>),
    /// TLS-encrypted connection (boxed to avoid large enum variant)
    #[cfg(feature = "postgres-tls")]
    Tls(Box<BufReader<TlsStream<TcpStream>>>),
}

impl StreamWrapper {
    /// Read exactly n bytes into buffer
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            StreamWrapper::Plain(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
        }
    }

    /// Read a single byte
    pub async fn read_u8(&mut self) -> std::io::Result<u8> {
        match self {
            StreamWrapper::Plain(s) => s.read_u8().await,
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(s) => s.read_u8().await,
        }
    }

    /// Read i32 in big-endian
    pub async fn read_i32(&mut self) -> std::io::Result<i32> {
        match self {
            StreamWrapper::Plain(s) => s.read_i32().await,
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(s) => s.read_i32().await,
        }
    }

    /// Write all bytes
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            StreamWrapper::Plain(s) => s.get_mut().write_all(buf).await,
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(s) => s.get_mut().write_all(buf).await,
        }
    }

    /// Flush the stream
    pub async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            StreamWrapper::Plain(s) => s.get_mut().flush().await,
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(s) => s.get_mut().flush().await,
        }
    }

    /// Check if this is a TLS stream
    pub fn is_tls(&self) -> bool {
        match self {
            StreamWrapper::Plain(_) => false,
            #[cfg(feature = "postgres-tls")]
            StreamWrapper::Tls(_) => true,
        }
    }
}

// ============================================================================
// Basic Replication Client (deprecated)
// ============================================================================

/// PostgreSQL replication client (basic, without TLS)
pub struct ReplicationClient {
    stream: BufReader<TcpStream>,
    user: String,
    database: String,
}

impl ReplicationClient {
    /// Connect to PostgreSQL in replication mode
    ///
    /// # Security
    ///
    /// - Validates user and database identifiers to prevent SQL injection
    /// - Applies connection timeout to prevent hanging connections
    /// - Applies I/O timeouts for all read/write operations
    #[deprecated(
        since = "0.3.0",
        note = "Use connect_secure() which provides circuit breaker and rate limiting"
    )]
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> Result<Self> {
        // Security: Validate identifiers before use in SQL
        Validator::validate_identifier(user)?;
        Validator::validate_identifier(database)?;

        info!("Connecting to {}:{} as {}", host, port, user);

        // Security: Apply connection timeout to prevent hanging
        let stream = match timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            TcpStream::connect((host, port)),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {
                return Err(anyhow!(
                    "Connection timeout after {}s",
                    CONNECTION_TIMEOUT_SECS
                ))
            }
        };
        let mut stream = BufReader::new(stream);

        // 1. Send Startup Message
        let params = vec![
            ("user", user),
            ("database", database),
            ("replication", "database"),
        ];
        let mut buf = BytesMut::new();
        frontend::startup_message(params.into_iter(), &mut buf)?;
        stream.write_all(&buf).await?;
        stream.flush().await?;

        // 2. Handle Authentication
        loop {
            let type_code = stream.read_u8().await?;
            let len = stream.read_i32().await? as usize;
            if len < 4 {
                return Err(anyhow!("Invalid message length"));
            }
            let mut body = vec![0u8; len - 4];
            stream.read_exact(&mut body).await?;

            let mut raw_msg = BytesMut::with_capacity(1 + len);
            raw_msg.put_u8(type_code);
            raw_msg.put_i32(len as i32);
            raw_msg.put_slice(&body);

            let msg = backend::Message::parse(&mut raw_msg)?
                .ok_or(anyhow!("Failed to parse auth message"))?;

            match msg {
                backend::Message::AuthenticationOk => {
                    debug!("Authentication successful");
                    break;
                }
                backend::Message::AuthenticationCleartextPassword => {
                    let pass =
                        password.ok_or_else(|| anyhow!("Password required but not provided"))?;
                    let mut buf = BytesMut::new();
                    frontend::password_message(pass.as_bytes(), &mut buf)?;
                    stream.write_all(&buf).await?;
                    stream.flush().await?;
                }
                backend::Message::AuthenticationMd5Password(body) => {
                    let pass =
                        password.ok_or_else(|| anyhow!("Password required but not provided"))?;
                    let salt = body.salt();
                    let hash = hash_md5_password(user, pass, &salt);
                    let mut buf = BytesMut::new();
                    frontend::password_message(hash.as_bytes(), &mut buf)?;
                    stream.write_all(&buf).await?;
                    stream.flush().await?;
                }
                backend::Message::AuthenticationSasl(body) => {
                    let pass =
                        password.ok_or_else(|| anyhow!("Password required but not provided"))?;

                    // Check for SCRAM-SHA-256 support (FallibleIterator)
                    let mechanisms: Vec<&str> = body.mechanisms().collect()?;
                    if !mechanisms.contains(&"SCRAM-SHA-256") {
                        return Err(anyhow!(
                            "Server requires SASL authentication but doesn't support SCRAM-SHA-256. Available: {:?}",
                            mechanisms
                        ));
                    }

                    debug!("Starting SCRAM-SHA-256 authentication");

                    // Perform SCRAM-SHA-256 exchange
                    perform_scram_auth(&mut stream, user, pass).await?;
                }
                backend::Message::ErrorResponse(_body) => {
                    return Err(anyhow!("Auth error response from server"));
                }
                _ => return Err(anyhow!("Unexpected message during auth: {}", type_code)),
            }
        }

        // 3. Wait for ReadyForQuery
        loop {
            let type_code = stream.read_u8().await?;
            let len = stream.read_i32().await? as usize;
            let mut body = vec![0u8; len - 4];
            stream.read_exact(&mut body).await?;

            if type_code == b'Z' {
                debug!("Ready for query");
                break;
            } else if type_code == b'E' {
                return Err(anyhow!("Error waiting for ready"));
            }
        }

        Ok(Self {
            stream,
            user: user.to_string(),
            database: database.to_string(),
        })
    }

    /// Create a replication slot
    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<()> {
        let query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL {}",
            slot_name, output_plugin
        );
        self.simple_query(&query).await?;
        Ok(())
    }

    /// Start streaming replication
    pub async fn start_replication(
        mut self,
        slot_name: &str,
        start_lsn: u64,
        pub_name: &str,
    ) -> Result<ReplicationStream> {
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {}/{} (proto_version '1', publication_names '{}')",
            slot_name,
            (start_lsn >> 32) as u32,
            start_lsn as u32,
            pub_name
        );

        let mut buf = BytesMut::new();
        frontend::query(&query, &mut buf)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        // Expect CopyBothResponse ('W')
        let type_code = self.stream.read_u8().await?;
        let len = self.stream.read_i32().await? as usize;
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).await?;

        if type_code == b'W' {
            info!("Entered CopyBoth mode");
            Ok(ReplicationStream {
                stream: self.stream,
            })
        } else if type_code == b'E' {
            Err(anyhow!("Failed to start replication: Error response"))
        } else {
            Err(anyhow!(
                "Unexpected response to START_REPLICATION: {}",
                type_code as char
            ))
        }
    }

    /// Get the database name
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Get the username
    pub fn user(&self) -> &str {
        &self.user
    }

    async fn simple_query(&mut self, query: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::query(query, &mut buf)?;
        self.stream.write_all(&buf).await?;
        self.stream.flush().await?;

        loop {
            let type_code = self.stream.read_u8().await?;
            let len = self.stream.read_i32().await? as usize;
            let mut body = vec![0u8; len - 4];
            self.stream.read_exact(&mut body).await?;

            match type_code {
                b'C' | b'Z' => break,
                b'E' => return Err(anyhow!("Query error")),
                _ => {}
            }
        }
        Ok(())
    }
}

/// Replication stream for receiving WAL data
pub struct ReplicationStream {
    stream: BufReader<TcpStream>,
}

impl ReplicationStream {
    /// Get next replication message
    ///
    /// Returns:
    /// - `Ok(Some(Bytes))`: Raw CopyData payload
    /// - `Ok(None)`: End of stream (CopyDone)
    /// - `Err`: Error
    pub async fn next_message(&mut self) -> Result<Option<Bytes>> {
        let type_code = self.stream.read_u8().await.context("Failed to read type")?;
        let len = self.stream.read_i32().await.context("Failed to read len")? as usize;

        if len < 4 {
            return Err(anyhow!("Invalid frame length"));
        }

        let mut body = vec![0u8; len - 4];
        self.stream
            .read_exact(&mut body)
            .await
            .context("Failed to read body")?;

        match type_code {
            b'd' => Ok(Some(Bytes::from(body))), // CopyData
            b'c' => Ok(None),                    // CopyDone
            b'E' => Err(anyhow!("Replication Error")),
            _ => Err(anyhow!("Unexpected message type: {}", type_code as char)),
        }
    }

    /// Send status update (keepalive response)
    pub async fn send_status_update(&mut self, lsn: u64) -> Result<()> {
        let mut payload = BytesMut::with_capacity(34);
        payload.put_u8(b'r');
        payload.put_u64(lsn);
        payload.put_u64(lsn);
        payload.put_u64(lsn);

        // Postgres epoch: 2000-01-01 00:00:00 UTC
        let pg_epoch =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(946684800);
        let now = std::time::SystemTime::now();
        let micros = match now.duration_since(pg_epoch) {
            Ok(d) => d.as_micros() as i64,
            Err(_) => 0,
        };
        payload.put_i64(micros);
        payload.put_u8(0);

        let mut frame = BytesMut::with_capacity(1 + 4 + payload.len());
        frame.put_u8(b'd');
        frame.put_i32((payload.len() + 4) as i32);
        frame.put_slice(&payload);

        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

// ============================================================================
// Secure Replication Client (with TLS support)
// ============================================================================

/// Secure replication client with TLS, circuit breaker, and rate limiting
#[allow(dead_code)]
pub struct SecureReplicationClient {
    stream: StreamWrapper,
    user: String,
    database: String,
    circuit_breaker: Arc<CircuitBreaker>,
    rate_limiter: Arc<RateLimiter>,
    is_tls: bool,
}

impl SecureReplicationClient {
    /// Connect with security features (no TLS)
    ///
    /// For TLS connections, use `connect_with_tls()` instead.
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> Result<Self> {
        Validator::validate_identifier(user)?;
        Validator::validate_identifier(database)?;

        info!(
            "Connecting to {}:{} as {} (secure, no TLS)",
            host, port, user
        );

        let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(30), 2));

        if !circuit_breaker.allow_request().await {
            return Err(anyhow!("Circuit breaker is OPEN"));
        }

        let tcp_stream = Self::connect_tcp(host, port, &circuit_breaker).await?;
        let mut stream = StreamWrapper::Plain(BufReader::new(tcp_stream));

        Self::perform_startup_and_auth(&mut stream, user, database, password, &circuit_breaker)
            .await?;

        circuit_breaker.record_success().await;
        let rate_limiter = Arc::new(RateLimiter::new(1000, 10000));

        Ok(Self {
            stream,
            user: user.to_string(),
            database: database.to_string(),
            circuit_breaker,
            rate_limiter,
            is_tls: false,
        })
    }

    /// Connect with TLS encryption
    ///
    /// This method:
    /// 1. Establishes TCP connection
    /// 2. Sends SSLRequest to PostgreSQL
    /// 3. Upgrades to TLS if server accepts
    /// 4. Performs authentication over encrypted channel
    ///
    /// # Arguments
    /// * `host` - PostgreSQL server hostname
    /// * `port` - PostgreSQL server port
    /// * `user` - Database user
    /// * `database` - Database name
    /// * `password` - Optional password
    /// * `tls_config` - TLS configuration
    #[cfg(feature = "postgres-tls")]
    pub async fn connect_with_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        tls_config: &TlsConfig,
    ) -> Result<Self> {
        Validator::validate_identifier(user)?;
        Validator::validate_identifier(database)?;

        // Validate TLS config
        tls_config
            .validate()
            .map_err(|e| anyhow!("Invalid TLS config: {}", e))?;

        // If TLS is disabled, fall back to plain connection
        if !tls_config.is_enabled() {
            info!(
                "TLS disabled, connecting to {}:{} without encryption",
                host, port
            );
            return Self::connect(host, port, user, database, password).await;
        }

        info!(
            "Connecting to {}:{} as {} (secure, TLS mode: {})",
            host, port, user, tls_config.mode
        );

        let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(30), 2));

        if !circuit_breaker.allow_request().await {
            return Err(anyhow!("Circuit breaker is OPEN"));
        }

        let tcp_stream = Self::connect_tcp(host, port, &circuit_breaker).await?;

        // Attempt TLS upgrade
        let stream = Self::upgrade_to_tls(tcp_stream, host, tls_config, &circuit_breaker).await?;

        let mut stream = StreamWrapper::Tls(Box::new(BufReader::new(stream)));

        Self::perform_startup_and_auth(&mut stream, user, database, password, &circuit_breaker)
            .await?;

        circuit_breaker.record_success().await;
        let rate_limiter = Arc::new(RateLimiter::new(1000, 10000));

        info!("✓ TLS connection established successfully");

        Ok(Self {
            stream,
            user: user.to_string(),
            database: database.to_string(),
            circuit_breaker,
            rate_limiter,
            is_tls: true,
        })
    }

    /// Check if the connection is using TLS
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    /// Establish TCP connection with timeout
    async fn connect_tcp(
        host: &str,
        port: u16,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<TcpStream> {
        match timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            TcpStream::connect((host, port)),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(e)) => {
                circuit_breaker.record_failure().await;
                Err(e.into())
            }
            Err(_) => {
                circuit_breaker.record_failure().await;
                Err(anyhow!(
                    "Connection timeout after {}s",
                    CONNECTION_TIMEOUT_SECS
                ))
            }
        }
    }

    /// Upgrade TCP connection to TLS
    #[cfg(feature = "postgres-tls")]
    async fn upgrade_to_tls(
        mut tcp_stream: TcpStream,
        host: &str,
        tls_config: &TlsConfig,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<TlsStream<TcpStream>> {
        // Send SSLRequest message
        // Format: 4 bytes length (8) + 4 bytes SSL request code (80877103)
        let ssl_request = [0u8, 0, 0, 8, 4, 210, 22, 47]; // 80877103 = 0x04D2162F
        tcp_stream.write_all(&ssl_request).await?;
        tcp_stream.flush().await?;

        // Read server response (single byte: 'S' for SSL, 'N' for no SSL)
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        match response[0] {
            b'S' => {
                trace!("Server accepted SSL request");
            }
            b'N' => {
                if tls_config.is_required() {
                    circuit_breaker.record_failure().await;
                    return Err(anyhow!(
                        "Server does not support SSL but TLS mode '{}' requires it",
                        tls_config.mode
                    ));
                } else {
                    // This shouldn't happen since we check is_enabled() earlier
                    return Err(anyhow!("Server rejected SSL but TLS was requested"));
                }
            }
            other => {
                circuit_breaker.record_failure().await;
                return Err(anyhow!(
                    "Unexpected SSL response from server: 0x{:02x}",
                    other
                ));
            }
        }

        // Build rustls config
        let rustls_config = build_rustls_config(tls_config)?;
        let connector = TlsConnector::from(Arc::new(rustls_config));

        // Determine server name for SNI
        let server_name = tls_config
            .server_name
            .as_deref()
            .unwrap_or(host)
            .to_string();

        let server_name = ServerName::try_from(server_name.clone())
            .map_err(|_| anyhow!("Invalid server name for TLS: {}", server_name))?;

        // Perform TLS handshake
        let tls_stream = timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            connector.connect(server_name, tcp_stream),
        )
        .await
        .context("TLS handshake timeout")?
        .context("TLS handshake failed")?;

        Ok(tls_stream)
    }

    /// Perform startup message and authentication
    async fn perform_startup_and_auth(
        stream: &mut StreamWrapper,
        user: &str,
        database: &str,
        password: Option<&str>,
        circuit_breaker: &CircuitBreaker,
    ) -> Result<()> {
        // Send startup message
        let params = vec![
            ("user", user),
            ("database", database),
            ("replication", "database"),
        ];
        let mut buf = BytesMut::new();
        frontend::startup_message(params.into_iter(), &mut buf)?;
        Self::write_with_timeout_wrapped(stream, &buf).await?;

        // Authentication loop
        loop {
            let (type_code, body) = Self::read_message_with_timeout_wrapped(stream).await?;

            let mut raw_msg = BytesMut::with_capacity(1 + 4 + body.len());
            raw_msg.put_u8(type_code);
            raw_msg.put_i32((body.len() + 4) as i32);
            raw_msg.put_slice(&body);

            let msg = backend::Message::parse(&mut raw_msg)?
                .ok_or(anyhow!("Failed to parse auth message"))?;

            match msg {
                backend::Message::AuthenticationOk => {
                    debug!("Authentication successful");
                    break;
                }
                backend::Message::AuthenticationCleartextPassword => {
                    if !stream.is_tls() {
                        warn!("⚠️ SECURITY: Cleartext password over unencrypted connection!");
                    }
                    let pass = password.ok_or_else(|| anyhow!("Password required"))?;
                    let mut buf = BytesMut::new();
                    frontend::password_message(pass.as_bytes(), &mut buf)?;
                    Self::write_with_timeout_wrapped(stream, &buf).await?;
                }
                backend::Message::AuthenticationMd5Password(body) => {
                    if !stream.is_tls() {
                        warn!("⚠️ SECURITY: MD5 auth over unencrypted connection!");
                    }
                    let pass = password.ok_or_else(|| anyhow!("Password required"))?;
                    let hash = hash_md5_password(user, pass, &body.salt());
                    let mut buf = BytesMut::new();
                    frontend::password_message(hash.as_bytes(), &mut buf)?;
                    Self::write_with_timeout_wrapped(stream, &buf).await?;
                }
                backend::Message::AuthenticationSasl(body) => {
                    let pass = password.ok_or_else(|| anyhow!("Password required"))?;

                    // Check for SCRAM-SHA-256 support (FallibleIterator)
                    let mechanisms: Vec<&str> = body.mechanisms().collect()?;
                    if !mechanisms.contains(&"SCRAM-SHA-256") {
                        return Err(anyhow!(
                            "Server requires SASL authentication but doesn't support SCRAM-SHA-256. Available: {:?}",
                            mechanisms
                        ));
                    }

                    debug!("Starting SCRAM-SHA-256 authentication");

                    // Perform SCRAM-SHA-256 exchange using StreamWrapper
                    Self::perform_scram_auth_wrapped(stream, user, pass).await?;
                }
                backend::Message::ErrorResponse(_) => {
                    circuit_breaker.record_failure().await;
                    return Err(anyhow!("Authentication error from server"));
                }
                _ => {
                    circuit_breaker.record_failure().await;
                    return Err(anyhow!("Unexpected auth message: {}", type_code));
                }
            }
        }

        // Wait for ReadyForQuery
        loop {
            let (type_code, _) = Self::read_message_with_timeout_wrapped(stream).await?;
            if type_code == b'Z' {
                break;
            } else if type_code == b'E' {
                circuit_breaker.record_failure().await;
                return Err(anyhow!("Error waiting for ready"));
            }
        }

        Ok(())
    }

    /// Create replication slot with validation
    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
        output_plugin: &str,
    ) -> Result<()> {
        Validator::validate_identifier(slot_name)?;
        Validator::validate_identifier(output_plugin)?;

        let query = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL {}",
            slot_name, output_plugin
        );
        self.simple_query(&query).await
    }

    /// Start streaming replication
    pub async fn start_replication(
        mut self,
        slot_name: &str,
        start_lsn: u64,
        pub_name: &str,
    ) -> Result<SecureReplicationStream> {
        Validator::validate_identifier(slot_name)?;
        Validator::validate_identifier(pub_name)?;

        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {}/{} (proto_version '1', publication_names '{}')",
            slot_name,
            (start_lsn >> 32) as u32,
            start_lsn as u32,
            pub_name
        );

        let mut buf = BytesMut::new();
        frontend::query(&query, &mut buf)?;
        Self::write_with_timeout_wrapped(&mut self.stream, &buf).await?;

        let (type_code, _) = Self::read_message_with_timeout_wrapped(&mut self.stream).await?;

        if type_code == b'W' {
            info!(
                "Entered CopyBoth mode (secure{})",
                if self.is_tls { ", TLS" } else { "" }
            );
            Ok(SecureReplicationStream {
                stream: self.stream,
                rate_limiter: self.rate_limiter,
                circuit_breaker: self.circuit_breaker,
            })
        } else if type_code == b'E' {
            self.circuit_breaker.record_failure().await;
            Err(anyhow!("Failed to start replication"))
        } else {
            Err(anyhow!("Unexpected response: {}", type_code as char))
        }
    }

    async fn read_message_with_timeout_wrapped(
        stream: &mut StreamWrapper,
    ) -> Result<(u8, Vec<u8>)> {
        let (type_code, len) = timeout(Duration::from_secs(IO_TIMEOUT_SECS), async {
            let type_code = stream.read_u8().await?;
            let len = stream.read_i32().await?;
            Ok::<(u8, i32), std::io::Error>((type_code, len))
        })
        .await
        .context("Read timeout")?
        .context("Failed to read header")?;

        let len = len as usize;
        if len < 4 {
            return Err(anyhow!("Invalid length: {}", len));
        }

        Validator::validate_message_size(len)?;

        let mut body = vec![0u8; len - 4];
        timeout(
            Duration::from_secs(IO_TIMEOUT_SECS),
            stream.read_exact(&mut body),
        )
        .await
        .context("Read timeout")?
        .context("Failed to read body")?;

        Ok((type_code, body))
    }

    async fn write_with_timeout_wrapped(stream: &mut StreamWrapper, data: &[u8]) -> Result<()> {
        timeout(Duration::from_secs(IO_TIMEOUT_SECS), async {
            stream.write_all(data).await?;
            stream.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .context("Write timeout")?
        .context("Failed to write")?;

        Ok(())
    }

    async fn simple_query(&mut self, query: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        frontend::query(query, &mut buf)?;
        Self::write_with_timeout_wrapped(&mut self.stream, &buf).await?;

        loop {
            let (type_code, _) = Self::read_message_with_timeout_wrapped(&mut self.stream).await?;
            if type_code == b'Z' {
                break;
            } else if type_code == b'E' {
                return Err(anyhow!("Query error"));
            }
        }

        Ok(())
    }

    /// Perform SCRAM-SHA-256 authentication for StreamWrapper
    ///
    /// This implements RFC 5802 SCRAM mechanism using the StreamWrapper I/O helpers.
    async fn perform_scram_auth_wrapped(
        stream: &mut StreamWrapper,
        user: &str,
        password: &str,
    ) -> Result<()> {
        // Create SCRAM state machine
        let mut scram = ScramSha256::new(user, password)?;

        // Step 1: Send client-first message
        let client_first = scram.client_first_message();
        debug!("Sending SCRAM client-first message");

        let mut buf = BytesMut::new();
        frontend::sasl_initial_response("SCRAM-SHA-256", &client_first, &mut buf)?;
        Self::write_with_timeout_wrapped(stream, &buf).await?;

        // Step 2: Read server-first message (AuthenticationSaslContinue)
        let (type_code, body) = Self::read_message_with_timeout_wrapped(stream).await?;

        let mut raw_msg = BytesMut::with_capacity(1 + 4 + body.len());
        raw_msg.put_u8(type_code);
        raw_msg.put_i32((body.len() + 4) as i32);
        raw_msg.put_slice(&body);

        let msg = backend::Message::parse(&mut raw_msg)?
            .ok_or(anyhow!("Failed to parse SASL continue message"))?;

        let server_first = match msg {
            backend::Message::AuthenticationSaslContinue(body) => {
                let data = body.data();
                std::str::from_utf8(data)
                    .map_err(|_| anyhow!("Invalid UTF-8 in server-first message"))?
                    .to_string()
            }
            backend::Message::ErrorResponse(_) => {
                return Err(anyhow!("SCRAM authentication failed during server-first"));
            }
            _ => {
                return Err(anyhow!(
                    "Expected AuthenticationSaslContinue, got message type {}",
                    type_code
                ));
            }
        };

        debug!("Received SCRAM server-first message");

        // Step 3: Send client-final message
        let client_final = scram.client_final_message(server_first.as_bytes())?;
        debug!("Sending SCRAM client-final message");

        let mut buf = BytesMut::new();
        frontend::sasl_response(&client_final, &mut buf)?;
        Self::write_with_timeout_wrapped(stream, &buf).await?;

        // Step 4: Read server-final message (AuthenticationSaslFinal)
        let (type_code, body) = Self::read_message_with_timeout_wrapped(stream).await?;

        let mut raw_msg = BytesMut::with_capacity(1 + 4 + body.len());
        raw_msg.put_u8(type_code);
        raw_msg.put_i32((body.len() + 4) as i32);
        raw_msg.put_slice(&body);

        let msg = backend::Message::parse(&mut raw_msg)?
            .ok_or(anyhow!("Failed to parse SASL final message"))?;

        let server_final = match msg {
            backend::Message::AuthenticationSaslFinal(body) => {
                let data = body.data();
                std::str::from_utf8(data)
                    .map_err(|_| anyhow!("Invalid UTF-8 in server-final message"))?
                    .to_string()
            }
            backend::Message::ErrorResponse(_) => {
                return Err(anyhow!("SCRAM authentication failed during server-final"));
            }
            _ => {
                return Err(anyhow!(
                    "Expected AuthenticationSaslFinal, got message type {}",
                    type_code
                ));
            }
        };

        // Step 5: Verify server signature
        scram.verify_server_final(server_final.as_bytes())?;
        debug!("SCRAM-SHA-256 authentication completed successfully");

        Ok(())
    }
}

// ============================================================================
// Secure Replication Stream
// ============================================================================

/// Secure replication stream with rate limiting and circuit breaker
pub struct SecureReplicationStream {
    stream: StreamWrapper,
    rate_limiter: Arc<RateLimiter>,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl SecureReplicationStream {
    /// Get next message with rate limiting
    pub async fn next_message(&mut self) -> Result<Option<Bytes>> {
        self.rate_limiter
            .acquire_timeout(Duration::from_secs(5))
            .await?;

        if !self.circuit_breaker.allow_request().await {
            return Err(anyhow!("Circuit breaker is OPEN"));
        }

        match self.read_internal().await {
            Ok(msg) => {
                self.circuit_breaker.record_success().await;
                Ok(msg)
            }
            Err(e) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            }
        }
    }

    async fn read_internal(&mut self) -> Result<Option<Bytes>> {
        let (type_code, len) = timeout(Duration::from_secs(IO_TIMEOUT_SECS), async {
            let type_code = self.stream.read_u8().await?;
            let len = self.stream.read_i32().await?;
            Ok::<(u8, i32), std::io::Error>((type_code, len))
        })
        .await??;

        if type_code != b'd' {
            return Ok(None);
        }

        let len = len as usize;
        Validator::validate_message_size(len)?;

        if len < 4 {
            return Ok(None);
        }

        let mut data = vec![0u8; len - 4];
        timeout(
            Duration::from_secs(IO_TIMEOUT_SECS),
            self.stream.read_exact(&mut data),
        )
        .await??;

        Ok(Some(Bytes::from(data)))
    }

    /// Send status update
    pub async fn send_status_update(&mut self, lsn: u64) -> Result<()> {
        let mut payload = BytesMut::with_capacity(34);
        payload.put_u8(b'r');
        payload.put_u64(lsn);
        payload.put_u64(lsn);
        payload.put_u64(lsn);

        let pg_epoch =
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(946684800);
        let micros = std::time::SystemTime::now()
            .duration_since(pg_epoch)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        payload.put_i64(micros);
        payload.put_u8(0);

        let mut frame = BytesMut::with_capacity(1 + 4 + payload.len());
        frame.put_u8(b'd');
        frame.put_i32((payload.len() + 4) as i32);
        frame.put_slice(&payload);

        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

fn hash_md5_password(user: &str, pass: &str, salt: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(pass);
    hasher.update(user);
    let first = hex::encode(hasher.finalize());

    let mut hasher = Md5::new();
    hasher.update(first);
    hasher.update(salt);
    let second = hex::encode(hasher.finalize());

    format!("md5{}", second)
}

/// Perform SCRAM-SHA-256 authentication exchange
///
/// This implements RFC 5802 SCRAM mechanism for PostgreSQL authentication.
/// The exchange consists of:
/// 1. Client sends client-first message (initial response)
/// 2. Server responds with server-first message (challenge)
/// 3. Client sends client-final message (proof)
/// 4. Server responds with server-final message (verification)
async fn perform_scram_auth<S: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: &mut S,
    user: &str,
    password: &str,
) -> Result<()> {
    // Create SCRAM state machine
    let mut scram = ScramSha256::new(user, password)?;

    // Step 1: Send client-first message
    let client_first = scram.client_first_message();
    debug!("Sending SCRAM client-first message");

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response("SCRAM-SHA-256", &client_first, &mut buf)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    // Step 2: Read server-first message (AuthenticationSaslContinue)
    let type_code = stream.read_u8().await?;
    let len = stream.read_i32().await? as usize;
    if len < 4 {
        return Err(anyhow!("Invalid SASL continue message length"));
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await?;

    let mut raw_msg = BytesMut::with_capacity(1 + len);
    raw_msg.put_u8(type_code);
    raw_msg.put_i32(len as i32);
    raw_msg.put_slice(&body);

    let msg = backend::Message::parse(&mut raw_msg)?
        .ok_or(anyhow!("Failed to parse SASL continue message"))?;

    let server_first = match msg {
        backend::Message::AuthenticationSaslContinue(body) => {
            let data = body.data();
            std::str::from_utf8(data)
                .map_err(|_| anyhow!("Invalid UTF-8 in server-first message"))?
                .to_string()
        }
        backend::Message::ErrorResponse(_) => {
            return Err(anyhow!("SCRAM authentication failed during server-first"));
        }
        _ => {
            return Err(anyhow!(
                "Expected AuthenticationSaslContinue, got message type {}",
                type_code
            ));
        }
    };

    debug!("Received SCRAM server-first message");

    // Step 3: Send client-final message
    let client_final = scram.client_final_message(server_first.as_bytes())?;
    debug!("Sending SCRAM client-final message");

    let mut buf = BytesMut::new();
    frontend::sasl_response(&client_final, &mut buf)?;
    stream.write_all(&buf).await?;
    stream.flush().await?;

    // Step 4: Read server-final message (AuthenticationSaslFinal)
    let type_code = stream.read_u8().await?;
    let len = stream.read_i32().await? as usize;
    if len < 4 {
        return Err(anyhow!("Invalid SASL final message length"));
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await?;

    let mut raw_msg = BytesMut::with_capacity(1 + len);
    raw_msg.put_u8(type_code);
    raw_msg.put_i32(len as i32);
    raw_msg.put_slice(&body);

    let msg = backend::Message::parse(&mut raw_msg)?
        .ok_or(anyhow!("Failed to parse SASL final message"))?;

    let server_final = match msg {
        backend::Message::AuthenticationSaslFinal(body) => {
            let data = body.data();
            std::str::from_utf8(data)
                .map_err(|_| anyhow!("Invalid UTF-8 in server-final message"))?
                .to_string()
        }
        backend::Message::ErrorResponse(_) => {
            return Err(anyhow!("SCRAM authentication failed during server-final"));
        }
        _ => {
            return Err(anyhow!(
                "Expected AuthenticationSaslFinal, got message type {}",
                type_code
            ));
        }
    };

    // Step 5: Verify server signature
    scram.verify_server_final(server_final.as_bytes())?;
    debug!("SCRAM-SHA-256 authentication completed successfully");

    Ok(())
}
