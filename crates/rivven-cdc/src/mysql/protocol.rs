//! MySQL binary log protocol implementation
//!
//! Implements the MySQL replication protocol for CDC:
//! - Handshake and authentication (mysql_native_password, caching_sha2_password, ed25519)
//! - Full caching_sha2_password support with RSA public key encryption
//! - MariaDB client_ed25519 authentication
//! - TLS/SSL encryption support
//! - COM_REGISTER_SLAVE
//! - COM_BINLOG_DUMP / COM_BINLOG_DUMP_GTID
//! - Binlog event streaming
//!
//! ## Authentication
//!
//! ### mysql_native_password (MySQL 5.x default)
//! Uses SHA1 hashing: `SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))`
//!
//! ### caching_sha2_password (MySQL 8.0+ default)
//! Uses SHA256 hashing with two authentication paths:
//! - **Fast auth**: Server has cached password hash, uses scramble verification
//! - **Full auth**: Server needs complete password, sent via:
//!   - TLS: Password sent in cleartext over encrypted connection
//!   - RSA: Password XORed with nonce, encrypted with server's public key
//!
//! ### client_ed25519 (MariaDB)
//! Uses Ed25519 digital signatures:
//! - Derives Ed25519 keypair from SHA-512(password)
//! - Signs the server's random nonce
//! - Sends 64-byte signature

use anyhow::{bail, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use rand::rngs::OsRng;
use rsa::{BigUint, Pkcs1v15Encrypt, RsaPublicKey};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::io::Read;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, trace, warn};

#[cfg(feature = "mysql-tls")]
use std::sync::Arc;

use crate::common::{Validator, CONNECTION_TIMEOUT_SECS};

#[cfg(feature = "mysql-tls")]
use crate::common::{tls::build_rustls_config, TlsConfig};
#[cfg(feature = "mysql-tls")]
use rustls::pki_types::ServerName;
#[cfg(feature = "mysql-tls")]
use tokio_rustls::{client::TlsStream, TlsConnector};

/// MySQL packet header size (4 bytes: 3 for length + 1 for sequence)
const PACKET_HEADER_SIZE: usize = 4;
/// Maximum packet payload
const MAX_PACKET_SIZE: usize = 16_777_215;

/// MySQL capability flags for protocol negotiation.
///
/// These flags are exchanged during the handshake to negotiate
/// protocol features between client and server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityFlags(u32);

impl CapabilityFlags {
    pub const CLIENT_LONG_PASSWORD: u32 = 0x00000001;
    pub const CLIENT_FOUND_ROWS: u32 = 0x00000002;
    pub const CLIENT_LONG_FLAG: u32 = 0x00000004;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;
    pub const CLIENT_NO_SCHEMA: u32 = 0x00000010;
    pub const CLIENT_COMPRESS: u32 = 0x00000020;
    pub const CLIENT_ODBC: u32 = 0x00000040;
    pub const CLIENT_LOCAL_FILES: u32 = 0x00000080;
    pub const CLIENT_IGNORE_SPACE: u32 = 0x00000100;
    pub const CLIENT_PROTOCOL_41: u32 = 0x00000200;
    pub const CLIENT_INTERACTIVE: u32 = 0x00000400;
    pub const CLIENT_SSL: u32 = 0x00000800;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 0x00001000;
    pub const CLIENT_TRANSACTIONS: u32 = 0x00002000;
    pub const CLIENT_RESERVED: u32 = 0x00004000;
    pub const CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 0x00010000;
    pub const CLIENT_MULTI_RESULTS: u32 = 0x00020000;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x00040000;
    pub const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
    pub const CLIENT_CONNECT_ATTRS: u32 = 0x00100000;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x00200000;
    pub const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

    pub fn new(flags: u32) -> Self {
        Self(flags)
    }

    /// Check if a capability flag is set.
    pub fn has(&self, flag: u32) -> bool {
        (self.0 & flag) != 0
    }

    /// Get the raw capability flags value.
    pub fn value(&self) -> u32 {
        self.0
    }

    /// Set a capability flag.
    pub fn set(&mut self, flag: u32) {
        self.0 |= flag;
    }

    /// Clear a capability flag.
    pub fn clear(&mut self, flag: u32) {
        self.0 &= !flag;
    }

    /// Compute intersection with another set of flags.
    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0 & other.0)
    }

    /// Check if SSL/TLS is supported.
    pub fn supports_ssl(&self) -> bool {
        self.has(Self::CLIENT_SSL)
    }

    /// Check if plugin authentication is supported.
    pub fn supports_plugin_auth(&self) -> bool {
        self.has(Self::CLIENT_PLUGIN_AUTH)
    }

    /// Check if secure connection (MySQL 4.1+) is supported.
    pub fn supports_secure_connection(&self) -> bool {
        self.has(Self::CLIENT_SECURE_CONNECTION)
    }

    /// Check if protocol 4.1 is supported.
    pub fn supports_protocol_41(&self) -> bool {
        self.has(Self::CLIENT_PROTOCOL_41)
    }

    /// Check if connection attributes are supported.
    pub fn supports_connect_attrs(&self) -> bool {
        self.has(Self::CLIENT_CONNECT_ATTRS)
    }

    /// Get default client capabilities for CDC connections.
    pub fn default_client() -> Self {
        Self(
            Self::CLIENT_LONG_PASSWORD
                | Self::CLIENT_LONG_FLAG
                | Self::CLIENT_PROTOCOL_41
                | Self::CLIENT_TRANSACTIONS
                | Self::CLIENT_SECURE_CONNECTION
                | Self::CLIENT_PLUGIN_AUTH
                | Self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
        )
    }
}

// ============================================================================
// Stream Wrapper for TLS Support
// ============================================================================

/// Wrapper for handling both plain TCP and TLS streams
pub enum MysqlStreamWrapper {
    /// Plain TCP connection (no encryption)
    Plain(BufReader<TcpStream>),
    /// TLS-encrypted connection (boxed to avoid large enum variant)
    #[cfg(feature = "mysql-tls")]
    Tls(Box<BufReader<TlsStream<TcpStream>>>),
}

impl MysqlStreamWrapper {
    /// Read exactly n bytes into buffer
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            MysqlStreamWrapper::Plain(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
            #[cfg(feature = "mysql-tls")]
            MysqlStreamWrapper::Tls(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
        }
    }

    /// Write all bytes
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            MysqlStreamWrapper::Plain(s) => s.get_mut().write_all(buf).await,
            #[cfg(feature = "mysql-tls")]
            MysqlStreamWrapper::Tls(s) => s.get_mut().write_all(buf).await,
        }
    }

    /// Flush the stream
    pub async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            MysqlStreamWrapper::Plain(s) => s.get_mut().flush().await,
            #[cfg(feature = "mysql-tls")]
            MysqlStreamWrapper::Tls(s) => s.get_mut().flush().await,
        }
    }

    /// Check if this is a TLS stream
    pub fn is_tls(&self) -> bool {
        match self {
            MysqlStreamWrapper::Plain(_) => false,
            #[cfg(feature = "mysql-tls")]
            MysqlStreamWrapper::Tls(_) => true,
        }
    }
}

// ============================================================================
// Handshake Packet
// ============================================================================

/// MySQL handshake packet (initial greeting from server)
#[derive(Debug)]
pub struct HandshakePacket {
    pub protocol_version: u8,
    pub server_version: String,
    pub connection_id: u32,
    pub auth_plugin_data_part1: Vec<u8>,
    pub capability_flags: CapabilityFlags,
    pub character_set: u8,
    pub status_flags: u16,
    pub auth_plugin_data_part2: Vec<u8>,
    pub auth_plugin_name: String,
}

impl HandshakePacket {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(data);

        // Protocol version
        let mut buf = [0u8; 1];
        Read::read_exact(&mut cursor, &mut buf)?;
        let protocol_version = buf[0];

        // Server version (null-terminated string)
        let mut server_version = Vec::new();
        loop {
            Read::read_exact(&mut cursor, &mut buf)?;
            if buf[0] == 0 {
                break;
            }
            server_version.push(buf[0]);
        }
        let server_version = String::from_utf8_lossy(&server_version).to_string();

        // Connection ID (4 bytes)
        let mut buf4 = [0u8; 4];
        Read::read_exact(&mut cursor, &mut buf4)?;
        let connection_id = u32::from_le_bytes(buf4);

        // Auth-plugin-data-part-1 (8 bytes)
        let mut auth_plugin_data_part1 = vec![0u8; 8];
        Read::read_exact(&mut cursor, &mut auth_plugin_data_part1)?;

        // Filler
        Read::read_exact(&mut cursor, &mut buf)?;

        // Capability flags (lower 2 bytes)
        let mut buf2 = [0u8; 2];
        Read::read_exact(&mut cursor, &mut buf2)?;
        let cap_lower = u16::from_le_bytes(buf2);

        // Character set
        Read::read_exact(&mut cursor, &mut buf)?;
        let character_set = buf[0];

        // Status flags
        Read::read_exact(&mut cursor, &mut buf2)?;
        let status_flags = u16::from_le_bytes(buf2);

        // Capability flags (upper 2 bytes)
        Read::read_exact(&mut cursor, &mut buf2)?;
        let cap_upper = u16::from_le_bytes(buf2);
        let capability_flags =
            CapabilityFlags::new(((cap_upper as u32) << 16) | (cap_lower as u32));

        // Auth plugin data length
        Read::read_exact(&mut cursor, &mut buf)?;
        let auth_data_len = buf[0] as usize;

        // Reserved (10 bytes)
        let mut reserved = [0u8; 10];
        Read::read_exact(&mut cursor, &mut reserved)?;

        // Auth-plugin-data-part-2 (max 13 bytes, null-terminated)
        let remaining_len = if auth_data_len > 8 {
            auth_data_len - 8
        } else {
            13
        };
        let mut auth_plugin_data_part2 = vec![0u8; remaining_len];
        Read::read_exact(&mut cursor, &mut auth_plugin_data_part2)?;
        // Remove trailing null if present
        if let Some(pos) = auth_plugin_data_part2.iter().position(|&b| b == 0) {
            auth_plugin_data_part2.truncate(pos);
        }

        // Auth plugin name (null-terminated)
        let mut auth_plugin_name = Vec::new();
        if capability_flags.has(CapabilityFlags::CLIENT_PLUGIN_AUTH) {
            loop {
                let n = Read::read(&mut cursor, &mut buf)?;
                if n == 0 || buf[0] == 0 {
                    break;
                }
                auth_plugin_name.push(buf[0]);
            }
        }
        let auth_plugin_name = String::from_utf8_lossy(&auth_plugin_name).to_string();

        Ok(Self {
            protocol_version,
            server_version,
            connection_id,
            auth_plugin_data_part1,
            capability_flags,
            character_set,
            status_flags,
            auth_plugin_data_part2,
            auth_plugin_name,
        })
    }

    /// Get full auth data (salt)
    pub fn auth_data(&self) -> Vec<u8> {
        let mut data = self.auth_plugin_data_part1.clone();
        data.extend_from_slice(&self.auth_plugin_data_part2);
        data
    }
}

// ============================================================================
// MySQL Binlog Client
// ============================================================================

/// MySQL binlog client for replication
pub struct MySqlBinlogClient {
    stream: MysqlStreamWrapper,
    sequence_id: u8,
    server_version: String,
    connection_id: u32,
    is_tls: bool,
}

impl std::fmt::Debug for MySqlBinlogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlBinlogClient")
            .field("sequence_id", &self.sequence_id)
            .field("server_version", &self.server_version)
            .field("connection_id", &self.connection_id)
            .field("is_tls", &self.is_tls)
            .finish_non_exhaustive()
    }
}

impl MySqlBinlogClient {
    /// Connect to MySQL server and authenticate (no TLS)
    ///
    /// For TLS connections, use `connect_with_tls()` instead.
    ///
    /// # Security
    ///
    /// - Validates user identifier to prevent injection attacks
    /// - Applies connection timeout to prevent hanging connections
    /// - Applies I/O timeouts for all read/write operations
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: Option<&str>,
        database: Option<&str>,
    ) -> Result<Self> {
        // Security: Validate identifier
        Validator::validate_identifier(user)?;
        if let Some(db) = database {
            Validator::validate_identifier(db)?;
        }

        let addr = format!("{}:{}", host, port);
        info!("Connecting to MySQL at {} (no TLS)", addr);

        // Security: Apply connection timeout
        let tcp_stream = Self::connect_tcp(&addr).await?;
        let mut stream = MysqlStreamWrapper::Plain(BufReader::new(tcp_stream));

        // Read handshake packet
        let (handshake_data, seq) = Self::read_packet_wrapped(&mut stream, 0).await?;
        let handshake =
            HandshakePacket::parse(&handshake_data).context("Failed to parse handshake packet")?;

        info!(
            "Connected to MySQL {} (connection_id={})",
            handshake.server_version, handshake.connection_id
        );
        debug!("Auth plugin: {}", handshake.auth_plugin_name);

        let mut client = Self {
            stream,
            sequence_id: seq,
            server_version: handshake.server_version.clone(),
            connection_id: handshake.connection_id,
            is_tls: false,
        };

        // Authenticate
        client
            .authenticate(user, password, database, &handshake)
            .await?;

        Ok(client)
    }

    /// Connect to MySQL server with TLS encryption
    ///
    /// This method:
    /// 1. Establishes TCP connection
    /// 2. Receives handshake from server
    /// 3. Sends SSL Request packet
    /// 4. Upgrades to TLS
    /// 5. Performs authentication over encrypted channel
    #[cfg(feature = "mysql-tls")]
    pub async fn connect_with_tls(
        host: &str,
        port: u16,
        user: &str,
        password: Option<&str>,
        database: Option<&str>,
        tls_config: &TlsConfig,
    ) -> Result<Self> {
        // Security: Validate identifier
        Validator::validate_identifier(user)?;
        if let Some(db) = database {
            Validator::validate_identifier(db)?;
        }

        // Validate TLS config
        tls_config
            .validate()
            .map_err(|e| anyhow::anyhow!("Invalid TLS config: {}", e))?;

        // If TLS is disabled, fall back to plain connection
        if !tls_config.is_enabled() {
            info!("TLS disabled, connecting to MySQL without encryption");
            return Self::connect(host, port, user, password, database).await;
        }

        let addr = format!("{}:{}", host, port);
        info!(
            "Connecting to MySQL at {} (TLS mode: {})",
            addr, tls_config.mode
        );

        // Connect TCP
        let tcp_stream = Self::connect_tcp(&addr).await?;
        let mut plain_stream = BufReader::new(tcp_stream);

        // Read handshake packet
        let (handshake_data, seq) = Self::read_packet_plain(&mut plain_stream, 0).await?;
        let handshake =
            HandshakePacket::parse(&handshake_data).context("Failed to parse handshake packet")?;

        info!(
            "Connected to MySQL {} (connection_id={})",
            handshake.server_version, handshake.connection_id
        );

        // Check if server supports SSL
        if !handshake.capability_flags.has(CapabilityFlags::CLIENT_SSL) {
            if tls_config.is_required() {
                bail!(
                    "Server does not support SSL but TLS mode '{}' requires it",
                    tls_config.mode
                );
            }
            warn!("Server does not support SSL, falling back to plain connection");
            let mut client = Self {
                stream: MysqlStreamWrapper::Plain(plain_stream),
                sequence_id: seq,
                server_version: handshake.server_version.clone(),
                connection_id: handshake.connection_id,
                is_tls: false,
            };
            client
                .authenticate(user, password, database, &handshake)
                .await?;
            return Ok(client);
        }

        // Send SSL Request packet
        let ssl_request = Self::build_ssl_request(database.is_some());
        Self::write_packet_plain(&mut plain_stream, &ssl_request, seq).await?;

        // Upgrade to TLS
        trace!("Upgrading MySQL connection to TLS");
        let tcp_stream = plain_stream.into_inner();
        let tls_stream = Self::upgrade_to_tls(tcp_stream, host, tls_config).await?;
        let stream = MysqlStreamWrapper::Tls(Box::new(BufReader::new(tls_stream)));

        info!("✓ MySQL TLS connection established");

        let mut client = Self {
            stream,
            sequence_id: seq + 1,
            server_version: handshake.server_version.clone(),
            connection_id: handshake.connection_id,
            is_tls: true,
        };

        // Authenticate over TLS
        client
            .authenticate(user, password, database, &handshake)
            .await?;

        Ok(client)
    }

    /// Check if the connection is using TLS
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    /// Connect TCP with timeout
    async fn connect_tcp(addr: &str) -> Result<TcpStream> {
        match timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(stream)) => Ok(stream),
            Ok(Err(e)) => Err(e).context("Failed to connect to MySQL server"),
            Err(_) => bail!(
                "Connection timeout after {}s connecting to MySQL",
                CONNECTION_TIMEOUT_SECS
            ),
        }
    }

    /// Build SSL Request packet
    #[cfg(feature = "mysql-tls")]
    fn build_ssl_request(with_db: bool) -> Vec<u8> {
        let mut request = BytesMut::with_capacity(32);

        // Client capabilities with SSL flag
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_DEPRECATE_EOF
            | CapabilityFlags::CLIENT_SSL;

        if with_db {
            client_flags |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        }

        // Client flags (4 bytes)
        request.put_u32_le(client_flags);
        // Max packet size (4 bytes)
        request.put_u32_le(MAX_PACKET_SIZE as u32);
        // Character set (1 byte) - utf8mb4 = 45
        request.put_u8(45);
        // Reserved (23 bytes)
        request.put_slice(&[0u8; 23]);

        request.to_vec()
    }

    /// Upgrade connection to TLS
    #[cfg(feature = "mysql-tls")]
    async fn upgrade_to_tls(
        tcp_stream: TcpStream,
        host: &str,
        tls_config: &TlsConfig,
    ) -> Result<TlsStream<TcpStream>> {
        let rustls_config = build_rustls_config(tls_config)?;
        let connector = TlsConnector::from(Arc::new(rustls_config));

        let server_name = tls_config
            .server_name
            .as_deref()
            .unwrap_or(host)
            .to_string();

        let server_name = ServerName::try_from(server_name.clone())
            .map_err(|_| anyhow::anyhow!("Invalid server name for TLS: {}", server_name))?;

        let tls_stream = timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            connector.connect(server_name, tcp_stream),
        )
        .await
        .context("TLS handshake timeout")?
        .context("TLS handshake failed")?;

        Ok(tls_stream)
    }

    /// Read a MySQL packet (plain stream version for TLS upgrade)
    #[cfg(feature = "mysql-tls")]
    async fn read_packet_plain(
        stream: &mut BufReader<TcpStream>,
        _seq: u8,
    ) -> Result<(Vec<u8>, u8)> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let sequence_id = header[3];

        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;

        Ok((payload, sequence_id.wrapping_add(1)))
    }

    /// Write a MySQL packet (plain stream version for TLS upgrade)
    #[cfg(feature = "mysql-tls")]
    async fn write_packet_plain(
        stream: &mut BufReader<TcpStream>,
        data: &[u8],
        seq: u8,
    ) -> Result<()> {
        let len = data.len();
        if len > MAX_PACKET_SIZE {
            bail!("Packet too large: {} bytes", len);
        }

        let mut packet = Vec::with_capacity(PACKET_HEADER_SIZE + len);
        packet.push((len & 0xFF) as u8);
        packet.push(((len >> 8) & 0xFF) as u8);
        packet.push(((len >> 16) & 0xFF) as u8);
        packet.push(seq);
        packet.extend_from_slice(data);

        stream.get_mut().write_all(&packet).await?;
        stream.get_mut().flush().await?;

        Ok(())
    }

    /// Read a MySQL packet using stream wrapper
    async fn read_packet_wrapped(
        stream: &mut MysqlStreamWrapper,
        _seq: u8,
    ) -> Result<(Vec<u8>, u8)> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let sequence_id = header[3];

        let mut payload = vec![0u8; payload_len];
        stream.read_exact(&mut payload).await?;

        Ok((payload, sequence_id.wrapping_add(1)))
    }

    /// Read a MySQL packet with I/O timeout.
    ///
    /// Uses `IO_TIMEOUT_SECS` to prevent indefinite hangs when the MySQL
    /// server stalls or drops the connection without sending a TCP RST.
    async fn read_packet(&mut self) -> Result<Vec<u8>> {
        use crate::common::IO_TIMEOUT_SECS;

        let mut header = [0u8; 4];
        tokio::time::timeout(
            std::time::Duration::from_secs(IO_TIMEOUT_SECS),
            self.stream.read_exact(&mut header),
        )
        .await
        .map_err(|_| anyhow::anyhow!("MySQL read timed out after {}s", IO_TIMEOUT_SECS))??;

        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        self.sequence_id = header[3].wrapping_add(1);

        let mut payload = vec![0u8; payload_len];
        tokio::time::timeout(
            std::time::Duration::from_secs(IO_TIMEOUT_SECS),
            self.stream.read_exact(&mut payload),
        )
        .await
        .map_err(|_| anyhow::anyhow!("MySQL read timed out after {}s", IO_TIMEOUT_SECS))??;

        Ok(payload)
    }

    /// Write a MySQL packet with I/O timeout.
    async fn write_packet(&mut self, data: &[u8]) -> Result<()> {
        use crate::common::IO_TIMEOUT_SECS;

        let len = data.len();
        if len > MAX_PACKET_SIZE {
            bail!("Packet too large: {} bytes", len);
        }

        let mut packet = Vec::with_capacity(PACKET_HEADER_SIZE + len);
        packet.push((len & 0xFF) as u8);
        packet.push(((len >> 8) & 0xFF) as u8);
        packet.push(((len >> 16) & 0xFF) as u8);
        packet.push(self.sequence_id);
        packet.extend_from_slice(data);

        tokio::time::timeout(std::time::Duration::from_secs(IO_TIMEOUT_SECS), async {
            self.stream.write_all(&packet).await?;
            self.stream.flush().await?;
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("MySQL write timed out after {}s", IO_TIMEOUT_SECS))??;

        self.sequence_id = self.sequence_id.wrapping_add(1);

        Ok(())
    }

    /// Authenticate with the server
    async fn authenticate(
        &mut self,
        user: &str,
        password: Option<&str>,
        database: Option<&str>,
        handshake: &HandshakePacket,
    ) -> Result<()> {
        // Build client capabilities
        let mut client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_DEPRECATE_EOF;

        if database.is_some() {
            client_flags |= CapabilityFlags::CLIENT_CONNECT_WITH_DB;
        }

        // Compute auth response
        let auth_response = match handshake.auth_plugin_name.as_str() {
            "mysql_native_password" => {
                Self::mysql_native_password(password, &handshake.auth_data())
            }
            "caching_sha2_password" => {
                Self::caching_sha2_password(password, &handshake.auth_data())
            }
            "client_ed25519" => Self::client_ed25519(password, &handshake.auth_data()),
            other => {
                warn!(
                    "Unknown auth plugin: {}, trying mysql_native_password",
                    other
                );
                Self::mysql_native_password(password, &handshake.auth_data())
            }
        };

        // Build handshake response
        let mut response = BytesMut::with_capacity(256);

        // Client flags (4 bytes)
        response.put_u32_le(client_flags);

        // Max packet size (4 bytes)
        response.put_u32_le(MAX_PACKET_SIZE as u32);

        // Character set (1 byte) - utf8mb4 = 45
        response.put_u8(45);

        // Reserved (23 bytes)
        response.put_slice(&[0u8; 23]);

        // Username (null-terminated)
        response.put_slice(user.as_bytes());
        response.put_u8(0);

        // Auth response (length-encoded)
        response.put_u8(auth_response.len() as u8);
        response.put_slice(&auth_response);

        // Database (if specified, null-terminated)
        if let Some(db) = database {
            response.put_slice(db.as_bytes());
            response.put_u8(0);
        }

        // Auth plugin name (null-terminated)
        response.put_slice(handshake.auth_plugin_name.as_bytes());
        response.put_u8(0);

        // Send handshake response
        self.write_packet(&response).await?;

        // Read response
        let resp = self.read_packet().await?;
        let auth_plugin = handshake.auth_plugin_name.as_str();

        match resp.first() {
            Some(0x00) => {
                debug!("Authentication successful");
                Ok(())
            }
            Some(0x01)
                if auth_plugin == "caching_sha2_password" || auth_plugin == "sha256_password" =>
            {
                // caching_sha2_password may require full authentication
                debug!("caching_sha2_password: received auth continuation");
                self.handle_caching_sha2_response(&resp, &handshake.auth_data(), password)
                    .await
            }
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([resp[1], resp[2]]);
                let err_msg = String::from_utf8_lossy(&resp[9..]);
                bail!("Authentication failed: {} - {}", err_code, err_msg);
            }
            Some(0xFE) => {
                // Auth switch request
                let plugin_name_end = resp[1..]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(resp.len() - 1);
                let plugin_name =
                    String::from_utf8_lossy(&resp[1..1 + plugin_name_end]).to_string();
                let auth_data = resp[2 + plugin_name_end..].to_vec();

                debug!("Auth switch to plugin: {}", plugin_name);
                self.handle_auth_switch(&plugin_name, &auth_data, password)
                    .await
            }
            Some(other) => {
                bail!("Unexpected auth response: 0x{:02X}", other);
            }
            None => {
                bail!("Empty auth response");
            }
        }
    }

    /// Handle auth switch request
    async fn handle_auth_switch(
        &mut self,
        plugin: &str,
        auth_data: &[u8],
        password: Option<&str>,
    ) -> Result<()> {
        let auth_response = match plugin {
            "mysql_native_password" => Self::mysql_native_password(password, auth_data),
            "caching_sha2_password" => Self::caching_sha2_password(password, auth_data),
            "sha256_password" => Self::caching_sha2_password(password, auth_data),
            "client_ed25519" => Self::client_ed25519(password, auth_data),
            _ => bail!("Unsupported auth plugin for switch: {}", plugin),
        };

        self.write_packet(&auth_response).await?;

        let resp = self.read_packet().await?;
        match resp.first() {
            Some(0x00) => Ok(()),
            Some(0x01) if plugin == "caching_sha2_password" || plugin == "sha256_password" => {
                // caching_sha2_password response
                self.handle_caching_sha2_response(&resp, auth_data, password)
                    .await
            }
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([resp[1], resp[2]]);
                let err_msg = String::from_utf8_lossy(&resp[9..]);
                bail!("Auth switch failed: {} - {}", err_code, err_msg);
            }
            _ => bail!("Unexpected auth switch response"),
        }
    }

    /// Handle caching_sha2_password protocol response
    ///
    /// After sending the scrambled password, the server responds with:
    /// - 0x01 0x03: Fast auth succeeded (password was cached)
    /// - 0x01 0x04: Need full authentication
    ///
    /// For full auth, we need to send the password:
    /// - Over TLS: Send password + null terminator in cleartext
    /// - Without TLS: Request server's RSA public key, encrypt password with it
    async fn handle_caching_sha2_response(
        &mut self,
        resp: &[u8],
        nonce: &[u8],
        password: Option<&str>,
    ) -> Result<()> {
        if resp.len() < 2 {
            bail!("Invalid caching_sha2_password response: too short");
        }

        match resp[1] {
            0x03 => {
                // Fast auth success - password was cached
                debug!("caching_sha2_password: fast auth succeeded");
                Ok(())
            }
            0x04 => {
                // Full auth required - need to send password securely
                debug!("caching_sha2_password: full authentication required");
                self.caching_sha2_full_auth(nonce, password).await
            }
            other => {
                bail!(
                    "Unknown caching_sha2_password response type: 0x{:02X}",
                    other
                );
            }
        }
    }

    /// Perform full caching_sha2_password authentication
    ///
    /// This is called when the server doesn't have the password cached and
    /// requires the actual password to be sent.
    async fn caching_sha2_full_auth(&mut self, nonce: &[u8], password: Option<&str>) -> Result<()> {
        let pwd = password.unwrap_or("");

        if self.is_tls {
            // Over TLS: Send password in cleartext (TLS provides encryption)
            debug!("caching_sha2_password: sending password over TLS");
            let mut auth_data = pwd.as_bytes().to_vec();
            auth_data.push(0); // Null terminator required
            self.write_packet(&auth_data).await?;
        } else {
            // Without TLS: Use RSA public key encryption
            debug!("caching_sha2_password: requesting server's RSA public key");

            // Request public key (send 0x02)
            self.write_packet(&[0x02]).await?;

            // Read public key response
            let pk_resp = self.read_packet().await?;
            if pk_resp.is_empty() {
                bail!("Empty public key response");
            }

            match pk_resp[0] {
                0x01 => {
                    // Public key data follows
                    let pem_data = &pk_resp[1..];
                    let public_key_pem = String::from_utf8_lossy(pem_data);
                    debug!("Received server's RSA public key");

                    // Encrypt password with RSA public key
                    let encrypted = self
                        .rsa_encrypt_password(pwd, nonce, &public_key_pem)
                        .context("Failed to encrypt password with RSA")?;

                    self.write_packet(&encrypted).await?;
                }
                0xFF => {
                    let err_code = u16::from_le_bytes([pk_resp[1], pk_resp[2]]);
                    let err_msg = String::from_utf8_lossy(&pk_resp[9..]);
                    bail!("Failed to get public key: {} - {}", err_code, err_msg);
                }
                _ => bail!("Unexpected public key response: 0x{:02X}", pk_resp[0]),
            }
        }

        // Read final auth result
        let final_resp = self.read_packet().await?;
        match final_resp.first() {
            Some(0x00) => {
                debug!("caching_sha2_password: full authentication succeeded");
                Ok(())
            }
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([final_resp[1], final_resp[2]]);
                let err_msg = String::from_utf8_lossy(&final_resp[9..]);
                bail!(
                    "caching_sha2_password full auth failed: {} - {}",
                    err_code,
                    err_msg
                );
            }
            _ => bail!(
                "Unexpected caching_sha2_password final response: {:?}",
                final_resp.first()
            ),
        }
    }

    /// Encrypt password using RSA public key for caching_sha2_password
    ///
    /// The password is XORed with the nonce (to prevent replay attacks),
    /// then encrypted with the server's RSA public key using PKCS#1 v1.5 padding.
    fn rsa_encrypt_password(&self, password: &str, nonce: &[u8], pem: &str) -> Result<Vec<u8>> {
        // Parse PEM public key
        let der = Self::parse_pem_public_key(pem)?;

        // XOR password with nonce (repeating nonce if password is longer)
        let mut pwd_bytes = password.as_bytes().to_vec();
        pwd_bytes.push(0); // Null terminator

        // MySQL requires XORing password with nonce
        for (i, byte) in pwd_bytes.iter_mut().enumerate() {
            *byte ^= nonce[i % nonce.len()];
        }

        // Parse the RSA public key components from DER
        let (n, e) = Self::parse_rsa_public_key_der(&der)?;

        // Create RSA public key using the rsa crate
        let n_bigint = BigUint::from_bytes_be(&n);
        let e_bigint = BigUint::from_bytes_be(&e);
        let public_key = RsaPublicKey::new(n_bigint, e_bigint)
            .map_err(|e| anyhow::anyhow!("Invalid RSA public key: {}", e))?;

        // Encrypt using PKCS#1 v1.5 padding (MySQL compatible)
        let mut rng = OsRng;
        let encrypted = public_key
            .encrypt(&mut rng, Pkcs1v15Encrypt, &pwd_bytes)
            .map_err(|e| anyhow::anyhow!("RSA encryption failed: {}", e))?;

        Ok(encrypted)
    }

    /// Parse PEM-encoded public key to DER
    fn parse_pem_public_key(pem: &str) -> Result<Vec<u8>> {
        let pem = pem.trim();

        // Find the base64 content between headers
        let start_marker = "-----BEGIN PUBLIC KEY-----";
        let end_marker = "-----END PUBLIC KEY-----";

        let start = pem
            .find(start_marker)
            .ok_or_else(|| anyhow::anyhow!("Invalid PEM: missing BEGIN marker"))?
            + start_marker.len();

        let end = pem
            .find(end_marker)
            .ok_or_else(|| anyhow::anyhow!("Invalid PEM: missing END marker"))?;

        let base64_content: String = pem[start..end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        use base64::Engine;
        let der = base64::engine::general_purpose::STANDARD
            .decode(&base64_content)
            .context("Failed to decode base64 public key")?;

        Ok(der)
    }

    /// Parse RSA public key from DER (SubjectPublicKeyInfo format)
    fn parse_rsa_public_key_der(der: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // SubjectPublicKeyInfo structure:
        // SEQUENCE {
        //   SEQUENCE { algorithm OID, parameters }
        //   BIT STRING { RSAPublicKey }
        // }
        //
        // RSAPublicKey structure:
        // SEQUENCE {
        //   INTEGER (modulus n)
        //   INTEGER (public exponent e)
        // }

        let mut pos = 0;

        // Outer SEQUENCE
        if der[pos] != 0x30 {
            bail!("Invalid DER: expected SEQUENCE");
        }
        pos += 1;
        let (_outer_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes;

        // Algorithm SEQUENCE (skip it)
        if der[pos] != 0x30 {
            bail!("Invalid DER: expected algorithm SEQUENCE");
        }
        pos += 1;
        let (algo_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes + algo_len;

        // BIT STRING
        if der[pos] != 0x03 {
            bail!("Invalid DER: expected BIT STRING");
        }
        pos += 1;
        let (_bitstring_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes;
        pos += 1; // Skip unused bits byte

        // RSAPublicKey SEQUENCE
        if der[pos] != 0x30 {
            bail!("Invalid DER: expected RSAPublicKey SEQUENCE");
        }
        pos += 1;
        let (_rsa_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes;

        // Modulus INTEGER
        if der[pos] != 0x02 {
            bail!("Invalid DER: expected modulus INTEGER");
        }
        pos += 1;
        let (n_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes;
        let mut n = der[pos..pos + n_len].to_vec();
        // Remove leading zero if present (ASN.1 encoding artifact)
        if !n.is_empty() && n[0] == 0x00 {
            n.remove(0);
        }
        pos += n_len;

        // Exponent INTEGER
        if der[pos] != 0x02 {
            bail!("Invalid DER: expected exponent INTEGER");
        }
        pos += 1;
        let (e_len, len_bytes) = Self::parse_der_length(&der[pos..])?;
        pos += len_bytes;
        let mut e = der[pos..pos + e_len].to_vec();
        // Remove leading zero if present
        if !e.is_empty() && e[0] == 0x00 {
            e.remove(0);
        }

        trace!(
            "Parsed RSA public key: n={} bytes, e={} bytes",
            n.len(),
            e.len()
        );

        Ok((n, e))
    }

    /// Parse DER length encoding
    fn parse_der_length(data: &[u8]) -> Result<(usize, usize)> {
        if data.is_empty() {
            bail!("Invalid DER: empty length");
        }

        if data[0] < 0x80 {
            // Short form
            Ok((data[0] as usize, 1))
        } else if data[0] == 0x81 {
            // Long form, 1 byte
            if data.len() < 2 {
                bail!("Invalid DER: truncated length");
            }
            Ok((data[1] as usize, 2))
        } else if data[0] == 0x82 {
            // Long form, 2 bytes
            if data.len() < 3 {
                bail!("Invalid DER: truncated length");
            }
            Ok((((data[1] as usize) << 8) | (data[2] as usize), 3))
        } else {
            bail!(
                "Invalid DER: unsupported length encoding: 0x{:02X}",
                data[0]
            );
        }
    }

    /// mysql_native_password authentication
    fn mysql_native_password(password: Option<&str>, salt: &[u8]) -> Vec<u8> {
        match password {
            None | Some("") => vec![],
            Some(pwd) => {
                // SHA1(password) XOR SHA1(salt + SHA1(SHA1(password)))
                let mut hasher = Sha1::new();
                hasher.update(pwd.as_bytes());
                let stage1 = hasher.finalize();

                let mut hasher = Sha1::new();
                hasher.update(stage1);
                let stage2 = hasher.finalize();

                let mut hasher = Sha1::new();
                hasher.update(salt);
                hasher.update(stage2);
                let stage3 = hasher.finalize();

                stage1
                    .iter()
                    .zip(stage3.iter())
                    .map(|(a, b)| a ^ b)
                    .collect()
            }
        }
    }

    /// caching_sha2_password authentication
    fn caching_sha2_password(password: Option<&str>, salt: &[u8]) -> Vec<u8> {
        match password {
            None | Some("") => vec![],
            Some(pwd) => {
                // XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + salt))
                let mut hasher = Sha256::new();
                hasher.update(pwd.as_bytes());
                let hash1 = hasher.finalize();

                let mut hasher = Sha256::new();
                hasher.update(hash1);
                let hash2 = hasher.finalize();

                let mut hasher = Sha256::new();
                hasher.update(hash2);
                hasher.update(salt);
                let hash3 = hasher.finalize();

                hash1.iter().zip(hash3.iter()).map(|(a, b)| a ^ b).collect()
            }
        }
    }

    /// client_ed25519 authentication (MariaDB)
    ///
    /// MariaDB's ed25519 authentication plugin uses the Ed25519 signature algorithm.
    /// The client signs the server's random challenge (nonce) with the Ed25519 private key
    /// derived from the password, and sends the signature back.
    ///
    /// Protocol:
    /// 1. Server sends 32-byte random nonce
    /// 2. Client derives Ed25519 keypair from SHA-512(password)
    /// 3. Client signs the nonce with the private key
    /// 4. Client sends the 64-byte signature
    fn client_ed25519(password: Option<&str>, nonce: &[u8]) -> Vec<u8> {
        match password {
            None | Some("") => vec![],
            Some(pwd) => {
                // Derive Ed25519 seed from password using SHA-512
                // MariaDB uses SHA-512 of the password as the Ed25519 seed
                use ed25519_dalek::{Signer, SigningKey};
                use sha2::{Digest, Sha512};

                // Hash password with SHA-512 to get 64 bytes
                let mut hasher = Sha512::new();
                hasher.update(pwd.as_bytes());
                let hash = hasher.finalize();

                // Ed25519 seed is the first 32 bytes
                let seed: [u8; 32] = match hash[..32].try_into() {
                    Ok(s) => s,
                    Err(_) => return vec![],
                };

                // Create Ed25519 signing key from seed
                let signing_key = SigningKey::from_bytes(&seed);

                // Sign the nonce
                let signature = signing_key.sign(nonce);
                signature.to_bytes().to_vec()
            }
        }
    }

    /// Execute a query and return OK or error
    pub async fn query(&mut self, sql: &str) -> Result<()> {
        self.sequence_id = 0;

        let mut packet = BytesMut::with_capacity(sql.len() + 1);
        packet.put_u8(0x03); // COM_QUERY
        packet.put_slice(sql.as_bytes());

        self.write_packet(&packet).await?;

        let resp = self.read_packet().await?;
        match resp.first() {
            Some(0x00) => Ok(()),
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([resp[1], resp[2]]);
                let err_msg = String::from_utf8_lossy(&resp[9..]);
                bail!("Query failed: {} - {}", err_code, err_msg);
            }
            _ => Ok(()), // Result set - we ignore for now
        }
    }

    /// Register as a replication slave
    pub async fn register_slave(&mut self, server_id: u32) -> Result<()> {
        self.sequence_id = 0;

        let mut packet = BytesMut::with_capacity(18);
        packet.put_u8(0x15); // COM_REGISTER_SLAVE
        packet.put_u32_le(server_id);
        packet.put_u8(0); // hostname length
        packet.put_u8(0); // user length
        packet.put_u8(0); // password length
        packet.put_u16_le(0); // port
        packet.put_u32_le(0); // replication rank (ignored)
        packet.put_u32_le(0); // master id (0 = use this connection's server id)

        self.write_packet(&packet).await?;

        let resp = self.read_packet().await?;
        match resp.first() {
            Some(0x00) => {
                info!("Registered as slave with server_id={}", server_id);
                Ok(())
            }
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([resp[1], resp[2]]);
                let err_msg = String::from_utf8_lossy(&resp[9..]);
                bail!("Failed to register as slave: {} - {}", err_code, err_msg);
            }
            _ => bail!("Unexpected response to COM_REGISTER_SLAVE"),
        }
    }

    /// Start binlog dump from a specific position
    pub async fn binlog_dump(
        &mut self,
        server_id: u32,
        binlog_filename: &str,
        binlog_position: u32,
    ) -> Result<BinlogStream<'_>> {
        self.sequence_id = 0;

        let mut packet = BytesMut::with_capacity(binlog_filename.len() + 11);
        packet.put_u8(0x12); // COM_BINLOG_DUMP
        packet.put_u32_le(binlog_position);
        packet.put_u16_le(0); // flags
        packet.put_u32_le(server_id);
        packet.put_slice(binlog_filename.as_bytes());

        self.write_packet(&packet).await?;

        info!(
            "Started binlog dump from {}:{}",
            binlog_filename, binlog_position
        );

        Ok(BinlogStream { client: self })
    }

    /// Start binlog dump using GTID
    pub async fn binlog_dump_gtid(
        &mut self,
        server_id: u32,
        gtid_set: &str,
    ) -> Result<BinlogStream<'_>> {
        self.sequence_id = 0;

        // Parse GTID set and encode
        let gtid_data = Self::encode_gtid_set(gtid_set)?;

        let mut packet = BytesMut::with_capacity(26 + gtid_data.len());
        packet.put_u8(0x1E); // COM_BINLOG_DUMP_GTID
        packet.put_u16_le(0x04); // flags: BINLOG_THROUGH_GTID
        packet.put_u32_le(server_id);
        packet.put_u32_le(0); // binlog filename length (empty for GTID)
        packet.put_u64_le(4); // binlog position (4 = start)
        packet.put_u32_le(gtid_data.len() as u32);
        packet.put_slice(&gtid_data);

        self.write_packet(&packet).await?;

        info!("Started GTID-based binlog dump");

        Ok(BinlogStream { client: self })
    }

    /// Encode GTID set for wire protocol
    fn encode_gtid_set(gtid_set: &str) -> Result<Vec<u8>> {
        if gtid_set.is_empty() {
            // Empty GTID set
            let mut data = Vec::with_capacity(8);
            data.extend_from_slice(&0u64.to_le_bytes()); // n_sids = 0
            return Ok(data);
        }

        // Parse format: uuid:interval[:interval],uuid:interval...
        let mut sids = Vec::new();
        for sid_str in gtid_set.split(',') {
            let sid_str = sid_str.trim();
            let parts: Vec<&str> = sid_str.split(':').collect();
            if parts.len() < 2 {
                bail!("Invalid GTID format: {}", sid_str);
            }

            let uuid = parts[0];
            let uuid_bytes = Self::parse_uuid(uuid)?;

            let mut intervals = Vec::new();
            for interval_str in &parts[1..] {
                let interval_parts: Vec<&str> = interval_str.split('-').collect();
                let start: u64 = interval_parts[0]
                    .parse()
                    .context("Invalid GTID interval start")?;
                let end: u64 = if interval_parts.len() > 1 {
                    interval_parts[1]
                        .parse()
                        .context("Invalid GTID interval end")?
                } else {
                    start
                };
                intervals.push((start, end + 1)); // End is exclusive in wire format
            }

            sids.push((uuid_bytes, intervals));
        }

        // Encode
        let mut data = Vec::new();
        data.extend_from_slice(&(sids.len() as u64).to_le_bytes());

        for (uuid, intervals) in sids {
            data.extend_from_slice(&uuid);
            data.extend_from_slice(&(intervals.len() as u64).to_le_bytes());
            for (start, end) in intervals {
                data.extend_from_slice(&start.to_le_bytes());
                data.extend_from_slice(&end.to_le_bytes());
            }
        }

        Ok(data)
    }

    /// Parse UUID string to bytes
    fn parse_uuid(uuid: &str) -> Result<[u8; 16]> {
        let hex: String = uuid.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        if hex.len() != 32 {
            bail!("Invalid UUID: {}", uuid);
        }

        let mut bytes = [0u8; 16];
        for i in 0..16 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
        }
        Ok(bytes)
    }

    /// Get server version
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Get connection ID
    pub fn connection_id(&self) -> u32 {
        self.connection_id
    }
}

/// Stream of binlog events
pub struct BinlogStream<'a> {
    client: &'a mut MySqlBinlogClient,
}

impl<'a> BinlogStream<'a> {
    /// Read next binlog event
    pub async fn next_event(&mut self) -> Result<Option<Bytes>> {
        let packet = self.client.read_packet().await?;

        if packet.is_empty() {
            return Ok(None);
        }

        match packet[0] {
            0x00 => {
                // OK packet with event data
                Ok(Some(Bytes::from(packet[1..].to_vec())))
            }
            0xFE => {
                // EOF packet
                debug!("Received EOF in binlog stream");
                Ok(None)
            }
            0xFF => {
                // Error packet
                let err_code = u16::from_le_bytes([packet[1], packet[2]]);
                let err_msg = String::from_utf8_lossy(&packet[9..]);
                bail!("Binlog error: {} - {}", err_code, err_msg);
            }
            _ => {
                // Raw event data (no OK header)
                Ok(Some(Bytes::from(packet)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_native_password() {
        let salt = b"12345678901234567890";
        let result = MySqlBinlogClient::mysql_native_password(Some("password"), salt);
        assert_eq!(result.len(), 20);
    }

    #[test]
    fn test_mysql_native_password_empty() {
        let salt = b"12345678901234567890";
        let result = MySqlBinlogClient::mysql_native_password(None, salt);
        assert!(result.is_empty());
    }

    #[test]
    fn test_caching_sha2_password() {
        let salt = b"12345678901234567890";
        let result = MySqlBinlogClient::caching_sha2_password(Some("password"), salt);
        assert_eq!(result.len(), 32);
    }

    #[test]
    fn test_parse_uuid() {
        let uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let bytes = MySqlBinlogClient::parse_uuid(uuid).unwrap();
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x3E);
    }

    #[test]
    fn test_encode_empty_gtid_set() {
        let data = MySqlBinlogClient::encode_gtid_set("").unwrap();
        assert_eq!(data.len(), 8);
        assert_eq!(u64::from_le_bytes(data[0..8].try_into().unwrap()), 0);
    }

    #[test]
    fn test_caching_sha2_password_empty() {
        let salt = b"12345678901234567890";
        let result = MySqlBinlogClient::caching_sha2_password(None, salt);
        assert!(result.is_empty());

        let result = MySqlBinlogClient::caching_sha2_password(Some(""), salt);
        assert!(result.is_empty());
    }

    #[test]
    fn test_caching_sha2_password_consistency() {
        let salt = b"random_salt_12345678";
        let password = "test_password_123";

        // Verify consistent output
        let result1 = MySqlBinlogClient::caching_sha2_password(Some(password), salt);
        let result2 = MySqlBinlogClient::caching_sha2_password(Some(password), salt);
        assert_eq!(result1, result2);

        // Different passwords should produce different results
        let result3 = MySqlBinlogClient::caching_sha2_password(Some("different"), salt);
        assert_ne!(result1, result3);

        // Different salts should produce different results
        let different_salt = b"different_salt_123";
        let result4 = MySqlBinlogClient::caching_sha2_password(Some(password), different_salt);
        assert_ne!(result1, result4);
    }

    #[test]
    fn test_parse_pem_public_key() {
        // Valid RSA 2048-bit public key in PEM format (generated for testing)
        let pem = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo
4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0/IzW7yWR7QkrmBL7jTKEn5u
+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyeh
kd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40RQzuVaE8AkAFmxZzow3x+VJYKdjykkJ
0iT9wCS0DRTXu269V264Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdg
cKWTjpBP2dPwVZ4WWC+9aGVd+Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbc
mwIDAQAB
-----END PUBLIC KEY-----"#;

        let result = MySqlBinlogClient::parse_pem_public_key(pem);
        assert!(result.is_ok(), "Failed to parse PEM: {:?}", result.err());
        let der = result.unwrap();
        // Should have decoded some data (RSA 2048-bit key is ~270 bytes in DER)
        assert!(!der.is_empty());
        assert!(
            der.len() > 200,
            "DER should be > 200 bytes for 2048-bit key"
        );
    }

    #[test]
    fn test_parse_pem_public_key_invalid() {
        // Missing markers
        let invalid = "not a valid pem";
        assert!(MySqlBinlogClient::parse_pem_public_key(invalid).is_err());

        // Missing end marker
        let invalid = "-----BEGIN PUBLIC KEY-----\nMIIBIjAN";
        assert!(MySqlBinlogClient::parse_pem_public_key(invalid).is_err());
    }

    #[test]
    fn test_parse_der_length_short_form() {
        // Short form: length < 128
        let data = [50u8]; // length = 50
        let (len, bytes_read) = MySqlBinlogClient::parse_der_length(&data).unwrap();
        assert_eq!(len, 50);
        assert_eq!(bytes_read, 1);
    }

    #[test]
    fn test_parse_der_length_long_form_1_byte() {
        // Long form with 1 byte: 0x81 followed by length
        let data = [0x81, 200]; // length = 200
        let (len, bytes_read) = MySqlBinlogClient::parse_der_length(&data).unwrap();
        assert_eq!(len, 200);
        assert_eq!(bytes_read, 2);
    }

    #[test]
    fn test_parse_der_length_long_form_2_bytes() {
        // Long form with 2 bytes: 0x82 followed by length
        let data = [0x82, 0x01, 0x00]; // length = 256
        let (len, bytes_read) = MySqlBinlogClient::parse_der_length(&data).unwrap();
        assert_eq!(len, 256);
        assert_eq!(bytes_read, 3);
    }

    #[test]
    fn test_client_ed25519() {
        // Test Ed25519 authentication produces 64-byte signature
        let nonce = b"12345678901234567890123456789012"; // 32-byte nonce
        let result = MySqlBinlogClient::client_ed25519(Some("password"), nonce);
        // Ed25519 signature is always 64 bytes
        assert_eq!(result.len(), 64);
    }

    #[test]
    fn test_client_ed25519_empty_password() {
        let nonce = b"12345678901234567890123456789012";
        let result = MySqlBinlogClient::client_ed25519(None, nonce);
        assert!(result.is_empty());

        let result = MySqlBinlogClient::client_ed25519(Some(""), nonce);
        assert!(result.is_empty());
    }

    #[test]
    fn test_client_ed25519_consistency() {
        // Same password + nonce should produce same signature
        let nonce = b"random_nonce_32_bytes_long_here!";
        let password = "test_password";

        let result1 = MySqlBinlogClient::client_ed25519(Some(password), nonce);
        let result2 = MySqlBinlogClient::client_ed25519(Some(password), nonce);
        assert_eq!(result1, result2);

        // Different password should produce different signature
        let result3 = MySqlBinlogClient::client_ed25519(Some("different"), nonce);
        assert_ne!(result1, result3);

        // Different nonce should produce different signature
        let different_nonce = b"different_nonce_32_bytes_here!!";
        let result4 = MySqlBinlogClient::client_ed25519(Some(password), different_nonce);
        assert_ne!(result1, result4);
    }
}
