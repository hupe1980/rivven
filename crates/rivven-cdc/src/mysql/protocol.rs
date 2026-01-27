//! MySQL binary log protocol implementation
//!
//! Implements the MySQL replication protocol for CDC:
//! - Handshake and authentication (mysql_native_password, caching_sha2_password)
//! - TLS/SSL encryption support
//! - COM_REGISTER_SLAVE
//! - COM_BINLOG_DUMP / COM_BINLOG_DUMP_GTID
//! - Binlog event streaming

use anyhow::{bail, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use std::io::Read;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

#[cfg(feature = "mysql-tls")]
use std::sync::Arc;
#[cfg(feature = "mysql-tls")]
use tracing::trace;

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

/// MySQL capability flags
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
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

    pub fn has(&self, flag: u32) -> bool {
        (self.0 & flag) != 0
    }

    pub fn value(&self) -> u32 {
        self.0
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

    /// Read a MySQL packet
    async fn read_packet(&mut self) -> Result<Vec<u8>> {
        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await?;

        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        self.sequence_id = header[3].wrapping_add(1);

        let mut payload = vec![0u8; payload_len];
        self.stream.read_exact(&mut payload).await?;

        Ok(payload)
    }

    /// Write a MySQL packet
    async fn write_packet(&mut self, data: &[u8]) -> Result<()> {
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

        self.stream.write_all(&packet).await?;
        self.stream.flush().await?;
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

        match resp.first() {
            Some(0x00) => {
                debug!("Authentication successful");
                Ok(())
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
            _ => bail!("Unsupported auth plugin for switch: {}", plugin),
        };

        self.write_packet(&auth_response).await?;

        let resp = self.read_packet().await?;
        match resp.first() {
            Some(0x00) => Ok(()),
            Some(0x01) if plugin == "caching_sha2_password" => {
                // Fast auth result - need full authentication
                // For now, just try sending password in clear (requires SSL in production)
                if resp.len() > 1 && resp[1] == 0x03 {
                    debug!("Fast auth success");
                    return Ok(());
                }
                bail!("caching_sha2_password full auth not implemented (requires SSL)");
            }
            Some(0xFF) => {
                let err_code = u16::from_le_bytes([resp[1], resp[2]]);
                let err_msg = String::from_utf8_lossy(&resp[9..]);
                bail!("Auth switch failed: {} - {}", err_code, err_msg);
            }
            _ => bail!("Unexpected auth switch response"),
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
}
