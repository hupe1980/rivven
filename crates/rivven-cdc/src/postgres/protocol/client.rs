//! PostgreSQL replication client
//!
//! Low-level TCP client for PostgreSQL replication protocol.
//! Supports MD5 authentication and pgoutput streaming.

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use md5::{Digest, Md5};
use postgres_protocol::message::{backend, frontend};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

use crate::common::{
    CircuitBreaker, RateLimiter, Validator, CONNECTION_TIMEOUT_SECS, IO_TIMEOUT_SECS,
};
use std::sync::Arc;

/// PostgreSQL replication client (basic, without TLS)
pub struct ReplicationClient {
    stream: BufReader<TcpStream>,
    user: String,
    database: String,
}

impl ReplicationClient {
    /// Connect to PostgreSQL in replication mode
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> Result<Self> {
        info!("Connecting to {}:{} as {}", host, port, user);
        let stream = TcpStream::connect((host, port)).await?;
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
                backend::Message::AuthenticationSasl(_) => {
                    return Err(anyhow!("SASL authentication not implemented"));
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

/// Secure replication client with circuit breaker and rate limiting
#[allow(dead_code)]
pub struct SecureReplicationClient {
    stream: BufReader<TcpStream>,
    user: String,
    database: String,
    circuit_breaker: Arc<CircuitBreaker>,
    rate_limiter: Arc<RateLimiter>,
}

impl SecureReplicationClient {
    /// Connect with security features
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
            "Connecting to {}:{} as {} (with security)",
            host, port, user
        );

        let circuit_breaker = Arc::new(CircuitBreaker::new(5, Duration::from_secs(30), 2));

        if !circuit_breaker.allow_request().await {
            return Err(anyhow!("Circuit breaker is OPEN"));
        }

        let stream = match timeout(
            Duration::from_secs(CONNECTION_TIMEOUT_SECS),
            TcpStream::connect((host, port)),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                circuit_breaker.record_failure().await;
                return Err(e.into());
            }
            Err(_) => {
                circuit_breaker.record_failure().await;
                return Err(anyhow!(
                    "Connection timeout after {}s",
                    CONNECTION_TIMEOUT_SECS
                ));
            }
        };

        let mut stream = BufReader::new(stream);

        // Startup
        let params = vec![
            ("user", user),
            ("database", database),
            ("replication", "database"),
        ];
        let mut buf = BytesMut::new();
        frontend::startup_message(params.into_iter(), &mut buf)?;
        Self::write_with_timeout(&mut stream, &buf).await?;

        // Authentication
        loop {
            let (type_code, body) = Self::read_message_with_timeout(&mut stream).await?;

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
                    warn!("⚠️ SECURITY: Cleartext password is insecure!");
                    let pass = password.ok_or_else(|| anyhow!("Password required"))?;
                    let mut buf = BytesMut::new();
                    frontend::password_message(pass.as_bytes(), &mut buf)?;
                    Self::write_with_timeout(&mut stream, &buf).await?;
                }
                backend::Message::AuthenticationMd5Password(body) => {
                    warn!("⚠️ SECURITY: MD5 is cryptographically weak!");
                    let pass = password.ok_or_else(|| anyhow!("Password required"))?;
                    let hash = hash_md5_password(user, pass, &body.salt());
                    let mut buf = BytesMut::new();
                    frontend::password_message(hash.as_bytes(), &mut buf)?;
                    Self::write_with_timeout(&mut stream, &buf).await?;
                }
                backend::Message::AuthenticationSasl(_) => {
                    return Err(anyhow!("SASL not implemented"));
                }
                backend::Message::ErrorResponse(_) => {
                    circuit_breaker.record_failure().await;
                    return Err(anyhow!("Auth error"));
                }
                _ => {
                    circuit_breaker.record_failure().await;
                    return Err(anyhow!("Unexpected auth message"));
                }
            }
        }

        // Wait for ReadyForQuery
        loop {
            let (type_code, _) = Self::read_message_with_timeout(&mut stream).await?;
            if type_code == b'Z' {
                break;
            } else if type_code == b'E' {
                circuit_breaker.record_failure().await;
                return Err(anyhow!("Error waiting for ready"));
            }
        }

        circuit_breaker.record_success().await;
        let rate_limiter = Arc::new(RateLimiter::new(1000, 10000));

        Ok(Self {
            stream,
            user: user.to_string(),
            database: database.to_string(),
            circuit_breaker,
            rate_limiter,
        })
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
        Self::write_with_timeout(&mut self.stream, &buf).await?;

        let (type_code, _) = Self::read_message_with_timeout(&mut self.stream).await?;

        if type_code == b'W' {
            info!("Entered CopyBoth mode (secure)");
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

    async fn read_message_with_timeout(stream: &mut BufReader<TcpStream>) -> Result<(u8, Vec<u8>)> {
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

    async fn write_with_timeout(stream: &mut BufReader<TcpStream>, data: &[u8]) -> Result<()> {
        timeout(Duration::from_secs(IO_TIMEOUT_SECS), async {
            stream.get_mut().write_all(data).await?;
            stream.get_mut().flush().await?;
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
        Self::write_with_timeout(&mut self.stream, &buf).await?;

        loop {
            let (type_code, _) = Self::read_message_with_timeout(&mut self.stream).await?;
            if type_code == b'Z' {
                break;
            } else if type_code == b'E' {
                return Err(anyhow!("Query error"));
            }
        }

        Ok(())
    }
}

/// Secure replication stream with rate limiting and circuit breaker
pub struct SecureReplicationStream {
    stream: BufReader<TcpStream>,
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
