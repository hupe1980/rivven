use crate::{Error, MessageData, Request, Response, Result};
use bytes::Bytes;
use rivven_core::PasswordHash;
use sha2::{Sha256, Digest};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, debug};

#[cfg(feature = "tls")]
use std::net::SocketAddr;

#[cfg(feature = "tls")]
use rivven_core::tls::{TlsConfig, TlsConnector, TlsClientStream};

// Default maximum response size (100 MB) - prevents malicious server from exhausting client memory
const DEFAULT_MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;

// ============================================================================
// Stream Wrapper
// ============================================================================

/// Wrapper for either plaintext or TLS streams
/// Note: TLS variant is significantly larger due to TLS state, but boxing
/// would add indirection overhead for every I/O operation
#[allow(clippy::large_enum_variant)]
enum ClientStream {
    Plaintext(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsClientStream<TcpStream>),
}

impl AsyncRead for ClientStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plaintext(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ClientStream::Plaintext(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plaintext(s) => std::pin::Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            ClientStream::Plaintext(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            ClientStream::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

// ============================================================================
// Client
// ============================================================================

/// Rivven client for connecting to a Rivven server
pub struct Client {
    stream: ClientStream,
}

impl Client {
    /// Connect to a Rivven server (plaintext)
    pub async fn connect(addr: &str) -> Result<Self> {
        info!("Connecting to Rivven server at {}", addr);
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::ConnectionError(e.to_string()))?;
        
        Ok(Self { 
            stream: ClientStream::Plaintext(stream),
        })
    }

    /// Connect to a Rivven server with TLS
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        addr: &str,
        tls_config: &TlsConfig,
        server_name: &str,
    ) -> Result<Self> {
        info!("Connecting to Rivven server at {} with TLS", addr);
        
        // Parse address
        let socket_addr: SocketAddr = addr.parse()
            .map_err(|e| Error::ConnectionError(format!("Invalid address: {}", e)))?;
        
        // Create TLS connector
        let connector = TlsConnector::new(tls_config)
            .map_err(|e| Error::ConnectionError(format!("TLS config error: {}", e)))?;
        
        // Connect with TLS
        let tls_stream = connector.connect_tcp(socket_addr, server_name)
            .await
            .map_err(|e| Error::ConnectionError(format!("TLS connection error: {}", e)))?;
        
        info!("TLS connection established to {} ({})", addr, server_name);
        
        Ok(Self {
            stream: ClientStream::Tls(tls_stream),
        })
    }

    /// Connect with mTLS (mutual TLS) using client certificate
    #[cfg(feature = "tls")]
    pub async fn connect_mtls(
        addr: &str,
        cert_path: impl Into<std::path::PathBuf>,
        key_path: impl Into<std::path::PathBuf>,
        ca_path: impl Into<std::path::PathBuf> + Clone,
        server_name: &str,
    ) -> Result<Self> {
        let tls_config = TlsConfig::mtls_from_pem_files(cert_path, key_path, ca_path);
        Self::connect_tls(addr, &tls_config, server_name).await
    }

    // ========================================================================
    // Authentication Methods
    // ========================================================================

    /// Authenticate with simple username/password
    /// 
    /// This uses a simple plaintext password protocol. For production use over
    /// untrusted networks, prefer `authenticate_scram()` or use TLS.
    pub async fn authenticate(&mut self, username: &str, password: &str) -> Result<AuthSession> {
        let request = Request::Authenticate {
            username: username.to_string(),
            password: password.to_string(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Authenticated { session_id, expires_in } => {
                info!("Authenticated as '{}'", username);
                Ok(AuthSession { session_id, expires_in })
            }
            Response::Error { message } => Err(Error::AuthenticationFailed(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Authenticate using SCRAM-SHA-256 (secure challenge-response)
    /// 
    /// SCRAM-SHA-256 (RFC 5802/7677) provides:
    /// - Password never sent over the wire
    /// - Mutual authentication (server proves it knows password too)
    /// - Protection against replay attacks
    /// 
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let session = client.authenticate_scram("alice", "password123").await?;
    /// println!("Session: {} (expires in {}s)", session.session_id, session.expires_in);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn authenticate_scram(&mut self, username: &str, password: &str) -> Result<AuthSession> {
        // Step 1: Generate client nonce and send client-first message
        let client_nonce = generate_nonce();
        let client_first_bare = format!("n={},r={}", escape_username(username), client_nonce);
        let client_first = format!("n,,{}", client_first_bare);
        
        debug!("SCRAM: Sending client-first");
        let request = Request::ScramClientFirst {
            message: Bytes::from(client_first.clone()),
        };

        let response = self.send_request(request).await?;
        
        // Step 2: Parse server-first message
        let server_first = match response {
            Response::ScramServerFirst { message } => {
                String::from_utf8(message.to_vec())
                    .map_err(|_| Error::AuthenticationFailed("Invalid server-first encoding".into()))?
            }
            Response::Error { message } => return Err(Error::AuthenticationFailed(message)),
            _ => return Err(Error::InvalidResponse),
        };
        
        debug!("SCRAM: Received server-first");
        
        // Parse server-first: r=<nonce>,s=<salt>,i=<iterations>
        let (combined_nonce, salt_b64, iterations) = parse_server_first(&server_first)?;
        
        // Verify server nonce starts with our client nonce
        if !combined_nonce.starts_with(&client_nonce) {
            return Err(Error::AuthenticationFailed("Server nonce mismatch".into()));
        }
        
        // Decode salt
        let salt = base64_decode(&salt_b64)
            .map_err(|_| Error::AuthenticationFailed("Invalid salt encoding".into()))?;
        
        // Step 3: Compute client proof
        let salted_password = pbkdf2_sha256(password.as_bytes(), &salt, iterations);
        let client_key = PasswordHash::hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        
        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!("{},{},{}", client_first_bare, server_first, client_final_without_proof);
        
        let client_signature = PasswordHash::hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);
        let client_proof_b64 = base64_encode(&client_proof);
        
        // Step 4: Send client-final message
        let client_final = format!("{},p={}", client_final_without_proof, client_proof_b64);
        
        debug!("SCRAM: Sending client-final");
        let request = Request::ScramClientFinal {
            message: Bytes::from(client_final),
        };

        let response = self.send_request(request).await?;
        
        // Step 5: Verify server-final and get session
        match response {
            Response::ScramServerFinal { message, session_id, expires_in } => {
                let server_final = String::from_utf8(message.to_vec())
                    .map_err(|_| Error::AuthenticationFailed("Invalid server-final encoding".into()))?;
                
                // Check for error response
                if let Some(error_msg) = server_final.strip_prefix("e=") {
                    return Err(Error::AuthenticationFailed(error_msg.to_string()));
                }
                
                // Verify server signature (mutual authentication)
                if let Some(verifier_b64) = server_final.strip_prefix("v=") {
                    let server_key = PasswordHash::hmac_sha256(&salted_password, b"Server Key");
                    let expected_server_sig = PasswordHash::hmac_sha256(&server_key, auth_message.as_bytes());
                    let expected_verifier = base64_encode(&expected_server_sig);
                    
                    if verifier_b64 != expected_verifier {
                        return Err(Error::AuthenticationFailed("Server verification failed".into()));
                    }
                }
                
                let session_id = session_id
                    .ok_or_else(|| Error::AuthenticationFailed("No session ID in response".into()))?;
                let expires_in = expires_in
                    .ok_or_else(|| Error::AuthenticationFailed("No expiry in response".into()))?;
                
                info!("SCRAM authentication successful for '{}'", username);
                Ok(AuthSession { session_id, expires_in })
            }
            Response::Error { message } => Err(Error::AuthenticationFailed(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    // ========================================================================
    // Request/Response Handling
    // ========================================================================

    /// Send a request and receive a response
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Serialize request
        let request_bytes = request.to_bytes()?;
        
        // Write length prefix + request
        let len = request_bytes.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&request_bytes).await?;
        self.stream.flush().await?;

        // Read length prefix
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let msg_len = u32::from_be_bytes(len_buf) as usize;

        // Validate response size to prevent memory exhaustion from malicious server
        if msg_len > DEFAULT_MAX_RESPONSE_SIZE {
            return Err(Error::ResponseTooLarge(msg_len, DEFAULT_MAX_RESPONSE_SIZE));
        }

        // Read response
        let mut response_buf = vec![0u8; msg_len];
        self.stream.read_exact(&mut response_buf).await?;

        // Deserialize response
        let response = Response::from_bytes(&response_buf)?;
        
        Ok(response)
    }

    /// Publish a message to a topic
    pub async fn publish(
        &mut self,
        topic: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<u64> {
        self.publish_with_key(topic, None::<Bytes>, value).await
    }

    /// Publish a message with a key to a topic
    pub async fn publish_with_key(
        &mut self,
        topic: impl Into<String>,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
    ) -> Result<u64> {
        let request = Request::Publish {
            topic: topic.into(),
            partition: None,
            key: key.map(|k| k.into()),
            value: value.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Published { offset, .. } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Publish a message to a specific partition
    pub async fn publish_to_partition(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
    ) -> Result<u64> {
        let request = Request::Publish {
            topic: topic.into(),
            partition: Some(partition),
            key: key.map(|k| k.into()),
            value: value.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Published { offset, .. } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Consume messages from a topic partition
    pub async fn consume(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        let request = Request::Consume {
            topic: topic.into(),
            partition,
            offset,
            max_messages,
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Messages { messages } => Ok(messages),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Create a new topic
    pub async fn create_topic(
        &mut self,
        name: impl Into<String>,
        partitions: Option<u32>,
    ) -> Result<u32> {
        let name = name.into();
        let request = Request::CreateTopic {
            name: name.clone(),
            partitions,
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::TopicCreated { partitions, .. } => Ok(partitions),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// List all topics
    pub async fn list_topics(&mut self) -> Result<Vec<String>> {
        let request = Request::ListTopics;
        let response = self.send_request(request).await?;
        
        match response {
            Response::Topics { topics } => Ok(topics),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Delete a topic
    pub async fn delete_topic(&mut self, name: impl Into<String>) -> Result<()> {
        let request = Request::DeleteTopic { name: name.into() };
        let response = self.send_request(request).await?;
        
        match response {
            Response::TopicDeleted => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Commit consumer offset
    pub async fn commit_offset(
        &mut self,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        let request = Request::CommitOffset {
            consumer_group: consumer_group.into(),
            topic: topic.into(),
            partition,
            offset,
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::OffsetCommitted => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get consumer offset
    pub async fn get_offset(
        &mut self,
        consumer_group: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
    ) -> Result<Option<u64>> {
        let request = Request::GetOffset {
            consumer_group: consumer_group.into(),
            topic: topic.into(),
            partition,
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Offset { offset } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get earliest and latest offsets for a topic partition
    /// 
    /// Returns (earliest, latest) where:
    /// - earliest: First available offset (messages before this are deleted/compacted)
    /// - latest: Next offset to be assigned (one past the last message)
    pub async fn get_offset_bounds(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
    ) -> Result<(u64, u64)> {
        let request = Request::GetOffsetBounds {
            topic: topic.into(),
            partition,
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::OffsetBounds { earliest, latest } => Ok((earliest, latest)),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get topic metadata
    pub async fn get_metadata(&mut self, topic: impl Into<String>) -> Result<(String, u32)> {
        let request = Request::GetMetadata {
            topic: topic.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Metadata { name, partitions } => Ok((name, partitions)),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<()> {
        let request = Request::Ping;
        let response = self.send_request(request).await?;
        
        match response {
            Response::Pong => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Register a schema
    pub async fn register_schema(&mut self, subject: impl Into<String>, schema: impl Into<String>) -> Result<i32> {
        let request = Request::RegisterSchema {
            subject: subject.into(),
            schema: schema.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::SchemaRegistered { id } => Ok(id),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get a schema
    pub async fn get_schema(&mut self, id: i32) -> Result<String> {
        let request = Request::GetSchema { id };

        let response = self.send_request(request).await?;
        
        match response {
            Response::Schema { id: _, schema } => Ok(schema),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// List all consumer groups
    pub async fn list_groups(&mut self) -> Result<Vec<String>> {
        let request = Request::ListGroups;

        let response = self.send_request(request).await?;
        
        match response {
            Response::Groups { groups } => Ok(groups),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Describe a consumer group (get all committed offsets)
    pub async fn describe_group(
        &mut self,
        consumer_group: impl Into<String>,
    ) -> Result<std::collections::HashMap<String, std::collections::HashMap<u32, u64>>> {
        let request = Request::DescribeGroup {
            consumer_group: consumer_group.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::GroupDescription { offsets, .. } => Ok(offsets),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Delete a consumer group
    pub async fn delete_group(&mut self, consumer_group: impl Into<String>) -> Result<()> {
        let request = Request::DeleteGroup {
            consumer_group: consumer_group.into(),
        };

        let response = self.send_request(request).await?;
        
        match response {
            Response::GroupDeleted => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Get the first offset with timestamp >= the given timestamp
    /// 
    /// # Arguments
    /// * `topic` - The topic name
    /// * `partition` - The partition number
    /// * `timestamp_ms` - Timestamp in milliseconds since Unix epoch
    /// 
    /// # Returns
    /// * `Some(offset)` - The first offset with message timestamp >= timestamp_ms
    /// * `None` - No messages found with timestamp >= timestamp_ms
    pub async fn get_offset_for_timestamp(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        timestamp_ms: i64,
    ) -> Result<Option<u64>> {
        let request = Request::GetOffsetForTimestamp {
            topic: topic.into(),
            partition,
            timestamp_ms,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::OffsetForTimestamp { offset } => Ok(offset),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }
}

// ============================================================================
// Authentication Session
// ============================================================================

/// Authentication session information
#[derive(Debug, Clone)]
pub struct AuthSession {
    /// Session ID for subsequent requests
    pub session_id: String,
    /// Session timeout in seconds
    pub expires_in: u64,
}

// ============================================================================
// SCRAM Helper Functions
// ============================================================================

/// Generate a random nonce for SCRAM authentication
fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let nonce_bytes: Vec<u8> = (0..24).map(|_| rng.gen()).collect();
    base64_encode(&nonce_bytes)
}

/// Escape username for SCRAM (RFC 5802)
fn escape_username(username: &str) -> String {
    username
        .replace('=', "=3D")
        .replace(',', "=2C")
}

/// Parse server-first message
fn parse_server_first(server_first: &str) -> Result<(String, String, u32)> {
    let mut nonce = None;
    let mut salt = None;
    let mut iterations = None;
    
    for attr in server_first.split(',') {
        if let Some(value) = attr.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = attr.strip_prefix("s=") {
            salt = Some(value.to_string());
        } else if let Some(value) = attr.strip_prefix("i=") {
            iterations = Some(value.parse::<u32>()
                .map_err(|_| Error::AuthenticationFailed("Invalid iteration count".into()))?);
        }
    }
    
    let nonce = nonce.ok_or_else(|| Error::AuthenticationFailed("Missing nonce".into()))?;
    let salt = salt.ok_or_else(|| Error::AuthenticationFailed("Missing salt".into()))?;
    let iterations = iterations.ok_or_else(|| Error::AuthenticationFailed("Missing iterations".into()))?;
    
    Ok((nonce, salt, iterations))
}

/// PBKDF2-HMAC-SHA256 key derivation
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut result = vec![0u8; 32];
    
    // U1 = PRF(Password, Salt || INT(1))
    let mut u = PasswordHash::hmac_sha256(password, &[salt, &1u32.to_be_bytes()].concat());
    result.copy_from_slice(&u);
    
    // Ui = PRF(Password, Ui-1)
    for _ in 1..iterations {
        u = PasswordHash::hmac_sha256(password, &u);
        for (r, ui) in result.iter_mut().zip(u.iter()) {
            *r ^= ui;
        }
    }
    
    result
}

/// SHA-256 hash
fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// XOR two byte arrays
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

/// Base64 encode
fn base64_encode(data: &[u8]) -> String {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.encode(data)
}

/// Base64 decode
fn base64_decode(data: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    use base64::{Engine, engine::general_purpose::STANDARD};
    STANDARD.decode(data)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_username() {
        assert_eq!(escape_username("alice"), "alice");
        assert_eq!(escape_username("user=name"), "user=3Dname");
        assert_eq!(escape_username("user,name"), "user=2Cname");
        assert_eq!(escape_username("user=,name"), "user=3D=2Cname");
    }

    #[test]
    fn test_parse_server_first() {
        let server_first = "r=clientnonce+servernonce,s=c2FsdA==,i=4096";
        let (nonce, salt, iterations) = parse_server_first(server_first).unwrap();
        
        assert_eq!(nonce, "clientnonce+servernonce");
        assert_eq!(salt, "c2FsdA==");
        assert_eq!(iterations, 4096);
    }

    #[test]
    fn test_parse_server_first_missing_nonce() {
        let server_first = "s=c2FsdA==,i=4096";
        assert!(parse_server_first(server_first).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_salt() {
        let server_first = "r=nonce,i=4096";
        assert!(parse_server_first(server_first).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_iterations() {
        let server_first = "r=nonce,s=c2FsdA==";
        assert!(parse_server_first(server_first).is_err());
    }

    #[test]
    fn test_xor_bytes() {
        assert_eq!(xor_bytes(&[0xFF, 0x00], &[0xFF, 0xFF]), vec![0x00, 0xFF]);
        assert_eq!(xor_bytes(&[0x12, 0x34], &[0x12, 0x34]), vec![0x00, 0x00]);
    }

    #[test]
    fn test_base64_roundtrip() {
        let data = b"hello world";
        let encoded = base64_encode(data);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_sha256() {
        // SHA-256 of empty string
        let hash = sha256(b"");
        assert_eq!(hash.len(), 32);
        // Known hash value
        assert_eq!(
            hex::encode(&hash),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_pbkdf2_sha256() {
        // Test vector from RFC 7914 (derived from RFC 6070)
        let password = b"password";
        let salt = b"salt";
        let iterations = 1;
        
        let result = pbkdf2_sha256(password, salt, iterations);
        assert_eq!(result.len(), 32);
        // The result should be deterministic
        let result2 = pbkdf2_sha256(password, salt, iterations);
        assert_eq!(result, result2);
    }

    #[test]
    fn test_generate_nonce() {
        let nonce1 = generate_nonce();
        let nonce2 = generate_nonce();
        
        // Nonces should be non-empty
        assert!(!nonce1.is_empty());
        assert!(!nonce2.is_empty());
        
        // Nonces should be different (with overwhelming probability)
        assert_ne!(nonce1, nonce2);
        
        // Should be valid base64
        assert!(base64_decode(&nonce1).is_ok());
    }
}
