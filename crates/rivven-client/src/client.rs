use crate::{Error, MessageData, Request, Response, Result};
use bytes::Bytes;
use rivven_core::PasswordHash;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

#[cfg(feature = "tls")]
use std::net::SocketAddr;

#[cfg(feature = "tls")]
use rivven_core::tls::{TlsClientStream, TlsConfig, TlsConnector};

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
    next_correlation_id: u32,
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
            next_correlation_id: 0,
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
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| Error::ConnectionError(format!("Invalid address: {}", e)))?;

        // Create TLS connector
        let connector = TlsConnector::new(tls_config)
            .map_err(|e| Error::ConnectionError(format!("TLS config error: {}", e)))?;

        // Connect with TLS
        let tls_stream = connector
            .connect_tcp(socket_addr, server_name)
            .await
            .map_err(|e| Error::ConnectionError(format!("TLS connection error: {}", e)))?;

        info!("TLS connection established to {} ({})", addr, server_name);

        Ok(Self {
            stream: ClientStream::Tls(tls_stream),
            next_correlation_id: 0,
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
            Response::Authenticated {
                session_id,
                expires_in,
            } => {
                info!("Authenticated as '{}'", username);
                Ok(AuthSession {
                    session_id,
                    expires_in,
                })
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
    pub async fn authenticate_scram(
        &mut self,
        username: &str,
        password: &str,
    ) -> Result<AuthSession> {
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
            Response::ScramServerFirst { message } => String::from_utf8(message.to_vec())
                .map_err(|_| Error::AuthenticationFailed("Invalid server-first encoding".into()))?,
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
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

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
            Response::ScramServerFinal {
                message,
                session_id,
                expires_in,
            } => {
                let server_final = String::from_utf8(message.to_vec()).map_err(|_| {
                    Error::AuthenticationFailed("Invalid server-final encoding".into())
                })?;

                // Check for error response
                if let Some(error_msg) = server_final.strip_prefix("e=") {
                    return Err(Error::AuthenticationFailed(error_msg.to_string()));
                }

                // Verify server signature (mutual authentication)
                if let Some(verifier_b64) = server_final.strip_prefix("v=") {
                    let server_key = PasswordHash::hmac_sha256(&salted_password, b"Server Key");
                    let expected_server_sig =
                        PasswordHash::hmac_sha256(&server_key, auth_message.as_bytes());
                    let expected_verifier = base64_encode(&expected_server_sig);

                    if verifier_b64 != expected_verifier {
                        return Err(Error::AuthenticationFailed(
                            "Server verification failed".into(),
                        ));
                    }
                }

                let session_id = session_id.ok_or_else(|| {
                    Error::AuthenticationFailed("No session ID in response".into())
                })?;
                let expires_in = expires_in
                    .ok_or_else(|| Error::AuthenticationFailed("No expiry in response".into()))?;

                info!("SCRAM authentication successful for '{}'", username);
                Ok(AuthSession {
                    session_id,
                    expires_in,
                })
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
        // Generate sequential correlation ID
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);

        // Serialize request with wire format prefix and correlation ID
        let request_bytes =
            request.to_wire(rivven_protocol::WireFormat::Postcard, correlation_id)?;

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

        // Deserialize response (auto-detects wire format)
        let (response, _format, _correlation_id) = Response::from_wire(&response_buf)?;

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
    ///
    /// Uses read_uncommitted isolation level (default).
    /// For transactional consumers that should not see aborted transaction messages,
    /// use [`Self::consume_with_isolation`] with `isolation_level = 1` (read_committed).
    pub async fn consume(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        self.consume_with_isolation(topic, partition, offset, max_messages, None)
            .await
    }

    /// Consume messages from a topic partition with specified isolation level
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `offset` - Starting offset
    /// * `max_messages` - Maximum messages to return
    /// * `isolation_level` - Transaction isolation level:
    ///   - `None` or `Some(0)` = read_uncommitted (default): Returns all messages
    ///   - `Some(1)` = read_committed: Filters out messages from aborted transactions
    ///
    /// # Read Committed Isolation
    ///
    /// When using `isolation_level = Some(1)` (read_committed), the consumer will:
    /// - Not see messages from transactions that were aborted
    /// - Not see control records (transaction markers)
    /// - Only see committed transactional messages
    ///
    /// This is essential for exactly-once semantics (EOS) consumers.
    pub async fn consume_with_isolation(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
        isolation_level: Option<u8>,
    ) -> Result<Vec<MessageData>> {
        let request = Request::Consume {
            topic: topic.into(),
            partition,
            offset,
            max_messages,
            isolation_level,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::Messages { messages } => Ok(messages),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Consume messages with read_committed isolation level
    ///
    /// This is a convenience method for transactional consumers that should
    /// only see committed messages. Messages from aborted transactions are filtered out.
    ///
    /// Equivalent to calling [`Self::consume_with_isolation`] with `isolation_level = Some(1)`.
    pub async fn consume_read_committed(
        &mut self,
        topic: impl Into<String>,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<MessageData>> {
        self.consume_with_isolation(topic, partition, offset, max_messages, Some(1))
            .await
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

    /// Register a schema with the schema registry (via HTTP REST API)
    ///
    /// The schema registry runs as a separate service (`rivven-schema`) with a
    /// Confluent-compatible REST API. This method performs a POST to
    /// `{registry_url}/subjects/{subject}/versions`.
    ///
    /// Supports both `http://` and `https://` registry URLs. HTTPS requires the
    /// `schema-registry` feature which brings in `reqwest` with `rustls-tls`.
    /// Without the feature flag, a minimal inline HTTP/1.1 client is used (HTTP only).
    ///
    /// # Arguments
    /// * `registry_url` - Schema registry base URL (e.g., `http://localhost:8081` or `https://registry.example.com`)
    /// * `subject` - Subject name (typically `{topic}-key` or `{topic}-value`)
    /// * `schema_type` - Schema format: `"AVRO"`, `"PROTOBUF"`, or `"JSON"`
    /// * `schema` - The schema definition string
    ///
    /// # Returns
    /// The global schema ID on success.
    pub async fn register_schema(
        &self,
        registry_url: &str,
        subject: &str,
        schema_type: &str,
        schema: &str,
    ) -> Result<u32> {
        let url = registry_url.trim_end_matches('/');
        let endpoint = format!("{}/subjects/{}/versions", url, subject);

        let body = serde_json::json!({
            "schema": schema,
            "schemaType": schema_type,
        });

        #[cfg(feature = "schema-registry")]
        {
            self.register_schema_reqwest(&endpoint, &body).await
        }

        #[cfg(not(feature = "schema-registry"))]
        {
            self.register_schema_inline(url, &endpoint, &body).await
        }
    }

    /// HTTPS-capable schema registration using reqwest (requires `schema-registry` feature)
    #[cfg(feature = "schema-registry")]
    async fn register_schema_reqwest(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<u32> {
        let client = reqwest::Client::new();
        let response = client
            .post(endpoint)
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .json(body)
            .send()
            .await
            .map_err(|e| Error::ConnectionError(format!("schema registry request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(Error::ServerError(format!(
                "schema registry returned HTTP {status}: {body_text}"
            )));
        }

        #[derive(serde::Deserialize)]
        struct RegisterResponse {
            id: u32,
        }

        let result: RegisterResponse = response
            .json()
            .await
            .map_err(|e| Error::ConnectionError(format!("failed to parse response: {e}")))?;

        Ok(result.id)
    }

    /// Minimal inline HTTP/1.1 client for schema registration (HTTP only, no external deps)
    #[cfg(not(feature = "schema-registry"))]
    async fn register_schema_inline(
        &self,
        base_url: &str,
        _endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<u32> {
        use tokio::net::TcpStream as TokioTcpStream;

        let stripped = base_url.strip_prefix("http://").ok_or_else(|| {
            Error::ConnectionError(
                "HTTPS requires the `schema-registry` feature; URL must start with http:// without it".into(),
            )
        })?;
        let (host_port, _) = stripped.split_once('/').unwrap_or((stripped, ""));

        // Extract path from endpoint (skip the base URL part)
        let path = _endpoint.strip_prefix(base_url).unwrap_or(_endpoint);

        let body_bytes = serde_json::to_vec(body)
            .map_err(|e| Error::ConnectionError(format!("failed to serialize schema: {e}")))?;

        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            path, host_port, body_bytes.len()
        );

        let mut stream = TokioTcpStream::connect(host_port).await.map_err(|e| {
            Error::ConnectionError(format!("failed to connect to schema registry: {e}"))
        })?;

        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| Error::ConnectionError(format!("failed to send request: {e}")))?;
        stream
            .write_all(&body_bytes)
            .await
            .map_err(|e| Error::ConnectionError(format!("failed to send body: {e}")))?;

        let mut response_buf = Vec::with_capacity(4096);
        stream
            .read_to_end(&mut response_buf)
            .await
            .map_err(|e| Error::ConnectionError(format!("failed to read response: {e}")))?;

        let response_str = String::from_utf8_lossy(&response_buf);

        let status_line = response_str.lines().next().unwrap_or("");
        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if !(200..300).contains(&status_code) {
            return Err(Error::ServerError(format!(
                "schema registry returned HTTP {status_code}"
            )));
        }

        let json_body = response_str
            .find("\r\n\r\n")
            .map(|i| &response_str[i + 4..])
            .unwrap_or("");

        #[derive(serde::Deserialize)]
        struct RegisterResponse {
            id: u32,
        }

        let result: RegisterResponse = serde_json::from_str(json_body.trim()).map_err(|e| {
            Error::ConnectionError(format!("failed to parse schema registry response: {e}"))
        })?;

        Ok(result.id)
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

    // ========================================================================
    // Admin API
    // ========================================================================

    /// Describe topic configurations
    ///
    /// Returns the current configuration for the specified topics.
    ///
    /// # Arguments
    /// * `topics` - Topics to describe (empty slice = all topics)
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let configs = client.describe_topic_configs(&["orders", "events"]).await?;
    /// for (topic, config) in configs {
    ///     println!("{}: {:?}", topic, config);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe_topic_configs(
        &mut self,
        topics: &[&str],
    ) -> Result<std::collections::HashMap<String, std::collections::HashMap<String, String>>> {
        let request = Request::DescribeTopicConfigs {
            topics: topics.iter().map(|s| s.to_string()).collect(),
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TopicConfigsDescribed { configs } => {
                let mut result = std::collections::HashMap::new();
                for desc in configs {
                    let mut topic_configs = std::collections::HashMap::new();
                    for (key, value) in desc.configs {
                        topic_configs.insert(key, value.value);
                    }
                    result.insert(desc.topic, topic_configs);
                }
                Ok(result)
            }
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Alter topic configuration
    ///
    /// Modifies configuration for an existing topic. Pass `None` as value to reset
    /// a configuration key to its default.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `configs` - Configuration changes: (key, value) pairs. Use `None` to reset to default.
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let result = client.alter_topic_config("orders", &[
    ///     ("retention.ms", Some("86400000")),  // 1 day retention
    ///     ("cleanup.policy", Some("compact")), // Enable compaction
    /// ]).await?;
    /// println!("Changed {} configs", result.changed_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn alter_topic_config(
        &mut self,
        topic: impl Into<String>,
        configs: &[(&str, Option<&str>)],
    ) -> Result<AlterTopicConfigResult> {
        use rivven_protocol::TopicConfigEntry;

        let request = Request::AlterTopicConfig {
            topic: topic.into(),
            configs: configs
                .iter()
                .map(|(k, v)| TopicConfigEntry {
                    key: k.to_string(),
                    value: v.map(|s| s.to_string()),
                })
                .collect(),
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TopicConfigAltered {
                topic,
                changed_count,
            } => Ok(AlterTopicConfigResult {
                topic,
                changed_count,
            }),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Create additional partitions for an existing topic
    ///
    /// Increases the partition count for a topic. The new partition count
    /// must be greater than the current count (you cannot reduce partitions).
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `new_partition_count` - New total partition count
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// // Increase from 3 to 6 partitions
    /// let new_count = client.create_partitions("orders", 6).await?;
    /// println!("Topic now has {} partitions", new_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_partitions(
        &mut self,
        topic: impl Into<String>,
        new_partition_count: u32,
    ) -> Result<u32> {
        let request = Request::CreatePartitions {
            topic: topic.into(),
            new_partition_count,
            assignments: vec![], // Let broker auto-assign
        };

        let response = self.send_request(request).await?;

        match response {
            Response::PartitionsCreated {
                new_partition_count,
                ..
            } => Ok(new_partition_count),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Delete records before a given offset (log truncation)
    ///
    /// Removes all records with offsets less than the specified offset for each
    /// partition. This is useful for freeing up disk space or removing old data.
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `partition_offsets` - List of (partition, before_offset) pairs
    ///
    /// # Returns
    /// A list of results indicating the new low watermark for each partition.
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// // Delete records before offset 1000 on partitions 0, 1, 2
    /// let results = client.delete_records("orders", &[
    ///     (0, 1000),
    ///     (1, 1000),
    ///     (2, 1000),
    /// ]).await?;
    /// for r in results {
    ///     println!("Partition {}: low watermark now {}", r.partition, r.low_watermark);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_records(
        &mut self,
        topic: impl Into<String>,
        partition_offsets: &[(u32, u64)],
    ) -> Result<Vec<DeleteRecordsResult>> {
        let request = Request::DeleteRecords {
            topic: topic.into(),
            partition_offsets: partition_offsets.to_vec(),
        };

        let response = self.send_request(request).await?;

        match response {
            Response::RecordsDeleted { results, .. } => Ok(results),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    // =========================================================================
    // Idempotent Producer API
    // =========================================================================

    /// Initialize an idempotent producer
    ///
    /// Returns a producer ID and epoch that should be used for all subsequent
    /// idempotent publish operations. The broker uses these to detect and
    /// deduplicate messages in case of retries.
    ///
    /// # Arguments
    /// * `previous_producer_id` - If reconnecting, pass the previous producer_id
    ///   to bump the epoch (prevents zombie producers)
    ///
    /// # Returns
    /// `ProducerState` containing the producer_id and producer_epoch
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let producer = client.init_producer_id(None).await?;
    /// println!("Producer ID: {}, Epoch: {}", producer.producer_id, producer.producer_epoch);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn init_producer_id(
        &mut self,
        previous_producer_id: Option<u64>,
    ) -> Result<ProducerState> {
        let request = Request::InitProducerId {
            producer_id: previous_producer_id,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::ProducerIdInitialized {
                producer_id,
                producer_epoch,
            } => Ok(ProducerState {
                producer_id,
                producer_epoch,
                next_sequence: 0,
            }),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Publish a message with idempotent semantics
    ///
    /// Uses producer_id/epoch/sequence for exactly-once delivery. The broker
    /// deduplicates messages based on these values, making retries safe.
    ///
    /// # Arguments
    /// * `topic` - Topic to publish to
    /// * `key` - Optional message key (used for partitioning)
    /// * `value` - Message payload
    /// * `producer` - Producer state from `init_producer_id`
    ///
    /// # Returns
    /// Tuple of (offset, partition, was_duplicate)
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let mut producer = client.init_producer_id(None).await?;
    ///
    /// let (offset, partition, duplicate) = client
    ///     .publish_idempotent("orders", None::<Vec<u8>>, b"order-1".to_vec(), &mut producer)
    ///     .await?;
    ///
    /// println!("Published to partition {} at offset {}", partition, offset);
    /// if duplicate {
    ///     println!("(This was a retry - message already existed)");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn publish_idempotent(
        &mut self,
        topic: impl Into<String>,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
        producer: &mut ProducerState,
    ) -> Result<(u64, u32, bool)> {
        let sequence = producer.next_sequence;
        producer.next_sequence = producer.next_sequence.wrapping_add(1);
        // After wrapping past i32::MAX, reset to 1 (not 0)
        // because sequence 0 was used for the first message. Reusing 0
        // could collide with the broker's dedup window.
        if producer.next_sequence <= 0 {
            producer.next_sequence = 1;
        }

        let request = Request::IdempotentPublish {
            topic: topic.into(),
            partition: None,
            key: key.map(|k| k.into()),
            value: value.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::IdempotentPublished {
                offset,
                partition,
                duplicate,
            } => Ok((offset, partition, duplicate)),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    // =========================================================================
    // Transaction API
    // =========================================================================

    /// Begin a new transaction
    ///
    /// Starts a transaction that can span multiple topics and partitions.
    /// All writes within the transaction are atomic - they either all succeed
    /// or all fail together.
    ///
    /// # Arguments
    /// * `txn_id` - Unique transaction identifier (should be stable per producer)
    /// * `producer` - Producer state from `init_producer_id`
    /// * `timeout_ms` - Optional transaction timeout (defaults to 60s)
    ///
    /// # Example
    /// ```no_run
    /// # use rivven_client::Client;
    /// # async fn example() -> rivven_client::Result<()> {
    /// let mut client = Client::connect("127.0.0.1:9092").await?;
    /// let producer = client.init_producer_id(None).await?;
    ///
    /// // Start a transaction
    /// client.begin_transaction("txn-1", &producer, None).await?;
    /// // ... publish messages ...
    /// client.commit_transaction("txn-1", &producer).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn begin_transaction(
        &mut self,
        txn_id: impl Into<String>,
        producer: &ProducerState,
        timeout_ms: Option<u64>,
    ) -> Result<()> {
        let request = Request::BeginTransaction {
            txn_id: txn_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            timeout_ms,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TransactionStarted { .. } => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Add partitions to an active transaction
    ///
    /// Registers partitions that will be written to within the transaction.
    /// This must be called before publishing to a new partition.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction identifier
    /// * `producer` - Producer state
    /// * `partitions` - List of (topic, partition) pairs to add
    pub async fn add_partitions_to_txn(
        &mut self,
        txn_id: impl Into<String>,
        producer: &ProducerState,
        partitions: &[(&str, u32)],
    ) -> Result<usize> {
        let request = Request::AddPartitionsToTxn {
            txn_id: txn_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            partitions: partitions
                .iter()
                .map(|(t, p)| (t.to_string(), *p))
                .collect(),
        };

        let response = self.send_request(request).await?;

        match response {
            Response::PartitionsAddedToTxn {
                partition_count, ..
            } => Ok(partition_count),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Publish a message within a transaction
    ///
    /// Like `publish_idempotent`, but the message is only visible to consumers
    /// after the transaction is committed.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction identifier
    /// * `topic` - Topic to publish to
    /// * `key` - Optional message key
    /// * `value` - Message payload
    /// * `producer` - Producer state with sequence tracking
    ///
    /// # Returns
    /// Tuple of (offset, partition, sequence) - offset is pending until commit
    pub async fn publish_transactional(
        &mut self,
        txn_id: impl Into<String>,
        topic: impl Into<String>,
        key: Option<impl Into<Bytes>>,
        value: impl Into<Bytes>,
        producer: &mut ProducerState,
    ) -> Result<(u64, u32, i32)> {
        let sequence = producer.next_sequence;
        producer.next_sequence = producer.next_sequence.wrapping_add(1);
        // Avoid reusing sequence 0 after wrap
        if producer.next_sequence <= 0 {
            producer.next_sequence = 1;
        }

        let request = Request::TransactionalPublish {
            txn_id: txn_id.into(),
            topic: topic.into(),
            partition: None,
            key: key.map(|k| k.into()),
            value: value.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            sequence,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TransactionalPublished {
                offset,
                partition,
                sequence,
            } => Ok((offset, partition, sequence)),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Add consumer offsets to a transaction
    ///
    /// For exactly-once consume-transform-produce patterns: commits consumer
    /// offsets atomically with the produced messages.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction identifier
    /// * `producer` - Producer state
    /// * `group_id` - Consumer group ID
    /// * `offsets` - List of (topic, partition, offset) to commit
    pub async fn add_offsets_to_txn(
        &mut self,
        txn_id: impl Into<String>,
        producer: &ProducerState,
        group_id: impl Into<String>,
        offsets: &[(&str, u32, i64)],
    ) -> Result<()> {
        let request = Request::AddOffsetsToTxn {
            txn_id: txn_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            group_id: group_id.into(),
            offsets: offsets
                .iter()
                .map(|(t, p, o)| (t.to_string(), *p, *o))
                .collect(),
        };

        let response = self.send_request(request).await?;

        match response {
            Response::OffsetsAddedToTxn { .. } => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Commit a transaction
    ///
    /// Makes all writes in the transaction visible to consumers atomically.
    /// If this fails, the transaction should be aborted.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction identifier
    /// * `producer` - Producer state
    pub async fn commit_transaction(
        &mut self,
        txn_id: impl Into<String>,
        producer: &ProducerState,
    ) -> Result<()> {
        let request = Request::CommitTransaction {
            txn_id: txn_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TransactionCommitted { .. } => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }

    /// Abort a transaction
    ///
    /// Discards all writes in the transaction. Call this if any write fails
    /// or if you need to cancel the transaction.
    ///
    /// # Arguments
    /// * `txn_id` - Transaction identifier
    /// * `producer` - Producer state
    pub async fn abort_transaction(
        &mut self,
        txn_id: impl Into<String>,
        producer: &ProducerState,
    ) -> Result<()> {
        let request = Request::AbortTransaction {
            txn_id: txn_id.into(),
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
        };

        let response = self.send_request(request).await?;

        match response {
            Response::TransactionAborted { .. } => Ok(()),
            Response::Error { message } => Err(Error::ServerError(message)),
            _ => Err(Error::InvalidResponse),
        }
    }
}

/// State for an idempotent/transactional producer
#[derive(Debug, Clone)]
pub struct ProducerState {
    /// Producer ID assigned by the broker
    pub producer_id: u64,
    /// Current epoch (increments on reconnect)
    pub producer_epoch: u16,
    /// Next sequence number to use (per-producer, not per-partition)
    pub next_sequence: i32,
}

/// Result of altering topic configuration
#[derive(Debug, Clone)]
pub struct AlterTopicConfigResult {
    /// Topic name
    pub topic: String,
    /// Number of configurations changed
    pub changed_count: usize,
}

/// Result of deleting records from a partition
pub use rivven_protocol::DeleteRecordsResult;

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
    username.replace('=', "=3D").replace(',', "=2C")
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
            iterations = Some(
                value
                    .parse::<u32>()
                    .map_err(|_| Error::AuthenticationFailed("Invalid iteration count".into()))?,
            );
        }
    }

    let nonce = nonce.ok_or_else(|| Error::AuthenticationFailed("Missing nonce".into()))?;
    let salt = salt.ok_or_else(|| Error::AuthenticationFailed("Missing salt".into()))?;
    let iterations =
        iterations.ok_or_else(|| Error::AuthenticationFailed("Missing iterations".into()))?;

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
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(data)
}

/// Base64 decode
fn base64_decode(data: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::STANDARD, Engine};
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
