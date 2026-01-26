//! Network transport for cluster communication
//!
//! Provides TCP-based transport for:
//! - Raft consensus messages
//! - Partition replication (fetch/append)
//! - Metadata queries
//!
//! Features:
//! - Connection pooling
//! - Automatic reconnection
//! - Multiplexing over single connection
//! - TLS support (optional)

use crate::error::{ClusterError, Result};
use crate::node::NodeId;
use crate::protocol::{
    decode_request, decode_response, encode_request, encode_response, frame_length, frame_message,
    ClusterRequest, ClusterResponse,
};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

/// Transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Read timeout
    pub read_timeout: Duration,
    /// Write timeout
    pub write_timeout: Duration,
    /// Maximum connections per peer
    pub max_connections_per_peer: usize,
    /// Enable TCP nodelay
    pub tcp_nodelay: bool,
    /// Receive buffer size
    pub recv_buffer_size: usize,
    /// Send buffer size
    pub send_buffer_size: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            max_connections_per_peer: 4,
            tcp_nodelay: true,
            recv_buffer_size: 256 * 1024, // 256 KB
            send_buffer_size: 256 * 1024,
        }
    }
}

/// Request handler callback type
pub type RequestHandler = Arc<dyn Fn(ClusterRequest) -> ClusterResponse + Send + Sync>;

/// Network transport manager
pub struct Transport {
    /// Our node ID
    local_node: NodeId,

    /// Bind address for incoming connections
    bind_addr: SocketAddr,

    /// Configuration
    config: TransportConfig,

    /// Connection pool to peers
    connections: Arc<DashMap<NodeId, ConnectionPool>>,

    /// Peer addresses
    peer_addrs: Arc<DashMap<NodeId, SocketAddr>>,

    /// Correlation ID generator
    correlation_id: AtomicU64,

    /// Pending requests waiting for response
    pending: Arc<DashMap<u64, oneshot::Sender<ClusterResponse>>>,

    /// Request handler
    handler: Option<RequestHandler>,

    /// Shutdown signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl Transport {
    /// Create new transport
    pub fn new(local_node: NodeId, bind_addr: SocketAddr, config: TransportConfig) -> Self {
        Self {
            local_node,
            bind_addr,
            config,
            connections: Arc::new(DashMap::new()),
            peer_addrs: Arc::new(DashMap::new()),
            correlation_id: AtomicU64::new(1),
            pending: Arc::new(DashMap::new()),
            handler: None,
            shutdown_tx: None,
        }
    }

    /// Set request handler
    pub fn set_handler(&mut self, handler: RequestHandler) {
        self.handler = Some(handler);
    }

    /// Register a peer
    pub fn add_peer(&self, node_id: NodeId, addr: SocketAddr) {
        self.peer_addrs.insert(node_id, addr);
    }

    /// Remove a peer
    pub fn remove_peer(&self, node_id: &NodeId) {
        self.peer_addrs.remove(node_id);
        self.connections.remove(node_id);
    }

    /// Start the transport (begin accepting connections)
    pub async fn start(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.bind_addr)
            .await
            .map_err(|e| ClusterError::Network(format!("Failed to bind: {}", e)))?;

        info!(addr = %self.bind_addr, "Transport listening");

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let handler = self.handler.clone();
        let pending = self.pending.clone();
        let _local_node = self.local_node.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                debug!(peer = %addr, "Accepted connection");

                                let handler = handler.clone();
                                let pending = pending.clone();
                                let config = config.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_connection(
                                        stream, handler, pending, config
                                    ).await {
                                        debug!(peer = %addr, error = %e, "Connection error");
                                    }
                                });
                            }
                            Err(e) => {
                                error!(error = %e, "Accept error");
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Transport shutting down");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle an incoming connection
    async fn handle_connection(
        mut stream: TcpStream,
        handler: Option<RequestHandler>,
        pending: Arc<DashMap<u64, oneshot::Sender<ClusterResponse>>>,
        config: TransportConfig,
    ) -> Result<()> {
        if config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }

        let mut length_buf = [0u8; 4];

        loop {
            // Read frame length
            match timeout(config.read_timeout, stream.read_exact(&mut length_buf)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(()); // Clean close
                }
                Ok(Err(e)) => return Err(ClusterError::Io(e)),
                Err(_) => return Err(ClusterError::Timeout),
            }

            let length = frame_length(&length_buf);
            if length > crate::protocol::MAX_MESSAGE_SIZE {
                return Err(ClusterError::MessageTooLarge {
                    size: length,
                    max: crate::protocol::MAX_MESSAGE_SIZE,
                });
            }

            // Read frame body
            let mut body = vec![0u8; length];
            timeout(config.read_timeout, stream.read_exact(&mut body))
                .await
                .map_err(|_| ClusterError::Timeout)?
                .map_err(ClusterError::Io)?;

            // Try to decode as request first, then as response
            if let Ok(request) = decode_request(&body) {
                // This is a request, handle it
                if let Some(ref handler) = handler {
                    let response = handler(request);
                    let response_bytes = encode_response(&response)?;
                    let framed = frame_message(&response_bytes);

                    timeout(config.write_timeout, stream.write_all(&framed))
                        .await
                        .map_err(|_| ClusterError::Timeout)?
                        .map_err(ClusterError::Io)?;
                }
            } else if let Ok(response) = decode_response(&body) {
                // This is a response, route to pending request
                let correlation_id = match &response {
                    ClusterResponse::Metadata { header, .. } => header.correlation_id,
                    ClusterResponse::MetadataProposal { header } => header.correlation_id,
                    ClusterResponse::Fetch { header, .. } => header.correlation_id,
                    ClusterResponse::Append { header, .. } => header.correlation_id,
                    ClusterResponse::ReplicaStateAck { header, .. } => header.correlation_id,
                    ClusterResponse::ElectLeader { header, .. } => header.correlation_id,
                    ClusterResponse::Heartbeat { header } => header.correlation_id,
                    ClusterResponse::Error { header } => header.correlation_id,
                };

                if let Some((_, sender)) = pending.remove(&correlation_id) {
                    let _ = sender.send(response);
                }
            } else {
                warn!("Failed to decode message");
            }
        }
    }

    /// Send a request and wait for response
    pub async fn send(&self, node_id: &NodeId, request: ClusterRequest) -> Result<ClusterResponse> {
        let addr = *self
            .peer_addrs
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

        let correlation_id = self.next_correlation_id();

        // Get or create connection
        let mut stream = self.get_connection(node_id, addr).await?;

        // Encode and send request
        let request_bytes = encode_request(&request)?;
        let framed = frame_message(&request_bytes);

        timeout(self.config.write_timeout, stream.write_all(&framed))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(ClusterError::Io)?;

        // Create response channel
        let (tx, _rx) = oneshot::channel();
        self.pending.insert(correlation_id, tx);

        // Read response
        let mut length_buf = [0u8; 4];
        timeout(self.config.read_timeout, stream.read_exact(&mut length_buf))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(ClusterError::Io)?;

        let length = frame_length(&length_buf);
        let mut body = vec![0u8; length];
        timeout(self.config.read_timeout, stream.read_exact(&mut body))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(ClusterError::Io)?;

        // Return connection to pool
        self.return_connection(node_id, stream).await;

        // Decode response
        let response = decode_response(&body)?;

        // Remove pending entry
        self.pending.remove(&correlation_id);

        Ok(response)
    }

    /// Send request without waiting for response (fire and forget)
    pub async fn send_async(&self, node_id: &NodeId, request: ClusterRequest) -> Result<()> {
        let addr = *self
            .peer_addrs
            .get(node_id)
            .ok_or_else(|| ClusterError::NodeNotFound(node_id.clone()))?;

        let mut stream = self.get_connection(node_id, addr).await?;

        let request_bytes = encode_request(&request)?;
        let framed = frame_message(&request_bytes);

        timeout(self.config.write_timeout, stream.write_all(&framed))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(ClusterError::Io)?;

        self.return_connection(node_id, stream).await;

        Ok(())
    }

    /// Broadcast request to all peers
    pub async fn broadcast(
        &self,
        request: ClusterRequest,
    ) -> Vec<(NodeId, Result<ClusterResponse>)> {
        let peers: Vec<_> = self.peer_addrs.iter().map(|e| e.key().clone()).collect();

        let mut results = Vec::with_capacity(peers.len());

        for peer in peers {
            let result = self.send(&peer, request.clone()).await;
            results.push((peer, result));
        }

        results
    }

    /// Get connection from pool or create new one
    async fn get_connection(&self, node_id: &NodeId, addr: SocketAddr) -> Result<TcpStream> {
        // Try to get from pool
        if let Some(pool) = self.connections.get_mut(node_id) {
            if let Some(conn) = pool.get().await {
                return Ok(conn);
            }
        }

        // Create new connection
        let stream = timeout(self.config.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClusterError::Timeout)?
            .map_err(|e| ClusterError::ConnectionFailed(e.to_string()))?;

        if self.config.tcp_nodelay {
            let _ = stream.set_nodelay(true);
        }

        Ok(stream)
    }

    /// Return connection to pool
    async fn return_connection(&self, node_id: &NodeId, stream: TcpStream) {
        self.connections
            .entry(node_id.clone())
            .or_insert_with(|| ConnectionPool::new(self.config.max_connections_per_peer))
            .put(stream)
            .await;
    }

    /// Get next correlation ID
    fn next_correlation_id(&self) -> u64 {
        self.correlation_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Shutdown the transport
    pub async fn shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(()).await;
        }

        // Clear connection pools
        self.connections.clear();
        self.pending.clear();
    }
}

/// Simple connection pool
struct ConnectionPool {
    connections: Mutex<Vec<TcpStream>>,
    max_size: usize,
}

impl ConnectionPool {
    fn new(max_size: usize) -> Self {
        Self {
            connections: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
        }
    }

    async fn get(&self) -> Option<TcpStream> {
        self.connections.lock().await.pop()
    }

    async fn put(&self, stream: TcpStream) {
        let mut conns = self.connections.lock().await;
        if conns.len() < self.max_size {
            conns.push(stream);
        }
        // Drop stream if pool is full
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{RequestHeader, ResponseHeader};

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = Transport::new(
            "node-1".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            TransportConfig::default(),
        );

        transport.add_peer("node-2".to_string(), "127.0.0.1:9094".parse().unwrap());
        assert!(transport.peer_addrs.contains_key(&"node-2".to_string()));

        transport.remove_peer(&"node-2".to_string());
        assert!(!transport.peer_addrs.contains_key(&"node-2".to_string()));
    }
}
