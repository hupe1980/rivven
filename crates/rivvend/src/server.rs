use crate::handler::RequestHandler;
use crate::protocol::Request;
use bytes::BytesMut;
use rivven_core::{storage::TieredStorage, Config, OffsetManager, TopicManager};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

/// Rivven server
pub struct Server {
    #[allow(dead_code)]
    config: Config,
    topic_manager: TopicManager,
    offset_manager: OffsetManager,
    listener: Option<TcpListener>,
    #[allow(dead_code)]
    tiered_storage: Option<Arc<TieredStorage>>,
}

impl Server {
    /// Create a new server with the given configuration
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        // Initialize tiered storage if enabled
        let tiered_storage = if config.tiered_storage.enabled {
            info!(
                "Initializing tiered storage with config: {:?}",
                config.tiered_storage
            );
            let ts = TieredStorage::new(config.tiered_storage.clone()).await?;
            Some(ts)
        } else {
            None
        };

        // Create TopicManager with or without tiered storage
        let topic_manager = if let Some(ref ts) = tiered_storage {
            TopicManager::new_with_tiered_storage(config.clone(), ts.clone())
        } else {
            TopicManager::new(config.clone())
        };

        // Recover persisted topics from disk
        if let Err(e) = topic_manager.recover().await {
            warn!("Failed to recover topics from disk: {}", e);
        }

        let offset_manager = OffsetManager::new();

        // Pre-bind the listener so we can report the actual address
        let addr = config.server_address();
        let listener = TcpListener::bind(&addr).await?;

        Ok(Self {
            config,
            topic_manager,
            offset_manager,
            listener: Some(listener),
            tiered_storage,
        })
    }

    /// Get the local address the server is bound to
    ///
    /// Useful for tests where port 0 is used for random port selection.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener
            .as_ref()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotConnected, "Server not bound")
            })
            .and_then(|l| l.local_addr())
    }

    /// Start the server
    pub async fn start(mut self) -> anyhow::Result<()> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| anyhow::anyhow!("Server already started"))?;

        let addr = listener.local_addr()?;
        info!("Starting Rivven server on {}", addr);

        let handler = Arc::new(RequestHandler::new(
            self.topic_manager.clone(),
            self.offset_manager.clone(),
        ));

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection from {}", addr);
                    let handler = handler.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, handler).await {
                            error!("Error handling connection from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut stream: TcpStream,
    handler: Arc<RequestHandler>,
) -> anyhow::Result<()> {
    let mut buffer = BytesMut::with_capacity(8192);

    loop {
        // Read length prefix (4 bytes)
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("Client disconnected");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let msg_len = u32::from_be_bytes(len_buf) as usize;

        if msg_len > 10 * 1024 * 1024 {
            warn!("Message too large: {} bytes", msg_len);
            return Ok(());
        }

        // Read message data
        buffer.clear();
        buffer.resize(msg_len, 0);
        stream.read_exact(&mut buffer).await?;

        // Deserialize request
        let request = match Request::from_bytes(&buffer) {
            Ok(req) => req,
            Err(e) => {
                error!("Failed to deserialize request: {}", e);
                continue;
            }
        };

        // Handle request
        let response = handler.handle(request).await;

        // Serialize response
        let response_bytes = match response.to_bytes() {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize response: {}", e);
                continue;
            }
        };

        // Write response with length prefix
        let len = response_bytes.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&response_bytes).await?;
        stream.flush().await?;
    }
}
