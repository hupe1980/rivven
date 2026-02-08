//! Broker client with reconnection support
//!
//! Provides a resilient wrapper around rivven-client that handles:
//! - Connection retries with exponential backoff
//! - Automatic reconnection on failure
//! - Health status tracking

use crate::config::{BrokerConfig, RetryConfig};
use crate::error::{ConnectError, ConnectorStatus, Result};
use rivven_client::Client;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

/// Resilient broker client with automatic reconnection
pub struct BrokerClient {
    config: BrokerConfig,
    retry_config: RetryConfig,
    client: Mutex<Option<Client>>,
    status: RwLock<ConnectorStatus>,
    reconnect_count: AtomicU64,
    last_error: RwLock<Option<String>>,
}

// Methods for health monitoring and observability (used by health.rs)
#[allow(dead_code)]
impl BrokerClient {
    /// Get current connection status
    pub async fn status(&self) -> ConnectorStatus {
        *self.status.read().await
    }

    /// Get reconnect count
    pub fn reconnect_count(&self) -> u64 {
        self.reconnect_count.load(Ordering::Relaxed)
    }

    /// Get last error message
    pub async fn last_error(&self) -> Option<String> {
        self.last_error.read().await.clone()
    }

    /// Close the connection
    pub async fn close(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
        *self.status.write().await = ConnectorStatus::Stopped;
    }

    /// Mark as unhealthy
    pub async fn mark_unhealthy(&self) {
        *self.status.write().await = ConnectorStatus::Unhealthy;
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        self.client.lock().await.is_some()
    }

    /// Publish a message with a key to a topic
    pub async fn publish_with_key(
        &self,
        topic: &str,
        key: Option<impl Into<bytes::Bytes>>,
        value: impl Into<bytes::Bytes>,
    ) -> Result<u64> {
        let mut guard = self.client.lock().await;
        let value = value.into();
        let key = key.map(|k| k.into());

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        match client.publish_with_key(topic, key, value).await {
            Ok(offset) => Ok(offset),
            Err(e) => {
                if Self::is_connection_error(&e) {
                    drop(guard);
                    self.invalidate().await;
                    *self.status.write().await = ConnectorStatus::Unhealthy;
                    Err(ConnectError::broker(format!(
                        "Connection error during publish: {}",
                        e
                    )))
                } else {
                    Err(ConnectError::broker(format!("Publish failed: {}", e)))
                }
            }
        }
    }
}

impl BrokerClient {
    /// Create a new broker client
    pub fn new(config: BrokerConfig, retry_config: RetryConfig) -> Self {
        Self {
            config,
            retry_config,
            client: Mutex::new(None),
            status: RwLock::new(ConnectorStatus::Starting),
            reconnect_count: AtomicU64::new(0),
            last_error: RwLock::new(None),
        }
    }

    /// Connect to the broker with bootstrap server failover and retries
    pub async fn connect(&self) -> Result<()> {
        let mut attempt = 0u32;
        let mut backoff = Duration::from_millis(self.retry_config.initial_backoff_ms);

        loop {
            attempt += 1;

            // Try each bootstrap server in order
            match self.try_connect_with_failover().await {
                Ok((client, server)) => {
                    let mut guard = self.client.lock().await;
                    *guard = Some(client);
                    *self.status.write().await = ConnectorStatus::Running;
                    *self.last_error.write().await = None;

                    if attempt > 1 {
                        info!("Connected to broker {} after {} attempts", server, attempt);
                        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        info!("Connected to broker: {}", server);
                    }
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    *self.last_error.write().await = Some(error_msg.clone());

                    if attempt >= self.retry_config.max_retries {
                        *self.status.write().await = ConnectorStatus::Failed;
                        return Err(ConnectError::broker(format!(
                            "Failed to connect to any bootstrap server after {} attempts: {}",
                            attempt, error_msg
                        )));
                    }

                    *self.status.write().await = ConnectorStatus::Unhealthy;
                    warn!(
                        "Broker connection attempt {}/{} failed: {}. Retrying in {:?}",
                        attempt, self.retry_config.max_retries, error_msg, backoff
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(
                        Duration::from_millis(
                            (backoff.as_millis() as f64 * self.retry_config.backoff_multiplier)
                                as u64,
                        ),
                        Duration::from_millis(self.retry_config.max_backoff_ms),
                    );
                }
            }
        }
    }

    /// Try connecting to bootstrap servers in order until one succeeds
    async fn try_connect_with_failover(
        &self,
    ) -> std::result::Result<(Client, String), rivven_client::Error> {
        let timeout = Duration::from_millis(self.config.connection_timeout_ms);
        let mut last_error = None;

        for server in &self.config.bootstrap_servers {
            info!("Attempting connection to bootstrap server: {}", server);

            match tokio::time::timeout(timeout, self.try_connect_to(server)).await {
                Ok(Ok(client)) => {
                    return Ok((client, server.clone()));
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to {}: {}", server, e);
                    last_error = Some(e);
                }
                Err(_) => {
                    warn!("Connection to {} timed out after {:?}", server, timeout);
                    last_error = Some(rivven_client::Error::ConnectionError(format!(
                        "Connection timeout after {:?}",
                        timeout
                    )));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            rivven_client::Error::ConnectionError("No bootstrap servers configured".to_string())
        }))
    }

    /// Try to establish a connection to a specific server
    async fn try_connect_to(
        &self,
        address: &str,
    ) -> std::result::Result<Client, rivven_client::Error> {
        #[cfg(feature = "tls")]
        if self.config.tls.enabled {
            return self.connect_tls_to(address).await;
        }

        Client::connect(address).await
    }

    #[cfg(feature = "tls")]
    async fn connect_tls_to(
        &self,
        address: &str,
    ) -> std::result::Result<Client, rivven_client::Error> {
        use rivven_client::TlsConfig;

        let tls_config = match (&self.config.tls.cert_path, &self.config.tls.key_path) {
            (Some(cert), Some(key)) => {
                if let Some(ca) = &self.config.tls.ca_path {
                    TlsConfig::mtls_from_pem_files(cert.clone(), key.clone(), ca.clone())
                } else {
                    TlsConfig::from_pem_files(cert.clone(), key.clone())
                }
            }
            _ => {
                let mut tls = TlsConfig {
                    enabled: true,
                    ..Default::default()
                };
                if let Some(ca) = &self.config.tls.ca_path {
                    tls.root_ca =
                        Some(rivven_core::tls::CertificateSource::File { path: ca.clone() });
                }
                if self.config.tls.insecure {
                    tls.insecure_skip_verify = true;
                }
                tls
            }
        };

        let server_name = self
            .config
            .tls
            .server_name
            .as_deref()
            .unwrap_or("localhost");
        Client::connect_tls(address, &tls_config, server_name).await
    }

    /// Check if an error indicates a connection problem
    pub fn is_connection_error(e: &rivven_client::Error) -> bool {
        // Check for common connection-related errors
        let msg = e.to_string().to_lowercase();
        msg.contains("connection")
            || msg.contains("refused")
            || msg.contains("timeout")
            || msg.contains("reset")
            || msg.contains("broken pipe")
            || msg.contains("eof")
    }

    /// Invalidate client to force reconnection
    pub async fn invalidate(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
    }

    /// Publish a message to a topic
    ///
    /// Returns Ok(offset) on success, or an error if publishing fails.
    /// On connection errors, the client is invalidated to trigger reconnection.
    pub async fn publish(&self, topic: &str, value: impl Into<bytes::Bytes>) -> Result<u64> {
        let mut guard = self.client.lock().await;
        let value = value.into();

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        match client.publish(topic, value).await {
            Ok(offset) => Ok(offset),
            Err(e) => {
                if Self::is_connection_error(&e) {
                    // Invalidate connection to trigger reconnect
                    drop(guard);
                    self.invalidate().await;
                    *self.status.write().await = ConnectorStatus::Unhealthy;
                    Err(ConnectError::broker(format!(
                        "Connection error during publish: {}",
                        e
                    )))
                } else {
                    Err(ConnectError::broker(format!("Publish failed: {}", e)))
                }
            }
        }
    }

    /// Create a topic if it doesn't exist
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<()> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        client
            .create_topic(name, Some(partitions))
            .await
            .map_err(|e| {
                ConnectError::Topic(format!("Failed to create topic '{}': {}", name, e))
            })?;

        Ok(())
    }

    /// Check if a topic exists
    pub async fn topic_exists(&self, name: &str) -> Result<bool> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        let topics = client
            .list_topics()
            .await
            .map_err(|e| ConnectError::broker(format!("Failed to list topics: {}", e)))?;

        Ok(topics.iter().any(|t| t == name))
    }

    /// List all topics
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        client
            .list_topics()
            .await
            .map_err(|e| ConnectError::broker(format!("Failed to list topics: {}", e)))
    }

    /// Consume messages from a topic/partition with batch size
    ///
    /// Returns consumed messages or an error. On connection errors, the client
    /// is invalidated to trigger reconnection.
    pub async fn consume_batch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_messages: usize,
    ) -> Result<Vec<rivven_client::MessageData>> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        match client.consume(topic, partition, offset, max_messages).await {
            Ok(messages) => Ok(messages),
            Err(e) => {
                if Self::is_connection_error(&e) {
                    drop(guard);
                    self.invalidate().await;
                    *self.status.write().await = ConnectorStatus::Unhealthy;
                    Err(ConnectError::broker(format!(
                        "Connection error during consume: {}",
                        e
                    )))
                } else {
                    Err(ConnectError::broker(format!("Consume failed: {}", e)))
                }
            }
        }
    }

    /// Commit consumer group offset
    pub async fn commit_offset(
        &self,
        consumer_group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        client
            .commit_offset(consumer_group, topic, partition, offset)
            .await
            .map_err(|e| ConnectError::broker(format!("Commit offset failed: {}", e)))
    }

    /// Get consumer group offset for a topic/partition
    pub async fn get_offset(
        &self,
        consumer_group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        match client.get_offset(consumer_group, topic, partition).await {
            Ok(offset) => Ok(offset),
            Err(e) => {
                // Offset not found is not an error, just return None
                let msg = e.to_string().to_lowercase();
                if msg.contains("not found") || msg.contains("no offset") {
                    Ok(None)
                } else {
                    Err(ConnectError::broker(format!("Get offset failed: {}", e)))
                }
            }
        }
    }

    /// Get earliest and latest offsets for a topic/partition
    /// Get the number of partitions for a topic
    pub async fn get_topic_partition_count(&self, topic: &str) -> Result<u32> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        let (_name, partitions) = client.get_metadata(topic).await.map_err(|e| {
            ConnectError::broker(format!("Failed to get metadata for '{}': {}", topic, e))
        })?;

        Ok(partitions)
    }

    ///
    /// Returns (earliest, latest) where:
    /// - earliest: First available offset (messages before this are deleted/compacted)
    /// - latest: Next offset to be assigned (one past the last message)
    pub async fn get_offset_bounds(&self, topic: &str, partition: u32) -> Result<(u64, u64)> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        client
            .get_offset_bounds(topic, partition)
            .await
            .map_err(|e| ConnectError::broker(format!("Get offset bounds failed: {}", e)))
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
        &self,
        topic: &str,
        partition: u32,
        timestamp_ms: i64,
    ) -> Result<Option<u64>> {
        let mut guard = self.client.lock().await;

        let client = guard
            .as_mut()
            .ok_or_else(|| ConnectError::broker("Not connected to broker"))?;

        client
            .get_offset_for_timestamp(topic, partition, timestamp_ms)
            .await
            .map_err(|e| ConnectError::broker(format!("Get offset for timestamp failed: {}", e)))
    }
}

/// Shared broker client that can be cloned across tasks
pub type SharedBrokerClient = Arc<BrokerClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error() {
        // These should be detected as connection errors
        assert!(BrokerClient::is_connection_error(
            &rivven_client::Error::ServerError("Connection refused".to_string())
        ));
        assert!(BrokerClient::is_connection_error(
            &rivven_client::Error::ServerError("connection reset by peer".to_string())
        ));
        assert!(BrokerClient::is_connection_error(
            &rivven_client::Error::ServerError("operation timeout".to_string())
        ));
    }
}
