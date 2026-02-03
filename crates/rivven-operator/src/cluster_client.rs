//! Rivven Cluster Client
//!
//! This module provides a client for communicating with Rivven clusters
//! from the Kubernetes operator. It wraps rivven-client with operator-specific
//! error handling and connection management.

use crate::error::{OperatorError, Result};
use rivven_client::Client;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Default connection timeout for cluster operations
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Default operation timeout for topic operations
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// Information about a topic in the Rivven cluster
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: u32,
    /// Whether the topic already existed
    pub existed: bool,
}

/// Configuration for cluster client
#[derive(Debug, Clone)]
pub struct ClusterClientConfig {
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Operation timeout
    pub operation_timeout: Duration,
    /// Maximum retry attempts
    pub max_retries: u32,
    /// Retry backoff duration
    pub retry_backoff: Duration,
}

impl Default for ClusterClientConfig {
    fn default() -> Self {
        Self {
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            max_retries: 3,
            retry_backoff: Duration::from_millis(500),
        }
    }
}

/// Client for interacting with a Rivven cluster
pub struct ClusterClient {
    config: ClusterClientConfig,
}

impl ClusterClient {
    /// Create a new cluster client with default configuration
    pub fn new() -> Self {
        Self {
            config: ClusterClientConfig::default(),
        }
    }

    /// Create a new cluster client with custom configuration
    pub fn with_config(config: ClusterClientConfig) -> Self {
        Self { config }
    }

    /// Connect to one of the broker endpoints
    async fn connect(&self, endpoints: &[String]) -> Result<Client> {
        if endpoints.is_empty() {
            return Err(OperatorError::ClusterNotFound(
                "No broker endpoints available".to_string(),
            ));
        }

        let mut last_error = None;

        for endpoint in endpoints {
            debug!(endpoint = %endpoint, "Attempting to connect to broker");

            match timeout(self.config.connection_timeout, Client::connect(endpoint)).await {
                Ok(Ok(client)) => {
                    info!(endpoint = %endpoint, "Successfully connected to broker");
                    return Ok(client);
                }
                Ok(Err(e)) => {
                    warn!(endpoint = %endpoint, error = %e, "Failed to connect to broker");
                    last_error = Some(e.to_string());
                }
                Err(_) => {
                    warn!(endpoint = %endpoint, "Connection to broker timed out");
                    last_error = Some("Connection timed out".to_string());
                }
            }
        }

        Err(OperatorError::ConnectionFailed(
            last_error.unwrap_or_else(|| "Unknown error".to_string()),
        ))
    }

    /// Create or ensure a topic exists in the cluster
    ///
    /// Returns information about the topic including whether it already existed.
    pub async fn ensure_topic(
        &self,
        endpoints: &[String],
        topic_name: &str,
        partitions: u32,
    ) -> Result<TopicInfo> {
        let mut client = self.connect(endpoints).await?;

        // First, check if topic already exists
        let existing_topics = timeout(self.config.operation_timeout, client.list_topics())
            .await
            .map_err(|_| OperatorError::Timeout("list_topics timed out".to_string()))?
            .map_err(|e| OperatorError::ClusterError(e.to_string()))?;

        if existing_topics.contains(&topic_name.to_string()) {
            info!(topic = %topic_name, "Topic already exists in cluster");
            // Topic exists - return info
            // Note: In a real implementation, we'd get actual partition count from describe_topic
            return Ok(TopicInfo {
                name: topic_name.to_string(),
                partitions,
                existed: true,
            });
        }

        // Topic doesn't exist, create it
        info!(topic = %topic_name, partitions = partitions, "Creating topic in cluster");

        let actual_partitions = timeout(
            self.config.operation_timeout,
            client.create_topic(topic_name, Some(partitions)),
        )
        .await
        .map_err(|_| OperatorError::Timeout("create_topic timed out".to_string()))?
        .map_err(|e| OperatorError::ClusterError(e.to_string()))?;

        info!(topic = %topic_name, partitions = actual_partitions, "Topic created successfully");

        Ok(TopicInfo {
            name: topic_name.to_string(),
            partitions: actual_partitions,
            existed: false,
        })
    }

    /// Delete a topic from the cluster
    pub async fn delete_topic(&self, endpoints: &[String], topic_name: &str) -> Result<()> {
        let mut client = self.connect(endpoints).await?;

        // Check if topic exists first
        let existing_topics = timeout(self.config.operation_timeout, client.list_topics())
            .await
            .map_err(|_| OperatorError::Timeout("list_topics timed out".to_string()))?
            .map_err(|e| OperatorError::ClusterError(e.to_string()))?;

        if !existing_topics.contains(&topic_name.to_string()) {
            info!(topic = %topic_name, "Topic does not exist in cluster, nothing to delete");
            return Ok(());
        }

        info!(topic = %topic_name, "Deleting topic from cluster");

        timeout(
            self.config.operation_timeout,
            client.delete_topic(topic_name),
        )
        .await
        .map_err(|_| OperatorError::Timeout("delete_topic timed out".to_string()))?
        .map_err(|e| OperatorError::ClusterError(e.to_string()))?;

        info!(topic = %topic_name, "Topic deleted successfully");
        Ok(())
    }

    /// List all topics in the cluster
    pub async fn list_topics(&self, endpoints: &[String]) -> Result<Vec<String>> {
        let mut client = self.connect(endpoints).await?;

        let topics = timeout(self.config.operation_timeout, client.list_topics())
            .await
            .map_err(|_| OperatorError::Timeout("list_topics timed out".to_string()))?
            .map_err(|e| OperatorError::ClusterError(e.to_string()))?;

        Ok(topics)
    }

    /// Check if a topic exists in the cluster
    pub async fn topic_exists(&self, endpoints: &[String], topic_name: &str) -> Result<bool> {
        let topics = self.list_topics(endpoints).await?;
        Ok(topics.contains(&topic_name.to_string()))
    }
}

impl Default for ClusterClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ClusterClientConfig::default();
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.operation_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_cluster_client_new() {
        let client = ClusterClient::new();
        assert_eq!(client.config.max_retries, 3);
    }

    #[test]
    fn test_custom_config() {
        let config = ClusterClientConfig {
            connection_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(15),
            max_retries: 5,
            retry_backoff: Duration::from_millis(250),
        };
        let client = ClusterClient::with_config(config.clone());
        assert_eq!(client.config.connection_timeout, Duration::from_secs(5));
        assert_eq!(client.config.max_retries, 5);
    }
}
