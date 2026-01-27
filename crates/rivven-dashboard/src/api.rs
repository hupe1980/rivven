//! REST API client for communicating with rivven-server
//!
//! Provides typed HTTP requests using gloo-net.

use crate::config::DashboardConfig;
use gloo_net::http::Request;
use serde::{Deserialize, Serialize};

/// API client for rivven-server
pub struct ApiClient {
    base_url: String,
}

impl ApiClient {
    /// Create a new API client with the given base URL
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }

    /// Create an API client from dashboard configuration
    /// This is the preferred method for air-gapped deployments
    pub fn from_config(config: &DashboardConfig) -> Self {
        Self::new(config.api_url())
    }

    /// Create an API client that uses the current origin
    /// Falls back to localhost:8080 if origin cannot be determined
    pub fn from_origin() -> Self {
        let config = DashboardConfig::load();
        Self::from_config(&config)
    }

    /// Fetch health status
    pub async fn health(&self) -> Result<HealthResponse, ApiError> {
        let url = format!("{}/health", self.base_url);
        let resp = Request::get(&url).send().await?;

        if resp.ok() {
            Ok(resp.json().await?)
        } else {
            Err(ApiError::Http(resp.status()))
        }
    }

    /// Fetch dashboard data
    pub async fn dashboard_data(&self) -> Result<DashboardData, ApiError> {
        let url = format!("{}/dashboard/data", self.base_url);
        let resp = Request::get(&url).send().await?;

        if resp.ok() {
            Ok(resp.json().await?)
        } else {
            Err(ApiError::Http(resp.status()))
        }
    }

    /// Fetch cluster membership
    pub async fn membership(&self) -> Result<MembershipResponse, ApiError> {
        let url = format!("{}/membership", self.base_url);
        let resp = Request::get(&url).send().await?;

        if resp.ok() {
            Ok(resp.json().await?)
        } else {
            Err(ApiError::Http(resp.status()))
        }
    }

    /// Fetch JSON metrics
    pub async fn metrics_json(&self) -> Result<MetricsResponse, ApiError> {
        let url = format!("{}/metrics/json", self.base_url);
        let resp = Request::get(&url).send().await?;

        if resp.ok() {
            Ok(resp.json().await?)
        } else {
            Err(ApiError::Http(resp.status()))
        }
    }
}

// ============================================================================
// API Response Types
// ============================================================================

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HealthResponse {
    pub status: String,
    pub node_id_str: Option<String>,
    pub cluster_mode: Option<bool>,
    pub is_leader: Option<bool>,
    pub leader_id: Option<u64>,
}

/// Dashboard data response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DashboardData {
    pub topics: Vec<TopicInfo>,
    pub consumer_groups: Vec<ConsumerGroupInfo>,
    pub active_connections: u64,
    pub total_requests: u64,
    pub uptime_secs: u64,
    pub timestamp: u64,
}

/// Topic information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub message_count: u64,
    pub partition_offsets: Vec<PartitionOffset>,
}

/// Partition offset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub partition: u32,
    pub earliest: u64,
    pub latest: u64,
    pub count: u64,
}

/// Consumer group information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub state: String,
    pub member_count: usize,
    pub topics: Vec<String>,
    pub total_lag: u64,
}

/// Cluster membership response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MembershipResponse {
    pub nodes: Vec<ClusterNode>,
}

/// Cluster node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: u64,
    pub addr: String,
    pub is_leader: bool,
    pub is_voter: bool,
}

/// JSON metrics response
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MetricsResponse {
    #[serde(flatten)]
    pub metrics: std::collections::HashMap<String, serde_json::Value>,
}

// ============================================================================
// Error Types
// ============================================================================

/// API error type
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("HTTP error: {0}")]
    Http(u16),

    #[error("Network error: {0}")]
    Network(#[from] gloo_net::Error),
}
