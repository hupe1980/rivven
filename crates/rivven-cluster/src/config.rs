//! Cluster configuration

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Cluster operating mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClusterMode {
    /// Single node, no replication (default for simplicity)
    #[default]
    Standalone,
    /// Multi-node cluster with replication
    Cluster,
}

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Operating mode
    pub mode: ClusterMode,
    
    /// Node identifier (unique across cluster)
    pub node_id: String,
    
    /// Rack identifier for rack-aware placement
    pub rack: Option<String>,
    
    /// Data directory for Raft logs and state
    pub data_dir: PathBuf,
    
    /// Client-facing address
    pub client_addr: SocketAddr,
    
    /// Cluster communication address
    pub cluster_addr: SocketAddr,
    
    /// Advertised cluster address (for NAT/container environments)
    pub advertise_addr: Option<SocketAddr>,
    
    /// Seed nodes for initial cluster discovery
    pub seeds: Vec<String>,
    
    /// SWIM membership configuration
    pub swim: SwimConfig,
    
    /// Raft consensus configuration
    pub raft: RaftConfig,
    
    /// Replication configuration
    pub replication: ReplicationConfig,
    
    /// Topic defaults
    pub topic_defaults: TopicDefaults,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::standalone()
    }
}

impl ClusterConfig {
    /// Create standalone configuration (single node, no cluster)
    pub fn standalone() -> Self {
        Self {
            mode: ClusterMode::Standalone,
            node_id: "standalone".to_string(),
            rack: None,
            data_dir: PathBuf::from("./data"),
            client_addr: "0.0.0.0:9092".parse().unwrap(),
            cluster_addr: "0.0.0.0:9093".parse().unwrap(),
            advertise_addr: None,
            seeds: vec![],
            swim: SwimConfig::default(),
            raft: RaftConfig::default(),
            replication: ReplicationConfig::standalone(),
            topic_defaults: TopicDefaults::standalone(),
        }
    }
    
    /// Create cluster configuration builder
    pub fn cluster() -> ClusterConfigBuilder {
        ClusterConfigBuilder::new()
    }
    
    /// Check if running in cluster mode
    pub fn is_cluster(&self) -> bool {
        matches!(self.mode, ClusterMode::Cluster)
    }
    
    /// Get the advertised address (for other nodes to connect)
    pub fn advertised_cluster_addr(&self) -> SocketAddr {
        self.advertise_addr.unwrap_or(self.cluster_addr)
    }
}

/// Builder for cluster configuration
#[derive(Debug, Default)]
pub struct ClusterConfigBuilder {
    node_id: Option<String>,
    rack: Option<String>,
    data_dir: Option<PathBuf>,
    client_addr: Option<SocketAddr>,
    cluster_addr: Option<SocketAddr>,
    advertise_addr: Option<SocketAddr>,
    seeds: Vec<String>,
    swim: Option<SwimConfig>,
    raft: Option<RaftConfig>,
    replication: Option<ReplicationConfig>,
    topic_defaults: Option<TopicDefaults>,
}

impl ClusterConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }
    
    pub fn rack(mut self, rack: impl Into<String>) -> Self {
        self.rack = Some(rack.into());
        self
    }
    
    pub fn data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }
    
    pub fn client_addr(mut self, addr: SocketAddr) -> Self {
        self.client_addr = Some(addr);
        self
    }
    
    pub fn cluster_addr(mut self, addr: SocketAddr) -> Self {
        self.cluster_addr = Some(addr);
        self
    }
    
    pub fn advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.advertise_addr = Some(addr);
        self
    }
    
    pub fn seeds(mut self, seeds: Vec<impl Into<String>>) -> Self {
        self.seeds = seeds.into_iter().map(|s| s.into()).collect();
        self
    }
    
    pub fn swim(mut self, config: SwimConfig) -> Self {
        self.swim = Some(config);
        self
    }
    
    pub fn raft(mut self, config: RaftConfig) -> Self {
        self.raft = Some(config);
        self
    }
    
    pub fn replication(mut self, config: ReplicationConfig) -> Self {
        self.replication = Some(config);
        self
    }
    
    pub fn build(self) -> ClusterConfig {
        ClusterConfig {
            mode: ClusterMode::Cluster,
            node_id: self.node_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            rack: self.rack,
            data_dir: self.data_dir.unwrap_or_else(|| PathBuf::from("./data")),
            client_addr: self.client_addr.unwrap_or_else(|| "0.0.0.0:9092".parse().unwrap()),
            cluster_addr: self.cluster_addr.unwrap_or_else(|| "0.0.0.0:9093".parse().unwrap()),
            advertise_addr: self.advertise_addr,
            seeds: self.seeds,
            swim: self.swim.unwrap_or_default(),
            raft: self.raft.unwrap_or_default(),
            replication: self.replication.unwrap_or_default(),
            topic_defaults: self.topic_defaults.unwrap_or_default(),
        }
    }
}

/// SWIM protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwimConfig {
    /// Interval between probe rounds
    pub ping_interval: Duration,
    
    /// Timeout for direct ping
    pub ping_timeout: Duration,
    
    /// Number of indirect probes on ping failure
    pub indirect_probes: usize,
    
    /// Multiplier for suspicion timeout (suspicion_mult * ping_interval)
    pub suspicion_multiplier: u32,
    
    /// Maximum number of updates to piggyback on messages
    pub max_gossip_updates: usize,
    
    /// Interval for full state sync
    pub sync_interval: Duration,
}

impl Default for SwimConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(1),
            ping_timeout: Duration::from_millis(500),
            indirect_probes: 3,
            suspicion_multiplier: 4,
            max_gossip_updates: 10,
            sync_interval: Duration::from_secs(30),
        }
    }
}

/// Raft consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Minimum election timeout
    pub election_timeout_min: Duration,
    
    /// Maximum election timeout
    pub election_timeout_max: Duration,
    
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    
    /// Snapshot threshold (entries before snapshot)
    pub snapshot_threshold: u64,
    
    /// Maximum entries per append
    pub max_entries_per_append: u64,
    
    /// Replication batch size
    pub replication_batch_size: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_threshold: 10000,
            max_entries_per_append: 100,
            replication_batch_size: 1000,
        }
    }
}

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Default replication factor for new topics
    pub default_replication_factor: u16,
    
    /// Minimum in-sync replicas required for writes
    pub min_isr: u16,
    
    /// Maximum lag (in messages) before removing from ISR
    pub replica_lag_max_messages: u64,
    
    /// Maximum lag (in time) before removing from ISR
    pub replica_lag_max_time: Duration,
    
    /// Interval for follower fetch requests
    pub fetch_interval: Duration,
    
    /// Maximum bytes per fetch request
    pub fetch_max_bytes: u32,
    
    /// Allow unclean leader election (may lose data)
    pub unclean_leader_election: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            default_replication_factor: 3,
            min_isr: 2,
            replica_lag_max_messages: 10000,
            replica_lag_max_time: Duration::from_secs(30),
            fetch_interval: Duration::from_millis(100),
            fetch_max_bytes: 10 * 1024 * 1024, // 10 MB
            unclean_leader_election: false,
        }
    }
}

impl ReplicationConfig {
    /// Standalone configuration (no replication)
    pub fn standalone() -> Self {
        Self {
            default_replication_factor: 1,
            min_isr: 1,
            ..Default::default()
        }
    }
}

/// Default topic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicDefaults {
    /// Default number of partitions
    pub partitions: u32,
    
    /// Default replication factor
    pub replication_factor: u16,
    
    /// Default retention period
    pub retention: Duration,
    
    /// Default segment size
    pub segment_size: u64,
}

impl Default for TopicDefaults {
    fn default() -> Self {
        Self {
            partitions: 6,
            replication_factor: 3,
            retention: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            segment_size: 1024 * 1024 * 1024, // 1 GB
        }
    }
}

impl TopicDefaults {
    /// Standalone defaults (single partition, no replication)
    pub fn standalone() -> Self {
        Self {
            partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_standalone_config() {
        let config = ClusterConfig::standalone();
        assert!(!config.is_cluster());
        assert_eq!(config.mode, ClusterMode::Standalone);
        assert_eq!(config.replication.default_replication_factor, 1);
    }
    
    #[test]
    fn test_cluster_config_builder() {
        let config = ClusterConfig::cluster()
            .node_id("node-1")
            .rack("rack-a")
            .seeds(vec!["node-1:9093", "node-2:9093"])
            .build();
        
        assert!(config.is_cluster());
        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.rack, Some("rack-a".to_string()));
        assert_eq!(config.seeds.len(), 2);
    }
}
