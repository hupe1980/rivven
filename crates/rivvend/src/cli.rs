//! CLI argument parsing for Rivven server
//!
//! Supports both standalone and cluster deployment modes with
//! extensive configuration options.

use clap::{Parser, ValueEnum};
use rivven_cluster::config::{
    ClusterConfig, ClusterMode as ClusterModeConfig, RaftConfig, ReplicationConfig, SwimConfig,
};
use rivven_core::Config;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Get system hostname via the `hostname` command, falling back to "unknown".
fn hostname() -> String {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Rivven - High-Performance Distributed Event Streaming Platform
///
/// A lightweight, single-binary event streaming platform designed for
/// production workloads. Supports both standalone and clustered deployment.
#[derive(Parser, Debug)]
#[command(name = "rivvend")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    // ============ Server Configuration ============
    /// Server bind address
    #[arg(short, long, default_value = "0.0.0.0:9092", env = "RIVVEN_BIND")]
    pub bind: SocketAddr,

    /// Data directory for persistence
    #[arg(short, long, default_value = "./data", env = "RIVVEN_DATA_DIR")]
    pub data_dir: PathBuf,

    /// Enable disk persistence
    #[arg(long, default_value = "true", env = "RIVVEN_PERSISTENCE")]
    pub persistence: bool,

    /// Default number of partitions for new topics
    #[arg(long, default_value = "3", env = "RIVVEN_DEFAULT_PARTITIONS")]
    pub default_partitions: u32,

    /// Maximum segment size in bytes
    #[arg(long, default_value = "1073741824", env = "RIVVEN_MAX_SEGMENT_SIZE")]
    pub max_segment_size: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info", env = "RUST_LOG")]
    pub log_level: String,

    // ============ Cluster Configuration ============
    /// Deployment mode
    #[arg(long, default_value = "standalone", env = "RIVVEN_MODE")]
    pub mode: DeploymentMode,

    /// Unique node identifier (required for cluster mode)
    #[arg(long, env = "RIVVEN_NODE_ID")]
    pub node_id: Option<String>,

    /// Cluster name for discovery
    #[arg(long, default_value = "rivven-cluster", env = "RIVVEN_CLUSTER_NAME")]
    pub cluster_name: String,

    /// Seed nodes for cluster discovery (comma-separated)
    /// Format: node1:9093,node2:9093
    #[arg(long, value_delimiter = ',', env = "RIVVEN_SEEDS")]
    pub seeds: Vec<String>,

    /// Cluster communication bind address
    /// Defaults to server bind + 1 (e.g., 9093 if server is 9092)
    #[arg(long, env = "RIVVEN_CLUSTER_BIND")]
    pub cluster_bind: Option<SocketAddr>,

    /// Public address this node advertises to other nodes
    /// Useful in NAT/container environments
    #[arg(long, env = "RIVVEN_ADVERTISE_ADDR")]
    pub advertise_addr: Option<SocketAddr>,

    /// Datacenter identifier for rack-aware placement
    #[arg(long, default_value = "default", env = "RIVVEN_DATACENTER")]
    pub datacenter: String,

    /// Rack identifier within datacenter
    #[arg(long, env = "RIVVEN_RACK")]
    pub rack: Option<String>,

    // ============ Replication Configuration ============
    /// Default replication factor for new topics
    #[arg(long, default_value = "3", env = "RIVVEN_REPLICATION_FACTOR")]
    pub replication_factor: u16,

    /// Minimum in-sync replicas required for writes
    #[arg(long, default_value = "2", env = "RIVVEN_MIN_ISR")]
    pub min_isr: u16,

    /// Replica fetch interval in milliseconds
    #[arg(long, default_value = "100", env = "RIVVEN_REPLICA_FETCH_MS")]
    pub replica_fetch_ms: u64,

    /// Replica lag tolerance before removal from ISR (in ms)
    #[arg(long, default_value = "30000", env = "RIVVEN_REPLICA_LAG_MS")]
    pub replica_lag_ms: u64,

    // ============ SWIM Configuration ============
    /// SWIM protocol interval in milliseconds
    #[arg(long, default_value = "1000", env = "RIVVEN_SWIM_INTERVAL_MS")]
    pub swim_interval_ms: u64,

    /// Number of SWIM indirect ping targets
    #[arg(long, default_value = "3", env = "RIVVEN_SWIM_INDIRECT_TARGETS")]
    pub swim_indirect_targets: usize,

    /// SWIM suspicion multiplier
    #[arg(long, default_value = "4", env = "RIVVEN_SWIM_SUSPICION_MULT")]
    pub swim_suspicion_mult: u32,

    // ============ Raft Configuration ============
    /// Raft heartbeat interval in milliseconds
    #[arg(long, default_value = "50", env = "RIVVEN_RAFT_HEARTBEAT_MS")]
    pub raft_heartbeat_ms: u64,

    /// Raft election timeout minimum in milliseconds
    #[arg(long, default_value = "150", env = "RIVVEN_RAFT_ELECTION_MIN_MS")]
    pub raft_election_min_ms: u64,

    /// Raft election timeout maximum in milliseconds
    #[arg(long, default_value = "300", env = "RIVVEN_RAFT_ELECTION_MAX_MS")]
    pub raft_election_max_ms: u64,

    // ============ Rate Limiting & DoS Protection ============
    /// Maximum number of concurrent connections per client IP
    #[arg(long, default_value = "100", env = "RIVVEN_MAX_CONNECTIONS_PER_IP")]
    pub max_connections_per_ip: u32,

    /// Maximum total concurrent connections
    #[arg(long, default_value = "10000", env = "RIVVEN_MAX_TOTAL_CONNECTIONS")]
    pub max_total_connections: u32,

    /// Requests per second limit per client IP (0 = unlimited)
    #[arg(long, default_value = "10000", env = "RIVVEN_RATE_LIMIT_PER_IP")]
    pub rate_limit_per_ip: u32,

    /// Maximum request size in bytes (default: 10MB)
    #[arg(long, default_value = "10485760", env = "RIVVEN_MAX_REQUEST_SIZE")]
    pub max_request_size: usize,

    /// Connection idle timeout in seconds
    #[arg(long, default_value = "120", env = "RIVVEN_IDLE_TIMEOUT_SECS")]
    pub idle_timeout_secs: u64,

    // ============ Performance Tuning ============
    /// Number of network I/O threads
    #[arg(long, env = "RIVVEN_IO_THREADS")]
    pub io_threads: Option<usize>,

    /// TCP_NODELAY for cluster connections
    #[arg(long, default_value = "true", env = "RIVVEN_TCP_NODELAY")]
    pub tcp_nodelay: bool,

    /// Connection timeout in milliseconds
    #[arg(long, default_value = "5000", env = "RIVVEN_CONNECT_TIMEOUT_MS")]
    pub connect_timeout_ms: u64,

    /// Enable metrics endpoint
    #[arg(long, default_value = "true", env = "RIVVEN_METRICS")]
    pub metrics: bool,

    /// Metrics endpoint bind address
    #[arg(long, default_value = "0.0.0.0:9090", env = "RIVVEN_METRICS_BIND")]
    pub metrics_bind: SocketAddr,

    /// HTTP API bind address (for Raft RPCs and management)
    #[arg(long, default_value = "0.0.0.0:9094", env = "RIVVEN_API_BIND")]
    pub api_bind: SocketAddr,

    // ============ Dashboard Configuration ============
    /// Disable web dashboard (dashboard is enabled by default when compiled with 'dashboard' feature)
    #[arg(long, default_value = "false", env = "RIVVEN_NO_DASHBOARD")]
    pub no_dashboard: bool,

    /// Dashboard bind address (uses api_bind if not specified)
    #[arg(long, env = "RIVVEN_DASHBOARD_BIND")]
    pub dashboard_bind: Option<SocketAddr>,

    // ============ Partitioner Configuration ============
    /// Sticky partitioner batch size (messages before rotating partition, 0 = time-based only)
    #[arg(long, default_value = "16384", env = "RIVVEN_PARTITIONER_BATCH_SIZE")]
    pub partitioner_batch_size: u32,

    /// Sticky partitioner linger time in milliseconds (time before rotating partition)
    #[arg(long, default_value = "100", env = "RIVVEN_PARTITIONER_LINGER_MS")]
    pub partitioner_linger_ms: u64,

    // ============ TLS Configuration ============
    /// Enable TLS for cluster communication (Raft RPCs)
    #[arg(long, default_value = "false", env = "RIVVEN_TLS_ENABLED")]
    pub tls_enabled: bool,

    /// Path to TLS certificate file (PEM format)
    #[arg(long, env = "RIVVEN_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM format)
    #[arg(long, env = "RIVVEN_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// Path to CA certificate for verifying peer certificates
    #[arg(long, env = "RIVVEN_TLS_CA")]
    pub tls_ca: Option<PathBuf>,

    /// Require client certificate verification
    #[arg(long, default_value = "false", env = "RIVVEN_TLS_VERIFY_CLIENT")]
    pub tls_verify_client: bool,

    // ============ Cluster Security ============
    /// Bearer token for authenticating Raft RPCs between cluster nodes.
    /// **Required** when running in cluster mode. Rejecting startup without
    /// a token prevents unauthenticated hosts from disrupting consensus.
    #[arg(long, env = "RIVVEN_CLUSTER_AUTH_TOKEN")]
    pub cluster_auth_token: Option<String>,
}

/// Deployment mode for the server
#[derive(ValueEnum, Clone, Debug, Default, PartialEq)]
pub enum DeploymentMode {
    /// Single-node mode, no clustering
    #[default]
    Standalone,
    /// Multi-node cluster mode
    Cluster,
}

impl Cli {
    /// Convert CLI args to core config
    pub fn to_core_config(&self) -> Config {
        Config {
            bind_address: self.bind.ip().to_string(),
            port: self.bind.port(),
            default_partitions: self.default_partitions,
            enable_persistence: self.persistence,
            data_dir: self.data_dir.display().to_string(),
            max_segment_size: self.max_segment_size,
            log_level: self.log_level.clone(),
            tiered_storage: rivven_core::storage::TieredStorageConfig::default(),
        }
    }

    /// Convert CLI args to cluster config
    pub fn to_cluster_config(&self) -> ClusterConfig {
        let node_id = self.effective_node_id();

        let cluster_bind = self.effective_cluster_bind();

        let mode = match self.mode {
            DeploymentMode::Standalone => ClusterModeConfig::Standalone,
            DeploymentMode::Cluster => ClusterModeConfig::Cluster,
        };

        ClusterConfig {
            mode,
            node_id,
            rack: self.rack.clone(),
            data_dir: self.data_dir.clone(),
            client_addr: self.bind,
            cluster_addr: cluster_bind,
            advertise_addr: self.advertise_addr,
            seeds: self.seeds.clone(),
            swim: SwimConfig {
                ping_interval: Duration::from_millis(self.swim_interval_ms),
                indirect_probes: self.swim_indirect_targets,
                suspicion_multiplier: self.swim_suspicion_mult,
                ..Default::default()
            },
            raft: RaftConfig {
                heartbeat_interval: Duration::from_millis(self.raft_heartbeat_ms),
                election_timeout_min: Duration::from_millis(self.raft_election_min_ms),
                election_timeout_max: Duration::from_millis(self.raft_election_max_ms),
                ..Default::default()
            },
            replication: ReplicationConfig {
                default_replication_factor: self.replication_factor,
                min_isr: self.min_isr,
                fetch_interval: Duration::from_millis(self.replica_fetch_ms),
                replica_lag_max_time: Duration::from_millis(self.replica_lag_ms),
                ..Default::default()
            },
            topic_defaults: Default::default(),
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        // In cluster mode, we need seeds for joining
        if self.mode == DeploymentMode::Cluster && self.seeds.is_empty() {
            // Allow first node to bootstrap
            tracing::warn!("No seed nodes specified - this node will bootstrap a new cluster");
        }

        // Validate replication factor vs min ISR
        if self.min_isr > self.replication_factor {
            return Err(format!(
                "min_isr ({}) cannot exceed replication_factor ({})",
                self.min_isr, self.replication_factor
            ));
        }

        // Validate Raft timeouts
        if self.raft_election_min_ms <= self.raft_heartbeat_ms {
            return Err(format!(
                "raft_election_min_ms ({}) must be greater than raft_heartbeat_ms ({})",
                self.raft_election_min_ms, self.raft_heartbeat_ms
            ));
        }

        if self.raft_election_max_ms < self.raft_election_min_ms {
            return Err(format!(
                "raft_election_max_ms ({}) must be >= raft_election_min_ms ({})",
                self.raft_election_max_ms, self.raft_election_min_ms
            ));
        }

        // Cluster auth token is mandatory in cluster mode.
        // An unauthenticated Raft port allows any host to disrupt consensus.
        if self.mode == DeploymentMode::Cluster && self.cluster_auth_token.is_none() {
            return Err(
                "cluster_auth_token (--cluster-auth-token / RIVVEN_CLUSTER_AUTH_TOKEN) is required \
                 in cluster mode. Raft RPCs without authentication allow any host to disrupt consensus."
                    .to_string(),
            );
        }

        Ok(())
    }

    /// Get cluster bind address (defaults to server port + 1)
    pub fn effective_cluster_bind(&self) -> SocketAddr {
        self.cluster_bind
            .unwrap_or_else(|| SocketAddr::new(self.bind.ip(), self.bind.port() + 1))
    }

    /// Get effective node ID (generated from hostname if not specified)
    pub fn effective_node_id(&self) -> String {
        self.node_id.clone().unwrap_or_else(|| {
            format!(
                "{}-{}",
                hostname(),
                self.cluster_bind
                    .map(|a| a.port())
                    .unwrap_or(self.bind.port() + 1)
            )
        })
    }

    /// Check if running in cluster mode
    pub fn is_cluster_mode(&self) -> bool {
        matches!(self.mode, DeploymentMode::Cluster)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cli() {
        let cli = Cli::parse_from(["rivvend"]);
        assert_eq!(cli.mode, DeploymentMode::Standalone);
        assert_eq!(cli.bind.port(), 9092);
        assert!(cli.validate().is_ok());
    }

    #[test]
    fn test_cluster_mode() {
        let cli = Cli::parse_from([
            "rivvend",
            "--mode",
            "cluster",
            "--node-id",
            "node-1",
            "--seeds",
            "host1:9093,host2:9093",
        ]);
        assert_eq!(cli.mode, DeploymentMode::Cluster);
        assert_eq!(cli.node_id, Some("node-1".to_string()));
        assert_eq!(cli.seeds.len(), 2);
    }

    #[test]
    fn test_validation_min_isr_exceeds_replication() {
        let cli = Cli::parse_from(["rivvend", "--replication-factor", "2", "--min-isr", "3"]);
        assert!(cli.validate().is_err());
    }

    #[test]
    fn test_cluster_config_generation() {
        let cli = Cli::parse_from([
            "rivvend",
            "--mode",
            "cluster",
            "--node-id",
            "node-1",
            "--bind",
            "192.168.1.1:9092",
            "--cluster-bind",
            "192.168.1.1:9093",
            "--rack",
            "rack-a",
        ]);

        let cluster_config = cli.to_cluster_config();
        assert_eq!(&cluster_config.node_id, "node-1");
        assert_eq!(cluster_config.rack, Some("rack-a".to_string()));
    }
}
