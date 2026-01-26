//! Unified observability layer for Rivven cluster
//!
//! This module provides lightweight, Rust-native metrics using the `metrics` crate
//! with Prometheus export support.
//!
//! # Feature Flags
//!
//! ```toml
//! # Cargo.toml
//! rivven-cluster = { version = "0.1", features = ["metrics-prometheus"] }  # Default, ~5ns overhead
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use rivven_cluster::observability::{RaftMetrics, init_metrics};
//!
//! // Initialize once at startup
//! init_metrics();
//!
//! // Record metrics (zero-cost if feature disabled)
//! RaftMetrics::record_proposal_latency(Duration::from_micros(150));
//! RaftMetrics::increment_elections();
//! RaftMetrics::set_current_term(42);
//! ```

use std::sync::OnceLock;
use std::time::Duration;

// ============================================================================
// Raft Metrics
// ============================================================================

/// Raft consensus metrics
pub struct RaftMetrics;

impl RaftMetrics {
    // ---- Counters ----
    
    /// Total number of proposals submitted
    pub fn increment_proposals() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_proposals_total").increment(1);
    }
    
    /// Total number of elections triggered
    pub fn increment_elections() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_elections_total").increment(1);
    }
    
    /// Total number of successful commits
    pub fn increment_commits() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_commits_total").increment(1);
    }
    
    /// Total number of snapshots taken
    pub fn increment_snapshots() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_snapshots_total").increment(1);
    }
    
    /// Total AppendEntries RPCs sent
    pub fn increment_append_entries_sent() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_append_entries_sent_total").increment(1);
    }
    
    /// Total AppendEntries RPCs received
    pub fn increment_append_entries_received() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_raft_append_entries_received_total").increment(1);
    }
    
    // ---- Gauges ----
    
    /// Current Raft term
    pub fn set_current_term(term: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_current_term").set(term as f64);
    }
    
    /// Current commit index
    pub fn set_commit_index(index: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_commit_index").set(index as f64);
    }
    
    /// Current applied index
    pub fn set_applied_index(index: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_applied_index").set(index as f64);
    }
    
    /// Last log index
    pub fn set_last_log_index(index: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_last_log_index").set(index as f64);
    }
    
    /// Whether this node is the leader (1 = yes, 0 = no)
    pub fn set_is_leader(is_leader: bool) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_is_leader").set(if is_leader { 1.0 } else { 0.0 });
    }
    
    /// Number of peers in the cluster
    pub fn set_peer_count(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_peer_count").set(count as f64);
    }
    
    /// Replication lag (leader's last log - follower's match index)
    pub fn set_replication_lag(node_id: u64, lag: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_raft_replication_lag", "node_id" => node_id.to_string()).set(lag as f64);
    }
    
    // ---- Histograms ----
    
    /// Record proposal latency (time from propose to commit)
    pub fn record_proposal_latency(duration: Duration) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_raft_proposal_latency_seconds").record(duration.as_secs_f64());
    }
    
    /// Record AppendEntries RPC latency
    pub fn record_append_entries_latency(duration: Duration) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_raft_append_entries_latency_seconds").record(duration.as_secs_f64());
    }
    
    /// Record Vote RPC latency
    pub fn record_vote_latency(duration: Duration) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_raft_vote_latency_seconds").record(duration.as_secs_f64());
    }
    
    /// Record snapshot creation time
    pub fn record_snapshot_duration(duration: Duration) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_raft_snapshot_duration_seconds").record(duration.as_secs_f64());
    }
    
    /// Record batch size for proposals
    pub fn record_batch_size(size: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_raft_batch_size").record(size as f64);
    }
}

// ============================================================================
// Cluster Metrics
// ============================================================================

/// Cluster-wide metrics (membership, partitions, replication)
pub struct ClusterMetrics;

impl ClusterMetrics {
    /// Number of nodes in the cluster
    pub fn set_node_count(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_cluster_node_count").set(count as f64);
    }
    
    /// Number of topics
    pub fn set_topic_count(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_cluster_topic_count").set(count as f64);
    }
    
    /// Number of partitions
    pub fn set_partition_count(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_cluster_partition_count").set(count as f64);
    }
    
    /// Number of under-replicated partitions
    pub fn set_under_replicated_partitions(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_cluster_under_replicated_partitions").set(count as f64);
    }
    
    /// Number of offline partitions
    pub fn set_offline_partitions(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_cluster_offline_partitions").set(count as f64);
    }
    
    /// Record a SWIM protocol ping
    pub fn increment_swim_pings() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_cluster_swim_pings_total").increment(1);
    }
    
    /// Record a SWIM protocol failure detection
    pub fn increment_swim_failures_detected() {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_cluster_swim_failures_detected_total").increment(1);
    }
}

// ============================================================================
// Network Metrics
// ============================================================================

/// Network and RPC metrics
pub struct NetworkMetrics;

impl NetworkMetrics {
    /// Total bytes sent
    pub fn add_bytes_sent(bytes: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_network_bytes_sent_total").increment(bytes);
    }
    
    /// Total bytes received
    pub fn add_bytes_received(bytes: u64) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_network_bytes_received_total").increment(bytes);
    }
    
    /// Active connections
    pub fn set_active_connections(count: usize) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::gauge!("rivven_network_active_connections").set(count as f64);
    }
    
    /// Record RPC latency by type
    pub fn record_rpc_latency(rpc_type: &str, duration: Duration) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::histogram!("rivven_network_rpc_latency_seconds", "rpc_type" => rpc_type.to_string())
            .record(duration.as_secs_f64());
    }
    
    /// Increment RPC errors
    pub fn increment_rpc_errors(rpc_type: &str) {
        #[cfg(feature = "metrics-prometheus")]
        metrics::counter!("rivven_network_rpc_errors_total", "rpc_type" => rpc_type.to_string())
            .increment(1);
    }
}

// ============================================================================
// Prometheus Backend (when enabled)
// ============================================================================

#[cfg(feature = "metrics-prometheus")]
mod prom {
    use metrics_exporter_prometheus::PrometheusBuilder;
    use std::net::SocketAddr;
    
    /// Initialize the metrics-rs Prometheus exporter
    ///
    /// This starts a HTTP server on the given address that serves `/metrics`
    pub fn init_prometheus_exporter(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()?;
        
        tracing::info!("Prometheus metrics exporter listening on http://{}/metrics", addr);
        Ok(())
    }
    
    /// Initialize metrics without starting a server (for embedding in existing server)
    pub fn init_prometheus_recorder() -> Result<metrics_exporter_prometheus::PrometheusHandle, Box<dyn std::error::Error + Send + Sync>> {
        let builder = PrometheusBuilder::new();
        let handle = builder.install_recorder()?;
        Ok(handle)
    }
}

#[cfg(feature = "metrics-prometheus")]
pub use prom::{init_prometheus_exporter, init_prometheus_recorder};

// ============================================================================
// Unified Initialization
// ============================================================================

static METRICS_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Initialize metrics subsystem
///
/// Call this once at application startup. Safe to call multiple times.
///
/// # Example
///
/// ```rust,ignore
/// use rivven_cluster::observability::init_metrics;
/// use std::net::SocketAddr;
///
/// #[tokio::main]
/// async fn main() {
///     // Start Prometheus exporter on port 9090
///     let metrics_addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
///     init_metrics(Some(metrics_addr)).expect("Failed to init metrics");
/// }
/// ```
pub fn init_metrics(prometheus_addr: Option<std::net::SocketAddr>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    METRICS_INITIALIZED.get_or_init(|| {
        #[cfg(feature = "metrics-prometheus")]
        if let Some(addr) = prometheus_addr {
            if let Err(e) = init_prometheus_exporter(addr) {
                tracing::error!("Failed to start Prometheus exporter: {}", e);
            }
        }
        
        tracing::info!(
            prometheus = cfg!(feature = "metrics-prometheus"),
            "Metrics subsystem initialized"
        );
    });
    
    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_raft_metrics_compile() {
        // Just verify metrics calls compile without panicking
        RaftMetrics::increment_proposals();
        RaftMetrics::increment_elections();
        RaftMetrics::set_current_term(1);
        RaftMetrics::set_is_leader(true);
        RaftMetrics::record_proposal_latency(Duration::from_millis(10));
    }
    
    #[test]
    fn test_cluster_metrics_compile() {
        ClusterMetrics::set_node_count(3);
        ClusterMetrics::set_topic_count(10);
        ClusterMetrics::set_partition_count(100);
    }
    
    #[test]
    fn test_network_metrics_compile() {
        NetworkMetrics::add_bytes_sent(1024);
        NetworkMetrics::add_bytes_received(2048);
        NetworkMetrics::set_active_connections(5);
        NetworkMetrics::record_rpc_latency("append_entries", Duration::from_micros(500));
    }
}
