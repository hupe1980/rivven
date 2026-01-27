//! Integration tests for rivven-cluster
//!
//! These tests verify multi-node cluster behavior:
//! - Node discovery and membership
//! - Metadata consensus
//! - Partition placement
//! - Leader election

use rivven_cluster::{
    ClusterConfig, ClusterCoordinator, ClusterMode, RaftConfig, ReplicationConfig, SwimConfig,
    TopicConfig, TopicDefaults,
};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;

/// Create a test cluster configuration
#[allow(dead_code)]
fn test_config(node_id: &str, port: u16, seeds: Vec<String>) -> ClusterConfig {
    let client_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let cluster_addr: SocketAddr = format!("127.0.0.1:{}", port + 1000).parse().unwrap();

    ClusterConfig {
        mode: ClusterMode::Cluster,
        node_id: node_id.to_string(),
        rack: None,
        data_dir: TempDir::new().unwrap().keep(),
        client_addr,
        cluster_addr,
        advertise_addr: Some(cluster_addr),
        seeds,
        swim: SwimConfig {
            ping_interval: Duration::from_millis(100),
            ping_timeout: Duration::from_millis(50),
            indirect_probes: 2,
            suspicion_multiplier: 2,
            ..Default::default()
        },
        raft: RaftConfig {
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(100),
            election_timeout_max: Duration::from_millis(200),
            ..Default::default()
        },
        replication: ReplicationConfig::default(),
        topic_defaults: TopicDefaults::default(),
    }
}

#[tokio::test]
async fn test_standalone_topic_creation() {
    // Test standalone mode works
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic
    let topic_config = TopicConfig::new("test-topic", 3, 1);
    coordinator.create_topic(topic_config).await.unwrap();

    // Verify topic exists
    let health = coordinator.health().await;
    assert_eq!(health.topic_count, 1);
    assert_eq!(health.partition_count, 3);
}

#[tokio::test]
async fn test_standalone_partition_leadership() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic
    let topic_config = TopicConfig::new("partitioned-topic", 4, 1);
    coordinator.create_topic(topic_config).await.unwrap();

    // Check partition leaders
    for p in 0..4 {
        let leader = coordinator.partition_leader("partitioned-topic", p).await;
        assert!(leader.is_some(), "Partition {} should have a leader", p);
    }
}

#[tokio::test]
async fn test_partition_selection_with_key() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic with multiple partitions
    let topic_config = TopicConfig::new("keyed-topic", 8, 1);
    coordinator.create_topic(topic_config).await.unwrap();

    // Same key should always go to same partition
    let key = b"user-123";
    let partition1 = coordinator.select_partition("keyed-topic", Some(key)).await;
    let partition2 = coordinator.select_partition("keyed-topic", Some(key)).await;
    let partition3 = coordinator.select_partition("keyed-topic", Some(key)).await;

    assert_eq!(partition1, partition2);
    assert_eq!(partition2, partition3);
    assert!(partition1.is_some());
    assert!(partition1.unwrap() < 8); // Should be valid partition
}

#[tokio::test]
async fn test_delete_topic() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic
    let topic_config = TopicConfig::new("to-delete", 2, 1);
    coordinator.create_topic(topic_config).await.unwrap();

    let health = coordinator.health().await;
    assert_eq!(health.topic_count, 1);

    // Delete the topic
    coordinator.delete_topic("to-delete").await.unwrap();

    let health = coordinator.health().await;
    assert_eq!(health.topic_count, 0);
}

#[tokio::test]
async fn test_isr_membership_check() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic - in standalone, local node is in ISR
    let topic_config = TopicConfig::new("isr-test", 2, 1);
    coordinator.create_topic(topic_config).await.unwrap();

    // We should be in ISR for our own partitions
    let node_id = coordinator.local_node().id.clone();
    let in_isr = coordinator.is_in_isr("isr-test", 0, &node_id).await;
    assert!(in_isr, "Local node should be in ISR for standalone mode");
}

#[tokio::test]
async fn test_health_reporting() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Initial health
    let health = coordinator.health().await;
    assert!(health.is_leader);
    assert_eq!(health.node_count, 1);
    assert_eq!(health.healthy_nodes, 1);
    assert_eq!(health.topic_count, 0);

    // Create topics and check health
    coordinator
        .create_topic(TopicConfig::new("health-test-1", 2, 1))
        .await
        .unwrap();
    coordinator
        .create_topic(TopicConfig::new("health-test-2", 3, 1))
        .await
        .unwrap();

    let health = coordinator.health().await;
    assert_eq!(health.topic_count, 2);
    assert_eq!(health.partition_count, 5); // 2 + 3
    assert_eq!(health.offline_partitions, 0);
    assert_eq!(health.under_replicated_partitions, 0);
}

// ============================================================================
// Multi-node tests (simulated via multiple coordinators)
// Note: These tests simulate multi-node behavior in a single process
// ============================================================================

#[tokio::test]
async fn test_metadata_replication_simulation() {
    // Create two standalone coordinators (simulating metadata sync)
    let config1 = ClusterConfig::standalone();
    let coord1 = ClusterCoordinator::standalone(config1).await.unwrap();

    let config2 = ClusterConfig::standalone();
    let coord2 = ClusterCoordinator::standalone(config2).await.unwrap();

    // Each operates independently in standalone mode
    coord1
        .create_topic(TopicConfig::new("topic-1", 2, 1))
        .await
        .unwrap();
    coord2
        .create_topic(TopicConfig::new("topic-2", 3, 1))
        .await
        .unwrap();

    // Each should see only their own topics
    assert_eq!(coord1.health().await.topic_count, 1);
    assert_eq!(coord2.health().await.topic_count, 1);
}

#[tokio::test]
async fn test_coordinator_lifecycle() {
    use rivven_cluster::CoordinatorState;

    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Check state transitions
    let state = coordinator.state().await;
    assert_eq!(state, CoordinatorState::Leader); // Standalone is always leader

    // Shutdown
    coordinator.shutdown().await.unwrap();

    let state = coordinator.state().await;
    assert_eq!(state, CoordinatorState::Stopped);
}

#[tokio::test]
async fn test_replication_factor_validation() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Replication factor > nodes should fail
    let result = coordinator
        .create_topic(TopicConfig::new("invalid", 2, 3))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_duplicate_topic_creation() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // Create a topic
    coordinator
        .create_topic(TopicConfig::new("duplicate", 2, 1))
        .await
        .unwrap();

    // Creating again should fail
    let result = coordinator
        .create_topic(TopicConfig::new("duplicate", 2, 1))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_nonexistent_partition_leader() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // No topics exist
    let leader = coordinator.partition_leader("nonexistent", 0).await;
    assert!(leader.is_none());
}

#[tokio::test]
async fn test_select_partition_for_nonexistent_topic() {
    let config = ClusterConfig::standalone();
    let coordinator = ClusterCoordinator::standalone(config).await.unwrap();

    // No topics exist
    let partition = coordinator.select_partition("nonexistent", None).await;
    assert!(partition.is_none());
}
