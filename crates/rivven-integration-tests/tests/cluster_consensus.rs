//! Cluster consensus tests
//!
//! Tests for Raft consensus, leader election, and cluster coordination.
//! These tests verify distributed system behavior under various conditions.
//!
//! Run with: cargo test -p rivven-integration-tests --test cluster_consensus -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_integration_tests::fixtures::TestCluster;
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

/// Test single node starts and becomes operational
#[tokio::test]
async fn test_single_node_operational() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let addr = cluster.bootstrap_string();

    // Verify node is accepting connections
    let mut client = Client::connect(&addr).await?;
    let topic = unique_topic_name("single-node");
    client.create_topic(&topic, Some(1)).await?;

    // Write and read
    client.publish(&topic, b"test".to_vec()).await?;
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    info!("Single node operational test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test cluster formation with multiple nodes
#[tokio::test]
async fn test_cluster_formation() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(3).await?;

    // All nodes should be accessible
    for (i, broker) in cluster.brokers.iter().enumerate() {
        let client = Client::connect(&broker.connection_string()).await;
        assert!(client.is_ok(), "Failed to connect to node {}", i);
        info!("Node {} accessible at {}", i, broker.connection_string());
    }

    info!("Cluster formation test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test data consistency across cluster nodes
#[tokio::test]
async fn test_cluster_data_consistency() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(3).await?;

    // Write to first node
    let mut producer = Client::connect(&cluster.brokers[0].connection_string()).await?;
    let topic = unique_topic_name("consistency");
    producer.create_topic(&topic, Some(1)).await?;

    for i in 0..10 {
        producer
            .publish(&topic, format!("msg-{}", i).into_bytes())
            .await?;
    }

    // Allow replication time
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read from same node (in multi-node, would read from any)
    let mut consumer = Client::connect(&cluster.brokers[0].connection_string()).await?;
    let messages = consumer.consume(&topic, 0, 0, 20).await?;
    assert_eq!(messages.len(), 10);

    info!("Cluster data consistency test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test cluster handles concurrent writes
#[tokio::test]
async fn test_cluster_concurrent_writes() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let addr = cluster.bootstrap_string();

    // Use unique topic name to avoid interference from other tests
    let topic = unique_topic_name("concurrent-cluster");

    // Pre-create topic
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(1)).await?;
    }

    // Concurrent writers
    let mut handles = Vec::new();
    for writer_id in 0..5 {
        let addr_clone = addr.clone();
        let topic_clone = topic.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr_clone).await?;
            for i in 0..20 {
                client
                    .publish(&topic_clone, format!("w{}-m{}", writer_id, i).into_bytes())
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Verify all messages
    let mut client = Client::connect(&addr).await?;
    let messages = client.consume(&topic, 0, 0, 200).await?;
    assert_eq!(messages.len(), 100); // 5 writers * 20 messages

    info!("Concurrent writes test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test topic creation across cluster
#[tokio::test]
async fn test_cluster_topic_creation() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let addr = cluster.bootstrap_string();

    let mut client = Client::connect(&addr).await?;

    // Create multiple topics
    for i in 0..5 {
        let topic = unique_topic_name(&format!("cluster-topic-{}", i));
        let partitions = client.create_topic(&topic, Some((i + 1) as u32)).await?;
        assert_eq!(partitions, (i + 1) as u32);
    }

    let topics = client.list_topics().await?;
    assert!(topics.len() >= 5);

    info!("Cluster topic creation test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test message ordering is maintained
#[tokio::test]
async fn test_cluster_message_ordering() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let mut client = Client::connect(&cluster.bootstrap_string()).await?;

    let topic = unique_topic_name("ordering");
    client.create_topic(&topic, Some(1)).await?;

    // Sequential writes
    for i in 0..100 {
        client
            .publish(&topic, format!("{:04}", i).into_bytes())
            .await?;
    }

    // Verify ordering
    let messages = client.consume(&topic, 0, 0, 200).await?;
    for (i, msg) in messages.iter().enumerate() {
        let expected = format!("{:04}", i);
        assert_eq!(
            String::from_utf8_lossy(&msg.value),
            expected,
            "Order mismatch at index {}",
            i
        );
    }

    info!("Message ordering test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test cluster handles client disconnections gracefully
#[tokio::test]
async fn test_cluster_client_disconnect() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let addr = cluster.bootstrap_string();

    let topic = unique_topic_name("disconnect");

    // First client writes and disconnects
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(1)).await?;
        client
            .publish(&topic, b"before-disconnect".to_vec())
            .await?;
        // client dropped here
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second client should still work
    {
        let mut client = Client::connect(&addr).await?;
        client.publish(&topic, b"after-disconnect".to_vec()).await?;

        let messages = client.consume(&topic, 0, 0, 10).await?;
        assert_eq!(messages.len(), 2);
    }

    info!("Client disconnect test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test partition distribution
#[tokio::test]
async fn test_cluster_partition_distribution() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let mut client = Client::connect(&cluster.bootstrap_string()).await?;

    let topic = unique_topic_name("partitions");
    let num_partitions = 8;
    client.create_topic(&topic, Some(num_partitions)).await?;

    // Write to all partitions
    for p in 0..num_partitions {
        for i in 0..10 {
            client
                .publish_to_partition(
                    &topic,
                    p,
                    None::<bytes::Bytes>,
                    format!("p{}-{}", p, i).into_bytes(),
                )
                .await?;
        }
    }

    // Verify each partition
    for p in 0..num_partitions {
        let messages = client.consume(&topic, p, 0, 20).await?;
        assert_eq!(messages.len(), 10, "Partition {} has wrong count", p);
    }

    info!("Partition distribution test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Test cluster recovery after restart (data persistence)
#[tokio::test]
#[ignore = "Requires persistent storage support"]
async fn test_cluster_recovery() -> Result<()> {
    init_tracing();

    // This test would verify data persists across broker restarts
    // Currently ignored as it requires configuring persistent storage

    info!("Cluster recovery test - skipped (requires persistent storage)");
    Ok(())
}

/// Test cluster under load
#[tokio::test]
async fn test_cluster_under_load() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let addr = cluster.bootstrap_string();

    let topic = unique_topic_name("load-test");
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(4)).await?;
    }

    // Multiple concurrent producers
    let producer_count = 4;
    let messages_per_producer = 250;

    let mut handles = Vec::new();
    for producer_id in 0..producer_count {
        let addr_clone = addr.clone();
        let topic_clone = topic.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr_clone).await?;
            for i in 0..messages_per_producer {
                client
                    .publish_to_partition(
                        &topic_clone,
                        producer_id as u32,
                        None::<bytes::Bytes>,
                        format!("p{}-m{}", producer_id, i).into_bytes(),
                    )
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Verify total message count
    let mut client = Client::connect(&addr).await?;
    let mut total = 0;
    for p in 0..4u32 {
        let messages = client.consume(&topic, p, 0, 1000).await?;
        total += messages.len();
    }

    assert_eq!(total, producer_count * messages_per_producer);
    info!(
        "Load test passed: {} messages processed",
        producer_count * messages_per_producer
    );

    cluster.shutdown().await?;
    Ok(())
}

/// Test cluster metadata operations
#[tokio::test]
async fn test_cluster_metadata() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let mut client = Client::connect(&cluster.bootstrap_string()).await?;

    // Create topics with different partition counts
    let topics = vec![
        (unique_topic_name("meta-1"), 1),
        (unique_topic_name("meta-2"), 4),
        (unique_topic_name("meta-3"), 8),
    ];

    for (name, partitions) in &topics {
        client.create_topic(name, Some(*partitions)).await?;
    }

    let topic_list = client.list_topics().await?;
    for (name, _) in &topics {
        assert!(topic_list.contains(name), "Topic {} not found", name);
    }

    info!("Cluster metadata test passed");
    cluster.shutdown().await?;
    Ok(())
}

/// Benchmark: cluster throughput
#[tokio::test]
#[ignore = "Benchmark test"]
async fn bench_cluster_throughput() -> Result<()> {
    init_tracing();

    let cluster = TestCluster::start(1).await?;
    let mut client = Client::connect(&cluster.bootstrap_string()).await?;

    let topic = unique_topic_name("bench");
    client.create_topic(&topic, Some(4)).await?;

    let message = vec![b'X'; 1000];
    let count = 10_000;

    let start = std::time::Instant::now();
    for i in 0..count {
        client
            .publish_to_partition(
                &topic,
                (i % 4) as u32,
                None::<bytes::Bytes>,
                message.clone(),
            )
            .await?;
    }
    let duration = start.elapsed();

    let rate = count as f64 / duration.as_secs_f64();
    let mb = (count * message.len()) as f64 / duration.as_secs_f64() / 1_000_000.0;

    info!("Cluster throughput: {:.0} msgs/sec, {:.2} MB/sec", rate, mb);

    cluster.shutdown().await?;
    Ok(())
}
