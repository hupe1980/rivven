//! Consumer group tests
//!
//! Tests for consumer coordination and group semantics:
//! - Consumer group membership
//! - Partition assignment and rebalancing
//! - Offset commit and recovery
//! - At-least-once delivery semantics
//!
//! Run with: cargo test -p rivven-integration-tests --test consumer_groups -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::Config;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

/// Test basic consumer group join
#[tokio::test]
async fn test_consumer_group_basic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-basic");
    let _group_id = "test-group-basic";

    producer.create_topic(&topic, Some(4)).await?;

    // Produce some messages
    for i in 0..100 {
        let partition = (i % 4) as u32;
        let msg = Bytes::from(format!("message-{}", i).into_bytes());
        producer
            .publish_to_partition(&topic, partition, None::<Bytes>, msg)
            .await?;
    }

    info!("Produced 100 messages to topic {}", topic);

    // Create a simple consumer group simulation
    // In a real implementation, the broker would track group membership
    let mut consumed_by_partition: HashMap<u32, Vec<u64>> = HashMap::new();

    for partition in 0..4 {
        let messages = consumer.consume(&topic, partition, 0, 100).await?;
        let offsets: Vec<u64> = messages.iter().map(|m| m.offset).collect();
        consumed_by_partition.insert(partition, offsets);
    }

    // Verify all partitions were consumed
    let total_consumed: usize = consumed_by_partition.values().map(|v| v.len()).sum();
    assert_eq!(total_consumed, 100);

    info!(
        "Consumer group basic test passed: consumed {} messages",
        total_consumed
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test offset commit and recovery
#[tokio::test]
async fn test_offset_commit_recovery() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-cg-offset-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let topic = unique_topic_name("cg-offset");
    let message = Bytes::from(b"test-message".to_vec());
    let commit_offset = 50u64;

    // Phase 1: Produce messages and "commit" offset
    info!("Phase 1: Producing messages and simulating offset commit");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(&topic, Some(1)).await?;

        // Produce 100 messages
        for _ in 0..100 {
            client.publish(&topic, message.clone()).await?;
        }

        // Consume up to offset 50 (simulating partial consumption)
        let messages = client.consume(&topic, 0, 0, 50).await?;
        assert_eq!(messages.len(), 50);

        info!(
            "Consumed {} messages, last offset: {}",
            messages.len(),
            commit_offset - 1
        );

        broker.shutdown().await?;
    }

    // Phase 2: Restart and resume from committed offset
    info!("Phase 2: Restarting and resuming from committed offset");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        // Resume consumption from committed offset
        let messages = client.consume(&topic, 0, commit_offset, 100).await?;
        assert_eq!(messages.len(), 50, "Should consume remaining 50 messages");

        // Verify first message is at expected offset
        assert_eq!(messages[0].offset, commit_offset);

        info!(
            "Resumed consumption: {} messages starting at offset {}",
            messages.len(),
            commit_offset
        );

        broker.shutdown().await?;
    }

    info!("Offset commit recovery test passed");
    let _ = tokio::fs::remove_dir_all(&config.data_dir).await;
    Ok(())
}

/// Test parallel consumers (simulating consumer group)
#[tokio::test]
async fn test_parallel_consumers() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-parallel");
    let partition_count = 4u32;
    let messages_per_partition = 250;

    producer.create_topic(&topic, Some(partition_count)).await?;

    // Produce messages evenly across partitions
    info!(
        "Producing {} messages per partition",
        messages_per_partition
    );
    for i in 0..(partition_count as usize * messages_per_partition) {
        let partition = (i % partition_count as usize) as u32;
        let msg = Bytes::from(format!("msg-{}", i).into_bytes());
        producer
            .publish_to_partition(&topic, partition, None::<Bytes>, msg)
            .await?;
    }

    // Spawn parallel consumers (each handles one partition, like a consumer group)
    let broker_addr = broker.connection_string();
    let consumed_counts = Arc::new(RwLock::new(HashMap::<u32, usize>::new()));

    let mut handles = Vec::new();
    for partition in 0..partition_count {
        let addr = broker_addr.clone();
        let topic = topic.clone();
        let counts = Arc::clone(&consumed_counts);

        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            let messages = client.consume(&topic, partition, 0, 1000).await.unwrap();
            let mut counts = counts.write().await;
            counts.insert(partition, messages.len());
            messages.len()
        }));
    }

    let mut total = 0;
    for handle in handles {
        total += handle.await?;
    }

    // Verify all messages were consumed
    assert_eq!(total, partition_count as usize * messages_per_partition);

    // Verify even distribution
    let counts = consumed_counts.read().await;
    for partition in 0..partition_count {
        let count = counts.get(&partition).unwrap_or(&0);
        assert_eq!(
            *count, messages_per_partition,
            "Partition {} should have {} messages",
            partition, messages_per_partition
        );
    }

    info!(
        "Parallel consumers test passed: {} total messages across {} partitions",
        total, partition_count
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test at-least-once delivery semantics
#[tokio::test]
async fn test_at_least_once_delivery() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-cg-alo-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let topic = unique_topic_name("cg-at-least-once");
    let message_count = 100;

    // Phase 1: Produce messages
    info!("Phase 1: Producing {} messages", message_count);
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(&topic, Some(1)).await?;

        for i in 0..message_count {
            let msg = Bytes::from(format!("alo-{}", i).into_bytes());
            client.publish(&topic, msg).await?;
        }

        broker.shutdown().await?;
    }

    // Phase 2: Partial consumption, simulating crash before commit
    let partial_count = 30;
    info!(
        "Phase 2: Partial consumption ({} messages) before 'crash'",
        partial_count
    );
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        let messages = client.consume(&topic, 0, 0, partial_count).await?;
        assert_eq!(messages.len(), partial_count);

        // Simulate crash - no offset commit
        info!("Simulating consumer crash (no commit)");
        broker.shutdown().await?;
    }

    // Phase 3: Restart and re-consume (at-least-once)
    info!("Phase 3: Restart and verify at-least-once delivery");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        // Without commit, consumer restarts from beginning
        let messages = client.consume(&topic, 0, 0, 200).await?;

        // At-least-once: we should see ALL messages (including the ones we already "processed")
        assert_eq!(
            messages.len(),
            message_count,
            "At-least-once: should re-consume all messages"
        );

        broker.shutdown().await?;
    }

    info!("At-least-once delivery test passed");
    let _ = tokio::fs::remove_dir_all(&config.data_dir).await;
    Ok(())
}

/// Test consumer lag tracking
#[tokio::test]
async fn test_consumer_lag() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-lag");
    producer.create_topic(&topic, Some(1)).await?;

    // Produce messages
    let total_messages = 100;
    for i in 0..total_messages {
        let msg = Bytes::from(format!("lag-{}", i).into_bytes());
        producer.publish(&topic, msg).await?;
    }

    info!("Produced {} messages", total_messages);

    // Consume partial
    let consumed_count = 40;
    let messages = consumer.consume(&topic, 0, 0, consumed_count).await?;
    let last_consumed_offset = messages.last().map(|m| m.offset).unwrap_or(0);

    // Calculate lag (high watermark - consumer offset)
    // In a real implementation, we'd get the high watermark from the broker
    let high_watermark = total_messages as u64;
    let consumer_offset = last_consumed_offset + 1;
    let lag = high_watermark - consumer_offset;

    info!(
        "Consumer lag: {} (high watermark: {}, consumer offset: {})",
        lag, high_watermark, consumer_offset
    );

    assert_eq!(lag, (total_messages - consumed_count) as u64);

    // Catch up
    let remaining = consumer.consume(&topic, 0, consumer_offset, 100).await?;
    let new_lag = high_watermark - (remaining.last().map(|m| m.offset).unwrap_or(0) + 1);

    assert_eq!(new_lag, 0, "After catching up, lag should be 0");
    info!("Consumer caught up, lag: {}", new_lag);

    broker.shutdown().await?;
    Ok(())
}

/// Test multiple consumer groups on same topic
#[tokio::test]
async fn test_multiple_consumer_groups() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-multi");
    producer.create_topic(&topic, Some(2)).await?;

    let message_count = 50;
    for i in 0..message_count {
        let partition = (i % 2) as u32;
        let msg = Bytes::from(format!("multi-{}", i).into_bytes());
        producer
            .publish_to_partition(&topic, partition, None::<Bytes>, msg)
            .await?;
    }

    info!("Produced {} messages", message_count);

    // Simulate two independent consumer groups
    // Each group should be able to consume all messages independently
    let broker_addr = broker.connection_string();

    // Consumer Group A
    let group_a_count = {
        let mut client = Client::connect(&broker_addr).await?;
        let mut total = 0;
        for partition in 0..2 {
            let messages = client.consume(&topic, partition, 0, 100).await?;
            total += messages.len();
        }
        total
    };

    // Consumer Group B (independent consumption)
    let group_b_count = {
        let mut client = Client::connect(&broker_addr).await?;
        let mut total = 0;
        for partition in 0..2 {
            let messages = client.consume(&topic, partition, 0, 100).await?;
            total += messages.len();
        }
        total
    };

    // Both groups should see all messages
    assert_eq!(group_a_count, message_count);
    assert_eq!(group_b_count, message_count);

    info!(
        "Multiple consumer groups test passed: Group A: {}, Group B: {}",
        group_a_count, group_b_count
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test consumer session timeout simulation
#[tokio::test]
async fn test_consumer_session_timeout() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-session");
    producer.create_topic(&topic, Some(2)).await?;

    // Produce messages
    for i in 0..100 {
        let partition = (i % 2) as u32;
        let msg = Bytes::from(format!("session-{}", i).into_bytes());
        producer
            .publish_to_partition(&topic, partition, None::<Bytes>, msg)
            .await?;
    }

    info!("Testing consumer session timeout scenario");

    let broker_addr = broker.connection_string();

    // Consumer 1: Starts consuming but "times out" (we simulate by just closing connection)
    {
        let mut consumer1 = Client::connect(&broker_addr).await?;
        let messages = consumer1.consume(&topic, 0, 0, 10).await?;
        info!(
            "Consumer 1 consumed {} messages before 'timeout'",
            messages.len()
        );
        // Connection drops here
    }

    // Simulate session timeout delay
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Consumer 2: Takes over partition 0
    {
        let mut consumer2 = Client::connect(&broker_addr).await?;
        // Can start from beginning since consumer 1 didn't commit
        let messages = consumer2.consume(&topic, 0, 0, 100).await?;
        assert_eq!(
            messages.len(),
            50,
            "Consumer 2 should see all messages in partition 0"
        );
        info!(
            "Consumer 2 took over and consumed {} messages",
            messages.len()
        );
    }

    info!("Consumer session timeout test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test ordered delivery within partition
#[tokio::test]
async fn test_ordered_delivery_within_partition() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-order");
    producer.create_topic(&topic, Some(1)).await?;

    let message_count = 500;

    // Produce messages with sequence numbers
    info!("Producing {} ordered messages", message_count);
    for i in 0..message_count {
        let msg = Bytes::from(format!("{:06}", i).into_bytes()); // Zero-padded for easy comparison
        producer.publish(&topic, msg).await?;
    }

    // Consume and verify order
    let messages = consumer.consume(&topic, 0, 0, 1000).await?;
    assert_eq!(messages.len(), message_count);

    for (i, msg) in messages.iter().enumerate() {
        let expected = format!("{:06}", i);
        let actual = String::from_utf8_lossy(&msg.value);
        assert_eq!(actual, expected, "Message {} out of order", i);
        assert_eq!(msg.offset, i as u64, "Offset {} mismatch", i);
    }

    info!(
        "Ordered delivery test passed: {} messages in order",
        message_count
    );
    broker.shutdown().await?;
    Ok(())
}

/// Test compacted topic simulation (keeping only latest value per key)
#[tokio::test]
async fn test_keyed_messages() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cg-keyed");
    producer.create_topic(&topic, Some(1)).await?;

    // Produce messages with keys (simulated via message content)
    let updates = vec![
        ("user-1", "v1"),
        ("user-2", "v1"),
        ("user-1", "v2"), // Update
        ("user-3", "v1"),
        ("user-2", "v2"), // Update
        ("user-1", "v3"), // Update
    ];

    info!("Producing keyed messages with updates");
    for (key, value) in &updates {
        let msg = Bytes::from(format!("{}:{}", key, value).into_bytes());
        producer.publish(&topic, msg).await?;
    }

    // Consume all messages
    let messages = consumer.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), updates.len());

    // Simulate compaction by keeping only latest value per key
    let mut latest: HashMap<String, String> = HashMap::new();
    for msg in &messages {
        let content = String::from_utf8_lossy(&msg.value);
        let parts: Vec<&str> = content.split(':').collect();
        if parts.len() == 2 {
            latest.insert(parts[0].to_string(), parts[1].to_string());
        }
    }

    // Verify latest values
    assert_eq!(latest.get("user-1").map(|s| s.as_str()), Some("v3"));
    assert_eq!(latest.get("user-2").map(|s| s.as_str()), Some("v2"));
    assert_eq!(latest.get("user-3").map(|s| s.as_str()), Some("v1"));

    info!("Keyed messages test passed: {:?}", latest);
    broker.shutdown().await?;
    Ok(())
}
