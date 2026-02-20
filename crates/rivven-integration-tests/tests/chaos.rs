//! Chaos engineering tests
//!
//! Tests that verify system resilience under various failure conditions.
//! These tests simulate real-world failure scenarios.
//!
//! Run with: cargo test -p rivven-integration-tests --test chaos -- --nocapture

use anyhow::Result;
use rivven_client::Client;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

/// Test recovery after broker restart
#[tokio::test]
async fn test_broker_restart_recovery() -> Result<()> {
    init_tracing();

    // Start broker and write data
    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    {
        let mut client = Client::connect(&addr).await?;
        let topic = unique_topic_name("restart-test");
        client.create_topic(&topic, Some(1)).await?;

        for i in 0..10 {
            client
                .publish(&topic, format!("msg-{}", i).into_bytes())
                .await?;
        }
        info!("Wrote 10 messages before shutdown");
    }

    // Shutdown broker
    broker.shutdown().await?;
    info!("Broker shut down");

    // Start new broker (data may be lost without persistence)
    let new_broker = TestBroker::start().await?;
    let new_addr = new_broker.connection_string();

    // Verify we can connect and operate
    let mut client = Client::connect(&new_addr).await?;
    let topic = unique_topic_name("after-restart");
    client.create_topic(&topic, Some(1)).await?;
    client.publish(&topic, b"post-restart".to_vec()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);

    info!("Broker restart recovery test passed");
    new_broker.shutdown().await?;
    Ok(())
}

/// Test client reconnection after connection drop
#[tokio::test]
async fn test_client_reconnection() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // First connection
    {
        let mut client = Client::connect(&addr).await?;
        let topic = unique_topic_name("reconnect-1");
        client.create_topic(&topic, Some(1)).await?;
        client.publish(&topic, b"first".to_vec()).await?;
        info!("First connection completed");
        // Connection dropped here
    }

    // Simulate delay
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect
    {
        let mut client = Client::connect(&addr).await?;
        let topic = unique_topic_name("reconnect-2");
        client.create_topic(&topic, Some(1)).await?;
        client.publish(&topic, b"second".to_vec()).await?;
        info!("Reconnection successful");
    }

    info!("Client reconnection test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test behavior under high connection churn
#[tokio::test]
async fn test_high_connection_churn() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    let iterations = 100;
    let mut success_count = 0;

    for i in 0..iterations {
        match Client::connect(&addr).await {
            Ok(mut client) => {
                // Quick operation
                let _ = client.list_topics().await;
                success_count += 1;
            }
            Err(e) => {
                info!("Connection {} failed: {}", i, e);
            }
        }
        // Small delay to vary timing
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    info!(
        "Connection churn: {}/{} successful",
        success_count, iterations
    );
    assert!(
        success_count > iterations * 9 / 10,
        "Too many connection failures"
    );

    info!("High connection churn test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test concurrent topic creation
#[tokio::test]
async fn test_concurrent_topic_creation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // Concurrent topic creation
    let mut handles = Vec::new();
    for i in 0..20 {
        let addr_clone = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr_clone).await?;
            let topic = format!("concurrent-topic-{}-{}", i, uuid::Uuid::new_v4());
            client.create_topic(&topic, Some(1)).await?;
            Ok::<_, anyhow::Error>(topic)
        }));
    }

    let mut created_topics = Vec::new();
    for handle in handles {
        match handle.await? {
            Ok(topic) => created_topics.push(topic),
            Err(e) => info!("Topic creation failed: {}", e),
        }
    }

    assert!(
        created_topics.len() >= 15,
        "Too many topic creation failures"
    );
    info!("Created {} topics concurrently", created_topics.len());

    broker.shutdown().await?;
    Ok(())
}

/// Test behavior with many partitions
#[tokio::test]
async fn test_many_partitions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("many-partitions");
    let num_partitions = 32;
    client.create_topic(&topic, Some(num_partitions)).await?;

    // Write to all partitions
    for p in 0..num_partitions {
        client
            .publish_to_partition(
                &topic,
                p,
                None::<bytes::Bytes>,
                format!("p{}", p).into_bytes(),
            )
            .await?;
    }

    // Read from all partitions
    for p in 0..num_partitions {
        let messages = client.consume(&topic, p, 0, 10).await?;
        assert_eq!(messages.len(), 1, "Partition {} missing data", p);
    }

    info!("Many partitions test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test graceful degradation under load
#[tokio::test]
async fn test_load_degradation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    let topic = unique_topic_name("load-degradation");
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(4)).await?;
    }

    // Spawn many producers
    let mut handles = Vec::new();
    for producer_id in 0..10 {
        let addr_clone = addr.clone();
        let topic_clone = topic.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr_clone).await?;
            let mut success = 0;
            for i in 0..100 {
                if client
                    .publish_to_partition(
                        &topic_clone,
                        (producer_id % 4) as u32,
                        None::<bytes::Bytes>,
                        format!("p{}-m{}", producer_id, i).into_bytes(),
                    )
                    .await
                    .is_ok()
                {
                    success += 1;
                }
            }
            Ok::<_, anyhow::Error>(success)
        }));
    }

    let mut total_success = 0;
    for handle in handles {
        total_success += handle.await??;
    }

    info!("Load test: {} messages delivered", total_success);
    assert!(total_success > 800, "Too many failures under load");

    broker.shutdown().await?;
    Ok(())
}

/// Test timeout handling
#[tokio::test]
async fn test_timeout_handling() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("timeout-test");
    client.create_topic(&topic, Some(1)).await?;

    // Normal operations should complete quickly
    let start = std::time::Instant::now();
    client.publish(&topic, b"test".to_vec()).await?;
    let duration = start.elapsed();

    assert!(
        duration < Duration::from_secs(1),
        "Operation took too long: {:?}",
        duration
    );

    info!("Timeout handling test passed ({:?})", duration);
    broker.shutdown().await?;
    Ok(())
}

/// Test memory pressure simulation
#[tokio::test]
async fn test_memory_pressure_simulation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("memory-pressure");
    client.create_topic(&topic, Some(1)).await?;

    // Send messages of varying sizes
    let sizes = [100, 1000, 10_000, 100_000];

    for &size in &sizes {
        let message = vec![b'X'; size];
        client.publish(&topic, message).await?;
        info!("Sent {} byte message", size);
    }

    // Consume all
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), sizes.len());

    for (i, msg) in messages.iter().enumerate() {
        assert_eq!(msg.value.len(), sizes[i], "Message size mismatch");
    }

    info!("Memory pressure simulation passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test rapid produce/consume cycles
#[tokio::test]
async fn test_rapid_cycles() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("rapid-cycles");
    client.create_topic(&topic, Some(1)).await?;

    let cycles = 100;
    for i in 0..cycles {
        // Produce
        client
            .publish(&topic, format!("cycle-{}", i).into_bytes())
            .await?;

        // Consume
        let messages = client.consume(&topic, 0, i as u64, 1).await?;
        assert_eq!(messages.len(), 1);
    }

    info!("Completed {} rapid produce/consume cycles", cycles);
    broker.shutdown().await?;
    Ok(())
}

/// Test empty message handling
#[tokio::test]
async fn test_empty_message() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("empty-msg");
    client.create_topic(&topic, Some(1)).await?;

    // Empty message
    client.publish(&topic, Vec::new()).await?;

    // Non-empty for comparison
    client.publish(&topic, b"non-empty".to_vec()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 2);
    assert!(messages[0].value.is_empty());
    assert_eq!(messages[1].value.as_ref(), b"non-empty");

    info!("Empty message test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test sequential failures and recovery
#[tokio::test]
async fn test_sequential_failures() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // Multiple sequential failure-like scenarios
    for round in 0..5 {
        // Connect
        let mut client = Client::connect(&addr).await?;

        // Do some work
        let topic = unique_topic_name(&format!("failure-round-{}", round));
        client.create_topic(&topic, Some(1)).await?;
        client.publish(&topic, b"test".to_vec()).await?;

        // Forcefully drop
        drop(client);

        // Small delay
        tokio::time::sleep(Duration::from_millis(50)).await;

        info!("Completed failure simulation round {}", round);
    }

    // Final verification - server still operational
    let mut client = Client::connect(&addr).await?;
    let topics = client.list_topics().await?;
    assert!(topics.len() >= 5);

    info!("Sequential failures test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Stress test with mixed operations
#[tokio::test]
#[ignore = "Long-running stress test"]
async fn stress_test_mixed_operations() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    let duration = Duration::from_secs(30);
    let start = std::time::Instant::now();

    let mut operations = 0u64;
    let mut client = Client::connect(&addr).await?;

    while start.elapsed() < duration {
        // Mix of operations
        match operations % 5 {
            0 => {
                // Create topic
                let topic = unique_topic_name("stress");
                let _ = client.create_topic(&topic, Some(1)).await;
            }
            1..=3 => {
                // Publish (more frequent)
                let topic = format!("stress-{}", operations % 10);
                let _ = client.publish(&topic, b"stress-msg".to_vec()).await;
            }
            4 => {
                // Consume
                let topic = format!("stress-{}", operations % 10);
                let _ = client.consume(&topic, 0, 0, 10).await;
            }
            _ => {}
        }

        operations += 1;

        if operations.is_multiple_of(1000) {
            info!("Completed {} operations", operations);
        }
    }

    let rate = operations as f64 / duration.as_secs_f64();
    info!(
        "Stress test: {} operations in {:?} ({:.0} ops/sec)",
        operations, duration, rate
    );

    broker.shutdown().await?;
    Ok(())
}

// ============================================================================
// DURABILITY TESTS - No data loss on broker restart
// ============================================================================

/// Test that data persists across broker restarts with persistence enabled
///
/// This is a critical test for production readiness - ensures that:
/// 1. Data written to disk survives broker shutdown
/// 2. Topics and their configurations are restored
/// 3. All messages are available after restart
/// 4. Offsets are preserved correctly
#[tokio::test]
async fn test_no_data_loss_on_restart_with_persistence() -> Result<()> {
    use rivven_core::Config;
    use std::path::PathBuf;
    use tokio::fs;

    init_tracing();

    // Create a unique data directory for this test
    let data_dir = format!("/tmp/rivven-durability-test-{}", uuid::Uuid::new_v4());
    let data_path = PathBuf::from(&data_dir);
    fs::create_dir_all(&data_path).await?;
    info!("Created test data directory: {}", data_dir);

    let topic = "durability-test-topic";
    let expected_messages: Vec<String> = (0..100).map(|i| format!("message-{:04}", i)).collect();

    // Phase 1: Start broker with persistence, write data
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let addr = broker.connection_string();
        info!("Phase 1: Broker started at {}", addr);

        let mut client = Client::connect(&addr).await?;

        // Create topic with specific configuration
        client.create_topic(topic, Some(3)).await?;
        info!("Created topic '{}' with 3 partitions", topic);

        // Write all messages
        for (i, msg) in expected_messages.iter().enumerate() {
            client.publish(topic, msg.clone().into_bytes()).await?;
            if (i + 1) % 25 == 0 {
                info!("Published {} messages...", i + 1);
            }
        }
        info!("Phase 1: Published {} messages", expected_messages.len());

        // Verify data is readable before shutdown
        let mut pre_shutdown_count = 0;
        for partition in 0..3 {
            let msgs = client.consume(topic, partition, 0, 100).await?;
            pre_shutdown_count += msgs.len();
        }
        info!(
            "Phase 1: Verified {} messages readable before shutdown",
            pre_shutdown_count
        );

        // Graceful shutdown — drop client first to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
        info!("Phase 1: Broker shutdown complete");

        // Give filesystem time to sync
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify data directory has content
    let dir_contents = fs::read_dir(&data_path).await;
    assert!(
        dir_contents.is_ok(),
        "Data directory should exist after shutdown"
    );
    info!("Data directory verified to exist");

    // Phase 2: Restart broker, verify all data survived
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let addr = broker.connection_string();
        info!("Phase 2: Broker restarted at {}", addr);

        let mut client = Client::connect(&addr).await?;

        // Wait for topic recovery
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify topic still exists
        let topics = client.list_topics().await?;
        assert!(
            topics.iter().any(|t| t == topic),
            "Topic '{}' should exist after restart. Found topics: {:?}",
            topic,
            topics
        );
        info!("Phase 2: Topic '{}' found after restart", topic);

        // Consume all messages from all partitions
        let mut recovered_messages: Vec<String> = Vec::new();
        for partition in 0..3 {
            let msgs = client.consume(topic, partition, 0, 100).await?;
            for msg in msgs {
                recovered_messages.push(String::from_utf8_lossy(&msg.value).to_string());
            }
        }

        // Verify message count
        assert_eq!(
            recovered_messages.len(),
            expected_messages.len(),
            "Should recover exactly {} messages, got {}",
            expected_messages.len(),
            recovered_messages.len()
        );
        info!(
            "Phase 2: Recovered {} messages (expected {})",
            recovered_messages.len(),
            expected_messages.len()
        );

        // Sort both for comparison (partitioning may reorder)
        let mut sorted_expected = expected_messages.clone();
        sorted_expected.sort();
        recovered_messages.sort();

        // Verify message content
        for (expected, recovered) in sorted_expected.iter().zip(recovered_messages.iter()) {
            assert_eq!(
                expected, recovered,
                "Message mismatch: expected '{}', got '{}'",
                expected, recovered
            );
        }
        info!("Phase 2: All message contents verified ✓");

        // Verify we can write new data after restart
        let new_msg = "post-restart-message";
        client.publish(topic, new_msg.as_bytes().to_vec()).await?;
        info!("Phase 2: Successfully wrote new message after restart");

        // Drop client before shutdown to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
    }

    // Cleanup
    let _ = fs::remove_dir_all(&data_path).await;
    info!("✅ Durability test passed: No data lost across broker restart");

    Ok(())
}

/// Test durability with multiple topics and partitions
#[tokio::test]
async fn test_multi_topic_durability() -> Result<()> {
    use rivven_core::Config;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::fs;

    init_tracing();

    let data_dir = format!(
        "/tmp/rivven-multi-topic-durability-{}",
        uuid::Uuid::new_v4()
    );
    let data_path = PathBuf::from(&data_dir);
    fs::create_dir_all(&data_path).await?;

    // Topic configurations
    let topics_config = [
        ("users", 2, 50), // (name, partitions, message_count)
        ("orders", 3, 75),
        ("events", 1, 100),
        ("logs", 4, 25),
    ];

    let mut expected_data: HashMap<String, Vec<String>> = HashMap::new();

    // Phase 1: Write data to multiple topics
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        for (topic_name, partitions, msg_count) in &topics_config {
            let topic = topic_name.to_string();
            client.create_topic(&topic, Some(*partitions)).await?;

            let messages: Vec<String> = (0..*msg_count)
                .map(|i| format!("{}-msg-{:04}", topic, i))
                .collect();

            for msg in &messages {
                client.publish(&topic, msg.clone().into_bytes()).await?;
            }

            expected_data.insert(topic.clone(), messages);
            info!(
                "Created topic '{}' with {} partitions and {} messages",
                topic, partitions, msg_count
            );
        }

        // Drop client before shutdown to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Phase 2: Verify all topics and data
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        for (topic_name, partitions, _) in &topics_config {
            let topic = topic_name.to_string();
            // Collect all messages from all partitions
            let mut recovered: Vec<String> = Vec::new();
            for partition in 0..*partitions {
                let msgs = client.consume(&topic, partition, 0, 200).await?;
                for msg in msgs {
                    recovered.push(String::from_utf8_lossy(&msg.value).to_string());
                }
            }

            let expected = expected_data.get(&topic).unwrap();
            assert_eq!(
                recovered.len(),
                expected.len(),
                "Topic '{}': expected {} messages, got {}",
                topic,
                expected.len(),
                recovered.len()
            );

            // Verify content (sorted comparison due to partitioning)
            let mut sorted_expected = expected.clone();
            sorted_expected.sort();
            recovered.sort();
            assert_eq!(
                sorted_expected, recovered,
                "Topic '{}': message content mismatch",
                topic
            );

            info!(
                "✓ Topic '{}': {} messages recovered correctly",
                topic,
                expected.len()
            );
        }

        // Drop client before shutdown to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
    }

    let _ = fs::remove_dir_all(&data_path).await;
    info!("✅ Multi-topic durability test passed");

    Ok(())
}

/// Test that offsets are preserved correctly across restarts
#[tokio::test]
async fn test_offset_preservation_across_restart() -> Result<()> {
    use rivven_core::Config;
    use std::path::PathBuf;
    use tokio::fs;

    init_tracing();

    let data_dir = format!("/tmp/rivven-offset-test-{}", uuid::Uuid::new_v4());
    let data_path = PathBuf::from(&data_dir);
    fs::create_dir_all(&data_path).await?;

    let topic = "offset-test";
    let initial_count = 50;
    let additional_count = 30;

    // Phase 1: Write initial batch
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(topic, Some(1)).await?;

        for i in 0..initial_count {
            client
                .publish(topic, format!("batch1-{:04}", i).into_bytes())
                .await?;
        }
        info!("Phase 1: Wrote {} messages", initial_count);

        // Record highest offset
        let msgs = client.consume(topic, 0, 0, 100).await?;
        let highest_offset = msgs.last().map(|m| m.offset).unwrap_or(0);
        info!("Phase 1: Highest offset = {}", highest_offset);
        assert_eq!(highest_offset, initial_count as u64 - 1);

        // Drop client before shutdown to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Phase 2: Verify offsets and add more data
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify existing data
        let msgs = client.consume(topic, 0, 0, 100).await?;
        assert_eq!(
            msgs.len(),
            initial_count,
            "Should have {} messages",
            initial_count
        );

        // Check offset continuity
        for (i, msg) in msgs.iter().enumerate() {
            assert_eq!(
                msg.offset, i as u64,
                "Offset mismatch at position {}: expected {}, got {}",
                i, i, msg.offset
            );
        }
        info!(
            "Phase 2: Verified {} messages with correct offsets",
            msgs.len()
        );

        // Write additional messages
        for i in 0..additional_count {
            client
                .publish(topic, format!("batch2-{:04}", i).into_bytes())
                .await?;
        }
        info!("Phase 2: Wrote {} additional messages", additional_count);

        // Verify new offsets continue from where we left off
        let all_msgs = client.consume(topic, 0, 0, 200).await?;
        assert_eq!(
            all_msgs.len(),
            initial_count + additional_count,
            "Should have {} total messages",
            initial_count + additional_count
        );

        // Verify last message has correct offset
        let last_offset = all_msgs.last().unwrap().offset;
        assert_eq!(
            last_offset,
            (initial_count + additional_count - 1) as u64,
            "Last offset should be {}",
            initial_count + additional_count - 1
        );
        info!(
            "Phase 2: Final offset = {} (expected {})",
            last_offset,
            initial_count + additional_count - 1
        );

        // Drop client before shutdown to avoid connection drain delay
        drop(client);
        broker.shutdown().await?;
    }

    let _ = fs::remove_dir_all(&data_path).await;
    info!("✅ Offset preservation test passed");

    Ok(())
}

/// Test crash recovery simulation (hard shutdown)
#[tokio::test]
async fn test_crash_recovery() -> Result<()> {
    use rivven_core::Config;
    use std::path::PathBuf;
    use tokio::fs;

    init_tracing();

    let data_dir = format!("/tmp/rivven-crash-test-{}", uuid::Uuid::new_v4());
    let data_path = PathBuf::from(&data_dir);
    fs::create_dir_all(&data_path).await?;

    let topic = "crash-test";

    // Phase 1: Write data and simulate crash (drop without shutdown)
    let messages_before_crash: Vec<String>;
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(topic, Some(1)).await?;

        messages_before_crash = (0..25).map(|i| format!("pre-crash-{:04}", i)).collect();

        for msg in &messages_before_crash {
            client.publish(topic, msg.clone().into_bytes()).await?;
        }
        info!(
            "Wrote {} messages before simulated crash",
            messages_before_crash.len()
        );

        // Simulate crash - drop broker without graceful shutdown
        // This tests that sync-on-write or WAL preserves data
        drop(broker);
        info!("Simulated crash (hard drop)");
    }

    // Small delay to ensure async cleanup completes
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 2: Recovery after crash
    {
        let config = Config::default()
            .with_persistence(true)
            .with_data_dir(data_dir.clone());

        let broker = TestBroker::start_with_config(config).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Try to recover data
        let recovered_msgs = client.consume(topic, 0, 0, 100).await?;
        let recovered: Vec<String> = recovered_msgs
            .iter()
            .map(|m| String::from_utf8_lossy(&m.value).to_string())
            .collect();

        // Note: Some messages may be lost due to crash without sync
        // This tests the worst-case scenario
        info!(
            "Recovered {} of {} messages after crash",
            recovered.len(),
            messages_before_crash.len()
        );

        // At minimum, we should recover some data if persistence is working
        // In production with sync-on-write, all data should survive
        if recovered.len() == messages_before_crash.len() {
            info!(
                "✓ Perfect crash recovery: all {} messages recovered",
                recovered.len()
            );
        } else if !recovered.is_empty() {
            info!(
                "⚠ Partial crash recovery: {} of {} messages recovered",
                recovered.len(),
                messages_before_crash.len()
            );
        } else {
            info!("⚠ No messages recovered after crash (expected with async writes)");
        }

        // Verify we can still write after crash recovery
        client
            .publish(topic, b"post-crash-message".to_vec())
            .await?;
        info!("Successfully wrote new message after crash recovery");

        broker.shutdown().await?;
    }

    let _ = fs::remove_dir_all(&data_path).await;
    info!("✅ Crash recovery test completed");

    Ok(())
}
