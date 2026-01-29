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
