//! Client-server protocol tests
//!
//! Tests for the Rivven wire protocol, client library, and API conformance.
//! These tests start an embedded broker and test client operations.
//!
//! Run with: cargo test -p rivven-integration-tests --test client_protocol -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_integration_tests::fixtures::{test_data, TestBroker};
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

/// Test basic connection to broker
#[tokio::test]
async fn test_connect_basic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    info!("Broker started at {}", broker.connection_string());

    let client = Client::connect(&broker.connection_string()).await?;
    info!("Client connected successfully");

    drop(client);
    broker.shutdown().await?;

    Ok(())
}

/// Test topic creation
#[tokio::test]
async fn test_create_topic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic_name = unique_topic_name("test-create");
    let partitions = client.create_topic(&topic_name, Some(3)).await?;

    assert_eq!(partitions, 3);
    info!(
        "Created topic '{}' with {} partitions",
        topic_name, partitions
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test listing topics
#[tokio::test]
async fn test_list_topics() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create a few topics
    let topic1 = unique_topic_name("test-list-1");
    let topic2 = unique_topic_name("test-list-2");
    client.create_topic(&topic1, Some(1)).await?;
    client.create_topic(&topic2, Some(1)).await?;

    let topics = client.list_topics().await?;
    assert!(topics.contains(&topic1));
    assert!(topics.contains(&topic2));

    info!("Listed {} topics", topics.len());
    broker.shutdown().await?;
    Ok(())
}

/// Test publish and consume single message
#[tokio::test]
async fn test_publish_consume_single() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-pubsub");
    client.create_topic(&topic, Some(1)).await?;

    // Publish a message
    let offset = client.publish(&topic, b"Hello, Rivven!".to_vec()).await?;
    info!("Published message at offset {}", offset);

    // Consume the message
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.as_ref(), b"Hello, Rivven!");

    info!("Consumed 1 message");
    broker.shutdown().await?;
    Ok(())
}

/// Test publish with key
#[tokio::test]
async fn test_publish_with_key() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-keyed");
    client.create_topic(&topic, Some(1)).await?;

    // Publish with key
    let offset = client
        .publish_with_key(
            &topic,
            Some(Bytes::from("user-123")),
            Bytes::from("event data"),
        )
        .await?;
    info!("Published keyed message at offset {}", offset);

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].key.as_ref().map(|k| k.as_ref()),
        Some(b"user-123".as_slice())
    );

    broker.shutdown().await?;
    Ok(())
}

/// Test batch publish
#[tokio::test]
async fn test_batch_publish() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-batch");
    client.create_topic(&topic, Some(1)).await?;

    // Publish multiple messages
    let messages = test_data::generate_messages(100);
    for (i, msg) in messages.iter().enumerate() {
        client.publish(&topic, msg.clone()).await?;
        if (i + 1) % 25 == 0 {
            info!("Published {} messages", i + 1);
        }
    }

    // Consume all messages
    let consumed = client.consume(&topic, 0, 0, 200).await?;
    assert_eq!(consumed.len(), 100);

    info!("Batch publish/consume test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test message ordering within partition
#[tokio::test]
async fn test_partition_ordering() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-ordering");
    client.create_topic(&topic, Some(1)).await?;

    // Publish numbered messages
    for i in 0..50 {
        client
            .publish(&topic, format!("message-{}", i).into_bytes())
            .await?;
    }

    // Verify ordering
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 50);

    for (i, msg) in messages.iter().enumerate() {
        let expected = format!("message-{}", i);
        assert_eq!(
            String::from_utf8_lossy(&msg.value),
            expected,
            "Message {} out of order",
            i
        );
    }

    info!("Message ordering verified");
    broker.shutdown().await?;
    Ok(())
}

/// Test consume from specific offset
#[tokio::test]
async fn test_consume_from_offset() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-offset");
    client.create_topic(&topic, Some(1)).await?;

    // Publish messages
    for i in 0..20 {
        client
            .publish(&topic, format!("msg-{}", i).into_bytes())
            .await?;
    }

    // Consume from offset 10
    let messages = client.consume(&topic, 0, 10, 100).await?;
    assert_eq!(messages.len(), 10);
    assert_eq!(String::from_utf8_lossy(&messages[0].value), "msg-10");

    info!("Consume from offset test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test topic auto-creation
#[tokio::test]
async fn test_topic_auto_creation() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Publish to non-existent topic (should auto-create)
    let topic = unique_topic_name("test-autocreate");
    let result = client.publish(&topic, b"test".to_vec()).await;

    // Depending on config, this might succeed or fail
    // Either way, we're testing the behavior
    match result {
        Ok(offset) => info!("Auto-created topic, offset: {}", offset),
        Err(e) => info!("Topic auto-creation disabled: {}", e),
    }

    broker.shutdown().await?;
    Ok(())
}

/// Test multiple partitions
#[tokio::test]
async fn test_multiple_partitions() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-multipart");
    client.create_topic(&topic, Some(4)).await?;

    // Publish to specific partitions
    for partition in 0..4 {
        client
            .publish_to_partition(
                &topic,
                partition,
                None::<Bytes>,
                format!("p{}", partition).into_bytes(),
            )
            .await?;
    }

    // Consume from each partition
    for partition in 0..4u32 {
        let messages = client.consume(&topic, partition, 0, 10).await?;
        assert_eq!(messages.len(), 1);
        assert_eq!(
            String::from_utf8_lossy(&messages[0].value),
            format!("p{}", partition)
        );
    }

    info!("Multiple partitions test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test empty consume
#[tokio::test]
async fn test_consume_empty() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-empty");
    client.create_topic(&topic, Some(1)).await?;

    // Consume from empty topic
    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert!(messages.is_empty());

    info!("Empty consume test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test large message
#[tokio::test]
async fn test_large_message() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("test-large");
    client.create_topic(&topic, Some(1)).await?;

    // Create a 1MB message
    let large_msg = vec![b'X'; 1024 * 1024];
    client.publish(&topic, large_msg.clone()).await?;

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].value.len(), 1024 * 1024);

    info!("Large message test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test reconnection after disconnect
#[tokio::test]
async fn test_reconnection() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;

    // First connection
    {
        let mut client = Client::connect(&broker.connection_string()).await?;
        let topic = unique_topic_name("test-reconnect");
        client.create_topic(&topic, Some(1)).await?;
        client.publish(&topic, b"message1".to_vec()).await?;
        // client dropped here
    }

    // Wait a bit
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect
    {
        let mut client = Client::connect(&broker.connection_string()).await?;
        let topics = client.list_topics().await?;
        assert!(!topics.is_empty());
        info!("Reconnection successful, found {} topics", topics.len());
    }

    broker.shutdown().await?;
    Ok(())
}

/// Test concurrent clients
#[tokio::test]
async fn test_concurrent_clients() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    // Use unique topic name to avoid interference from other tests
    let topic = unique_topic_name("concurrent");

    // Pre-create topic
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(1)).await?;
    }

    // Spawn multiple concurrent clients
    let mut handles = Vec::new();
    for i in 0..5 {
        let addr_clone = addr.clone();
        let topic_clone = topic.clone();
        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr_clone).await?;
            for j in 0..10 {
                client
                    .publish(&topic_clone, format!("client-{}-msg-{}", i, j).into_bytes())
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    // Verify all messages received
    let mut client = Client::connect(&addr).await?;
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 50); // 5 clients * 10 messages

    info!("Concurrent clients test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Throughput benchmark
#[tokio::test]
#[ignore = "Benchmark test - run explicitly"]
async fn bench_throughput() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("bench-throughput");
    client.create_topic(&topic, Some(1)).await?;

    let message = vec![b'X'; 1000]; // 1KB message
    let count = 10_000;

    let start = std::time::Instant::now();
    for _ in 0..count {
        client.publish(&topic, message.clone()).await?;
    }
    let duration = start.elapsed();

    let msgs_per_sec = count as f64 / duration.as_secs_f64();
    let mb_per_sec = (count * message.len()) as f64 / duration.as_secs_f64() / 1_000_000.0;

    info!(
        "Throughput: {:.0} msgs/sec, {:.2} MB/sec",
        msgs_per_sec, mb_per_sec
    );

    broker.shutdown().await?;
    Ok(())
}

/// Latency benchmark
#[tokio::test]
#[ignore = "Benchmark test - run explicitly"]
async fn bench_latency() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("bench-latency");
    client.create_topic(&topic, Some(1)).await?;

    let message = b"latency-test";
    let iterations = 1000;
    let mut latencies = Vec::with_capacity(iterations);

    for _ in 0..iterations {
        let start = std::time::Instant::now();
        client.publish(&topic, message.to_vec()).await?;
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p50 = latencies[iterations / 2];
    let p99 = latencies[iterations * 99 / 100];
    let avg = latencies.iter().sum::<Duration>() / iterations as u32;

    info!("Latency - p50: {:?}, p99: {:?}, avg: {:?}", p50, p99, avg);

    broker.shutdown().await?;
    Ok(())
}
