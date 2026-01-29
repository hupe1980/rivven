//! End-to-end pipeline tests
//!
//! Tests for complete data pipelines through Rivven, including:
//! - Source → Broker → Sink workflows
//! - Multi-stage pipelines
//! - Data transformation pipelines
//!
//! Run with: cargo test -p rivven-integration-tests --test e2e_pipeline -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_integration_tests::fixtures::{test_data, TestBroker, TestPostgres};
use rivven_integration_tests::helpers::*;
use std::time::Duration;
use tracing::info;

/// Test basic end-to-end pipeline: produce → broker → consume
#[tokio::test]
async fn test_e2e_basic_pipeline() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut producer = Client::connect(&broker.connection_string()).await?;
    let mut consumer = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("e2e-basic");
    producer.create_topic(&topic, Some(1)).await?;

    // Produce messages
    let events = test_data::generate_events(50, "user_action");
    for event in &events {
        producer.publish(&topic, event.to_bytes()).await?;
    }
    info!("Produced {} events", events.len());

    // Consume and verify
    let messages = consumer.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 50);

    // Verify event integrity
    for (original, received) in events.iter().zip(messages.iter()) {
        let parsed: test_data::TestEvent = serde_json::from_slice(&received.value)?;
        assert_eq!(parsed.event_type, original.event_type);
        assert_eq!(parsed.id, original.id);
    }

    info!("E2E basic pipeline test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test multi-topic pipeline: events routed to different topics
#[tokio::test]
async fn test_e2e_multi_topic_pipeline() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    // Create topics for different event types
    let user_topic = unique_topic_name("events-user");
    let order_topic = unique_topic_name("events-order");
    let system_topic = unique_topic_name("events-system");

    client.create_topic(&user_topic, Some(1)).await?;
    client.create_topic(&order_topic, Some(1)).await?;
    client.create_topic(&system_topic, Some(1)).await?;

    // Route events by type
    let user_events = test_data::generate_events(20, "user");
    let order_events = test_data::generate_events(30, "order");
    let system_events = test_data::generate_events(10, "system");

    for event in &user_events {
        client.publish(&user_topic, event.to_bytes()).await?;
    }
    for event in &order_events {
        client.publish(&order_topic, event.to_bytes()).await?;
    }
    for event in &system_events {
        client.publish(&system_topic, event.to_bytes()).await?;
    }

    // Verify each topic has correct count
    let user_msgs = client.consume(&user_topic, 0, 0, 100).await?;
    let order_msgs = client.consume(&order_topic, 0, 0, 100).await?;
    let system_msgs = client.consume(&system_topic, 0, 0, 100).await?;

    assert_eq!(user_msgs.len(), 20);
    assert_eq!(order_msgs.len(), 30);
    assert_eq!(system_msgs.len(), 10);

    info!("Multi-topic pipeline test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test fan-out pattern: one producer, multiple consumers
#[tokio::test]
async fn test_e2e_fanout_pattern() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let addr = broker.connection_string();

    let topic = unique_topic_name("fanout");
    {
        let mut client = Client::connect(&addr).await?;
        client.create_topic(&topic, Some(1)).await?;
    }

    // Producer task
    let producer_addr = addr.clone();
    let producer_topic = topic.clone();
    let producer_handle = tokio::spawn(async move {
        let mut client = Client::connect(&producer_addr).await?;
        for i in 0..100 {
            client
                .publish(&producer_topic, format!("event-{}", i).into_bytes())
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    // Wait for producer
    producer_handle.await??;

    // Multiple consumers reading the same data
    let mut consumers = Vec::new();
    for i in 0..3 {
        let consumer_addr = addr.clone();
        let consumer_topic = topic.clone();
        consumers.push(tokio::spawn(async move {
            let mut client = Client::connect(&consumer_addr).await?;
            let messages = client.consume(&consumer_topic, 0, 0, 200).await?;
            info!("Consumer {} read {} messages", i, messages.len());
            Ok::<_, anyhow::Error>(messages.len())
        }));
    }

    // All consumers should see all messages
    for handle in consumers {
        let count = handle.await??;
        assert_eq!(count, 100);
    }

    info!("Fan-out pattern test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test partitioned processing: messages distributed across partitions
#[tokio::test]
async fn test_e2e_partitioned_processing() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("partitioned");
    let num_partitions = 4;
    client.create_topic(&topic, Some(num_partitions)).await?;

    // Produce to specific partitions
    for partition in 0..num_partitions {
        for i in 0..25 {
            client
                .publish_to_partition(
                    &topic,
                    partition,
                    Some(Bytes::from(format!("key-{}", partition))),
                    format!("p{}-msg{}", partition, i).into_bytes(),
                )
                .await?;
        }
    }

    // Verify each partition
    for partition in 0..num_partitions {
        let messages = client.consume(&topic, partition, 0, 100).await?;
        assert_eq!(messages.len(), 25);

        // Verify all messages belong to this partition
        for msg in &messages {
            let content = String::from_utf8_lossy(&msg.value);
            assert!(content.starts_with(&format!("p{}-", partition)));
        }
    }

    info!("Partitioned processing test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test streaming window: time-based batching
#[tokio::test]
async fn test_e2e_streaming_window() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("streaming-window");
    client.create_topic(&topic, Some(1)).await?;

    // Simulate streaming: produce in batches over time
    for batch in 0..5 {
        for i in 0..10 {
            client
                .publish(&topic, format!("batch{}-msg{}", batch, i).into_bytes())
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Consume all
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 50);

    info!("Streaming window test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test CDC to broker pipeline (PostgreSQL → Rivven)
#[tokio::test]
async fn test_e2e_cdc_to_broker() -> Result<()> {
    init_tracing();

    // Start infrastructure
    let pg = TestPostgres::start().await?;
    let broker = TestBroker::start().await?;

    pg.setup_test_schema().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("cdc-events");
    client.create_topic(&topic, Some(1)).await?;

    // Simulate CDC pipeline: database changes → broker
    let user_ids = pg.insert_test_users(10).await?;

    // In a real CDC setup, changes would stream automatically
    // Here we simulate by publishing change events
    for (i, id) in user_ids.iter().enumerate() {
        let change_event = serde_json::json!({
            "op": "c",  // create
            "table": "users",
            "after": {
                "id": id,
                "username": format!("user_{}", i),
                "email": format!("user{}@example.com", i)
            }
        });
        client
            .publish(&topic, serde_json::to_vec(&change_event)?)
            .await?;
    }

    // Simulate updates
    for &id in user_ids.iter().take(5) {
        pg.update_user(id, &format!("updated_{}", id)).await?;
        let change_event = serde_json::json!({
            "op": "u",  // update
            "table": "users",
            "before": { "id": id },
            "after": { "id": id, "username": format!("updated_{}", id) }
        });
        client
            .publish(&topic, serde_json::to_vec(&change_event)?)
            .await?;
    }

    // Verify all events captured
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 15); // 10 inserts + 5 updates

    info!("CDC to broker pipeline test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test exactly-once semantics simulation
#[tokio::test]
async fn test_e2e_exactly_once() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("exactly-once");
    client.create_topic(&topic, Some(1)).await?;

    // Produce messages with idempotency keys
    let mut published_ids = std::collections::HashSet::new();
    for i in 0..50 {
        let msg_id = format!("msg-{}", i);
        if !published_ids.contains(&msg_id) {
            client
                .publish_with_key(
                    &topic,
                    Some(Bytes::from(msg_id.clone())),
                    format!("data-{}", i).into_bytes(),
                )
                .await?;
            published_ids.insert(msg_id);
        }
    }

    // Simulate duplicate sends (should be deduplicated at consumer)
    let messages = client.consume(&topic, 0, 0, 100).await?;
    assert_eq!(messages.len(), 50);

    info!("Exactly-once simulation test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Throughput benchmark for E2E pipeline
#[tokio::test]
#[ignore = "Benchmark test"]
async fn bench_e2e_throughput() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("bench-e2e");
    client.create_topic(&topic, Some(4)).await?;

    let msg_size = 1000; // 1KB
    let msg_count = 50_000;
    let message = vec![b'X'; msg_size];

    let start = std::time::Instant::now();
    for i in 0..msg_count {
        let partition = (i % 4) as u32;
        client
            .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
            .await?;
    }
    let produce_duration = start.elapsed();

    let produce_rate = msg_count as f64 / produce_duration.as_secs_f64();
    let produce_mb = (msg_count * msg_size) as f64 / produce_duration.as_secs_f64() / 1_000_000.0;

    info!(
        "Produce: {:.0} msgs/sec, {:.2} MB/sec",
        produce_rate, produce_mb
    );

    // Consume benchmark
    let start = std::time::Instant::now();
    let mut total_consumed = 0;
    for partition in 0..4u32 {
        let messages = client.consume(&topic, partition, 0, 20_000).await?;
        total_consumed += messages.len();
    }
    let consume_duration = start.elapsed();

    let consume_rate = total_consumed as f64 / consume_duration.as_secs_f64();
    info!("Consume: {:.0} msgs/sec", consume_rate);

    assert_eq!(total_consumed, msg_count);

    broker.shutdown().await?;
    Ok(())
}

/// Test data integrity across pipeline
#[tokio::test]
async fn test_e2e_data_integrity() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("integrity");
    client.create_topic(&topic, Some(1)).await?;

    // Generate diverse data types
    let test_cases = vec![
        ("empty", Bytes::new()),
        ("binary", Bytes::from(vec![0u8, 1, 2, 255, 254, 253])),
        ("unicode", Bytes::from("Hello 世界 🌍")),
        ("json", Bytes::from(r#"{"key": "value", "number": 42}"#)),
        ("large", Bytes::from(vec![b'A'; 100_000])),
    ];

    for (name, data) in &test_cases {
        client
            .publish_with_key(&topic, Some(Bytes::from(*name)), data.clone())
            .await?;
    }

    let messages = client.consume(&topic, 0, 0, 10).await?;
    assert_eq!(messages.len(), test_cases.len());

    for (i, (name, expected)) in test_cases.iter().enumerate() {
        let actual = &messages[i].value;
        assert_eq!(
            actual.as_ref(),
            expected.as_ref(),
            "Data integrity failed for '{}'",
            name
        );
    }

    info!("Data integrity test passed");
    broker.shutdown().await?;
    Ok(())
}
