//! Stress tests for Rivven platform
//!
//! Tests for high-load scenarios and system stability:
//! - High throughput under sustained load
//! - Memory stability during long operations  
//! - Recovery from high-load conditions
//! - Latency characteristics under pressure
//!
//! Run with: cargo test -p rivven-integration-tests --test stress -- --nocapture

use anyhow::Result;
use bytes::Bytes;
use rivven_client::Client;
use rivven_core::Config;
use rivven_integration_tests::fixtures::TestBroker;
use rivven_integration_tests::helpers::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::info;

/// Configuration for stress tests
struct StressConfig {
    /// Number of messages to produce
    message_count: usize,
    /// Message size in bytes
    message_size: usize,
    /// Number of concurrent producers
    producer_count: usize,
    /// Number of partitions
    partition_count: u32,
    /// Duration limit for the test
    timeout: Duration,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            message_count: 10_000,
            message_size: 1024,
            producer_count: 4,
            partition_count: 4,
            timeout: Duration::from_secs(60),
        }
    }
}

/// Helper to consume all messages from all partitions
async fn consume_all(client: &mut Client, topic: &str, partition_count: u32) -> Result<usize> {
    let mut total = 0;
    for partition in 0..partition_count {
        let mut offset = 0u64;
        loop {
            let messages = client.consume(topic, partition, offset, 2000).await?;
            if messages.is_empty() {
                break;
            }
            total += messages.len();
            offset = messages.last().unwrap().offset + 1;
        }
    }
    Ok(total)
}

/// Test sustained high throughput
#[tokio::test]
async fn test_sustained_throughput() -> Result<()> {
    init_tracing();

    let config = StressConfig::default();
    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("stress-throughput");
    client
        .create_topic(&topic, Some(config.partition_count))
        .await?;

    let message = Bytes::from(vec![b'x'; config.message_size]);
    let start = Instant::now();
    let mut produced = 0;

    info!(
        "Starting sustained throughput test: {} messages of {} bytes",
        config.message_count, config.message_size
    );

    while produced < config.message_count && start.elapsed() < config.timeout {
        let partition = (produced % config.partition_count as usize) as u32;
        client
            .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
            .await?;
        produced += 1;

        if produced % 1000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let throughput = produced as f64 / elapsed;
            info!(
                "Progress: {}/{} messages, throughput: {:.0} msg/s",
                produced, config.message_count, throughput
            );
        }
    }

    let elapsed = start.elapsed();
    let throughput = produced as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (produced * config.message_size) as f64 / elapsed.as_secs_f64() / 1_000_000.0;

    info!(
        "Sustained throughput test complete: {} messages in {:.2}s ({:.0} msg/s, {:.2} MB/s)",
        produced,
        elapsed.as_secs_f64(),
        throughput,
        mb_per_sec
    );

    // Verify all messages were stored
    let total_consumed = consume_all(&mut client, &topic, config.partition_count).await?;
    assert_eq!(total_consumed, produced);

    broker.shutdown().await?;
    Ok(())
}

/// Test concurrent producers
#[tokio::test]
async fn test_concurrent_producers() -> Result<()> {
    init_tracing();

    let config = StressConfig {
        message_count: 5_000,
        producer_count: 8,
        ..Default::default()
    };

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("stress-concurrent");
    client
        .create_topic(&topic, Some(config.partition_count))
        .await?;

    let produced = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let messages_per_producer = config.message_count / config.producer_count;
    let broker_addr = broker.connection_string();

    info!(
        "Starting concurrent producer test: {} producers, {} messages each",
        config.producer_count, messages_per_producer
    );

    let mut handles = Vec::new();
    for producer_id in 0..config.producer_count {
        let topic = topic.clone();
        let addr = broker_addr.clone();
        let produced = Arc::clone(&produced);
        let errors = Arc::clone(&errors);
        let message_size = config.message_size;

        handles.push(tokio::spawn(async move {
            let mut client = match Client::connect(&addr).await {
                Ok(c) => c,
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let message = Bytes::from(vec![b'x'; message_size]);
            for _ in 0..messages_per_producer {
                let partition = (producer_id % 4) as u32;
                if client
                    .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
                    .await
                    .is_ok()
                {
                    produced.fetch_add(1, Ordering::Relaxed);
                } else {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_produced = produced.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);
    let throughput = total_produced as f64 / elapsed.as_secs_f64();

    info!(
        "Concurrent producer test complete: {} produced, {} errors in {:.2}s ({:.0} msg/s)",
        total_produced,
        total_errors,
        elapsed.as_secs_f64(),
        throughput
    );

    assert_eq!(
        total_errors, 0,
        "Expected no errors during concurrent produce"
    );
    assert_eq!(total_produced as usize, config.message_count);

    broker.shutdown().await?;
    Ok(())
}

/// Test burst traffic patterns
#[tokio::test]
async fn test_burst_traffic() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("stress-burst");
    client.create_topic(&topic, Some(4)).await?;

    let message = Bytes::from(vec![b'x'; 512]);
    let burst_size = 500;
    let burst_count = 10;
    let cooldown = Duration::from_millis(100);

    info!(
        "Starting burst traffic test: {} bursts of {} messages",
        burst_count, burst_size
    );

    let mut total_produced = 0;
    let start = Instant::now();

    for burst in 0..burst_count {
        let burst_start = Instant::now();

        for i in 0..burst_size {
            let partition = (i % 4) as u32;
            client
                .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
                .await?;
            total_produced += 1;
        }

        let burst_elapsed = burst_start.elapsed();
        info!(
            "Burst {}/{}: {} messages in {:?}",
            burst + 1,
            burst_count,
            burst_size,
            burst_elapsed
        );

        tokio::time::sleep(cooldown).await;
    }

    let elapsed = start.elapsed();
    info!(
        "Burst traffic test complete: {} messages in {:.2}s",
        total_produced,
        elapsed.as_secs_f64()
    );

    // Verify all messages stored
    let mut consumed = 0;
    for partition in 0..4 {
        let mut offset = 0u64;
        loop {
            let messages = client.consume(&topic, partition, offset, 2000).await?;
            if messages.is_empty() {
                break;
            }
            consumed += messages.len();
            offset = messages.last().unwrap().offset + 1;
        }
    }
    assert_eq!(consumed, total_produced);

    broker.shutdown().await?;
    Ok(())
}

/// Test large message handling
#[tokio::test]
async fn test_large_messages() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("stress-large-msg");
    client.create_topic(&topic, Some(1)).await?;

    // Test various message sizes
    let sizes = [
        1024,        // 1 KB
        10 * 1024,   // 10 KB
        100 * 1024,  // 100 KB
        512 * 1024,  // 512 KB
        1024 * 1024, // 1 MB
    ];

    info!("Testing large message handling with sizes: {:?}", sizes);

    for &size in &sizes {
        let message = Bytes::from(vec![b'A'; size]);
        let start = Instant::now();
        client.publish(&topic, message.clone()).await?;
        let publish_time = start.elapsed();
        info!("Published {} byte message in {:?}", size, publish_time);
    }

    // Consume and verify - need to consume in chunks for large messages
    let mut messages = Vec::new();
    let mut offset = 0u64;
    while messages.len() < sizes.len() {
        let batch = client.consume(&topic, 0, offset, 10).await?;
        if batch.is_empty() {
            break;
        }
        offset = batch.last().unwrap().offset + 1;
        messages.extend(batch);
    }
    assert_eq!(
        messages.len(),
        sizes.len(),
        "Expected {} messages",
        sizes.len()
    );

    for (msg, &expected_size) in messages.iter().zip(sizes.iter()) {
        assert_eq!(
            msg.value.len(),
            expected_size,
            "Message size mismatch for {} bytes",
            expected_size
        );
    }

    info!("Large message test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test memory stability under load
#[tokio::test]
async fn test_memory_stability() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-stress-memory-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };
    let broker = TestBroker::start_with_config(config).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("stress-memory");
    client.create_topic(&topic, Some(4)).await?;

    let message = Bytes::from(vec![b'M'; 4096]); // 4KB messages
    let batch_size = 1000;
    let batch_count = 10;

    info!(
        "Testing memory stability: {} batches of {} messages (4KB each)",
        batch_count, batch_size
    );

    let mut total = 0;
    for batch in 0..batch_count {
        for i in 0..batch_size {
            let partition = (i % 4) as u32;
            client
                .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
                .await?;
            total += 1;
        }
        info!("Completed batch {}/{}", batch + 1, batch_count);

        // Small delay between batches to allow processing
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify data integrity
    let consumed = consume_all(&mut client, &topic, 4).await?;
    assert_eq!(consumed, total);

    info!("Memory stability test passed: {} messages processed", total);
    broker.shutdown().await?;
    Ok(())
}

/// Test many topics scenario
#[tokio::test]
async fn test_many_topics() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic_count = 50;
    let messages_per_topic = 100;
    let message = Bytes::from(b"test-message".to_vec());

    info!(
        "Testing many topics: {} topics with {} messages each",
        topic_count, messages_per_topic
    );

    let start = Instant::now();
    let mut topics = Vec::new();

    // Create topics
    for i in 0..topic_count {
        let topic = unique_topic_name(&format!("many-{}", i));
        client.create_topic(&topic, Some(1)).await?;
        topics.push(topic);
    }
    info!("Created {} topics in {:?}", topic_count, start.elapsed());

    // Produce to all topics
    let produce_start = Instant::now();
    for topic in &topics {
        for _ in 0..messages_per_topic {
            client.publish(topic, message.clone()).await?;
        }
    }
    info!(
        "Produced {} total messages in {:?}",
        topic_count * messages_per_topic,
        produce_start.elapsed()
    );

    // Verify each topic
    for topic in &topics {
        let messages = client.consume(topic, 0, 0, 200).await?;
        assert_eq!(messages.len(), messages_per_topic);
    }

    info!("Many topics test passed");
    broker.shutdown().await?;
    Ok(())
}

/// Test recovery after simulated high load
#[tokio::test]
async fn test_recovery_after_high_load() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-stress-recovery-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let topic = unique_topic_name("stress-recovery");
    let message = Bytes::from(vec![b'R'; 2048]);
    let high_load_count = 5000;

    // Phase 1: Apply high load
    info!("Phase 1: Applying high load ({} messages)", high_load_count);
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        client.create_topic(&topic, Some(4)).await?;

        for i in 0..high_load_count {
            let partition = (i % 4) as u32;
            client
                .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
                .await?;
        }

        broker.shutdown().await?;
    }

    // Phase 2: Restart and verify recovery
    info!("Phase 2: Restarting broker and verifying recovery");
    {
        let broker = TestBroker::start_with_config(config.clone()).await?;
        let mut client = Client::connect(&broker.connection_string()).await?;

        // Verify topic exists and has all data
        let total = consume_all(&mut client, &topic, 4).await?;

        assert_eq!(
            total, high_load_count,
            "Expected all {} messages to be recovered",
            high_load_count
        );

        // Verify we can still produce after recovery
        client
            .publish(&topic, Bytes::from(b"post-recovery".to_vec()))
            .await?;

        broker.shutdown().await?;
    }

    info!("Recovery after high load test passed");

    // Cleanup
    let _ = tokio::fs::remove_dir_all(&config.data_dir).await;

    Ok(())
}

/// Test concurrent readers and writers
#[tokio::test]
async fn test_concurrent_read_write() -> Result<()> {
    init_tracing();

    let broker = TestBroker::start().await?;
    let topic = unique_topic_name("stress-rw");

    // Setup
    {
        let mut client = Client::connect(&broker.connection_string()).await?;
        client.create_topic(&topic, Some(4)).await?;
    }

    let broker_addr = broker.connection_string();
    let write_count = Arc::new(AtomicU64::new(0));
    let read_count = Arc::new(AtomicU64::new(0));
    let test_duration = Duration::from_secs(5);

    info!(
        "Starting concurrent read/write test for {:?}",
        test_duration
    );

    let start = Instant::now();
    let mut handles = Vec::new();

    // Writer tasks
    for writer_id in 0..4 {
        let addr = broker_addr.clone();
        let topic = topic.clone();
        let write_count = Arc::clone(&write_count);
        let message = Bytes::from(format!("writer-{}-message", writer_id).into_bytes());

        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            let start = Instant::now();
            while start.elapsed() < test_duration {
                let partition = (writer_id % 4) as u32;
                if client
                    .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
                    .await
                    .is_ok()
                {
                    write_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Reader tasks
    for reader_id in 0..4 {
        let addr = broker_addr.clone();
        let topic = topic.clone();
        let read_count = Arc::clone(&read_count);

        handles.push(tokio::spawn(async move {
            let mut client = Client::connect(&addr).await.unwrap();
            let start = Instant::now();
            let mut offset = 0u64;
            while start.elapsed() < test_duration {
                let partition = (reader_id % 4) as u32;
                if let Ok(msgs) = client.consume(&topic, partition, offset, 100).await {
                    read_count.fetch_add(msgs.len() as u64, Ordering::Relaxed);
                    if !msgs.is_empty() {
                        offset = msgs.last().unwrap().offset + 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let writes = write_count.load(Ordering::Relaxed);
    let reads = read_count.load(Ordering::Relaxed);

    info!(
        "Concurrent read/write test complete: {} writes, {} reads in {:?}",
        writes,
        reads,
        start.elapsed()
    );

    assert!(writes > 0, "Expected some writes to succeed");
    assert!(reads > 0, "Expected some reads to succeed");

    broker.shutdown().await?;
    Ok(())
}

/// Long-running soak test (marked as ignored by default)
#[tokio::test]
#[ignore = "Long-running soak test - run manually"]
async fn soak_test_extended_load() -> Result<()> {
    init_tracing();

    let config = Config {
        enable_persistence: true,
        data_dir: format!("/tmp/rivven-soak-{}", uuid::Uuid::new_v4()),
        ..Default::default()
    };

    let broker = TestBroker::start_with_config(config.clone()).await?;
    let mut client = Client::connect(&broker.connection_string()).await?;

    let topic = unique_topic_name("soak");
    client.create_topic(&topic, Some(8)).await?;

    let duration = Duration::from_secs(300); // 5 minutes
    let message = Bytes::from(vec![b'S'; 1024]);
    let start = Instant::now();
    let mut produced = 0u64;
    let mut checkpoint_produced = 0u64;
    let checkpoint_interval = Duration::from_secs(30);
    let mut last_checkpoint = Instant::now();

    info!("Starting soak test for {:?}", duration);

    while start.elapsed() < duration {
        let partition = (produced % 8) as u32;
        client
            .publish_to_partition(&topic, partition, None::<Bytes>, message.clone())
            .await?;
        produced += 1;

        if last_checkpoint.elapsed() >= checkpoint_interval {
            let delta = produced - checkpoint_produced;
            let throughput = delta as f64 / checkpoint_interval.as_secs_f64();
            info!(
                "Soak checkpoint: {} total messages, {:.0} msg/s in last interval",
                produced, throughput
            );
            checkpoint_produced = produced;
            last_checkpoint = Instant::now();
        }
    }

    let final_throughput = produced as f64 / start.elapsed().as_secs_f64();
    info!(
        "Soak test complete: {} messages in {:?} ({:.0} msg/s average)",
        produced,
        start.elapsed(),
        final_throughput
    );

    // Verify data integrity
    let mut consumed = 0;
    for partition in 0..8 {
        let messages = client
            .consume(&topic, partition, 0, u32::MAX as usize)
            .await?;
        consumed += messages.len();
    }
    assert_eq!(consumed as u64, produced);

    broker.shutdown().await?;
    let _ = tokio::fs::remove_dir_all(&config.data_dir).await;

    Ok(())
}
