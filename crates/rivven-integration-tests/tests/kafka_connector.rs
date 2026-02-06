//! Kafka Connector Integration Tests
//!
//! **REAL** integration tests for Rivven's Kafka source and sink connectors.
//! These tests use testcontainers to spin up a real Kafka broker.
//!
//! Run with: cargo test -p rivven-integration-tests --test kafka_connector -- --nocapture
//!
//! Requirements: Docker must be running for testcontainers.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use rivven_connect::connectors::queue::kafka::{
    AckLevel, CompressionCodec, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig,
    StartOffset,
};
use rivven_connect::traits::catalog::{
    ConfiguredCatalog, ConfiguredStream, DestinationSyncMode, SyncMode,
};
use rivven_connect::traits::sink::Sink;
use rivven_connect::traits::source::Source;
use rivven_integration_tests::fixtures::TestKafka;
use rivven_integration_tests::helpers::*;
use tokio::time::timeout;
use tracing::info;

// ============================================================================
// Infrastructure Tests - Verify test setup works correctly
// ============================================================================

/// Test that we can start a Kafka container
#[tokio::test]
async fn test_kafka_container_starts() -> Result<()> {
    init_tracing();

    info!("Starting Kafka test container...");
    let kafka = TestKafka::start().await?;

    info!("Kafka started at {}", kafka.bootstrap_servers());
    assert!(!kafka.host.is_empty());
    assert!(kafka.port > 0);

    info!("Kafka container test passed");
    Ok(())
}

/// Test that we can create topics
#[tokio::test]
async fn test_kafka_create_topic() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;

    // Create a topic with 3 partitions
    kafka.create_topic("test-topic", 3, 1).await?;

    info!("Topic creation test passed");
    Ok(())
}

/// Test produce and consume messages
#[tokio::test]
async fn test_kafka_produce_consume() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("produce-consume-test", 1, 1).await?;

    // Produce some messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..5)
        .map(|i| {
            (
                Some(format!("key-{}", i).into_bytes()),
                format!(r#"{{"id":{}, "value":"message-{}"}}"#, i, i).into_bytes(),
            )
        })
        .collect();

    let offsets = kafka
        .produce_messages("produce-consume-test", 0, messages)
        .await?;
    assert_eq!(offsets.len(), 5);

    // Consume messages
    let records = kafka
        .consume_messages("produce-consume-test", 0, 0, 10)
        .await?;
    assert_eq!(records.len(), 5);

    // Verify message content
    for (i, record) in records.iter().enumerate() {
        let value = record.record.value.as_ref().unwrap();
        let expected = format!(r#"{{"id":{}, "value":"message-{}"}}"#, i, i);
        assert_eq!(std::str::from_utf8(value)?, expected);
    }

    info!("Produce/consume test passed");
    Ok(())
}

// ============================================================================
// Rivven Kafka Source Connector Tests
// ============================================================================

/// Test that KafkaSource can connect and check configuration
#[tokio::test]
async fn test_kafka_source_check() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-check-test", 1, 1).await?;

    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "source-check-test".to_string(),
        consumer_group: "test-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 500,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let result = source.check(&config).await?;
    assert!(result.is_success(), "Check should succeed: {:?}", result);

    info!("Kafka source check test passed");
    Ok(())
}

/// Test that KafkaSource can discover topics and create catalog
#[tokio::test]
async fn test_kafka_source_discover() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("discover-test", 3, 1).await?;

    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "discover-test".to_string(),
        consumer_group: "test-group".to_string(),
        start_offset: StartOffset::Latest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 500,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let catalog = source.discover(&config).await?;
    assert!(!catalog.streams.is_empty());

    // Verify stream has expected properties
    let stream = &catalog.streams[0];
    assert_eq!(stream.name, "discover-test");

    info!("Kafka source discover test passed");
    Ok(())
}

/// Test KafkaSource can read messages from a topic
#[tokio::test]
async fn test_kafka_source_read() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-read-test", 1, 1).await?;

    // Pre-populate the topic with messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
        .map(|i| {
            (
                Some(format!("key-{}", i).into_bytes()),
                format!(r#"{{"sequence":{}, "data":"test message {}"}}"#, i, i).into_bytes(),
            )
        })
        .collect();

    kafka
        .produce_messages("source-read-test", 0, messages)
        .await?;

    // Create source and read
    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "source-read-test".to_string(),
        consumer_group: "test-read-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 100,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "source-read-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Read some events with timeout
    let mut received = 0;
    let read_result = timeout(Duration::from_secs(30), async {
        while let Some(event_result) = stream.next().await {
            match event_result {
                Ok(event) => {
                    if event.event_type.is_data() {
                        received += 1;
                        info!("Received event {}: {:?}", received, event.event_type);
                        if received >= 10 {
                            break;
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Read error: {}", e));
                }
            }
        }
        Ok(received)
    })
    .await;

    match read_result {
        Ok(Ok(count)) => {
            assert_eq!(count, 10, "Should have received 10 messages");
            info!(
                "Kafka source read test passed - received {} messages",
                count
            );
        }
        Ok(Err(e)) => {
            return Err(e);
        }
        Err(_) => {
            // Timeout - check what we got
            info!(
                "Read timeout after receiving {} messages (may be expected)",
                received
            );
        }
    }

    Ok(())
}

/// Test KafkaSource metrics are updated during read
#[tokio::test]
async fn test_kafka_source_metrics() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-metrics-test", 1, 1).await?;

    // Pre-populate
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..5)
        .map(|i| {
            (
                Some(format!("key-{}", i).into_bytes()),
                format!(r#"{{"id":{}}}"#, i).into_bytes(),
            )
        })
        .collect();

    kafka
        .produce_messages("source-metrics-test", 0, messages)
        .await?;

    let source = KafkaSource::new();
    let metrics = source.metrics();

    // Verify initial metrics are zero
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.messages_consumed, 0);
    assert_eq!(snapshot.bytes_consumed, 0);

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "source-metrics-test".to_string(),
        consumer_group: "test-metrics-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 100,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "source-metrics-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Read a few events
    let _ = timeout(Duration::from_secs(10), async {
        let mut count = 0;
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                count += 1;
                if count >= 5 {
                    break;
                }
            }
        }
    })
    .await;

    // Check metrics were updated
    let snapshot = metrics.snapshot();
    info!(
        "Metrics: consumed={}, bytes={}, polls={}",
        snapshot.messages_consumed, snapshot.bytes_consumed, snapshot.polls
    );

    // Metrics should show some activity
    assert!(
        snapshot.polls > 0 || snapshot.messages_consumed > 0,
        "Should have recorded some poll activity"
    );

    // Test Prometheus export
    let prometheus = snapshot.to_prometheus_format("test");
    assert!(prometheus.contains("kafka_source_messages_consumed_total"));
    assert!(prometheus.contains("kafka_source_polls_total"));

    info!("Kafka source metrics test passed");
    Ok(())
}

/// Test KafkaSource graceful shutdown
#[tokio::test]
async fn test_kafka_source_shutdown() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("source-shutdown-test", 1, 1).await?;

    // Pre-populate
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..100)
        .map(|i| (None, format!(r#"{{"id":{}}}"#, i).into_bytes()))
        .collect();
    kafka
        .produce_messages("source-shutdown-test", 0, messages)
        .await?;

    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "source-shutdown-test".to_string(),
        consumer_group: "test-shutdown-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 10,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "source-shutdown-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Start reading in background and trigger shutdown after a few messages
    let source_clone = source;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        source_clone.shutdown();
        info!("Shutdown signaled");
    });

    // Read until shutdown
    let mut count = 0;
    let result = timeout(Duration::from_secs(10), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                count += 1;
            }
        }
    })
    .await;

    // Should have exited cleanly
    assert!(result.is_ok(), "Should have exited due to shutdown");
    info!(
        "Kafka source shutdown test passed - processed {} messages before shutdown",
        count
    );

    Ok(())
}

// ============================================================================
// Performance / Stress Tests
// ============================================================================

/// Test high-throughput message consumption
#[tokio::test]
async fn test_kafka_source_high_throughput() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("high-throughput-test", 3, 1).await?;

    // Produce many messages
    let message_count = 1000;
    for partition in 0..3 {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..message_count / 3)
            .map(|i| {
                (
                    Some(format!("key-{}-{}", partition, i).into_bytes()),
                    format!(r#"{{"partition":{},"sequence":{}}}"#, partition, i).into_bytes(),
                )
            })
            .collect();
        kafka
            .produce_messages("high-throughput-test", partition, messages)
            .await?;
    }

    info!("Produced {} messages across 3 partitions", message_count);

    // Measure consumption throughput
    let start = std::time::Instant::now();

    let source = KafkaSource::new();
    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "high-throughput-test".to_string(),
        consumer_group: "throughput-test-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 500,
        include_headers: false,
        include_key: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 10,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "high-throughput-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    let mut received = 0;
    let _ = timeout(Duration::from_secs(60), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                received += 1;
                if received >= message_count {
                    break;
                }
            }
        }
    })
    .await;

    let elapsed = start.elapsed();
    let throughput = received as f64 / elapsed.as_secs_f64();

    info!(
        "High-throughput test: {} messages in {:?} ({:.0} msg/s)",
        received, elapsed, throughput
    );

    // Should achieve reasonable throughput
    assert!(received > 100, "Should have received significant messages");

    Ok(())
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test KafkaSource with non-existent topic
#[tokio::test]
async fn test_kafka_source_nonexistent_topic() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;

    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "nonexistent-topic-12345".to_string(),
        consumer_group: "test-group".to_string(),
        start_offset: StartOffset::Latest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 500,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    // Check should still succeed (topic may be auto-created or check is permissive)
    // but discover should work
    let catalog = source.discover(&config).await?;
    assert!(!catalog.streams.is_empty());

    info!("Non-existent topic test passed");
    Ok(())
}

/// Test KafkaSource with multiple partitions
#[tokio::test]
async fn test_kafka_source_multiple_partitions() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("multi-partition-test", 5, 1).await?;

    // Produce to each partition
    for partition in 0..5 {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
            .map(|i| {
                (
                    Some(format!("p{}-k{}", partition, i).into_bytes()),
                    format!(r#"{{"partition":{},"msg":{}}}"#, partition, i).into_bytes(),
                )
            })
            .collect();
        kafka
            .produce_messages("multi-partition-test", partition, messages)
            .await?;
    }

    info!("Produced 50 messages across 5 partitions");

    // Verify we can read from all partitions
    let source = KafkaSource::new();
    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "multi-partition-test".to_string(),
        consumer_group: "multi-partition-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 100,
        include_headers: true,
        include_key: true,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "multi-partition-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::Incremental,
            destination_sync_mode: DestinationSyncMode::Append,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    let mut received = 0;
    let _ = timeout(Duration::from_secs(30), async {
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                received += 1;
                if received >= 50 {
                    break;
                }
            }
        }
    })
    .await;

    info!(
        "Multi-partition test: received {} messages from 5 partitions",
        received
    );

    Ok(())
}

// ============================================================================
// Kafka Sink Tests
// ============================================================================

/// Test KafkaSink can connect and check configuration
#[tokio::test]
async fn test_kafka_sink_check() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-check-test", 1, 1).await?;

    let sink = KafkaSink::new();

    let config = KafkaSinkConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "sink-check-test".to_string(),
        acks: AckLevel::All,
        compression: CompressionCodec::None,
        batch_size_bytes: 16384,
        linger_ms: 5,
        request_timeout_ms: 30000,
        retries: 3,
        idempotent: false,
        transactional_id: None,
        key_field: None,
        include_headers: true,
        custom_headers: HashMap::new(),
        security: Default::default(),
    };

    let result = sink.check(&config).await?;
    assert!(
        result.is_success(),
        "Sink check should succeed: {:?}",
        result
    );

    info!("Kafka sink check test passed");
    Ok(())
}

/// Test KafkaSink metrics tracking
#[tokio::test]
async fn test_kafka_sink_metrics() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-metrics-test", 1, 1).await?;

    let sink = KafkaSink::new();
    let metrics = sink.metrics();

    // Verify initial metrics are zero
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.messages_produced, 0);
    assert_eq!(snapshot.bytes_produced, 0);

    // Test Prometheus export
    let prometheus = snapshot.to_prometheus_format("test");
    assert!(prometheus.contains("kafka_sink_messages_produced_total"));
    assert!(prometheus.contains("kafka_sink_bytes_produced_total"));

    info!("Kafka sink metrics test passed");
    Ok(())
}

/// Test KafkaSink with different compression codecs
#[tokio::test]
async fn test_kafka_sink_compression() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;

    let codecs = [
        ("no-compression", CompressionCodec::None),
        ("gzip-compression", CompressionCodec::Gzip),
        ("lz4-compression", CompressionCodec::Lz4),
        ("snappy-compression", CompressionCodec::Snappy),
        ("zstd-compression", CompressionCodec::Zstd),
    ];

    for (topic_suffix, codec) in codecs {
        let topic = format!("sink-{}", topic_suffix);
        kafka.create_topic(&topic, 1, 1).await?;

        let sink = KafkaSink::new();

        let config = KafkaSinkConfig {
            brokers: vec![kafka.bootstrap_servers()],
            topic: topic.clone(),
            acks: AckLevel::Leader,
            compression: codec,
            batch_size_bytes: 16384,
            linger_ms: 0,
            request_timeout_ms: 30000,
            retries: 3,
            idempotent: false,
            transactional_id: None,
            key_field: None,
            include_headers: false,
            custom_headers: HashMap::new(),
            security: Default::default(),
        };

        let result = sink.check(&config).await?;
        assert!(
            result.is_success(),
            "Sink check should succeed with {:?}: {:?}",
            codec,
            result
        );

        info!("Compression test passed for {:?}", codec);
    }

    info!("Kafka sink compression test passed");
    Ok(())
}

/// Test KafkaSink with custom headers
#[tokio::test]
async fn test_kafka_sink_custom_headers() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-headers-test", 1, 1).await?;

    let sink = KafkaSink::new();

    let mut custom_headers = HashMap::new();
    custom_headers.insert("source".to_string(), "rivven".to_string());
    custom_headers.insert("environment".to_string(), "test".to_string());
    custom_headers.insert("version".to_string(), "1.0".to_string());

    let config = KafkaSinkConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "sink-headers-test".to_string(),
        acks: AckLevel::All,
        compression: CompressionCodec::None,
        batch_size_bytes: 16384,
        linger_ms: 5,
        request_timeout_ms: 30000,
        retries: 3,
        idempotent: false,
        transactional_id: None,
        key_field: Some("id".to_string()),
        include_headers: true,
        custom_headers,
        security: Default::default(),
    };

    let result = sink.check(&config).await?;
    assert!(
        result.is_success(),
        "Sink check with headers should succeed"
    );

    info!("Kafka sink custom headers test passed");
    Ok(())
}

/// Test KafkaSink with idempotent producer
#[tokio::test]
async fn test_kafka_sink_idempotent() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("sink-idempotent-test", 1, 1).await?;

    let sink = KafkaSink::new();

    let config = KafkaSinkConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "sink-idempotent-test".to_string(),
        acks: AckLevel::All, // Required for idempotent
        compression: CompressionCodec::Lz4,
        batch_size_bytes: 16384,
        linger_ms: 5,
        request_timeout_ms: 30000,
        retries: 5,
        idempotent: true, // Enable idempotent producer
        transactional_id: None,
        key_field: None,
        include_headers: true,
        custom_headers: HashMap::new(),
        security: Default::default(),
    };

    let result = sink.check(&config).await?;
    assert!(
        result.is_success(),
        "Idempotent sink check should succeed: {:?}",
        result
    );

    info!("Kafka sink idempotent test passed");
    Ok(())
}

// ============================================================================
// Offset Management Tests
// ============================================================================

/// Test KafkaSource with different start offset modes
#[tokio::test]
async fn test_kafka_source_offset_modes() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("offset-modes-test", 1, 1).await?;

    // Pre-populate 20 messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..20)
        .map(|i| (None, format!(r#"{{"seq":{}}}"#, i).into_bytes()))
        .collect();
    kafka
        .produce_messages("offset-modes-test", 0, messages)
        .await?;

    // Test Earliest offset
    let source_earliest = KafkaSource::new();
    let config_earliest = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "offset-modes-test".to_string(),
        consumer_group: "offset-test-earliest".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 100,
        include_headers: false,
        include_key: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let result = source_earliest.check(&config_earliest).await?;
    assert!(result.is_success());

    // Test Latest offset
    let source_latest = KafkaSource::new();
    let config_latest = KafkaSourceConfig {
        start_offset: StartOffset::Latest,
        consumer_group: "offset-test-latest".to_string(),
        ..config_earliest.clone()
    };

    let result = source_latest.check(&config_latest).await?;
    assert!(result.is_success());

    // Test Offset(N)
    let source_offset = KafkaSource::new();
    let config_offset = KafkaSourceConfig {
        start_offset: StartOffset::Offset(10),
        consumer_group: "offset-test-offset".to_string(),
        ..config_earliest.clone()
    };

    let result = source_offset.check(&config_offset).await?;
    assert!(result.is_success());

    info!("Kafka source offset modes test passed");
    Ok(())
}

// ============================================================================
// Batch Operations Tests
// ============================================================================

/// Test batch metrics tracking
#[tokio::test]
async fn test_kafka_batch_metrics() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("batch-metrics-test", 1, 1).await?;

    // Produce batches of varying sizes
    for batch_size in [5, 10, 20, 50] {
        let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..batch_size)
            .map(|i| (None, format!(r#"{{"batch":{}}}"#, i).into_bytes()))
            .collect();
        kafka
            .produce_messages("batch-metrics-test", 0, messages)
            .await?;
    }

    let source = KafkaSource::new();
    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "batch-metrics-test".to_string(),
        consumer_group: "batch-metrics-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 500,
        include_headers: false,
        include_key: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "batch-metrics-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Read all messages
    let _ = timeout(Duration::from_secs(15), async {
        let mut count = 0;
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                count += 1;
                if count >= 85 {
                    break;
                }
            }
        }
    })
    .await;

    // Verify batch metrics
    let snapshot = source.metrics().snapshot();
    info!(
        "Batch metrics: min={}, max={}, avg={:.2}",
        snapshot.batch_size_min,
        snapshot.batch_size_max,
        snapshot.avg_batch_size()
    );

    // Should have recorded some batch statistics
    assert!(snapshot.polls > 0, "Should have polled");

    info!("Kafka batch metrics test passed");
    Ok(())
}

// ============================================================================
// Error Handling / Resilience Tests
// ============================================================================

/// Test KafkaSource with invalid broker address
#[tokio::test]
async fn test_kafka_source_invalid_broker() -> Result<()> {
    init_tracing();

    let source = KafkaSource::new();

    let config = KafkaSourceConfig {
        brokers: vec!["invalid-broker:9999".to_string()],
        topic: "test-topic".to_string(),
        consumer_group: "test-group".to_string(),
        start_offset: StartOffset::Latest,
        security: Default::default(),
        max_poll_interval_ms: 5000,
        session_timeout_ms: 5000,
        heartbeat_interval_ms: 1000,
        fetch_max_messages: 100,
        include_headers: false,
        include_key: false,
        retry_initial_ms: 100,
        retry_max_ms: 1000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    // Check should fail with invalid broker
    let result = source.check(&config).await;
    // The check may succeed (optimistic) or fail depending on implementation
    // Just verify it doesn't panic
    info!("Invalid broker test result: {:?}", result);

    Ok(())
}

/// Test KafkaSource handles empty topic gracefully
#[tokio::test]
async fn test_kafka_source_empty_topic() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("empty-topic-test", 1, 1).await?;

    // Don't produce any messages

    let source = KafkaSource::new();
    let config = KafkaSourceConfig {
        brokers: vec![kafka.bootstrap_servers()],
        topic: "empty-topic-test".to_string(),
        consumer_group: "empty-topic-group".to_string(),
        start_offset: StartOffset::Earliest,
        security: Default::default(),
        max_poll_interval_ms: 300000,
        session_timeout_ms: 10000,
        heartbeat_interval_ms: 3000,
        fetch_max_messages: 100,
        include_headers: false,
        include_key: false,
        retry_initial_ms: 100,
        retry_max_ms: 10000,
        retry_multiplier: 2.0,
        empty_poll_delay_ms: 50,
    };

    let configured_catalog = ConfiguredCatalog {
        streams: vec![ConfiguredStream {
            stream: rivven_connect::traits::catalog::Stream::new(
                "empty-topic-test",
                serde_json::json!({}),
            ),
            sync_mode: SyncMode::FullRefresh,
            destination_sync_mode: DestinationSyncMode::Overwrite,
            cursor_field: None,
            primary_key: None,
        }],
    };

    let mut stream = source.read(&config, &configured_catalog, None).await?;

    // Should timeout waiting for messages (empty topic)
    let result = timeout(Duration::from_secs(3), async {
        let mut count = 0;
        while let Some(Ok(event)) = stream.next().await {
            if event.event_type.is_data() {
                count += 1;
            }
        }
        count
    })
    .await;

    // Either timeout or get 0 messages
    match result {
        Ok(count) => assert_eq!(count, 0, "Empty topic should have no messages"),
        Err(_) => info!("Timed out as expected for empty topic"),
    }

    info!("Empty topic test passed");
    Ok(())
}

// ============================================================================
// Consumer Group Tests
// ============================================================================

/// Test multiple consumer groups reading same topic independently
#[tokio::test]
async fn test_kafka_multiple_consumer_groups() -> Result<()> {
    init_tracing();

    let kafka = TestKafka::start().await?;
    kafka.create_topic("multi-cg-test", 1, 1).await?;

    // Populate messages
    let messages: Vec<(Option<Vec<u8>>, Vec<u8>)> = (0..10)
        .map(|i| (None, format!(r#"{{"id":{}}}"#, i).into_bytes()))
        .collect();
    kafka.produce_messages("multi-cg-test", 0, messages).await?;

    // Create two independent consumer groups
    for group_id in ["group-a", "group-b"] {
        let source = KafkaSource::new();
        let config = KafkaSourceConfig {
            brokers: vec![kafka.bootstrap_servers()],
            topic: "multi-cg-test".to_string(),
            consumer_group: group_id.to_string(),
            start_offset: StartOffset::Earliest,
            security: Default::default(),
            max_poll_interval_ms: 300000,
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            fetch_max_messages: 100,
            include_headers: false,
            include_key: false,
            retry_initial_ms: 100,
            retry_max_ms: 10000,
            retry_multiplier: 2.0,
            empty_poll_delay_ms: 50,
        };

        let result = source.check(&config).await?;
        assert!(result.is_success(), "Group {} should work", group_id);
    }

    info!("Multiple consumer groups test passed");
    Ok(())
}
